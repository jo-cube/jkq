use std::{
    fmt,
    io::{self, Write},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use crossbeam_channel::{Receiver, Sender, TryRecvError, bounded};
use signal_hook::{
    SigId,
    consts::{SIGINT, SIGTERM},
    flag, low_level,
};

use crate::{
    cli::{OutputPlan, RuntimeConfig},
    kafka::{KafkaInput, OwnedRecord, PollEvent},
    output::{self, EmittedAction, Header, OutputRecord, Payload, Timestamp},
    transform::jsonata::{self, Action, ExecutionIssue, PassPayload, TransformError},
};

mod state;

use state::{Admission, Orderer, Release};

const CONTROL_POLL: Duration = Duration::from_millis(10);
const BATCH_RECORDS: usize = 64;
const BATCH_BYTES: usize = 1024 * 1024;

#[derive(Debug)]
pub enum PipelineError {
    Runtime(String),
    Output(io::Error),
}

enum RecordedFailure {
    Runtime(String),
    Output(io::ErrorKind, String),
}

impl RecordedFailure {
    fn from_error(error: &PipelineError) -> Self {
        match error {
            PipelineError::Runtime(message) => Self::Runtime(message.clone()),
            PipelineError::Output(error) => Self::Output(error.kind(), error.to_string()),
        }
    }

    fn into_error(self) -> PipelineError {
        match self {
            Self::Runtime(message) => PipelineError::Runtime(message),
            Self::Output(kind, message) => PipelineError::Output(io::Error::new(kind, message)),
        }
    }
}

fn record_failure(first: &OnceLock<RecordedFailure>, error: &PipelineError) {
    let _ = first.set(RecordedFailure::from_error(error));
}

impl fmt::Display for PipelineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(message) => formatter.write_str(message),
            Self::Output(error) => write!(formatter, "output error: {error}"),
        }
    }
}

pub struct SignalControl {
    armed: Arc<AtomicBool>,
    received: Arc<AtomicUsize>,
    registrations: Vec<SigId>,
}

impl SignalControl {
    pub fn install() -> Result<Self, String> {
        let armed = Arc::new(AtomicBool::new(false));
        let received = Arc::new(AtomicUsize::new(0));
        let mut registrations = Vec::new();
        for (signal, status) in [(SIGINT, 130), (SIGTERM, 143)] {
            registrations.push(
                flag::register_conditional_shutdown(signal, status, Arc::clone(&armed))
                    .map_err(|error| format!("cannot install signal handler: {error}"))?,
            );
        }
        for signal in [SIGINT, SIGTERM] {
            registrations.push(
                flag::register_usize(
                    signal,
                    Arc::clone(&received),
                    usize::try_from(signal).expect("termination signals are positive"),
                )
                .map_err(|error| format!("cannot install signal handler: {error}"))?,
            );
            registrations.push(
                flag::register(signal, Arc::clone(&armed))
                    .map_err(|error| format!("cannot install signal handler: {error}"))?,
            );
        }
        Ok(Self {
            armed,
            received,
            registrations,
        })
    }

    pub fn parts(&self) -> (Arc<AtomicBool>, Arc<AtomicUsize>) {
        (Arc::clone(&self.armed), Arc::clone(&self.received))
    }
}

impl Drop for SignalControl {
    fn drop(&mut self) {
        for registration in self.registrations.drain(..) {
            low_level::unregister(registration);
        }
    }
}

#[derive(Default)]
pub struct Stats {
    enabled: bool,
    admitted: AtomicU64,
    input_tombstones: AtomicU64,
    input_bytes: AtomicU64,
    dropped: AtomicU64,
    generated_tombstones: AtomicU64,
    passed: AtomicU64,
    projected: AtomicU64,
    invalid_json: AtomicU64,
    evaluation_failures: AtomicU64,
    output_records: AtomicU64,
    output_bytes: AtomicU64,
}

impl Stats {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            ..Self::default()
        }
    }

    pub fn report(&self, elapsed: Duration) -> String {
        format!(
            "admitted={} input_tombstones={} input_bytes={} dropped={} generated_tombstones={} passed={} projected={} invalid_json={} evaluation_failures={} output_records={} output_bytes={} elapsed_ms={}",
            self.admitted.load(Ordering::Relaxed),
            self.input_tombstones.load(Ordering::Relaxed),
            self.input_bytes.load(Ordering::Relaxed),
            self.dropped.load(Ordering::Relaxed),
            self.generated_tombstones.load(Ordering::Relaxed),
            self.passed.load(Ordering::Relaxed),
            self.projected.load(Ordering::Relaxed),
            self.invalid_json.load(Ordering::Relaxed),
            self.evaluation_failures.load(Ordering::Relaxed),
            self.output_records.load(Ordering::Relaxed),
            self.output_bytes.load(Ordering::Relaxed),
            elapsed.as_millis(),
        )
    }

    fn admit(&self, record: &OwnedRecord) {
        if !self.enabled {
            return;
        }
        self.admitted.fetch_add(1, Ordering::Relaxed);
        if record.payload.is_none() {
            self.input_tombstones.fetch_add(1, Ordering::Relaxed);
        }
        add(
            &self.input_bytes,
            record.payload.as_deref().map_or(0, <[u8]>::len),
        );
    }

    fn transformed(&self, action: &Action, issue: Option<ExecutionIssue>, source_tombstone: bool) {
        if !self.enabled {
            return;
        }
        match issue {
            Some(ExecutionIssue::InvalidJson) => {
                self.invalid_json.fetch_add(1, Ordering::Relaxed);
            }
            Some(ExecutionIssue::Evaluation) => {
                self.evaluation_failures.fetch_add(1, Ordering::Relaxed);
            }
            None => {}
        }
        match action {
            Action::Drop => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
            Action::Tombstone if !source_tombstone => {
                self.generated_tombstones.fetch_add(1, Ordering::Relaxed);
            }
            Action::Tombstone => {}
            Action::PassThrough(_) => {
                self.passed.fetch_add(1, Ordering::Relaxed);
            }
            Action::Project(_) => {
                self.projected.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn failed(&self, error: &TransformError) {
        if !self.enabled {
            return;
        }
        match error {
            TransformError::InvalidJson(_) => {
                self.invalid_json.fetch_add(1, Ordering::Relaxed);
            }
            TransformError::Evaluation { .. } => {
                self.evaluation_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn emitted(&self, bytes: usize) {
        if self.enabled {
            self.output_records.fetch_add(1, Ordering::Relaxed);
            add(&self.output_bytes, bytes);
        }
    }
}

fn add(counter: &AtomicU64, value: usize) {
    counter.fetch_add(u64::try_from(value).unwrap_or(u64::MAX), Ordering::Relaxed);
}

struct WorkItem {
    sequence: u64,
    record: OwnedRecord,
}

struct BatchSender<T> {
    sender: Sender<Vec<T>>,
    pending: Vec<T>,
    retained_bytes: usize,
}

enum Dispatcher {
    Transform(BatchSender<WorkItem>),
    Identity(BatchSender<Completion>),
}

#[derive(Debug)]
struct SourceRecord {
    partition: i32,
    offset: i64,
    timestamp: Option<Timestamp>,
    key: Option<Vec<u8>>,
    headers: Vec<Header>,
}

#[derive(Debug)]
struct Completion {
    partition: i32,
    sequence: u64,
    retained_bytes: usize,
    source: SourceRecord,
    outcome: CompletionOutcome,
}

#[derive(Debug)]
enum CompletionOutcome {
    Action(Action),
    Fatal(String),
}

impl<T> BatchSender<T> {
    fn new(sender: Sender<Vec<T>>) -> Self {
        Self {
            sender,
            pending: Vec::with_capacity(BATCH_RECORDS),
            retained_bytes: 0,
        }
    }

    fn push(&mut self, item: T, retained_bytes: usize) -> Result<(), String> {
        self.pending.push(item);
        self.retained_bytes += retained_bytes;
        if self.pending.len() >= BATCH_RECORDS || self.retained_bytes >= BATCH_BYTES {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), String> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.retained_bytes = 0;
        self.sender
            .try_send(std::mem::take(&mut self.pending))
            .map_err(|error| error.to_string())
    }
}

impl Dispatcher {
    fn transform(sender: Sender<Vec<WorkItem>>) -> Self {
        Self::Transform(BatchSender::new(sender))
    }

    fn identity(sender: Sender<Vec<Completion>>) -> Self {
        Self::Identity(BatchSender::new(sender))
    }

    fn push(&mut self, record: OwnedRecord, sequence: u64, stats: &Stats) -> Result<(), String> {
        let record_bytes = record.retained_bytes;
        match self {
            Self::Transform(batch) => batch.push(WorkItem { sequence, record }, record_bytes),
            Self::Identity(batch) => {
                let OwnedRecord {
                    partition,
                    offset,
                    timestamp,
                    key,
                    headers,
                    payload,
                    retained_bytes: record_bytes,
                } = record;
                let source_tombstone = payload.is_none();
                let action = match payload {
                    Some(bytes) => Action::PassThrough(PassPayload::Exact(bytes)),
                    None => Action::Tombstone,
                };
                stats.transformed(&action, None, source_tombstone);
                batch.push(
                    Completion {
                        partition,
                        sequence,
                        retained_bytes: record_bytes,
                        source: SourceRecord {
                            partition,
                            offset,
                            timestamp,
                            key,
                            headers,
                        },
                        outcome: CompletionOutcome::Action(action),
                    },
                    record_bytes,
                )
            }
        }
    }

    fn flush(&mut self) -> Result<(), String> {
        match self {
            Self::Transform(batch) => batch.flush(),
            Self::Identity(batch) => batch.flush(),
        }
    }
}

pub fn run_pipeline(
    config: &RuntimeConfig,
    input: KafkaInput,
    writer: &mut impl Write,
    shutdown: Arc<AtomicBool>,
    received_signal: Arc<AtomicUsize>,
    stats: Arc<Stats>,
) -> Result<Option<i32>, PipelineError> {
    let capacity = config.limits.max_inflight_records;
    let (work_tx, work_rx) = bounded::<Vec<WorkItem>>(capacity);
    let (completion_tx, completion_rx) = bounded::<Vec<Completion>>(capacity);
    let (release_tx, release_rx) = bounded::<Vec<Release>>(capacity);
    let started = Instant::now();
    let first_failure = Arc::new(OnceLock::new());
    let transforms_json = config.transform.capabilities.parses_json;

    let signal = thread::scope(|scope| {
        let poller_shutdown = Arc::clone(&shutdown);
        let poller_stats = Arc::clone(&stats);
        let poller_failure = Arc::clone(&first_failure);
        let dispatcher = if transforms_json {
            Dispatcher::transform(work_tx)
        } else {
            Dispatcher::identity(completion_tx.clone())
        };
        let poller = match thread::Builder::new()
            .name("jkq-kafka-poll".to_owned())
            .spawn_scoped(scope, move || {
                poll_loop(
                    config,
                    input,
                    dispatcher,
                    release_rx,
                    poller_shutdown,
                    received_signal,
                    poller_stats,
                    started,
                    poller_failure,
                )
            }) {
            Ok(poller) => poller,
            Err(error) => {
                thread_start_failure(&first_failure, &shutdown, "Kafka poll thread", error);
                return None;
            }
        };

        let mut workers = Vec::with_capacity(if transforms_json { config.jobs } else { 0 });
        if transforms_json {
            for index in 0..config.jobs {
                let receiver = work_rx.clone();
                let sender = completion_tx.clone();
                let worker_stats = Arc::clone(&stats);
                let worker_shutdown = Arc::clone(&shutdown);
                let worker_failure = Arc::clone(&first_failure);
                match thread::Builder::new()
                    .name(format!("jkq-worker-{index}"))
                    .spawn_scoped(scope, move || {
                        guard_worker(worker_shutdown, worker_failure, || {
                            worker_loop(config, receiver, sender, worker_stats);
                        });
                    }) {
                    Ok(worker) => workers.push(worker),
                    Err(error) => {
                        thread_start_failure(
                            &first_failure,
                            &shutdown,
                            "compute worker thread",
                            error,
                        );
                        break;
                    }
                }
            }
        }
        drop(work_rx);
        drop(completion_tx);

        let writer_result = catch_unwind(AssertUnwindSafe(|| {
            writer_loop(
                config,
                writer,
                completion_rx,
                release_tx,
                Arc::clone(&shutdown),
                Arc::clone(&stats),
                Arc::clone(&first_failure),
            )
        }));
        if writer_result.is_err() {
            let error = PipelineError::Runtime("output writer panicked".to_owned());
            record_failure(&first_failure, &error);
            shutdown.store(true, Ordering::SeqCst);
        }

        for worker in workers {
            if worker.join().is_err() {
                let error = PipelineError::Runtime("compute worker panicked".to_owned());
                record_failure(&first_failure, &error);
                shutdown.store(true, Ordering::SeqCst);
            }
        }
        match poller.join() {
            Ok(result) => result,
            Err(_) => {
                let error = PipelineError::Runtime("Kafka poll thread panicked".to_owned());
                record_failure(&first_failure, &error);
                None
            }
        }
    });

    match Arc::try_unwrap(first_failure) {
        Ok(first_failure) => match first_failure.into_inner() {
            Some(error) => Err(error.into_error()),
            None => Ok(signal),
        },
        Err(_) => Err(PipelineError::Runtime(
            "internal failure state remained shared after shutdown".to_owned(),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn poll_loop(
    config: &RuntimeConfig,
    mut input: KafkaInput,
    dispatcher: Dispatcher,
    release_rx: Receiver<Vec<Release>>,
    shutdown: Arc<AtomicBool>,
    received_signal: Arc<AtomicUsize>,
    stats: Arc<Stats>,
    started: Instant,
    first_failure: Arc<OnceLock<RecordedFailure>>,
) -> Option<i32> {
    let partitions = input.assigned_partitions();
    let mut admission = Admission::new(&partitions, config.limits);
    let mut dispatcher = Some(dispatcher);
    let mut pending: Option<OwnedRecord> = None;
    let mut stopping = false;
    let mut admitted = 0_u64;
    let mut next_stats = config.stats_interval;

    'polling: loop {
        loop {
            match release_rx.try_recv() {
                Ok(releases) => {
                    if let Err(release_error) = release_batch(&mut admission, releases) {
                        runtime_failure(&first_failure, &shutdown, release_error);
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }
        if shutdown.load(Ordering::SeqCst) {
            stopping = true;
        }
        if config.count_limit.is_some_and(|limit| admitted >= limit) {
            stopping = true;
        }
        if (stopping || pending.is_some() || admission.should_wait_for_capacity())
            && let Some(dispatcher) = dispatcher.as_mut()
            && let Err(error) = dispatcher.flush()
        {
            runtime_failure(
                &first_failure,
                &shutdown,
                format!("cannot dispatch admitted record batch: {error}"),
            );
            break 'polling;
        }
        if stopping {
            pending = None;
            dispatcher.take();
        }
        if !stopping && pending.is_none() && admission.should_wait_for_capacity() {
            match release_rx.recv_timeout(CONTROL_POLL) {
                Ok(releases) => {
                    if let Err(release_error) = release_batch(&mut admission, releases) {
                        runtime_failure(&first_failure, &shutdown, release_error);
                    }
                    continue;
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    runtime_failure(
                        &first_failure,
                        &shutdown,
                        "completion release channel closed early".to_owned(),
                    );
                    stopping = true;
                    dispatcher.take();
                }
            }
        }
        if let Err(pause_error) = admission.sync_pauses(&mut input, stopping, pending.is_some()) {
            runtime_failure(&first_failure, &shutdown, pause_error);
            stopping = true;
            dispatcher.take();
        }
        let all_paused = input.all_active_partitions_paused();
        report_periodic(config, &stats, started, &mut next_stats);

        if stopping {
            if admission.total_records == 0 {
                break;
            }
            match release_rx.recv_timeout(CONTROL_POLL) {
                Ok(releases) => {
                    if let Err(release_error) = release_batch(&mut admission, releases) {
                        runtime_failure(&first_failure, &shutdown, release_error);
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if let Err(poll_error) = input.poll() {
                        runtime_failure(&first_failure, &shutdown, poll_error);
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    runtime_failure(
                        &first_failure,
                        &shutdown,
                        "completion release channel closed early".to_owned(),
                    );
                    break;
                }
            }
            continue;
        }

        if let Some(record) = pending.take() {
            let partition = record.partition;
            if admission.can_reserve(record.partition, record.retained_bytes) {
                match admit(
                    record,
                    &mut admission,
                    dispatcher
                        .as_mut()
                        .expect("running poller has a dispatcher"),
                    &stats,
                ) {
                    Err(admit_error) => {
                        runtime_failure(&first_failure, &shutdown, admit_error);
                        break 'polling;
                    }
                    Ok(sequence) => {
                        admitted += 1;
                        if config
                            .count_per_partition
                            .is_some_and(|limit| sequence + 1 >= limit)
                            && let Err(error) = input.finish(partition)
                        {
                            runtime_failure(&first_failure, &shutdown, error);
                        }
                    }
                }
            } else {
                pending = Some(record);
            }
            if pending.is_none() {
                continue;
            }
        }

        if all_paused {
            if let Some(dispatcher) = dispatcher.as_mut()
                && let Err(error) = dispatcher.flush()
            {
                runtime_failure(
                    &first_failure,
                    &shutdown,
                    format!("cannot dispatch admitted record batch: {error}"),
                );
                break 'polling;
            }
            match release_rx.recv_timeout(CONTROL_POLL) {
                Ok(releases) => {
                    if let Err(release_error) = release_batch(&mut admission, releases) {
                        runtime_failure(&first_failure, &shutdown, release_error);
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    match input.poll_nonblocking() {
                        Ok(PollEvent::Record(_)) => {
                            runtime_failure(
                                &first_failure,
                                &shutdown,
                                "Kafka returned a record while all partitions were paused for byte backpressure"
                                    .to_owned(),
                            );
                        }
                        Ok(PollEvent::Done | PollEvent::Idle) => {}
                        Err(poll_error) => {
                            runtime_failure(&first_failure, &shutdown, poll_error);
                        }
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    runtime_failure(
                        &first_failure,
                        &shutdown,
                        "completion release channel closed early".to_owned(),
                    );
                }
            }
            continue;
        }

        match input.poll() {
            Ok(PollEvent::Record(record)) => {
                let partition = record.partition;
                if admission.can_reserve(record.partition, record.retained_bytes) {
                    match admit(
                        record,
                        &mut admission,
                        dispatcher
                            .as_mut()
                            .expect("running poller has a dispatcher"),
                        &stats,
                    ) {
                        Err(admit_error) => {
                            runtime_failure(&first_failure, &shutdown, admit_error);
                            break 'polling;
                        }
                        Ok(sequence) => {
                            admitted += 1;
                            if config
                                .count_per_partition
                                .is_some_and(|limit| sequence + 1 >= limit)
                                && let Err(error) = input.finish(partition)
                            {
                                runtime_failure(&first_failure, &shutdown, error);
                            }
                        }
                    }
                } else {
                    pending = Some(record);
                }
            }
            Ok(PollEvent::Idle) => {
                if let Some(dispatcher) = dispatcher.as_mut()
                    && let Err(error) = dispatcher.flush()
                {
                    runtime_failure(
                        &first_failure,
                        &shutdown,
                        format!("cannot dispatch admitted record batch: {error}"),
                    );
                    break 'polling;
                }
            }
            Ok(PollEvent::Done) => stopping = true,
            Err(poll_error) => {
                runtime_failure(&first_failure, &shutdown, poll_error);
            }
        }
    }

    let signal = received_signal.load(Ordering::SeqCst);
    (signal != 0).then(|| i32::try_from(signal).expect("registered signal fits in i32"))
}

fn release_batch(admission: &mut Admission, releases: Vec<Release>) -> Result<(), String> {
    for release in releases {
        admission.release(release)?;
    }
    Ok(())
}

fn runtime_failure(first: &OnceLock<RecordedFailure>, shutdown: &AtomicBool, message: String) {
    record_failure(first, &PipelineError::Runtime(message));
    shutdown.store(true, Ordering::SeqCst);
}

fn thread_start_failure(
    first: &OnceLock<RecordedFailure>,
    shutdown: &AtomicBool,
    role: &str,
    error: io::Error,
) {
    runtime_failure(first, shutdown, format!("cannot start {role}: {error}"));
}

fn guard_worker(
    shutdown: Arc<AtomicBool>,
    first: Arc<OnceLock<RecordedFailure>>,
    worker: impl FnOnce(),
) {
    if catch_unwind(AssertUnwindSafe(worker)).is_err() {
        let error = PipelineError::Runtime("compute worker panicked".to_owned());
        record_failure(&first, &error);
        shutdown.store(true, Ordering::SeqCst);
    }
}

fn report_periodic(
    config: &RuntimeConfig,
    stats: &Stats,
    started: Instant,
    next: &mut Option<Duration>,
) {
    let Some(deadline) = *next else {
        return;
    };
    let elapsed = started.elapsed();
    if elapsed >= deadline {
        eprintln!("jkq: stats {}", stats.report(elapsed));
        *next = config
            .stats_interval
            .and_then(|interval| elapsed.checked_add(interval));
    }
}

fn admit(
    record: OwnedRecord,
    admission: &mut Admission,
    dispatcher: &mut Dispatcher,
    stats: &Stats,
) -> Result<u64, String> {
    let partition = record.partition;
    let retained_bytes = record.retained_bytes;
    let sequence = admission.reserve(partition, retained_bytes)?;
    stats.admit(&record);
    dispatcher
        .push(record, sequence, stats)
        .map_err(|error| format!("cannot dispatch admitted record batch: {error}"))?;
    Ok(sequence)
}

fn worker_loop(
    config: &RuntimeConfig,
    work_rx: Receiver<Vec<WorkItem>>,
    completion_tx: Sender<Vec<Completion>>,
    stats: Arc<Stats>,
) {
    let worker = jsonata::Worker::new(&config.transform, config.output.embeds_json());
    for work_batch in work_rx {
        let mut completions = Vec::with_capacity(work_batch.len());
        for work in work_batch {
            let WorkItem { sequence, record } = work;
            let OwnedRecord {
                partition,
                offset,
                timestamp,
                key,
                headers,
                payload,
                retained_bytes,
            } = record;
            let source_tombstone = payload.is_none();
            let result = catch_unwind(AssertUnwindSafe(|| {
                worker.execute_report(payload, config.errors)
            }));
            let outcome = match result {
                Ok(Ok(execution)) => {
                    stats.transformed(&execution.action, execution.issue, source_tombstone);
                    CompletionOutcome::Action(execution.action)
                }
                Ok(Err(transform_error)) => {
                    stats.failed(&transform_error);
                    CompletionOutcome::Fatal(format!(
                        "transform failed at {} partition {partition} offset {offset}: {transform_error}",
                        config.topic
                    ))
                }
                Err(_) => CompletionOutcome::Fatal(format!(
                    "compute worker panicked at {} partition {partition} offset {offset}",
                    config.topic
                )),
            };
            completions.push(Completion {
                partition,
                sequence,
                retained_bytes,
                source: SourceRecord {
                    partition,
                    offset,
                    timestamp,
                    key,
                    headers,
                },
                outcome,
            });
        }
        if completion_tx.send(completions).is_err() {
            break;
        }
    }
}

fn writer_loop(
    config: &RuntimeConfig,
    writer: &mut impl Write,
    completion_rx: Receiver<Vec<Completion>>,
    release_tx: Sender<Vec<Release>>,
    shutdown: Arc<AtomicBool>,
    stats: Arc<Stats>,
    first_failure: Arc<OnceLock<RecordedFailure>>,
) -> Result<(), PipelineError> {
    let mut orderer = Orderer::default();
    let mut ready = Vec::new();
    let mut failure = None;
    let ordered = !config.unordered && config.transform.capabilities.parses_json;
    for completion_batch in completion_rx {
        let mut releases = Vec::with_capacity(completion_batch.len());
        for completion in completion_batch {
            ready.clear();
            if ordered {
                if let Err((message, completion)) = orderer.insert(completion, &mut ready) {
                    let completion = *completion;
                    release(&mut releases, &completion);
                    writer_failure(
                        &mut failure,
                        &first_failure,
                        &shutdown,
                        PipelineError::Runtime(message),
                    );
                }
            } else {
                ready.push(completion);
            }
            for completion in ready.drain(..) {
                if failure.is_none() {
                    match completion.outcome {
                        CompletionOutcome::Fatal(ref message) => {
                            writer_failure(
                                &mut failure,
                                &first_failure,
                                &shutdown,
                                PipelineError::Runtime(message.clone()),
                            );
                        }
                        CompletionOutcome::Action(ref action) => {
                            if let Err(error) =
                                write_action(config, writer, &completion.source, action, &stats)
                            {
                                writer_failure(
                                    &mut failure,
                                    &first_failure,
                                    &shutdown,
                                    PipelineError::Output(error),
                                );
                            }
                        }
                    }
                }
                release(&mut releases, &completion);
            }
        }
        if !releases.is_empty() {
            let _ = release_tx.send(releases);
        }
    }

    let pending = orderer.take_pending();
    if !pending.is_empty() {
        let mut releases = Vec::with_capacity(pending.len());
        for completion in &pending {
            release(&mut releases, completion);
        }
        let _ = release_tx.send(releases);
        writer_failure(
            &mut failure,
            &first_failure,
            &shutdown,
            PipelineError::Runtime(
                "completion channel closed with a partition sequence gap".to_owned(),
            ),
        );
    }
    if failure.is_none()
        && let Err(error) = writer.flush()
    {
        writer_failure(
            &mut failure,
            &first_failure,
            &shutdown,
            PipelineError::Output(error),
        );
    }
    failure.map_or(Ok(()), Err)
}

fn writer_failure(
    failure: &mut Option<PipelineError>,
    first: &OnceLock<RecordedFailure>,
    shutdown: &AtomicBool,
    error: PipelineError,
) {
    record_failure(first, &error);
    failure.get_or_insert(error);
    shutdown.store(true, Ordering::SeqCst);
}

fn release(releases: &mut Vec<Release>, completion: &Completion) {
    releases.push(Release {
        partition: completion.partition,
        retained_bytes: completion.retained_bytes,
    });
}

fn write_action(
    config: &RuntimeConfig,
    writer: &mut impl Write,
    source: &SourceRecord,
    action: &Action,
    stats: &Stats,
) -> io::Result<()> {
    let embeds_json = config.output.embeds_json();
    let (payload, emitted_action) = match action {
        Action::Drop => return Ok(()),
        Action::Tombstone => (Payload::Tombstone, EmittedAction::Tombstone),
        Action::PassThrough(PassPayload::Exact(_)) if embeds_json => {
            return Err(io::Error::other(
                "JSON-value envelope received a non-JSON pass-through payload",
            ));
        }
        Action::PassThrough(PassPayload::Exact(bytes)) => {
            (Payload::Bytes(bytes), EmittedAction::PassThrough)
        }
        Action::PassThrough(PassPayload::Json {
            bytes,
            source_length,
        }) if embeds_json => (
            Payload::Json {
                bytes,
                source_length: *source_length,
            },
            EmittedAction::PassThrough,
        ),
        Action::PassThrough(PassPayload::Json { .. }) => {
            return Err(io::Error::other(
                "JSON pass-through payload requires a JSON-value envelope",
            ));
        }
        Action::Project(bytes) if embeds_json => (
            Payload::Json {
                bytes,
                source_length: bytes.len(),
            },
            EmittedAction::Project,
        ),
        Action::Project(bytes) => (Payload::Bytes(bytes), EmittedAction::Project),
    };
    let output_record = OutputRecord {
        topic: &config.topic,
        partition: source.partition,
        offset: source.offset,
        timestamp: source.timestamp,
        key: source.key.as_deref(),
        headers: &source.headers,
        payload,
        action: emitted_action,
    };
    let output_bytes = match &config.output {
        OutputPlan::Format(format) => format.write_to(&output_record, writer)?,
        OutputPlan::Envelope(_) => output::write_envelope(&output_record, writer)?,
    };
    if config.unbuffered {
        writer.flush()?;
    }
    stats.emitted(output_bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;
    use crate::cli::RawCli;

    fn completion(partition: i32, sequence: u64, bytes: usize, action: Action) -> Completion {
        Completion {
            partition,
            sequence,
            retained_bytes: bytes,
            source: SourceRecord {
                partition,
                offset: i64::try_from(sequence).unwrap(),
                timestamp: None,
                key: None,
                headers: Vec::new(),
            },
            outcome: CompletionOutcome::Action(action),
        }
    }

    fn fatal_completion(partition: i32, sequence: u64, bytes: usize) -> Completion {
        let mut completion = completion(partition, sequence, bytes, Action::Drop);
        completion.outcome = CompletionOutcome::Fatal("fatal transform".to_owned());
        completion
    }

    fn pass(bytes: &[u8]) -> Action {
        Action::PassThrough(PassPayload::Exact(bytes.to_vec()))
    }

    fn config(arguments: &[&str]) -> RuntimeConfig {
        let mut base = vec!["jkq", "-b", "unused", "-t", "events", "-p", "0"];
        base.extend_from_slice(arguments);
        RawCli::try_parse_from(base).unwrap().resolve().unwrap()
    }

    #[test]
    fn thread_start_failure_records_the_error_and_requests_shutdown() {
        let first = OnceLock::new();
        let shutdown = AtomicBool::new(false);

        thread_start_failure(
            &first,
            &shutdown,
            "compute worker thread",
            io::Error::other("resource limit"),
        );

        assert!(shutdown.load(Ordering::SeqCst));
        let Some(RecordedFailure::Runtime(message)) = first.get() else {
            panic!("expected a runtime failure");
        };
        assert_eq!(
            message,
            "cannot start compute worker thread: resource limit"
        );
    }

    #[test]
    fn identity_dispatch_completes_without_a_worker() {
        let (completion_tx, completion_rx) = bounded(1);
        let mut dispatcher = Dispatcher::identity(completion_tx);
        let mut admission = Admission::new(
            &[0],
            crate::cli::RuntimeLimits {
                max_inflight_records: 1,
                max_inflight_bytes: 1024,
                max_inflight_per_partition: 1,
            },
        );
        admit(
            OwnedRecord {
                partition: 0,
                offset: 7,
                timestamp: None,
                key: None,
                headers: Vec::new(),
                payload: Some(b"exact".to_vec()),
                retained_bytes: 5,
            },
            &mut admission,
            &mut dispatcher,
            &Stats::default(),
        )
        .unwrap();
        dispatcher.flush().unwrap();

        let batch = completion_rx.recv().unwrap();
        assert_eq!(batch.len(), 1);
        let completion = &batch[0];
        assert_eq!(completion.sequence, 0);
        assert!(matches!(
            completion.outcome,
            CompletionOutcome::Action(Action::PassThrough(PassPayload::Exact(ref bytes)))
                if bytes == b"exact"
        ));
    }

    #[test]
    fn batch_sender_flushes_at_the_record_limit() {
        let (sender, receiver) = bounded(1);
        let mut sender = BatchSender::new(sender);
        for value in 0..BATCH_RECORDS {
            sender.push(value, 0).unwrap();
        }

        assert_eq!(
            receiver.recv().unwrap(),
            (0..BATCH_RECORDS).collect::<Vec<_>>()
        );
    }

    #[test]
    fn disabled_statistics_do_not_touch_counters() {
        let stats = Stats::default();
        stats.emitted(10);
        assert!(stats.report(Duration::ZERO).starts_with("admitted=0 "));
        assert!(stats.report(Duration::ZERO).contains("output_bytes=0 "));
    }

    #[test]
    fn json_value_envelope_embeds_projected_payload() {
        let config = config(&[
            "-J",
            "--envelope-payload",
            "value",
            "--project",
            r#"{"id":id}"#,
        ]);
        let source = SourceRecord {
            partition: 0,
            offset: 7,
            timestamp: None,
            key: None,
            headers: Vec::new(),
        };
        let mut output = Vec::new();

        write_action(
            &config,
            &mut output,
            &source,
            &Action::Project(br#"{"id":1}"#.to_vec()),
            &Stats::default(),
        )
        .unwrap();

        assert!(output.ends_with(
            br#""action":"project","payload":{"id":1},"payloadEncoding":"json","payloadLength":8}
"#
        ));
    }

    #[test]
    fn ordered_writer_drains_reverse_completions_and_releases_drops() {
        let config = config(&["--drop-if", "false", "-f", "%p:%o:%S:%s\\n"]);
        let (completion_tx, completion_rx) = bounded(3);
        let (release_tx, release_rx) = bounded(3);
        completion_tx
            .send(vec![
                completion(0, 2, 3, Action::Tombstone),
                completion(0, 1, 2, Action::Drop),
                completion(0, 0, 1, pass(b"a")),
            ])
            .unwrap();
        drop(completion_tx);

        let mut output = Vec::new();
        writer_loop(
            &config,
            &mut output,
            completion_rx,
            release_tx,
            Arc::new(AtomicBool::new(false)),
            Arc::new(Stats::default()),
            Arc::new(OnceLock::new()),
        )
        .unwrap();

        assert_eq!(output, b"0:0:1:a\n0:2:-1:\n");
        let releases = release_rx.iter().flatten().collect::<Vec<_>>();
        assert_eq!(releases.len(), 3);
        assert_eq!(
            releases
                .iter()
                .map(|release| release.retained_bytes)
                .sum::<usize>(),
            6
        );
    }

    #[test]
    fn ordered_writer_stops_at_first_fatal_result_and_releases_every_completion() {
        let config = config(&["--drop-if", "false"]);
        let (completion_tx, completion_rx) = bounded(3);
        let (release_tx, release_rx) = bounded(3);
        completion_tx
            .send(vec![
                completion(0, 2, 3, pass(b"c")),
                fatal_completion(0, 1, 2),
                completion(0, 0, 1, pass(b"a")),
            ])
            .unwrap();
        drop(completion_tx);

        let shutdown = Arc::new(AtomicBool::new(false));
        let first_failure = Arc::new(OnceLock::new());
        let mut output = Vec::new();
        let error = writer_loop(
            &config,
            &mut output,
            completion_rx,
            release_tx,
            Arc::clone(&shutdown),
            Arc::new(Stats::default()),
            Arc::clone(&first_failure),
        )
        .unwrap_err();

        assert!(
            matches!(error, PipelineError::Runtime(ref message) if message == "fatal transform")
        );
        assert_eq!(output, b"a\n");
        assert!(shutdown.load(Ordering::SeqCst));
        assert!(matches!(
            first_failure.get(),
            Some(RecordedFailure::Runtime(message)) if message == "fatal transform"
        ));
        let releases = release_rx.iter().flatten().collect::<Vec<_>>();
        assert_eq!(releases.len(), 3);
        assert_eq!(
            releases
                .iter()
                .map(|release| release.retained_bytes)
                .sum::<usize>(),
            6
        );
    }

    #[test]
    fn unordered_writer_emits_completion_arrival_order() {
        let config = config(&["--unordered", "-f", "%o\\n"]);
        let (completion_tx, completion_rx) = bounded(2);
        let (release_tx, release_rx) = bounded(2);
        completion_tx
            .send(vec![
                completion(0, 1, 1, pass(b"b")),
                completion(0, 0, 1, pass(b"a")),
            ])
            .unwrap();
        drop(completion_tx);

        let mut output = Vec::new();
        writer_loop(
            &config,
            &mut output,
            completion_rx,
            release_tx,
            Arc::new(AtomicBool::new(false)),
            Arc::new(Stats::default()),
            Arc::new(OnceLock::new()),
        )
        .unwrap();

        assert_eq!(output, b"1\n0\n");
        assert_eq!(release_rx.iter().flatten().count(), 2);
    }

    #[test]
    fn periodic_statistics_accept_the_largest_cli_duration() {
        let config = config(&["--stats-interval", "18446744073709551615ms"]);
        let mut next = config.stats_interval;
        report_periodic(&config, &Stats::default(), Instant::now(), &mut next);
        assert_eq!(next, config.stats_interval);
    }

    #[test]
    fn worker_panic_records_the_first_failure_and_starts_shutdown() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let first = Arc::new(OnceLock::new());
        guard_worker(Arc::clone(&shutdown), Arc::clone(&first), || {
            panic!("worker failure")
        });
        assert!(shutdown.load(Ordering::SeqCst));
        assert!(matches!(
            first.get(),
            Some(RecordedFailure::Runtime(message)) if message == "compute worker panicked"
        ));

        record_failure(&first, &PipelineError::Runtime("later failure".to_owned()));
        assert!(matches!(
            first.get(),
            Some(RecordedFailure::Runtime(message)) if message == "compute worker panicked"
        ));
    }
}
