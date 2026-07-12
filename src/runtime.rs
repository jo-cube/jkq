use std::{
    fmt,
    io::{self, Write},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use crossbeam_channel::{Receiver, Sender, TryRecvError, bounded};
use signal_hook::{
    SigId,
    consts::{SIGINT, SIGTERM},
    flag,
    iterator::Signals,
    low_level,
};

use crate::{
    cli::{OutputPlan, RuntimeConfig},
    kafka::{KafkaInput, OwnedHeader, OwnedRecord, PollEvent},
    output::{self, EmittedAction, Header, OutputRecord, Payload, Timestamp},
    transform::json::{self, Action, ExecutionIssue, TransformError},
};

mod state;

use state::{Admission, Orderer, Release};

const CONTROL_POLL: Duration = Duration::from_millis(10);

#[derive(Debug)]
pub enum PipelineError {
    Runtime(String),
    Output(io::Error),
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
    signals: Signals,
    registrations: Vec<SigId>,
}

impl SignalControl {
    pub fn install() -> Result<Self, String> {
        let armed = Arc::new(AtomicBool::new(false));
        let mut registrations = Vec::new();
        for (signal, status) in [(SIGINT, 130), (SIGTERM, 143)] {
            registrations.push(
                flag::register_conditional_shutdown(signal, status, Arc::clone(&armed))
                    .map_err(|error| format!("cannot install signal handler: {error}"))?,
            );
        }
        let signals = Signals::new([SIGINT, SIGTERM])
            .map_err(|error| format!("cannot install signal handler: {error}"))?;
        for signal in [SIGINT, SIGTERM] {
            registrations.push(
                flag::register(signal, Arc::clone(&armed))
                    .map_err(|error| format!("cannot install signal handler: {error}"))?,
            );
        }
        Ok(Self {
            armed,
            signals,
            registrations,
        })
    }

    pub fn parts(&mut self) -> (Arc<AtomicBool>, &mut Signals) {
        (Arc::clone(&self.armed), &mut self.signals)
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
        match error {
            TransformError::InvalidJson(_) => {
                self.invalid_json.fetch_add(1, Ordering::Relaxed);
            }
            TransformError::Evaluation { .. } => {
                self.evaluation_failures.fetch_add(1, Ordering::Relaxed);
            }
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

#[derive(Debug)]
struct SourceRecord {
    partition: i32,
    offset: i64,
    timestamp: Option<Timestamp>,
    key: Option<Vec<u8>>,
    headers: Vec<OwnedHeader>,
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

pub fn run_pipeline(
    config: &RuntimeConfig,
    input: KafkaInput,
    writer: &mut impl Write,
    shutdown: Arc<AtomicBool>,
    signals: &mut Signals,
    stats: Arc<Stats>,
) -> Result<Option<i32>, PipelineError> {
    let capacity = config.limits.max_inflight_records;
    let (work_tx, work_rx) = bounded::<WorkItem>(capacity);
    let (completion_tx, completion_rx) = bounded::<Completion>(capacity);
    let (release_tx, release_rx) = bounded::<Release>(capacity);
    let started = Instant::now();

    thread::scope(|scope| {
        let poller_shutdown = Arc::clone(&shutdown);
        let poller_stats = Arc::clone(&stats);
        let poller = scope.spawn(move || {
            poll_loop(
                config,
                input,
                work_tx,
                release_rx,
                poller_shutdown,
                signals,
                poller_stats,
                started,
            )
        });

        let mut workers = Vec::with_capacity(config.jobs);
        for _ in 0..config.jobs {
            let receiver = work_rx.clone();
            let sender = completion_tx.clone();
            let worker_stats = Arc::clone(&stats);
            workers.push(scope.spawn(move || worker_loop(config, receiver, sender, worker_stats)));
        }
        drop(work_rx);
        drop(completion_tx);

        let writer_result = writer_loop(
            config,
            writer,
            completion_rx,
            release_tx,
            Arc::clone(&shutdown),
            Arc::clone(&stats),
        );

        let mut join_error = None;
        for worker in workers {
            if worker.join().is_err() && join_error.is_none() {
                join_error = Some("compute worker panicked".to_owned());
                shutdown.store(true, Ordering::SeqCst);
            }
        }
        let poll_result = poller
            .join()
            .map_err(|_| PipelineError::Runtime("Kafka poll thread panicked".to_owned()))?;

        writer_result?;
        if let Some(error) = join_error.or(poll_result.error) {
            return Err(PipelineError::Runtime(error));
        }
        Ok(poll_result.signal)
    })
}

struct PollResult {
    error: Option<String>,
    signal: Option<i32>,
}

#[allow(clippy::too_many_arguments)]
fn poll_loop(
    config: &RuntimeConfig,
    mut input: KafkaInput,
    work_tx: Sender<WorkItem>,
    release_rx: Receiver<Release>,
    shutdown: Arc<AtomicBool>,
    signals: &mut Signals,
    stats: Arc<Stats>,
    started: Instant,
) -> PollResult {
    let mut admission = Admission::new(&config.partitions, config.limits);
    let mut work_tx = Some(work_tx);
    let mut pending: Option<OwnedRecord> = None;
    let mut stopping = false;
    let mut error = None;
    let mut signal = None;
    let mut admitted = 0_u64;
    let mut next_stats = config.stats_interval;

    loop {
        loop {
            match release_rx.try_recv() {
                Ok(release) => {
                    if let Err(release_error) = admission.release(release) {
                        error.get_or_insert(release_error);
                        shutdown.store(true, Ordering::SeqCst);
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }
        for received in signals.pending() {
            signal.get_or_insert(received);
        }
        if shutdown.load(Ordering::SeqCst) {
            stopping = true;
        }
        if config.count_limit.is_some_and(|limit| admitted >= limit) {
            stopping = true;
        }
        if stopping {
            pending = None;
            work_tx.take();
        }
        if let Err(pause_error) = admission.sync_pauses(&mut input, stopping, pending.is_some()) {
            error.get_or_insert(pause_error);
            shutdown.store(true, Ordering::SeqCst);
            stopping = true;
            work_tx.take();
        }
        report_periodic(config, &stats, started, &mut next_stats);

        if stopping {
            if admission.total_records == 0 {
                break;
            }
            match release_rx.recv_timeout(CONTROL_POLL) {
                Ok(release) => {
                    if let Err(release_error) = admission.release(release) {
                        error.get_or_insert(release_error);
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if let Err(poll_error) = input.poll() {
                        error.get_or_insert(poll_error);
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    error.get_or_insert("completion release channel closed early".to_owned());
                    break;
                }
            }
            continue;
        }

        if let Some(record) = pending.take() {
            if admission.can_reserve(record.partition, record.retained_bytes) {
                if let Err(admit_error) = admit(
                    record,
                    &mut admission,
                    work_tx.as_ref().expect("running poller has work sender"),
                    &stats,
                ) {
                    error = Some(admit_error);
                    shutdown.store(true, Ordering::SeqCst);
                } else {
                    admitted += 1;
                }
            } else {
                pending = Some(record);
                match input.poll() {
                    Ok(PollEvent::Record(_)) => {
                        error = Some(
                            "Kafka returned a record while all partitions were paused for byte backpressure"
                                .to_owned(),
                        );
                        shutdown.store(true, Ordering::SeqCst);
                    }
                    Ok(PollEvent::Done | PollEvent::Idle) => {}
                    Err(poll_error) => {
                        error = Some(poll_error);
                        shutdown.store(true, Ordering::SeqCst);
                    }
                }
            }
            continue;
        }

        match input.poll() {
            Ok(PollEvent::Record(record)) => {
                if admission.can_reserve(record.partition, record.retained_bytes) {
                    if let Err(admit_error) = admit(
                        record,
                        &mut admission,
                        work_tx.as_ref().expect("running poller has work sender"),
                        &stats,
                    ) {
                        error = Some(admit_error);
                        shutdown.store(true, Ordering::SeqCst);
                    } else {
                        admitted += 1;
                    }
                } else {
                    pending = Some(record);
                }
            }
            Ok(PollEvent::Idle) => {}
            Ok(PollEvent::Done) => stopping = true,
            Err(poll_error) => {
                error = Some(poll_error);
                shutdown.store(true, Ordering::SeqCst);
            }
        }
    }

    PollResult { error, signal }
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
    work_tx: &Sender<WorkItem>,
    stats: &Stats,
) -> Result<(), String> {
    let partition = record.partition;
    let retained_bytes = record.retained_bytes;
    let sequence = admission.reserve(partition, retained_bytes)?;
    stats.admit(&record);
    if let Err(send_error) = work_tx.try_send(WorkItem { sequence, record }) {
        admission.release(Release {
            partition,
            retained_bytes,
        })?;
        return Err(format!("cannot dispatch admitted record: {send_error}"));
    }
    Ok(())
}

fn worker_loop(
    config: &RuntimeConfig,
    work_rx: Receiver<WorkItem>,
    completion_tx: Sender<Completion>,
    stats: Arc<Stats>,
) {
    for work in work_rx {
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
            json::execute_report(&config.transform, payload, config.errors)
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
        if completion_tx
            .send(Completion {
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
            })
            .is_err()
        {
            break;
        }
    }
}

fn writer_loop(
    config: &RuntimeConfig,
    writer: &mut impl Write,
    completion_rx: Receiver<Completion>,
    release_tx: Sender<Release>,
    shutdown: Arc<AtomicBool>,
    stats: Arc<Stats>,
) -> Result<(), PipelineError> {
    let mut orderer = Orderer::default();
    let mut failure = None;
    for completion in completion_rx {
        let ready = if config.unordered {
            vec![completion]
        } else {
            match orderer.insert(completion) {
                Ok(ready) => ready,
                Err((message, completion)) => {
                    let completion = *completion;
                    release(&release_tx, &completion);
                    failure.get_or_insert(PipelineError::Runtime(message));
                    shutdown.store(true, Ordering::SeqCst);
                    Vec::new()
                }
            }
        };
        for completion in ready {
            if failure.is_none() {
                match completion.outcome {
                    CompletionOutcome::Fatal(ref message) => {
                        failure = Some(PipelineError::Runtime(message.clone()));
                        shutdown.store(true, Ordering::SeqCst);
                    }
                    CompletionOutcome::Action(ref action) => {
                        if let Err(error) =
                            write_action(config, writer, &completion.source, action, &stats)
                        {
                            failure = Some(PipelineError::Output(error));
                            shutdown.store(true, Ordering::SeqCst);
                        }
                    }
                }
            }
            release(&release_tx, &completion);
        }
    }

    let pending = orderer.take_pending();
    if !pending.is_empty() {
        for completion in &pending {
            release(&release_tx, completion);
        }
        failure.get_or_insert(PipelineError::Runtime(
            "completion channel closed with a partition sequence gap".to_owned(),
        ));
    }
    if failure.is_none() {
        writer.flush().map_err(PipelineError::Output)?;
    }
    failure.map_or(Ok(()), Err)
}

fn release(sender: &Sender<Release>, completion: &Completion) {
    let _ = sender.send(Release {
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
    let (payload, emitted_action) = match action {
        Action::Drop => return Ok(()),
        Action::Tombstone => (Payload::Tombstone, EmittedAction::Tombstone),
        Action::PassThrough(bytes) => (Payload::Bytes(bytes), EmittedAction::PassThrough),
        Action::Project(bytes) => (Payload::Bytes(bytes), EmittedAction::Project),
    };
    let headers = source
        .headers
        .iter()
        .map(|header| Header {
            name: &header.name,
            value: header.value.as_deref(),
        })
        .collect::<Vec<_>>();
    let output_record = OutputRecord {
        topic: &config.topic,
        partition: source.partition,
        offset: source.offset,
        timestamp: source.timestamp,
        key: source.key.as_deref(),
        headers: &headers,
        payload,
        action: emitted_action,
    };
    let output_bytes = match &config.output {
        OutputPlan::Format(format) => format.write_to(&output_record, writer)?,
        OutputPlan::Envelope => output::write_envelope(&output_record, writer)?,
    };
    if config.unbuffered {
        writer.flush()?;
    }
    stats.output_records.fetch_add(1, Ordering::Relaxed);
    add(&stats.output_bytes, output_bytes);
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

    fn config(arguments: &[&str]) -> RuntimeConfig {
        let mut base = vec!["jkq", "-b", "unused", "-t", "events", "-p", "0"];
        base.extend_from_slice(arguments);
        RawCli::try_parse_from(base).unwrap().resolve().unwrap()
    }

    #[test]
    fn ordered_writer_drains_reverse_completions_and_releases_drops() {
        let config = config(&["-f", "%p:%o:%S:%s\\n"]);
        let (completion_tx, completion_rx) = bounded(3);
        let (release_tx, release_rx) = bounded(3);
        completion_tx
            .send(completion(0, 2, 3, Action::Tombstone))
            .unwrap();
        completion_tx
            .send(completion(0, 1, 2, Action::Drop))
            .unwrap();
        completion_tx
            .send(completion(0, 0, 1, Action::PassThrough(b"a".to_vec())))
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
        )
        .unwrap();

        assert_eq!(output, b"0:0:1:a\n0:2:-1:\n");
        let releases = release_rx.iter().collect::<Vec<_>>();
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
            .send(completion(0, 1, 1, Action::PassThrough(b"b".to_vec())))
            .unwrap();
        completion_tx
            .send(completion(0, 0, 1, Action::PassThrough(b"a".to_vec())))
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
        )
        .unwrap();

        assert_eq!(output, b"1\n0\n");
        assert_eq!(release_rx.iter().count(), 2);
    }

    #[test]
    fn periodic_statistics_accept_the_largest_cli_duration() {
        let config = config(&["--stats-interval", "18446744073709551615ms"]);
        let mut next = config.stats_interval;
        report_periodic(&config, &Stats::default(), Instant::now(), &mut next);
        assert_eq!(next, config.stats_interval);
    }
}
