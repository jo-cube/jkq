use std::{collections::BTreeMap, time::Duration};

use rdkafka::{
    ClientConfig, Message,
    consumer::{BaseConsumer, Consumer},
    error::{KafkaError, RDKafkaErrorCode},
    message::{Headers, Timestamp as KafkaTimestamp},
    topic_partition_list::{Offset, TopicPartitionList},
};

use crate::{
    cli::{
        EndPosition, KafkaErrorPolicy, MAX_ASSIGNED_PARTITIONS, RuntimeConfig, RuntimeLimits,
        StartPosition,
    },
    output::{Header, OutputRequirements, Timestamp, TimestampType},
};

const METADATA_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_TIMEOUT: Duration = Duration::from_millis(100);

pub(crate) const MAX_REUSABLE_PAYLOAD_CAPACITY: usize = 16 * 1024;
const MAX_SPARE_PAYLOAD_BYTES: usize = 1024 * 1024;

pub(crate) struct PayloadBuffers {
    free: Vec<Vec<u8>>,
    capacity: usize,
    max_records: usize,
    max_bytes: usize,
    max_buffer_capacity: usize,
}

impl PayloadBuffers {
    pub(crate) fn new(limits: RuntimeLimits) -> Self {
        Self {
            free: Vec::new(),
            capacity: 0,
            max_records: limits.max_inflight_records,
            max_bytes: limits.max_inflight_bytes.min(MAX_SPARE_PAYLOAD_BYTES),
            max_buffer_capacity: limits.max_inflight_bytes.min(MAX_REUSABLE_PAYLOAD_CAPACITY),
        }
    }

    fn copy(&mut self, source: &[u8]) -> Vec<u8> {
        if source.len() > self.max_buffer_capacity {
            return source.to_vec();
        }
        let mut buffer = self.free.pop().unwrap_or_default();
        self.capacity -= buffer.capacity();
        buffer.extend_from_slice(source);
        buffer
    }

    pub(crate) fn recycle(&mut self, mut buffer: Vec<u8>) {
        let capacity = buffer.capacity();
        if capacity == 0
            || capacity > self.max_buffer_capacity
            || self.free.len() == self.max_records
            || capacity > self.max_bytes - self.capacity
        {
            return;
        }
        buffer.clear();
        self.capacity += capacity;
        self.free.push(buffer);
    }
}

#[derive(Debug)]
pub struct OwnedRecord {
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<Timestamp>,
    pub key: Option<Vec<u8>>,
    pub headers: Vec<Header>,
    pub payload: Option<Vec<u8>>,
    pub retained_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct PartitionRange {
    partition: i32,
    start: Offset,
    end_exclusive: Option<i64>,
}

struct ConsumerAssignment {
    ranges: Vec<PartitionRange>,
    max_inflight_per_partition: usize,
}

#[derive(Clone, Copy, Debug)]
struct PartitionState {
    range: PartitionRange,
    done: bool,
}

pub enum PollEvent {
    Record(OwnedRecord),
    Idle,
    Done,
}

pub struct KafkaInput {
    pub(crate) payload_buffers: PayloadBuffers,
    consumer: BaseConsumer,
    topic: String,
    partitions: BTreeMap<i32, PartitionState>,
    remaining_partitions: usize,
    max_inflight_per_partition: usize,
    requirements: OutputRequirements,
    exit_at_end: bool,
    error_policy: KafkaErrorPolicy,
    quiet: bool,
}

impl KafkaInput {
    pub fn prepare(config: &RuntimeConfig) -> Result<Vec<Self>, String> {
        let consumer = create_consumer(config)?;
        let partitions = match &config.partitions {
            Some(partitions) => partitions.clone(),
            None => fetch_topic_partitions(&consumer, &config.topic)?,
        };
        let watermarks = (config.range_sharding
            || watermarks_required(&config.start, config.end.as_ref()))
        .then(|| fetch_watermarks(&consumer, &config.topic, &partitions))
        .transpose()?;
        let timestamp_starts = match config.start {
            StartPosition::TimestampMillis(timestamp) => Some(offsets_for_timestamp(
                &consumer,
                &config.topic,
                &partitions,
                timestamp,
                watermarks
                    .as_ref()
                    .expect("timestamp starts require watermarks"),
            )?),
            _ => None,
        };
        let starts = partitions
            .iter()
            .map(|partition| {
                let start = match config.start {
                    StartPosition::Beginning => {
                        watermarks.as_ref().map_or(Offset::Beginning, |watermarks| {
                            Offset::Offset(watermarks[partition].0)
                        })
                    }
                    StartPosition::End => Offset::Offset(
                        watermarks.as_ref().expect("end requires watermarks")[partition].1,
                    ),
                    StartPosition::Absolute(offset) => Offset::Offset(offset),
                    StartPosition::RelativeToEnd(distance) => {
                        let (low, high) = watermarks
                            .as_ref()
                            .expect("relative start requires watermarks")[partition];
                        Offset::Offset(
                            high.saturating_sub(i64::try_from(distance).unwrap_or(i64::MAX))
                                .max(low),
                        )
                    }
                    StartPosition::TimestampMillis(_) => Offset::Offset(
                        timestamp_starts
                            .as_ref()
                            .expect("timestamp starts were resolved")[partition],
                    ),
                };
                (*partition, start)
            })
            .collect::<BTreeMap<_, _>>();

        let fixed_ends = match config.end {
            Some(EndPosition::ExclusiveOffset(offset)) => Some(
                partitions
                    .iter()
                    .map(|partition| (*partition, offset))
                    .collect(),
            ),
            Some(EndPosition::TimestampMillis(timestamp)) => Some(offsets_for_timestamp(
                &consumer,
                &config.topic,
                &partitions,
                timestamp,
                watermarks
                    .as_ref()
                    .expect("timestamp ends require watermarks"),
            )?),
            Some(EndPosition::Snapshot) => Some(
                watermarks
                    .as_ref()
                    .expect("snapshot requires watermarks")
                    .iter()
                    .map(|(partition, (_, high))| (*partition, *high))
                    .collect(),
            ),
            None => None,
        };

        let ranges = partitions
            .iter()
            .map(|partition| PartitionRange {
                partition: *partition,
                start: starts[partition],
                end_exclusive: fixed_ends.as_ref().map(|ends| ends[partition]),
            })
            .collect::<Vec<_>>();
        if config.range_sharding {
            for range in &ranges {
                let (low, high) =
                    watermarks.as_ref().expect("sharding needs watermarks")[&range.partition];
                let (Offset::Offset(start), Some(end)) = (range.start, range.end_exclusive) else {
                    return Err("range sharding requires resolved fixed boundaries".to_owned());
                };
                if end > high || (start < end && (start < low || start > high)) {
                    return Err(format!(
                        "cannot shard {} partition {} range [{start}, {end}): outside startup watermarks [{low}, {high})",
                        config.topic, range.partition
                    ));
                }
            }
        }
        let assignments = consumer_assignments(
            &ranges,
            config.consumers,
            config.range_sharding,
            config.limits.max_inflight_per_partition,
        )?;
        let mut consumers = Vec::with_capacity(assignments.len());
        consumers.push(consumer);
        for _ in 1..assignments.len() {
            consumers.push(create_consumer(config)?);
        }
        consumers
            .into_iter()
            .zip(assignments)
            .map(|(consumer, assignment)| Self::assign(config, consumer, assignment))
            .collect()
    }

    fn assign(
        config: &RuntimeConfig,
        consumer: BaseConsumer,
        assignment: ConsumerAssignment,
    ) -> Result<Self, String> {
        let mut partitions = TopicPartitionList::with_capacity(assignment.ranges.len());
        for range in &assignment.ranges {
            partitions
                .add_partition_offset(&config.topic, range.partition, range.start)
                .map_err(|error| assignment_error(&config.topic, range.partition, error))?;
        }
        consumer
            .assign(&partitions)
            .map_err(|error| format!("cannot assign topic {}: {error}", config.topic))?;
        let partitions = assignment
            .ranges
            .into_iter()
            .map(|range| {
                let done = matches!((range.start, range.end_exclusive),
                    (Offset::Offset(start), Some(end)) if start >= end);
                (range.partition, PartitionState { range, done })
            })
            .collect::<BTreeMap<_, _>>();
        let remaining_partitions = partitions.values().filter(|state| !state.done).count();
        let input = Self {
            payload_buffers: PayloadBuffers::new(config.limits),
            consumer,
            topic: config.topic.clone(),
            partitions,
            remaining_partitions,
            max_inflight_per_partition: assignment.max_inflight_per_partition,
            requirements: config.output.requirements(),
            exit_at_end: config.exit_at_end,
            error_policy: config.kafka_error,
            quiet: config.quiet,
        };
        let initially_done = input
            .partitions
            .iter()
            .filter_map(|(partition, state)| state.done.then_some(*partition))
            .collect::<Vec<_>>();
        for partition in initially_done {
            input.pause(partition)?;
        }
        Ok(input)
    }

    pub(crate) fn max_inflight_per_partition(&self) -> usize {
        self.max_inflight_per_partition
    }

    pub(crate) fn assigned_partitions(&self) -> Vec<i32> {
        self.partitions.keys().copied().collect()
    }

    pub fn poll(&mut self) -> Result<PollEvent, String> {
        self.poll_with_timeout(POLL_TIMEOUT)
    }

    fn poll_with_timeout(&mut self, timeout: Duration) -> Result<PollEvent, String> {
        if self.remaining_partitions == 0 {
            return Ok(PollEvent::Done);
        }
        let Some(result) = self.consumer.poll(timeout) else {
            return Ok(PollEvent::Idle);
        };
        let message = match result {
            Ok(message) => message,
            Err(KafkaError::PartitionEOF(partition)) => {
                self.handle_eof(partition)?;
                return Ok(PollEvent::Idle);
            }
            Err(error) if error.rdkafka_error_code() == Some(RDKafkaErrorCode::AutoOffsetReset) => {
                return Err(format!("Kafka offset error: {error}"));
            }
            Err(error @ KafkaError::MessageConsumptionFatal(_)) => {
                return Err(format!("fatal Kafka consumer error: {error}"));
            }
            Err(error) if self.error_policy == KafkaErrorPolicy::Continue => {
                if !self.quiet {
                    eprintln!("jkq: Kafka record error: {error}");
                }
                return Ok(PollEvent::Idle);
            }
            Err(error) => return Err(format!("Kafka record error: {error}")),
        };

        let partition = message.partition();
        let Some(state) = self.partitions.get(&partition).copied() else {
            return Err(format!(
                "received unassigned record for topic {} partition {partition}",
                self.topic
            ));
        };
        if state.done {
            return Ok(PollEvent::Idle);
        }
        if state
            .range
            .end_exclusive
            .is_some_and(|end| message.offset() >= end)
        {
            drop(message);
            self.finish(partition)?;
            return Ok(PollEvent::Idle);
        }

        if matches!(state.range.start, Offset::Offset(start) if message.offset() < start) {
            return Ok(PollEvent::Idle);
        }
        let retained_bytes = retained_bytes(&message, self.requirements)?;
        let record = OwnedRecord {
            partition,
            offset: message.offset(),
            timestamp: self
                .requirements
                .timestamp
                .then(|| timestamp(message.timestamp()))
                .flatten(),
            key: self
                .requirements
                .key
                .then(|| message.key().map(<[u8]>::to_vec))
                .flatten(),
            headers: if self.requirements.headers {
                message
                    .headers()
                    .into_iter()
                    .flat_map(Headers::iter)
                    .map(|header| Header {
                        name: header.key.to_owned(),
                        value: header.value.map(<[u8]>::to_vec),
                    })
                    .collect()
            } else {
                Vec::new()
            },
            payload: message
                .payload()
                .map(|bytes| self.payload_buffers.copy(bytes)),
            retained_bytes,
        };
        Ok(PollEvent::Record(record))
    }

    fn handle_eof(&mut self, partition: i32) -> Result<(), String> {
        let Some(state) = self.partitions.get(&partition).copied() else {
            return Ok(());
        };
        let should_finish = match state.range.end_exclusive {
            Some(end) => {
                self.consumer
                    .fetch_watermarks(&self.topic, partition, METADATA_TIMEOUT)
                    .map_err(|error| watermark_error(&self.topic, partition, error))?
                    .1
                    >= end
            }
            None => self.exit_at_end,
        };
        if should_finish {
            self.finish(partition)?;
        }
        Ok(())
    }

    pub(crate) fn finish(&mut self, partition: i32) -> Result<(), String> {
        let Some(state) = self.partitions.get_mut(&partition) else {
            return Err(format!(
                "cannot finish unassigned topic {} partition {partition}",
                self.topic
            ));
        };
        if state.done {
            return Ok(());
        }
        self.remaining_partitions -= 1;
        state.done = true;
        self.pause(partition)
    }

    fn pause(&self, partition: i32) -> Result<(), String> {
        if !self.partitions.contains_key(&partition) {
            return Err(format!(
                "cannot pause unassigned topic {} partition {partition}",
                self.topic
            ));
        }
        let mut partitions = TopicPartitionList::new();
        partitions.add_partition(&self.topic, partition);
        self.consumer
            .pause(&partitions)
            .map_err(|error| format!("cannot pause {} partition {partition}: {error}", self.topic))
    }
}

fn consumer_assignments(
    ranges: &[PartitionRange],
    consumers: usize,
    range_sharding: bool,
    max_inflight_per_partition: usize,
) -> Result<Vec<ConsumerAssignment>, String> {
    if !range_sharding || consumers <= ranges.len() {
        let mut assignments = (0..consumers.min(ranges.len()))
            .map(|_| ConsumerAssignment {
                ranges: Vec::new(),
                max_inflight_per_partition,
            })
            .collect::<Vec<_>>();
        for (index, range) in ranges.iter().enumerate() {
            let consumer = index % assignments.len();
            assignments[consumer].ranges.push(*range);
        }
        return Ok(assignments);
    }
    let mut assignments = Vec::new();
    for (index, range) in ranges.iter().enumerate() {
        let (Offset::Offset(start), Some(end)) = (range.start, range.end_exclusive) else {
            return Err("range sharding requires resolved fixed boundaries".to_owned());
        };
        let width = end.saturating_sub(start).max(0);
        let requested = consumers / ranges.len() + usize::from(index < consumers % ranges.len());
        let count = requested
            .min(usize::try_from(width).unwrap_or(usize::MAX).max(1))
            .min(max_inflight_per_partition);
        let divisor =
            i64::try_from(count).map_err(|_| "range shard count exceeds i64".to_owned())?;
        let mut next = start;
        for shard in 0..divisor {
            let shard_end = if width == 0 {
                end
            } else {
                next + width / divisor + i64::from(shard < width % divisor)
            };
            assignments.push(ConsumerAssignment {
                ranges: vec![PartitionRange {
                    partition: range.partition,
                    start: Offset::Offset(next),
                    end_exclusive: Some(shard_end),
                }],
                // Static allowances sum to at most the original partition budget.
                max_inflight_per_partition: max_inflight_per_partition / count,
            });
            next = shard_end;
        }
    }
    Ok(assignments)
}

fn retained_bytes(
    message: &rdkafka::message::BorrowedMessage<'_>,
    requirements: OutputRequirements,
) -> Result<usize, String> {
    let mut bytes = message.payload().map_or(0, <[u8]>::len);
    if requirements.key {
        bytes = bytes
            .checked_add(message.key_len())
            .ok_or_else(|| "record retained-byte charge overflowed usize".to_owned())?;
    }
    if requirements.headers
        && let Some(headers) = message.headers()
    {
        for header in headers.iter() {
            bytes = bytes
                .checked_add(header.key.len())
                .and_then(|bytes| bytes.checked_add(header.value.map_or(0, <[u8]>::len)))
                .ok_or_else(|| "record retained-byte charge overflowed usize".to_owned())?;
        }
    }
    Ok(bytes)
}

fn create_consumer(config: &RuntimeConfig) -> Result<BaseConsumer, String> {
    consumer_config(config)
        .create()
        .map_err(client_creation_error)
}

fn consumer_config(config: &RuntimeConfig) -> ClientConfig {
    let mut client = ClientConfig::new();
    client.set("fetch.queue.backoff.ms", "100");
    for (key, value) in &config.kafka_properties {
        client.set(key, value);
    }
    if !config.kafka_properties.contains_key("group.id") {
        client.set("group.id", "jkq");
    }
    client
        .set("auto.offset.reset", "error")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("enable.partition.eof", "true");
    client
}

fn client_creation_error(error: KafkaError) -> String {
    match error {
        KafkaError::ClientConfig(_, description, key, _) => {
            format!("cannot create Kafka consumer: client configuration {key:?}: {description}")
        }
        error => format!("cannot create Kafka consumer: {error}"),
    }
}

fn fetch_watermarks(
    consumer: &BaseConsumer,
    topic: &str,
    partitions: &[i32],
) -> Result<BTreeMap<i32, (i64, i64)>, String> {
    partitions
        .iter()
        .map(|partition| {
            consumer
                .fetch_watermarks(topic, *partition, METADATA_TIMEOUT)
                .map(|watermarks| (*partition, watermarks))
                .map_err(|error| watermark_error(topic, *partition, error))
        })
        .collect()
}

fn watermarks_required(start: &StartPosition, end: Option<&EndPosition>) -> bool {
    match start {
        StartPosition::Beginning => end.is_some(),
        StartPosition::End
        | StartPosition::RelativeToEnd(_)
        | StartPosition::TimestampMillis(_) => true,
        StartPosition::Absolute(_) => {
            matches!(
                end,
                Some(EndPosition::TimestampMillis(_) | EndPosition::Snapshot)
            )
        }
    }
}

fn fetch_topic_partitions(consumer: &BaseConsumer, topic: &str) -> Result<Vec<i32>, String> {
    let metadata = consumer
        .fetch_metadata(Some(topic), METADATA_TIMEOUT)
        .map_err(|error| format!("cannot fetch metadata for topic {topic}: {error}"))?;
    let metadata = metadata
        .topics()
        .iter()
        .find(|metadata| metadata.name() == topic)
        .ok_or_else(|| format!("metadata response did not include topic {topic}"))?;
    if let Some(error) = metadata.error() {
        let error = RDKafkaErrorCode::from(error);
        return Err(format!("cannot fetch metadata for topic {topic}: {error}"));
    }
    if metadata.partitions().is_empty() {
        return Err(format!("topic {topic} has no partitions"));
    }
    if metadata.partitions().len() > MAX_ASSIGNED_PARTITIONS {
        return Err(format!(
            "topic {topic} has more than the {MAX_ASSIGNED_PARTITIONS} partition limit"
        ));
    }
    Ok(metadata
        .partitions()
        .iter()
        .map(|partition| partition.id())
        .collect())
}

fn offsets_for_timestamp(
    consumer: &BaseConsumer,
    topic: &str,
    partitions: &[i32],
    timestamp: i64,
    watermarks: &BTreeMap<i32, (i64, i64)>,
) -> Result<BTreeMap<i32, i64>, String> {
    let mut request = TopicPartitionList::with_capacity(partitions.len());
    for partition in partitions {
        request
            .add_partition_offset(topic, *partition, Offset::Offset(timestamp))
            .map_err(|error| assignment_error(topic, *partition, error))?;
    }
    let resolved = consumer
        .offsets_for_times(request, METADATA_TIMEOUT)
        .map_err(|error| {
            format!("cannot resolve timestamp {timestamp} for topic {topic}: {error}")
        })?;
    resolved
        .elements_for_topic(topic)
        .into_iter()
        .map(|element| {
            let partition = element.partition();
            element.error().map_err(|error| {
                format!("cannot resolve timestamp {timestamp} for topic {topic} partition {partition}: {error}")
            })?;
            let offset = timestamp_offset(
                element.offset(),
                watermarks[&partition].1,
                topic,
                partition,
                timestamp,
            )?;
            Ok((partition, offset))
        })
        .collect()
}

fn timestamp_offset(
    offset: Offset,
    high: i64,
    topic: &str,
    partition: i32,
    timestamp: i64,
) -> Result<i64, String> {
    match offset {
        Offset::Offset(offset) => Ok(offset),
        Offset::Invalid | Offset::End => Ok(high),
        other => Err(format!(
            "timestamp {timestamp} for topic {topic} partition {partition} resolved to unexpected offset {other:?}"
        )),
    }
}

fn timestamp(timestamp: KafkaTimestamp) -> Option<Timestamp> {
    match timestamp {
        KafkaTimestamp::NotAvailable => None,
        KafkaTimestamp::CreateTime(milliseconds) => Some(Timestamp {
            milliseconds,
            kind: TimestampType::CreateTime,
        }),
        KafkaTimestamp::LogAppendTime(milliseconds) => Some(Timestamp {
            milliseconds,
            kind: TimestampType::LogAppendTime,
        }),
    }
}

fn assignment_error(topic: &str, partition: i32, error: KafkaError) -> String {
    format!("cannot set offset for topic {topic} partition {partition}: {error}")
}

fn watermark_error(topic: &str, partition: i32, error: KafkaError) -> String {
    format!("cannot fetch watermarks for topic {topic} partition {partition}: {error}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetch_queue_backoff_default_preserves_explicit_overrides() {
        use clap::Parser;

        for (property, expected) in [
            (None, "100"),
            (Some("fetch.queue.backoff.ms=0"), "0"),
            (Some("fetch.queue.backoff.ms=10"), "10"),
            (Some("fetch.queue.backoff.ms=1000"), "1000"),
        ] {
            let mut arguments = vec!["jkq", "-b", "localhost:9092", "-t", "events"];
            if let Some(property) = property {
                arguments.extend(["-X", property]);
            }
            let config = crate::cli::RawCli::try_parse_from(arguments)
                .unwrap()
                .resolve()
                .unwrap();
            assert_eq!(
                consumer_config(&config).get("fetch.queue.backoff.ms"),
                Some(expected)
            );
        }
    }

    fn range(partition: i32, start: i64, end: i64) -> PartitionRange {
        PartitionRange {
            partition,
            start: Offset::Offset(start),
            end_exclusive: Some(end),
        }
    }

    #[test]
    fn partitions_are_distributed_across_available_consumers() {
        let ranges = [range(0, 0, 100), range(1, 0, 100), range(2, 0, 100)];
        for (count, expected) in [
            (1, vec![vec![0, 1, 2]]),
            (2, vec![vec![0, 2], vec![1]]),
            (8, vec![vec![0], vec![1], vec![2]]),
        ] {
            let assignments = consumer_assignments(&ranges, count, false, 8).unwrap();
            assert_eq!(
                assignments
                    .iter()
                    .map(|a| a.ranges.iter().map(|r| r.partition).collect::<Vec<_>>())
                    .collect::<Vec<_>>(),
                expected
            );
            assert!(
                assignments
                    .iter()
                    .all(|a| a.max_inflight_per_partition == 8)
            );
        }
    }

    #[test]
    fn range_shards_are_disjoint_half_open_and_share_the_partition_budget() {
        for (start, end, consumers, limit) in [
            (10, 21, 4, 10),
            (0, 2, 8, 8),
            (100, 100, 8, 8),
            (100, 99, 8, 8),
            (i64::MAX - 10, i64::MAX, 8, 8),
            (0, 100, 8, 2),
        ] {
            let assignments =
                consumer_assignments(&[range(3, start, end)], consumers, true, limit).unwrap();
            let mut next = start;
            for assignment in &assignments {
                let shard = assignment.ranges[0];
                assert_eq!(shard.partition, 3);
                assert_eq!(shard.start, Offset::Offset(next));
                next = shard.end_exclusive.unwrap();
                assert!(assignment.max_inflight_per_partition > 0);
            }
            assert_eq!(next, end);
            assert!(
                assignments
                    .iter()
                    .map(|a| a.max_inflight_per_partition)
                    .sum::<usize>()
                    <= limit
            );
            for offset in [start, start.saturating_add(1), end.saturating_sub(1), end] {
                let owners = assignments.iter().filter(|a| matches!(a.ranges[0].start, Offset::Offset(s) if offset >= s && offset < a.ranges[0].end_exclusive.unwrap())).count();
                assert_eq!(owners, usize::from(offset >= start && offset < end));
            }
        }
        let assignments =
            consumer_assignments(&[range(0, 0, 100), range(1, 10, 50)], 5, true, 8).unwrap();
        assert_eq!(assignments.len(), 5);
        for (partition, expected) in [(0, 3), (1, 2)] {
            assert_eq!(
                assignments
                    .iter()
                    .filter(|a| a.ranges[0].partition == partition)
                    .count(),
                expected
            );
        }
    }

    #[test]
    fn recycled_payloads_keep_storage_and_replace_every_source_byte() {
        let mut buffers = PayloadBuffers::new(RuntimeLimits {
            max_inflight_records: 2,
            max_inflight_bytes: 64,
            max_inflight_per_partition: 2,
        });
        let mut original = Vec::with_capacity(32);
        original.extend_from_slice(b"previous contents");
        let storage = original.as_ptr();
        buffers.recycle(original);
        let copied = buffers.copy(b"new");
        assert_eq!(copied, b"new");
        assert_eq!(copied.as_ptr(), storage);
        assert_eq!(copied.capacity(), 32);
        assert_eq!(buffers.capacity, 0);
        buffers.recycle(copied);
        assert!(buffers.copy(b"").is_empty());
    }

    #[test]
    fn spare_payloads_obey_record_capacity_and_byte_limits() {
        let limits = RuntimeLimits {
            max_inflight_records: 2,
            max_inflight_bytes: 64,
            max_inflight_per_partition: 2,
        };
        let mut buffers = PayloadBuffers::new(limits);
        for _ in 0..3 {
            buffers.recycle(Vec::with_capacity(1));
        }
        assert_eq!(buffers.free.len(), 2);
        assert_eq!(buffers.capacity, 2);

        let mut buffers = PayloadBuffers::new(limits);
        buffers.recycle(Vec::with_capacity(32));
        buffers.recycle(Vec::with_capacity(32));
        buffers.recycle(Vec::with_capacity(1));
        assert_eq!(buffers.free.len(), 2);
        assert_eq!(buffers.capacity, 64);

        let mut buffers = PayloadBuffers::new(RuntimeLimits {
            max_inflight_records: 10000,
            max_inflight_bytes: 256 * 1024 * 1024,
            max_inflight_per_partition: 10000,
        });
        for _ in 0..10000 {
            buffers.recycle(Vec::with_capacity(1024));
        }
        assert_eq!(buffers.capacity, MAX_SPARE_PAYLOAD_BYTES);
        let retained = buffers.free.len();
        buffers.recycle(Vec::with_capacity(MAX_REUSABLE_PAYLOAD_CAPACITY + 1));
        buffers.recycle(Vec::new());
        assert_eq!(buffers.free.len(), retained);

        let large = vec![b'x'; MAX_REUSABLE_PAYLOAD_CAPACITY + 1];
        assert_eq!(buffers.copy(&large), large);
        assert_eq!(buffers.free.len(), retained);
        let mut buffers = PayloadBuffers::new(limits);
        buffers.recycle(Vec::with_capacity(65));
        assert!(buffers.free.is_empty());
    }

    #[test]
    fn timestamp_without_a_matching_record_resolves_to_current_end() {
        assert_eq!(timestamp_offset(Offset::Invalid, 7, "t", 0, 10).unwrap(), 7);
        assert_eq!(timestamp_offset(Offset::End, 7, "t", 0, 10).unwrap(), 7);
        assert_eq!(
            timestamp_offset(Offset::Offset(3), 7, "t", 0, 10).unwrap(),
            3
        );
    }

    #[test]
    fn only_ranges_that_need_watermarks_request_them() {
        for (start, end, expected) in [
            (StartPosition::Beginning, None, false),
            (StartPosition::Absolute(3), None, false),
            (
                StartPosition::Absolute(3),
                Some(EndPosition::ExclusiveOffset(7)),
                false,
            ),
            (
                StartPosition::Beginning,
                Some(EndPosition::ExclusiveOffset(7)),
                true,
            ),
            (StartPosition::End, None, true),
            (StartPosition::RelativeToEnd(3), None, true),
            (StartPosition::TimestampMillis(3), None, true),
            (
                StartPosition::Absolute(3),
                Some(EndPosition::TimestampMillis(7)),
                true,
            ),
            (
                StartPosition::Absolute(3),
                Some(EndPosition::Snapshot),
                true,
            ),
        ] {
            assert_eq!(
                watermarks_required(&start, end.as_ref()),
                expected,
                "{start:?} {end:?}"
            );
        }
    }

    #[test]
    fn timestamp_lookup_rejects_partition_errors() {
        use rdkafka::mocking::MockCluster;

        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("timestamp-errors", 1, 1).unwrap();
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", cluster.bootstrap_servers())
            .create()
            .unwrap();
        consumer
            .fetch_metadata(Some("timestamp-errors"), METADATA_TIMEOUT)
            .unwrap();

        // A mixed result can succeed overall while the missing partition fails.
        let error = offsets_for_timestamp(
            &consumer,
            "timestamp-errors",
            &[0, 1],
            10,
            &BTreeMap::from([(0, (0, 7)), (1, (0, 7))]),
        )
        .unwrap_err();
        assert!(
            error
                .starts_with("cannot resolve timestamp 10 for topic timestamp-errors partition 1:"),
            "{error}"
        );
    }

    #[test]
    fn client_configuration_errors_do_not_expose_values() {
        let error = match ClientConfig::new()
            .set("secret.password", "do-not-print")
            .create::<BaseConsumer>()
        {
            Ok(_) => panic!("invalid property unexpectedly created a consumer"),
            Err(error) => error,
        };
        let message = client_creation_error(error);
        assert!(message.contains("secret.password"));
        assert!(!message.contains("do-not-print"));
    }
}
