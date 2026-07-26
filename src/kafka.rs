use std::{collections::BTreeMap, time::Duration};

use rdkafka::{
    ClientConfig, Message,
    consumer::{BaseConsumer, Consumer},
    error::{KafkaError, RDKafkaErrorCode},
    message::{Headers, Timestamp as KafkaTimestamp},
    topic_partition_list::{Offset, TopicPartitionList},
};

use crate::{
    cli::{EndPosition, KafkaErrorPolicy, MAX_ASSIGNED_PARTITIONS, RuntimeConfig, StartPosition},
    output::{Header, OutputRequirements, Timestamp, TimestampType},
};

const METADATA_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_TIMEOUT: Duration = Duration::from_millis(100);

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

#[derive(Clone, Copy, Debug)]
struct PartitionState {
    end_exclusive: Option<i64>,
    done: bool,
}

pub enum PollEvent {
    Record(OwnedRecord),
    Idle,
    Done,
}

pub struct KafkaInput {
    consumer: BaseConsumer,
    topic: String,
    partitions: BTreeMap<i32, PartitionState>,
    remaining_partitions: usize,
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
        let watermarks = watermarks_required(&config.start, config.end.as_ref())
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

        let partition_shards = shard_partitions(&partitions, config.consumers);
        let mut consumers = Vec::with_capacity(partition_shards.len());
        consumers.push(consumer);
        for _ in 1..partition_shards.len() {
            consumers.push(create_consumer(config)?);
        }

        consumers
            .into_iter()
            .zip(partition_shards)
            .map(|(consumer, partitions)| {
                Self::assign(config, consumer, &partitions, &starts, fixed_ends.as_ref())
            })
            .collect()
    }

    fn assign(
        config: &RuntimeConfig,
        consumer: BaseConsumer,
        assigned: &[i32],
        starts: &BTreeMap<i32, Offset>,
        fixed_ends: Option<&BTreeMap<i32, i64>>,
    ) -> Result<Self, String> {
        let mut assignment = TopicPartitionList::with_capacity(assigned.len());
        for partition in assigned {
            assignment
                .add_partition_offset(&config.topic, *partition, starts[partition])
                .map_err(|error| assignment_error(&config.topic, *partition, error))?;
        }
        consumer
            .assign(&assignment)
            .map_err(|error| format!("cannot assign topic {}: {error}", config.topic))?;
        let partitions = assigned
            .iter()
            .map(|partition| {
                let start = starts[partition];
                let end_exclusive = fixed_ends.map(|boundaries| boundaries[partition]);
                let done = match (start, end_exclusive) {
                    (Offset::Offset(start), Some(end)) => start >= end,
                    _ => false,
                };
                (
                    *partition,
                    PartitionState {
                        end_exclusive,
                        done,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let remaining_partitions = partitions.values().filter(|state| !state.done).count();

        let input = Self {
            consumer,
            topic: config.topic.clone(),
            partitions,
            remaining_partitions,
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
            .end_exclusive
            .is_some_and(|end| message.offset() >= end)
        {
            drop(message);
            self.finish(partition)?;
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
            payload: message.payload().map(<[u8]>::to_vec),
            retained_bytes,
        };
        Ok(PollEvent::Record(record))
    }

    fn handle_eof(&mut self, partition: i32) -> Result<(), String> {
        let Some(state) = self.partitions.get(&partition).copied() else {
            return Ok(());
        };
        let should_finish = match state.end_exclusive {
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

fn shard_partitions(partitions: &[i32], consumer_count: usize) -> Vec<Vec<i32>> {
    let shard_count = consumer_count.min(partitions.len());
    let mut shards = vec![Vec::new(); shard_count];
    for (index, partition) in partitions.iter().enumerate() {
        shards[index % shard_count].push(*partition);
    }
    shards
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
    let mut client = ClientConfig::new();
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
    client.create().map_err(client_creation_error)
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
    fn partitions_are_distributed_across_available_consumers() {
        assert_eq!(
            shard_partitions(&[0, 1, 2, 3, 4, 5], 4),
            [vec![0, 4], vec![1, 5], vec![2], vec![3]]
        );
        assert_eq!(shard_partitions(&[0, 1], 4), [vec![0], vec![1]]);
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
