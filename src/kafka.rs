use std::{collections::BTreeMap, time::Duration};

use rdkafka::{
    ClientConfig, Message,
    consumer::{BaseConsumer, Consumer},
    error::KafkaError,
    message::{Headers, Timestamp as KafkaTimestamp},
    topic_partition_list::{Offset, TopicPartitionList},
};

use crate::{
    cli::{EndPosition, KafkaErrorPolicy, RuntimeConfig, StartPosition},
    output::{OutputRequirements, Timestamp, TimestampType},
    transform::compile::PayloadBudget,
};

const METADATA_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub struct OwnedHeader {
    pub name: String,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug)]
pub struct OwnedRecord {
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<Timestamp>,
    pub key: Option<Vec<u8>>,
    pub headers: Vec<OwnedHeader>,
    pub payload: Option<Vec<u8>>,
    pub retained_bytes: usize,
}

#[derive(Clone, Copy, Debug)]
struct PartitionState {
    end_exclusive: Option<i64>,
    done: bool,
    paused: bool,
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
    requirements: OutputRequirements,
    payload_budget: PayloadBudget,
    exit_at_end: bool,
    error_policy: KafkaErrorPolicy,
    quiet: bool,
}

impl KafkaInput {
    pub fn prepare(config: &RuntimeConfig) -> Result<Self, String> {
        let consumer = create_consumer(config)?;
        let watermarks = fetch_watermarks(&consumer, &config.topic, &config.partitions)?;
        let timestamp_starts = match config.start {
            StartPosition::TimestampMillis(timestamp) => Some(offsets_for_timestamp(
                &consumer,
                &config.topic,
                &config.partitions,
                timestamp,
                &watermarks,
            )?),
            _ => None,
        };
        let starts = config
            .partitions
            .iter()
            .map(|partition| {
                let (low, high) = watermarks[partition];
                let start = match config.start {
                    StartPosition::Beginning => low,
                    StartPosition::End => high,
                    StartPosition::Absolute(offset) => offset,
                    StartPosition::RelativeToEnd(distance) => high
                        .saturating_sub(i64::try_from(distance).unwrap_or(i64::MAX))
                        .max(low),
                    StartPosition::TimestampMillis(_) => timestamp_starts
                        .as_ref()
                        .expect("timestamp starts were resolved")[partition],
                };
                (*partition, start)
            })
            .collect::<BTreeMap<_, _>>();

        let mut fixed_ends = match config.end {
            Some(EndPosition::ExclusiveOffset(offset)) => Some(
                config
                    .partitions
                    .iter()
                    .map(|partition| (*partition, offset))
                    .collect(),
            ),
            Some(EndPosition::TimestampMillis(timestamp)) => Some(offsets_for_timestamp(
                &consumer,
                &config.topic,
                &config.partitions,
                timestamp,
                &watermarks,
            )?),
            Some(EndPosition::Snapshot) => None,
            None => None,
        };

        let mut assignment = TopicPartitionList::with_capacity(config.partitions.len());
        for partition in &config.partitions {
            let start = starts[partition];
            assignment
                .add_partition_offset(&config.topic, *partition, Offset::Offset(start))
                .map_err(|error| assignment_error(&config.topic, *partition, error))?;
        }
        consumer
            .assign(&assignment)
            .map_err(|error| format!("cannot assign topic {}: {error}", config.topic))?;
        if config.end == Some(EndPosition::Snapshot) {
            fixed_ends = Some(
                fetch_watermarks(&consumer, &config.topic, &config.partitions)?
                    .into_iter()
                    .map(|(partition, (_, high))| (partition, high))
                    .collect(),
            );
        }
        let partitions = starts
            .into_iter()
            .map(|(partition, start)| {
                let end_exclusive = fixed_ends.as_ref().map(|boundaries| boundaries[&partition]);
                (
                    partition,
                    PartitionState {
                        end_exclusive,
                        done: end_exclusive.is_some_and(|end| start >= end),
                        paused: false,
                    },
                )
            })
            .collect();

        let mut input = Self {
            consumer,
            topic: config.topic.clone(),
            partitions,
            requirements: config.output.requirements(),
            payload_budget: config.transform.payload_budget(),
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
            input.set_paused(partition, true)?;
        }
        Ok(input)
    }

    pub fn poll(&mut self) -> Result<PollEvent, String> {
        if self.partitions.values().all(|state| state.done) {
            return Ok(PollEvent::Done);
        }
        let Some(result) = self.consumer.poll(POLL_TIMEOUT) else {
            return Ok(PollEvent::Idle);
        };
        let message = match result {
            Ok(message) => message,
            Err(KafkaError::PartitionEOF(partition)) => {
                self.handle_eof(partition)?;
                return Ok(PollEvent::Idle);
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

        let retained_bytes = retained_bytes(&message, self.requirements, self.payload_budget)?;
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
                    .map(|header| OwnedHeader {
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

    fn finish(&mut self, partition: i32) -> Result<(), String> {
        if let Some(state) = self.partitions.get_mut(&partition) {
            state.done = true;
        }
        self.set_paused(partition, true)
    }

    pub fn set_paused(&mut self, partition: i32, paused: bool) -> Result<(), String> {
        let Some(state) = self.partitions.get_mut(&partition) else {
            return Err(format!(
                "cannot control unassigned topic {} partition {partition}",
                self.topic
            ));
        };
        let paused = paused || state.done;
        if state.paused == paused {
            return Ok(());
        }
        let mut partitions = TopicPartitionList::new();
        partitions.add_partition(&self.topic, partition);
        if paused {
            self.consumer.pause(&partitions).map_err(|error| {
                format!("cannot pause {} partition {partition}: {error}", self.topic)
            })?;
        } else {
            self.consumer.resume(&partitions).map_err(|error| {
                format!(
                    "cannot resume {} partition {partition}: {error}",
                    self.topic
                )
            })?;
        }
        state.paused = paused;
        Ok(())
    }
}

fn retained_bytes(
    message: &rdkafka::message::BorrowedMessage<'_>,
    requirements: OutputRequirements,
    payload_budget: PayloadBudget,
) -> Result<usize, String> {
    let mut bytes = message
        .payload()
        .map_or(Ok(0), |payload| payload_budget.bytes(payload.len()))?;
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
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("enable.partition.eof", "true");
    client
        .create()
        .map_err(|error| format!("cannot create Kafka consumer: {error}"))
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
    fn timestamp_without_a_matching_record_resolves_to_current_end() {
        assert_eq!(timestamp_offset(Offset::Invalid, 7, "t", 0, 10).unwrap(), 7);
        assert_eq!(timestamp_offset(Offset::End, 7, "t", 0, 10).unwrap(), 7);
        assert_eq!(
            timestamp_offset(Offset::Offset(3), 7, "t", 0, 10).unwrap(),
            3
        );
    }
}
