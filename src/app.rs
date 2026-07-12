use std::{
    fmt,
    io::{self, BufWriter, Write},
};

use crate::{
    cli::{OutputPlan, RuntimeConfig},
    kafka::{KafkaInput, OwnedRecord, PollEvent},
    output::{self, EmittedAction, Header, OutputRecord, Payload},
    transform::json::{self, Action},
};

#[derive(Debug)]
pub enum AppError {
    Runtime(String),
    Output(io::Error),
}

impl AppError {
    pub fn is_broken_pipe(&self) -> bool {
        matches!(self, Self::Output(error) if error.kind() == io::ErrorKind::BrokenPipe)
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(message) => formatter.write_str(message),
            Self::Output(error) => write!(formatter, "output error: {error}"),
        }
    }
}

pub fn run(config: RuntimeConfig) -> Result<(), AppError> {
    let stdout = io::stdout();
    let lock = stdout.lock();
    if config.unbuffered {
        run_with_writer(&config, lock)
    } else {
        run_with_writer(&config, BufWriter::new(lock))
    }
}

fn run_with_writer(config: &RuntimeConfig, mut writer: impl Write) -> Result<(), AppError> {
    let mut input = KafkaInput::prepare(config).map_err(AppError::Runtime)?;
    consume(config, &mut input, &mut writer)
}

fn consume(
    config: &RuntimeConfig,
    input: &mut KafkaInput,
    mut writer: impl Write,
) -> Result<(), AppError> {
    let mut admitted = 0_u64;
    loop {
        if config.count_limit.is_some_and(|limit| admitted >= limit) {
            break;
        }
        match input.poll().map_err(AppError::Runtime)? {
            PollEvent::Record(record) => {
                admitted += 1;
                write_record(config, record, &mut writer)?;
            }
            PollEvent::Idle => {}
            PollEvent::Done => break,
        }
    }
    writer.flush().map_err(AppError::Output)
}

fn write_record(
    config: &RuntimeConfig,
    record: OwnedRecord,
    writer: &mut impl Write,
) -> Result<(), AppError> {
    let action =
        json::execute(&config.transform, record.payload, config.errors).map_err(|error| {
            AppError::Runtime(format!(
                "transform failed at {} partition {} offset {}: {error}",
                config.topic, record.partition, record.offset
            ))
        })?;
    let (payload, emitted_action) = match action {
        Action::Drop => return Ok(()),
        Action::Tombstone => (Payload::Tombstone, EmittedAction::Tombstone),
        Action::PassThrough(ref bytes) => (Payload::Bytes(bytes), EmittedAction::PassThrough),
        Action::Project(ref bytes) => (Payload::Bytes(bytes), EmittedAction::Project),
    };
    let headers = record
        .headers
        .iter()
        .map(|header| Header {
            name: &header.name,
            value: header.value.as_deref(),
        })
        .collect::<Vec<_>>();
    let output_record = OutputRecord {
        topic: &config.topic,
        partition: record.partition,
        offset: record.offset,
        timestamp: record.timestamp,
        key: record.key.as_deref(),
        headers: &headers,
        payload,
        action: emitted_action,
    };
    let bytes = match &config.output {
        OutputPlan::Format(format) => format
            .render(&output_record)
            .map_err(|error| AppError::Runtime(error.to_string()))?,
        OutputPlan::Envelope => output::render_envelope(&output_record),
    };
    writer.write_all(&bytes).map_err(AppError::Output)?;
    if config.unbuffered {
        writer.flush().map_err(AppError::Output)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use clap::Parser;
    use rdkafka::{
        ClientConfig,
        message::{Header as KafkaHeader, OwnedHeaders},
        mocking::MockCluster,
        producer::{BaseProducer, BaseRecord, Producer},
    };

    use super::*;
    use crate::cli::RawCli;

    struct Fixture {
        _cluster: MockCluster<'static, rdkafka::producer::DefaultProducerContext>,
        producer: BaseProducer,
        brokers: String,
        topic: &'static str,
    }

    impl Fixture {
        fn new(topic: &'static str, partitions: i32) -> Self {
            let cluster = MockCluster::new(1).unwrap();
            cluster.create_topic(topic, partitions, 1).unwrap();
            let brokers = cluster.bootstrap_servers();
            let producer = ClientConfig::new()
                .set("bootstrap.servers", &brokers)
                .create()
                .unwrap();
            Self {
                _cluster: cluster,
                producer,
                brokers,
                topic,
            }
        }

        fn produce(
            &self,
            partition: i32,
            payload: Option<&[u8]>,
            key: Option<&[u8]>,
            timestamp: i64,
            headers: Option<OwnedHeaders>,
        ) {
            let mut record = BaseRecord::<[u8], [u8]>::to(self.topic)
                .partition(partition)
                .timestamp(timestamp);
            if let Some(payload) = payload {
                record = record.payload(payload);
            }
            if let Some(key) = key {
                record = record.key(key);
            }
            if let Some(headers) = headers {
                record = record.headers(headers);
            }
            self.producer.send(record).unwrap();
            self.producer.flush(Duration::from_secs(5)).unwrap();
        }

        fn config(&self, arguments: &[&str]) -> RuntimeConfig {
            let mut base = vec!["jkq", "-b", self.brokers.as_str(), "-t", self.topic];
            base.extend_from_slice(arguments);
            RawCli::try_parse_from(base).unwrap().resolve().unwrap()
        }
    }

    #[test]
    fn direct_snapshot_excludes_later_records_and_preserves_owned_metadata() {
        let fixture = Fixture::new("snapshot-owned", 2);
        fixture.produce(
            0,
            Some(br#"{"value":0}"#),
            Some(b"key"),
            100,
            Some(OwnedHeaders::new().insert(KafkaHeader {
                key: "trace",
                value: Some(b"abc" as &[u8]),
            })),
        );
        fixture.produce(1, None, None, 101, None);
        let config = fixture.config(&[
            "-p",
            "0",
            "-p",
            "1",
            "--snapshot",
            "-f",
            "%p:%o:%K:%k:%S:%s:%h\n",
        ]);
        let mut input = KafkaInput::prepare(&config).unwrap();
        fixture.produce(0, Some(br#"{"value":1}"#), None, 102, None);

        let mut output = Vec::new();
        consume(&config, &mut input, &mut output).unwrap();
        let mut lines = output.split(|byte| *byte == b'\n').collect::<Vec<_>>();
        lines.retain(|line| !line.is_empty());
        lines.sort_unstable();
        assert_eq!(
            lines,
            [
                b"0:0:3:key:11:{\"value\":0}:trace=abc".as_slice(),
                b"1:0:-1::-1::".as_slice(),
            ]
        );
    }

    #[test]
    fn absolute_and_relative_offsets_are_exclusive() {
        let fixture = Fixture::new("offset-ranges", 1);
        for (value, timestamp) in [100, 200, 300, 400].into_iter().enumerate() {
            fixture.produce(0, Some(value.to_string().as_bytes()), None, timestamp, None);
        }
        for (arguments, expected) in [
            (
                vec!["-p", "0", "-o", "1", "--end-offset", "3", "-f", "%o\n"],
                b"1\n2\n".as_slice(),
            ),
            (
                vec!["-p", "0", "-o", "-2", "--snapshot", "-f", "%o\n"],
                b"2\n3\n".as_slice(),
            ),
            (
                vec!["-p", "0", "-o", "end", "--snapshot", "-f", "%o\n"],
                b"".as_slice(),
            ),
            (
                vec!["-p", "0", "-o", "s@9999", "--snapshot", "-f", "%o\n"],
                b"".as_slice(),
            ),
            (
                vec!["-p", "0", "-o", "e@9999", "-f", "%o\n"],
                b"0\n1\n2\n3\n".as_slice(),
            ),
        ] {
            let config = fixture.config(&arguments);
            let mut output = Vec::new();
            run_with_writer(&config, &mut output).unwrap();
            assert_eq!(output, expected, "{arguments:?}");
        }
    }

    #[test]
    fn count_counts_drops_and_eof_drains_the_partition() {
        let fixture = Fixture::new("count-eof", 1);
        for value in 0..3 {
            fixture.produce(0, Some(value.to_string().as_bytes()), None, value, None);
        }
        let dropped = fixture.config(&["-p", "0", "-c", "1", "--drop-if", "true", "-f", "%o\n"]);
        let mut output = Vec::new();
        run_with_writer(&dropped, &mut output).unwrap();
        assert!(output.is_empty());

        let eof = fixture.config(&["-p", "0", "-e", "-f", "%o\n"]);
        run_with_writer(&eof, &mut output).unwrap();
        assert_eq!(output, b"0\n1\n2\n");
    }
}
