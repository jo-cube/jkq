use std::{
    fmt,
    io::{self, BufWriter, Write},
    sync::Arc,
    time::Instant,
};

use crate::{
    cli::RuntimeConfig,
    kafka::KafkaInput,
    runtime::{self, PipelineError, SignalControl, Stats},
};

#[derive(Debug)]
pub enum AppError {
    Runtime(String),
    Output(io::Error),
    Interrupted(i32),
}

impl AppError {
    pub fn is_broken_pipe(&self) -> bool {
        matches!(self, Self::Output(error) if error.kind() == io::ErrorKind::BrokenPipe)
    }

    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Interrupted(signal_hook::consts::SIGINT) => 130,
            Self::Interrupted(signal_hook::consts::SIGTERM) => 143,
            _ => 1,
        }
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(message) => formatter.write_str(message),
            Self::Output(error) => write!(formatter, "output error: {error}"),
            Self::Interrupted(signal) => write!(formatter, "interrupted by signal {signal}"),
        }
    }
}

impl From<PipelineError> for AppError {
    fn from(error: PipelineError) -> Self {
        match error {
            PipelineError::Runtime(message) => Self::Runtime(message),
            PipelineError::Output(error) => Self::Output(error),
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
    let mut signals = SignalControl::install().map_err(AppError::Runtime)?;
    let input = KafkaInput::prepare(config).map_err(AppError::Runtime)?;
    consume(config, input, &mut writer, &mut signals)
}

fn consume(
    config: &RuntimeConfig,
    input: KafkaInput,
    writer: &mut impl Write,
    signals: &mut SignalControl,
) -> Result<(), AppError> {
    let stats = Arc::new(Stats::default());
    let started = Instant::now();
    let (shutdown, pending_signals) = signals.parts();
    let result = runtime::run_pipeline(
        config,
        input,
        writer,
        shutdown,
        pending_signals,
        Arc::clone(&stats),
    );
    if config.stats {
        eprintln!("jkq: stats {}", stats.report(started.elapsed()));
    }
    match result.map_err(AppError::from)? {
        Some(signal) => Err(AppError::Interrupted(signal)),
        None => Ok(()),
    }
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
        let input = KafkaInput::prepare(&config).unwrap();
        fixture.produce(0, Some(br#"{"value":1}"#), None, 102, None);

        let mut output = Vec::new();
        let mut signals = SignalControl::install().unwrap();
        consume(&config, input, &mut output, &mut signals).unwrap();
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

    #[test]
    fn byte_backpressure_drains_oversized_records_one_at_a_time() {
        let fixture = Fixture::new("oversized-backpressure", 1);
        for value in [br#"{"value":0}"#, br#"{"value":1}"#, br#"{"value":2}"#] {
            fixture.produce(0, Some(value), None, 0, None);
        }
        let config = fixture.config(&[
            "-p",
            "0",
            "--snapshot",
            "-j",
            "2",
            "--max-inflight-records",
            "2",
            "--max-inflight-per-partition",
            "1",
            "--max-inflight-bytes",
            "4",
            "-f",
            "%o:%s\\n",
        ]);
        let mut output = Vec::new();
        run_with_writer(&config, &mut output).unwrap();
        assert_eq!(
            output,
            b"0:{\"value\":0}\n1:{\"value\":1}\n2:{\"value\":2}\n"
        );
    }
}
