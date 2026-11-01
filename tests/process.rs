#![cfg(unix)]

use std::{
    io::{BufRead, BufReader, Read},
    net::TcpListener,
    process::{Child, Command, Output, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use rdkafka::{
    ClientConfig, bindings as sys,
    mocking::MockCluster,
    producer::{BaseProducer, BaseRecord, Producer},
};

struct Fixture {
    _cluster: MockCluster<'static, rdkafka::producer::DefaultProducerContext>,
    producer: BaseProducer,
    brokers: String,
    topic: &'static str,
}

impl Fixture {
    fn new(topic: &'static str) -> Self {
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic(topic, 1, 1).unwrap();
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

    fn produce(&self, payload: &[u8]) {
        self.producer
            .send(BaseRecord::<(), [u8]>::to(self.topic).payload(payload))
            .map_err(|(error, _record)| error)
            .unwrap();
        self.producer.flush(Duration::from_secs(5)).unwrap();
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_jkq"));
        command.args(["-b", &self.brokers, "-t", self.topic, "-p", "0"]);
        command
    }
}

fn wait(mut child: Child) -> Output {
    let deadline = Instant::now() + Duration::from_secs(5);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("jkq process did not stop within five seconds");
        }
        thread::sleep(Duration::from_millis(20));
    }
    child.wait_with_output().unwrap()
}

fn wait_until_polling(
    mut child: Child,
) -> (Child, thread::JoinHandle<Vec<u8>>, mpsc::Receiver<()>) {
    let stderr = child.stderr.take().unwrap();
    let (line_tx, line_rx) = mpsc::channel();
    let stderr_reader = thread::spawn(move || {
        let mut stderr = BufReader::new(stderr);
        let mut output = Vec::new();
        loop {
            if stderr.read_until(b'\n', &mut output).unwrap() == 0 {
                break;
            }
            if line_tx.send(()).is_err() {
                stderr.read_to_end(&mut output).unwrap();
                break;
            }
        }
        output
    });
    line_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("jkq did not begin polling");
    (child, stderr_reader, line_rx)
}

#[test]
fn broken_pipe_is_quiet_success() {
    let fixture = Fixture::new("broken-pipe");
    fixture.produce(br#"{"value":1}"#);
    let mut child = fixture
        .command()
        .args(["--snapshot", "-f", "%s"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    drop(child.stdout.take());

    let output = wait(child);
    assert!(output.status.success(), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
}

#[test]
fn check_validates_locally_without_connecting() {
    let child = Command::new(env!("CARGO_BIN_EXE_jkq"))
        .args([
            "-b",
            "unreachable.invalid:9092",
            "-t",
            "events",
            "-p",
            "0-2",
            "--drop-if",
            "$not(status in $vars.allowed)",
            "--vars",
            r#"{"allowed":["open"],"fallback":"unknown"}"#,
            "--project",
            r#"{"status": status = "open" ? status : $vars.fallback, "value": a ?? b ?? null}"#,
            "-f",
            "%a:%s\\n",
            "--check",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let output = wait(child);
    assert!(output.status.success(), "{output:?}");
    assert!(output.stdout.is_empty(), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
}

#[test]
fn check_rejects_invalid_native_properties_without_connecting() {
    for property in [
        "not.a.kafka.property=1",
        "fetch.queue.backoff.ms=invalid",
        "security.protocol=invalid",
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_jkq"))
            .args([
                "-b",
                "unreachable.invalid:9092",
                "-t",
                "events",
                "--check",
                "-X",
                property,
            ])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{output:?}");
        assert!(output.stdout.is_empty());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("configuration"),
            "{output:?}"
        );
    }
}

#[test]
fn check_rejects_jsonata_and_variables_without_connecting() {
    for arguments in [
        vec!["--drop-if", "environment == \"production\""],
        vec!["--vars", "{tenant: \"acme\"}"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_jkq"))
            .args([
                "-b",
                "unreachable.invalid:9092",
                "-t",
                "events",
                "-p",
                "0",
                "--check",
            ])
            .args(arguments)
            .output()
            .unwrap();

        assert_eq!(output.status.code(), Some(2), "{output:?}");
        assert!(output.stdout.is_empty(), "{output:?}");
    }
}

#[test]
fn first_termination_signal_drains_and_uses_signal_exit_code() {
    let fixture = Fixture::new("signal-drain");
    for (signal, expected) in [("-INT", 130), ("-TERM", 143)] {
        let child = fixture
            .command()
            .args(["--stats-interval", "100ms"])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let (child, stderr_reader, _stats_lines) = wait_until_polling(child);
        let status = Command::new("kill")
            .args([signal, &child.id().to_string()])
            .status()
            .unwrap();
        assert!(status.success());

        let output = wait(child);
        assert_eq!(output.status.code(), Some(expected));
        let stderr = stderr_reader.join().unwrap();
        assert!(
            String::from_utf8_lossy(&stderr).contains("interrupted by signal"),
            "{}",
            String::from_utf8_lossy(&stderr)
        );
        assert!(
            String::from_utf8_lossy(&stderr).contains("jkq: stats total admitted=0"),
            "{}",
            String::from_utf8_lossy(&stderr)
        );
    }
}

#[test]
fn second_termination_signal_forces_a_blocked_writer_to_exit() {
    let fixture = Fixture::new("signal-force");
    let mut payload = vec![b'a'; 512 * 1024];
    payload.insert(0, b'"');
    payload.push(b'"');
    fixture.produce(&payload);
    for (first_signal, second_signal, expected) in [("-INT", "-TERM", 143), ("-TERM", "-INT", 130)]
    {
        let child = fixture
            .command()
            .args(["--stats-interval", "100ms"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let (mut child, stderr_reader, stats_lines) = wait_until_polling(child);
        let mut stdout = child.stdout.take().unwrap();
        let mut first_output_byte = [0];
        stdout.read_exact(&mut first_output_byte).unwrap();
        assert_eq!(first_output_byte, [b'"']);
        while stats_lines.try_recv().is_ok() {}

        let status = Command::new("kill")
            .args([first_signal, &child.id().to_string()])
            .status()
            .unwrap();
        assert!(status.success());
        stats_lines
            .recv_timeout(Duration::from_secs(3))
            .expect("poller did not begin graceful drain while stdout was blocked");

        let status = Command::new("kill")
            .args([second_signal, &child.id().to_string()])
            .status()
            .unwrap();
        assert!(status.success());

        let output = wait(child);
        assert_eq!(output.status.code(), Some(expected));
        stderr_reader.join().unwrap();
    }
}

#[test]
fn range_sharding_drains_on_signal_and_treats_broken_pipe_as_success() {
    let fixture = Fixture::new("sharded-signal");
    let payload = vec![b'x'; 64 * 1024];
    for _ in 0..32 {
        fixture.produce(&payload);
    }
    let args = [
        "--snapshot",
        "--range-sharding",
        "--unordered",
        "--consumers",
        "4",
        "--stats-interval",
        "100ms",
        "--max-inflight-records",
        "4",
        "--max-inflight-per-partition",
        "4",
        "-f",
        "%o:%s\\n",
    ];
    let child = fixture
        .command()
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let (mut child, stderr_reader, _lines) = wait_until_polling(child);
    let mut stdout = child.stdout.take().unwrap();
    let mut first = [0];
    stdout.read_exact(&mut first).unwrap();
    assert!(
        Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let mut bytes = vec![first[0]];
    stdout.read_to_end(&mut bytes).unwrap();
    let output = wait(child);
    assert_eq!(output.status.code(), Some(143));
    let stderr = stderr_reader.join().unwrap();
    let stderr = String::from_utf8_lossy(&stderr);
    let total = stderr
        .lines()
        .find(|line| line.contains("stats total admitted="))
        .unwrap();
    let admitted = total
        .split("admitted=")
        .nth(1)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let mut offsets = bytes
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| line.split(|b| *b == b':').next().unwrap().to_vec())
        .collect::<Vec<_>>();
    assert_eq!(offsets.len(), admitted);
    offsets.sort_unstable();
    offsets.dedup();
    assert_eq!(offsets.len(), admitted);
    assert!(admitted > 0 && admitted <= 32);

    let mut child = fixture
        .command()
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    drop(child.stdout.take());
    let output = wait(child);
    assert!(output.status.success(), "{output:?}");
    assert!(!String::from_utf8_lossy(&output.stderr).contains("error"));
}

struct StartupProcess(Option<Child>);

impl StartupProcess {
    fn spawn(brokers: &str, arguments: &[&str]) -> Self {
        Self(Some(
            Command::new(env!("CARGO_BIN_EXE_jkq"))
                .args(["-b", brokers, "-t", "startup-cancellation"])
                .args(arguments)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        ))
    }

    fn wait_until(&mut self, mut ready: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !ready() {
            assert!(self.0.as_mut().unwrap().try_wait().unwrap().is_none());
            assert!(
                Instant::now() < deadline,
                "startup request was not observed"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn terminate(mut self, signal: &str, expected: i32) {
        let started = Instant::now();
        assert!(
            Command::new("kill")
                .args([signal, &self.0.as_ref().unwrap().id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let output = wait(self.0.take().unwrap());
        assert_eq!(output.status.code(), Some(expected), "{output:?}");
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(output.stdout.is_empty(), "{output:?}");
        assert!(output.stderr.is_empty(), "{output:?}");
    }
}

impl Drop for StartupProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
fn startup_signals_interrupt_unavailable_broker_metadata_discovery() {
    for (signal, expected) in [("-INT", 130), ("-TERM", 143)] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut child =
            StartupProcess::spawn(&listener.local_addr().unwrap().to_string(), &["--snapshot"]);
        let mut connection = None;
        child.wait_until(|| match listener.accept() {
            Ok((stream, _)) => {
                connection = Some(stream);
                true
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => false,
            Err(error) => panic!("cannot accept startup connection: {error}"),
        });
        // A connection proves handlers are installed. No Kafka server responds,
        // so automatic partition discovery cannot finish before the signal.
        child.terminate(signal, expected);
        let mut stream = connection.unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut requests = Vec::new();
        stream.read_to_end(&mut requests).unwrap();
    }
}

#[test]
fn startup_signals_interrupt_watermarks_and_timestamp_resolution() {
    const LIST_OFFSETS: i16 = 2;

    for arguments in [
        vec!["-p", "0", "--snapshot"],
        vec!["-p", "0", "-o", "s@1", "--snapshot"],
        vec!["-p", "0", "-o", "e@2"],
    ] {
        for (signal, expected) in [("-INT", 130), ("-TERM", 143)] {
            let owner: BaseProducer = ClientConfig::new()
                .set("test.mock.num.brokers", "1")
                .create()
                .unwrap();
            let cluster = owner.client().mock_cluster().unwrap();
            cluster.create_topic("startup-cancellation", 1, 1).unwrap();
            // SAFETY: owner retains the mock client throughout this test. The
            // variadic API takes (error: int, delay_ms: int) pairs. Holding the
            // target reply beyond the native deadline lets the error-stack
            // count acknowledge that exact request without a timing guess.
            // Timestamp lookup follows the two watermark requests.
            let native = unsafe { sys::rd_kafka_handle_mock_cluster(owner.client().native_ptr()) };
            let result = unsafe {
                if arguments.contains(&"-o") {
                    sys::rd_kafka_mock_broker_push_request_error_rtts(
                        native,
                        1,
                        LIST_OFFSETS,
                        3,
                        0_i32,
                        0_i32,
                        0_i32,
                        0_i32,
                        0_i32,
                        60_000_i32,
                    )
                } else {
                    sys::rd_kafka_mock_broker_push_request_error_rtts(
                        native,
                        1,
                        LIST_OFFSETS,
                        1,
                        0_i32,
                        60_000_i32,
                    )
                }
            };
            assert_eq!(result, sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR);
            let mut child = StartupProcess::spawn(&cluster.bootstrap_servers(), &arguments);
            child.wait_until(|| {
                let mut remaining = 0;
                // SAFETY: native still belongs to the live owner; remaining is
                // a valid writable count for this synchronous mock API.
                let result = unsafe {
                    sys::rd_kafka_mock_broker_error_stack_cnt(
                        native,
                        1,
                        LIST_OFFSETS,
                        &mut remaining,
                    )
                };
                assert_eq!(result, sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR);
                remaining == 0
            });
            child.terminate(signal, expected);
        }
    }
}

#[test]
fn slow_snapshot_startup_preserves_metadata_watermarks_and_timestamps() {
    let fixture = Fixture::new("slow-startup");
    fixture
        ._cluster
        .broker_round_trip_time(1, Duration::from_millis(250))
        .unwrap();
    for arguments in [
        vec!["--snapshot", "-o", "s@1"],
        vec!["-o", "s@1", "-o", "e@2"],
    ] {
        let output = wait(
            Command::new(env!("CARGO_BIN_EXE_jkq"))
                .args(["-b", &fixture.brokers, "-t", fixture.topic])
                .args(arguments)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        assert!(output.status.success(), "{output:?}");
        assert!(output.stdout.is_empty(), "{output:?}");
        assert!(output.stderr.is_empty(), "{output:?}");
    }
}
