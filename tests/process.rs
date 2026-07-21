#![cfg(unix)]

use std::{
    io::{BufRead, BufReader, Read},
    process::{Child, Command, Output, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use rdkafka::{
    ClientConfig,
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
    let child = fixture
        .command()
        .args(["--stats-interval", "100ms"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let (child, stderr_reader, _stats_lines) = wait_until_polling(child);
    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());

    let output = wait(child);
    assert_eq!(output.status.code(), Some(143));
    let stderr = stderr_reader.join().unwrap();
    assert!(
        String::from_utf8_lossy(&stderr).contains("interrupted by signal"),
        "{}",
        String::from_utf8_lossy(&stderr)
    );
    assert!(String::from_utf8_lossy(&stderr).contains("admitted=0"));
}

#[test]
fn second_termination_signal_forces_a_blocked_writer_to_exit() {
    let fixture = Fixture::new("signal-force");
    let mut payload = vec![b'a'; 512 * 1024];
    payload.insert(0, b'"');
    payload.push(b'"');
    fixture.produce(&payload);
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
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    stats_lines
        .recv_timeout(Duration::from_secs(3))
        .expect("poller did not begin graceful drain while stdout was blocked");

    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());

    let output = wait(child);
    assert_eq!(output.status.code(), Some(143));
    stderr_reader.join().unwrap();
}
