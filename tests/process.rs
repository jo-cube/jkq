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

fn wait_until_polling(mut child: Child) -> (Child, thread::JoinHandle<Vec<u8>>) {
    let stderr = child.stderr.take().unwrap();
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let stderr_reader = thread::spawn(move || {
        let mut stderr = BufReader::new(stderr);
        let mut output = Vec::new();
        stderr.read_until(b'\n', &mut output).unwrap();
        ready_tx.send(()).unwrap();
        stderr.read_to_end(&mut output).unwrap();
        output
    });
    ready_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("jkq did not begin polling");
    (child, stderr_reader)
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
fn first_termination_signal_drains_and_uses_signal_exit_code() {
    let fixture = Fixture::new("signal-drain");
    let child = fixture
        .command()
        .args(["--stats-interval", "100ms"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let (child, stderr_reader) = wait_until_polling(child);
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
    let mut payload = vec![b'a'; 128 * 1024];
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
    let (child, stderr_reader) = wait_until_polling(child);
    for _ in 0..2 {
        let status = Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .unwrap();
        assert!(status.success());
        thread::sleep(Duration::from_millis(50));
    }

    let output = wait(child);
    assert_eq!(output.status.code(), Some(143));
    stderr_reader.join().unwrap();
}
