//! Fetch-only batching for librdkafka 2.12.1; rust-rdkafka still services events.
use std::{cell::OnceCell, ffi::CStr, marker::PhantomData, ptr, time::Duration};

use rdkafka::{
    bindings as sys,
    consumer::{BaseConsumer, Consumer, ConsumerContext, DefaultConsumerContext},
    error::{KafkaError, KafkaResult, RDKafkaErrorCode},
    message::{Header, Headers, Timestamp},
    topic_partition_list::TopicPartitionList,
};

const BATCH_SIZE: usize = 32;

struct Queue(*mut sys::rd_kafka_queue_t);

impl Drop for Queue {
    fn drop(&mut self) {
        // SAFETY: every Queue owns one non-null native queue reference.
        unsafe { sys::rd_kafka_queue_destroy(self.0) }
    }
}

pub(super) struct FetchConsumer<C: ConsumerContext = DefaultConsumerContext> {
    partitions: Vec<Queue>,
    queue: Queue,
    pending: [*mut sys::rd_kafka_message_t; BATCH_SIZE],
    next: usize,
    len: usize,
    // Drop queues and pending messages before the client that owns their memory.
    consumer: BaseConsumer<C>,
}

// SAFETY: librdkafka queues/messages support thread transfer. The client and all
// owned references move together; extraction requires exclusive access. No Sync
// implementation or transferable message handle is exposed.
unsafe impl<C: ConsumerContext> Send for FetchConsumer<C> {}

impl<C: ConsumerContext> FetchConsumer<C> {
    pub(super) fn new(
        consumer: BaseConsumer<C>,
        topic: &str,
        assignment: &TopicPartitionList,
    ) -> Result<Self, String> {
        if consumer.assignment().map_err(|e| e.to_string())?.count() != 0 {
            return Err("fetch queues require a consumer without an existing assignment".into());
        }
        let topic_name = std::ffi::CString::new(topic)
            .map_err(|_| "Kafka topic contains a NUL byte".to_owned())?;
        // SAFETY: the client remains owned by this reader until all queues drop.
        let queue = unsafe { sys::rd_kafka_queue_new(consumer.client().native_ptr()) };
        if queue.is_null() {
            return Err("cannot create Kafka fetch queue".into());
        }
        let mut input = Self {
            partitions: Vec::with_capacity(assignment.count()),
            queue: Queue(queue),
            pending: [ptr::null_mut(); BATCH_SIZE],
            next: 0,
            len: 0,
            consumer,
        };
        for partition in assignment.elements_for_topic(topic) {
            // SAFETY: get_partition creates/retains the partition queue even
            // before assignment. Forward first: FWD_APP prevents fetch startup
            // from routing records to rust-rdkafka's combined event queue.
            let queue = unsafe {
                sys::rd_kafka_queue_get_partition(
                    input.consumer.client().native_ptr(),
                    topic_name.as_ptr(),
                    partition.partition(),
                )
            };
            if queue.is_null() {
                return Err(format!(
                    "cannot get Kafka fetch queue for partition {}",
                    partition.partition()
                ));
            }
            input.partitions.push(Queue(queue));
            // SAFETY: both queue references are live and belong to this client.
            unsafe { sys::rd_kafka_queue_forward(queue, input.queue.0) };
        }
        input
            .consumer
            .assign(assignment)
            .map_err(|error| format!("cannot assign topic {topic}: {error}"))?;
        Ok(input)
    }

    pub(super) fn consumer(&self) -> &BaseConsumer<C> {
        &self.consumer
    }

    pub(super) fn poll(&mut self, timeout: Duration) -> Option<KafkaResult<NativeMessage<'_>>> {
        if self.next == self.len {
            // Only callbacks/errors belong here. Never batch the combined queue:
            // the legacy consume API cannot preserve rust-rdkafka event delivery.
            match self.consumer.poll(Duration::ZERO) {
                Some(Err(error)) => return Some(Err(error)),
                Some(Ok(_)) => {
                    return Some(Err(KafkaError::MessageConsumptionFatal(
                        RDKafkaErrorCode::State,
                    )));
                }
                None => {}
            }
            let mut len = self.extract(0, BATCH_SIZE);
            if len == 0 && !timeout.is_zero() {
                // A timed batch waits for its full size. Wait for just one to
                // preserve partial-batch latency and the 100 ms control cadence.
                let millis = timeout.as_millis().try_into().unwrap_or(i32::MAX);
                len = self.extract(millis, 1);
            }
            if len < 0 {
                // SAFETY: last_error is thread-local to the preceding call.
                return Some(Err(KafkaError::MessageConsumption(
                    unsafe { sys::rd_kafka_last_error() }.into(),
                )));
            }
            self.next = 0;
            self.len = len as usize;
            if self.len == 0 {
                return None;
            }
        }
        let ptr = std::mem::replace(&mut self.pending[self.next], ptr::null_mut());
        self.next += 1;
        let message = NativeMessage {
            ptr,
            headers: OnceCell::new(),
            _owner: PhantomData,
        };
        // SAFETY: extraction returned a live, independently destroyable message.
        let error = unsafe { (*ptr).err };
        match error {
            sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR => Some(Ok(message)),
            sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__PARTITION_EOF => {
                Some(Err(KafkaError::PartitionEOF(message.partition())))
            }
            sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__FATAL => {
                Some(Err(KafkaError::MessageConsumptionFatal(error.into())))
            }
            _ => Some(Err(KafkaError::MessageConsumption(error.into()))),
        }
    }

    fn extract(&mut self, timeout: i32, size: usize) -> isize {
        // SAFETY: this fetch-only queue is live; the empty fixed array has room
        // for size (1 or BATCH_SIZE) independently owned message pointers.
        unsafe {
            sys::rd_kafka_consume_batch_queue(
                self.queue.0,
                timeout,
                self.pending.as_mut_ptr(),
                size,
            )
        }
    }
}

impl<C: ConsumerContext> Drop for FetchConsumer<C> {
    fn drop(&mut self) {
        // SAFETY: remaining slots own messages not handed to NativeMessage;
        // forwarding references are removed before their destination/client drop.
        unsafe {
            for &message in &self.pending[self.next..self.len] {
                sys::rd_kafka_message_destroy(message);
            }
            for queue in &self.partitions {
                sys::rd_kafka_queue_forward(queue.0, ptr::null_mut());
            }
        }
    }
}

pub(super) struct NativeMessage<'a> {
    ptr: *mut sys::rd_kafka_message_t,
    headers: OnceCell<Option<NativeHeaders>>,
    _owner: PhantomData<&'a ()>,
}

impl Drop for NativeMessage<'_> {
    fn drop(&mut self) {
        // SAFETY: owns exactly one message; all borrowed views end before drop.
        unsafe { sys::rd_kafka_message_destroy(self.ptr) }
    }
}

impl NativeMessage<'_> {
    fn raw(&self) -> &sys::rd_kafka_message_t {
        // SAFETY: message stays live until this owner drops.
        unsafe { &*self.ptr }
    }

    pub(super) fn partition(&self) -> i32 {
        self.raw().partition
    }
    pub(super) fn offset(&self) -> i64 {
        self.raw().offset
    }
    pub(super) fn key_len(&self) -> usize {
        self.raw().key_len
    }
    pub(super) fn key(&self) -> Option<&[u8]> {
        let raw = self.raw();
        // SAFETY: a non-null key points to key_len immutable bytes in the message.
        (!raw.key.is_null())
            .then(|| unsafe { std::slice::from_raw_parts(raw.key.cast(), raw.key_len) })
    }
    pub(super) fn payload(&self) -> Option<&[u8]> {
        let raw = self.raw();
        // SAFETY: preserves null versus non-null zero-length payloads; memory is
        // immutable, valid for len bytes, and borrowed only while this owner lives.
        (!raw.payload.is_null())
            .then(|| unsafe { std::slice::from_raw_parts(raw.payload.cast(), raw.len) })
    }
    pub(super) fn timestamp(&self) -> Timestamp {
        let mut kind = sys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_NOT_AVAILABLE;
        // SAFETY: live message and valid output pointer; same mapping as rdkafka.
        let value = unsafe { sys::rd_kafka_message_timestamp(self.ptr, &mut kind) };
        match (value, kind) {
            (-1, _) => Timestamp::NotAvailable,
            (_, sys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_CREATE_TIME) => {
                Timestamp::CreateTime(value)
            }
            (_, sys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME) => {
                Timestamp::LogAppendTime(value)
            }
            _ => Timestamp::NotAvailable,
        }
    }
    pub(super) fn headers(&self) -> Option<&NativeHeaders> {
        self.headers
            .get_or_init(|| {
                let mut headers = ptr::null_mut();
                // SAFETY: headers remain owned by the live message, never detached.
                let error = unsafe { sys::rd_kafka_message_headers(self.ptr, &mut headers) };
                (error == sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR)
                    .then_some(NativeHeaders(headers))
            })
            .as_ref()
    }
}

pub(super) struct NativeHeaders(*mut sys::rd_kafka_headers_t);

impl Headers for NativeHeaders {
    fn count(&self) -> usize {
        // SAFETY: NativeHeaders is created and borrowed from a live message only.
        unsafe { sys::rd_kafka_header_cnt(self.0) }
    }
    fn try_get(&self, index: usize) -> Option<Header<'_, &[u8]>> {
        let (mut name, mut value, mut len) = (ptr::null(), ptr::null(), 0);
        // SAFETY: live headers, valid outputs; returned bytes/name borrow them.
        let error =
            unsafe { sys::rd_kafka_header_get_all(self.0, index, &mut name, &mut value, &mut len) };
        if error != sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            return None;
        }
        // SAFETY: librdkafka returns a NUL-terminated header name, decoded as UTF-8
        // with the same precondition as rust-rdkafka. A non-null value has len
        // valid bytes.
        Some(unsafe {
            Header {
                key: CStr::from_ptr(name).to_str().unwrap(),
                value: (!value.is_null()).then(|| std::slice::from_raw_parts(value.cast(), len)),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::{
        ClientConfig,
        client::ClientContext,
        message::OwnedHeaders,
        mocking::MockCluster,
        producer::{BaseProducer, BaseRecord, Producer},
        topic_partition_list::Offset,
    };
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Instant,
    };

    #[derive(Clone, Default)]
    struct Context {
        stats: Arc<AtomicUsize>,
    }
    impl ClientContext for Context {
        fn stats_raw(&self, _: &[u8]) {
            self.stats.fetch_add(1, Ordering::Relaxed);
        }
    }
    impl ConsumerContext for Context {}

    fn consumer<C: ClientContext>(
        cluster: &MockCluster<'_, C>,
        context: Context,
    ) -> BaseConsumer<Context> {
        ClientConfig::new()
            .set("bootstrap.servers", cluster.bootstrap_servers())
            .set("group.id", "fetch-tests")
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("enable.partition.eof", "true")
            .set("statistics.interval.ms", "10")
            .create_with_context(context)
            .unwrap()
    }

    fn assignment(topic: &str) -> TopicPartitionList {
        let mut list = TopicPartitionList::new();
        list.add_partition_offset(topic, 0, Offset::Beginning)
            .unwrap();
        list
    }

    #[test]
    fn fetch_batches_preserve_metadata_eof_and_consumer_callbacks() {
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("fetch-metadata", 1, 1).unwrap();
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", cluster.bootstrap_servers())
            .create()
            .unwrap();
        for offset in 0..70 {
            let payload = match offset {
                0 => None,
                1 => Some(b"".as_slice()),
                _ => Some(b"\xff\0bytes".as_slice()),
            };
            let mut record = BaseRecord::<[u8], [u8]>::to("fetch-metadata")
                .partition(0)
                .key(b"\xffkey")
                .timestamp(100 + offset)
                .headers(
                    OwnedHeaders::new()
                        .insert(Header::<&[u8]> {
                            key: "null",
                            value: None,
                        })
                        .insert(Header {
                            key: "empty",
                            value: Some(b"" as &[u8]),
                        })
                        .insert(Header {
                            key: "binary",
                            value: Some(b"\xff\0" as &[u8]),
                        }),
                );
            if let Some(payload) = payload {
                record = record.payload(payload);
            }
            producer.send(record).unwrap();
        }
        producer.flush(Duration::from_secs(5)).unwrap();
        let context = Context::default();
        let observed = context.stats.clone();
        let mut input = FetchConsumer::new(
            consumer(&cluster, context),
            "fetch-metadata",
            &assignment("fetch-metadata"),
        )
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut seen = 0;
        let mut eof = false;
        while !eof || observed.load(Ordering::Relaxed) == 0 {
            assert!(Instant::now() < deadline);
            match input.poll(Duration::from_millis(100)) {
                Some(Ok(message)) => {
                    assert_eq!(message.partition(), 0);
                    assert_eq!(message.offset(), seen);
                    assert_eq!(message.timestamp(), Timestamp::CreateTime(100 + seen));
                    assert_eq!(message.key(), Some(b"\xffkey".as_slice()));
                    assert_eq!(
                        message.payload(),
                        match seen {
                            0 => None,
                            1 => Some(b"".as_slice()),
                            _ => Some(b"\xff\0bytes".as_slice()),
                        }
                    );
                    let headers = message.headers().unwrap();
                    assert_eq!(headers.count(), 3);
                    assert_eq!(headers.get(0).value, None);
                    assert_eq!(headers.get(1).value, Some(b"".as_slice()));
                    assert_eq!(headers.get(2).value, Some(b"\xff\0".as_slice()));
                    assert!(headers.try_get(3).is_none());
                    seen += 1;
                }
                Some(Err(KafkaError::PartitionEOF(0))) => eof = true,
                Some(Err(error)) => panic!("{error}"),
                None => {}
            }
        }
        assert_eq!(seen, 70);
    }

    #[test]
    fn pending_messages_are_bounded_and_destroyed_before_reopening() {
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("fetch-pending", 1, 1).unwrap();
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", cluster.bootstrap_servers())
            .create()
            .unwrap();
        for _ in 0..80 {
            producer
                .send(
                    BaseRecord::<(), [u8]>::to("fetch-pending")
                        .partition(0)
                        .payload(b"record"),
                )
                .unwrap();
        }
        producer.flush(Duration::from_secs(5)).unwrap();
        for _ in 0..3 {
            let mut input = FetchConsumer::new(
                consumer(&cluster, Context::default()),
                "fetch-pending",
                &assignment("fetch-pending"),
            )
            .unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            // SAFETY: test observes the queue owned by input without consuming it.
            while unsafe { sys::rd_kafka_queue_length(input.queue.0) } < BATCH_SIZE {
                assert!(Instant::now() < deadline);
                input.consumer.poll(Duration::ZERO).transpose().unwrap();
                std::thread::sleep(Duration::from_millis(10));
            }
            let message = input.poll(Duration::ZERO).unwrap().unwrap();
            assert_eq!(message.offset(), 0);
            let bytes = message.payload().unwrap().to_vec();
            drop(message);
            assert_eq!(input.len - input.next, BATCH_SIZE - 1);
            drop(input);
            assert_eq!(bytes, b"record");
        }
    }

    #[test]
    fn existing_assignments_cannot_be_silently_rerouted() {
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("fetch-assigned", 1, 1).unwrap();
        let consumer = consumer(&cluster, Context::default());
        let assignment = assignment("fetch-assigned");
        consumer.assign(&assignment).unwrap();
        let error = FetchConsumer::new(consumer, "fetch-assigned", &assignment)
            .err()
            .unwrap();
        assert!(error.contains("existing assignment"));
    }
}
