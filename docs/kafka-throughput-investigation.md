# Kafka throughput investigation

Measured on 2026-10-04, against jkq 0.10.1 at
`cb733cd8db1a06282a580aba51a439fe7b2e4106`, with the working tree's existing
expression changes present. Those changes are bypassed by the identity workload.

The retained change sets `fetch.queue.backoff.ms=100` before applying user
properties. It removes long fetch starvation on the fast compressed workload
without changing polling, ownership, admission, ordering, or termination.
On the 16 GiB LZ4 scan, changing only this property increased median throughput
by 51% with four consumers and 3.4 times with eight. This is workload dependent:
Zstandard decoding limited the slower workload, where the property was neutral.
No batch adapter, separate partition queues, payload pool, or sharding feature
was retained in this initial investigation. The subsequent
[bounded range-sharding milestone](kafka-range-sharding.md) implements and
measures opt-in unordered subranges.

## Environment and method

| Component | Configuration |
|---|---|
| Client host | Apple M4, 10 logical CPUs, 24 GiB RAM |
| OS | macOS 26.5.1, Darwin 25.5.0, arm64 |
| Compiler | rustc 1.98.1, LLVM 22.1.8, release build |
| Client libraries | rust-rdkafka 0.39.0; rdkafka-sys 4.10.0+2.12.1; bundled librdkafka 2.12.1 |
| Broker | Kafka 4.1.1, single KRaft broker/controller, replication factor 1, plaintext loopback |
| Native broker | Java 21.0.9, 1 GiB heap; observed RSS about 1.04 GiB |
| Initial Docker broker | Same Kafka image, capped at 4 CPUs and 3 GiB; Docker VM had 10 CPUs and about 7.75 GiB |
| Docker image digest | `sha256:0bc1bb2478f45b6cea78864df86acdc11e8df2c5172477819a4d12942cbe5d40` |

An isolated Docker broker was established first. Its log was then copied into a
native broker of the same version, with Docker stopped, to distinguish the
virtualized broker/network path from client CPU limits. No production cluster
was contacted. The native broker listened at `localhost:19092`; all scans forced
`broker.address.family=v4` to match the local listener.

Each source value is exactly 1,024 bytes of valid JSON. A deterministic pool of
2,048 fixtures contains a string id, 512 pseudorandom alphanumeric characters,
and padding. There are no keys, headers, or tombstones in the throughput fixture.
Producer settings were `linger.ms=20`, `batch.size=1048576`, and
`queue.buffering.max.kbytes=131072`. Topics used Zstandard, LZ4, or no compression.
These codecs and a repeated synthetic fixture do not cover every production
compression ratio or record distribution.

Initially each of eight partitions had 524,288 records: 4,194,304 records / 4 GiB
of source payload. LZ4 was extended to 2,097,152 records per partition (16 GiB).
Zstandard partition 0 was extended to 4,194,304 records (4 GiB in one partition).
Ranges ending one offset before the captured high were used to isolate sustained
consumption from EOF handling. Single-partition and exact-high controls are
identified separately below.

The jkq workload omitted all expressions and used `-f ''`, with stdout discarded.
This keeps assignment, payload copying, metadata selection, admission, channel
batching, completion, and writer release in the measurement; it skips JSON
processing and stdout volume. Rates are **uncompressed source payload rates**,
not compressed network throughput. Elapsed time includes process setup and
close. Consumer-only probes are explicitly identified and are not compared to
full-pipeline EOF timings as if they were interchangeable.

Tables use medians of three runs, except the ready-queue microbenchmark, which
uses five. `/usr/bin/time -l` supplied peak client RSS; child resource accounting
supplied user plus system CPU. “CPU cores” means CPU seconds / elapsed seconds,
including all client threads and excluding the broker. RSS includes native
librdkafka allocations, unlike the Rust allocation counter. Samples and stats
were collected in separate runs, excluded from the throughput tables.

Most comparisons were repeated against warm retained logs. Extending the data
set introduced cold-cache outliers, and fast scans have appreciable startup and
scheduling noise. One initial probe overlapped profiling, and one first split
trial overlapped compilation; those are unsuitable individual comparisons.
Their medians were not driven by those outliers. Raw trials, failures, arguments,
and stderr were preserved; differences of a few percent are not treated as a
reason to retain unsafe code. This is a GiB-scale local investigation, not a
billion-record production endurance test, remote-network test, or TLS benchmark.

## Current design and baseline scaling

`KafkaInput::prepare()` captures bounds and directly assigns partitions, without
consumer-group offset management. Each selected partition has one consumer
owner. More `--consumers` creates independent librdkafka clients, broker
connections, fetch/decompression machinery, and polling threads. The count is
capped by the number of assigned partitions; it cannot parallelize one partition.

`KafkaInput::poll()` calls `BaseConsumer::poll()` for one borrowed message, checks
bounds, and copies the payload and required source metadata into owned buffers.
The runtime batches channel transfers at 64 records or 1 MiB **after** those
polls and copies. Shared admission accounts for records and source bytes;
per-partition limits and completion frontiers bound work and preserve ordering.
A poller with an unadmitted owned record waits instead of requesting another
record. Permanent completion pauses that partition. The identity path releases
records through the writer without expression workers.

The source-byte budget is not a librdkafka prefetch budget. Before this change,
librdkafka's relevant defaults were:

| Property | Original value |
|---|---:|
| `fetch.message.max.bytes` | 1 MiB per partition, initially; librdkafka can grow it for a larger batch |
| `fetch.max.bytes` | 50 MiB per request, subject to oversized-batch behavior |
| `queued.max.messages.kbytes` | 64 MiB per common consumer queue |
| `queued.min.messages` | 100,000 |
| `fetch.wait.max.ms` | 500 ms |
| `fetch.queue.backoff.ms` | 1,000 ms |

Native broker, eight Zstandard partitions, `[0,524287)` per partition:

| Consumers | Seconds | Million records/s | Input MiB/s | Client CPU cores | Peak client RSS MiB |
|---:|---:|---:|---:|---:|---:|
| 1 | 5.122 | 0.819 | 800 | 1.02 | 28 |
| 2 | 2.535 | 1.655 | 1,616 | 2.07 | 54 |
| 4 | 1.465 | 2.863 | 2,796 | 4.79 | 121 |
| 8 | 1.160 | 3.614 | 3,530 | 6.87 | 202 |

Eight clients deliver 4.4 times the one-client rate, with diminishing returns.
Uncompressed scans plateau much sooner: 2.366 s / 1.101 s / 0.987 s with
1 / 4 / 8 consumers, at 1.22 / 4.07 / 4.67 client cores. Increasing consumers
is not uniformly beneficial: the long LZ4 scan is faster at four than eight
even after the backoff improvement. Ten host cores must accommodate the broker,
client fetch/decode threads, application threads, and OS.

The initial Docker exact-high Zstandard runs show why environment and endpoints
must be recorded:

| Assigned partitions | Consumers | Seconds | Million records/s | Input MiB/s | Client CPU cores |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 2.586 | 0.203 | 198 | 0.29 |
| 8 | 1 | 14.965 | 0.280 | 274 | 0.38 |
| 8 | 2 | 10.953 | 0.383 | 374 | 0.55 |
| 8 | 4 | 9.202 | 0.456 | 445 | 0.70 |
| 8 | 8 | 8.764 | 0.479 | 467 | 0.76 |

Those runs plateau despite low client CPU. On the native broker, the same
exact-high cases took 1.760 s for one partition and 9.705 / 5.051 / 2.919 /
2.101 s for eight partitions with 1 / 2 / 4 / 8 clients. Both environment and
termination inflated the apparent ingestion bottleneck.

In the one-client native exact-high control, lowering `fetch.wait.max.ms` from
500 to 10 changed 9.659 s to 5.234 s. The matching inside-log default scan was
5.096 s. EOF handling performs fresh synchronous watermark queries; those can
wait behind outstanding empty fetches. This is mostly termination latency,
not a doubling of sustained ingestion. The existing EOF correctness and
future-end handling were not changed. A long exact-high single-partition
Zstandard scan took 6.001 s; 8 MiB fetches took 5.477 s, including EOF costs.

## Profiling and queue evidence

macOS `sample` at a 1 ms interval and librdkafka statistics identified distinct
limits. Stack percentages here are **wall-state samples for a particular
thread**, not shares of total process CPU.

* Docker baseline: the poller waited on the native consumer queue in 6,388 of
  6,999 samples (91%); the writer waited in 95%. The learned-broker thread was
  in socket polling in about 61% and Zstandard decompression in about 31%.
  The virtualized broker/network path left much of the client idle.
* Native, one-client, eight-partition Zstandard: the poller waited in
  `BaseConsumer`/the C queue in 3,227 of 4,158 samples (78%); writer semaphore
  waits occupied 88%. The learned-broker thread was in `ZSTD_decompress` in
  2,587 samples (62%), with fetch/message-set parsing including decompression
  at about 66%. Statistics peaked at only 1,934 queued messages / 1,980,416
  bytes. Fetch queues were far below their byte threshold. Broker RTT averages
  were roughly 5.5–6.3 ms. This workload is principally client fetch/decode
  limited, not admission limited; increasing queue capacity cannot supply
  missing decoded records.
* Native LZ4, eight clients, original backoff: a sampled partition queue reached
  62,040 messages / 63,528,960 bytes, close to its 64 MiB threshold. One client
  reached `rxmsgs=230145` with 46,343 messages queued. About 105 ms later the
  application had drained all of them; `rxmsgs` then remained unchanged and the
  queue empty through about 935 ms after the first sample, despite remaining
  retained input. This is approximately 830 ms of empty-queue starvation
  following a burst. The poller waits match the fetch-queue backoff mechanism.
* After 100 ms backoff on the long LZ4 scan, client utilization rose markedly.
  The writer still had substantial channel-wait samples (about 64% inclusive
  in that profile), alongside record release/allocation work. Eight clients
  remaining slower than four suggests scheduling, allocation/release and shared
  pipeline coordination are becoming relevant. The profile does not establish
  a specific admission atomic as the next dominant cost.

Measured native broker CPU in the final controls was approximately 0.12 cores
for one-client Zstandard, 0.70–0.72 for four/eight-client Zstandard, and 1.64–1.79
for four/eight-client uncompressed scans. It was about 1.21 / 0.92 cores during
four/eight-client long LZ4 scans. These local measurements argue against broker
CPU saturation in the Zstandard case; they do not establish remote broker or
network capacity.

## Fetch, prefetch, and backoff experiments

Native eight-partition Zstandard, one client, inside-log range; all properties
other than the listed variation used their original defaults:

| Variation | Seconds | Client RSS MiB | Interpretation |
|---|---:|---:|---|
| Defaults | 5.096 | 36 | Reference |
| Partition fetch 8 MiB | 4.454 | 98 | 14% more throughput, materially more memory |
| Partition fetch 32 MiB | 6.445 | 190 | Large bursts and backoff reduce throughput |
| Partition fetch 32 MiB, backoff 10 ms | 4.472 | 186 | Recovers speed, twice the memory of 8 MiB fetches |
| Request fetch maximum 8 MiB | 5.113 | 34 | No gain |
| Prefetch byte maximum 8 MiB | 5.111 | 33 | No gain |
| Prefetch byte maximum 256 MiB | 5.219 | 35 | No gain |
| Prefetch minimum 1 million messages | 5.219 | 34 | No gain |
| Queue backoff 10 ms | 5.108 | 37 | Empty queues seldom hit the threshold |
| Fetch wait 10 ms | 5.144 | 26 | No sustained gain |
| Prefetch 8 MiB, backoff 10 ms | 5.229 | 32 | No gain |

Docker also showed about 13% better eight-partition throughput with 8 MiB
partition fetches, but RSS rose from about 41 to 108 MiB. Enlarging queues
alone and shortening backoff alone did not fix that environment's slower
fetch/decode supply. Fetch sizes remain workload tuning parameters, not new
jkq defaults.

The fast LZ4 workload exposed threshold-induced starvation. Long scan:
16,777,208 records, `[0,2097151)` on each of eight partitions, almost 16 GiB:

| Consumers | Backoff ms | Seconds | Million records/s | Input GiB/s | Client CPU cores | RSS MiB |
|---:|---:|---:|---:|---:|---:|---:|
| 4 | 1,000 | 4.852 | 3.458 | 3.30 | 2.88 | 346 |
| 4 | 100 | 3.208 | 5.230 | 4.99 | 5.12 | 435 |
| 4 | 10 | 2.867 | 5.852 | 5.58 | 6.35 | 457 |
| 8 | 1,000 | 14.421 | 1.163 | 1.11 | 1.90 | 772 |
| 8 | 100 | 4.236 | 3.961 | 3.78 | 7.52 | 737 |
| 8 | 10 | 3.878 | 4.326 | 4.13 | 7.65 | 919 |

The shorter 4 GiB LZ4 scans also improved: four clients went from 2.807 s at
1,000 ms to 0.923 s at 100 ms; eight went from 3.784 s to 1.207 s. Values of
10 and 0 ms were sometimes faster, but zero increased system CPU and aggressive
settings can keep more prefetch memory occupied. The retained 100 ms default
captures the large gain without selecting the most aggressive retry policy.

Burst timings vary. A separate four-client 16 GiB ABBA control, alternating
1,000 / 100 / 100 / 1,000 ms, took 5.125 / 3.394 / 3.132 / 10.316 s. The final
original-default run was slower, which rules out a simple cache-warming
explanation but also cautions against promising a universal percentage.

The actual rebuilt binary with the retained default took 2.991 s with four
clients and 3.853 s with eight on the long LZ4 scan. Zstandard controls at
1 / 4 / 8 clients took 5.280 / 1.437 / 1.133 s; uncompressed controls took
2.322 / 1.097 / 1.017 s. These controls are broadly neutral relative to their
original settings. The conservative improvement claims above use the
property-only comparison rather than attributing run-to-run variation to code.

Shorter backoff changes queue occupancy and CPU wakeups, not configured memory
limits. More clients still multiply native prefetch budgets; a faster refill
can increase actual memory use when output is slow. User `-F` and `-X` properties
win over the default, including values 0, 10, and 1,000.

## Poll boundary, copying, and batch consumption

A prefilled queue of 262,144 records (256 MiB payload) separated record extraction
from fetch and decompression. The queue used a 1 GiB byte limit, 300,000 message
minimum, and 8 MiB partition fetches. Drain timing starts after prefill.
“Borrow” black-boxes the borrowed slice; “copy” creates and releases the same
1 KiB owned `Vec` used by the payload path. It is not a proposed zero-copy runtime.

| Extraction | Borrow ns/record | Copy ns/record | Rust allocations/record, borrow / copy |
|---|---:|---:|---:|
| `BaseConsumer::poll()` | 119.6 | 163.8 | 1 / 2 |
| Native queue batch capacity 1 | 85.4 | 124.9 | 0 / 1 |
| Native queue batch capacity 64 | 45.2 | 86.9 | 0 / 1 |

A separate thread-local allocation counter found the extra high-level allocation
was 24 bytes per message (the event `Arc`); copying added one 1,024-byte allocation.
The counter excludes native C allocations and was not enabled for primary
throughput timings. The combined malloc/copy/free increment was about 40–44 ns
per record in this same-thread drain. The boundary/event overhead is measurable,
and batching nearly halves ready-queue extraction time. Neither number is the
share of complete Kafka consumption: native fetch/decompression can run in
parallel and most poll wall time in Zstandard was waiting for input.

Production frees generally happen on the writer rather than the copying thread,
so the microbenchmark is a lower-level cost estimate, not an exact cross-thread
allocator attribution. Consumer-only copy/borrow scans showed modest CPU savings
(roughly 0.15–0.3 CPU seconds over 4 GiB in representative comparisons), without
a consistent meaningful sustained rate improvement. Metadata allocation for
keys/headers was not exercised by this fixture. No elaborate ownership change
is justified by these measurements.

The prototype used `rd_kafka_queue_get_consumer()` and
`rd_kafka_consume_batch_queue()` through `rdkafka::bindings`; no added dependency
was needed. A queue guard releases the native queue before consumer destruction;
each returned message is read-only, copied if requested, and destroyed exactly
once. Dense fixture offsets were checked for omissions, duplicates, and ordering,
and completed partitions were paused. The final common-queue prototype drained
up to 64 ready messages with zero timeout; an empty queue blocks for a single
message, avoiding the initial prototype's wait-for-a-full-batch artifact.

Matched consumer-only sustained scans, eight partitions and one client,
`fetch.queue.backoff.ms=10`, copying payloads:

| Codec | High-level seconds | Batch-64 seconds | Result |
|---|---:|---:|---|
| Zstandard | 5.082 | 4.952 | 2.6% apparent gain; too small to justify adapter |
| LZ4 | 1.572 | 1.671 | 6% slower |
| Uncompressed | 2.283 | 2.577 | 13% slower |

Batching lowered CPU but did not improve sustained throughput materially.
The native batch loop still locks/dequeues individual operations internally;
it principally saves Rust event handling and boundary overhead.

The experiment is not a fully integrated correctness-qualified replacement.
Raw consumption bypasses `BaseConsumer`'s event dispatch; global errors, fatal
errors, statistics, authentication/control callbacks, and EOF handling require
explicit integration. Staged native messages would also require a defined
bounded ownership/admission rule. Preserving existing tombstones, metadata,
range/EOF policies, one pending admission record, ordering, pause, and graceful
close would need focused tests. With no sustained win, introducing that unsafe
boundary and completing those semantics would add maintenance risk without a
measured product benefit. The prototype was removed from the build.

## Separate partition queues

The normal consumer forwards assigned partition queues into a common queue.
Its prefetch thresholds therefore apply to their shared backlog. Splitting
queues changes the threshold to a per-partition budget: eight 64 MiB queues can
permit about 512 MiB of backlog instead of 64 MiB, before batch overshoot and
other allocations. That multiplication must not be mistaken for free throughput.

The experiment split queues before polling, retained common-queue callback
service, and drained partitions fairly. It compared public
`split_partition_queue()` with native partition queue forwarding disabled.
Idle sweeps slept briefly rather than spinning. Both one-message round robin
and a 64-message per-partition quantum were tested. Assignment changes would
require reestablishing split queues; the current static direct assignment
simplifies that concern but does not remove callback obligations.

Eight Zstandard partitions, one client, same record count and backoff:

| Strategy | Queue KiB per partition/common queue | Seconds | Client CPU cores |
|---|---:|---:|---:|
| Common high-level queue | 65,536 common | 4.875 | 0.87 |
| Public split, single-message fair drain | 8,192 each | 4.982 | 1.43 |
| Public split, single-message fair drain | 65,536 each | 4.986 | 1.43 |
| Native split, batch-64 fair drain | 8,192 each | 5.113 | 0.84 |
| Native split, batch-64 fair drain | 65,536 each | 5.020 | 0.84 |
| Public split, 64-message fair quantum | 8,192 each | 5.106 | 0.92 |
| Public split, 64-message fair quantum | 65,536 each | 5.108 | 0.92 |

Neither equal aggregate queue budget nor eight times the allowed budget helped
Zstandard. The split queues did not create additional broker fetch/decompression
threads; this workload's queues were already mostly empty. The initial busy
fair drain wasted more CPU, and reducing that overhead did not improve its rate.

A final fast-codec control used the 64-message public fair quantum and 100 ms
backoff. This matters because the decode-limited test alone cannot exclude a
benefit when records become ready much faster:

| Consumer-only strategy | LZ4 seconds | Uncompressed seconds |
|---|---:|---:|
| Common queue, 64 MiB | 1.562 | 2.439 |
| Public split, 8 MiB each (64 MiB aggregate) | 1.493 | 1.963 |
| Public split, 64 MiB each (512 MiB aggregate) | 1.461 | 1.815 |
| Native split batch-64, 8 MiB each | 1.652 | 2.568 |
| Native split batch-64, 64 MiB each | 1.646 | 2.544 |

Public split queues improved the bare uncompressed probe by about 24% at equal
aggregate budget. An alternating common / split / split / common control took
2.400 / 2.010 / 2.111 / 2.370 s, confirming a real consumer-only effect, albeit
smaller than the first comparison. The native batch variant still lost. The
first common trial after restarting the broker was cold; subsequent trials were
warm. These results warranted a full-pipeline experiment before rejecting queues.

The isolated runtime prototype changed only Kafka queue selection using the safe
public API. It split before polling, drained a ready queue for at most 64 records
before rotating, skipped empty queues, served common callbacks regularly, and
left record copying, range/EOF/error handling, pause, admission, ordering, and
writer release on the existing path. It did not pre-admit or stage record batches.
Queue budgets were divided among the assigned partitions to keep the same
64 MiB aggregate prefetch limit per consumer. Runs alternated common/split order,
with three trials of each configuration:

| Codec | Consumers | Common pipeline seconds | Split pipeline seconds | Common / split RSS MiB |
|---|---:|---:|---:|---:|
| Uncompressed | 1 | 2.586 | 2.505 | 31 / 31 |
| Uncompressed | 4 | 1.102 | 1.110 | 43 / 46 |
| LZ4 | 1 | 1.689 | 1.675 | 38 / 42 |
| LZ4 | 4 | 0.879 | 0.953 | 312 / 337 |
| Zstandard | 1 | 5.129 | 5.192 | 35 / 41 |
| Zstandard | 4 | 1.443 | 1.417 | 105 / 111 |

The bare-probe gain did not survive the complete runtime materially: the nominal
one-client uncompressed gain was about 3%, with appreciable trial variation;
four clients were neutral, while four-client LZ4 was slower. The common and split
one-client uncompressed runs both averaged about 1.15 client CPU cores. Queue
management would add scheduling, callback and memory-budget obligations without
a consistent product gain, so the runtime prototype was also removed. This
result does not exclude a future use for a strongly skewed workload; it supplies
no evidence to add a second queue path now.

## Same-partition range sharding

Low partition count demonstrably limited client-side parallelism. A small
external-process prototype divided a single Zstandard partition's
`[0,4194303)` range into 1 / 2 / 4 / 8 disjoint half-open intervals, using
independent jkq clients, unordered mode, and separate empty-output sinks.
Aggregate admission budgets were held at 8,192 records / 256 MiB, and the 64 MiB
prefetch budget was divided across shards.

| Shards | Seconds | Million records/s | Input GiB/s | Aggregate client CPU cores | Sum of process peak RSS MiB |
|---:|---:|---:|---:|---:|---:|
| 1 | 4.878 | 0.860 | 0.82 | 1.03 | 19 |
| 2 | 2.628 | 1.596 | 1.52 | 2.13 | 36 |
| 4 | 1.372 | 3.056 | 2.92 | 4.64 | 72 |
| 8 | 1.001 | 4.190 | 4.00 | 7.06 | 183 |

This is a 4.87-fold measured upper bound for that fixture. Four small disjoint
ranges also emitted and validated 4,096 exact offsets, with no overlap or omission
and order preserved inside each interval. RSS is the sum of individual peaks,
not a synchronized aggregate peak. Separate processes omit jkq's shared writer
and shared admission contention, so an integrated feature cannot claim this gain
without another experiment.

Disjoint offset ranges remain correct across compaction gaps: admission uses
`start <= offset < end`, not an assumed record count. However:

* Capture a consistent set of startup bounds and use the same isolation settings
  for every shard. Retention, truncation, aborted transactions and control records
  can change visible work. Numeric ranges may be very uneven in record count.
* Each shard must stop at its exclusive boundary, including when the last visible
  record is below it. EOF must reflect consumed/log position, not the last visible
  offset. The existing Rust EOF event exposes only a partition id and jkq uses
  fresh watermark checks; this needs deliberate review for trailing gaps and
  future-end behavior, rather than weakening termination to make the benchmark
  pass. Captured snapshot boundaries must not move independently among shards.
* Adjacent shards may fetch/decompress the same physical Kafka batch at a boundary;
  interval checks prevent logical duplicates. More clients add connections,
  requests, decompression work and broker concurrency. Real broker disk/network
  limits can erase the local benefit.
* This deliberately revises the current one-consumer-owner-per-partition invariant.
  An initial feature would need an explicit bounded, unordered mode. Per-shard
  sequence identifiers, completion ownership, global/per-partition count limits,
  errors and cancellation need integration; independent local partition sequences
  cannot simply enter the existing orderer.
* Exact ordered output requires holding later intervals until earlier intervals
  finish. If later ranges consume the global admission budget, the earliest range
  can be unable to progress, causing a deadlock. Reserving admission for the
  earliest interval, scheduling windows, or spooling would add substantial
  coordination. Unbounded reorder buffering is unacceptable.
* Admission and native prefetch must remain globally budgeted or divided among
  shards. Multiplying consumers without dividing prefetch silently multiplies
  memory. Slow-output behavior must be tested before any feature ships.

For one-client Zstandard ingestion, the current poll path is already close to
what this librdkafka fetch/decompression thread can supply: batch extraction's
ready-queue savings translated to only 2.6% apparent sustained gain. There is no
evidence for making that path more complex on this workload. With enough
partitions, selecting a measured consumer count and shortening threshold-induced
backoff are the useful existing controls. Once decode is parallelized, allocation,
shared pipeline coordination, host CPU and broker/network capacity need renewed
profiling rather than extrapolating the one-client profile.

No sharding option was implemented. The measured potential makes an integrated
**unordered bounded-scan** experiment the next highest-value throughput research
for a single heavily compressed partition. Ordered sharding is a different,
more complex proposition.

## Retained change, rejected work, and validation

The only production-code change is a 100 ms default queue backoff plus a private
configuration helper that makes precedence testable. A regression test failed
before the change and passes afterward; it verifies the default and explicit
0 / 10 / 1,000 ms overrides. No dependency or CLI option was added. Direct
assignment, borrowed-message copying, metadata selection, ranges, tombstones,
EOF policy, admission budgets, pause rules, error handling and close remain on
the existing path. The owning usage document describes the new default and
memory/tuning implications.

Rejected work: native batching saves extraction CPU but did not improve sustained
rate; separate queues added scheduling and potential memory multiplication
without a gain; eliminating payload ownership or adding pools was not supported
by the measured copy cost; fetch-size and larger-queue defaults would impose
memory costs without a consistent gain; zero/10 ms backoff was more aggressive
than necessary; ordered same-partition sharding requires too much coordination
to treat as a small ingestion optimization. Temporary examples were archived
under `target` and removed from the repository build.

`make check` passed: installer integration, formatting, clippy with all targets
and features and warnings denied, 90 unit/mock-Kafka tests, and five process
tests. These include byte backpressure, range boundaries, direct snapshots,
metadata/tombstones, multi-consumer counts, partition completion, broken pipes,
and graceful/forced termination. `git diff --check` also passed.

The required release-mode expression smoke consumed partition 0 offsets
`[0,100000)` with `--project '{"id": id}' -f ''` three times. All runs succeeded;
elapsed times were 0.239 / 0.225 / 0.229 s. This is a pipeline smoke check, not an
expression optimization comparison. Its arguments and timing are saved in
[`expression-smoke.jsonl`](../target/kafka-investigation/expression-smoke.jsonl).
The fixture throughput checks do not replace correctness tests for hypothetical
adapters or sharding.

## Reproduction and evidence

The one-off local artifacts are in `target/kafka-investigation/` (git-ignored).
They include:

* [`results.jsonl`](../target/kafka-investigation/results.jsonl): arguments, all
  trials, elapsed/CPU/RSS, stderr, and exit status;
* [`shards.jsonl`](../target/kafka-investigation/shards.jsonl) and
  [`shards.py`](../target/kafka-investigation/shards.py): disjoint-range trials;
* [`native-zstd.sample`](../target/kafka-investigation/native-zstd.sample),
  [`native-lz4-stalls.sample`](../target/kafka-investigation/native-lz4-stalls.sample),
  [`native-lz4-100ms.sample`](../target/kafka-investigation/native-lz4-100ms.sample),
  and matching `.stderr` files: stacks and raw librdkafka stats;
* [`allocations.stderr`](../target/kafka-investigation/allocations.stderr):
  thread-local Rust allocation counts;
* [`run.py`](../target/kafka-investigation/run.py), producer/consumer/allocator/
  diagnostic-runtime and split-runtime probe sources, preserved baseline binary and Kafka/runtime
  source, native broker config, retained logs, and broker start script.

The native broker can be restarted with `start-native.sh` in that directory.
It uses the local Java installation and absolute log path; adapt those paths
on another machine. The disposable Docker broker and native broker must not
run concurrently on port 19092. The saved driver contains this session's broker
PID for CPU accounting; update it after restarting. Recreating probe executables
requires copying the archived `_kafka_*.rs` sources back into `examples/` and
building them with `cargo build --release --locked --examples`; diagnostic module
paths in the runtime probe also need the local checkout path. These are isolated
experiments, not a new maintained benchmark framework.

A direct repeat of the main property comparison on the retained LZ4 data is:

```sh
cargo build --release --locked
/usr/bin/time -l target/release/jkq \
  -b localhost:19092 -t jkq-lz4 --consumers 4 \
  -o 0 --end-offset 2097151 -f '' \
  -X broker.address.family=v4 -X fetch.queue.backoff.ms=1000 >/dev/null
/usr/bin/time -l target/release/jkq \
  -b localhost:19092 -t jkq-lz4 --consumers 4 \
  -o 0 --end-offset 2097151 -f '' \
  -X broker.address.family=v4 -X fetch.queue.backoff.ms=100 >/dev/null
```

Repeat at least three times, alternate order, and repeat with eight consumers.
For Zstandard scaling, use `jkq-zstd`, `--end-offset 524287`, and
`--consumers 1`, `2`, `4`, `8`. For the microbenchmark, restore the probe and use
`BENCH_BACKEND=native python3 target/kafka-investigation/run.py prefill`; for
ready-batch comparisons use `batch-ready`, and for split queues use `split-fair`
and `split-quantum`. The archived producer/consumer probe contains the final
64-message public quantum; the earlier one-message variant is recorded in the
raw trials. `split-fast.py` repeats the fast-codec queue controls;
`split-pipeline.py` uses the archived `_kafka_split_runtime.rs` example and
`split-kafka.rs` / `split-runtime.rs` copies to repeat the full-pipeline comparison.
Prefill results are the probe's `DRAIN` intervals in stderr,
not the enclosing process times. Producing new fixture topics with the archived
probe requires matching partition counts and starting from empty logs;
appending again changes high-watermarks.

Primary implementation references: pinned
[librdkafka configuration](https://github.com/confluentinc/librdkafka/blob/v2.12.1/CONFIGURATION.md),
[queue implementation](https://github.com/confluentinc/librdkafka/blob/v2.12.1/src/rdkafka_queue.c),
[statistics fields](https://github.com/confluentinc/librdkafka/blob/v2.12.1/STATISTICS.md),
and [rust-rdkafka consumer implementation](https://github.com/fede1024/rust-rdkafka/blob/v0.39.0/src/consumer/base_consumer.rs).
All library hot-path and default claims were checked against the locally built
versions, not assumed from a newer release.
