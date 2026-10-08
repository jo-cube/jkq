# Bounded Kafka range sharding

This is a historical investigation. Its `-j`/worker-count examples describe
an earlier CLI; evaluator parallelism is now automatic. See
[the current runtime controls](usage.md#parallelism-and-memory).

Measured on 2026-10-04 after the initial [Kafka throughput investigation](kafka-throughput-investigation.md).
The retained implementation adds `--range-sharding`, requiring `--unordered`
and a fixed end boundary. `--consumers` remains the upper bound on independent
Kafka clients. No JSON implementation changes belong to this milestone.

On the single-partition identity workload, the implementation raises
Zstandard throughput from 0.837 to 3.762 million records/s with eight clients,
a 4.49x gain. LZ4 improves from 3.034 to 3.887 million records/s with two
clients, a 28% gain with the retained 100 ms queue backoff. More LZ4 clients
require tuning and do not monotonically improve throughput.

**Kafka is no longer the sole bottleneck once JSON processing is enabled.**
A small projection makes parsing/value construction and expression execution
major consumers of CPU. Admission backpressure then restricts ingestion, even
while Kafka queues contain records. Queue backoff also matters; a shorter
backoff recovers some scaling without removing the downstream limit. This
milestone characterizes that transition and does not optimize JSON processing.

## Environment and measurement method

The environment, producer fixture, and library versions are unchanged from the
[initial investigation](kafka-throughput-investigation.md#environment-and-method):
Apple M4, 10 logical CPUs, 24 GiB RAM, macOS 26.5.1, release Rust 1.98.1,
rust-rdkafka 0.39.0, bundled librdkafka 2.12.1, and a native Kafka 4.1.1
single-node KRaft broker on plaintext loopback with replication factor one.
The broker uses Java 21 and a 1 GiB heap. The baseline includes the committed
100 ms fetch-queue backoff and the existing scalar variable-lookup changes;
identity consumption bypasses the JSON workers.

Each value is exactly 1,024 bytes: two JSON string fields, about half random
alphanumeric data and half repeated padding, drawn from 2,048 fixtures.
Compressed producer batches target 1 MiB with a 20 ms linger. Measurements
select source partition 0 only:

| Codec/topic | Half-open interval | Records | Logical input |
|---|---|---:|---:|
| LZ4 / `jkq-lz4` | `[0,2097151)` | 2,097,151 | almost 2 GiB |
| Zstandard / `jkq-zstd` | `[0,4194303)` | 4,194,303 | almost 4 GiB |

Ends lie inside the retained log to avoid final EOF watermark-query latency.
The last record in each topic is outside the measured interval. Numeric
subranges are contiguous and non-overlapping. Output uses `-f ''` and
`/dev/null`; logical input throughput measures payload bytes, not compressed
network traffic. All measurements include client startup and shutdown.

Tables report medians of three runs. CPU cores means process user plus system
CPU seconds divided by elapsed wall seconds: 1.0 is equivalent to one fully
occupied core averaged over the invocation. Broker CPU is measured separately
with `ps`. M4 cores are not uniform, so these figures do not imply equal core
performance or exact host saturation. RSS is maximum resident size; external
process RSS sums each process's individual maximum, which need not coincide.

Unless indicated otherwise, the aggregate nominal native prefetch byte limit
is held at 64 MiB by setting `queued.max.messages.kbytes=65536/clients` on
each client. Global source-byte admission stays at 256 MiB and global admitted
records at 8,192. External processes divide both budgets and the per-partition
limit evenly. The integrated implementation shares global admission and
divides the per-partition limit across ranges. Identity runs create no JSON
workers. Processing controls hold the total worker count at eight.

There are 192 timed scans across the external prototype, integrated curves,
processing controls, and property controls. They run sequentially; profiling,
fixture production, and compilation are separate from timed scans. No broker
or remote network saturation claim follows from localhost measurements. The
short LZ4 runs include meaningful startup and scheduling variance: one initial
one-client integrated run took 1.426 s, versus 0.687 and 0.691 s thereafter.
The Zstandard result is much larger than this variance. These are GiB scans,
not a completed billion-record production run; remote/TLS, realistic nested
JSON, cold storage, and long-run behavior remain deployment checks.

## External-process prototype

Each process directly assigns the same source partition at its own absolute
start and stops before its exclusive end. Shards emit independently, and no
ordered merge is attempted. The prototype is a performance upper bound rather
than a product implementation.

| Clients/shards | LZ4 seconds | LZ4 M records/s | LZ4 CPU cores | LZ4 RSS MiB | Zstd seconds | Zstd M records/s | Zstd CPU cores | Zstd RSS MiB |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0.710 | 2.952 | 1.67 | 21 | 5.047 | 0.831 | 1.01 | 19 |
| 2 | 0.525 | 3.998 | 2.89 | 45 | 2.646 | 1.585 | 2.13 | 36 |
| 4 | 0.425 | 4.939 | 4.09 | 96 | 1.392 | 3.013 | 4.63 | 72 |
| 8 | 0.441 | 4.758 | 4.08 | 232 | 1.069 | 3.922 | 6.63 | 206 |
| 16 | 0.673 | 3.116 | 2.83 | 382 | 1.196 | 3.507 | 5.99 | 356 |

LZ4 peaks around four processes; Zstandard peaks around eight. Broker average
CPU grows from 0.15 to about 0.89 cores on Zstandard, and from 0.66 to 2.07
cores at the LZ4 peak. This does not indicate broker CPU exhaustion.
More clients increase independent fetch/decompression machinery and
connections; they are not merely extra application poll threads.

## Integrated implementation

The same subranges feed one bounded jkq pipeline, one worker pool when needed,
and one stdout writer. Each range retains its source partition identity.

| Clients/shards | LZ4 seconds | LZ4 M records/s | LZ4 GiB/s | LZ4 CPU cores | LZ4 RSS MiB | Zstd seconds | Zstd M records/s | Zstd GiB/s | Zstd CPU cores | Zstd RSS MiB |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0.691 | 3.034 | 2.894 | 1.71 | 18 | 5.009 | 0.837 | 0.799 | 1.03 | 20 |
| 2 | 0.539 | 3.887 | 3.707 | 3.04 | 53 | 2.580 | 1.626 | 1.550 | 2.16 | 36 |
| 4 | 0.558 | 3.756 | 3.582 | 3.28 | 113 | 1.419 | 2.957 | 2.820 | 4.79 | 101 |
| 8 | 0.849 | 2.470 | 2.356 | 3.15 | 167 | 1.115 | 3.762 | 3.588 | 6.83 | 126 |
| 16 | 1.152 | 1.821 | 1.736 | 2.82 | 175 | 1.561 | 2.687 | 2.562 | 4.81 | 192 |

The Zstandard implementation captures about 96% of the external prototype's
peak records/s. The LZ4 pipeline reaches its useful point earlier. Its common
writer, shared admission, channel handoffs, and queue refill scheduling differ
from independent processes; the profiles do not establish one exclusive cause
for that gap. Adding consumers beyond the measured useful count is rejected
as a tuning choice, not automatically enabled.

### Queue controls

These controls use the same integrated single-partition identity scan.

| Codec | Clients | Backoff ms | Aggregate nominal prefetch MiB | Seconds | CPU cores | RSS MiB |
|---|---:|---:|---:|---:|---:|---:|
| LZ4 | 4 | 100 | 64 | 0.558 | 3.28 | 113 |
| LZ4 | 4 | 10 | 64 | 0.477 | 4.93 | 111 |
| LZ4 | 8 | 100 | 64 | 0.849 | 3.15 | 167 |
| LZ4 | 8 | 10 | 64 | 0.557 | 5.87 | 149 |
| LZ4 | 8 | 0 | 64 | 0.547 | 5.96 | 185 |
| LZ4 | 4 | 100 | 256 (defaults) | 0.506 | 4.55 | 216 |
| LZ4 | 8 | 100 | 512 (defaults) | 0.616 | 6.01 | 697 |
| Zstd | 8 | 100 | 64 | 1.115 | 6.83 | 126 |
| Zstd | 8 | 10 | 64 | 1.077 | 7.15 | 130 |
| Zstd | 8 | 0 | 64 | 1.072 | 7.20 | 155 |
| Zstd | 8 | 100 | 512 (defaults) | 1.102 | 7.18 | 132 |

LZ4 is sensitive to refill delays; 10 ms raises four-client throughput by
about 45% over the one-client 100 ms reference, with the same aggregate
prefetch setting. Enlarging prefetch budgets also helps the eight-client LZ4
case but costs substantial memory. The 100 ms default is retained. Explicit
Kafka properties remain overrides; sharding does not silently divide or
increase librdkafka configuration values. A default client has a nominal
64 MiB queue, so eight default clients allow 512 MiB before accounting for
fetch/decode bursts, other native allocations, and jkq-owned data.

With backoff set to zero, sixteen clients still lose: LZ4 identity takes
0.891 s and Zstandard 1.660 s. Median system CPU alone increases to about
3.70 and 4.12 seconds respectively. For projection, sixteen clients take
1.063 and 2.261 s. Removing queue delay does not make unlimited parallelism
useful; aggressive retries can materially increase system CPU. No new default
or adaptive scheduling machinery is warranted by these controls.

## Processing and the bottleneck transition

Explicit JSON validation uses `--on-invalid-json fail`. The projection control
uses `--project '{"id": id}'`, exercising parsing, value construction,
expression evaluation, result validation, and serialization. Neither control
changes the implementation of those operations. Both keep eight workers.

| Codec | Work | 1 shard s | 2 shards s | 4 shards s | 8 shards s | CPU cores at 8 |
|---|---|---:|---:|---:|---:|---:|
| LZ4 | Identity | 0.691 | 0.539 | 0.558 | 0.849 | 3.15 |
| LZ4 | Validation | 0.732 | 0.625 | 0.583 | 0.949 | 3.29 |
| LZ4 | Projection | 0.851 | 0.685 | 0.916 | 0.989 | 4.38 |
| Zstd | Identity | 5.009 | 2.580 | 1.419 | 1.115 | 6.83 |
| Zstd | Validation | 4.625 | 2.594 | 1.515 | 1.374 | 6.85 |
| Zstd | Projection | 4.726 | 2.786 | 1.929 | 2.197 | 5.37 |

The slightly faster one-client processing controls are observed variation and
scheduling effects, not evidence that parsing accelerates ingestion.

A 10 ms backoff makes LZ4 projection take 0.643 / 0.667 s at four/eight
clients, versus identity 0.477 / 0.557 s. Zstandard projection takes
1.974 / 1.618 s, versus identity 1.418 / 1.077 s. Thus the apparent reversal
at eight with 100 ms includes Kafka queue delays. After that control,
projection still costs roughly 50% more wall time than identity at eight
Zstandard shards, and uses 7.72 average CPU cores versus 7.15. Sixteen clients
with zero backoff regress for both work types. JSON work and runtime overhead
now constrain the available ingestion capacity alongside decompression.

### Profiler and statistics evidence

A temporary executable copies the final adapter/runtime and adds only
`ClientContext::stats_raw` every 200 ms and counters around admission waits.
`sample` captures thread stacks at 1 ms intervals. These instrumented runs use
`--stats`; they establish where threads wait and execute, not the uninstrumented
throughput numbers above. Temporary executable sources are removed from
`examples/` and kept only in ignored local artifacts.

At one Zstandard shard, the identity poller waits in Kafka polling for about
97% of sampled wall time, the writer waits about 99%, native prefetch peaks
around 1.85 MiB, and `ZSTD_*` decoding appears in roughly 59% of the fetch
broker thread's wall samples. Admission waits total zero. This is Kafka-client
fetch/decompression supply limiting the pipeline.

At eight shards with the 10 ms backoff:

| Evidence | LZ4 identity | LZ4 projection | Zstd identity | Zstd projection |
|---|---:|---:|---:|---:|
| Fetch-thread wall samples blocked in kernel waits | 88% | 90% | 26% | 35% |
| Worker wall samples blocked | n/a | 12% | n/a | 38% |
| Sum of client queue peaks, MiB | 49.0 | 61.3 | 32.1 | 62.8 |
| Sum of poller admission-wait seconds | 1.855 | 3.572 | 0.963 | 6.819 |
| Longest poller's admission-wait seconds | 0.249 | 0.465 | 0.133 | 0.896 |

Queue peaks sum each client's observed maximum and need not be simultaneous.
Admission times sum parallel waits and may exceed elapsed wall time. They
exclude the final graceful-drain wait. Kernel waiting samples are per-thread
wall states, not percentages of aggregate process CPU.

In tuned projection runs, approximately 48% of non-blocked worker leaf samples
have `JValue::from_json_str` on their stack: simd-json parsing and JSONata value
construction, including string/object allocations. About 14–15% include
native evaluator calls, and about 3% include result serialization. Remaining
worker samples include document/context destruction, allocation/free work,
validation, and pipeline bookkeeping. These are approximate stack categories;
inlining prevents an exact global CPU attribution.

LZ4 fetch/decompression is largely idle while JSON workers are busy. In the
Zstandard case, decompression remains a large CPU contributor, but downstream
workers are now another major contributor. Queues fill while pollers wait for
admission, so downstream backpressure restricts ingestion. It would be wrong
to describe either projection workload solely as a lack of Kafka consumers,
or to attribute all flattened scaling to JSON while ignoring queue refill
and shared-runtime overhead. Substantial JSON optimization is deferred.

## Correctness and retained design

The [architecture](architecture.md#startup) and [usage](usage.md#assignment-and-ranges)
documents own the behavior. The implementation is deliberately static:

- `PartitionRange` contains the real source partition, start position, and
  exclusive end. It does not invent partition identities for shards.
- If consumers exceed selected partitions, extra clients are allocated evenly
  across partitions. Each numeric interval is divided into adjacent half-open
  ranges; unequal remainders differ by one offset. Offset width is not a count
  of visible records.
- Each range has one independent directly assigned consumer. No consumer
  receives two ranges of the same source partition. Assignment never subscribes
  or enables offset commits/storage.
- Only explicit unordered bounded execution is accepted. Ordered behavior and
  ordinary `--consumers` behavior are unchanged. Future end offsets are
  rejected for sharding; the initial range must already be available.
- Small/empty intervals and the per-partition admission budget cap shard count.
  Static per-range allowances sum to at most the original partition allowance;
  global record/source-byte admission and global `-c` remain shared. Per-shard
  local sequences are not used as a cross-shard output frontier.
- `--count-per-partition` is rejected with the new option. Global `-c` selects
  an unordered subset, counts source inputs, and does not duplicate admissions.
- Existing payload/metadata copying, tombstone distinctions, error policies,
  permanent pause, EOF watermark checks, and shutdown code remain in use.
  One pending owned source record per consumer can remain outside admission,
  and native prefetch is separately configured, as before.

Pure range tests check adjacency, unique ownership at boundaries and sparse
sample offsets, empty/reversed intervals, short spans, near-i64-max boundaries,
and admission allowances. Mock-Kafka tests compare exact output for two source
partitions across identity/validation/projection, keys, headers, timestamps,
tombstones, empty payloads, and oversized records under tight byte/record
budgets. They also cover captured snapshots excluding later appends, global
count, empty snapshots, rejected future boundaries, and transform failure
propagating across ranges. A process test verifies first-signal draining of
admitted records and quiet successful broken-pipe termination. Existing
ordered, error, and second-signal tests remain applicable.

A real compacted topic provides a separate offset-gap check: high watermark
65,537, 1,118 visible records, 14 tombstones, first visible offset 64,419.
Five intervals at 2, 4, and 8 consumers match unsharded output byte-for-byte
(after sorting complete records), including `[10000,20000)` with no visible
records and an empty interval. No duplicates or omissions were observed.
Most work falls into the last shard in this fixture, illustrating that static
numeric splitting can be severely imbalanced on compacted topics. This
correctness fixture does not justify a density estimator or work stealing.
Concurrent compaction/retention can still remove records during a scan; captured
watermarks are boundaries, not immutable Kafka data snapshots.

Compressed batches may cross a shard boundary. Multiple clients can fetch and
decode overlapping native batches, and prefetch can run past a shard's end;
application admission/output remains disjoint. Sampled compressed receive
volume is about 2.1 GiB for the 4 GiB Zstandard scan and about 1.0–1.1 GiB for
the 2 GiB LZ4 scan. Sharding adds connections, request load, and some boundary
read amplification. Short ranges suffer startup/EOF costs disproportionately.

## Retained and rejected approaches

Retained: one explicit CLI flag, an internal range type and static assignment
planner, a per-input admission allowance, tests, and documentation. The runtime
uses its existing threads, bounded channels, releases, and shared admission.
No unsafe Rust, dependency, payload pool, zero-copy ownership, batch polling,
separate partition queues, or new scheduler is introduced.

Rejected or deferred:

- Automatic sharding through `--consumers`: requires an explicit option because
  it changes ownership, ordering, broker load, and native memory use.
- Ordered concurrent ranges: later ranges can occupy admission while earlier
  output is required, creating deadlock or requiring dedicated budgets/merge
  machinery. No ordered sharding prototype is retained.
- Unbounded/future ranges: unsupported; fixed available boundaries make EOF and
  non-overlap straightforward.
- More than the useful client count: sixteen clients regress, including with
  zero queue backoff, and increase memory/system CPU.
- Larger prefetch as a universal fix: helps fast LZ4 but can cost hundreds of
  MiB without helping Zstandard. Explicit user tuning remains available.
- Dynamic balancing, work stealing, and density estimation: numeric splitting
  captures the large dense-scan gain. Sparse imbalance is documented rather
  than hidden behind speculative machinery.
- JSON optimization: processing is now a significant limit on the measured
  projection workloads; it belongs to the separately planned subsystem work.

## Reproduction and validation

Example integrated commands for the retained Zstandard workload:

```sh
cargo build --release --locked
/usr/bin/time -l target/release/jkq \
  -b localhost:19092 -t jkq-zstd -p 0 -o 0 --end-offset 4194303 \
  --range-sharding --unordered --consumers 8 -j 8 -f '' \
  -X broker.address.family=v4 -X queued.max.messages.kbytes=8192 >/dev/null
```

Repeat three times at 1/2/4/8/16 consumers, dividing 65,536 KiB by the client
count to hold the aggregate nominal queue budget fixed. Change topic and end
to `jkq-lz4` / `2097151` for LZ4. For processing controls add
`--on-invalid-json fail` or `--project '{"id": id}'`; keep `-j 8` fixed.
Compare `-X fetch.queue.backoff.ms=10` separately. Remove only the queue-size
override to measure native default memory multiplication.

The external harness, integrated harness, controls, raw JSONL records, copied
profiling sources, system profiles, and compacted-topic hashes are local
artifacts in ignored `target/kafka-investigation/`:
`range-milestone.py`, `range-integrated.py`, `range-settings.py`,
`range-ceiling.py`, `range-milestone.jsonl`, `range-profiles*.py`,
`range-*.sample`, `range-*.stderr`, `range-profile-summary-corrected.jsonl`,
and `range-sparse-validation.json`. These are intentionally not committed.
The existing producer fixture/harness is archived there as well; synthetic
Kafka data and the broker remain disposable local resources.

Validation includes `cargo test --locked shard`,
`cargo clippy --locked --all-targets --all-features -- -D warnings`,
`make check`, and three release runs of the documented 100,000-record
projection smoke test. No throughput thresholds or benchmark machinery are
added to CI.

The next highest-value Kafka check is this option on a representative remote
broker, long bounded ranges, and a fixed native-memory budget. On fast LZ4,
common runtime handoffs and refill scheduling deserve measurement before more
clients. When a projection or realistic JSON payload fills compute workers,
profile the separately planned JSON subsystem; increasing Kafka parallelism
alone cannot recover that processing cost.
