# Architecture

`jkq` is a threaded Kafka JSON-processing pipeline around directly assigned
librdkafka consumers. The design keeps Kafka ownership, record actions,
ordering, and shutdown visible while isolating expression runtime values
inside compute workers.

```text
CLI and Kafka properties
→ startup expression and output plans
→ partition discovery, direct assignment, and offset resolution
→ one or more Kafka pollers with disjoint partition or bounded range assignments
→ bounded record batches through compute workers
→ batched completions and per-partition ordering
→ one output writer
```

Plans that neither evaluate expressions nor explicitly validate JSON bypass the
worker pool. The poller sends pass-through and tombstone completions directly
to the writer.

## Boundaries

```text
src/main.rs                process exit behavior
src/cli.rs                 parsing, validation, config, startup plans
src/app.rs                 process IO, signals, pipeline assembly
src/kafka.rs               assignment, offsets, polling, owned records
src/transform/mod.rs       shared compiled expressions and variable validation
src/transform/jsonata.rs   jx evaluation, serialization, and record actions
src/runtime.rs             poller, workers, writer, shutdown, statistics
src/runtime/state.rs       admission and completion-frontier state
src/output.rs              compiled formats and JSON envelopes
tests/process.rs           Unix process and signal behavior
```

The transform modules use jx's public compilation, shared-input preparation,
immutable binding, and compact serialization APIs. simd-json is used only to
serialize JSON envelope strings.

## Startup

Before polling, `jkq`:

1. parses and validates the CLI and librdkafka properties;
2. reads an optional `--vars-file`, compiles every expression, and
   validates the strict JSON `$vars` object;
3. stores immutable compiled `jx::Expression` values with shared compile-time
   `$vars` bindings in the transform plan;
4. compiles the optional payload format and final output format, combining
   their metadata requirements;
5. creates a consumer and discovers all topic partitions when none were
   selected;
6. fetches watermarks only for ranges that need them, resolves partition
   starts and ends, and captures snapshot highs in that same watermark pass;
7. distributes partitions across the requested number of consumers and
   assigns each partition directly to exactly one, unless explicit unordered
   range sharding divides its bounded interval across independent consumers;
8. installs the bounded pipeline.

With `--range-sharding`, startup resolves explicit `PartitionRange` values:
a source partition, a start offset, and an exclusive end offset. Sharding is
permitted only with `--unordered` and a fixed end. Startup watermarks reject
unavailable starts and ends beyond the retained log. If the consumer count
exceeds partition count, extra consumers are divided evenly across partitions;
each interval is split numerically into contiguous, non-overlapping ranges.
Each range has one owner, and one consumer never owns two ranges of the same
partition. Small spans and admission allowances cap the number of shards.
Without this option, assignment and partition ordering are unchanged.

No shared sequence or output frontier is needed between shards because output
is explicitly unordered. Source offsets and partition identities remain
unchanged. A record at or above its exclusive end finishes that range; EOF
uses the existing watermark check. Sparse offsets need no special handling:
each visible record falls into exactly one half-open range. A compacted tail
with no visible records completes at EOF. Snapshots capture their ends once
before creating any shard clients; later appends cannot extend them. Concurrent
retention/compaction can still remove records, as in an unsharded scan.

A startup failure cannot produce partial record output. `--check` exits after
local plan validation and does not create a consumer.

The Kafka adapter calls `assign`, never `subscribe`. Automatic commits and
offset storage are disabled. Each consumer is owned by one poller thread.
Partitions are paused only after reaching a permanent end or count boundary.
Automatic partition discovery is a startup snapshot; partitions added later
are not assigned to the running process.

## Record Ownership

librdkafka messages are borrowed. The poller copies the payload and only the
source metadata required by either compiled format, then releases the borrowed
message. It never mutates librdkafka-owned memory.

Each admitted input gets a dense local partition sequence. Kafka offsets remain
source metadata; the local sequence drives completion ordering even when
offsets are sparse.

Each poller groups admitted inputs into batches capped by record count and
retained source bytes. Partial batches flush when Kafka is idle, backpressure
starts, or consumption stops. All pollers feed the same bounded worker and
completion channels. Workers process each received batch
sequentially, and completions and source-byte releases cross their channels in
batches. Admission, actions, ordering, and byte accounting remain per record.
The completion source metadata retains the original payload length without
retaining another payload copy.

An action is separate from its output representation:

```text
Drop
Tombstone
PassThrough(exact source bytes | compact JSON bytes plus source length)
Project(compact JSON bytes)
```

Every admitted record produces one completion, including drops and fatal
transform results. This lets the partition completion frontier advance and
releases source-byte accounting exactly once even though channel handoffs are
batched.

## Expression Execution

The startup plan contains immutable compiled `jx::Expression` values shared
across all workers. `$vars` is validated and captured once as an immutable
`jx::OwnedValue` bound through `CompileOptions::constant_binding`. Reusing the
compile options shares its storage across expressions; workers require no
per-record binding setup.

Before workers start, one `jx::InputPlan` borrows the compiled expressions
in drop, tombstone, then projection order. Workers share this immutable plan.
Each required payload is prepared once: jx validates the complete input and
captures eligible bounded static paths during that traversal. Predicates and
projection evaluate through stable indices with independent state. Unsupported
expressions and deferred array paths use jx's ordinary evaluation fallback;
immutable `$vars` reads can participate in those captures. There is no tape,
mutable parsing copy, worker AST compilation, input tree, or jkq acquisition
cache or fallback evaluator. jx owns acquisition and expression semantics.

Predicates run in command-line order and require exactly one Boolean result.
Drop and tombstone actions short-circuit later expressions. Projection consumes
all results before publishing a completion: no results are an error, one
result is one JSON value, and multiple results form one array payload. Explicit
arrays remain single values. Lazy evaluation and serialization failures follow
`--on-eval-error` without publishing a partial projection.

Borrowed results serialize directly with `Value::write_compact` into a worker
byte buffer, without detached values or an intermediate JSON string. A
JSON-value pass uses `RawJson::write_compact` and retains source byte length.
After successful serialization, the worker transfers the output buffer into
the action and recycles the consumed source allocation as its next output
buffer. That retained allocation is worker scratch outside the source-byte
admission budget. Exact pass actions transfer the original bytes unchanged.

See [expression-language.md](expression-language.md) for native jx serialization
and missing-value behavior.

Existing Kafka tombstones bypass all JSONata work. They remain tombstones by
default and become drops when `--drop-tombstones` is set. Records selected by
`--tombstone-if` follow the same option before projection. An identity
transform also bypasses parsing unless `--on-invalid-json` was supplied
explicitly or a JSON-value envelope was requested.

## Ordering and Output

Compute workers may process records from the same partition concurrently.
Completions enter a per-partition frontier:

```text
next sequence → write or drop → release charge → advance
```

Ahead-of-frontier completions wait in a `BTreeMap`. Once a gap closes, the
writer drains the contiguous range. This preserves source order within each
partition but imposes no order across partitions. `--unordered` writes
completion arrival order instead.

One writer owns stdout. By default, formats and JSON envelopes stream directly
to its buffer, avoiding byte interleaving and an additional record-sized
staging buffer. With `--payload-format`, the writer formats each non-tombstone
action into one reused buffer before applying the final output format. Final
`%s`, `%S`, and `%R` therefore operate on the exact generated payload without
putting Kafka metadata into JSONata. Drops produce no bytes, tombstones bypass
payload formatting, and `%L` continues to report the source payload length.
JSON envelope strings use simd-json's streaming serializer; output accounting
and I/O error kinds, including broken pipe, are preserved. Broken pipe is normal
pipeline termination.
For a JSON-value envelope, the writer inserts compact JSON bytes produced by
the worker or projection and labels them with `payloadEncoding: "json"`.

Statistics are disabled by default. Disabled counter updates return before any
atomic operation. When `--stats-interval` is set, one reporter thread samples
relaxed atomic counters and writes per-window differences to stderr; Kafka
pollers and the output writer do no timer or reporting work. The final report
uses the cumulative counters.

## Backpressure and Memory Accounting

Shared atomic admission tracks global records and source bytes across all
consumers. Each poller separately tracks its partition sequences and
per-partition records. Range sharding divides the configured per-partition
limit into fixed local allowances whose sum is at most that limit. Global
record and source-byte admission remain shared, and releases return to the
range owner. `--count-per-partition` is rejected with sharding because local
sequences count one range; the global count remains shared. The byte charge
covers owned bytes copied from the source record:

- payload;
- key, when required;
- header names and header values, when required.

The charge intentionally excludes the worker-local output buffer, evaluation
intermediates, projected output, the writer-local payload-format buffer, and compact pass output for a JSON-value envelope. It
also excludes librdkafka's internal prefetch queue, which follows librdkafka's
own configuration. Those allocations depend on the input, formats,
expressions, and Kafka client settings. Bounded channels,
`--max-inflight-records`, `--max-inflight-per-partition`, and the owned
source-byte admission budget bound queued source work. Batches do not admit
records ahead of those limits.

Charges are released only after ordered write or drop. Slow output therefore
propagates pressure back to Kafka, and the reorder buffer cannot hold more
records than admission permits.

When a poller cannot reserve shared or per-partition capacity, it stops polling
its consumer and waits for completions to release capacity. A record already
returned by Kafka is held until it can be admitted. This can temporarily delay
other partitions assigned to the same consumer, but does not affect ordering or
memory bounds.

Because a source record's size is known only after polling, at most one owned
record per consumer may wait outside admitted accounting. A source record
larger than the byte budget is admitted only when no other admitted bytes
remain, preventing deadlock while preserving the runs-alone behavior.

## Termination and Failure

Fixed ranges use exclusive end offsets. Snapshot boundaries are captured once
and never extended. Completion means that the poller has stopped admitting the
range and every admitted partition sequence has crossed its frontier.

rdkafka 0.39 reports partition EOF without the event's offset. Fixed-end EOF
handling therefore queries a fresh broker high watermark, which leaves the
[future-end race documented in usage](usage.md#assignment-and-ranges). A
compatible fix needs the actual EOF offset: the last delivered record alone
cannot account for trailing compacted offsets or transaction control records.

Global counts atomically stop all admission after the configured number of
input records.
Per-partition counts mark each partition complete independently after its
limit; already admitted records still cross the normal completion frontier.

The first fatal error wins. It triggers shared cancellation, closes the work
path, and drains retained work. The ordered writer emits and flushes preceding
in-order records, emits nothing after the first fatal result, and releases
accounting for every completion. A flush failure never replaces an earlier
fatal error. Poller, worker, and writer panics become pipeline failures rather
than leaving another stage blocked.

The first termination signal stops admission and drains. signal-hook arms the
second signal for immediate process exit, which also handles a poller, worker,
or stdout that cannot make progress.

## Invariants

- One invocation consumes one topic and directly assigns either explicitly
  selected partitions or every partition discovered at startup.
- Every assigned partition belongs to exactly one consumer for the complete
  run.
- JSONata is the only expression language.
- Every successfully transformed non-tombstone input resolves to exactly one
  action.
- One input never expands into multiple output records.
- Existing tombstones bypass JSON and JSONata and remain tombstones unless
  `--drop-tombstones` is set.
- Pass-through preserves exact source payload bytes unless the user explicitly
  requests a JSON-value envelope or payload format.
- Payload formatting applies only to emitted non-tombstone actions and does not
  change their action name.
- Ordering is per partition, never global.
- Count limits apply to admitted input, not emitted output.
- Per-partition count limits apply independently to each assigned partition.
- Tombstones, empty payloads, and JSON `null` remain distinct.
- Channels, admitted record counts, per-partition records, and owned source
  bytes are bounded; JSONata intermediates, projected output, payload-formatted
  output, and compact JSON-value envelope output are not covered by the byte
  budget.
- stdout is record data; diagnostics and statistics use stderr.
- Errors follow explicit policy and are never silently successful.
