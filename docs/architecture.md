# Architecture

`jkq` is a threaded Kafka-to-JSONata pipeline around a directly assigned
librdkafka consumer. The design keeps Kafka ownership, record actions,
ordering, and shutdown visible while isolating jsonata-core's single-threaded
runtime values inside compute workers.

```text
CLI and Kafka properties
→ startup JSONata and output plans
→ partition discovery, direct assignment, and offset resolution
→ Kafka poller and source-byte admission control
→ bounded JSONata workers
→ per-partition completion ordering
→ one output writer
```

Plans that neither evaluate JSONata nor explicitly validate JSON bypass the
worker pool. The poller sends pass-through and tombstone completions directly
to the writer.

## Boundaries

```text
src/main.rs                process exit behavior
src/cli.rs                 parsing, validation, config, startup plans
src/app.rs                 process IO, signals, pipeline assembly
src/kafka.rs               assignment, offsets, polling, owned records
src/transform/mod.rs       startup JSONata source plan and validation
src/transform/jsonata.rs   worker-local JSONata execution and actions
src/runtime.rs             poller, workers, writer, shutdown, statistics
src/runtime/state.rs       admission and completion-frontier state
src/output.rs              compiled formats and JSON envelopes
tests/process.rs           Unix process and signal behavior
```

The transform modules call jsonata-core directly through its public parser,
evaluator, context, and value APIs.

## Startup

Before polling, `jkq`:

1. parses and validates the CLI and librdkafka properties;
2. reads an optional `--vars-file`, parses every JSONata expression, and
   validates the strict JSON `$vars` object;
3. stores only expression source and variable JSON in the shared transform
   plan;
4. compiles the output format and its metadata requirements;
5. creates the consumer and discovers all topic partitions when none were
   selected;
6. fetches watermarks only for ranges that need them, resolves partition
   starts and ends, and captures snapshot highs in that same watermark pass;
7. assigns partitions directly;
8. installs the bounded pipeline.

A startup failure cannot produce partial record output. `--check` exits after
local plan validation and does not create a consumer.

The Kafka adapter calls `assign`, never `subscribe`. Automatic commits and
offset storage are disabled. The poller is the only thread that calls the
consumer, including pause and resume. Automatic partition discovery is a
startup snapshot; partitions added later are not assigned to the running
process.

## Record Ownership

librdkafka messages are borrowed. The poller copies the payload and only the
source metadata required by the compiled output plan, then releases the
borrowed message. It never mutates librdkafka-owned memory.

Each admitted input gets a dense local partition sequence. Kafka offsets remain
source metadata; the local sequence drives completion ordering even when
offsets are sparse.

An action is separate from its output representation:

```text
Drop
Tombstone
PassThrough(exact source bytes | compact JSON bytes plus source length)
Project(compact JSON bytes)
```

Every admitted record produces one completion, including drops and fatal
transform results. This lets the partition completion frontier advance and
releases source-byte accounting exactly once.

## JSONata Execution

The startup plan contains `String` expression sources and optional variable
JSON, all safe to share across worker threads. Each worker parses its own
JSONata ASTs and `$vars` value because jsonata-core values use `Rc` and are not
`Send` or `Sync`.

For a non-tombstone input, the worker validates UTF-8 and parses the payload
once with `JValue::from_json_str`. The same worker-local document is used for
all drop predicates, tombstone predicates, and the optional projection.
Original payload bytes remain untouched for an eventual pass action or the
invalid-JSON `pass` policy. With `--envelope-payload value`, a surviving pass
instead serializes the existing parsed document once, retains the source byte
length for envelope metadata, and releases the source buffer. The writer never
parses payload JSON.

jsonata-core's `Evaluator` retains its first parent/root value. jkq therefore
does not reuse evaluators: it creates a fresh `Context` and `Evaluator` for
every expression evaluation and binds the worker-local `$vars` value into that
context. This prevents a root document, assignment, lambda, or other context
state from leaking between expressions or input records.

Predicates run in command-line order and must return a Boolean. Projection
results are checked recursively for `Undefined` and non-JSON internal values,
then serialized through `JValue::to_json_string`. jsonata-core result sequences
remain one value and therefore one jkq output record.

jsonata-core 2.2.7 does not expose its bytecode compiler as a stable production
Rust API. Workers therefore use the public AST evaluator. jkq does not use the
feature-gated internal `_bench` facade.

Existing Kafka tombstones bypass all JSONata work. An identity transform also
bypasses parsing unless `--on-invalid-json` was supplied explicitly or a
JSON-value envelope was requested.

## Ordering and Output

JSONata workers may process records from the same partition concurrently.
Completions enter a per-partition frontier:

```text
next sequence → write or drop → release charge → advance
```

Ahead-of-frontier completions wait in a `BTreeMap`. Once a gap closes, the
writer drains the contiguous range. This preserves source order within each
partition but imposes no order across partitions. `--unordered` writes
completion arrival order instead.

One writer owns stdout. Formats and JSON envelopes stream directly to its
buffer, avoiding byte interleaving and an additional record-sized staging
buffer. `%s`, `%S`, `%R`, envelopes, and action names operate on the
post-transform action. Broken pipe is normal pipeline termination.
For a JSON-value envelope, the writer inserts compact JSON bytes produced by
the worker or projection and labels them with `payloadEncoding: "json"`.

## Backpressure and Memory Accounting

Admission tracks global records, source bytes, and records per partition. The
byte charge covers owned bytes copied from the source record:

- payload;
- key, when required;
- header names and header values, when required.

The charge intentionally excludes jsonata-core's parsed value tree,
evaluation intermediates, projected output, and compact pass output for a
JSON-value envelope. Full JSONata can construct data-dependent values, so total
evaluator and output memory cannot be bounded by a static expression compiler.
Bounded channels, `--max-inflight-records`, `--max-inflight-per-partition`, and
the owned source-byte admission budget still bound queued source work.

Charges are released only after ordered write or drop. Slow output therefore
propagates pressure back to Kafka, and the reorder buffer cannot hold more
records than admission permits.

Global pressure pauses all assigned partitions. Per-partition record pressure
pauses only that partition. Resume uses a 75% low-water threshold to avoid
thrashing. The poller continues serving Kafka events while paused.

Because a source record's size is known only after polling, at most one owned
record may wait outside admitted accounting. A source record larger than the
byte budget is admitted only when no other admitted bytes remain, preventing
deadlock while preserving the runs-alone behavior.

## Termination and Failure

Fixed ranges use exclusive end offsets. Snapshot boundaries are captured once
and never extended. Completion means that the poller has stopped admitting the
range and every admitted partition sequence has crossed its frontier.

Global counts stop all admission after the configured number of input records.
Per-partition counts mark each partition complete independently after its
limit; already admitted records still cross the normal completion frontier.

The first fatal error wins. It triggers shared cancellation, closes the work
path, and drains retained work. The ordered writer emits preceding in-order
records, emits nothing after the first fatal result, and releases accounting
for every completion. Worker and writer panics become pipeline failures rather
than leaving another stage blocked.

The first termination signal stops admission and drains. signal-hook arms the
second signal for immediate process exit, which also handles a worker or
stdout that cannot make progress.

## Invariants

- One invocation consumes one topic and directly assigns either explicitly
  selected partitions or every partition discovered at startup.
- JSONata is the only expression language.
- Every successfully transformed non-tombstone input resolves to exactly one
  action.
- One input never expands into multiple output records.
- Existing tombstones bypass JSON and JSONata and remain tombstones.
- Pass-through preserves exact source payload bytes unless the user explicitly
  requests a JSON-value envelope.
- Ordering is per partition, never global.
- Count limits apply to admitted input, not emitted output.
- Per-partition count limits apply independently to each assigned partition.
- Tombstones, empty payloads, and JSON `null` remain distinct.
- Channels, admitted record counts, per-partition records, and owned source
  bytes are bounded; JSONata intermediates, projected output, and compact
  JSON-value envelope output are not covered by the byte budget.
- stdout is record data; diagnostics and statistics use stderr.
- Errors follow explicit policy and are never silently successful.
