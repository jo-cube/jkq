# Architecture

`jkq` is a bounded, threaded pipeline around a directly assigned librdkafka
consumer. The design optimizes the record path while keeping ownership,
ordering, and shutdown visible.

```text
CLI and Kafka properties
→ compiled transform and output plans
→ direct assignment and offset resolution
→ Kafka poller and admission control
→ bounded JSON workers
→ per-partition completion ordering
→ one output writer
```

Plans that do not parse JSON skip the worker pool. The poller sends pass-through
and tombstone completions directly to the writer.

## Boundaries

```text
src/main.rs                process exit behavior
src/cli.rs                 parsing, validation, config, compiled plans
src/app.rs                 process IO, signals, pipeline assembly
src/kafka.rs               assignment, offsets, polling, owned records
src/transform/syntax.rs    lexer and parser
src/transform/compile.rs   path interning and execution plan
src/transform/json.rs      simd-json backend and evaluator
src/runtime.rs             poller, workers, writer, shutdown, statistics
src/runtime/state.rs       admission and completion-frontier state
src/output.rs              compiled formats and JSON envelopes
tests/process.rs           Unix process and signal behavior
```

The modules are intentionally concrete. The JSON backend and output encoding
are the only current replacement boundaries; the pipeline is not a generic
framework.

## Startup

Before polling, `jkq`:

1. parses and validates the CLI and librdkafka properties;
2. parses and compiles all expressions;
3. compiles the output format and its metadata requirements;
4. creates the consumer and resolves partition starts and ends;
5. assigns partitions directly;
6. captures snapshot high watermarks when requested;
7. installs the bounded pipeline.

A startup failure cannot produce partial record output.

The Kafka adapter calls `assign`, never `subscribe`. Automatic commits and
offset storage are disabled. The poller is the only thread that calls the
consumer, including pause and resume.

## Record Ownership

librdkafka messages are borrowed. The poller copies the payload and only the
metadata required by the compiled output plan, then releases the borrowed
message. It never mutates librdkafka-owned memory.

Each admitted input gets a dense local partition sequence. Kafka offsets remain
source metadata; the local sequence drives completion ordering even when
offsets are sparse.

An action is separate from its output representation:

```text
Drop
Tombstone
PassThrough(original bytes)
Project(compact JSON bytes)
```

Every admitted record produces one completion, including drops and fatal
transform results. This lets the partition completion frontier advance and
releases accounting exactly once.

## JSON Execution

The transform compiler parses expressions once, interns identical complete
paths, and records whether original bytes must survive parsing. The backend
uses simd-json's tape rather than an owned JSON tree and resolves only compiled
paths.

Each worker owns its parser, tape, and scratch buffers, so evaluation needs no
shared lock. Worker storage is reused across records and discarded after an
input or tape allocation exceeds 8 MiB, preventing one exceptional record from
permanently growing every worker.

When pass-through remains possible, the source payload stays unchanged and a
worker-local copy is parsed. When every successful record is projected and no
error policy needs the original, the owned payload can be parsed in place.

The backend validates container ranges iteratively and rejects nesting beyond
128 levels through the invalid-JSON policy. Projection serialization is
recursive within that bound and moves its completed `Vec<u8>` to the writer.

The current plan performs independent tape lookups for distinct paths. A
shared-prefix trie is deliberately absent until representative profiling shows
it improves real workloads.

## Ordering and Output

JSON workers may process records from the same partition concurrently.
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
buffer. Broken pipe is treated as normal pipeline termination.

## Backpressure

Admission tracks global records, retained bytes, and records per partition.
The retained-byte charge conservatively covers:

- the owned payload;
- an original-preserving parse copy when required;
- the maximum projected output implied by the expression;
- copied keys, header names, and header values.

Charges are released only after ordered write or drop. Slow output therefore
propagates pressure back to Kafka, and the reorder buffer cannot grow beyond
admission limits.

Global pressure pauses all selected partitions. Per-partition record pressure
pauses only that partition. Resume uses a 75% low-water threshold to avoid
thrashing. The poller continues serving Kafka events while paused.

Because a record's size is known only after polling, at most one owned record
may wait outside admitted accounting. A record larger than the byte budget is
admitted only when no other admitted bytes remain, preventing deadlock while
keeping oversized work single-file.

## Termination and Failure

Fixed ranges use exclusive end offsets. Snapshot boundaries are captured once
and never extended. Completion means that the poller has stopped admitting the
range and every admitted sequence has crossed its frontier.

The first fatal error wins. It triggers shared cancellation, closes the work
path, and drains or releases retained work where safe. Worker and writer panics
are converted to pipeline failures rather than leaving another stage blocked.

The first termination signal stops admission and drains. signal-hook arms the
second signal for immediate process exit, which also handles a worker or stdout
that cannot make progress.

## Invariants

- One invocation consumes one topic and explicitly selected partitions.
- Every non-tombstone input resolves to exactly one action.
- One input never expands into multiple output records.
- Existing tombstones bypass JSON and remain tombstones.
- Pass-through preserves exact source payload bytes.
- Ordering is per partition, never global.
- Count limits apply to admitted input, not emitted output.
- Tombstones, empty payloads, and JSON `null` remain distinct.
- Queues, admitted records, and retained bytes are bounded.
- stdout is record data; diagnostics and statistics use stderr.
- Errors follow explicit policy and are never silently successful.
