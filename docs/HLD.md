# High-Level Design

## 1. Context

The system consumes explicitly assigned Kafka partitions, transforms JSON values, and writes a byte stream to stdout.

The main design tension is:

- Kafka records should be consumed and copied promptly so librdkafka buffers are released;
- JSON evaluation is CPU-intensive and should run in parallel;
- output must remain ordered within each partition;
- stdout can be slower than Kafka and compute;
- memory must remain bounded for large and highly variable payloads.

The design uses a poller, a fixed compute pool, an ordered completion stage, and a writer. Bounded channels and a shared byte budget connect the stages.

## 2. System Boundary

```text
Kafka brokers
    │
    ▼
librdkafka / rust-rdkafka
    │
    ▼
poll and admission
    │
    ▼
bounded work queue
    │
    ├───────────────┐
    ▼               ▼
compute worker ... compute worker
    │               │
    └───────┬───────┘
            ▼
bounded completion queue
            ▼
partition order restoration
            ▼
output formatter / JSON envelope encoder
            ▼
buffered stdout
```

stderr receives diagnostics and optional statistics from a centralized reporting path.

## 3. Major Components

### 3.1 CLI and configuration

Responsibilities:

- parse command-line options;
- load configuration files;
- apply precedence;
- validate incompatible options;
- compile format strings;
- parse and compile transform expressions;
- build a fully resolved immutable runtime configuration.

Consumption must not start until this stage succeeds.

### 3.2 Kafka consumer adapter

Responsibilities:

- construct the `BaseConsumer`;
- assign the selected topic partitions;
- resolve start and end offsets;
- capture snapshot high watermarks;
- poll messages and Kafka events;
- expose pause/resume by partition;
- report EOF and Kafka errors;
- copy only required record fields into owned work items.

The adapter does not evaluate JSON or format output.

### 3.3 Admission controller

Responsibilities:

- assign a monotonically increasing local sequence per partition;
- enforce global record and byte budgets;
- enforce per-partition pending limits;
- stop admission at count and end boundaries;
- pause and resume partitions;
- keep Kafka polling active while downstream stages are congested.

The admission controller tracks retained bytes until ordered completion releases them.

### 3.4 Transform compiler

Responsibilities:

- parse repeated drop predicates, tombstone predicates, and the optional projection;
- produce one immutable transform program;
- collect referenced JSON paths;
- perform type-independent validation;
- produce backend-friendly extraction and evaluation plans;
- calculate whether exact original payload bytes must be preserved.

### 3.5 JSON backend

The first implementation uses `simd-json`'s non-recursive tape behind a narrow internal interface.

Responsibilities:

- prepare mutable input safely;
- parse or scan JSON;
- resolve referenced paths;
- evaluate compiled operations;
- serialize projected JSON;
- return one explicit action.

The backend rejects JSON deeper than 128 container levels before evaluation. This
keeps recursive projection serialization within a controlled stack bound; the
configured invalid-JSON policy handles the record.

The interface must not expose `simd-json` value types outside the backend module.

### 3.6 Compute workers

Each long-lived worker keeps backend execution state local. The initial backend reuses
simd-json parser buffers, tape storage, and the original-preserving parse buffer across
records. Parser state is discarded after an input or tape allocation exceeds 8 MiB so
one exceptional record does not permanently inflate a worker. Projection output remains
record-owned because it moves to the writer.

Workers:

1. receive an owned input record;
2. handle input tombstones without parsing;
3. execute the transform plan;
4. apply configured error policy;
5. send a completion carrying the partition sequence and action result.

### 3.7 Partition order restorer

For each partition, maintain:

- the next sequence expected;
- a map of out-of-order completions;
- completion and byte-release state;
- terminal boundary state.

When a completion arrives:

1. emit it immediately if it matches the frontier;
2. store it only if its sequence is ahead;
3. repeatedly drain newly contiguous completions;
4. release admission budget for every drained input, including drops;
5. mark a partition complete when its terminal boundary has drained.

No ordering relation is imposed between different partitions.

### 3.8 Output encoder and writer

Responsibilities:

- map actions to a post-transform payload;
- compile source metadata and transformed data into `-f` or `-J` output;
- write with a locked buffered stdout;
- flush according to normal, unbuffered, shutdown, or error behavior;
- recognize broken pipe.

The writer is single-threaded to prevent byte interleaving.

## 4. Runtime Topology

The default runtime uses operating-system threads:

- one poll/admission thread;
- `N` compute workers;
- one completion/order/write thread.

When the compiled plan does not parse JSON, the poller sends pass-through and tombstone
completions directly to the writer. Compute workers and order restoration are skipped;
the single poller already observes each partition in source order.

A dedicated reporting thread is not required initially. Periodic statistics can be emitted from the poll loop or writer based on shared atomics and snapshots.

An asynchronous runtime is not used because:

- Kafka polling is already backed by librdkafka threads;
- JSON work is CPU-bound;
- stdout is blocking;
- bounded channels and dedicated threads model the pipeline directly;
- async CPU work would still require a separate blocking or compute pool.

## 5. Data Model

### 5.1 Owned input record

The poller creates an owned record containing:

- topic identity;
- partition;
- source offset;
- per-partition sequence;
- timestamp;
- key when required;
- headers when required;
- nullable value bytes;
- original retained-byte charge.

Only fields required by the compiled output mode are copied. Partition and offset are always retained.

### 5.2 Action

```text
Drop
Tombstone
PassThrough
Project
```

The action is distinct from output encoding.

### 5.3 Post-transform payload

```text
Null
Bytes
```

Mapping:

| Action | Output |
|---|---|
| Drop | no formatter invocation |
| Tombstone | null payload |
| PassThrough | original value bytes |
| Project | projected JSON bytes |

### 5.4 Completion

A completion contains:

- partition;
- sequence;
- source offset;
- action or fatal processing error;
- metadata needed for output;
- retained-byte charge.

A dropped record still produces a completion.

## 6. Main Data Flows

### 6.1 Startup

```text
parse CLI
→ load config
→ validate
→ compile expressions
→ compile output format
→ create consumer
→ resolve offsets
→ capture snapshot boundaries
→ start workers and writer
→ begin polling
```

Any failure before polling exits without partial record output.

### 6.2 Normal record

```text
poll borrowed message
→ verify partition boundary and count
→ reserve count and byte budget
→ copy required fields
→ assign partition sequence
→ enqueue
→ worker parses and evaluates
→ completion arrives
→ order restorer drains contiguous records
→ writer encodes and writes
→ budget released
```

### 6.3 Existing tombstone

```text
poll tombstone
→ reserve/admit
→ assign sequence
→ worker or fast path returns Tombstone
→ ordered output
```

No JSON buffer is allocated.

### 6.4 Drop

```text
worker returns Drop
→ completion reaches partition frontier
→ writer emits nothing
→ frontier advances
→ budget released
```

### 6.5 Snapshot completion

```text
captured boundary reached by poller
→ stop admitting partition
→ wait for its admitted sequences
→ frontier drains through last admitted record
→ mark partition complete
→ exit after all partitions complete
```

### 6.6 Downstream backpressure

```text
work/completion/output pressure rises
→ byte or record budget reached
→ pause affected partitions
→ continue polling events
→ ordered completions release budget
→ resume below low-water threshold
```

## 7. Ordering Model

### 7.1 Default

Records are emitted in source order within each partition.

For a partition:

```text
offset 100 → offset 101 → offset 102
```

This remains true when:

- workers complete out of order;
- offset 101 is dropped;
- offset 101 becomes a tombstone;
- recoverable errors are converted by policy.

Across partitions, output may interleave in any order:

```text
p0:100, p1:50, p1:51, p0:101
```

### 7.2 Unordered mode

When explicitly selected, completed records are sent directly to the writer. No ordering guarantee is provided, including within a partition.

Unordered mode still preserves record byte integrity and bounded memory.

## 8. Backpressure Model

### 8.1 Limits

At minimum:

- global in-flight records;
- global retained payload bytes;
- per-partition in-flight records;
- worker queue capacity;
- completion queue capacity.

Global retained bytes include records waiting for compute, being processed, waiting for order, and waiting for write.

Admission uses a conservative bound compiled from the transform plan. The charge
includes the owned input, any original-preserving parse copy, and the maximum
serialized projection size. This may admit fewer records than their eventual
actions require, but transformed completions cannot escape the configured byte
budget. Format and envelope encoding stream through the single writer.

### 8.2 Pause strategy

Pause only partitions contributing to pressure when possible.

A simple initial policy is:

- pause a partition when its per-partition limit is reached;
- pause all assigned partitions when the global byte or record limit is reached;
- resume eligible partitions when usage falls below 75% of the triggering limit.

Pause and resume requests are serialized through the poller because it owns the consumer.

### 8.3 Oversized record

A record larger than the configured global byte budget is admitted only when retained payload bytes are zero. While it is in flight, all partitions remain paused.

This prevents deadlock while maintaining bounded concurrency.

## 9. Offset and Termination Model

Each partition has a resolved range:

```text
[start_offset, end_offset)
```

The end may be unbounded until EOF, or fixed by:

- explicit offset;
- end timestamp resolution;
- snapshot high watermark.

The poller does not admit records at or beyond the exclusive end.

Completion is based on admitted work draining, not merely observing Kafka EOF.

Count termination is global. After the count is reached, admission stops for every partition and already admitted work drains.

## 10. Error Propagation

Errors are divided into:

- startup/configuration errors;
- Kafka control-plane errors;
- per-record Kafka errors;
- invalid JSON;
- expression evaluation errors;
- output errors;
- internal invariant violations.

Recoverable per-record errors pass through explicit policies. Fatal errors trigger shared cancellation.

The first fatal error is retained as the process outcome. Secondary shutdown errors may be reported but must not replace the primary cause.

## 11. Dependency Architecture

### Required direct dependencies

- `rdkafka`: Kafka protocol and client behavior;
- `simd-json`: initial JSON backend;
- `clap`: CLI parsing;
- `crossbeam-channel`: bounded multi-thread channels;
- `signal-hook`: signal delivery.

No async runtime is required.

Use minimal feature sets. Prefer a build configuration that bundles or consistently links librdkafka for release portability, subject to target-specific validation.

## 12. Observability

Diagnostics should identify:

- topic and partition;
- source offset where applicable;
- error category;
- selected policy;
- whether the record was dropped, tombstoned, passed, or fatal.

Optional statistics should be aggregatable without locks on every record. Worker-local counters or relaxed atomics are acceptable when exact instantaneous values are not required.

## 13. Security and Secrets

Kafka configuration may include credentials.

Requirements:

- never print raw secret-bearing property values;
- redact known password, token, and private-key properties;
- avoid including complete configuration in panic output;
- do not include payloads in default error messages;
- include source coordinates rather than data when reporting malformed JSON;
- make any future payload preview explicit and bounded.

## 14. Extensibility Boundaries

Deliberate replacement boundaries:

- JSON backend;
- output mode;
- Kafka adapter around rust-rdkafka;
- transform compiler versus evaluator.

Not intended as extension frameworks:

- arbitrary plugin actions;
- custom sinks;
- user-defined language functions;
- runtime-loaded modules;
- generic middleware stages.

New behavior should first prove that it belongs in the product scope.
