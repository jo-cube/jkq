# Low-Level Design

## 1. Proposed Source Layout

```text
Cargo.toml
Cargo.lock
src/
  main.rs
  app.rs
  cli.rs
  config.rs
  error.rs
  signal.rs
  kafka/
    mod.rs
    consumer.rs
    offsets.rs
    snapshot.rs
    record.rs
  transform/
    mod.rs
    action.rs
    ast.rs
    lexer.rs
    parser.rs
    compile.rs
    plan.rs
    value.rs
  json/
    mod.rs
    backend.rs
    simd_backend.rs
    extract.rs
    project.rs
  runtime/
    mod.rs
    admission.rs
    worker.rs
    completion.rs
    ordering.rs
    shutdown.rs
    stats.rs
  output/
    mod.rs
    format.rs
    envelope.rs
    writer.rs
tests/
  cli/
  kafka/
  fixtures/
  support/
benches/
  transform.rs
  formatter.rs
  pipeline.rs
docs/
```

This is a target shape, not a requirement to create every file immediately. Start cohesive and split only when a module has a clear responsibility.

Kafka assignment, offset resolution, snapshot state, owned-record conversion, and pause/resume remain cohesive in `src/kafka.rs`. Stage 3 adds thread orchestration in `src/runtime.rs` and keeps admission plus completion-frontier state in `src/runtime/state.rs`. Further splitting would create wrappers until one of the remaining runtime responsibilities grows independently.

## 2. Application Assembly

`main.rs` should remain small:

1. parse process arguments;
2. call the application entry point;
3. map the result to an exit code;
4. suppress normal broken-pipe diagnostics.

`app.rs` owns startup sequencing, signal installation, final statistics, and runtime assembly. It does not contain parsing, Kafka polling, evaluator, or ordering logic.

## 3. Resolved Configuration

Command-line parsing should produce a raw structure. Validation and configuration loading should produce an immutable resolved structure.

```rust
struct RuntimeConfig {
    brokers: Vec<String>,
    topic: String,
    partitions: Vec<PartitionSpec>,
    count_limit: Option<u64>,
    exit_at_end: bool,
    snapshot: bool,
    jobs: usize,
    unordered: bool,
    limits: RuntimeLimits,
    transform: TransformPlan,
    output: OutputPlan,
    errors: ErrorPolicies,
    kafka_properties: BTreeMap<String, String>,
    flush_mode: FlushMode,
}
```

Dedicated CLI options override equivalent arbitrary Kafka properties only when explicitly documented. Conflicting duplicated ownership should be rejected rather than silently ambiguous.

## 4. Kafka Types

The initial consumer uses `rdkafka::consumer::BaseConsumer` and calls `assign`; it never calls `subscribe`. Client construction forces automatic commit and automatic offset storage off and partition EOF events on. The default `group.id` exists only because librdkafka requires one for the consumer type.

Offset resolution uses broker watermarks and `offsets_for_times` before assignment. Missing timestamp matches resolve to the current high watermark. Relative-to-end starts clamp at the low watermark. Snapshot mode performs a fresh high-watermark query after start resolution and stores that value as the immutable exclusive boundary.

### 4.1 Partition request

```rust
struct PartitionSpec {
    id: i32,
    start: StartPosition,
    end: Option<EndPosition>,
}
```

When one start option applies to all selected partitions, represent it once in raw CLI configuration and expand it during resolution.

### 4.2 Start position

```rust
enum StartPosition {
    Beginning,
    End,
    Absolute(i64),
    RelativeToEnd(u64),
    TimestampMillis(i64),
}
```

### 4.3 End position

```rust
enum EndPosition {
    ExclusiveOffset(i64),
    TimestampMillis(i64),
    Snapshot,
    CurrentEof,
}
```

Resolved execution uses only offsets:

```rust
struct ResolvedPartition {
    id: i32,
    start_offset: i64,
    end_offset_exclusive: Option<i64>,
}
```

### 4.4 Record requirements

Compile output and transform needs into one copy plan:

```rust
struct RecordRequirements {
    key: bool,
    headers: bool,
    timestamp: bool,
    topic: bool,
    original_payload: OriginalPayloadNeed,
}
```

`original_payload` captures whether exact source bytes must survive parsing.

## 5. Owned Record

```rust
struct InputRecord {
    partition: i32,
    sequence: u64,
    offset: i64,
    timestamp: Timestamp,
    key: Option<Vec<u8>>,
    headers: Vec<Header>,
    payload: Option<Vec<u8>>,
    retained_bytes: usize,
}
```

The topic string should be shared through an `Arc<str>` or retained in immutable runtime state rather than copied per record.

Header values remain nullable byte vectors.

```rust
struct Header {
    name: String,
    value: Option<Vec<u8>>,
}
```

Only allocate headers when required.

The retained charge includes required key bytes, required header names and values,
and a conservative payload peak compiled from the transform plan. The payload
peak covers the owned input, an original-preserving parse copy when required, and
the maximum serialized projection size. Topic and fixed-size source coordinates
are not charged. A message whose charge cannot yet be admitted is the sole pending
poll candidate while all partitions are paused; its owned copy is bounded by
librdkafka's configured message limit but is not added to the admitted-byte
counter until capacity is available.

## 6. Action and Completion Types

```rust
enum Action {
    Drop,
    Tombstone,
    PassThrough,
    Project(Vec<u8>),
}
```

Workers consume `InputRecord` by value. A successful completion owns whatever is needed by the writer:

```rust
struct Completion {
    partition: i32,
    sequence: u64,
    offset: i64,
    retained_bytes: usize,
    outcome: CompletionOutcome,
}
```

```rust
enum CompletionOutcome {
    Drop,
    Emit(OutputRecord),
    Fatal(ProcessingError),
}
```

```rust
struct OutputRecord {
    metadata: RecordMetadata,
    payload: OutputPayload,
    action: EmittedAction,
}
```

```rust
enum OutputPayload {
    Tombstone,
    Bytes(Vec<u8>),
}
```

Pass-through moves the original payload vector into `Bytes`. Projection moves the projection vector. This avoids an additional copy between worker and writer.

Input tombstones become `OutputPayload::Tombstone`.

## 7. Transform Program

### 7.1 Abstract syntax

```rust
struct Program {
    drop_predicates: Vec<Expr>,
    tombstone_predicates: Vec<Expr>,
    projection: Option<Expr>,
}
```

Core expression nodes:

```rust
enum Expr {
    Literal(Literal),
    Path(Path),
    Array(Vec<Expr>),
    Object(Vec<ObjectField>),
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    Call {
        function: Function,
        arguments: Vec<Expr>,
    },
}
```

Projection field order is source expression order and should be preserved in serialized output.

### 7.2 Paths

```rust
struct Path {
    segments: Vec<PathSegment>,
}
```

```rust
enum PathSegment {
    Field(String),
    Index(usize),
}
```

No wildcard, iterator, slice, or recursive path segment is supported initially.

### 7.3 Compiled plan

The compiler should:

1. intern equal paths;
2. assign each path a slot;
3. build a path trie;
4. replace path expressions with slot references;
5. validate function arity;
6. precompute constant subexpressions where safe;
7. determine original-byte preservation needs.

```rust
struct TransformPlan {
    paths: PathPlan,
    drops: Vec<CompiledExpr>,
    tombstones: Vec<CompiledExpr>,
    projection: Option<CompiledExpr>,
    capabilities: PlanCapabilities,
    payload_budget: PayloadBudget,
}
```

```rust
struct PlanCapabilities {
    parses_json: bool,
    can_pass_through: bool,
    requires_original_on_error: bool,
    requires_original_bytes: bool,
}
```

### 7.4 Evaluation values

Keep the internal value model narrow:

```rust
enum EvalValue<'a> {
    Missing,
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(Cow<'a, str>),
    Array(ArrayRef<'a>),
    Object(ObjectRef<'a>),
}
```

Backend-specific references must not leak beyond the JSON module. If this lifetime shape becomes cumbersome, use backend-owned slots and expose accessor methods rather than general borrowed containers.

## 8. Lexer

The handwritten lexer should produce positioned tokens.

```rust
struct Token {
    kind: TokenKind,
    span: Span,
}
```

Token categories:

- punctuation: `.`, `[`, `]`, `{`, `}`, `(`, `)`, `,`, `:`;
- operators: `==`, `!=`, `<`, `<=`, `>`, `>=`;
- keywords: `and`, `or`, `not`, `true`, `false`, `null`;
- identifier;
- string;
- integer;
- decimal;
- end of input.

Requirements:

- UTF-8 input;
- JSON-compatible quoted string escapes;
- useful byte-offset spans;
- rejection of malformed escape sequences;
- no implicit comments initially;
- no locale-dependent number parsing.

## 9. Pratt Parser

Suggested binding powers, lowest to highest:

1. `or`
2. `and`
3. comparisons
4. unary `not`
5. primary expressions

Comparisons are non-associative. Reject chained comparisons such as:

```text
.a < .b < .c
```

Primary expressions include literals, paths, calls, parenthesized expressions, arrays, and objects.

Object keys are identifiers or quoted strings.

Parser errors should contain:

- expression category;
- byte span;
- expected token or construct;
- encountered token;
- concise source excerpt with caret when possible.

## 10. Expression Compilation

### 10.1 Predicate validation

Drop and tombstone expressions must evaluate to boolean. Because path types are dynamic, this is partly runtime validation.

Literal non-boolean predicates can be rejected at compile time.

### 10.2 Projection validation

Any expression type is permitted. The serializer must support all runtime values except `Missing`.

A `Missing` projection result is an evaluation error. A missing object field value should also be an evaluation error initially; implicit omission is too easy to misunderstand. Users can express defaults through a future explicit function if added.

### 10.3 Short circuiting

- `and` does not evaluate the right side when the left side is false.
- `or` does not evaluate the right side when the left side is true.
- drop and tombstone predicate lists stop at the first true predicate.

This is both semantic and performance-sensitive.

## 11. JSON Backend Interface

Keep the initial interface internal and shaped by actual use.

```rust
trait JsonBackend: Send {
    fn execute(
        &mut self,
        plan: &TransformPlan,
        record: InputRecord,
        policies: &ErrorPolicies,
    ) -> Result<CompletionOutcome, ProcessingError>;
}
```

A worker owns one backend instance, avoiding synchronization.

A more granular interface may be used internally:

```rust
trait ParsedDocument {
    fn slot(&self, slot: PathSlot) -> BackendValue<'_>;
}
```

The public transform layer should not know whether slots come from a tape, lazy view, or custom scanner.

## 12. `simd-json` Execution

### 12.1 Safe mutable input

Never obtain mutable access to librdkafka-owned bytes.

The poller copies payload bytes into an owned vector before enqueueing.

Two execution paths are useful:

#### Preserve-original path

Required when pass-through or invalid-JSON pass policy is possible.

```text
original Vec<u8>
→ copy into a reusable worker-local parse buffer
→ parse mutable buffer
→ retain original unchanged
```

#### Consume-in-place path

Allowed when:

- a projection is guaranteed for every valid non-terminal record;
- no pass-through error policy is configured;
- no output mode needs pre-transform payload bytes.

```text
owned payload Vec<u8>
→ parse in place
→ reuse allocation where possible
```

Choose the path through a compile-time capability flag, not per-record guesswork.
Each worker also reuses simd-json's parser buffers and tape allocation. Inputs or tape
allocations above 8 MiB are processed normally and then discarded rather than retained.

### 12.2 Path extraction

The first implementation uses simd-json's tape representation and resolves only
compiled paths. It does not create an owned source `Value` tree. A local release-mode
probe on Rust 1.97 and Apple ARM64 found tape materially faster for one to five early
or late fields; twenty late independent lookups were slower because tape objects are
scanned linearly. A shared-prefix trie remains deferred until representative
benchmarks show that case matters.

The initial compiler deduplicates identical complete paths. A later path trie may
also deduplicate shared prefixes such as:

```text
.customer.id
.customer.status
```

so they become one traversal into `.customer`.

Add that trie only when the selected backend can exploit it and benchmarks show
the independent traversals matter.

The backend accepts at most 128 nested JSON containers. It validates tape node
ranges iteratively before evaluation; exceeding the limit is an invalid-JSON
condition and follows the selected policy.

### 12.3 Projection serialization

Serialize into a record-owned `Vec<u8>`.

Requirements:

- compact output;
- deterministic object field order from the projection;
- valid escaping;
- no trailing newline;
- distinguish projected `null` from tombstone.

Move the completed vector to the output record. A future worker-local pool must cap
retained capacity and requires benchmark evidence.

## 13. Admission Controller

### 13.1 State

```rust
struct AdmissionState {
    total_records: usize,
    total_bytes: usize,
    per_partition: HashMap<i32, PartitionAdmission>,
    limits: RuntimeLimits,
}
```

```rust
struct PartitionAdmission {
    next_sequence: u64,
    in_flight: usize,
    paused: bool,
    stopped: bool,
}
```

### 13.2 Reservation

After polling a payload:

1. copy only required fields and calculate their retained-byte charge;
2. check global and partition limits;
3. if unavailable, keep this as the sole pending candidate, pause all partitions, and poll control events;
4. reserve counters when capacity becomes available;
5. enqueue work.

If enqueue fails due to shutdown, roll back the reservation.

One record larger than the byte budget is admitted only when no other admitted bytes remain. The implementation holds at most one already-polled, not-yet-admitted candidate. Kafka reveals the next record size only after polling, so this copy is outside the admitted-byte counter until it can reserve capacity. No second candidate is accepted while byte pressure is active.

### 13.3 Release

Budget is released only when a completion drains through the ordering frontier or is emitted immediately in unordered mode. This accounts for reorder retention and slow stdout.

### 13.4 Low-water resume

A paused partition becomes eligible when:

- its in-flight count is below 75% of the per-partition maximum; and
- global records and bytes are below 75% of their maxima; and
- it has not reached an end boundary.

All consumer pause/resume calls occur on the poll thread.

## 14. Worker Pool

Use a bounded `crossbeam-channel`.

Work distribution may use cloned receivers. Worker selection does not need partition affinity because ordering is restored later and parallelism within a partition is desired.

Each worker loop:

1. receive work or shutdown;
2. execute action;
3. update local counters;
4. send completion;
5. terminate when the work channel closes or cancellation is fatal.

If the compiled plan does not parse JSON, do not start compute workers. The poller moves
the owned payload directly into a completion, preserving tombstones and exact source
bytes while retaining the same admission and writer backpressure.

Worker panics must not silently hang the pipeline. Join failures become internal fatal errors.

## 15. Ordering Algorithm

### 15.1 State

```rust
struct PartitionOrderState {
    next_sequence: u64,
    pending: BTreeMap<u64, Completion>,
    admitted_end_sequence: Option<u64>,
}
```

A `BTreeMap` stores only genuine out-of-order gaps. An in-order completion advances the
frontier directly without entering the map. Since sequences are dense, a deque-based gap
buffer may be benchmarked later.

### 15.2 Insert and drain

```text
if completion matches next_sequence:
    emit it directly
else:
    insert completion by sequence
while pending contains next_sequence:
    remove completion
    handle outcome
    release admission reservation
    next_sequence += 1
```

A duplicate or sequence lower than the frontier is an internal invariant violation.

### 15.3 Fairness across partitions

The completion thread handles arrivals in channel order. After each insertion, it drains only the affected partition's contiguous range. To avoid one partition monopolizing the writer after a very large gap closes, optionally cap the number drained before checking the completion channel again.

This fairness cap must not reorder records within a partition.

## 16. Output Formatter

### 16.1 Compiled tokens

```rust
enum FormatToken {
    Literal(Vec<u8>),
    Offset,
    Key,
    KeyLength,
    Payload,
    PayloadLength,
    PayloadLengthBinary,
    Topic,
    Partition,
    Timestamp,
    Headers,
}
```

Compile once at startup.

### 16.2 Literal escape rules

Support:

- `\n`
- `\r`
- `\t`
- `\\`
- `\xNN`
- numeric escape compatibility only when deliberately specified and tested

Unknown escapes should be rejected rather than silently altered, except where compatibility testing requires a known behavior.

### 16.3 Null semantics

For key:

- `%k`: zero bytes for null unless a future explicit null token option is introduced;
- `%K`: `-1` for null and `0` for empty.

For post-transform payload:

- `%s`: zero bytes for tombstone and empty payload;
- `%S`: `-1` for tombstone, otherwise decimal byte length;
- `%R`: signed 32-bit big-endian length, `-1` for tombstone.

Reject payloads larger than `i32::MAX` when `%R` is used, with a precise output error.

### 16.4 Headers

Render headers as comma-separated `name=value` pairs:

- null value: `name=NULL`;
- empty value: `name=`;
- non-empty value: raw bytes.

This format is not binary-safe. Users requiring binary-safe headers should use JSON envelope output after its encoding is frozen.

## 17. JSON Envelope

The initial schema is compact, newline-terminated, and emitted in the following field order:

```json
{
  "topic": "events",
  "partition": 3,
  "offset": 42,
  "timestamp": 1720000000000,
  "timestampType": "createTime",
  "key": "abc",
  "keyEncoding": "utf8",
  "keyLength": 3,
  "headers": [
    {
      "name": "trace-id",
      "value": "abc",
      "valueEncoding": "utf8",
      "valueLength": 3
    }
  ],
  "action": "project",
  "payload": "{\"id\":1}",
  "payloadEncoding": "utf8",
  "payloadLength": 8
}
```

For keys, header values, and payloads, valid UTF-8 is represented directly with encoding `"utf8"`; other bytes use RFC 4648 base64 with encoding `"base64"`. Null bytes use JSON `null`, a null encoding, and length `-1`. Header objects contain `name`, `value`, `valueEncoding`, and `valueLength` in source order.

Payloads are strings rather than embedded JSON. This preserves exact pass-through bytes, supports invalid-JSON pass policy, and avoids changing JSON whitespace, numeric spelling, or object order. Tombstones use `payload: null`, `payloadEncoding: null`, `payloadLength: -1`, and action `"tombstone"`. JSON text `null` is the UTF-8 string `"null"` with length `4`.

An absent timestamp uses null for both `timestamp` and `timestampType`. Available types are `"createTime"` and `"logAppendTime"`. Golden tests freeze binary encoding, null representation, field order, and the trailing newline. The envelope is streamed directly to the writer.

## 18. Error Policies

```rust
struct ErrorPolicies {
    invalid_json: InvalidJsonPolicy,
    evaluation: EvaluationPolicy,
    kafka_record: KafkaRecordPolicy,
}
```

Policy conversion occurs in workers for JSON/evaluation errors and in the poller for Kafka record errors.

When invalid JSON uses `pass`, the worker returns exact original bytes without projection.

Error counters increment even when policy converts the error into a non-fatal action.

## 19. Cancellation and Shutdown

Use a shared atomic cancellation state plus channel closure.

Suggested states:

```rust
enum ShutdownState {
    Running,
    Draining,
    Forced,
}
```

First signal:

- set draining;
- stop new admission;
- close work producer after poller stops;
- workers finish queued work;
- completion writer drains;
- flush stdout.

Second signal:

- set forced;
- stop waiting for drain;
- best-effort flush;
- exit with signal-related status.

The implementation uses signal-hook's conditional shutdown handler: the first `SIGINT` or `SIGTERM` arms forced termination and starts the normal drain; another termination signal exits immediately even when a worker or stdout is blocked.

A fatal error follows the draining path where safe but retains a failure exit code.

## 20. Exit Codes

Recommended initial mapping:

- `0`: success, including normal broken pipe;
- `1`: runtime failure;
- `2`: command-line, configuration, or expression error;
- `130`: interrupted by SIGINT before graceful completion;
- `143`: terminated by SIGTERM before graceful completion.

If graceful first-signal drain completes, choose either `0` or the signal code and document it consistently. Prefer signal codes because the requested range was not fully consumed.

## 21. Version Information

Embed the release tag or build version at compile time.

`--version` should print:

- executable version;
- target triple when practical;
- librdkafka version;
- optional git revision for development builds.

Do not expose unstable internal feature details in a format that becomes a compatibility burden.

## 22. Internal Assertions and Invariants

Use debug assertions for developer invariants and runtime errors for states reachable through external input.

Important invariants:

- one sequence assigned exactly once;
- one completion per admitted record;
- retained byte charge released exactly once;
- partition frontier never decreases;
- no output at or beyond an exclusive end;
- count admission never exceeds the configured limit;
- only the poll thread calls consumer pause/resume;
- only the writer thread writes stdout.
