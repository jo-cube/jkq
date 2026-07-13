# Testing and Benchmarking Strategy

## 1. Objectives

Testing should establish:

- expression correctness;
- byte-exact format compatibility;
- record-action semantics;
- partition ordering;
- offset and termination correctness;
- bounded-memory behavior;
- predictable failure and shutdown behavior;
- release artifact usability.

Benchmarks should answer performance questions without being mistaken for functional tests.

## 2. Test Layers

### 2.1 Pure unit tests

Use for:

- lexer tokenization and spans;
- parser precedence and errors;
- path interning and trie construction;
- expression compilation;
- evaluator semantics;
- format-string compilation;
- size parsing;
- offset syntax parsing;
- configuration precedence;
- action-to-payload mapping.

These tests should be fast and run in normal CI.

### 2.2 Golden byte tests

Use checked-in expected byte files or explicit byte arrays for:

- `%R` binary framing;
- tombstone versus empty value;
- key null lengths;
- literal escapes;
- headers;
- format combinations;
- JSON envelope schema;
- projected escaping;
- exact pass-through bytes.

Golden updates require review. Do not automatically regenerate expected output in normal tests.

### 2.3 Concurrency tests

Use controlled fake workers or injected executors to force completion order.

Required cases:

- same-partition completions arrive in reverse order;
- one sequence is dropped;
- one sequence becomes a tombstone;
- a recoverable evaluation error becomes drop;
- different partitions progress independently;
- a large gap closes and drains correctly;
- unordered mode bypasses restoration;
- cancellation while records are pending;
- byte charge released exactly once.

Avoid timing-only assertions. Use barriers, channels, and deterministic schedules.

### 2.4 Process integration tests

Spawn the built executable and verify:

- CLI errors and exit codes;
- stdout/stderr separation;
- default formatting;
- broken pipe;
- signal behavior;
- config-file errors;
- invalid expressions;
- invalid option combinations;
- final statistics.

Use short timeouts only to prevent hangs, not as the primary assertion.

### 2.5 Kafka-backed integration tests

Run against a disposable Kafka environment for:

- direct assignment;
- multiple partitions;
- beginning and end;
- absolute and negative offsets;
- timestamp start and end;
- count across partitions;
- EOF termination;
- snapshot while producers append;
- source tombstones;
- keys and headers;
- pause and resume;
- oversized records;
- Kafka error policy.

Keep Kafka setup reusable and isolated under `tests/support`.

Fast repository tests may use librdkafka's disposable mock cluster for protocol-backed assignment, watermark, polling, EOF, tombstone, key, and header behavior. The mock cluster does not provide reliable timestamp-index semantics, so positive timestamp-range integration cases still belong in the disposable real-Kafka suite; unit tests cover mapping no-match results to the high watermark.

The Stage 3 suite uses deterministic admission and completion-frontier unit tests, a mock-cluster oversized-record pause/resume case, and process tests for broken pipes plus graceful and forced signals. The ordering tests inject completions directly rather than relying on thread timing. The forced-signal process test observes writer output and poller progress before sending the second signal instead of sleeping for an assumed pipe state.

### 2.6 Differential compatibility tests

Where behavior overlaps, run the same fixture through kcat and compare:

- format placeholder output;
- null key and payload lengths;
- binary `%R`;
- escapes;
- timestamps;
- headers;
- offset/count/EOF behavior.

Differential tests should pin or report the reference version. A changed external version must not silently rewrite expected behavior.

### 2.7 Expression differential tests

For syntax intentionally shared with jq, compare selected examples when semantics are meant to match.

Do not use jq as an oracle for deliberate differences such as:

- missing handling;
- strict booleans;
- one-result restriction;
- unsupported operators.

## 3. Fixture Design

Keep fixtures purposeful and reusable.

Suggested JSON fixtures:

- flat object;
- deeply nested object;
- large unrelated arrays;
- escaped strings;
- Unicode strings;
- integer, unsigned-range integer, and decimal numbers;
- missing fields;
- explicit nulls;
- empty object and array;
- duplicate source keys;
- malformed documents at different positions;
- documents immediately below and above the maximum nesting depth;
- very large string values.

Kafka fixtures:

- null key;
- empty key;
- binary key;
- no headers;
- null and empty header values;
- duplicate header names;
- create-time and log-append-time timestamps where available;
- tombstone value;
- empty value;
- JSON `null`;
- formatted and whitespace-heavy JSON.

## 4. Required Semantic Tests

### 4.1 Action precedence

- drop wins over tombstone;
- tombstone wins over projection;
- projection occurs after false predicates;
- pass-through occurs only without projection;
- input tombstone bypasses all rules.

### 4.2 Missing and null

- `exists` is true for null;
- `missing` is false for null;
- missing comparison behavior;
- null equality;
- missing projection failure;
- `coalesce` for missing and null;
- projected null is not tombstone.

### 4.3 Number behavior

- signed integer;
- unsigned integer;
- floating point;
- cross-representation comparison;
- out-of-range parse failure;
- serialization round-trip within supported semantics.

### 4.4 String behavior

- escapes;
- Unicode;
- contains/prefix/suffix;
- length;
- invalid type errors.

### 4.5 Objects and arrays

- nested paths;
- array indices;
- out-of-range index;
- quoted field names;
- projection order;
- duplicate projection key rejection.

## 5. Required Formatter Tests

For each of key and payload:

- null;
- empty;
- non-empty;
- binary bytes.

For metadata:

- topic;
- partition;
- offset;
- available timestamp;
- absent timestamp;
- no headers;
- null header;
- empty header;
- duplicate header names.

For parser:

- each supported placeholder;
- `%%`;
- each supported escape;
- unsupported placeholder;
- incomplete percent;
- malformed hex escape.

## 6. Ordering Model Tests

Model a partition with dense local sequences independent of Kafka offsets.

Required properties:

- emitted sequence is monotonically increasing per partition;
- every completion advances exactly once;
- drops emit nothing but advance;
- fatal completion stops further output according to shutdown policy;
- per-partition ordering does not impose global poll order;
- frontier and pending map remain bounded by admission limits.

Property-based testing may be useful here if it does not introduce a heavy dependency. A small deterministic permutation generator may be sufficient.

## 7. Backpressure Tests

Use a fake consumer adapter and blocked writer.

Verify:

- global record limit pauses admission;
- global byte limit pauses admission;
- per-partition limit pauses only the affected partition when possible;
- low-water resume works;
- Kafka event polling continues while paused;
- one oversized record is admitted when alone;
- a second record waits behind the oversized record;
- buffer retention cap discards oversized scratch allocations;
- shutdown releases all accounting.

Tests should inspect counters and state transitions rather than process RSS where possible.

## 8. Snapshot Tests

Required cases:

- non-empty fixed partition;
- empty partition;
- start at boundary;
- start after captured boundary;
- multiple partitions with different boundaries;
- producers append after capture;
- count terminates before boundary;
- one partition completes early while another continues;
- final admitted record is dropped;
- final admitted record finishes out of order.

The expected consumed range is always:

```text
resolved_start <= offset < captured_high_watermark
```

## 9. Error Policy Tests

For invalid JSON:

- fail;
- drop;
- tombstone;
- exact pass-through.

For evaluation error:

- fail;
- drop;
- tombstone.

For Kafka errors:

- recoverable continue;
- fatal consumer error remains fatal.

Every converted error increments its error counter and action counter.

## 10. Signal and Pipe Tests

### Broken pipe

Pipe output to a process that closes early. Verify:

- no noisy error by default;
- process exits successfully;
- workers and consumer stop promptly.

### First signal

Verify:

- admission stops;
- queued work drains;
- stdout flushes;
- exit status follows documented signal semantics.

### Second signal

Verify forced termination without waiting for a blocked worker or writer indefinitely.

Platform-specific tests may be conditionally compiled.

## 11. Benchmarks

### 11.1 Microbenchmarks

- lex and parse one predicate;
- compile a representative program;
- evaluate flat paths;
- evaluate shared-prefix nested paths;
- predicate short-circuit;
- projection serialization;
- format `%s\n`;
- format metadata-heavy record;
- JSON envelope encoding.

### 11.2 Parser/backend benchmarks

Payload sizes:

- 256 B;
- 2 KiB;
- 16 KiB;
- 256 KiB;
- 2 MiB.

Path counts:

- 1;
- 5;
- 20.

Path positions:

- early;
- late;
- deep;
- shared prefix;
- inside large arrays.

Measure:

- throughput in GiB/s;
- records/s;
- allocations/record where available;
- copied bytes/record;
- peak retained capacity.

### 11.3 Pipeline benchmarks

Scenarios:

1. Kafka to `/dev/null` equivalent with identity output disabled or minimal;
2. pass-through;
3. one false predicate;
4. one highly selective drop predicate;
5. tombstone predicate;
6. small projection;
7. twenty-field projection;
8. ordered one worker;
9. ordered multiple workers;
10. unordered multiple workers;
11. slow writer;
12. one huge record among small records.

Compare against:

- kcat alone;
- any other relevant tool only when version and command are recorded.

A kcat-plus-jq comparison is deferred until it represents a useful supported workload.

### 11.4 Benchmark reporting

Store methodology and representative results under `docs/benchmarks/`.

Report:

- hardware;
- operating system;
- compiler version;
- target;
- dependency versions;
- Kafka setup;
- command line;
- payload distribution;
- result variance.

Do not enforce absolute performance thresholds in ordinary CI. A dedicated benchmark workflow may compare trends with broad tolerances.

## 12. CI Test Matrix

Normal pull-request CI:

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

Also include:

- at least Linux AMD64;
- macOS ARM64 when available;
- minimal-feature build if feature flags exist;
- dependency and license audit when configured.

Kafka-backed tests may run in CI if reliable and reasonably fast. Expensive volume and benchmark suites should be separate manual or scheduled workflows.

## 13. Test Naming

Names should state behavior and condition:

```text
drop_predicate_precedes_tombstone
tombstone_payload_length_is_negative_one
completion_frontier_advances_across_drop
snapshot_ignores_records_appended_after_capture
invalid_json_pass_preserves_original_bytes
```

Avoid names such as `test_parser_1` or `works`.

## 14. Review Checklist for Tests

A reviewer should be able to answer:

- What behavior does this test freeze?
- Could a cheaper layer test it?
- Does it fail for the intended regression?
- Is the fixture smaller than necessary?
- Is timing involved unnecessarily?
- Does the assertion cover observable behavior rather than implementation trivia?
- Would deleting this test remove meaningful protection?
