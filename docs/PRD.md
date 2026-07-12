# Product Requirements Document

## 1. Summary

Build a fast, script-friendly Rust command-line program that directly consumes JSON-valued records from one Apache Kafka topic, applies restricted filtering and projection rules, and writes transformed records to stdout using kcat-compatible formatting conventions.

The product is intended for high-volume stream and historical-topic workflows where:

- payloads may be large;
- only a small subset of JSON fields is relevant;
- users need Kafka keys, offsets, partitions, timestamps, and headers;
- users need to drop records, turn records into downstream tombstones, preserve source payloads, or emit projected JSON;
- spawning a general JSON processor for every record or constructing a full JSON DOM creates unacceptable overhead.

The product is a consumer and transform tool. It is not a producer, consumer-group application, Kafka administration suite, or general-purpose JSON language.

## 2. Product Principles

### 2.1 Unix pipeline behavior

- stdout is record data.
- stderr is diagnostics.
- Output is suitable for redirection and piping.
- Broken-pipe termination is successful and quiet by default.
- Binary payload framing remains possible through format placeholders.

### 2.2 Narrow, explicit semantics

- One Kafka input record yields zero or one output record.
- Record outcomes are explicit actions, not overloaded JSON values.
- Missing JSON, JSON `null`, empty payloads, and Kafka tombstones remain distinguishable.
- Direct assignment and termination rules are deterministic and documented.

### 2.3 Performance by avoiding work

Performance should come primarily from:

- compiling expressions once;
- identifying referenced paths before consumption;
- avoiding full owned DOM construction where possible;
- skipping parsing for input tombstones and identity transforms;
- reusing buffers;
- bounding queues and retained bytes;
- parallelizing CPU work while restoring partition order.

### 2.4 Compatibility where useful

The consumer and formatting surface should be familiar to kcat users. Compatibility is a product goal for the supported subset, not a commitment to reproduce unrelated modes.

The expression language is jq-inspired but intentionally not jq-compatible in full.

## 3. Goals

### G1. Direct Kafka consumption

Consume one topic through explicit partition assignment with support for one or more partitions, starting offsets, count limits, end-of-partition termination, timestamp ranges, and deterministic snapshots.

### G2. Four record actions

For each input record, produce exactly one of:

- **drop**: no output record;
- **tombstone**: output metadata with a null post-transform payload;
- **pass through**: preserve the exact original value bytes;
- **project**: emit compact JSON constructed from selected source fields and expressions.

### G3. Restricted JSON predicates and projections

Provide simple repeated command-line options that compile into one internal program:

- `--drop-if <predicate>`
- `--tombstone-if <predicate>`
- `--project <projection>`

The language must cover common field filtering, existence checks, comparisons, boolean composition, string checks, length checks, and object or array projection.

### G4. High throughput with bounded memory

Use a bounded worker pool, worker-local scratch buffers, per-partition completion ordering, byte-aware admission control, and Kafka partition pause/resume.

### G5. Familiar output formatting

Support:

- kcat-style `-f` formatting for the supported placeholders;
- JSON envelope output through `-J`;
- correct tombstone lengths through `%S` and `%R`;
- original Kafka metadata and transformed payload data.

### G6. Portable binary releases

Publish versioned archives for:

- Linux AMD64;
- Linux ARM64;
- macOS ARM64.

Each archive must have a separate SHA-256 checksum file and contain one executable. A portable shell installer must support latest-version installation, pinned versions, default and custom install directories, platform detection, and checksum verification.

## 4. Non-Goals

The first major product line does not include:

- Kafka producer mode;
- consumer groups or offset commits;
- exactly-once output;
- multiple topics in one invocation;
- schema registry integration;
- Avro, Protobuf, MessagePack, or primitive deserialization;
- non-JSON non-tombstone values;
- a complete jq interpreter;
- multiple outputs from one input;
- recursive descent;
- arbitrary iteration over arrays or objects;
- reductions, sorting, grouping, joins, or stateful aggregation;
- user-defined functions or modules;
- network or file output sinks;
- a daemon or service mode;
- interactive queries;
- metadata administration commands;
- a public Rust library API;
- package-registry publication.

## 5. Target Users

### 5.1 Data and platform engineers

Users inspecting, extracting, or transforming large Kafka topics in shell workflows.

### 5.2 Software engineers

Users materializing subsets of compacted topics, generating binary-safe export streams, or preparing records for another command.

### 5.3 Incident and operations workflows

Users requiring precise offsets, keys, partitions, timestamps, headers, and error policies while diagnosing data.

## 6. Core User Stories

### US1. Filter irrelevant records

As a user, I can drop records whose JSON does not satisfy business criteria so that downstream commands receive only relevant records.

### US2. Emit logical deletion

As a user, I can convert matching records into tombstones while retaining the source key and metadata so that a downstream producer or storage command can interpret deletion.

### US3. Extract a small projection

As a user, I can emit a compact object containing only selected fields without materializing or serializing the entire source document.

### US4. Preserve unchanged data

As a user, I can apply conditional actions and retain exact source bytes when no projection is configured and no destructive action matches.

### US5. Consume a stable historical snapshot

As a user, I can capture each selected partition's high watermark at startup and terminate after processing exactly that bounded range, even if producers continue appending.

### US6. Preserve Kafka ordering

As a user, output records from each partition remain in source offset order even when JSON processing is parallel.

### US7. Distinguish null forms

As a user, I can distinguish a Kafka tombstone, an empty value, and the JSON text `null` using output lengths.

### US8. Control malformed-data behavior

As a user, I can select whether invalid JSON or expression failures stop the process, drop a record, tombstone it, or pass through the original where allowed.

### US9. Reuse existing Kafka configuration

As a user, I can supply librdkafka properties through command-line key/value options and a configuration file.

## 7. Functional Requirements

### FR1. Topic and partition selection

- Exactly one topic is required.
- At least one partition must be selected explicitly.
- A partition option may be repeated.
- Duplicate partition selections are rejected.
- Negative partition identifiers are rejected.
- The program must not silently consume all partitions when none are supplied.

### FR2. Offset selection

Support start positions:

- `beginning`;
- `end`;
- non-negative absolute offset;
- negative relative offset from the current end;
- start timestamp in milliseconds since Unix epoch.

Support an optional exclusive end timestamp or explicit exclusive end offset.

Offset resolution errors must identify the topic and partition.

### FR3. Count limit

- `-c <count>` limits admitted input records across all selected partitions.
- The count includes source tombstones and records later dropped or transformed.
- Once the limit is reached, no more records are admitted.
- Already admitted work is drained and emitted according to ordering rules.
- The count must be a positive integer.

### FR4. End termination

- `-e` exits when every selected partition reaches its effective end.
- An explicit end boundary is exclusive.
- Snapshot mode implies bounded-end termination.
- Without an explicit end or snapshot, partition EOF is the current broker-reported end.
- The program must drain admitted work before successful exit.

### FR5. Snapshot mode

At startup, after assignment and start resolution:

- capture each selected partition's high watermark;
- store it as an exclusive boundary;
- never extend that boundary;
- stop admitting a partition after its next offset reaches the boundary;
- finish only after its completion frontier reaches the boundary.

Empty partitions complete immediately when start equals boundary.

### FR6. Existing tombstones

An input record with a null Kafka value:

- bypasses JSON parsing;
- bypasses predicates and projection;
- emits a tombstone by default;
- still counts toward `-c`;
- participates in partition ordering.

A future explicit option may change this behavior, but it is not required initially.

### FR7. Predicate precedence

For non-tombstone JSON values:

1. evaluate repeated drop predicates in command-line order;
2. the first true drop predicate returns drop;
3. evaluate repeated tombstone predicates in command-line order;
4. the first true tombstone predicate returns tombstone;
5. apply the projection if configured;
6. otherwise pass through the exact source bytes.

Repeated predicates are semantically ORed, while retaining short-circuit order.

### FR8. Projection

- At most one projection is accepted.
- Projection can emit a JSON object, array, string, number, boolean, or JSON `null`.
- Projection output is compact UTF-8 JSON.
- Projected JSON `null` is a four-byte payload and is not a tombstone.
- Projection cannot emit multiple records.

### FR9. Format output

The supported format placeholders are:

- `%o`: source offset;
- `%k`: source key bytes;
- `%K`: source key byte length, or `-1` for null;
- `%s`: post-transform payload bytes;
- `%S`: post-transform payload byte length, or `-1` for tombstone;
- `%R`: four-byte big-endian signed post-transform payload length, or `-1` for tombstone;
- `%t`: source topic;
- `%p`: source partition;
- `%T`: source timestamp in milliseconds, or `-1` when unavailable;
- `%h`: source headers using documented text rendering;
- `%%`: literal percent sign.

Literal escape handling must support at least `\n`, `\r`, `\t`, `\\`, and hexadecimal byte escapes.

Unsupported placeholders are command-line errors.

### FR10. JSON envelope output

`-J` writes one compact JSON object per emitted record and appends a newline.

The envelope must include:

- topic;
- partition;
- offset;
- timestamp and timestamp type when available;
- key length and a safe key representation;
- headers;
- payload length;
- payload or explicit tombstone state;
- action.

The post-transform payload is represented, not the pre-transform payload. The exact schema is compatibility-sensitive and must be frozen by golden tests before the first stable release.

`-J` and `-f` are mutually exclusive.

### FR11. Error policies

Provide separate policy options for:

- invalid JSON;
- predicate or projection evaluation errors;
- Kafka consumption errors.

Initial policies:

| Condition | Supported policies |
|---|---|
| Invalid JSON | `fail`, `drop`, `tombstone`, `pass` |
| Evaluation error | `fail`, `drop`, `tombstone` |
| Kafka record error | `fail`, `continue` |

`pass` means preserve the exact original payload. It is valid only where original bytes remain available.

Default policies are `fail`.

### FR12. Parallelism

- `-j, --jobs <count>` configures compute workers.
- Default worker count is based on available parallelism and leaves capacity for the poller, writer, and librdkafka threads.
- A value of one remains fully supported and deterministic.
- Compute work may run concurrently within one partition.
- Default output restores per-partition sequence.
- `--unordered` may emit completed records immediately without partition restoration.

### FR13. Backpressure

The product must bound:

- admitted record count;
- admitted payload bytes;
- pending records per partition;
- reorder-buffer entries;
- worker scratch retention.

When limits are reached:

- affected partitions are paused;
- Kafka event polling continues;
- partitions resume below a documented low-water threshold;
- a single record larger than the byte budget is admitted only when no other payload bytes are retained.

### FR14. Configuration

Support:

- broker list;
- explicit configuration file;
- repeated librdkafka `key=value` properties;
- deterministic precedence;
- redaction of secrets in diagnostics;
- configuration validation before consumption.

Precedence from lowest to highest:

1. built-in defaults;
2. optional default config file;
3. explicit config file;
4. repeated property options;
5. dedicated command-line options.

### FR15. Signals and shutdown

- First termination signal stops admission and begins graceful drain.
- A second termination signal may force termination.
- Broken pipe stops processing without an error diagnostic.
- Fatal worker or writer errors cancel all stages.
- The consumer is closed cleanly within a bounded shutdown path.

### FR16. Statistics

Optional stderr statistics should include:

- admitted input records;
- input tombstones;
- input bytes;
- dropped records;
- generated tombstones;
- pass-through records;
- projected records;
- invalid JSON records;
- evaluation failures;
- output records;
- output bytes;
- per-stage elapsed time or rate where practical.

Statistics must not alter stdout.

## 8. Non-Functional Requirements

### NFR1. Performance

- Identity consumption should add minimal overhead above librdkafka and stdout.
- Predicate-only and projection paths should avoid unnecessary owned JSON trees.
- Work should scale across compute workers until limited by Kafka, memory bandwidth, or stdout.
- Performance claims require reproducible benchmark data.
- No fixed records-per-second requirement is set before representative benchmarks exist.

### NFR2. Memory

- Memory usage must be configurable and bounded by design.
- Payload-size variance must not bypass admission control.
- Large worker buffers must be capped or discarded after use.
- Reorder buffering must not grow without bound behind one slow record.

### NFR3. Reliability

- Partition ordering must hold across drops, tombstones, errors converted by policy, and worker completion reordering.
- Snapshot boundaries must not move after capture.
- Format output must be byte-exact.
- Invalid command combinations must fail before consuming.

### NFR4. Portability

- The release process must produce archives for the supported platform set.
- Release binaries should avoid undocumented runtime prerequisites where practical.
- Build details may differ by target as long as the installed executable behavior is consistent.

### NFR5. Maintainability

- Runtime dependencies remain few.
- Public behavior is specified in tests and documents.
- Hot-path modules have focused benchmarks.
- The implementation does not expose a public library compatibility commitment.

## 9. CLI Compatibility Position

The product intentionally supports a consumer-oriented subset of familiar options and formatting behavior.

Supported compatibility areas include:

- broker, topic, partition, offset, count, EOF exit, JSON envelope, format string, config file, and arbitrary librdkafka property options;
- the listed formatter placeholders;
- null length behavior for keys and values;
- direct partition consumption.

Explicitly unsupported areas include:

- producer mode;
- group mode;
- schema deserialization;
- metadata and query modes unless separately added later;
- options meaningful only to those modes.

Compatibility tests should compare byte output and termination behavior for overlapping supported cases.

## 10. Acceptance Criteria for the First Usable Release

The first usable release is complete when:

1. supported target archives are published with checksums;
2. the installer can install latest and pinned versions;
3. direct assignment works for multiple partitions;
4. beginning, end, absolute, relative, and timestamp starts work;
5. count, EOF, explicit end, and snapshot termination are tested;
6. all four record actions work;
7. the initial expression grammar is implemented and documented;
8. format placeholders produce byte-correct output, including tombstones;
9. JSON envelope output has a frozen schema;
10. per-partition ordering holds under parallel worker completion;
11. bounded memory and pause/resume are exercised by integration tests;
12. invalid JSON and evaluation policies are implemented;
13. signal and broken-pipe behavior are tested;
14. normal formatting, linting, unit, integration, and Kafka-backed tests pass;
15. benchmark baselines are checked into the repository as documentation, without enforcing fragile performance thresholds in normal CI.
