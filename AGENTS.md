# Repository Instructions

## Purpose

This repository contains a small, high-throughput Rust command-line consumer for Apache Kafka records whose values are JSON.

The product combines:

- direct Kafka partition consumption;
- native JSONata predicates and projections powered by `jsonata-core`;
- explicit record actions: drop, tombstone, pass through, and project;
- kcat-style record formatting;
- bounded parallel execution with per-partition output ordering.

Keep the product narrow. It is a stream-processing command, not a general Kafka administration suite, a complete jq implementation, a producer, or a long-running service.

## Read Before Changing Behavior

Treat these documents as the current product and architecture contract:

1. `README.md` for purpose and scope;
2. `docs/usage.md` for CLI and output behavior;
3. `docs/expression-language.md` for expression semantics;
4. `docs/architecture.md` for boundaries and invariants;
5. `docs/development.md` for tests, dependencies, and local workflows.

When code and documentation disagree, determine whether the code is wrong or the contract needs an intentional revision. Do not silently preserve accidental behavior.

## Engineering Principles

- Prefer simple, explicit Rust over clever abstractions.
- Optimize the data path, not incidental setup code.
- Make ownership, ordering, and shutdown behavior visible in types.
- Keep modules cohesive and moderately sized.
- Avoid files that are tiny wrappers without a meaningful boundary.
- Split files that mix unrelated responsibilities or become difficult to navigate.
- Prefer standard library facilities when they remain clear and efficient.
- Add dependencies only when they are mature, widely used, performant, and remove substantial implementation risk.
- Do not add an asynchronous runtime unless the architecture materially changes.
- Do not add a general jq engine, parser generator, actor framework, or generic pipeline framework.
- Do not introduce unsafe Rust without a measured need, a contained boundary, and focused tests.
- Do not mutate librdkafka-owned message memory.
- Keep stdout exclusively for record data. Send diagnostics, progress, statistics, and errors to stderr.
- Treat a broken stdout pipe as normal pipeline termination.

## Code Style

- Use stable Rust and idiomatic ownership.
- Prefer concrete types and enums over stringly typed internal state.
- Prefer small public surfaces and private implementation details.
- Use descriptive names based on domain concepts: assignment, record, action, plan, completion, frontier, formatter, and snapshot.
- Keep comments uncommon. Use them for invariants, non-obvious safety constraints, protocol compatibility, and performance-sensitive reasoning.
- Do not narrate straightforward code.
- Let tests and documentation explain behavior.
- Avoid speculative generalization. Do not wrap JSONata in a backend trait or factory.
- Avoid clones in the hot path unless the ownership transfer would otherwise be less clear or less efficient.
- Reuse worker-local buffers where practical.
- Make integer conversions checked when values can cross API or platform boundaries.
- Preserve exact source bytes for pass-through output.

Run formatting and linting with no ignored warnings:

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
```

## Dependency Policy

Expected initial runtime dependencies are limited to:

- `rdkafka`
- `jsonata-core`
- `clap`
- `crossbeam-channel`
- `signal-hook`

Transitive dependencies are acceptable. Additional direct dependencies require a concrete justification in the change description.

Before adding a dependency, evaluate:

1. whether the standard library is sufficient;
2. whether the dependency is actively maintained and widely deployed;
3. whether it affects binary portability or static linking;
4. whether it appears in the hot path;
5. whether a smaller feature set can be selected;
6. whether its license is compatible with distribution.

Do not add both overlapping libraries for the same concern without benchmark or compatibility evidence.

## Architectural Invariants

These invariants must remain true unless the owning behavior or architecture document is deliberately revised:

- Consumption uses direct partition assignment only.
- One invocation consumes one topic and directly assigns either explicitly selected partitions or every partition discovered at startup.
- Existing Kafka tombstones bypass JSON parsing and remain tombstones by default.
- Every non-tombstone input produces exactly one action: drop, tombstone, pass through, or project.
- One input record never expands into multiple output records.
- `-c` counts admitted Kafka input records, including records later dropped or tombstoned.
- Output ordering is preserved within each partition by default.
- There is no global ordering contract across partitions.
- A dropped record still advances the partition completion frontier.
- `%s`, `%S`, and `%R` operate on the post-transform payload.
- A tombstone is distinct from an empty byte payload and JSON text `null`.
- `%S` reports `-1` for a tombstone and `0` for an empty payload.
- `%R` writes signed `-1` as a four-byte big-endian length for a tombstone.
- All predicates and projections use native JSONata.
- Channels, admitted record counts, per-partition work, and owned source record bytes are bounded.
- `--max-inflight-bytes` accounts for owned source payload, key, header names, and header values; it does not cover jsonata-core's parsed tree, evaluation intermediates, or projected output.
- Backpressure may pause partitions, but the consumer must continue serving Kafka events.
- Snapshot termination is based on captured exclusive high-watermark offsets.
- Shared transform plans contain only thread-safe JSONata expression source and variable JSON; ASTs, values, contexts, and evaluators are worker-local.
- Each Kafka payload is parsed into jsonata-core's value representation at most once per record and reused across predicates and projection.
- Evaluator state, assignments, variables, and root context never leak between records.
- The default runtime uses dedicated threads and bounded channels rather than an async runtime.
- Errors are governed by explicit policy and never silently converted into successful output.

## Testing Philosophy

Tests exist to freeze meaningful behavior and expose regressions.

Each test should establish one identifiable contract, invariant, edge case, compatibility rule, or previously observed failure. Avoid tests whose only purpose is increasing coverage.

Use the cheapest suitable layer:

- startup-plan tests for JSONata parsing and strict variable validation;
- transform tests for jkq's JSONata integration semantics;
- formatter golden tests for byte-exact output;
- ordering and backpressure tests for concurrency invariants;
- integration tests for process behavior;
- Kafka-backed tests only for behavior that cannot be established without Kafka;
- benchmarks for performance questions, not correctness claims.

Tests may contain concise comments when the intent or fixture is not self-evident. Prefer table-driven cases when cases share one behavioral purpose. Do not compress unrelated scenarios into unreadable test matrices.

When fixing a defect:

1. add a focused failing regression test;
2. implement the smallest correct fix;
3. run the relevant focused test;
4. run the normal repository checks.

## Documentation Rules

Documentation is part of the implementation.

- Update the owning document when observable behavior changes.
- Keep user-facing documentation concrete and example-driven.
- Keep architecture documentation focused on boundaries, data flow, ownership, and invariants.
- Record deliberate compatibility deviations.
- Avoid marketing language and unverified performance claims.
- Do not duplicate entire sections across files. Link to the authoritative document.
- Keep generated or maintained documentation readable by both human contributors and automated development tools.
- Use stable terminology consistently:
  - input record;
  - source metadata;
  - action;
  - post-transform payload;
  - partition sequence;
  - completion frontier;
  - snapshot boundary.

## Work Process

Before implementation:

1. read the relevant documentation;
2. identify the behavior being added or changed;
3. locate the owning module rather than creating a parallel path;
4. decide how the behavior will be verified.

During implementation:

1. keep changes narrowly scoped;
2. preserve architectural invariants;
3. avoid unrelated cleanup;
4. run focused tests while iterating;
5. keep public errors actionable and stable where documented.

Before considering work complete:

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

Also run Kafka integration tests and performance smoke tests when the change affects consumption, offsets, ordering, backpressure, JSON execution, or formatting.

## Definition of Done

A change is complete only when:

- behavior matches the product and architecture documentation;
- new behavior has purposeful tests;
- failure behavior is tested where material;
- formatting, linting, and tests pass;
- public documentation is updated;
- no avoidable dependency or abstraction was introduced;
- stdout remains data-only;
- channels, admitted records, per-partition work, and owned source-byte accounting remain bounded under slow output and large source records;
- partition ordering remains correct unless unordered mode was explicitly selected;
- performance-sensitive changes include benchmark evidence or a clear non-regression rationale.

## graphify

This project may have a local knowledge graph at `graphify-out/`. The directory
is generated and git-ignored, so fresh clones will not have it until an agent or
developer builds it locally.

Rules:

- For codebase questions, first run `graphify query "<question>"` when
  `graphify-out/graph.json` exists.
- If `graphify-out/graph.json` is missing and graphify is available, run
  `graphify .` before relying on graph queries.
- Use `graphify path "<A>" "<B>"` for relationships and
  `graphify explain "<concept>"` for focused concepts.
- Dirty graph files are expected after hooks or incremental updates; they are
  not a reason to skip graphify.
- If `graphify-out/wiki/index.md` exists, use it for broad navigation instead
  of raw source browsing.
- Read `graphify-out/GRAPH_REPORT.md` only for broad architecture review or
  when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current.
