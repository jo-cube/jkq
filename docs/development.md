# Development

`jkq` is intentionally a small Rust binary. Keep changes in the module that
owns the behavior and avoid turning the data path into a generic framework.

## Repository Shape

```text
src/main.rs              process entrypoint
src/cli.rs               CLI and configuration
src/app.rs               application assembly
src/kafka.rs             direct Kafka input
src/transform/           expression parser, compiler, and JSON backend
src/runtime.rs           bounded execution pipeline
src/runtime/state.rs     admission and ordering state
src/output.rs            formats and JSON envelopes
tests/process.rs         Unix process tests
.github/workflows/ci.yml source checks on Linux
```

[architecture.md](architecture.md) explains ownership and data flow.
[usage.md](usage.md) and [expression-language.md](expression-language.md) are
the observable behavior contracts.

## Prerequisites

The repository follows stable Rust through `rust-toolchain.toml`. Building the
bundled librdkafka and vendored OpenSSL normally requires a C compiler, `make`,
Perl, and `pkg-config` on Unix.

Build a release binary with:

```sh
cargo build --release --locked
```

## Normal Checks

```sh
make check
```

This runs:

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

CI runs the same checks with the lockfile enforced. There is currently no
release packaging workflow or installer; do not document release artifacts
until that machinery exists.

## Tests

Most tests live beside the code they cover:

- lexer and parser tests freeze syntax and positioned errors;
- compiler and evaluator tests freeze action precedence, missing/null
  semantics, number behavior, depth limits, and error policies;
- output tests use exact bytes for formatting, framing, headers, and envelopes;
- runtime state tests deterministically exercise admission and partition
  frontiers;
- app tests use librdkafka's mock cluster for assignment, ranges, tombstones,
  metadata, snapshots, and oversized records;
- `tests/process.rs` covers broken pipes and signal behavior on Unix.

Prefer the cheapest layer that establishes the behavior. Concurrency tests
should control event order with channels or explicit state rather than timing.
Kafka-backed tests belong only where a pure state or mock test cannot establish
the contract.

When fixing a defect:

1. add one focused regression test;
2. implement the smallest root-cause fix;
3. run the focused test;
4. run `make check`.

## Performance Work

Optimize the record path, not startup convenience. Preserve these properties:

- exact source bytes for pass-through;
- bounded queues and retained bytes;
- continued Kafka event polling during backpressure;
- per-partition ordering unless `--unordered` is selected;
- worker-local scratch reuse without unbounded retained capacity.

Use release builds and representative payloads for measurements. Record the
hardware, compiler, command, input distribution, and median results. Do not add
CI throughput thresholds or permanent benchmark machinery for a one-off
question.

## Dependencies

The direct runtime dependencies are deliberately limited to:

- `rdkafka`
- `simd-json`
- `clap`
- `crossbeam-channel`
- `signal-hook`

Before adding another, confirm that the standard library or an existing
dependency is insufficient and that the new dependency removes meaningful
risk or code. Do not add an async runtime, parser generator, jq engine, actor
framework, or generic pipeline framework without a design change backed by a
real requirement.

## Documentation

Keep each fact in one place:

| Document | Owns |
|---|---|
| root `README.md` | purpose, scope, quick start, build entrypoint |
| `usage.md` | CLI workflows and output behavior |
| `expression-language.md` | grammar and evaluation semantics |
| `architecture.md` | module boundaries, ownership, and invariants |
| `development.md` | contributor workflow, tests, and dependencies |

Update the owning document when observable behavior or an architectural
invariant changes. Prefer examples and current facts over roadmaps, historical
rationale, or exhaustive checklists.

## Optional Graphify Workflow

`graphify-out/` is generated and git-ignored. When Graphify is installed, build
the local code graph with:

```sh
graphify .
```

Query it before broad source browsing and refresh it after code or
documentation changes:

```sh
graphify update .
```

Fresh clones do not contain the generated graph.
