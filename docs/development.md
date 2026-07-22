# Development

`jkq` is intentionally a small Rust binary. Keep changes in the module that
owns the behavior and avoid turning the data path into a generic framework.

## Repository Shape

```text
src/main.rs              process entrypoint
src/cli.rs               CLI and configuration
src/app.rs               application assembly
src/kafka.rs             direct Kafka input
src/transform/           startup JSONata plan and worker-local execution
src/runtime.rs           bounded execution pipeline
src/runtime/state.rs     admission and ordering state
src/output.rs            formats and JSON envelopes
tests/process.rs         Unix process tests
tests/install.sh         release installer integration test
.github/workflows/ci.yml source checks on Linux
.github/workflows/release.yml tagged release archives and checksums
.github/dependabot.yml weekly dependency update checks
scripts/install.sh       checksum-verifying release installer
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
sh tests/install.sh
cargo fmt --all --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
```

CI runs the same checks. A release tag must match the package version, such as
`v0.1.0`. Release jobs build native Linux amd64, Linux arm64, and macOS arm64
archives, execute each binary as a smoke test, and attach each archive and its
SHA-256 checksum to the GitHub release. GNU binaries are built on Ubuntu 26.04;
build locally when compatibility with an older glibc-based system is required.
`scripts/install.sh` installs supported release binaries after verifying their
published checksums. There is no crates.io publication.

## Tests

Most tests live beside the code they cover:

- startup-plan tests freeze native JSONata parsing and strict `--vars`
  validation;
- transform tests freeze jkq's JSONata embedding contract: action precedence,
  strict Boolean predicates, result serialization, state isolation, number
  behavior, and error policies;
- output tests use exact bytes for formatting, framing, headers, and envelopes;
- runtime state tests deterministically exercise admission and partition
  frontiers;
- app tests use librdkafka's mock cluster for assignment, ranges, tombstones,
  metadata, snapshots, and oversized records;
- `tests/process.rs` covers broken pipes and signal behavior on Unix.
- `tests/install.sh` verifies release URL selection, checksums, and installation.

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

- exact source bytes for pass-through unless JSON-value envelope serialization
  was explicitly selected;
- bounded channels, admitted records, per-partition work, and owned source
  bytes;
- continued Kafka event polling during backpressure;
- per-partition ordering unless `--unordered` is selected;
- worker-local JSONata values and evaluator contexts without cross-record
  state.

Full JSONata can construct data-dependent intermediate and output values.
`--max-inflight-bytes` does not bound those allocations, so performance smoke
tests should include representative expressions and result sizes as well as
source payloads.

Use release builds and representative payloads for measurements. Record the
hardware, compiler, command, input distribution, and median results. Do not add
CI throughput thresholds or permanent benchmark machinery for a one-off
question.

## Dependencies

The direct runtime dependencies are deliberately limited to:

- `rdkafka`
- `jsonata-core`
- `clap`
- `crossbeam-channel`
- `signal-hook`

`jsonata-core` is the sole expression engine and is used through its public
`parser`, `evaluator`, and `value` APIs. Its default `simd` feature brings
`simd-json` transitively; jkq does not depend on simd-json directly. Do not use
jsonata-core's `_bench` module or other internal APIs.

Before adding another dependency, confirm that the standard library or an
existing dependency is insufficient and that the new dependency removes
meaningful risk or code. Do not add an async runtime, parser generator, second
expression engine, actor framework, or generic pipeline framework without an
intentional architecture change.

Dependabot checks Cargo and GitHub Actions dependencies weekly. Vulnerability
alerts and security-only update pull requests also require the corresponding
repository security settings to be enabled.

## Documentation

Keep each fact in one place:

| Document | Owns |
|---|---|
| root `README.md` | purpose, scope, quick start, build entrypoint |
| `usage.md` | CLI workflows and output behavior |
| `expression-language.md` | jkq's JSONata integration contract and deviations |
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
