# Release and Distribution

## 1. Distribution Model

The project distributes executable archives through repository releases. It does not publish a package-registry crate.

A release is created from a version tag matching:

```text
v*
```

Examples:

```text
v0.1.0
v0.2.0
v1.0.0
```

Semantic Versioning is used for user-visible behavior.

## 2. Supported Release Platforms

Initial release assets:

| Platform | Architecture | Archive label |
|---|---|---|
| Linux | AMD64 | `linux_amd64` |
| Linux | ARM64 | `linux_arm64` |
| macOS | ARM64 | `darwin_arm64` |

Additional platforms require a support and testing decision. Source builds remain possible on other Rust-supported systems.

## 3. Asset Naming

For an executable named from repository configuration:

```text
<binary>_linux_amd64.tar.gz
<binary>_linux_amd64.tar.gz.sha256

<binary>_linux_arm64.tar.gz
<binary>_linux_arm64.tar.gz.sha256

<binary>_darwin_arm64.tar.gz
<binary>_darwin_arm64.tar.gz.sha256
```

Each archive contains exactly one executable at its root:

```text
<binary>
```

Do not add an extra top-level directory inside the archive.

## 4. Release Trigger and Jobs

A release workflow is triggered by:

- pushing a tag matching `v*`;
- optional manual dispatch for validation, without publishing unless running on a tag.

The workflow has two logical phases.

### 4.1 Test phase

Run on Linux AMD64:

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

Publishing depends on this phase.

### 4.2 Build and publish phase

For each supported platform:

1. check out the exact tag;
2. install the pinned Rust toolchain;
3. build a release executable;
4. embed the tag as the version;
5. strip symbols where appropriate;
6. smoke-test `--version` on native targets;
7. create a `.tar.gz` archive;
8. create a separate SHA-256 checksum file;
9. upload both files to the tag's repository release.

The matrix should not stop other platforms when one target fails, but the release should be considered incomplete until every required asset exists.

## 5. Reproducibility and Locking

- Commit `Cargo.lock`.
- Build releases with `--locked`.
- Pin action major versions at minimum; commit-SHA pinning may be added for stronger supply-chain control.
- Record the Rust toolchain through `rust-toolchain.toml`.
- Avoid downloading unverified build tools during release where practical.
- Treat target-specific native libraries as part of the release reproducibility problem.

The project should prefer consistent bundled librdkafka builds for release artifacts if authentication and TLS functionality remain correct. Any dynamic runtime dependency must be documented.

## 6. Version Embedding

The build should expose the tag through `--version`.

Recommended mechanism:

- a small `build.rs` reads an environment variable supplied by the workflow;
- development builds fall back to the package version plus optional git revision;
- the runtime does not shell out to Git.

The version reported by an archive must match the release tag.

## 7. Installer Contract

Provide:

```text
scripts/install.sh
```

The installer must be POSIX-oriented and work with common macOS and Linux shells.

### 7.1 Install latest

The default invocation resolves the latest repository release.

### 7.2 Install pinned version

An environment variable selects a version:

```sh
VERSION=v0.1.0 sh scripts/install.sh
```

When used through a hosted raw script, the same environment contract applies.

### 7.3 Install directory

Default:

```text
$HOME/.local/bin
```

A first positional argument selects another directory:

```sh
sh scripts/install.sh "$HOME/bin"
```

### 7.4 Platform detection

Map:

```text
Linux x86_64/amd64 → linux_amd64
Linux aarch64/arm64 → linux_arm64
Darwin arm64       → darwin_arm64
```

Unsupported combinations fail with a clear message.

### 7.5 Download and verification

The installer:

1. determines version;
2. determines platform label;
3. downloads archive;
4. downloads matching checksum file;
5. verifies SHA-256;
6. extracts to a temporary directory;
7. creates the destination directory;
8. installs executable atomically where practical;
9. ensures executable permissions;
10. removes temporary files;
11. prints installed path and version.

Use `curl` when available and optionally `wget` as a fallback.

Checksum verification should support:

- `sha256sum` on Linux;
- `shasum -a 256` on macOS.

Failure to verify aborts installation.

## 8. Release Notes

Each release should describe:

- user-visible additions;
- behavior changes;
- compatibility changes;
- performance changes with evidence;
- fixes;
- upgrade considerations;
- known limitations.

Do not publish generated notes without reviewing them for behavioral accuracy.

## 9. Versioning Policy

### Patch

- bug fixes;
- performance improvements without behavior changes;
- documentation corrections;
- additional tests;
- packaging fixes.

### Minor

- new compatible options;
- new expression functions;
- new format placeholders;
- new supported platforms;
- additive envelope fields only if the schema permits them.

### Major

- breaking CLI behavior;
- changed expression semantics;
- changed default output;
- changed envelope schema;
- changed ordering or termination contract;
- removal or reinterpretation of options.

Before `v1.0.0`, breaking changes may occur in minor versions but must be explicit in release notes.

## 10. Artifact Verification

Before publishing or immediately after:

- download every archive from the release;
- verify checksum;
- inspect archive layout;
- run `--version` on native platforms;
- run a minimal help command;
- where practical, run a Kafka smoke test.

A release workflow should eventually automate post-upload verification.

## 11. CI and Release Separation

Normal CI validates source changes.

Release automation additionally validates:

- version embedding;
- target builds;
- archive naming;
- checksum generation;
- installer compatibility.

Do not make release-only behavior depend on uncommitted local scripts.

## 12. Release Checklist

1. all required checks pass;
2. design documents reflect behavior;
3. changelog or release notes are prepared;
4. version tag follows `v<major>.<minor>.<patch>`;
5. tag points to the intended commit;
6. all platform archives exist;
7. all checksum files exist;
8. checksums verify;
9. archive layout is correct;
10. version output matches tag;
11. installer succeeds for latest;
12. installer succeeds for pinned version.
