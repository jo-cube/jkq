# Documentation Map

This directory is the authoritative design set for the project.

| Document | Purpose |
|---|---|
| [PRD](PRD.md) | Product goals, scope, requirements, and acceptance criteria |
| [HLD](HLD.md) | System boundaries, component model, runtime topology, and major flows |
| [LLD](LLD.md) | Concrete modules, types, algorithms, state machines, and failure handling |
| [CLI](CLI.md) | Command-line contract, option semantics, output behavior, and exit codes |
| [Expression language](EXPRESSION_LANGUAGE.md) | Restricted predicate and projection grammar and semantics |
| [Testing](TESTING.md) | Test strategy, fixture design, integration approach, and benchmarks |
| [Releases](RELEASES.md) | Versioning, release assets, checksums, supported platforms, and installer contract |
| [Decisions](DECISIONS.md) | Accepted design decisions and deliberately deferred work |

The root `AGENTS.md` contains repository-wide contribution instructions.

## Authority

When requirements conflict, use this order:

1. explicit decisions in `DECISIONS.md`;
2. observable product behavior in `PRD.md` and `CLI.md`;
3. expression semantics in `EXPRESSION_LANGUAGE.md`;
4. architecture in `HLD.md`;
5. implementation detail in `LLD.md`;
6. testing and release guidance.

A later intentional decision should update every affected document in the same change.
