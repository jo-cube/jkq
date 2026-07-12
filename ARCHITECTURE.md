# Architecture

`jkq` compiles configuration before consumption begins:

```text
raw CLI and Kafka properties
→ resolved configuration
→ compiled transform plan and output plan
→ direct Kafka assignment and offset resolution
→ owned-record polling with fixed snapshot boundaries
→ synchronous transform execution
→ per-partition completion ordering (deferred)
→ compiled output writer
```

The implemented transform boundary owns a positioned handwritten parser, deduplicated path slots, strict evaluation, and a `simd-json` backend. Kafka tombstones bypass parsing. Valid unmatched input retains exact source bytes; projections serialize compact JSON in expression field order.

The output boundary compiles format strings once and operates on source metadata plus the post-transform payload. It distinguishes tombstones from empty bytes and JSON `null`. JSON envelopes use a golden-tested byte representation described in [the low-level design](docs/LLD.md#17-json-envelope).

`src/kafka.rs` owns librdkafka configuration, direct assignment, watermark and timestamp lookup, snapshot capture, EOF handling, and the borrowed-to-owned record boundary. `src/app.rs` assembles that source with the existing transform and output modules. It processes one admitted record to completion before polling the next, which preserves per-partition order and bounds retained records without introducing the Stage 3 concurrency machinery.

The planned bounded channels, byte-aware admission, completion frontier, signals, and shutdown rules remain specified in the [high-level design](docs/HLD.md) and [low-level design](docs/LLD.md).
