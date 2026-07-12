# Architecture

`jkq` compiles configuration before consumption begins:

```text
raw CLI and Kafka properties
→ resolved configuration
→ compiled transform plan and output plan
→ direct Kafka assignment and offset resolution
→ owned-record polling with fixed snapshot boundaries
→ record- and byte-aware admission
→ bounded compute workers
→ per-partition completion ordering
→ single compiled output writer
```

The implemented transform boundary owns a positioned handwritten parser, deduplicated path slots, strict evaluation, and a `simd-json` tape backend. Kafka tombstones bypass parsing. Valid unmatched input retains exact source bytes; projections serialize compact JSON in expression field order. Parsed JSON is limited to 128 nesting levels and over-limit records follow the invalid-JSON policy.

The output boundary compiles format strings once and operates on source metadata plus the post-transform payload. It distinguishes tombstones from empty bytes and JSON `null`. JSON envelopes use a golden-tested byte representation described in [the low-level design](docs/LLD.md#17-json-envelope).

`src/kafka.rs` owns librdkafka configuration, direct assignment, watermark and timestamp lookup, snapshot capture, EOF handling, pause/resume, and the borrowed-to-owned record boundary. `src/runtime.rs` owns bounded channels, workers, the writer, statistics, and shutdown; `src/runtime/state.rs` holds admission and completion-frontier state. `src/app.rs` only assembles those boundaries and maps process outcomes.

The poller is the only thread that calls Kafka. Retained charges are released only after a completion crosses its partition frontier and is written or dropped. Slow output therefore propagates pressure back to Kafka without allowing reorder buffers to grow independently.

The [high-level design](docs/HLD.md) and [low-level design](docs/LLD.md) contain the detailed ownership, ordering, and shutdown contracts.
