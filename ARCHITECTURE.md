# Architecture

`jkq` compiles configuration before consumption begins:

```text
raw CLI and Kafka properties
→ resolved configuration
→ compiled transform plan and output plan
→ Kafka poll/admission (deferred)
→ bounded compute workers (deferred)
→ per-partition completion ordering (deferred)
→ single output writer (deferred)
```

The implemented transform boundary owns a positioned handwritten parser, deduplicated path slots, strict evaluation, and a `simd-json` backend. Kafka tombstones bypass parsing. Valid unmatched input retains exact source bytes; projections serialize compact JSON in expression field order.

The output boundary compiles format strings once and operates on source metadata plus the post-transform payload. It distinguishes tombstones from empty bytes and JSON `null`. JSON envelopes use a golden-tested byte representation described in [the low-level design](docs/LLD.md#17-json-envelope).

The planned poller, bounded channels, completion frontier, snapshot boundaries, and shutdown rules are specified in the [high-level design](docs/HLD.md) and [low-level design](docs/LLD.md). No placeholder runtime modules exist yet.

