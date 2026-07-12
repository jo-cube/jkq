# jkq

`jkq` is a Rust command-line consumer for explicitly selected Kafka partitions whose record values are JSON. It applies a restricted predicate and projection language, then emits dropped, tombstoned, exact pass-through, or compact projected records with kcat-style formatting.

The current implementation directly assigns Kafka partitions, resolves documented start and end positions, captures fixed snapshots, copies borrowed messages into owned records, applies the compiled transform, and writes formatted output. This path is deliberately single-threaded. Bounded parallel workers, backpressure, signal handling, and graceful draining are the next runtime slice.

## Examples

Project selected fields after filtering:

```sh
jkq -b localhost:9092 -t events -p 0 --snapshot \
  --drop-if '.environment != "production"' \
  --project '{id: .id, owner: coalesce(.owner.name, "unknown")}' \
  -f '%p\t%o\t%R%s\n'
```

Preserve exact source JSON unless the record represents deletion:

```sh
jkq -b localhost:9092 -t events -p 0 \
  --tombstone-if '.deleted == true' -f '%K%k%R%s'
```

## Non-goals

`jkq` is not a producer, consumer-group client, Kafka administration suite, service, or complete jq implementation. One input record produces at most one output record.

## Documentation

- [Product requirements](docs/PRD.md)
- [Architecture](ARCHITECTURE.md)
- [High-level design](docs/HLD.md)
- [Low-level design](docs/LLD.md)
- [CLI](docs/CLI.md)
- [Expression language](docs/EXPRESSION_LANGUAGE.md)
- [Testing](docs/TESTING.md)
- [Release process](docs/RELEASES.md)
- [Accepted decisions](docs/DECISIONS.md)

Run the normal repository checks with `make check`.

## Current runtime limits

The Stage 2 runtime admits and completes one record at a time, so partition order is preserved without a reorder buffer. Worker-count, unordered-mode, in-flight-limit, and statistics options are validated and retained in resolved configuration but do not change execution until the bounded worker runtime is implemented.
