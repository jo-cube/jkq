# jkq

`jkq` is a Rust command-line consumer for explicitly selected Kafka partitions whose record values are JSON. It applies a restricted predicate and projection language, then emits dropped, tombstoned, exact pass-through, or compact projected records with kcat-style formatting.

The current implementation directly assigns Kafka partitions, resolves documented start and end positions, captures fixed snapshots, and runs owned records through a bounded compute pool. It restores source order within each partition before one writer emits output, pauses Kafka partitions under record or byte pressure, and drains admitted work on termination.

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

## Runtime limits

`--max-inflight-records`, `--max-inflight-bytes`, and `--max-inflight-per-partition` bound admitted work. A record larger than the byte budget runs alone. Kafka polling continues while assigned partitions are paused, and partitions resume below the documented low-water thresholds.
