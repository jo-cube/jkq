# jkq

`jkq` is a Rust command-line consumer for explicitly selected Kafka partitions whose record values are JSON. It applies a restricted predicate and projection language, then emits dropped, tombstoned, exact pass-through, or compact projected records with kcat-style formatting.

The repository currently contains the first implementation slice: validated CLI configuration, expression parsing and compilation, `simd-json` evaluation, format compilation, JSON envelopes, and focused tests. Kafka polling, concurrency, ordering, and snapshot execution remain to be implemented; a valid invocation exits with a clear runtime-not-implemented error rather than pretending to consume.

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

