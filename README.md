# jkq

`jkq` is a Kafka JSONata runner. It consumes explicitly selected Kafka
partitions, evaluates native [JSONata](https://jsonata.org/) over JSON record
values, and writes record data to stdout. It is built for shell pipelines that
need Kafka metadata, predictable ordering, and JSON query or transformation
without a JavaScript runtime.

```sh
jkq -b localhost:9092 -t events -p 0 --snapshot \
  --drop-if 'environment != "production"' \
  --project '{"id": id, "total": $sum(items.price)}' \
  -f '%p\t%o\t%R%s\n'
```

One input record produces one of four actions:

- **drop**: write nothing;
- **tombstone**: retain the source metadata with a null Kafka payload;
- **pass**: preserve the exact source value bytes;
- **project**: write compact JSON produced by JSONata.

Kafka tombstones bypass JSON parsing and JSONata evaluation and remain
tombstones. Records stay in source order within each partition by default;
there is no global order across partitions.

## What It Is For

Use `jkq` to:

- inspect or export a bounded range from a large JSON topic;
- filter records with JSONata before sending them to another command;
- preserve source keys, offsets, timestamps, and headers;
- emit compact JSONata projections instead of complete documents;
- retain tombstone semantics in text or binary-framed output.

`jkq` is deliberately not a producer, consumer-group client, Kafka
administration suite, service, or general stream-processing framework. It
consumes one topic per invocation through direct partition assignment.

## Build

Install stable Rust and the native tools required to build bundled librdkafka
and OpenSSL, then run:

```sh
cargo build --release --locked
```

On Unix, this normally requires a C compiler, `make`, Perl, and `pkg-config`.
The executable is written to `target/release/jkq`.

Tagged GitHub releases provide binary archives and SHA-256 checksums for Linux
amd64, Linux arm64, and macOS arm64. There is no installer.

## Quick Use

Consume a fixed snapshot of two explicitly selected partitions:

```sh
jkq -b localhost:9092 -t events -p 0-1 --snapshot
```

Turn logical deletions into Kafka tombstones while preserving the key:

```sh
jkq -b localhost:9092 -t events -p 0 --snapshot \
  --tombstone-if 'deleted = true' -f '%K%k%R%s'
```

Pass a strict JSON object to every expression as `$vars`:

```sh
jkq -b localhost:9092 -t events -p 0 --snapshot \
  --vars '{"tenant":"acme","cutoff":1000}' \
  --drop-if 'tenant != $vars.tenant' \
  --project '{"id": id, "large": amount >= $vars.cutoff}'
```

Write metadata and the post-transform payload as newline-delimited JSON
envelopes:

```sh
jkq -b localhost:9092 -t events -p 0 --snapshot -J
```

The default output is `%s\n`. Use `%R%s` or another explicit frame when
payloads may contain newlines or arbitrary bytes.

## Documentation

- [Usage](docs/usage.md): offsets, transforms, output, errors, and runtime
  controls.
- [JSONata integration](docs/expression-language.md): jkq's predicate,
  projection, variables, result, and error contracts.
- [Architecture](docs/architecture.md): data flow, ownership, ordering, and
  backpressure.
- [Development](docs/development.md): repository shape, checks, tests, and
  contribution boundaries.

## Development

Run the repository checks with:

```sh
make check
```

See [development.md](docs/development.md) before changing observable behavior
or the data path.
