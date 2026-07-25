# jkq

`jkq` is a bounded Kafka JSON processor for shell pipelines. It directly
assigns Kafka partitions, filters or reshapes JSON record values, preserves
Kafka metadata and tombstones, and streams record data to stdout with
predictable per-partition ordering.

Filtering and projection expressions use [JSONata](https://jsonata.org/).

```sh
jkq -b localhost:9092 -t events -p 0 --snapshot \
  --drop-if 'environment != "production"' \
  --project '{"id": id, "total": $sum(items.price)}' \
  -f '%p\t%o\t%R%s\n'
```

A successfully processed input record produces one of four actions:

- **drop**: write nothing;
- **tombstone**: retain the source metadata with a null Kafka payload;
- **pass**: preserve the exact source value bytes unless a JSON-value envelope
  is requested;
- **project**: write a compact JSON result.

Kafka tombstones bypass JSON parsing and expression evaluation and remain
tombstones by default. `--drop-tombstones` drops them, as well as records
selected by `--tombstone-if`, before projection. Records stay in source order
within each partition by default; there is no global order across partitions.

## What It Is For

Use `jkq` to:

- inspect or export a bounded range from a large JSON topic;
- filter records before sending them to another command;
- preserve source keys, offsets, timestamps, and headers;
- emit compact projections instead of complete documents;
- retain tombstone semantics in text or binary-framed output.

`jkq` is deliberately not a producer, consumer-group client, Kafka
administration suite, service, or general stream-processing framework. It
consumes one topic per invocation through direct partition assignment.

## Install

Install the latest release to `~/.local/bin`:

```sh
curl -fsSL https://raw.githubusercontent.com/jo-cube/jkq/main/scripts/install.sh | sh
```

Install to a custom directory:

```sh
curl -fsSL https://raw.githubusercontent.com/jo-cube/jkq/main/scripts/install.sh | sh -s -- "$HOME/bin"
```

Install a specific version:

```sh
curl -fsSL https://raw.githubusercontent.com/jo-cube/jkq/main/scripts/install.sh | VERSION=v0.3.0 sh
```

Release archives are verified with SHA-256 checksums. Published platforms:

- `linux/amd64`
- `linux/arm64`
- `darwin/arm64`

## Build from Source

Install stable Rust and the native tools required to build bundled librdkafka
and OpenSSL, then run:

```sh
cargo build --release --locked
```

On Unix, this normally requires a C compiler, `make`, Perl, `pkg-config`, and
libclang. The executable is written to `target/release/jkq`.

The bundled Kafka client supports gzip, Snappy, LZ4, and Zstandard-compressed
record batches.

Other platforms can build from source.

## Quick Use

Consume a fixed snapshot of every current topic partition:

```sh
jkq -b localhost:9092 -t events --snapshot
```

Add `--consumers 3` to poll disjoint partition subsets with three Kafka
consumers when one consumer does not saturate a multi-partition topic.

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

Add `--envelope-payload value` to embed the post-transform payload as JSON
instead of a JSON string.

The default output is `%s\n`. Use `%R%s` or another explicit frame when
payloads may contain newlines or arbitrary bytes.

## Documentation

- [Usage](docs/usage.md): offsets, transforms, output, errors, and runtime
  controls.
- [High-throughput patterns](docs/usage.md#high-throughput-patterns): large
  policy sets, output framing, partition scaling, and bounded validation.
- [Expression language](docs/expression-language.md): predicates, projections,
  variables, results, and errors.
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
