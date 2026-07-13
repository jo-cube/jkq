# Usage

`jkq` has one mode: consume one Kafka topic through direct partition
assignment, transform JSON values, and write record data to stdout. Run
`jkq --help` for the complete option list.

## Assignment and Ranges

A broker list, topic, and at least one partition are required:

```sh
jkq -b localhost:9092 -t events -p 0 -p 1
```

`-p` is repeatable. Duplicate and negative partitions are rejected. `jkq`
does not join a consumer group or commit offsets; a configured `group.id` is
only passed to librdkafka.

The default start is `beginning`. `-o, --offset` accepts:

| Form | Meaning |
|---|---|
| `beginning` | partition low watermark |
| `end` | current high watermark |
| `42` | absolute offset 42 |
| `-100` | 100 records before the current high watermark, clamped to the low watermark |
| `s@1720000000000` | first offset at or after this Unix timestamp in milliseconds |
| `e@1720000000000` | exclusive timestamp end |

A timestamp with no matching record resolves to the current high watermark.

`--end-offset <offset>` sets an exclusive offset end for every selected
partition. It cannot be combined with `e@...`.

Termination controls:

- `-c, --count <n>` stops after admitting `n` input records across all
  partitions. Tombstones and records later dropped still count.
- `-e, --exit-at-end` exits after all partitions reach their current ends and
  admitted work drains.
- `--snapshot` captures each startup high watermark as a fixed exclusive end.
  Records appended after capture are not included.
- An explicit end or snapshot implies `--exit-at-end`.

`--snapshot` cannot be combined with an explicit end. All start and end
positions apply to every selected partition.

## Transforms

Transform options are compiled before consumption:

```sh
jkq -b localhost:9092 -t events -p 0 --snapshot \
  --drop-if '.tenant != "acme"' \
  --tombstone-if '.deleted == true' \
  --project '{id: .id, customer: .customer.id}'
```

For each non-tombstone input, `jkq`:

1. evaluates repeated `--drop-if` predicates in command-line order;
2. evaluates repeated `--tombstone-if` predicates in command-line order;
3. applies the optional `--project` expression;
4. otherwise passes the exact source bytes through.

Each predicate list stops at its first true result. An existing Kafka
tombstone bypasses predicates and projection.

See [expression-language.md](expression-language.md) for the supported
language. It is jq-inspired but intentionally much smaller.

## Output

stdout contains record data only. Diagnostics and statistics go to stderr.

The default format is `%s\n`. `-f, --format` accepts these placeholders:

| Placeholder | Value |
|---|---|
| `%o` | source offset |
| `%k` | source key bytes |
| `%K` | source key byte length, or `-1` for null |
| `%s` | post-transform payload bytes |
| `%S` | post-transform payload byte length, or `-1` for a tombstone |
| `%R` | four-byte big-endian signed payload length |
| `%t` | source topic |
| `%p` | source partition |
| `%T` | source timestamp in milliseconds, or `-1` |
| `%h` | source headers |
| `%%` | literal `%` |

Format literals support `\n`, `\r`, `\t`, `\\`, and `\xNN`. Unsupported or
incomplete placeholders and escapes fail before consumption.

`%h` writes comma-separated `name=value` pairs in source order. A null header
value is `NULL`; an empty value writes nothing after `=`. This representation
is not binary-safe.

Payload lengths distinguish values that look identical through `%s`:

| Payload | `%s` | `%S` | `%R` |
|---|---|---:|---|
| Kafka or generated tombstone | empty | `-1` | signed big-endian `-1` |
| empty byte payload | empty | `0` | signed big-endian `0` |
| source or projected JSON `null` | `null` | `4` | signed big-endian `4` |

`%R` rejects a payload larger than `i32::MAX` bytes.

### JSON envelopes

`-J, --json-envelope` writes one compact, newline-terminated JSON object per
emitted record and cannot be combined with `-f`:

```json
{"topic":"events","partition":0,"offset":42,"timestamp":null,"timestampType":null,"key":"key","keyEncoding":"utf8","keyLength":3,"headers":[],"action":"project","payload":"{\"id\":1}","payloadEncoding":"utf8","payloadLength":8}
```

Keys, header values, and payloads are strings rather than embedded JSON.
Valid UTF-8 uses encoding `"utf8"`; other bytes use RFC 4648 base64 and
encoding `"base64"`. Null uses a JSON null value, null encoding, and length
`-1`. This keeps pass-through bytes exact and represents invalid JSON handled
with the `pass` policy without changing the schema.

Available timestamp types are `"createTime"` and `"logAppendTime"`. Envelope
headers contain `name`, `value`, `valueEncoding`, and `valueLength` in source
order. Envelope field order and framing are compatibility-sensitive and covered
by byte-exact tests.

`-u, --unbuffered` flushes after every emitted record. Use it only when latency
matters more than throughput.

## Error Policies

Errors fail the process by default. Per-record policies can convert supported
failures into an explicit action:

| Option | Values |
|---|---|
| `--on-invalid-json` | `fail`, `drop`, `tombstone`, `pass` |
| `--on-eval-error` | `fail`, `drop`, `tombstone` |
| `--on-kafka-error` | `fail`, `continue` |

`pass` preserves the original payload exactly. JSON nested more than 128
arrays or objects is handled by the invalid-JSON policy. Fatal Kafka state
errors remain fatal even when `--on-kafka-error continue` is selected.

When `--on-invalid-json` is omitted and no expression needs JSON, the identity
path does not parse payloads. Supplying the option explicitly forces JSON
validation, including for an otherwise identity transform.

## Kafka Configuration

`-F, --config <path>` reads librdkafka properties as `key=value` lines. Blank
lines and lines beginning with `#` are ignored. `-X, --property key=value` is
repeatable; later values replace earlier ones. Dedicated `-b` brokers take
precedence over both. For `-X`, everything after the first `=` is the property
value, including surrounding whitespace.

`jkq` owns these properties and rejects attempts to set them:

```text
enable.auto.commit
enable.auto.offset.store
enable.partition.eof
```

There is no implicit configuration-file discovery.

## Parallelism and Memory

`-j, --jobs` controls JSON compute workers. The default is available CPU
parallelism minus two, with a minimum of one. Identity transforms bypass the
worker pool.

Output remains ordered within each partition. `--unordered` emits transformed
records as workers complete and removes that guarantee.

These limits bound admitted work:

| Option | Default |
|---|---:|
| `--max-inflight-records` | `1024` |
| `--max-inflight-bytes` | `256MiB` |
| `--max-inflight-per-partition` | `256` |

Sizes accept bytes or `KiB`, `MiB`, and `GiB`. The byte charge conservatively
covers owned input, required parsing copies, projected output, keys, and
headers. A record larger than the byte budget may run alone. When limits are
reached, `jkq` pauses affected partitions while continuing to serve Kafka
events.

## Statistics, Signals, and Exit Status

`--stats` writes a final report to stderr. `--stats-interval` also writes
periodic reports and accepts positive integer durations such as `500ms`, `5s`,
and `1m`. `-q, --quiet` suppresses non-error diagnostics but not explicitly
requested statistics.

The first `SIGINT` or `SIGTERM` stops admission, drains admitted records, and
flushes stdout. A second termination signal exits immediately. A downstream
broken pipe is normal, quiet termination.

| Outcome | Exit code |
|---|---:|
| completed range or broken pipe | `0` |
| runtime, Kafka, transform, or output failure | `1` |
| command-line, configuration, or expression error | `2` |
| interrupted by `SIGINT` | `130` |
| terminated by `SIGTERM` | `143` |
