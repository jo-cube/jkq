# Usage

`jkq` consumes one Kafka topic through direct partition assignment, transforms
JSON values, and writes record data to stdout. Run `jkq --help` for the complete
option list.

`--check` validates the complete local invocation and exits without creating a
Kafka consumer. It checks required arguments, option combinations, partition
selection, config-file syntax, expressions, and output formats. It does not
test broker reachability or ask librdkafka to validate property values.

## Assignment and Ranges

A broker list, topic, and at least one partition are required:

```sh
jkq -b localhost:9092 -t events -p 0,2,4-7 -p 9
```

`-p` accepts comma-separated partitions and inclusive ascending ranges, and is
repeatable. Selection order is preserved but does not create a cross-partition
output order. Duplicate, descending, negative, and empty selections are
rejected. A selection may expand to at most 100,000 partitions. `jkq` does not
join a consumer group or commit offsets; a configured `group.id` is only passed
to librdkafka.

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
An unavailable absolute offset is an error; `jkq` never silently resets it to
the beginning or end of the partition.

`--end-offset <offset>` sets an exclusive offset end for every selected
partition. It cannot be combined with `e@...`.

Termination controls:

- `-c, --count <n>` stops after admitting `n` input records across all
  partitions. Tombstones and records later dropped still count.
- `--count-per-partition <n>` stops each partition after admitting `n` input
  records from that partition. It cannot be combined with `--count`; tombstones
  and records later dropped still count.
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
  --vars '{"tenant":"acme","cutoff":1000,"statuses":["open","pending"]}' \
  --drop-if 'tenant != $vars.tenant or $not(status in $vars.statuses)' \
  --tombstone-if 'deleted = true' \
  --project '{
    "id": id,
    "size": amount >= $vars.cutoff ? "large" : "small",
    "owner": owner.name ?? owner.id ?? "unknown"
  }'
```

All expressions use native [JSONata](https://jsonata.org/) syntax and semantics.
`--vars <object>` accepts one strict JSON object and binds it as `$vars` for
every expression. `--vars-file <path>` reads the same object from a UTF-8 file;
the two options are mutually exclusive. File errors, invalid JSON, and
non-object roots are startup errors and are also rejected by `--check`.
Expressions access values through paths such as `$vars.tenant`,
`$vars.policy.cutoff`, and `$vars["non-identifier"]`. Referencing `$vars`
without supplying it evaluates as JSONata `Undefined`.

For each non-tombstone input, `jkq`:

1. evaluates repeated `--drop-if` predicates in command-line order;
2. evaluates repeated `--tombstone-if` predicates in command-line order;
3. applies the optional `--project` expression;
4. otherwise passes the exact source bytes through.

Each predicate list stops at its first Boolean `true` result. The top-level
result of an action predicate must be a JSONata Boolean; JSONata truthiness is
not applied at this external boundary. An existing Kafka tombstone bypasses
JSON parsing, predicates, and projection.

Projection results are compact JSON. `Undefined`, functions, regular
expressions, nested non-JSON values, and serialization failures are evaluation
errors. A result sequence with multiple items is one JSON array payload, never
multiple output records. See [expression-language.md](expression-language.md)
for the complete jkq-to-JSONata integration contract and links to the language
reference.

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
| `%a` | emitted action: `tombstone`, `pass`, or `project` |
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

`pass` preserves the original payload exactly. Fatal Kafka state errors and
unavailable requested offsets remain fatal even when `--on-kafka-error
continue` is selected.

JSONata parse errors are command-line errors. Runtime evaluation errors name
the failing drop predicate, tombstone predicate, or projection. The pipeline
adds topic, partition, and offset context. `jkq` does not automatically add
source payload contents, but native messages deliberately produced by JSONata
expressions, such as `$error()` and `$assert()` messages, are preserved and may
contain record data.

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
auto.offset.reset
enable.auto.commit
enable.auto.offset.store
enable.partition.eof
```

TLS client authentication uses the standard librdkafka property names, so the
same connection settings used with kcat can be supplied in a configuration
file:

```properties
security.protocol=SSL
ssl.ca.location=/path/to/ca.pem
ssl.certificate.location=/path/to/client.pem
ssl.key.location=/path/to/client.key
```

```sh
jkq -F kafka.properties -t events -p 0 --snapshot
```

The official build includes TLS support through vendored OpenSSL. Additional
TLS properties can be passed through with `-F` or `-X` unless listed as owned
above.

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
| `--max-inflight-records` | `8192` |
| `--max-inflight-bytes` | `256MiB` |
| `--max-inflight-per-partition` | `8192` |

Sizes accept bytes or `KiB`, `MiB`, and `GiB`. `--max-inflight-bytes` is an
admission budget for owned source record bytes and copied source metadata: the
payload, key, header names, and header values required by the output plan. It
does not account for jsonata-core's parsed value tree, evaluation
intermediates, or projected output. Full JSONata can construct data-dependent
results, so those allocations are not statically bounded by this option.

A source record larger than the byte budget may run alone. When admission
limits are reached, `jkq` pauses affected partitions while continuing to serve
Kafka events. Channels, admitted record counts, per-partition admission, and
source-byte accounting remain bounded.

## High-throughput Patterns

High-throughput runs benefit most from keeping the work per input record
predictable. Build with `--release`, test against representative records, and
change one setting at a time rather than assuming that more workers or larger
buffers will help.

### Use objects for large membership sets

JSONata defines [`in`](https://docs.jsonata.org/comparison-operators) as array
inclusion, which tests values in the right-hand array. For policy sets that may
grow to thousands of entries, encode each set as an object and use the standard
[`$lookup`](https://docs.jsonata.org/object-functions) function to look up the
concatenated key.

For example, `policy.json` can contain:

```json
{
  "whitelist": {
    "acme:123": true,
    "acme:456": true
  },
  "blacklist": {
    "blocked:999": true
  }
}
```

This predicate computes the key once, tombstones blacklisted records, lets the
whitelist override the blacklist, and passes records in neither set:

```sh
jkq -F kafka.properties -t events -p 0-31 --snapshot \
  --vars-file policy.json \
  --tombstone-if '(
    $key := tenant & ":" & account;
    ($lookup($vars.blacklist, $key) = true) and
      $not($lookup($vars.whitelist, $key) = true)
  )' \
  -f '%K\t%k%R%s'
```

The `true` values make membership explicit: a present key compares equal to
`true`, while a missing key does not. Change the Boolean expression when the
sets use different precedence. For example, to pass only whitelisted records
that are not blacklisted, use:

```jsonata
$not($lookup($vars.whitelist, $key) = true) or
  ($lookup($vars.blacklist, $key) = true)
```

Keep related policy in one expression when it shares derived values such as
`$key`. Separate repeated predicates remain useful when an early, cheap
predicate commonly matches and avoids later work. The JSONata
[`&` operator](https://docs.jsonata.org/other-operators) converts non-string
operands to strings; include separators or other disambiguation when different
attribute combinations could otherwise produce the same key.

The variables object is parsed once per worker rather than once per record.
Each worker holds its own copy, so account for the set size when increasing
`--jobs`.

### Preserve bytes and emit only required metadata

When predicates choose between pass and tombstone, omit `--project` unless the
payload must change. Passing preserves the exact source payload and avoids
projection serialization.

The example format above is compact and stream-decodable: `%K\t` writes a
decimal key length followed by a tab, `%k` writes that many key bytes, and
`%R%s` writes a signed four-byte payload length followed by the payload. A
payload length of `-1` represents a tombstone. Choose a different format when
the downstream protocol has its own framing.

Avoid `--unbuffered` on throughput-oriented runs. JSON envelopes are useful
when their self-describing metadata is required; a focused `-f` format avoids
encoding fields the downstream consumer does not use.

### Scale with partitions, then tune workers

The default worker count leaves one CPU for Kafka polling and one for output.
Measure nearby `--jobs` values with representative payloads and expressions;
additional workers can increase contention after polling or output becomes the
bottleneck.

Keep the default per-partition ordering when later records update earlier
records. Use `--unordered` only when completion order is acceptable. If one
invocation reaches its polling or output limit and the topic has enough
partitions, run independent invocations over disjoint partition sets:

```sh
jkq -F kafka.properties -t events -p 0-15 --snapshot \
  -f '%R%s' > shard-0.bin &
jkq -F kafka.properties -t events -p 16-31 --snapshot \
  -f '%R%s' > shard-1.bin &
wait
```

This preserves ordering within every partition; `jkq` never provides global
ordering across partitions. Adjust the in-flight limits only when measurements
show worker starvation or excessive retained memory.

### Validate a bounded slice first

Use the same expressions, variables, output format, and Kafka properties that
the full run will use. `--check` validates them without connecting to Kafka.
Then process a bounded sample before starting the complete range:

```sh
policy='(
  $key := tenant & ":" & account;
  ($lookup($vars.blacklist, $key) = true) and
    $not($lookup($vars.whitelist, $key) = true)
)'

jkq -F kafka.properties -t events -p 0-31 \
  --count-per-partition 100000 \
  --vars-file policy.json \
  --tombstone-if "$policy" \
  --stats-interval 10s \
  -f '%K\t%k%R%s' > sample.bin
```

Check the action counts, output framing, memory use, and downstream behavior.
Statistics add per-record bookkeeping, so compare with them disabled when
measuring maximum throughput.

For Kafka client tuning, use librdkafka's authoritative
[configuration reference](https://github.com/confluentinc/librdkafka/blob/v2.12.1/CONFIGURATION.md)
and [statistics reference](https://github.com/confluentinc/librdkafka/blob/v2.12.1/STATISTICS.md)
for the version currently bundled by jkq.
Pass measured configuration changes through `-F` or `-X`; avoid copying a
generic tuning profile without validating it against the brokers and payloads
used by the run.

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
