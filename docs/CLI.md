# Command-Line Specification

## 1. Invocation Shape

```text
<binary> [Kafka options] [assignment options] [transform options] [output options] [runtime options]
```

The program has one mode: consume directly assigned Kafka partitions, transform JSON values, and write records.

There is no producer-mode selector and no consumer-group selector.

## 2. Required Options

### `-b, --brokers <brokers>`

Comma-separated bootstrap broker list.

May also be supplied through the corresponding librdkafka property. The dedicated option has higher precedence.

### `-t, --topic <topic>`

Exactly one topic.

An empty topic is rejected.

### `-p, --partition <partition>`

Partition identifier. Repeat to consume more than one partition.

At least one is required. Duplicate values are rejected.

## 3. Offset and Termination Options

### `-o, --offset <position>`

Defines a start or timestamp boundary.

Supported forms:

```text
beginning
end
<absolute-offset>
-<count-from-end>
s@<unix-milliseconds>
e@<unix-milliseconds>
```

Rules:

- `beginning` starts at the low watermark.
- `end` starts at the current high watermark.
- a non-negative integer is an absolute start offset.
- a negative integer is relative to the current high watermark.
- `s@...` resolves the first offset at or after the timestamp.
- `e@...` resolves an exclusive end boundary.
- a relative start that precedes the low watermark is clamped to the low watermark.
- a timestamp with no matching record resolves to the current high watermark.
- one start form is allowed;
- one end form is allowed;
- repeated partition-specific offsets are not supported initially;
- a start applies to every selected partition.

Default start: `beginning`.

### `--end-offset <offset>`

Optional explicit exclusive end offset applied to every selected partition.

This is clearer than overloading repeated `-o` for non-timestamp ranges.

It is mutually exclusive with `e@...`.

### `-c, --count <count>`

Stop after admitting this many Kafka input records across all partitions.

- Must be positive.
- Includes input tombstones.
- Includes records later dropped.
- Drains admitted work before exit.

### `-e, --exit-at-end`

Exit when all selected partitions reach their effective ends and admitted work drains.

Required for unbounded direct consumption to terminate on current EOF.

Implied by `--snapshot`, `--end-offset`, and `e@...`.

### `--snapshot`

Capture each selected partition's startup high watermark as its exclusive end.

May be combined with any supported start position.

An explicit end offset or timestamp is rejected when snapshot is selected because it would create competing boundaries.

## 4. Transform Options

### `--drop-if <predicate>`

Drop the record when the predicate is true.

Repeatable. Predicates are evaluated in command-line order and short-circuit at the first true result.

### `--tombstone-if <predicate>`

Emit a tombstone when the predicate is true.

Repeatable. Evaluated after all drop predicates and before projection.

### `--project <expression>`

Emit compact JSON produced by the expression when no earlier predicate terminates the record.

At most one projection is allowed.

When omitted, an unmatched valid JSON record passes through with exact source value bytes.

## 5. Output Options

### `-f, --format <format>`

Compile and use a kcat-style format string.

Supported placeholders:

| Placeholder | Meaning |
|---|---|
| `%o` | source offset |
| `%k` | source key bytes |
| `%K` | key byte length, `-1` for null |
| `%s` | post-transform payload bytes |
| `%S` | payload byte length, `-1` for tombstone |
| `%R` | four-byte big-endian signed payload length |
| `%t` | source topic |
| `%p` | source partition |
| `%T` | source timestamp milliseconds or `-1` |
| `%h` | source headers |
| `%%` | literal `%` |

Example:

```sh
<binary> \
  -b localhost:9092 \
  -t events \
  -p 0 \
  --snapshot \
  --project '{id: .id, status: .status}' \
  -f '%p\t%o\t%K\t%k\t%S\t%s\n'
```

### `-J, --json-envelope`

Write one compact JSON envelope per emitted record.

Mutually exclusive with `-f`.

#### Schema

The envelope represents byte fields as JSON strings. Valid UTF-8 is emitted directly with encoding `"utf8"`; other bytes use RFC 4648 base64 with encoding `"base64"`. Null keys, header values, and payloads use JSON `null`, a null encoding, and length `-1`.

Fields are emitted in this order:

```json
{
  "topic": "events",
  "partition": 3,
  "offset": 42,
  "timestamp": null,
  "timestampType": null,
  "key": "key",
  "keyEncoding": "utf8",
  "keyLength": 3,
  "headers": [],
  "action": "project",
  "payload": "{\"id\":1}",
  "payloadEncoding": "utf8",
  "payloadLength": 8
}
```

UTF-8 JSON payload bytes are represented as a string, not embedded as a JSON value. This preserves exact pass-through bytes, including whitespace and object order, and permits `--on-invalid-json pass` without a second envelope schema. `timestampType` is `"createTime"`, `"logAppendTime"`, or null. Each header contains `name`, `value`, `valueEncoding`, and `valueLength` in source order. Envelopes are compact and newline-terminated.

### Default output

When neither `-f` nor `-J` is selected, default to:

```text
%s\n
```

This default is convenient but not binary-safe for arbitrary projected strings containing newlines. Users needing binary-safe output should use `%R%s` or another explicit frame.

### `-u, --unbuffered`

Flush stdout after each emitted record.

This reduces throughput and should be used only for interactive or latency-sensitive workflows.

### `--stats`

Write final statistics to stderr.

### `--stats-interval <duration>`

Write periodic statistics to stderr. A final report is still written.

Supported duration syntax should be simple and documented, such as `500ms`, `5s`, and `1m`. Implement duration parsing without adding a dependency unless that makes error handling materially worse.

### `-q, --quiet`

Suppress non-error diagnostics. Does not suppress explicitly requested statistics.

## 6. Runtime Options

### `-j, --jobs <count>`

Number of compute workers.

- Minimum: `1`.
- Default: `max(1, available_parallelism - 2)`.
- A practical upper bound may be enforced to prevent accidental resource exhaustion.

### `--unordered`

Write records as processing completes.

Disables per-partition ordering. Does not allow byte interleaving.

### `--max-inflight-records <count>`

Global maximum admitted records not yet fully drained.

Default: `1024`.

### `--max-inflight-bytes <size>`

Global retained byte budget.

Size syntax should support binary units such as `MiB` and `GiB`, with plain integers interpreted as bytes.

Default: `256MiB`.

### `--max-inflight-per-partition <count>`

Maximum admitted, not-yet-drained records for one partition.

Default: `256`. This value cannot exceed the global record limit.

### `--worker-buffer-retain <size>`

Optional advanced limit for worker scratch buffer capacity retained after a record. This may remain hidden or undocumented until a measured need exists.

## 7. Error Policy Options

### `--on-invalid-json <policy>`

Values:

```text
fail
drop
tombstone
pass
```

Default: `fail`.

### `--on-eval-error <policy>`

Values:

```text
fail
drop
tombstone
```

Default: `fail`.

### `--on-kafka-error <policy>`

Values:

```text
fail
continue
```

Default: `fail`.

Policies are applied per input record where possible. Fatal consumer-state errors cannot be safely continued and remain fatal.

## 8. Kafka Configuration

### `-F, --config <path>`

Load librdkafka properties from a file.

File format:

```text
key=value
```

Blank lines and lines beginning with `#` are ignored.

Malformed lines report their line number.

### `-X, --property <key=value>`

Set a librdkafka property. Repeatable.

Later repeated values for the same key override earlier ones.

The runtime always sets `enable.auto.commit=false`, `enable.auto.offset.store=false`, and `enable.partition.eof=true`. These properties are owned by the direct-assignment and termination model and cannot be overridden through `-F` or `-X`. When `group.id` is absent, the runtime supplies `jkq`; a configured group identifier is accepted but no group subscription or offset commit occurs.

### Default config discovery

If no explicit file is supplied, a future default path may be supported. Do not add implicit discovery until the environment-variable and precedence contract is documented and tested.

## 9. Information Options

### `--version`

Print version and exit.

### `--help`

Print concise usage and exit.

A longer manual should live in repository documentation rather than making terminal help excessively large.

## 10. Validation Rules

Reject before consumption:

- missing broker, topic, or partition;
- duplicate partitions;
- unsupported offset syntax;
- conflicting start or end forms;
- snapshot with explicit end;
- `-J` with `-f`;
- zero jobs;
- zero count;
- zero memory or record limits;
- malformed expression;
- multiple projections;
- unsupported format placeholder;
- `%R` combined with a configured maximum message size beyond its representable range only if this can be known statically;
- `--on-invalid-json pass` when the selected execution plan cannot preserve original bytes, unless the plan automatically switches to preserving them.

## 11. Action and Output Examples

### Drop

```sh
<binary> ... --drop-if '.tenant != "acme"'
```

A matching record produces no bytes, regardless of the selected format.

### Tombstone

```sh
<binary> ... \
  --tombstone-if '.deleted == true' \
  -f '%K%k%R%s'
```

The payload frame contains signed length `-1` and no payload bytes.

### Pass through

```sh
<binary> ... \
  --drop-if '.ignored == true' \
  -f '%s\n'
```

Unmatched records preserve exact source JSON bytes, including whitespace and object key order.

### Project

```sh
<binary> ... \
  --project '{id: .id, customer: .customer.id}' \
  -f '%s\n'
```

Projection output is compact JSON.

## 12. Tombstone and Empty-Value Semantics

| Source or result | `%s` | `%S` | `%R` |
|---|---|---:|---|
| Kafka tombstone | empty | `-1` | big-endian `-1` |
| generated tombstone | empty | `-1` | big-endian `-1` |
| empty byte value | empty | `0` | big-endian `0` |
| JSON text `null` | `null` | `4` | big-endian `4` |
| projected JSON `null` | `null` | `4` | big-endian `4` |

## 13. Header Rendering

`%h` renders headers in source order:

```text
name=value,name=NULL,name=
```

- null header value becomes `NULL`;
- empty header value is empty after `=`;
- duplicate names are retained;
- bytes are written directly and may not be safe for arbitrary binary headers.

## 14. Exit Behavior

| Outcome | Exit code |
|---|---:|
| completed requested range | 0 |
| normal downstream broken pipe | 0 |
| runtime/Kafka/processing/output failure | 1 |
| CLI/config/expression error | 2 |
| interrupted before completion | 130 |
| terminated before completion | 143 |

Exact signal behavior should be frozen by process integration tests.

## 15. Compatibility Testing

For overlapping behavior, compare against kcat for:

- offset starts;
- count and EOF termination;
- key and payload null lengths;
- format escapes and placeholders;
- timestamp and header rendering;
- direct partition output.

Document any intentional deviation rather than hiding it.
