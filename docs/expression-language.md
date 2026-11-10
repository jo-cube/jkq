# Expression Language

`jkq` uses JSONata as its expression language for drop predicates, tombstone
predicates, and projections. The
[JSONata documentation](https://docs.jsonata.org/) defines the language, and
[jsonata-js](https://github.com/jsonata-js/jsonata) is the semantic reference
implementation.

The runtime implementation is the public Rust API of
[`jx`](https://github.com/jo-cube/jx). Expressions use JSONata syntax directly:

```sh
--drop-if 'environment != "production"'
--tombstone-if 'deleted = true'
--project '{"id": id, "total": $sum(items.price)}'
```

JSONata uses bare input paths such as `customer.id`, `=` for equality,
`$`-prefixed built-ins, quoted object keys, and native conditionals, paths,
sequences, functions, variables, and assignments.

## Startup and Record Evaluation

Every configured JSONata expression is compiled once during CLI resolution.
A compilation failure is a command-line error and exits with status 2 before Kafka
consumption. `--check` performs the same compilation and variable validation
without creating a Kafka consumer.

For each non-tombstone input record that needs JSON, `jkq` validates the
payload and:

1. evaluates `--drop-if` and `--drop-unless` expressions together in
   command-line order, dropping the record at the first trigger;
2. evaluates `--tombstone-if` and `--tombstone-unless` expressions together
   in command-line order, tombstoning the record at the first trigger, or
   dropping it when `--drop-tombstones` is set;
3. evaluates `--project` for surviving records, when present;
4. otherwise passes through the source payload, preserving its exact bytes
   unless `--envelope-payload value` requests compact JSON serialization.

Workers validate each payload once into a borrowed `jx::RawJson` and reuse it
across expressions without constructing a JSON tree; see
[expression execution](architecture.md#expression-execution).

By default, existing Kafka tombstones bypass JSON parsing and every expression.
A source tombstone remains a tombstone by default and is dropped when
`--drop-tombstones` is set. A successfully evaluated input record produces one
action; a JSONata result sequence never expands into multiple jkq output
records.

## Source Metadata and Deferred JSON

`$meta` is an evaluation-local lexical binding. Bare paths, `$`, and `$$` keep
referring to the payload. Each predicate and projection receives a fresh
binding; local assignments, parameters, closures, and `$eval` follow normal
JSONata lexical rules. `$vars` remains an immutable compile-time binding.

| Field | Representation |
|---|---|
| `topic`, `partition` | source topic string and partition number |
| `offset` | canonical decimal **string**, preserving all signed 64-bit offsets |
| `timestamp` | Unix milliseconds, or JSON null when unavailable |
| `timestampType` | `"createTime"`, `"logAppendTime"`, or JSON null |
| `key`, `keyEncoding`, `keyLength` | same binary-safe fields as the JSON envelope |
| `headers` | ordered array of `{name, value, valueEncoding, valueLength}`; duplicate names retained |
| `valueLength` | original payload byte length; `-1` for a source tombstone, `0` for empty bytes |
| `isTombstone` | whether the source value was Kafka null, regardless of subsequent actions |

Keys and header values are UTF-8 strings with encoding `"utf8"` or padded
base64 strings with encoding `"base64"`. A null byte value has JSON null value
and encoding, and length `-1`; an empty value is `""`, `"utf8"`, and `0`.
Missing fields produce JSONata `Undefined`. Offset string equality is exact;
string ordering is lexicographic. Explicit `$number($meta.offset)` uses
JSONata binary64 arithmetic and may lose precision above 2^53.

```sh
--defer-json --drop-if '$meta.partition = 2' \
  --project '{"offset": $meta.offset, "length": $meta.valueLength}'
```

Without `--defer-json`, non-null payloads that need evaluation are validated
before any expression, and source tombstones bypass every expression.
`--defer-json` changes that contract explicitly:

- Reached expressions proven independent of the original focus and root run
  without input preparation, even over malformed JSON or arbitrary bytes.
- The first reached expression that may need input prepares the complete
  non-null payload once, shared across remaining expressions. Proof is
  conservative: uncertain closures, dynamic evaluation, and unsupported cases
  may require preparation even if they only use metadata in practice.
  A mixed payload/metadata expression is prepared as a unit; short-circuiting
  inside that expression does not defer preparation of its payload.
- Predicate stages and command-line ordering do not change. Earlier drops or
  tombstones skip all later preparation, expressions, and projection.
- Source tombstones participate with **absent input**, not JSON `null`.
  `$meta.isTombstone` is true; `$` and `$$` are missing. They can be projected
  into ordinary output records. `--drop-tombstones` still drops them first.
- Without projection, source bytes pass through exactly. A surviving tombstone
  remains a tombstone. A JSON-value envelope prepares a surviving non-null
  passthrough payload when it needs serialization; a projection is already JSON.

In deferred mode, `--on-invalid-json` selects the policy for preparation that
is actually reached; supplying it does not force early validation. An earlier
metadata action or evaluation error can therefore precede an invalid-JSON
error. Evaluation errors retain `--on-eval-error` behavior. JSON text `null`,
empty bytes, absent input, and Kafka tombstones remain distinct. Deferred mode
skips payload validation/acquisition and unnecessary serialization, but retains
source copying, admission accounting, and ownership until completion.

## Action Predicates

All four action predicates require exactly one JSONata Boolean result:

| Option | Triggers on |
|---|---|
| `--drop-if`, `--tombstone-if` | Boolean `true` |
| `--drop-unless`, `--tombstone-unless` | Boolean `false` |

Missing, null, numbers, strings, arrays, objects, functions, and regular
expressions are evaluation errors governed by `--on-eval-error`; `unless`
does not coerce them to false. Expressions are compiled directly without
rewriting through `$not`.

All options are repeatable. Mixed `if`/`unless` occurrences retain command-line
order within their action stage and stop at the first trigger. The drop stage
always precedes the tombstone stage, regardless of interleaving on the command
line. A trigger skips remaining predicates and projection. `--drop-tombstones`
also drops records selected by either tombstone option. Metadata access,
deferred preparation, and source tombstone defaults are identical for both
polarities.

```sh
--drop-unless 'environment = "production"' \
  --drop-if 'disabled = true' \
  --tombstone-unless 'retained = true'
```

Compile and evaluation diagnostics identify each option's occurrence number,
for example `drop-unless predicate #2`. Existing `if` diagnostics retain
`drop predicate #N` and `tombstone predicate #N` labels.

This strict embedding boundary applies only to the final action result. Native
JSONata effective-Boolean rules still apply inside path filters, conditionals,
`and`, `or`, `$boolean`, and other language constructs.

## Projection Results

A successful projection is serialized as compact JSON. JSONata result
sequences with multiple values are serialized as one JSON array payload:

```text
items.price  ->  [2,3]
```

Zero emitted results (including a missing top-level value) are an evaluation
error. One emitted result is serialized as that JSON value; multiple results
are wrapped in one JSON array. An explicit JSON array is a single value,
including an empty array. The entire evaluation and serialization must finish
before a successful action is published, so a late evaluation error cannot
emit a partial record.

Serialization uses jx's shared complete-result serializer through
`PreparedInput::write_json`, `PreparedInput::write_json_with`, or
`Evaluation::write_json`, and native jx value encoding.
Evaluation continues after an encoding failure; a later evaluation error takes
precedence. Borrowed values retain number and escape spelling and duplicate object
members; only whitespace outside strings is removed. Constructed values use jx's native JSON encoding. Retained
missing values inside sequences and non-finite computed numbers serialize as
`null`; function values (including regex functions) fail serialization, also
when nested. There is no additional jkq value-tree compatibility check.

Native jx sequence flattening, missing-value behavior, and object-property
omission apply. A projected JSON `null` is the four-byte payload `null`; it is
not a Kafka tombstone.

## Variables

`--vars` accepts exactly one strict JSON object:

```sh
--vars '{"tenant":"acme","cutoff":1000}'
```

`--vars-file` reads the same object from a UTF-8 file and is mutually exclusive
with `--vars`:

```sh
--vars-file variables.json
```

File errors, invalid JSON, and non-object roots fail during startup and
`--check`. Expressions access the immutable object as `$vars`, for example
`$vars.tenant` and `$vars.cutoff`.

The object is bound once before compilation through jx immutable bindings and
shared across expressions. Each evaluation has independent state. Local JSONata
assignments and parameters may shadow `$vars` within that expression; closures
and `$eval` retain native lexical semantics. Evaluator state, assignments, the
root document, and local rebinding do not carry into another expression or
input record.

## Numbers

jx borrows JSON number tokens until computation needs IEEE-754 `f64` values.
Projecting the input token `9007199254740993` preserves it exactly. Arithmetic
on that value can lose precision. Computed numbers use jx's compact binary64
encoding.

## Errors and Upstream Deviations

Invalid UTF-8 or malformed JSON follows `--on-invalid-json`. JSONata runtime
failures, strict predicate-result failures, empty projections, non-JSON
results, and serialization failures follow `--on-eval-error`. Runtime errors
identify the drop predicate, tombstone predicate, or projection and are
wrapped with topic, partition, and offset by the pipeline. `jkq` does not
automatically add source payload contents to diagnostics. Native messages
deliberately produced by JSONata expressions, including `$error()` and
`$assert()` messages, are preserved and may contain record data.

jkq exposes jx compiler, validator, evaluator, and serializer diagnostics,
including available byte offsets. Dependency semantics are used directly.

Intentional changes from the former jsonata-core integration include:

- `$lookup` of an absent key is missing rather than JSON `null`; projecting it
  is an empty-result error, and comparing it with `null` returns false.
- Missing operands follow jx comparisons: both `missing = null` and
  `missing != null` return false. Use `$exists` to test presence.
- Borrowed compact output preserves tokens and duplicate members instead of
  parsing and re-encoding the entire JSON tree.
- Retained missing sequence entries and non-finite computed results use jx's
  native `null` serialization.
- Regex literals in object constructors compile successfully; serialization
  still rejects a function-valued result.
