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

1. evaluates `--drop-if` expressions in command-line order, dropping the
   record at the first Boolean `true`;
2. evaluates `--tombstone-if` expressions in command-line order, tombstoning
   the record at the first Boolean `true`, or dropping it when
   `--drop-tombstones` is set;
3. evaluates `--project` for surviving records, when present;
4. otherwise passes through the source payload, preserving its exact bytes
   unless `--envelope-payload value` requests compact JSON serialization.

Workers validate each payload once into a borrowed `jx::RawJson` and reuse it
across expressions without constructing a JSON tree; see
[expression execution](architecture.md#expression-execution).

Existing Kafka tombstones bypass JSON parsing and every expression. A
source tombstone remains a tombstone by default and is dropped when
`--drop-tombstones` is set. A successfully evaluated input record produces one
action; a JSONata result sequence never expands into multiple jkq output
records.

## Action Predicates

The top-level result of `--drop-if` and `--tombstone-if` must be the JSONata
Boolean `true` or `false`. `Undefined`, null, numbers, strings, arrays,
objects, functions, and regular expressions are evaluation errors governed by
`--on-eval-error`.

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

Serialization uses `jx::Value::write_compact`. Borrowed values retain number
and escape spelling and duplicate object members; only whitespace outside
strings is removed. Constructed values use jx's native JSON encoding. Retained
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
