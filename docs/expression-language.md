# JSONata Integration

`jkq` uses native JSONata for every drop predicate, tombstone predicate, and
projection. The [JSONata documentation](https://docs.jsonata.org/) defines the
language, and [jsonata-js](https://github.com/jsonata-js/jsonata) is the
semantic reference implementation.

The runtime implementation is the public Rust API of
[`jsonata-core`](https://github.com/txjmb/jsonata-core), currently version
2.2.7. Expressions use JSONata syntax directly:

```sh
--drop-if 'environment != "production"'
--tombstone-if 'deleted = true'
--project '{"id": id, "total": $sum(items.price)}'
```

JSONata uses bare input paths such as `customer.id`, `=` for equality,
`$`-prefixed built-ins, quoted object keys, and native conditionals, paths,
sequences, functions, variables, and assignments.

## Startup and Record Evaluation

Every configured JSONata expression is parsed during CLI resolution. A parse
failure is a command-line error and exits with status 2 before Kafka
consumption. `--check` performs the same parsing and variable validation
without creating a Kafka consumer.

For each non-tombstone input record, `jkq` parses the payload into one
jsonata-core value and reuses it while it:

1. evaluates `--drop-if` expressions in command-line order, dropping the
   record at the first Boolean `true`;
2. evaluates `--tombstone-if` expressions in command-line order, tombstoning
   the record at the first Boolean `true`, or dropping it when
   `--drop-tombstones` is set;
3. evaluates `--project` for surviving records, when present;
4. otherwise passes through the source payload, preserving its exact bytes
   unless `--envelope-payload value` requests compact JSON serialization.

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

Top-level `Undefined` is an evaluation error. A function, regular expression,
or any other non-JSON internal value is also an error, including when nested
inside an array or object. jkq checks the value tree before serialization so
jsonata-core cannot silently convert such values to null, an empty string, or
another JSON-looking representation. A serialization failure is an evaluation
error.

Native JSONata sequence flattening, missing-value behavior, and object-property
omission otherwise apply. A projected JSON `null` is the four-byte payload
`null`; it is not a Kafka tombstone.

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

Each expression evaluation receives a clean JSONata context containing the
same worker-local variable value. JSONata assignments are scoped normally
within that expression, but evaluator state, assignments, the root document,
and variable mutations do not carry into another expression or input record.

## Numbers

jsonata-core uses JSONata and IEEE-754 `f64` number semantics. Integers outside
the exactly representable range can lose precision while the payload is
parsed. For example, projecting an input value of `9007199254740993` produces
`9007199254740992`.

## Errors and Upstream Deviations

Invalid UTF-8 or malformed JSON follows `--on-invalid-json`. JSONata runtime
failures, strict predicate-result failures, `Undefined` projections, non-JSON
results, and serialization failures follow `--on-eval-error`. Runtime errors
identify the drop predicate, tombstone predicate, or projection and are
wrapped with topic, partition, and offset by the pipeline. `jkq` does not
automatically add source payload contents to diagnostics. Native messages
deliberately produced by JSONata expressions, including `$error()` and
`$assert()` messages, are preserved and may contain record data.

jkq exposes useful jsonata-core parser and evaluator messages but does not
invent byte positions that its public API does not reliably provide. When
jsonata-core differs from jsonata-js, jkq reports and documents the dependency
behavior directly.

One known jsonata-core 2.2.7 deviation is that a regular-expression literal
used directly as an object-constructor value, such as `{"value": /x/}`, is
rejected during parsing, while jsonata-js 2.2.2 accepts that syntax. jkq reports
the startup parse error. A top-level regular expression is parsed by
jsonata-core, then rejected by jkq because it is not a JSON projection result.
