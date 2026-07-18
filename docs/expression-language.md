# Expression Language

`jkq` provides a restricted, jq-inspired language for predicates and
projections. One expression evaluates against one JSON document and produces
one value or one error; it never produces a stream of values.

## Paths and Literals

Paths start at the input root:

```text
.
.id
.customer.id
.items[0].sku
.["customer-id"]
.["a.b"]
```

`.` selects the complete input document. Dot fields use identifiers. Bracket
fields use JSON strings. Array indices are non-negative integers. There are no
wildcards, slices, implicit iteration, or recursive descent.

Supported literals are JSON null, booleans, strings, and practical signed,
unsigned, and floating-point numbers:

```text
null  true  false  42  -7  12.5  "hello"
```

A path that cannot be traversed produces `Missing`. Missing is an internal
state, not JSON `null`.

## Variables

`--vars` supplies one immutable JSON-shaped object that is parsed at startup:

```sh
--vars '{tenant: "acme", policy: {cutoff: 1000}, statuses: ["open", "pending"]}'
```

Expressions access it through the reserved `$vars` root:

```text
$vars.tenant
$vars.policy.cutoff
$vars.statuses[0]
$vars["non-identifier"]
```

Variable paths use the same field and index syntax and the same `Missing`
semantics as input paths. The variables object may contain only nulls,
booleans, numbers, strings, arrays, and objects. Duplicate keys, non-object
roots, and record-dependent expressions are rejected at startup. Variables are
constants: there is no assignment or per-record mutation. The object uses
projection syntax, so identifier keys may be bare and other keys must be JSON
strings.

## Operators

From lowest to highest precedence:

1. `or`
2. `and`
3. `==`, `!=`, `<`, `<=`, `>`, `>=`
4. unary `not`
5. primary expressions

Comparisons cannot be chained. A comparison result may be compared when the
nested comparison is explicitly parenthesized. Use parentheses when precedence
is not obvious.

Boolean operators require booleans and short-circuit. There is no truthiness:
null, zero, empty strings, arrays, and objects are not booleans.

Equality supports nulls, booleans, strings, and numbers. Numeric comparison
works across signed, unsigned, and floating-point representations without first
rounding every integer through `f64`. Different non-numeric types are unequal.
Any equality comparison involving an array or object and a non-missing value is
an evaluation error.

`Missing == value` is false and `Missing != value` is true, including when both
sides are missing. Ordering with a missing operand is false. Other ordering
requires two numbers or two strings.

## Functions

| Function | Behavior |
|---|---|
| `exists(value)` | true unless the value is missing; null exists |
| `missing(value)` | true only for missing |
| `is_null(value)` | JSON null check |
| `is_boolean(value)` | boolean type check |
| `is_number(value)` | numeric type check |
| `is_string(value)` | string type check |
| `is_array(value)` | array type check |
| `is_object(value)` | object type check |
| `contains(string, part)` | case-sensitive substring check |
| `starts_with(string, prefix)` | case-sensitive prefix check |
| `ends_with(string, suffix)` | case-sensitive suffix check |
| `length(value)` | Unicode scalar count, array length, or object field count |
| `coalesce(value, ..., fallback)` | first non-missing, non-null value; accepts at least two arguments |
| `if(condition, when_true, when_false)` | selected branch; the other branch is not evaluated |
| `in(value, array)` | scalar membership using the language's equality rules |

Type checks return false for missing. String functions return false for missing
arguments and error on other non-string arguments. `length(Missing)` produces
Missing; unsupported scalar types are errors.

`coalesce` evaluates arguments from left to right and stops at the first value
that is neither missing nor null. Its final argument is returned as-is, so an
all-missing call still produces Missing. `if` requires a boolean condition and
evaluates only the selected branch.

`in` returns false when the value or array is missing. Its second argument must
otherwise be an array, and its first argument must be a scalar. Numeric
membership uses the same exact cross-representation comparison as `==`. A
scalar is null, a boolean, a number, or a string.

## Projections

Arrays and objects construct compact JSON:

```text
[.location.latitude, .location.longitude]
```

```text
{
  id: .id,
  owner: coalesce(.owner.name, "unknown"),
  active: .status == "active"
}
```

Object keys may be identifiers or JSON strings. Their expression order is
preserved in output. Duplicate object keys are rejected. A missing array
element, object value, or top-level projection result is an evaluation error;
use `coalesce` when a default is intended.

A projection may return any JSON value, including a scalar or JSON `null`.
Projected `null` is the four-byte payload `null`, not a Kafka tombstone.

## Predicates and Record Actions

`--drop-if` and `--tombstone-if` require a boolean result. Repeated predicates
short-circuit in command-line order:

```text
drop predicates
→ tombstone predicates
→ projection, when present
→ exact pass-through
```

Kafka tombstones bypass the expression program.

Examples:

```sh
--drop-if '.environment != "production"'
--drop-if 'missing(.id)'
--tombstone-if 'exists(.expires_at) and .expires_at <= 1720000000000'
--project '{id: .id, plan: coalesce(.plan, "unknown")}'
```

## Errors and Limits

Parse errors include an expression category and byte position. Evaluation
errors identify the failing expression and, while processing records, the
topic, partition, and offset.

Expressions nested more than 128 levels are rejected at startup. JSON nested
more than 128 arrays or objects is treated as invalid JSON. Source objects with
duplicate keys follow simd-json's effective lookup behavior and are not
separately diagnosed.

The variables object follows the expression nesting limit and is included in
the compiled projection-size bound. It is never reparsed for individual input
records.

The language intentionally omits pipes, multiple results, assignments,
reductions, sorting, grouping, joins, regular expressions, user functions,
modules, and automatic omission of missing object fields.

## Grammar

```text
expression      := or_expression
or_expression   := and_expression { "or" and_expression }
and_expression  := comparison { "and" comparison }
comparison      := unary [ comparison_operator unary ]
unary           := [ "not" ] primary
primary         := literal | path | variable | call | array | object | "(" expression ")"
variable        := "$vars" { "." identifier | "[" (string | nonnegative_integer) "]" }
call            := identifier "(" [ expression { "," expression } ] ")"
array           := "[" [ expression { "," expression } ] "]"
object          := "{" [ object_field { "," object_field } ] "}"
object_field    := (identifier | string) ":" expression
```
