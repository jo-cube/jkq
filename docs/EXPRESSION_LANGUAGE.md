# Expression Language Specification

## 1. Purpose

The language supports common JSON predicates and projections without implementing general stream generation.

It is designed for:

- field access;
- simple comparisons;
- boolean composition;
- existence and type checks;
- string containment and prefix/suffix checks;
- length checks;
- object and array construction.

One expression evaluates against one JSON document and produces one value or one error.

## 2. Deliberate Differences from jq

The language does not provide:

- multiple results;
- implicit iteration;
- pipes;
- recursive descent;
- wildcard paths;
- slices;
- assignments;
- reductions;
- sorting or grouping;
- user functions;
- modules;
- regex initially;
- broad truthiness;
- automatic object-field omission for missing values.

It should be described as jq-inspired, not jq-compatible.

## 3. Lexical Grammar

Informal grammar:

```text
identifier  = letter_or_underscore { letter_digit_or_underscore }
integer     = ["-"] digit { digit }
decimal     = ["-"] digit { digit } "." digit { digit } [exponent]
string      = JSON quoted string
```

Keywords:

```text
and
or
not
true
false
null
```

Whitespace is insignificant outside strings.

Keywords are lowercase and case-sensitive.

## 4. Paths

### 4.1 Rooted field path

```text
.id
.customer.id
```

### 4.2 Array index

```text
.items[0]
.items[0].sku
```

Array indices are non-negative integers.

### 4.3 Quoted field names

Support quoted bracket fields for names that are not identifiers:

```text
.["customer-id"]
.["a.b"]
```

This is preferable to inventing escaping rules for dot notation.

### 4.4 Missing path

A path is `Missing` when:

- an object field does not exist;
- an array index is out of range;
- traversal expects an object or array but finds another type.

Missing is not JSON `null`.

## 5. Literals

Supported:

```text
null
true
false
123
-123
12.5
"hello"
```

Number handling follows the backend's practical signed 64-bit, unsigned 64-bit, and floating-point representations. The product does not promise arbitrary-precision numbers.

## 6. Operators

### 6.1 Equality

```text
==
!=
```

Rules:

- values of the same JSON type compare naturally;
- signed and unsigned integers compare numerically when representable;
- integer and floating-point comparison is numeric;
- different non-numeric types are unequal;
- `Missing == anything` is false, including `Missing`;
- use `missing(path)` for missing checks;
- arrays and objects are not initially comparable with equality except as a future extension.

### 6.2 Ordering

```text
<
<=
>
>=
```

Supported for:

- numbers;
- strings, using Unicode scalar/UTF-8 lexical behavior chosen by the implementation and documented in tests.

Type mismatch is an evaluation error rather than an arbitrary total ordering.

Missing in an ordering comparison yields false rather than an error.

### 6.3 Boolean

```text
and
or
not
```

Operands must be booleans.

No jq-style truthiness exists. `null`, zero, empty string, empty array, and empty object are not booleans.

Short-circuiting is required.

### 6.4 Precedence

From low to high:

1. `or`
2. `and`
3. comparison operators
4. unary `not`
5. primary expressions

Use parentheses to make intent clear.

## 7. Functions

### 7.1 Existence

```text
exists(.path)
missing(.path)
```

- `exists` is true when the path is present, including JSON `null`.
- `missing` is the inverse.

### 7.2 Type checks

```text
is_null(expr)
is_boolean(expr)
is_number(expr)
is_string(expr)
is_array(expr)
is_object(expr)
```

For `Missing`, all type checks return false.

### 7.3 String functions

```text
contains(string, substring)
starts_with(string, prefix)
ends_with(string, suffix)
```

Both arguments must be strings. Missing arguments produce false. Other type mismatches are evaluation errors.

Case-sensitive byte/Unicode behavior is the default. No locale-sensitive comparison is used.

### 7.4 Length

```text
length(expr)
```

Returns:

- Unicode scalar count or UTF-8 byte count for strings, chosen once and frozen by tests;
- element count for arrays;
- field count for objects.

Use Unicode scalar count for user expectations, unless benchmark evidence demonstrates unacceptable cost. Document the final choice in this file.

Missing produces `Missing`. Unsupported scalar types are evaluation errors.

### 7.5 Default value

An initial useful function:

```text
coalesce(expr, fallback)
```

Returns `fallback` when `expr` is `Missing` or JSON `null`; otherwise returns `expr`.

This enables explicit missing handling in projections without implicit omission.

Additional variadic arguments should not be added initially.

## 8. Arrays

Array construction:

```text
[.latitude, .longitude]
```

Each element must evaluate successfully. A missing element is an evaluation error unless wrapped in `coalesce`.

There is no array iteration or mapping.

## 9. Objects

Object construction:

```text
{
  id: .id,
  customer_id: .customer.id,
  active: .status == "active"
}
```

Quoted keys:

```text
{
  "customer-id": .customer.id
}
```

Rules:

- field order is expression order;
- duplicate projection keys are rejected at compile time;
- missing field results are evaluation errors;
- JSON `null` values are serialized normally;
- keys are always strings.

Rejecting duplicate projection keys is simple and avoids backend-dependent ambiguity. Duplicate keys in source JSON follow backend effective behavior and are not separately diagnosed.

## 10. Predicate Semantics

A drop or tombstone predicate must evaluate to boolean.

Examples:

```text
.tenant != "acme"
.deleted == true
missing(.customer.id)
exists(.expires_at) and .expires_at < 1720000000000
starts_with(.account, "test-")
length(.items) == 0
```

A final `Missing` predicate result is treated as false only where a supported operation explicitly produces false for missing. A bare missing path is a type error because predicates require boolean.

## 11. Projection Semantics

Examples:

```text
{id: .id}
```

```text
{
  id: .id,
  owner: coalesce(.owner.name, "unknown"),
  active: .status == "active"
}
```

```text
[.location.latitude, .location.longitude]
```

```text
.id
```

Projection may produce any JSON value.

## 12. Record Program Semantics

CLI options compile to:

```text
for each drop predicate:
    if true: Drop

for each tombstone predicate:
    if true: Tombstone

if projection exists:
    Project(projection result)

otherwise:
    PassThrough
```

Input Kafka tombstones bypass this program.

## 13. Errors

### Parse errors

Examples:

- unterminated string;
- malformed escape;
- unexpected token;
- missing delimiter;
- chained comparison;
- duplicate object projection key;
- wrong function arity.

### Evaluation errors

Examples:

- boolean operator on non-boolean;
- string function on a number;
- ordering comparison across incompatible types;
- missing projection value;
- unsupported length operand;
- projected number cannot be serialized.

Errors should include the expression category and source span. Per-record errors should also include partition and offset.

## 14. Example Set

### Keep active production accounts and reduce fields

```sh
--drop-if '.environment != "production"' \
--tombstone-if '.deleted == true or .active == false' \
--project '{
  id: .id,
  owner_id: .owner.id,
  plan: coalesce(.plan, "unknown")
}'
```

### Drop records missing a required identifier

```sh
--drop-if 'missing(.id)'
```

### Tombstone expired records

```sh
--tombstone-if 'exists(.expires_at) and .expires_at <= 1720000000000'
```

### Project a scalar

```sh
--project '.id'
```

## 15. Grammar Sketch

```text
expression      := or_expression

or_expression   := and_expression { "or" and_expression }
and_expression  := comparison { "and" comparison }
comparison      := unary [ comparison_op unary ]
unary           := [ "not" ] primary

primary         := literal
                 | path
                 | call
                 | array
                 | object
                 | "(" expression ")"

call            := identifier "(" [ expression { "," expression } ] ")"
array           := "[" [ expression { "," expression } ] "]"
object          := "{" [ object_field { "," object_field } ] "}"
object_field    := object_key ":" expression
object_key      := identifier | string

path            := "." path_segment { path_segment }
path_segment    := identifier
                 | "." identifier
                 | "[" integer "]"
                 | "[" string "]"
```

The implementation grammar may differ mechanically, but observable accepted syntax must match this specification.
