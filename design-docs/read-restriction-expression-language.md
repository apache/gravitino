<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Design: Snowflake-Style SQL Policy Expressions

## Scope

This document defines Snowflake-style SQL expressions for row filters and column masks. It covers
expression syntax, supported functions, type checking, SQL null semantics, and validation.
Conditional behavior is written as an ordinary Boolean expression or SQL `CASE` expression.

The authoring dialect is `restricted-sql-v1`. Row-filter expressions return `BOOLEAN`; only true
retains a row. Mask expressions return the target column's logical type. Both use the same
expression grammar. The surrounding policy supplies the available input names, their types, and
the expected result type. This specification defines expressions only.

## Authoring Examples

With a string input `region`, a row filter may be a direct Boolean expression:

```sql
region = user_attribute('region')
```

The same filter can express conditional behavior with SQL `CASE`:

```sql
CASE
  WHEN CURRENT_USER() = 'alice' THEN TRUE
  ELSE region = user_attribute('region')
END
```

With a string input `email`, a mask may return the original value or a transformed value:

```sql
CASE
  WHEN CURRENT_USER() = 'alice' THEN email
  ELSE mask_alphanum(email)
END
```

It may also return a literal or null of the expected type:

```sql
CASE
  WHEN email IS NULL THEN NULL
  ELSE 'XXXXXXXX'
END
```

## Expression Inputs and Results

An expression may reference only inputs supplied by its policy context. Each input has a known
logical type and nullability. Input names must be unique under the identifier rules below. The
expected result type is also known before evaluation:

| Use | Expected result | Accepted expression forms |
| --- | --- | --- |
| Row filter | `BOOLEAN` | Predicate, Boolean input or literal, Boolean function result, Boolean `CASE` |
| Column mask | Target column's logical type | Original input, literal, null, function result, scalar `CASE` |

A mask's result type must match the target column, including precision, scale, and timezone
characteristics. A mask may use other available inputs in conditions. A non-Boolean filter result
or incompatible mask result is an error. This expression specification does not define how the
surrounding policy selects or binds those inputs.

## SQL Expression Language

### Grammar

The expression grammar is:

```text
expression  := expr EOF
expr        := orExpr
orExpr      := andExpr (OR andExpr)*
andExpr     := notExpr (AND notExpr)*
notExpr     := NOT notExpr | predicate
predicate   := scalar (compareOp scalar
             | IS NOT? NULL
             | NOT? IN "(" literal ("," literal)* ")"
             | NOT? BETWEEN scalar AND scalar
             | NOT? LIKE string)?
scalar      := inputRef | literal | functionCall | caseExpr | "(" expr ")"
inputRef    := identifier
functionCall := identifier "(" (expr ("," expr)*)? ")"
caseExpr    := CASE expr? (WHEN expr THEN expr)+ (ELSE expr)? END
compareOp   := "=" | "<>" | "!=" | "<" | "<=" | ">" | ">="
literal     := string | number | boolean | null | temporal
temporal    := (DATE | TIME | TIMESTAMP) string
             | TIMESTAMP WITH TIME ZONE string
boolean     := TRUE | FALSE
null        := NULL
number      := "-"? ("0" | nonZeroDigit digit*) ("." digit+)?
digit       := "0" | nonZeroDigit
nonZeroDigit := "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"
```

Keywords and allowlisted built-in function names are case-insensitive. `functionCall` does not
permit arbitrary functions: names and signatures must match the function tables below. `EOF`
requires complete consumption. Semantic validation restricts operand and result types.

Precedence, from highest to lowest, is parentheses and function calls, predicates, `NOT`, `AND`,
and `OR`. `NOT active = TRUE` means `NOT (active = TRUE)`. The `AND` inside `BETWEEN` separates
the bounds. Comparisons do not chain: `amount > 10 AND amount < 100` is valid; `10 < amount < 100`
is invalid.

### Identifiers and Literals

An unquoted identifier starts with an ASCII letter or underscore, followed by ASCII letters,
digits, or underscores. Keywords in this grammar are reserved. A quoted identifier uses double
quotes, escaping an embedded double quote by doubling it, for example `"Customer Region"`.
Unquoted input names normalize to uppercase; quoted names preserve case, following Snowflake's
identifier convention. Names must be unique after that normalization. `val` and `VAL` refer to
the same input; `"val"` is distinct. Names bind only within the supplied input context;
qualified input references and nested paths are unsupported.

String literals use single quotes; an embedded single quote is doubled, as in `'O''Brien'`.
Backslashes are literal characters. Unicode is not normalized, and isolated surrogates are invalid.
Whitespace is allowed between tokens. Numeric literals are exact base-10 values until type binding.
Leading `+`, exponents, leading zeroes, and trailing decimal points are unsupported.

Examples of literals:

```sql
'US'
'O''Brien'
100
-10.50
TRUE
NULL
DATE '2026-01-01'
TIME '12:30:00'
TIMESTAMP '2026-01-01 12:30:00'
TIMESTAMP WITH TIME ZONE '2026-01-01 12:30:00+00:00'
```

Comments, semicolons, additional statements, and square-bracket literal arrays are not accepted
as part of an expression.

### Supported Predicates

| Category | Examples | Requirements |
| --- | --- | --- |
| Comparison | `amount = 100`, `amount <> 100`, `amount < 100`, `amount <= 100`, `amount > 100`, `amount >= 100` | Compatible scalar operands, including available inputs |
| Range | `amount BETWEEN 10 AND 100`, `amount NOT BETWEEN 10 AND 100` | Inclusive bounds; compatible ordered type |
| Membership | `region IN ('US', 'CA')`, `region NOT IN ('US', 'CA')` | Non-empty literal list; non-null values have compatible types |
| Null test | `val IS NULL`, `user_attribute('region') IS NOT NULL` | Typed scalar operand |
| NaN test | `is_nan(score)`, `NOT is_nan(score)` | Floating-point argument |
| Prefix matching | `name LIKE 'Acme%'`, `name NOT LIKE 'Acme%'` | String operand; non-empty fixed prefix followed by exactly one `%` |
| Boolean composition | `region = 'US' AND amount < 100`, `region = 'US' OR region = 'CA'`, `NOT (region = 'US')` | Boolean operands; parentheses may nest |
| Boolean constant | `TRUE`, `FALSE` | Visible-row predicate |
| Date part | `year(order_date) = 2026`, `month(order_date) = 3`, `day(order_date) = 15` | Date-part result compared with a compatible integer literal |
| Current user | `owner = CURRENT_USER()` | Compatible string operands |
| User attribute | `region = user_attribute('region')` | Compatible declared scalar types |

`!=` is accepted as an alias for `<>`. `BETWEEN` is equivalent to inclusive lower and upper
comparisons joined by `AND`; `NOT BETWEEN` negates that result. Prefix patterns cannot contain
`_` or any other `%`, and the first version has no `ESCAPE` clause. Matching is case-sensitive.

Comparisons between available inputs and constant comparisons are valid when types match.
A non-Boolean result is invalid for a row filter, while a bare input is a valid mask expression when
it has the expected result type. Comparisons with a null value produce `UNKNOWN`; use `IS NULL`
or `IS NOT NULL` to test for null. Lists contain literals only and cannot contain expressions,
references, or nested lists. Null members are accepted with ordinary SQL membership semantics:
if no non-null member matches and at least one member is null, both `IN` and `NOT IN` are unknown.

### CASE Expressions

Both Snowflake forms of `CASE` are supported:

```sql
CASE
  WHEN region IS NULL THEN FALSE
  WHEN region = 'US' THEN TRUE
  ELSE region = user_attribute('region')
END

CASE region
  WHEN 'US' THEN TRUE
  WHEN 'CA' THEN TRUE
  ELSE FALSE
END
```

In searched `CASE`, every condition must be Boolean. Conditions are evaluated in order, and the
first true condition selects its result; false and unknown conditions do not match. In simple
`CASE`, the selector and each match expression must have compatible types. Matching uses SQL
equality, so a null selector does not match a null match value. Only the selected result is
evaluated. No match returns the `ELSE` result, or null if `ELSE` is omitted.

All result branches of one `CASE` must resolve to one logical type compatible with its surrounding
expression. Null literals take the type expected at that position. The complete expression must
match the expected result type. Every branch is statically validated, including branches that
cannot be reached for a particular row.
Nested `CASE` expressions and built-in calls are allowed within the resource limits.

## Type and Context Binding

Input names, logical types, and nullability come from the policy context. Missing or ambiguous
names, unavailable inputs, unsupported types, incompatible operands, and invalid function
arguments are errors.

| Gravitino type | Supported value predicates |
| --- | --- |
| `BOOLEAN` | Equality and membership |
| Signed `INTEGER`, signed `LONG` | Equality, ordering, membership, and ranges |
| `FLOAT`, `DOUBLE` | Equality, ordering, membership, ranges, and NaN tests |
| `DECIMAL(p,s)` | Equality, ordering, membership, and ranges; no rounding |
| `STRING` | Equality, ordering, membership, ranges, and prefix matching |
| `DATE` | Equality, ordering, membership, ranges, and date-part functions |
| `TIME(6)` | Equality, ordering, membership, and ranges |
| `TIMESTAMP(6)` with or without timezone | Equality, ordering, membership, ranges, and date-part functions |

Unset temporal precision means six fractional digits in this profile. Other value-predicate types
and temporal precisions are unsupported. Null tests may apply to any top-level column. On a
required column, `IS NULL` is false and `IS NOT NULL` is true.

There is no implicit cross-family conversion, signedness conversion, rounding, or timezone
assumption. Integer values must be mathematical integers within range. Decimal values must fit
the target precision and scale exactly. Floating-point literals preserve negative zero and reject
overflow, infinity, and NaN. String comparisons are case-sensitive and do not normalize Unicode.
Temporal literals must use the matching type keyword, valid calendar or clock values, and at
most six fractional-second digits. Timestamp-with-timezone literals require an explicit offset.

### Context Functions

| Function | Result | Meaning |
| --- | --- | --- |
| `CURRENT_USER()` | Non-null `STRING` | Trusted effective subject name |
| `user_attribute('key')` | Declared scalar type, possibly null | Trusted attribute of the effective subject |

`CURRENT_USER()` follows Snowflake's context-function spelling. `user_attribute` remains a
Gravitino-specific function with the same existing meaning; it is not a Snowflake built-in.
Attribute keys are non-empty, case-sensitive string literals. Attribute types must be compatible
with their use in the body. Lists, objects, and implicit string-to-number conversions are
unsupported. Context values are stable for one request and use the effective subject.

An absent or explicitly null value for a declared attribute returns SQL `NULL`. For example,
`region = user_attribute('region')` returns `UNKNOWN` when the region attribute has no value, so
that expression retains no rows. The enclosing Boolean expression still follows SQL null rules.
An unknown attribute key, incompatible type, missing subject, or failed lookup is an error.

### Date-Part Functions

`year(column)`, `month(column)`, and `day(column)` accept date and supported timestamp columns.
They return an integer year, month of year, or day of month. Null input returns null.
Timestamp-with-timezone values use UTC; timestamps without timezone use their recorded calendar
fields. Dates use the proleptic Gregorian calendar.

`month(order_date) = 3` matches March dates in every year; it does not imply a particular year.

## Null Semantics

Row filters use SQL three-valued logic: `TRUE`, `FALSE`, and `UNKNOWN` (SQL `NULL`). Only `TRUE`
retains a row. A null column produces:

| Predicate | Result |
| --- | --- |
| `column IS NULL` | `TRUE` |
| `column IS NOT NULL` | `FALSE` |
| Comparison with a non-null value | `UNKNOWN` |
| `IN`, `NOT IN`, `BETWEEN`, `NOT BETWEEN` | `UNKNOWN` |
| `LIKE`, `NOT LIKE`, `is_nan(column)` | `UNKNOWN` |

`NOT UNKNOWN` is unknown. `FALSE AND UNKNOWN` is false; `TRUE AND UNKNOWN` is unknown.
`TRUE OR UNKNOWN` is true; `FALSE OR UNKNOWN` is unknown.

Consequently, `region <> 'US'` and `NOT (region = 'US')` both exclude null regions. To include
them, write `region IS NULL OR region <> 'US'`. A constant-false filter returns an empty result,
rather than denying access to the table.

## Mask Functions

The Gravitino mask functions defined by this profile remain available inside a SQL mask expression.
They are ordinary typed scalar calls and may appear in `CASE` branches or nested calls with matching types. They
are Gravitino functions rather than a claim of Snowflake function compatibility. Each function
preserves its input logical type. Examples below assume matching input names and types.

| SQL expression | Input types | Meaning |
| --- | --- | --- |
| `mask_alphanum(email)` | `STRING` | ASCII digits become `n`; `(),.@-` are preserved; every other Unicode code point becomes `x` |
| `show_first_4(email)` | `STRING` | Keep the first four code points; apply the same masking rules to the remainder |
| `show_last_4(card)` | `STRING` | Keep the last four code points; apply the same masking rules to the preceding portion |
| `mask_to_fixed_value(region)` | Types with a defined fixed value | Replace with the type's fixed value, including null input |
| `replace_with_null(email)` | Any typed scalar | Return a null of the input's type; the result is nullable |
| `truncate_to_year(order_date)` | Date and supported timestamp types | Truncate to the beginning of the year |
| `truncate_to_month(order_date)` | Date and supported timestamp types | Truncate to the beginning of the month |
| `sha256_global(customer_id)` | `STRING`, signed `INTEGER`, signed `LONG`, `BINARY` | Stable type-preserving SHA-256 transformation |
| `sha256_query_local(customer_id)` | Same as `sha256_global` | Type-preserving SHA-256 transformation salted per query |

The mask behaviors remain the same. All masks except `mask_to_fixed_value` preserve null input.
Strings of four or fewer Unicode code points are returned unchanged by
`show_first_4` and `show_last_4`. Timestamp-with-timezone truncation uses UTC.

For example, `show_last_4(card)` changes `4111-1111-1111-4444` to `nnnn-nnnn-nnnn-4444`.
`mask_to_fixed_value` returns `'XXXXXXXX'` for strings, zero for numbers, false for booleans,
and the epoch date or timestamp for those temporal types.

Hash inputs use UTF-8 for strings, little-endian two's-complement bytes for signed integers, and
raw bytes for binary. String output is the 64-character lowercase hexadecimal digest; integer
output uses the first four or eight digest bytes interpreted as the corresponding signed
little-endian type; binary output is the complete 32-byte digest. Query-local hashing prepends
a cryptographically random salt of at least 16 bytes, shared by every evaluation in one query;
separate queries use independent salts.

Only allowlisted functions and the shared expression grammar are accepted. Each mask function
takes one compatible scalar argument; additional arguments to these calls are invalid. Other
available inputs may be used elsewhere in the expression. Conditional disclosure is expressed with
ordinary SQL `CASE`, as in the email mask example above.

A mask expression must have the expected result type on every branch. Returning null, explicitly or
through an omitted `ELSE`, is allowed only for a nullable target column. An expression whose inferred
nullability is incompatible with a required target is rejected. For example, a string-returning
mask cannot return a number in one branch, and a required target cannot use `THEN NULL`.

## Validation and Failure Behavior

Static validation checks complete expression parsing, Boolean row-filter results, exact mask
result types, every `CASE` branch, supported operand shapes, allowlisted functions, function
arguments, and resource limits. Input and context validation check unique names, compatible types,
literal ranges, nullability, and trusted context values. Invalid content is rejected; an input or
context error must never silently remove a required restriction.

The expression limits remain:

- Source length: 16 KiB of UTF-8.
- Operation depth: 8, including nested `CASE` expressions and function calls.
- Expression nodes: 256.
- Decoded string literal: 4 KiB of UTF-8.
- `IN` list elements: 256.

Limits apply before normalization. Unknown functions, unsupported types, and expressions outside
this grammar are errors. This Snowflake-style expression profile supports conditional SQL
expressions without adopting every Snowflake SQL feature. The first version rejects SQL statements,
casts, arithmetic, undeclared column references, nested fields, subqueries, custom functions,
general regular expressions, and general wildcard patterns such as `'%Acme'` or `'A_me'`.

## Conformance

Examples and validation cases cover every supported operator and type; SQL quoting and escaping;
numeric boundaries, decimal scale, and temporal precision; null comparisons and nested negation;
prefix matching; NaN tests; date parts; current-user and attribute binding; every mask function;
expected result types; available inputs; searched and simple `CASE`; first-match
evaluation; null conditions and selectors; omitted `ELSE`; nested expressions; incompatible branch
types and nullability; invalid type/function pairs; and all resource limits.

Input cases must verify unknown names, incompatible types, and expected result types. Identifier
cases must verify unquoted case folding and quoted exact names. All expression forms share the
same null, type, and resource-limit rules.

## References

- [Snowflake row access policy expressions](https://docs.snowflake.com/en/sql-reference/sql/create-row-access-policy)
- [Snowflake masking policy expressions](https://docs.snowflake.com/en/sql-reference/sql/create-masking-policy)
- [Snowflake CASE expressions](https://docs.snowflake.com/en/sql-reference/functions/case)
- [Snowflake CURRENT_USER](https://docs.snowflake.com/en/sql-reference/functions/current_user)
