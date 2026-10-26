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

# Design: SQL Expressions for Read Restriction Policies

## Scope

This document defines the SQL expressions used for row filters and column masks. It covers
expression syntax, supported operands and functions, type checking, null semantics, and validation.

The policy name, target, `FOR GROUP`, and `EXCEPT GROUP` clauses retain their existing meaning.
This proposal changes only the expression following `USING`. It does not introduce a new policy
structure, conditional-rule fields, or policy-selection behavior.

The authoring dialect is `restricted-sql-v1`. A row-filter expression returns a Boolean result;
only rows for which it returns `TRUE` are visible. A column-mask expression returns a replacement
value of the target column's logical type. Existing authorization still determines table access.

## Authoring Examples

The following policy statements illustrate where the expression is used; this document defines
the expression language rather than a new policy DDL grammar.

```sql
CREATE ROW FILTER analyst_region ON sales.crm.customers
  FOR GROUP analyst
  USING region = user_attribute('region');

CREATE COLUMN MASK email_mask ON sales.crm.customers (email)
  EXCEPT GROUP pii_reader
  USING mask_alphanum(email);

CREATE COLUMN MASK card_mask ON sales.crm.customers (card)
  EXCEPT GROUP pii_reader
  USING show_last_4(card);
```

Group inclusion and exclusion remain policy clauses. They do not need to be repeated inside the
filter or mask expression.

## Row Filter Language

### Grammar

The expression grammar is:

```text
rowFilter   := expr EOF
expr        := orExpr
orExpr      := andExpr (OR andExpr)*
andExpr     := notExpr (AND notExpr)*
notExpr     := NOT notExpr | primary
primary     := "(" expr ")" | predicate | boolean | null
predicate   := value compareOp value
             | column IS NOT? NULL
             | column NOT? IN "(" literal ("," literal)* ")"
             | column NOT? BETWEEN literal AND literal
             | column NOT? LIKE string
             | "is_nan" "(" column ")"
value       := column | sessionUser | userAttribute | datePart | literal
compareOp   := "=" | "<>" | "!=" | "<" | "<=" | ">" | ">="
column      := identifier
sessionUser := "session_user" "(" ")"
userAttribute := "user_attribute" "(" string ")"
datePart    := ("year" | "month" | "day") "(" column ")"
literal     := string | number | boolean | null | temporal
temporal    := (DATE | TIME | TIMESTAMP) string
             | TIMESTAMP WITH TIME ZONE string
boolean     := TRUE | FALSE
null        := NULL
number      := "-"? ("0" | nonZeroDigit digit*) ("." digit+)?
digit       := "0" | nonZeroDigit
nonZeroDigit := "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"
```

Keywords and built-in function names are case-insensitive. `EOF` requires complete consumption
of the expression. Semantic validation further restricts operand combinations as described below.

Precedence, from highest to lowest, is parentheses and function calls, predicates, `NOT`, `AND`,
and `OR`. `NOT active = TRUE` means `NOT (active = TRUE)`. The `AND` inside `BETWEEN` separates
the bounds. Comparisons do not chain: `amount > 10 AND amount < 100` is valid; `10 < amount < 100`
is invalid.

### Identifiers and Literals

An unquoted identifier starts with an ASCII letter or underscore, followed by ASCII letters,
digits, or underscores. Keywords in this grammar are reserved. A quoted identifier uses double
quotes, escaping an embedded double quote by doubling it, for example `"Customer Region"`.
Both forms reference the exact decoded top-level column name; unquoted names are not case-folded.
Qualified names and nested paths are unsupported. `"a.b"` names one top-level column.

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
as part of an expression. The semicolons in the policy examples terminate the surrounding DDL.

### Supported Predicates

| Category | Examples | Requirements |
| --- | --- | --- |
| Comparison | `amount = 100`, `amount <> 100`, `amount < 100`, `amount <= 100`, `amount > 100`, `amount >= 100` | Column and compatible non-null literal, in either order |
| Range | `amount BETWEEN 10 AND 100`, `amount NOT BETWEEN 10 AND 100` | Inclusive bounds; compatible ordered type |
| Membership | `region IN ('US', 'CA')`, `region NOT IN ('US', 'CA')` | Non-empty homogeneous literal list without null elements |
| Null test | `email IS NULL`, `email IS NOT NULL` | Top-level column reference |
| NaN test | `is_nan(score)`, `NOT is_nan(score)` | Floating-point column |
| Prefix matching | `name LIKE 'Acme%'`, `name NOT LIKE 'Acme%'` | String column; non-empty fixed prefix followed by exactly one `%` |
| Boolean composition | `region = 'US' AND amount < 100`, `region = 'US' OR region = 'CA'`, `NOT (region = 'US')` | Boolean operands; parentheses may nest |
| Boolean constant | `TRUE`, `FALSE` | Visible-row predicate |
| Date part | `year(order_date) = 2026`, `month(order_date) = 3`, `day(order_date) = 15` | Date-part result compared with a compatible integer literal |
| Current user | `owner = session_user()` | String column and current user; equality or inequality, in either order |
| User attribute | `region = user_attribute('region')` | Column and compatible declared scalar attribute, in either order |

`!=` is accepted as an alias for `<>`. `BETWEEN` is equivalent to inclusive lower and upper
comparisons joined by `AND`; `NOT BETWEEN` negates that result. Prefix patterns cannot contain
`_` or any other `%`, and the first version has no `ESCAPE` clause. Matching is case-sensitive.

Column-to-column and literal-to-literal comparisons are unsupported. A bare column or non-Boolean
value is invalid at the root. Comparisons with a `NULL` literal are rejected; use `IS NULL` or
`IS NOT NULL`. Lists cannot contain expressions, references, nested lists, or null elements.

## Type and Context Binding

Column references bind against the concrete Gravitino table schema. Missing or ambiguous names,
unsupported types, incompatible operands, and invalid function arguments are errors.

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
| `session_user()` | Non-null `STRING` | Trusted effective subject name |
| `user_attribute('key')` | Declared scalar type, possibly null | Trusted attribute of the effective subject |

Attribute keys are non-empty, case-sensitive string literals. Attribute types must be compatible
with the referenced column. Lists, objects, and implicit string-to-number conversions are
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

## Column Mask Language

The expression following `USING` is one of the following function calls over the target column:

```text
maskExpression := maskFunction "(" column ")" EOF
maskFunction   := "mask_alphanum" | "mask_to_fixed_value" | "replace_with_null"
                | "show_first_4" | "show_last_4" | "truncate_to_year"
                | "truncate_to_month" | "sha256_global" | "sha256_query_local"
```

Identifier and literal rules are shared with row filters. The argument must reference the column
being masked. Each function preserves its input logical type.

| SQL expression | Input types | Meaning |
| --- | --- | --- |
| `mask_alphanum(email)` | `STRING` | ASCII digits become `n`; `(),.@-` are preserved; every other Unicode code point becomes `x` |
| `show_first_4(email)` | `STRING` | Keep the first four code points; apply the same masking rules to the remainder |
| `show_last_4(card)` | `STRING` | Keep the last four code points; apply the same masking rules to the preceding portion |
| `mask_to_fixed_value(region)` | Types with a defined fixed value | Replace with the type's fixed value, including null input |
| `replace_with_null(email)` | Nullable columns | Return a null of the target column's type |
| `truncate_to_year(order_date)` | Date and supported timestamp types | Truncate to the beginning of the year |
| `truncate_to_month(order_date)` | Date and supported timestamp types | Truncate to the beginning of the month |
| `sha256_global(customer_id)` | `STRING`, signed `INTEGER`, signed `LONG`, `BINARY` | Stable type-preserving SHA-256 transformation |
| `sha256_query_local(customer_id)` | Same as `sha256_global` | Type-preserving SHA-256 transformation salted per query |

The mask behaviors remain the same; this proposal changes their authoring form to SQL function
calls. All masks except `mask_to_fixed_value` preserve null input. `replace_with_null` is invalid
for a required column. Strings of four or fewer Unicode code points are returned unchanged by
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

Only the listed calls are accepted. Additional arguments, nested calls, references to other
columns, and arbitrary transformations are unsupported. Group exemptions continue to use the
policy's `EXCEPT GROUP` clause.

## Validation and Failure Behavior

Static validation checks complete expression parsing, Boolean row-filter roots, mask-call roots,
supported operand shapes, allowlisted functions, function arguments, and resource limits.
Schema and context binding check exact column names, compatible types, literal ranges, nullability,
and trusted context values. Invalid content is rejected; a binding or context error must never
silently remove a required restriction.

The expression limits remain:

- Source length: 16 KiB of UTF-8.
- Operation depth: 8.
- Expression nodes: 256.
- Decoded string literal: 4 KiB of UTF-8.
- `IN` list elements: 256.

Limits apply before normalization. Unknown functions, unsupported types, and expressions outside
this grammar are errors. The first version rejects SQL statements, `CASE`, casts, arithmetic,
column-to-column comparisons, nested fields, subqueries, custom functions, general regular
expressions, and general wildcard patterns such as `'%Acme'` or `'A_me'`.

## Conformance

Examples and validation cases cover every supported operator and type; SQL quoting and escaping;
numeric boundaries, decimal scale, and temporal precision; null comparisons and nested negation;
prefix matching; NaN tests; date parts; user and attribute binding; every mask function; invalid
type/function pairs; and all resource limits.

Policy scope, group inclusion and exclusion, target binding, and policy conflict behavior retain
their existing contracts. This expression change must not alter them.

## References

- [Databricks row-filter and column-mask expressions](https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks/manually-apply)
- [Snowflake row access policy expressions](https://docs.snowflake.com/en/sql-reference/sql/create-row-access-policy)
- [Snowflake masking policy expressions](https://docs.snowflake.com/en/sql-reference/sql/create-masking-policy)
