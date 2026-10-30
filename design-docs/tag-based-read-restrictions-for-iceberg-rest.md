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

# Design: Tag-Based Read Restrictions for Iceberg REST

## Background

Gravitino can associate governance policies with tags and resolve those policies for metadata
objects. Existing authorization decides whether a subject may read a table, but it cannot restrict
which rows or column values are visible after access is granted.

The Iceberg REST specification defines `read-restrictions` in a load-table response. A conforming
reader applies a required row filter and required column projections before returning data. This
provides a standard enforcement boundary for portable restrictions.

This design adds tag-based row-filter and column-mask policies and resolves them into Iceberg REST
`read-restrictions`. It is disabled by default and requires an explicit client capability
declaration while reader support is maturing.

## Goals

1. Define typed row-filter and column-mask policy content.
2. Select policies through the policy-on-tag model and effective tags.
3. Support small, deterministic expression syntax for row predicates and mask selection.
4. Bind authored expressions to the authenticated subject and an Iceberg table schema.
5. Return only closed, typed Iceberg expressions and standard Iceberg mask actions.
6. Fail closed when an applicable restriction cannot be resolved or enforced.
7. Provide an end-to-end path that can later move to official Iceberg runtime types without
   changing policy content.
8. Reserve a fail-closed extension model for UDF references.

## Non-Goals

1. Replacing table-level authorization or granting access through a read-restriction policy.
2. Executing arbitrary expressions or UDFs through the first Iceberg REST implementation.
3. Supporting nested-field masks, roles, identity attributes, nested groups, or general attribute
   expressions in the first version.
4. Defining a new direct policy-to-metadata-object association model.
5. Guaranteeing that a reader without Iceberg read-restriction support can safely read governed
   tables.

## Architecture

The resolution path is:

```text
Policy and tag administration
  -> effective tags for a table and its columns
  -> effective row-filter and column-mask policies
  -> subject and schema binding
  -> canonical Iceberg read restrictions
  -> loadTable response
  -> trusted Iceberg reader enforcement
```

Authorization runs before restriction resolution. A restriction only reduces data visible through
an already-authorized read. It never changes an authorization deny into an allow.

The first implementation runs only where the Iceberg REST service has the trusted end user in its
request context and can resolve Gravitino policies, tags, users, and groups directly.

## Policy Model

This design introduces two built-in policy types:

| Policy type          | Evaluation target | Effect                                             |
| -------------------- | ----------------- | -------------------------------------------------- |
| `system_row_filter`  | Table             | Retains only rows matching one resolved predicate. |
| `system_column_mask` | Top-level column  | Produces a type-preserving SQL masking expression. |

Policies are associated with tags. The existing policy-on-tag resolver selects enabled policies
from effective tags. Row-filter resolution consumes effective policies for a table. Column-mask
resolution consumes effective policies for each top-level column. Direct policy
associations are not used.

### Target scope and tag inheritance

The policy type determines the evaluation target, but it does not restrict where its selecting tag
can be assigned. Effective tags include direct assignments and assignments inherited from metadata
ancestors. A tag assigned to a schema can therefore select a row-filter policy for every descendant
table and a column-mask policy for every descendant column. The policy content does not contain a
second list of table or column targets; tag assignment and policy-on-tag selectors are the only
selection mechanism.

For example, assume schema `sales` contains these tables:

```text
sales.orders(region field-id=4)
sales.customers(region field-id=9)
```

If a tag selecting this policy is assigned only to `sales.orders`, only that table receives the
filter:

```sql
(region STRING) RETURNS BOOLEAN -> region = 'US'
```

The resolver binds `region` to field ID 4 when loading `orders`. If the same tag is assigned to the
`sales` schema, both tables inherit it. The same authored expression is then bound independently to
field ID 4 for `orders` and field ID 9 for `customers`. A descendant table without exactly one
compatible `region` field fails its governed load; the resolver never skips an inherited policy or
returns an unrestricted response because schema binding failed.

For a column mask, assigning a tag directly to `sales.customers.phone` selects the mask only for
that field. Assigning the same tag to `sales` makes it effective for every descendant column. This
is supported, but it is intentionally broad: every selected column must support the resolved mask
action or the table load fails. Administrators should normally assign mask-selecting tags directly
to the affected columns.

Tag-value selectors can narrow an inherited assignment. For example, a schema assignment of
`data_access=restricted` can select a policy through `TAG_VALUE("restricted")`, while a nearer
`data_access=public` assignment on one table overrides the inherited value and does not match that
selector. This exclusion pattern does not work with `ALL_VALUES`, because the nearer assignment
still makes the tag present.

First-version policy content stores one expression. It does not store parser names,
action-vocabulary names, resolved subjects, group membership snapshots, table schemas, field IDs,
or serialized load-table responses. The built-in policy type determines how the expression is
parsed.

Policy selection and policy effect are separate. Tags and policy-on-tag selectors decide whether a
policy is applicable; a row-filter or column-mask definition states what the selected policy does.
The effect content has no `rules` list and no `when` field. Conditional filter results are written
inside one SQL expression. This follows Snowflake's expression model: a row filter produces
a Boolean value and a column mask produces a value of the target column's logical type. SQL
`CASE` expresses conditional results while policy applicability remains separate. A future
principal-aware selector belongs to the selection model, not inside a restriction definition.

### Row-filter content

The content of a row-filter policy contains exactly one `expression`.

```json
{
  "name": "restrict_orders",
  "comment": "Auditors see US orders; other users see their own orders",
  "policyType": "system_row_filter",
  "enabled": false,
  "content": {
    "expression": "(region STRING, owner STRING) RETURNS BOOLEAN -> CASE WHEN is_group_member('auditors') THEN region = 'US' ELSE owner = CURRENT_USER() END"
  }
}
```

The definition declares two string parameters and a Boolean result. `region` and `owner` bind to
the corresponding columns of the target table. In the body, SQL `CASE` means “if the subject is an
auditor, then use the region predicate; otherwise use the owner predicate.” It does not select
another policy rule. The expression may use trusted request context and row values in either
conditions or results. The first policy-on-tag selector version
does not select by principal, so subject-dependent filtering can remain inside this one expression.

### Column-mask content

The content of a column-mask policy also contains exactly one `expression`. Its result has the
same logical type as the target column.

```json
{
  "name": "mask_phone_number",
  "comment": "Auditors see the final four characters; other users see null",
  "policyType": "system_column_mask",
  "enabled": false,
  "content": {
    "expression": "(value STRING) RETURNS STRING -> CASE WHEN is_group_member('auditors') THEN show_last_4(value) ELSE NULL END"
  }
}
```

`value` is explicitly declared as a string parameter. Column-mask evaluation binds that sole
parameter to the original value of the column selected by policy and tag resolution; the
parameter name is local to this definition, not the name of a physical column.
For example, the same expression receives a phone number when applied to `phone_number` and a
card number when applied to `card`. Both columns must match the declared `STRING` type. Parameter
nullability is inherited from the selected column, and the return type must match that column.

The SQL expression returns a masked value rather than an action name. `CASE` selects the first
true condition, returning the transformed column value or a typed null. A mask may also return a
literal or the original column value when its type matches the target. Multiple selected policies
that resolve to distinct masks for the same field remain a conflict. A required restriction that
cannot be represented by the enforcement target must fail closed; SQL syntax alone does not add
new target capabilities.

### Future function reference

A future UDF-backed definition uses a stable function reference rather than inline implementation
source. It replaces `expression`; exactly one of `expression` and `function` can be present.

```json
{
  "function": {
    "reference": "governance.filters.filter_by_region@v3",
    "arguments": [
      { "column": "region" },
      { "literal": { "type": "string", "value": "EMEA" } }
    ]
  }
}
```

The shape is modeled after Databricks ABAC's row-filter UDF and argument binding. `reference`
identifies an immutable function revision. Each argument is explicitly a column or a typed literal;
future schemas can add context and tagged-column bindings without changing existing expression
content.

Before enabling this form, a separate design must define function resolution authority, execution
privileges, determinism, null behavior, and enforcement capabilities. A row-filter function must
return Boolean. A column-mask function must return the exact logical type required for the masked
field. Function arguments and results do not use implicit conversion: every bound argument must
exactly match the declared function signature. A missing, changed, type-mismatched, or unsupported
function fails closed and never falls back to an unrestricted read.

The first Iceberg REST implementation rejects `function`. Future support may enable it only when
the resolver can compile the function to a closed standard Iceberg restriction or when a separately
specified enforcement path declares native function support. A raw function reference never enters
an Iceberg `read-restrictions` response.

## SQL Expressions

Both built-in policy types keep exactly one `expression` string. The authoring language
is `restricted-sql-v1`, using a Snowflake-style typed definition and SQL `CASE` for conditional
results. Every definition contains a parameter list, `RETURNS`, a result type, and `->` followed
by the SQL body. Row filters declare `BOOLEAN`; column masks declare the masked column's logical
type. There is no separate function name or creation statement.

An ANTLR parser builds immutable typed operation nodes and validates complete SQL definitions.
Definitions must use `restricted-sql-v1`; Rego syntax is invalid. The server validates SQL
on policy creation and content updates. Metadata reads, renames, and comment updates do not
reparse source. The experimental loader binds selected definitions to the authenticated subject
and table schema before converting them to closed Iceberg restrictions.

The grammar below defines authoring validity. The experimental converter supports the narrower
enforcement subset documented below; a valid SQL expression that cannot be represented by that
target fails the governed load. The runtime reference is [Policies](../docs/policies.md).

### Authoring Examples

With a string parameter `region`, a row filter declares a Boolean result:

```sql
(region STRING) RETURNS BOOLEAN -> region = 'US'
```

The same filter can express conditional behavior with SQL `CASE`:

```sql
(region STRING) RETURNS BOOLEAN -> CASE
  WHEN CURRENT_USER() = 'alice' THEN TRUE
  ELSE region = 'US'
END
```

With a string target column, a mask declares a string parameter and a string result:

```sql
(value STRING) RETURNS STRING -> CASE
  WHEN CURRENT_USER() = 'alice' THEN value
  ELSE mask_alphanum(value)
END
```

It may also return a literal or null of the expected type:

```sql
(value STRING) RETURNS STRING -> CASE
  WHEN value IS NULL THEN NULL
  ELSE 'XXXXXXXX'
END
```

### Expression Inputs and Results

Every data input must be declared in the parameter list with its logical type. Parameter names
must be unique under the identifier rules below. The body may reference only those parameters
and allowlisted context functions; a column existing in a table does not implicitly declare it.

- A row filter has zero or more parameters. Each parameter binds to a column of the target table
  with the same name under the identifier matching rules. Its declared type must match the column
  exactly. Missing or ambiguous columns are errors. A context-only or constant filter may use an
  empty parameter list, for example `() RETURNS BOOLEAN -> TRUE`.
- A column mask has exactly one parameter. It binds to the selected column regardless of its
  local name, and its declared type must match that column. For example,
  `(xxx STRING) RETURNS STRING -> show_last_4(xxx)` and
  `(value STRING) RETURNS STRING -> show_last_4(value)` differ only in the local parameter name.
  Neither requires a physical column named `xxx` or `value`.

Bound parameters inherit the columns' nullability. No implicit `value` parameter is injected, and
the column's physical name is not an additional alias in a mask body. Trusted context functions
do not require parameter declarations. The definition also declares the result type:

| Use         | Expected result              | Accepted expression forms                                                    |
| ----------- | ---------------------------- | ---------------------------------------------------------------------------- |
| Row filter  | `BOOLEAN`                    | Predicate, Boolean input or literal, Boolean function result, Boolean `CASE` |
| Column mask | Target column's logical type | Original input, literal, null, function result, scalar `CASE`                |

A mask's result type must match the target column, including precision, scale, and timezone
characteristics. Its conditions may use its declared parameter and trusted context functions.
A non-Boolean filter result or incompatible mask result is an error. When one policy applies to several
columns, each mask evaluation binds the parameter independently to its selected column.

### SQL Expression Language

#### Grammar

The complete stored definition includes the parameter and result declarations. The remaining
productions describe its SQL body:

```text
expression  := "(" (parameter ("," parameter)*)? ")" RETURNS type "->" expr EOF
parameter   := identifier type
type        := BOOLEAN | INTEGER | LONG | FLOAT | DOUBLE | STRING | BINARY | DATE
             | DECIMAL "(" unsignedInteger "," unsignedInteger ")"
             | TIME ("(" "6" ")")?
             | TIMESTAMP ("(" "6" ")")? ((WITH | WITHOUT) TIME ZONE)?
expr        := orExpr
orExpr      := andExpr (OR andExpr)*
andExpr     := notExpr (AND notExpr)*
notExpr     := NOT* predicate
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
unsignedInteger := "0" | nonZeroDigit digit*
identifier  := unquotedIdentifier | quotedIdentifier
unquotedIdentifier := (asciiLetter | "_") (asciiLetter | digit | "_")*
quotedIdentifier := '"' (nonDoubleQuote | '""')+ '"'
string      := "'" (nonSingleQuote | "''")* "'"
```

`asciiLetter` means A–Z or a–z. Quoted identifiers must not be empty. Quoted identifiers and
strings decode doubled quotes once; there is no backslash escape layer inside SQL literals.

Keywords and allowlisted built-in function names are case-insensitive. `functionCall` does not
permit arbitrary functions: names and signatures must match the function tables below. `EOF`
requires complete consumption. Semantic validation restricts operand and result types.
Decimal precision must be between 1 and 38, with scale between zero and the precision. Row
definitions must declare `BOOLEAN`; mask definitions must have one parameter and declare its
exact logical type as their result. A body-only string without these declarations is invalid.

Predicate and `CASE` snippets below illustrate the body portion; the stored `expression` always
includes the complete definition shown above.

Precedence, from highest to lowest, is parentheses and function calls, predicates, `NOT`, `AND`,
and `OR`. `NOT active = TRUE` means `NOT (active = TRUE)`. The `AND` inside `BETWEEN` separates
the bounds. Comparisons do not chain: `amount > 10 AND amount < 100` is valid; `10 < amount < 100`
is invalid.

#### Identifiers and Literals

An unquoted identifier starts with an ASCII letter or underscore, followed by ASCII letters,
digits, or underscores. Keywords in this grammar are reserved. A quoted identifier uses double
quotes, escaping an embedded double quote by doubling it, for example `"Customer Region"`.
Unquoted input names normalize to uppercase; quoted names preserve case, following Snowflake's
identifier convention. Names must be unique after that normalization. `val` and `VAL` refer to
the same parameter; `"val"` is distinct. Names bind only within the declared parameter list;
qualified input references and nested paths are unsupported.

String literals use single quotes; an embedded single quote is doubled, as in `'O''Brien'`.
Backslashes are literal characters. Unicode is not normalized; isolated surrogates and NUL are invalid.
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

#### Supported Predicates

| Category            | Examples                                                                                          | Requirements                                                       |
| ------------------- | ------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| Comparison          | `amount = 100`, `amount <> 100`, `amount < 100`, `amount <= 100`, `amount > 100`, `amount >= 100` | Compatible scalar operands, including available inputs             |
| Range               | `amount BETWEEN 10 AND 100`, `amount NOT BETWEEN 10 AND 100`                                      | Inclusive bounds; compatible ordered type                          |
| Membership          | `region IN ('US', 'CA')`, `region NOT IN ('US', 'CA')`                                            | Non-empty literal list; non-null values have compatible types      |
| Null test           | `val IS NULL`, `region IS NOT NULL`                                                               | Typed scalar operand; non-null Boolean result                      |
| NaN test            | `is_nan(score)`, `NOT is_nan(score)`                                                              | Floating-point argument                                            |
| Prefix matching     | `name LIKE 'Acme%'`, `name NOT LIKE 'Acme%'`                                                      | String operand; non-empty fixed prefix followed by exactly one `%` |
| Boolean composition | `region = 'US' AND amount < 100`, `region = 'US' OR region = 'CA'`, `NOT (region = 'US')`         | Boolean operands; parentheses may nest                             |
| Boolean constant    | `TRUE`, `FALSE`                                                                                   | Visible-row predicate                                              |
| Date part           | `year(order_date) = 2026`, `month(order_date) = 3`, `day(order_date) = 15`                        | Date-part result compared with a compatible integer literal        |
| Current user        | `owner = CURRENT_USER()`                                                                          | Compatible string operands                                         |
| Group membership    | `is_group_member('auditors')`                                                                     | Non-empty literal group name; trusted flat membership              |

`!=` is accepted as an alias for `<>`. `BETWEEN` is equivalent to inclusive lower and upper
comparisons joined by `AND`; `NOT BETWEEN` negates that result. Prefix patterns cannot contain
`_` or any other `%`, and the first version has no `ESCAPE` clause. Matching is case-sensitive.

Comparisons between available inputs and constant comparisons are valid when types match.
A non-Boolean result is invalid for a row filter, while a bare input is a valid mask expression when
it has the expected result type. Comparisons with a null value produce `UNKNOWN`; use `IS NULL`
or `IS NOT NULL` to test for null. Lists contain literals only and cannot contain expressions,
references, or nested lists. Null members are accepted with ordinary SQL membership semantics:
if no non-null member matches and at least one member is null, both `IN` and `NOT IN` are unknown.

#### CASE Expressions

Both Snowflake forms of `CASE` are supported:

```sql
CASE
  WHEN region IS NULL THEN FALSE
  WHEN region = 'US' THEN TRUE
  ELSE FALSE
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

### Type and Context Binding

Parameter names and types are explicit in the definition. Type checking validates the SQL body
against those declarations and the result type before evaluation. Binding verifies row-filter
parameters against their target table columns, or the sole mask parameter against the selected
column. Declared types must match exactly; bound nullability comes from the column. A mask's local
parameter name is independent of the physical column name.
Missing or ambiguous names, unavailable inputs, unsupported types, incompatible operands, and
invalid function arguments are errors.

| Gravitino type                          | Supported value predicates                                      |
| --------------------------------------- | --------------------------------------------------------------- |
| `BOOLEAN`                               | Equality and membership                                         |
| Signed `INTEGER`, signed `LONG`         | Equality, ordering, membership, and ranges                      |
| `FLOAT`, `DOUBLE`                       | Equality, ordering, membership, ranges, and NaN tests           |
| `DECIMAL(p,s)`                          | Equality, ordering, membership, and ranges; no rounding         |
| `STRING`                                | Equality, ordering, membership, ranges, and prefix matching     |
| `DATE`                                  | Equality, ordering, membership, ranges, and date-part functions |
| `TIME(6)`                               | Equality, ordering, membership, and ranges                      |
| `TIMESTAMP(6)` with or without timezone | Equality, ordering, membership, ranges, and date-part functions |

Unset temporal precision means six fractional digits in this profile. Other value-predicate types
and temporal precisions are unsupported. Null tests may apply to any top-level column. On a
required column, `IS NULL` is false and `IS NOT NULL` is true.

There is no implicit cross-family conversion, signedness conversion, rounding, or timezone
assumption. Integer values must be mathematical integers within range. Decimal values must fit
the target precision and scale exactly. Floating-point literals preserve negative zero and reject
overflow, infinity, and NaN. Strings compare lexicographically by Unicode code point,
case-sensitively and without Unicode normalization. Constant/context comparisons and bound string
column predicates use the same ordering. `BINARY` supports null tests and compatible scalar masks,
but no value comparisons.
Temporal literals must use the matching type keyword, valid calendar or clock values, and at
most six fractional-second digits. Timestamp-with-timezone literals require an explicit offset.

#### Context Functions

| Function                   | Result             | Meaning                                       |
| -------------------------- | ------------------ | --------------------------------------------- |
| `CURRENT_USER()`           | Non-null `STRING`  | Trusted effective subject name                |
| `is_group_member('group')` | Non-null `BOOLEAN` | Exact flat membership in the current metalake |

`CURRENT_USER()` follows Snowflake's context-function spelling. `is_group_member` retains the
trusted group-membership behavior and is a Gravitino extension, not a Snowflake built-in.
Group names must be non-empty string literals. An unknown group, group lookup failure, missing
subject, or inconsistent identity snapshot is an error. An existing group without the subject
returns false; nested group expansion is unsupported. Context values are stable for one conversion
and use the effective subject. Group lookups are cached within that conversion.

Membership comes from the authenticated principal's supplied groups. An empty group list is valid
and means non-membership. Enabling authorization does not populate groups; authenticators such
as `simple` that supply none take the non-member branch. The referenced group must still exist
in Gravitino. User attributes, including `user_attribute(...)`, are unsupported.

#### Date-Part Functions

`year(column)`, `month(column)`, and `day(column)` accept date and supported timestamp columns.
They return an integer year, month of year, or day of month. Null input returns null.
Timestamp-with-timezone values use UTC; timestamps without timezone use their recorded calendar
fields. Dates use the proleptic Gregorian calendar.

`month(order_date) = 3` matches March dates in every year; it does not imply a particular year.

### Null Semantics

Row filters use SQL three-valued logic: `TRUE`, `FALSE`, and `UNKNOWN` (SQL `NULL`). Only `TRUE`
retains a row. A null column produces:

| Predicate                                | Result    |
| ---------------------------------------- | --------- |
| `column IS NULL`                         | `TRUE`    |
| `column IS NOT NULL`                     | `FALSE`   |
| Comparison with a non-null value         | `UNKNOWN` |
| `IN`, `NOT IN`, `BETWEEN`, `NOT BETWEEN` | `UNKNOWN` |
| `LIKE`, `NOT LIKE`, `is_nan(column)`     | `UNKNOWN` |

`NOT UNKNOWN` is unknown. `FALSE AND UNKNOWN` is false; `TRUE AND UNKNOWN` is unknown.
`TRUE OR UNKNOWN` is true; `FALSE OR UNKNOWN` is unknown.

Consequently, `region <> 'US'` and `NOT (region = 'US')` both exclude null regions. To include
them, write `region IS NULL OR region <> 'US'`. A constant-false filter returns an empty result,
rather than denying access to the table.

### Mask Functions

The Gravitino mask functions defined by this profile remain available inside a SQL mask expression.
They are ordinary typed scalar calls and may appear in `CASE` branches or nested calls with matching
types. They are Gravitino functions rather than a claim of Snowflake function compatibility. Each
function preserves its input logical type. In the examples below, `value` denotes the explicitly
declared mask parameter, which must have a type accepted by the function.

| SQL expression               | Input types                                                        | Meaning                                                                                     |
| ---------------------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------- |
| `mask_alphanum(value)`       | `STRING`                                                           | ASCII digits become `n`; `(),.@-` are preserved; every other Unicode code point becomes `x` |
| `show_first_4(value)`        | `STRING`                                                           | Keep the first four code points; apply the same masking rules to the remainder              |
| `show_last_4(value)`         | `STRING`                                                           | Keep the last four code points; apply the same masking rules to the preceding portion       |
| `mask_to_fixed_value(value)` | `STRING`, numeric, `BOOLEAN`, `DATE`, or supported timestamp types | Replace with the type's fixed value, including null input                                   |
| `replace_with_null(value)`   | Any typed scalar                                                   | Return a null of the input's type; the result is nullable                                   |
| `truncate_to_year(value)`    | Date and supported timestamp types                                 | Truncate to the beginning of the year                                                       |
| `truncate_to_month(value)`   | Date and supported timestamp types                                 | Truncate to the beginning of the month                                                      |
| `sha256_global(value)`       | `STRING`, signed `INTEGER`, signed `LONG`, `BINARY`                | Stable type-preserving SHA-256 transformation                                               |
| `sha256_query_local(value)`  | Same as `sha256_global`                                            | Type-preserving SHA-256 transformation salted per query                                     |

The mask behaviors remain the same. All masks except `mask_to_fixed_value` preserve null input.
Strings of four or fewer Unicode code points are returned unchanged by
`show_first_4` and `show_last_4`. Timestamp-with-timezone truncation uses UTC.

For example, `show_last_4(value)` changes `4111-1111-1111-4444` to `nnnn-nnnn-nnnn-4444`.
`mask_to_fixed_value` returns `'XXXXXXXX'` for strings, zero for numbers, false for booleans,
and the epoch date or timestamp for those temporal types.

Hash inputs use UTF-8 for strings, little-endian two's-complement bytes for signed integers, and
raw bytes for binary. String output is the 64-character lowercase hexadecimal digest; integer
output uses the first four or eight digest bytes interpreted as the corresponding signed
little-endian type; binary output is the complete 32-byte digest. Query-local hashing prepends
a cryptographically random salt of at least 16 bytes, shared by every evaluation in one query;
separate queries use independent salts.

Only allowlisted functions and the shared expression grammar are accepted. Each mask function
takes one compatible scalar argument; additional arguments to these calls are invalid. Conditions
may use the declared mask parameter and trusted context functions. Conditional disclosure is
expressed with ordinary SQL `CASE`, as in the column-mask example above. Row-dependent conditions
and nested mask calls may be valid source but are outside the experimental conversion subset.

A mask expression must have the expected result type on every branch. Returning null, explicitly or
through an omitted `ELSE`, is allowed only for a nullable target column. An expression whose inferred
nullability is incompatible with a required target is rejected. For example, a string-returning
mask cannot return a number in one branch, and a required target cannot use `THEN NULL`, even in
an unselected arm. `IS NULL` and `IS NOT NULL` always produce non-null Boolean results;
`mask_to_fixed_value` also produces a non-null result for null input. Nullability is propagated
by operation rather than inherited unconditionally from every child.

### Validation and Failure Behavior

Static validation checks complete definitions, declared parameter and result types, unique parameter
names, Boolean row-filter results, exact mask result types, every `CASE` branch, supported operand
shapes, allowlisted functions, function arguments, and resource limits. Binding and context
validation check compatible types, literal ranges, nullability, and trusted context values.
Invalid content is rejected; an input or context error must never silently remove a required
restriction. The parser builds typed operation nodes without executing expressions. Binding
checks every declared input before selecting branches and checks mask nullability across all
result arms. Conversion and context lookups evaluate only selected `CASE` results and stop
evaluating later conditions after a match.

The expression limits apply to the complete stored definition, including its parameter list,
return type, and SQL body:

- Source length: 16 KiB of UTF-8.
- Operation depth: 16, including nested `CASE` expressions and function calls.
- Expression nodes: 1024.
- Declared parameters: 256.
- Each decoded quoted identifier or string literal: 4 KiB of UTF-8.
- Numeric literal: 256 bytes.
- `IN` list elements: 1000 per list and 1000 in the complete source.
- Parenthesis nesting: 64 levels.

A consecutive `AND` or `OR` chain is one n-ary operation. Parentheses alone add no AST nodes or
operation depth. Literal lists and all `CASE` arms count toward the node limit, even when an arm
is unreachable. These source limits are separate from the target conversion limits below.

Limits apply before normalization. Unknown functions, unsupported types, and expressions outside
this grammar are errors. This Snowflake-style expression profile supports conditional SQL
expressions without adopting every Snowflake SQL feature. The first version rejects SQL statements,
casts, arithmetic, undeclared column references, nested fields, subqueries, custom functions,
general regular expressions, and general wildcard patterns such as `'%Acme'` or `'A_me'`.

### Experimental Iceberg Conversion

The experimental converter supports the following subset of native Iceberg restrictions:

| SQL form                                                                     | Iceberg conversion                                           |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------ |
| Column compared with a compatible literal or bound context scalar            | Field-ID predicate with SQL null semantics                   |
| Boolean inputs, comparisons, `AND`, `OR`, and `NOT`                          | Three-valued lowering; only true retains a row               |
| `IN`, `NOT IN`, ranges, null tests, prefix `LIKE`, and NaN tests             | Closed predicates with explicit unknown handling             |
| Searched/simple Boolean `CASE` and scalar `CASE` inside supported predicates | Ordered lowering; false and unknown conditions fall through  |
| `year(date_or_timestamp_column)`                                             | Iceberg year transform with calendar-year literal adjustment |
| `month` or `day` on constant temporal values                                 | Scalar constant evaluation                                   |
| Mask returning its local parameter                                           | No projection; retain the original selected column           |
| Null or `replace_with_null(value)`                                           | Null projection on a nullable column                         |
| One of the nine direct mask functions applied to the local parameter         | Corresponding Iceberg projection                             |
| Literal equal to the target type's fixed mask value                          | Fixed-value projection                                       |
| Mask `CASE` whose conditions resolve to constants after binding              | First matching compatible projection                         |

General column-to-column comparisons, month/day extraction from row values, row-dependent mask
selection, composed mask calls, and arbitrary literal mask values are not representable by the
current target and are rejected. An incompatible selected mask fails the table load. SQL
authoring support does not imply that the Iceberg target supports every expression form.

The converter tracks separate predicates for rows where a SQL expression is true and false;
unknown matches neither. Negation exchanges those predicates instead of treating unknown as
false. Ordered `CASE` lowering preserves this distinction for both conditions and results.
Generated Iceberg expressions are limited to 16384 nodes and depth 256, with an additional bound
on compilation work. Exceeding a target limit rejects the governed load.

### Conformance

Examples and validation cases cover every supported operator and type; SQL quoting and escaping;
numeric boundaries, decimal scale, and temporal precision; null comparisons and nested negation;
prefix matching; NaN tests; date parts; current-user and group binding; every mask function;
expected result types; available inputs; searched and simple `CASE`; first-match
evaluation; null conditions and selectors; omitted `ELSE`; nested expressions; incompatible branch
types and nullability; invalid type/function pairs; and all resource limits.

Input cases must verify unknown names, incompatible types, and expected result types. Identifier
cases must verify unquoted case folding and quoted exact names. All expression forms share the
same null, type, and resource-limit rules.

Definition cases must reject missing parameter lists, missing `RETURNS`, duplicate names,
undeclared body references, non-Boolean filter results, and mismatched mask parameter/result types.
Mask-input cases must apply `(value STRING) RETURNS STRING -> show_last_4(value)` to differently
named string columns and verify independent binding, local parameter renaming, inherited
nullability, and type checks. A reference to `phone_number` or `email` in a mask body is invalid
unless it is the explicitly declared parameter. Row-filter cases must resolve declared parameters
against the target table schema and reject mismatched column types.

Conformance also covers rejection of user attributes, NUL, and empty quoted identifiers;
Unicode code-point string ordering for constants, context values, and bound columns;
operation-specific mask nullability, including unselected null arms; and lazy context lookup
on selected `CASE` paths. Versioned golden fixtures pin parsed ASTs and rejected-input diagnostics.
Conversion cases cover the supported target subset, unsupported source-valid expressions,
three-valued negation, and generated-expression and compilation-work limits.

## Resolution and Conflicts

For one authenticated subject and table load:

1. Resolve effective tags for the table and all top-level columns.
2. Resolve enabled policy-on-tag matches.
3. Parse and type-check the expression required by each selected policy type.
4. Bind and resolve each row-filter expression and column-mask expression against the request
   context, table schema, and Iceberg field IDs.
5. Canonicalize resolved restrictions and compute signatures.
6. Deduplicate equal signatures while retaining policy and tag provenance.
7. Reject multiple distinct row-filter signatures for one table.
8. Reject multiple distinct mask-action signatures for one field.

A constant-true row filter is omitted. Constant false remains a deny-all filter. Any loading,
parsing, context, schema, type, action, or conflict error aborts the governed load.

Canonicalization binds context and literals, normalizes comparisons with the field reference first,
preserves SQL three-valued null semantics, folds Boolean constants, sorts and deduplicates
commutative children and `IN` values, and validates the final closed Iceberg expression.
Canonicalization is deterministic and idempotent.

## Iceberg REST Response

When restrictions resolve successfully, the server adds the standard `read-restrictions` object to
the Iceberg load-table response. The response contains at most one required row filter and at most
one required projection per field ID.

The implementation may use Gravitino-owned DTOs and serializers, but its JSON must match the merged
Iceberg REST schema exactly and must not add classes under `org.apache.iceberg`.

Response reconstruction for credentials, snapshot filtering, federation, and other load-table
features must preserve read restrictions.

### Caching

A load-table response can vary without a table metadata commit. Cache identity and ETags for a
governed response must include at least:

- the table metadata representation;
- authenticated subject and identity revision;
- effective policy and tag revisions;
- schema revision; and
- canonical read-restriction signature;
- read-restriction capability version; and
- trusted-reader identity or channel revision.

The existing metadata-location-only conditional-GET fast path must not return `304 Not Modified`
before restriction resolution. A response resolved for one subject must never be reused for another
subject, capability version, or trusted-reader identity.

## Delivery

The implementation ships through the normal Gravitino Iceberg REST build and distribution.

### Client capability declaration

The feature is disabled by default. A compatible client declares support with this request header:

```http
X-Gravitino-Client-Capabilities: read-restrictions.v1
```

`X-Gravitino-Client-Capabilities` follows the generic client-capability pattern proposed for the
Iceberg REST protocol, but uses the Gravitino namespace because that Iceberg proposal is not part of
the published REST specification. The value is a comma-separated list of independent, versioned
capability tokens. For example:

```http
X-Gravitino-Client-Capabilities: read-restrictions.v1,future-capability.v2
```

Capability tokens are lowercase and case-sensitive. A token starts with an ASCII lowercase letter
or digit and then contains only ASCII lowercase letters, digits, `-`, or `.`, so
`read-restrictions.v1` is valid while `Read-Restrictions.v1`, `read_restrictions.v1`, and an empty
token are malformed. Parsing trims optional HTTP whitespace around each comma-separated token and
deduplicates repeated tokens. Unknown well-formed tokens are ignored for forward compatibility. A
malformed header does not declare any capability. A client SDK may send the static header on every
Iceberg REST request; the first implementation consumes `read-restrictions.v1` only for operations
that can return governed table metadata.

Generic Iceberg REST clients can configure the header through their normal custom-header property:

```properties
header.X-Gravitino-Client-Capabilities=read-restrictions.v1
```

For Spark, the corresponding catalog property is:

```properties
spark.sql.catalog.<catalog-name>.header.X-Gravitino-Client-Capabilities=read-restrictions.v1
```

A Gravitino-provided reader integration should set the capability automatically after its complete
enforcement path passes the read-restriction conformance suite. Manual header configuration is a
compatibility declaration only.

### Trust and failure behavior

The capability header is a forward-compatibility signal, not authorization or evidence that a
reader enforces restrictions. A caller can copy it. The deployment must separately bind an
authenticated reader identity or mutually authenticated channel to an operator-reviewed reader
implementation, and it must carry the effective end user through an authorized delegation
mechanism. Governed data credentials must not be issued when that trust check fails.

The server applies these outcomes:

| Active restriction | `read-restrictions.v1` | Trusted reader | Result                                                            |
| ------------------ | ---------------------- | -------------- | ----------------------------------------------------------------- |
| No                 | Missing or present     | Any            | Return the normal response without read restrictions.             |
| Yes                | Missing                | Any            | Return `406 Not Acceptable`; do not return unrestricted metadata. |
| Yes                | Unsupported version    | Any            | Return `406 Not Acceptable`; do not return unrestricted metadata. |
| Yes                | Present                | No             | Return `403 Forbidden`; the header does not establish trust.      |
| Yes                | Present                | Yes            | Return `200 OK` with the resolved read restrictions.              |

An applicable restriction that cannot be loaded, parsed, bound, canonicalized, serialized, or
enforced also fails the request. No error path falls back to an unrestricted response.

When an active restriction applies:

- a client that has not opted in is rejected rather than given an unrestricted response;
- a missing, unsupported, or malformed restriction is rejected;
- the server never assumes that an unknown client enforces an unknown response field.

Compatibility tests pin the Iceberg implementation revision. When official Iceberg runtime support
is available, Gravitino replaces its compatibility DTOs and reader integration with official types
and runs the same conformance fixtures against both implementations.

If Iceberg standardizes `X-Iceberg-Client-Capabilities`, Gravitino can temporarily accept the union
of recognized capabilities from the Iceberg and Gravitino headers, deprecate the Gravitino header,
and remove it in a later major release.

## Persistence and Administration

Policy revisions store the single authored restriction definition, enabled state, and normal policy
audit information.
Resolved field IDs, identity values, and load-table responses are request-scoped and are not stored
as policy content.

Administrators should create a policy disabled, associate it with a tag, preview representative
subjects and tables, verify that a compatible reader is deployed, and then enable it. Enabling a
policy does not make an incompatible reader safe.

Explain output should include selected policies, matching tags, selected conditional branches,
canonical signatures, omission reasons, and conflicts. Query users receive a stable error code and
request ID; policy details remain subject to policy-view authorization.

## Failure and Security Requirements

1. Applicable restrictions fail closed on parse, lookup, binding, serialization, or enforcement
   capability errors.
2. Only the authenticated end user is bound to `CURRENT_USER()`.
3. Policy enforcement uses the unfiltered internal policy result and is independent of whether the
   subject may view policy metadata.
4. Restriction errors and user-facing responses do not reveal policy, tag, or group names without
   the corresponding metadata privileges.
5. Unknown groups and identity lookup failures do not become non-membership.
6. An old client that may ignore `read-restrictions` is not a trusted enforcement target.
7. A `function` definition remains invalid until its complete contract and enforcement capability
   are enabled.
8. A capability declaration does not establish reader trust, end-user identity, or authorization.
9. Governed data credentials and storage authorization prevent reads that bypass the trusted
   restriction-enforcement path.

## Testing

Versioned conformance fixtures cover:

```text
source
  -> unresolved expression
  -> subject and schema binding
  -> canonical Iceberg predicate and actions
  -> loadTable JSON
  -> compatible reader result or fail-closed error
```

Coverage includes:

- every supported operator/type pair and numeric boundary;
- Unicode, escaping, negative zero, temporal precision, and null semantics;
- source limits and invalid operand shapes;
- session user, group membership, unknown groups, and identity lookup failure;
- common keyword boundaries, nested JSON escaping, keyword-named columns, and invalid identifiers;
- exact literal typing and rejection of every implicit-conversion path;
- direct SQL filters, context and row-dependent conditions, searched and simple `CASE`, omitted
  `ELSE`, null conditions, and deterministic conditional evaluation;
- scalar masks, context and row-dependent `CASE` branches, literals and typed nulls, invalid
  result types, unsupported target expressions, and deterministic branch selection;
- reserved function definitions, missing function versions, exact argument and result types, and
  invalid argument bindings;
- duplicate and conflicting row-filter definitions and mask actions;
- all nine mask actions and unsupported action/type pairs;
- table and column effective-tag selection;
- renamed, missing, required, and unsupported fields;
- direct table and column tags, inherited schema tags, tag-value overrides, and broad inherited
  column-mask failures;
- subject, policy, tag, schema, capability version, trusted-reader identity, and metadata cache
  changes;
- missing, malformed, duplicate, unknown, and versioned client capability tokens;
- old-client rejection, spoofed-header rejection, trusted-reader binding, and capability
  negotiation; and
- row filtering before masking in an end-to-end reader test.

## Implementation Plan

1. Add the SQL expression parser, `CASE` evaluation, and typed row-filter and column-mask
   validation; reject unsupported function content explicitly.
2. Add a Gravitino-owned Iceberg read-restriction model and exact wire serializer.
3. Implement policy-on-tag selection, subject binding, schema binding, canonicalization, and conflict
   detection.
4. Integrate restriction resolution with load-table responses and restriction-aware ETags.
5. Add the false-by-default server setting, trusted-reader binding, and versioned
   `X-Gravitino-Client-Capabilities` handling.
6. Build the pinned compatible reader and add end-to-end conformance tests.
7. Replace compatibility protocol and reader classes with official Iceberg types when available.

## References

- [Policy expression runtime reference](../docs/policies.md)
- [Policy-on-tag design](policy-on-tag.md)
- [Apache Iceberg read-restrictions specification](https://github.com/apache/iceberg/pull/13879)
- [Iceberg client-capabilities proposal](https://github.com/apache/iceberg/pull/16394)
- [Pinned Iceberg REST schema](https://github.com/apache/iceberg/blob/6dec25e430b33a8b4f623b14940110d459581826/open-api/rest-catalog-open-api.yaml)
- [Iceberg read-restriction actions implementation](https://github.com/apache/iceberg/pull/16198)
- [Iceberg generic reader implementation](https://github.com/apache/iceberg/pull/16131)
- [Databricks ABAC core concepts](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/core-concepts)
- [Databricks ABAC policy management](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/policies)
- [Databricks row-filter performance and conditional UDF examples](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/performance)
- [Snowflake row access policy expressions](https://docs.snowflake.com/en/sql-reference/sql/create-row-access-policy)
- [Snowflake masking policy expressions](https://docs.snowflake.com/en/sql-reference/sql/create-masking-policy)
- [Snowflake CASE expressions](https://docs.snowflake.com/en/sql-reference/functions/case)
- [Snowflake CURRENT_USER](https://docs.snowflake.com/en/sql-reference/functions/current_user)
