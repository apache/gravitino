---
title: "Policies"
slug: "/policies"
keyword: "policy, policies, governance, tags, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

A policy is a named set of rules in a metalake. Associate it with a tag, then assign that tag to
metadata objects. When a client reads an object's policies, Gravitino finds its effective tags and
returns the enabled policies whose association selectors match. The policy remains a separate
object: changing its rules updates every object where it applies.

Policies come in two kinds. Built-in policy types have rules and consumers defined by Gravitino.
Custom policies carry rules that your own system interprets. For example, the table maintenance
service consumes the built-in Iceberg compaction policy.

## Quick Start

1. [Create a policy](./manage-policies-in-gravitino.md#create-a-policy) and
   [create a tag](./manage-tags-in-gravitino.md#create-a-tag) in the same metalake.
2. [Associate the policy with the tag](./manage-policies-in-gravitino.md#associate-a-policy-with-a-tag).
   Choose `ALL_VALUES` to match tag presence or `TAG_VALUE` to match when an assignment contains
   one specified value.
3. [Assign the tag](./manage-tags-in-gravitino.md#object-operations) to an object or its ancestor.
4. [List the object's policies](./manage-policies-in-gravitino.md#list-policies-on-an-object) to
   confirm the result.

For example, associate `retention_30d` with `data_domain` using
`TAG_VALUE("finance")`. A table with an effective `data_domain=finance` assignment receives the
policy. A table with only `data_domain=risk` does not.

## The Policy Model

### Policy Types and Content

| Type                                 | Rules                                      | Consumer                  |
| ------------------------------------ | ------------------------------------------ | ------------------------- |
| `system_iceberg_compaction`          | Compaction thresholds and scheduling       | Table maintenance service |
| `system_iceberg_rewrite_manifests`   | Manifest count and average-size thresholds | Table maintenance service |
| `system_iceberg_orphan_file_removal` | Orphan-file cleanup options                | Table maintenance service |
| `system_row_filter`                  | Row-filter expression                      | Experimental Iceberg REST |
| `system_column_mask`                 | Column-mask expression                     | Experimental Iceberg REST |
| `system_access_control`              | Privileges and applicable roles            | Not enforced yet          |
| `custom`                             | A free-form map that you define            | A system that you provide |

Row-filter and column-mask policies can be associated with tags and returned as read restrictions
by the experimental Iceberg REST service. Compatible readers enforce those restrictions. The
stable service does not load the experimental provider by default. Access-control policy
enforcement remains separate.

### Read-Restriction Expressions

Row-filter and column-mask expressions use `restricted-sql-v1`. A complete definition declares
its parameters and result type before the SQL body:

```sql
(region STRING, owner STRING) RETURNS BOOLEAN -> CASE
  WHEN is_group_member('auditors') THEN region = 'US'
  ELSE owner = CURRENT_USER()
END
```

```sql
(value STRING) RETURNS STRING -> CASE
  WHEN CURRENT_USER() = 'alice' THEN value
  ELSE show_last_4(value)
END
```

The server validates complete definitions on policy creation and content updates. Ordinary policy
metadata reads, renames, and comment updates do not reparse source. The SQL parser rejects the previous
`filter :=` and `mask :=` syntax.
The experimental Iceberg REST loader binds applicable SQL definitions to the request identity and
table schema, validates target nullability, and converts supported expressions to closed Iceberg
read restrictions.

A row filter declares zero or more parameters and `RETURNS BOOLEAN`. Parameters bind to table
columns by name and must match their logical types. A mask declares exactly one parameter and
returns that exact type. Its parameter name is local: `(xxx STRING) RETURNS STRING -> show_last_4(xxx)`
can apply to a column named `phone_number`. No physical column alias or implicit `value` is injected.
Masks can return the input, a literal, NULL, a scalar function result, or CASE branches.

When the experimental Iceberg REST read-restriction loader resolves several row-filter policies
for one table, a filter that resolves to `true` adds no restriction and is ignored. Identical
effective filters are deduplicated. Distinct restrictive filters conflict and cause the load-table
request to fail; the loader does not combine them.

All mask branches must match the declared result type. A possibly null result, including a missing
ELSE or an explicit NULL branch, is invalid for a required target. If the selected SQL result cannot
be represented by a compatible Iceberg mask, the experimental loader fails the table load.
Unselected CASE results and later conditions are not evaluated after a matching branch.

Experimental Iceberg REST read restrictions require authorization to be enabled with
`gravitino.authorization.enable=true` and a policy dispatcher to be available. When a policy dispatcher
is available but authorization is disabled, the Iceberg REST service fails to start with an error
identifying the required configuration, even when no policy applies. In auxiliary mode, this also
aborts startup of the Gravitino server.

`is_group_member` uses group membership supplied by the authenticated principal; enabling
authorization does not populate these groups. An empty group list is valid and means that the user
is not a member of any group. Authenticators that do not supply groups, including `simple`, therefore
select the non-member branch of group-based policies, which may be unrestricted if the policy allows
it. The referenced group must still exist in Gravitino; an unknown group or an unresolved identity
causes the load-table request to fail.

When the experimental Iceberg REST read-restriction loader is enabled, load-table does not support
names containing `.` in the metalake, catalog, schema, table, or top-level column path. The
metadata-object API reserves `.` as a qualified-name separator, so a tag cannot be assigned
directly to such an object. The loader returns HTTP 400 for these names even when no policy is
assigned. A standalone service without a policy dispatcher does not perform policy resolution.

Supported declaration types are `BOOLEAN`, signed `INTEGER` and `LONG`, `FLOAT`, `DOUBLE`,
`DECIMAL(p,s)`, `STRING`, `BINARY`, `DATE`, `TIME(6)` and `TIMESTAMP(6)` with or without time zone.
Decimal precision is 1–38 and scale is 0–precision. Omitted temporal precision means 6.
Numeric literals are exact until type validation; overflow, rounding, and implicit cross-family
conversions are rejected. Temporal literals use `DATE '2026-01-01'`, `TIME '12:30:00'`,
`TIMESTAMP '2026-01-01 12:30:00'`, or
`TIMESTAMP WITH TIME ZONE '2026-01-01 12:30:00+00:00'`.

The expression body supports:

- Comparisons `=`, `<>` (`!=`), `<`, `<=`, `>`, `>=` between compatible scalar values.
- `IS NULL` and `IS NOT NULL`; Boolean equality and membership, without ordering.
- `IN` and `NOT IN` with non-empty literal lists, including NULL members.
- Inclusive `BETWEEN` and `NOT BETWEEN` ranges.
- `LIKE` and `NOT LIKE` with a non-empty fixed prefix followed by exactly one `%`.
  Other `%` positions, `_` wildcards and `ESCAPE` are rejected.
- `NOT`, `AND`, `OR`, in that precedence order after predicates; parentheses may nest.
- Searched `CASE WHEN condition THEN result ... ELSE result END` and simple
  `CASE selector WHEN match THEN result ... ELSE result END`. ELSE is optional.

CASE preserves branch order and every branch is type checked, even if unreachable. SQL nulls
are retained in the parsed tree: only TRUE keeps a row, NOT NULL remains unknown, and ordinary
comparisons with NULL are unknown. A missing ELSE returns NULL. Schema binding must reject a
nullable mask result for a required column. Binary values support null tests and compatible mask
functions, but not value predicates.

Context functions are `CURRENT_USER()` and `is_group_member('group')`. Group names must be
non-empty string literals. Context values come from the trusted effective subject and remain stable
for a conversion. User attributes are not supported.
`is_nan(value)` accepts FLOAT or DOUBLE; `year(value)`, `month(value)`, and `day(value)` accept DATE
or supported TIMESTAMP values and return INTEGER.

The type-preserving mask functions each take one scalar argument:

| Function                                       | Input type                                    |
| ---------------------------------------------- | --------------------------------------------- |
| `mask_alphanum`, `show_first_4`, `show_last_4` | STRING                                        |
| `mask_to_fixed_value`                          | STRING, numeric, BOOLEAN, DATE or TIMESTAMP   |
| `replace_with_null`                            | Any typed scalar                              |
| `truncate_to_year`, `truncate_to_month`        | DATE or TIMESTAMP                             |
| `sha256_global`, `sha256_query_local`          | STRING, signed INTEGER, signed LONG or BINARY |

Keywords and built-in function names are case-insensitive. Unquoted identifiers use ASCII
letters or underscore followed by letters, digits or underscores, and normalize to uppercase.
Double-quoted identifiers preserve case; double a quote to escape it. Parameter names must be
unique after normalization. String literals use single quotes, with doubled single quotes for
escaping. Backslashes are literal and Unicode is preserved; isolated surrogates and NUL are rejected.
Comments, statements, semicolons, casts, arithmetic, qualified names, nested fields, subqueries,
unknown functions, exponents, leading `+`, leading zeroes and trailing decimal points are rejected.

Limits apply before normalization and serve different purposes:

| Resource                                                      | Limit           |
| ------------------------------------------------------------- | --------------- |
| Complete definition, including declarations                   | 16 KiB of UTF-8 |
| Expression nodes                                              | 1024            |
| Operation depth, including CASE and function calls            | 16              |
| IN elements, both per list and across the complete definition | 1000            |
| Parenthesis nesting                                           | 64              |
| Declared parameters                                           | 256             |
| Each decoded quoted identifier or string literal              | 4 KiB of UTF-8  |
| Each numeric literal                                          | 256 bytes       |

Node and IN limits allow ordinary SQL predicates and membership lists while bounding parsing and
validation work. Depth limits bound recursive operations independently of expression width. A
consecutive AND/OR chain is one n-ary operation; parentheses alone add no AST nodes or operation
depth. Literal lists and every CASE arm count toward the node limit even if a branch is unreachable.

#### Complete SQL Grammar

The stored definition must match this grammar and consume the complete input. Keywords and
allowlisted function names are case-insensitive; arbitrary function names remain invalid.

```text
definition  := "(" (parameter ("," parameter)*)? ")" RETURNS type "->" expr EOF
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
scalar      := identifier | literal | functionCall | caseExpr | "(" expr ")"
functionCall := identifier "(" (expr ("," expr)*)? ")"
caseExpr    := CASE expr? (WHEN expr THEN expr)+ (ELSE expr)? END
compareOp   := "=" | "<>" | "!=" | "<" | "<=" | ">" | ">="
literal     := string | number | TRUE | FALSE | NULL | temporal
temporal    := (DATE | TIME | TIMESTAMP) string | TIMESTAMP WITH TIME ZONE string
number      := "-"? ("0" | nonZeroDigit digit*) ("." digit+)?
unsignedInteger := "0" | nonZeroDigit digit*
digit       := "0" | nonZeroDigit
nonZeroDigit := "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"
identifier  := unquotedIdentifier | quotedIdentifier
unquotedIdentifier := (asciiLetter | "_") (asciiLetter | digit | "_")*
quotedIdentifier := '"' (nonDoubleQuote | '""')+ '"'
string      := "'" (nonSingleQuote | "''")* "'"
```

`asciiLetter` means A–Z or a–z. Keywords shown in the grammar are reserved; quote them when used
as parameter names. Quoted identifiers and strings preserve Unicode and decode doubled quotes
once. There is no backslash escape layer inside SQL literals. Every number must end at a token
boundary: `1and TRUE` is invalid. The `AND` in `BETWEEN` separates its two scalar bounds;
`NOT active = TRUE` means `NOT (active = TRUE)`. Chained comparisons are invalid.

#### Result Types and Binding Contract

Each declared row-filter input must resolve to exactly one table column under the identifier
matching rules; each declared type must match exactly. Missing, ambiguous or mismatched inputs
are errors. A context-only filter can declare no inputs:

```sql
() RETURNS BOOLEAN -> is_group_member('auditors')
```

A column mask has exactly one local parameter and the same result type, including decimal
precision/scale and timestamp time-zone characteristics. Binding applies that parameter to the
selected column regardless of its physical name and inherits that column's nullability. A physical
name that was not declared is not an alias. For example, this can mask either `phone_number` or
`card`, provided its selected column has type STRING:

```sql
(value STRING) RETURNS STRING -> show_last_4(value)
```

BINARY supports null tests and compatible scalar masks, but no value comparisons. BOOLEAN
supports equality and membership; all other supported predicate types also support ordering and
ranges. Temporal literals require matching type keywords, valid calendar/clock values and at most
six fractional digits; timestamp-with-timezone literals require an explicit offset. Integer
literals must fit the signed range and be mathematical integers. Decimals must fit without
rounding. Floating-point literals retain negative zero and reject overflow, infinity and NaN.
Strings compare case-sensitively without Unicode normalization.

#### CASE and SQL NULL Semantics

A searched CASE evaluates conditions in source order; only TRUE selects a branch. FALSE and
UNKNOWN skip it. A simple CASE compares its selector with each match using SQL equality, so a
NULL selector does not match a NULL match value. Only the selected result is evaluated. If no arm
matches, CASE returns ELSE, or SQL NULL when ELSE is absent. Every result arm is statically type
checked, including unreachable arms. Nested CASE and function calls obey the same limits.

A filter retains rows only when its result is TRUE. These are SQL three-valued rules:

| Expression                                | Result          |
| ----------------------------------------- | --------------- |
| `NULL IS NULL` / `NULL IS NOT NULL`       | TRUE / FALSE    |
| Comparison with a NULL operand            | UNKNOWN         |
| `NOT NULL`                                | UNKNOWN         |
| `FALSE AND NULL` / `TRUE AND NULL`        | FALSE / UNKNOWN |
| `TRUE OR NULL` / `FALSE OR NULL`          | TRUE / UNKNOWN  |
| NULL input to IN, BETWEEN, LIKE or is_nan | UNKNOWN         |
| Nonmatching IN list containing NULL       | UNKNOWN         |
| Nonmatching NOT IN list containing NULL   | UNKNOWN         |

For example, both `region <> 'US'` and `NOT (region = 'US')` exclude null regions. Use
`region IS NULL OR region <> 'US'` to include them. A constant FALSE filter returns no rows.
A mask that can return NULL, including an omitted ELSE, requires a nullable target. The binding
and enforcement layer must reject a nullable result for a required column.

#### Function Contract

`CURRENT_USER()` returns the non-null STRING name of the trusted effective subject.
`is_group_member('group')` returns BOOLEAN for exact flat membership in the current metalake.
An unknown group, unavailable subject or failed lookup is an error; an existing group without
the subject returns FALSE. Nested group expansion is unsupported.

Context values remain stable for a conversion. User-attribute functions and implicit
string-to-number conversions are unsupported.

Date parts `year`, `month`, and `day` return INTEGER and preserve null input. Timestamp-with-timezone
values use UTC; timestamps without timezone use their recorded calendar fields. Dates use the
proleptic Gregorian calendar. `month(order_date) = 3` means March in any year.

Mask function behavior is as follows; each call takes one compatible scalar argument and preserves
its input logical type:

| Function                                 | Behavior                                                                                                                                |
| ---------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `mask_alphanum`                          | ASCII digits become `n`; `(),.@-` are preserved; other Unicode code points become `x`                                                   |
| `show_first_4` / `show_last_4`           | Preserve the first/last four code points and mask the remainder as above; strings of four or fewer code points are unchanged            |
| `mask_to_fixed_value`                    | Return `XXXXXXXX` for STRING, zero for numeric values, FALSE for BOOLEAN, or the epoch date/timestamp, including for null input         |
| `replace_with_null`                      | Return a typed null                                                                                                                     |
| `truncate_to_year` / `truncate_to_month` | Truncate to the start of the year/month, using UTC for timestamps with time zone                                                        |
| `sha256_global` / `sha256_query_local`   | Type-preserving SHA-256; the query-local variant prepends a cryptographically random salt of at least 16 bytes, shared within one query |

All masks except `mask_to_fixed_value` preserve null input. Hash input encoding is UTF-8 for
strings, little-endian two's-complement for signed integers, and raw bytes for BINARY. STRING
results are 64 lowercase hexadecimal characters; INTEGER/LONG results are the first four/eight
digest bytes interpreted as signed little-endian; BINARY results contain the complete 32-byte
digest. Separate queries require independent query-local salts.

#### Validation Boundary

ANTLR parses the complete stored definition, then an AST visitor builds separate typed operation
nodes. Static validation checks parameter/result declarations, unique names, available local
references, every CASE arm, operand types, allowlisted calls and arguments, exact literal ranges,
and source limits. Golden fixtures pin parsed ASTs and rejected-input diagnostics.

The parser does not execute expressions. The experimental converter binds every declared input
before selecting branches, checks exact schema types and mask nullability, snapshots the effective
subject, and caches trusted group lookups for a conversion. Any binding, translation or
capability error fails the governed load; an unsupported restriction never becomes an unrestricted
response.

#### Experimental Iceberg Conversion

The current converter supports the following enforcement subset of the shared SQL grammar:

| SQL form                                                                 | Iceberg conversion                                                      |
| ------------------------------------------------------------------------ | ----------------------------------------------------------------------- |
| Column and compatible literal/context scalar comparison                  | Bound field-ID predicate, preserving SQL NULL semantics                 |
| Boolean inputs, AND/OR/NOT and Boolean comparisons                       | Three-valued lowering; only TRUE retains a row                          |
| IN/NOT IN, BETWEEN/NOT BETWEEN, null tests, prefix LIKE and NaN tests    | Closed native predicates with explicit unknown handling                 |
| Searched/simple Boolean CASE and scalar CASE inside supported predicates | Ordered conditional lowering; false and unknown conditions fall through |
| `year(date_or_timestamp_column)`                                         | Iceberg year transform with calendar-year literal adjustment            |
| `month`/`day` on constant temporal values                                | Evaluated as scalar constants                                           |
| Mask returning its local input                                           | No projection, preserving the original selected column                  |
| NULL or `replace_with_null`                                              | Null projection on a nullable column                                    |
| One of the nine direct mask functions applied to the local input         | Corresponding Iceberg projection                                        |
| A literal equal to the target type's fixed mask value                    | Fixed-value projection                                                  |
| Mask CASE using conditions that resolve to constants after binding       | First matching compatible projection                                    |

General column-to-column comparisons, month/day extraction from row values, row-dependent mask
selection, composed mask calls and arbitrary literal mask values are not representable by the
current closed target and are rejected. A SQL mask parameter binds to the selected column by field
ID, independently of its local name. Quoted row-filter declarations match exact physical names;
unquoted declarations match case-folded names and reject ambiguous matches.

Source limits still apply. Conversion additionally bounds generated Iceberg expressions to 16384
nodes and depth 256, and bounds compilation work. This protects conditional lowering and
serialization independently of source width. Exceeding a target limit rejects the load. Closed
filters and mask projections are canonicalized deterministically for deduplication, conflict
checks and response ETags.

### Policy-to-Tag Associations

Each association connects one policy to one tag and stores a selector. The selector determines
whether that association contributes the policy to an object's lookup result.

| Selector       | When it matches                                                            |
| -------------- | -------------------------------------------------------------------------- |
| `ALL_VALUES`   | The effective tag is present, including an assignment without a value.     |
| `TAG_VALUE`    | One of the effective tag assignment values equals the specified value.     |

A policy may be associated with multiple tags. Association listings show those direct relations
and their selectors, even if no object currently matches them. An object policy lookup returns each
matching policy once.

A selector cannot be changed in place. Remove the policy-to-tag association and add it again with
the new selector. Removing an association leaves the policy, tag, and tag assignments intact.
Deleting a policy removes its associations.

Object policy lookup covers `CATALOG`, `SCHEMA`, `TABLE`, `VIEW`, `COLUMN`, `FILESET`, `TOPIC`,
`MODEL`, `MODEL_VERSION`, `SEMANTIC_MODEL`, and `FUNCTION`. A model version cannot carry a tag directly,
but it can inherit tags from its model and higher ancestors, so their policies appear in model
version lookups.
This also includes columns, which did not support direct policy associations.
A tag assigned to a catalog or schema can therefore make its policies appear in descendant column
lookups. Because lookup does not filter by `supportedObjectTypes`, a policy whose content lists
only `TABLE` can still appear in a column lookup; consumers must enforce the intended scope.

### Effective Tags and Inheritance

An object receives tags assigned directly to it and tags inherited from its metadata object
ancestors. Resolution starts at the object and walks upward, so the nearest assignment of a tag
name wins, including its assignment values. A direct assignment therefore overrides every
ancestor, and a schema assignment overrides the same tag assigned on its catalog for the schema's
descendants. Tag names themselves are flat; tags do not inherit from other tags.

For example, a catalog with `data_domain=finance` gives its tables that effective assignment.
A table assigned `data_domain=risk` instead uses `risk`, so a policy associated with
`TAG_VALUE("finance")` no longer matches the table. `ALL_VALUES` still matches because the
tag is present.

Object policy lookup is read-only. To change its result, update the policy or its enabled state,
change a policy-to-tag association, or change a tag assignment on the object or an ancestor.
With `details=true`, the lookup includes an `inherited` field. It is `true` when the policy
matches only through an inherited tag.

### Enabled State

Disabling a policy preserves the policy and its tag associations, but excludes it from object
policy lookup. Policy and association listings still show it. Enabling it makes matching object
lookups include it again.

## Managing Policies

The UI supports policy lifecycle operations such as creating custom policies, editing their
content, and changing their enabled state. Use the REST API or Java client to manage
policy-to-tag associations and to read the resulting object policies. The
[Manage Policies](./manage-policies-in-gravitino.md) guide has requests and examples.
For existing direct object policy associations, see
[Migration Guide](./migration-guide.md).

To add or remove an association, a user must own the metalake or have the required access to both
the tag and the policy (`APPLY_TAG` and `APPLY_POLICY`, or ownership of each). Object policy
reads also respect the caller's access to the returned policies. `VIEW_TAG` and `VIEW_POLICY`
grant read-only access to tags and policies; `APPLY_TAG` and `APPLY_POLICY` also allow reads.
