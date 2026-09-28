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
provides a standard enforcement boundary for portable restrictions. Some engines also support
native filter expressions or functions that cannot be represented by the common Iceberg profile,
so policy content must distinguish portable definitions from explicitly engine-scoped ones.

This design adds tag-based row-filter and column-mask policies and resolves them into Iceberg REST
`read-restrictions`. The first implementation is experimental while Iceberg reader support is
maturing. It is disabled by default and requires an explicit client opt-in.

## Goals

1. Define typed row-filter and column-mask policy content.
2. Select policies through the policy-on-tag model and effective tags.
3. Support a small, deterministic common authoring language for row predicates.
4. Bind authored expressions to the authenticated subject and an Iceberg table schema.
5. Return only closed, typed Iceberg expressions and standard Iceberg mask actions.
6. Fail closed when an applicable restriction cannot be resolved or enforced.
7. Provide an experimental end-to-end path that can later move to official Iceberg runtime types
   without changing policy content.
8. Reserve an explicit, fail-closed extension model for engine expressions and UDF references.

## Non-Goals

1. Replacing table-level authorization or granting access through a read-restriction policy.
2. Executing arbitrary engine expressions or UDFs through the first Iceberg REST implementation.
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

The first experimental implementation runs only where the Iceberg REST service has the trusted end
user in its request context and can resolve Gravitino policies, tags, users, and groups directly.

## Policy Model

This design introduces two built-in policy types:

| Policy type | Evaluation target | Effect |
| --- | --- | --- |
| `system_row_filter` | Table | Retains only rows matching one resolved predicate. |
| `system_column_mask` | Top-level column | Replaces visible values using one Iceberg mask action. |

Policies are associated with tags. The existing policy-on-tag resolver selects enabled policies
from effective tags. Row-filter resolution consumes effective policies for a table. Column-mask
resolution consumes effective policies for each top-level column. Direct policy
associations are not used.

Policy content stores one restriction definition and its declared language or action profile. It
does not store a resolved subject, group membership snapshot, table schema, field ID, or serialized
load-table response.

Policy selection and policy effect are separate. Tags and policy-on-tag selectors decide whether a
policy is applicable; a row-filter or column-mask definition states what the selected policy does.
The effect content has no ordered rules and no `when` field. This follows the same separation used
by Databricks ABAC, where policy applicability is distinct from the row-filter or mask function and
its bound inputs. A future principal-aware selector belongs to the selection model, not inside a
restriction definition.

### Row-filter content

A row-filter policy contains exactly one `filter`. The common form has `kind` set to `expression`,
uses the `gravitino-filter-v1` language, and omits `engine`.

```json
{
  "name": "restrict_orders",
  "comment": "Users see only their own orders",
  "policyType": "system_row_filter",
  "enabled": false,
  "content": {
    "filter": {
      "kind": "expression",
      "language": "gravitino-filter-v1",
      "expression": "col(\"owner\") == session_user()"
    }
  }
}
```

An expression may use trusted request context, including `session_user()` and
`is_group_member(...)`, but context does not select another rule. If different subjects need
different policy applicability, the selection layer must express that distinction. Until a
principal-aware selector is designed, authors can express a Boolean distinction in one row
predicate, but should not use an ordered rule list as a substitute for selection semantics. The
first policy-on-tag selector version does not select by principal.

### Column-mask content

A column-mask policy contains exactly one `mask`. The first version supports the portable
`iceberg-action-v1` profile.

```json
{
  "name": "mask_phone_number",
  "comment": "Show only the final four characters",
  "policyType": "system_column_mask",
  "enabled": false,
  "content": {
    "mask": {
      "kind": "action",
      "profile": "iceberg-action-v1",
      "action": "show-last-4"
    }
  }
}
```

If more than one selected policy defines a different mask for the same field, resolution fails as a
conflict. Subject-dependent mask applicability requires a principal-aware policy selector; it is
not encoded as a mask condition.

### Restriction definition variants

The `kind` discriminator prevents an engine expression or function reference from being
misinterpreted as a common expression. The content model reserves these variants:

| Kind | Scope | First Iceberg REST implementation |
| --- | --- | --- |
| `expression` without `engine` | Common, portable profile named by `language` | Supports `gravitino-filter-v1` for row filters. |
| `expression` with `engine` | Exact engine and language pair | Rejected unless the selected adapter advertises that exact pair and produces a closed common result. |
| `function` | Versioned function reference and typed argument bindings | Reserved; rejected by the first implementation. |
| `action` | Named portable action profile | Supports `iceberg-action-v1` for column masks. |

An engine-scoped row-filter definition is explicit:

```json
{
  "filter": {
    "kind": "expression",
    "engine": "spark",
    "language": "spark-sql-3.5",
    "expression": "owner = current_user()"
  }
}
```

`engine` and `language` are non-empty, case-sensitive identifiers defined by an adapter capability;
the language identifier must include a compatibility version. An omitted `engine` means common,
and the literal engine name `common` is invalid. An engine-scoped definition is opaque to the
common parser, is never sent to a different engine, and must not fall back to a common or differently
versioned language. It is valid only when the adapter can validate it, bind all identifiers and
context, and lower it to the closed Iceberg predicate profile before an Iceberg REST response is
returned. Raw engine text never crosses the Iceberg `read-restrictions` boundary.

A future UDF-backed definition uses a stable function reference rather than inline implementation
source:

```json
{
  "filter": {
    "kind": "function",
    "engine": "databricks",
    "functionReference": "governance.filters.filter_by_region@v3",
    "arguments": [
      { "kind": "column", "name": "region" },
      { "kind": "literal", "type": "string", "value": "EMEA" }
    ]
  }
}
```

The shape is modeled after Databricks ABAC's row-filter UDF and argument binding. Before enabling
this variant, a separate profile must define function identity and versioning, resolution
authority, argument types, context arguments, execution privileges, determinism, null behavior,
and target capabilities. A row-filter function must return Boolean, and its profile must define how
a null result is handled. A mask function must return a value compatible with the masked field. A
missing, changed, or unsupported function fails closed; it never falls back to an unrestricted
read. Tagged-column matching and context argument bindings can be added as new argument kinds
without changing the function variant.

## Common Expression Profile

The `gravitino-filter-v1` profile is the portable authoring language for row-filter expressions.

The grammar is:

```text
expr        := orExpr
orExpr      := andExpr ("or" andExpr)*
andExpr     := notExpr ("and" notExpr)*
notExpr     := "not" notExpr | compareExpr
compareExpr := primary (("==" | "!=" | "<" | "<=" | ">" | ">=" | "in") primary)?
primary     := colRef | sessionUser | groupMember | literal | array | "(" expr ")"
colRef      := "col" "(" string ")"
sessionUser := "session_user" "(" ")"
groupMember := "is_group_member" "(" string ")"
literal     := string | number | boolean | null
array       := "[" literal ("," literal)* "]"
boolean     := "true" | "false"
null        := "null"
number      := "-"? ("0" | nonZeroDigit digit*) ("." digit+)?
digit       := "0" | nonZeroDigit
nonZeroDigit := "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"
```

Strings use JSON double-quoted syntax. Comments, exponent notation, leading `+`, leading zeroes,
trailing decimal points, chained comparisons, arbitrary functions, and bare non-Boolean roots are
invalid.

### Keywords, identifiers, and escaping

The reserved, lowercase keywords are `and`, `or`, `not`, `in`, `true`, `false`, and `null`. The
reserved built-in function identifiers are `col`, `session_user`, and `is_group_member`. They are
case-sensitive and are recognized only as complete tokens; for example, `notebook` is not `not`
followed by an identifier. Bare identifiers are not part of `gravitino-filter-v1`, so an unknown
word is always invalid rather than an implicit column reference or function call.

Column names, group names, and string values appear only as JSON string literals. A name equal to a
keyword needs no special keyword escape: `col("and")` references the column named `and`. Backticks,
single quotes, SQL delimited identifiers, and backslash escaping outside a JSON string are invalid.

There are two syntactic JSON layers in an API request. The HTTP JSON parser decodes the outer
`expression` field once, and the expression parser decodes each inner JSON string literal once. For
example, the request fragment `"expression": "col(\"and\") == \"open\""` becomes the source
`col("and") == "open"`, whose decoded column name is `and`. No layer performs an additional or
implicit unescape.

After decoding, identifiers and values are preserved exactly. Gravitino performs no Unicode
normalization, case folding, whitespace trimming, environment expansion, URL decoding, or SQL
quoting. Adapters must bind typed AST nodes or parameters and must not concatenate decoded names or
values into engine text. Canonical serialization applies JSON escaping; it does not change the
logical value.

Supported operand shapes are:

| Form | Operators | Requirements |
| --- | --- | --- |
| Boolean predicates | `and`, `or`, `not` | Every operand is Boolean. |
| Column and non-null literal | `==`, `!=`, `<`, `<=`, `>`, `>=` | Either operand order; compatible types. |
| String column and session user | `==`, `!=` | Either operand order. |
| Session user and string literal | `==`, `!=` | Folded during context binding. |
| Column and `null` | `==`, `!=` | Becomes `is-null` or `not-null`. |
| Column and literal array | `in` | Column on the left; non-empty homogeneous array. |
| Session user and string array | `in` | Folded during context binding. |
| Group membership | none | Folded during context binding. |

Column-to-column comparisons, literal-to-literal comparisons, ordering on `session_user()`, null
array elements, nested arrays, and comparisons on `is_group_member(...)` are invalid.

### Limits

Save-time validation applies these limits before canonicalization:

- source length: 16 KiB of UTF-8;
- operation depth: 8;
- AST nodes: 256;
- decoded string literal: 4 KiB of UTF-8; and
- array elements: 256.

The resolved predicate also has maximum operation depth 8. Canonicalization cannot make an
oversized expression valid.

## Context and Schema Binding

The first expression profile provides two request-stable context functions:

| Function | Result | Binding |
| --- | --- | --- |
| `session_user()` | Non-null string | Authenticated effective user name. |
| `is_group_member("group")` | Non-null Boolean | Exact flat membership in the current metalake. |

An unknown group, group lookup failure, missing subject, or inconsistent identity snapshot is a
resolution error. An existing group that does not contain the subject returns `false`. Nested group
expansion is not performed.

Each column name is bound exactly once against the concrete table schema and converted to its stable
Iceberg field ID. Missing or ambiguous columns, incompatible types, schema drift, or unsupported
field-ID mapping fail closed.

The first version supports these value predicates:

| Gravitino type | Iceberg type | Predicates |
| --- | --- | --- |
| `BOOLEAN` | `boolean` | Equality and `in` |
| `INTEGER` | `int` | Equality, ordering, and `in` |
| `LONG` | `long` | Equality, ordering, and `in` |
| `FLOAT` | `float` | Equality, ordering, and `in`; finite values only |
| `DOUBLE` | `double` | Equality, ordering, and `in`; finite values only |
| `DECIMAL(p,s)` | `decimal(p,s)` | Equality, ordering, and `in`; no rounding |
| `DATE` | `date` | Equality, ordering, and `in` |
| `TIME(6)` or unset precision | `time` | Equality, ordering, and `in` |
| `TIMESTAMP(6)` without timezone or unset precision | `timestamp` | Equality, ordering, and `in` |
| `STRING` | `string` | Equality, ordering, and `in` |

Null tests may apply to any nullable top-level field with a stable field ID. No implicit numeric,
temporal, collation, signedness, or timezone coercion is allowed.

All context functions and named references are removed before a resolved predicate is serialized.
The Iceberg wire expression contains only Boolean constants, logical operators, supported
predicates, field-ID references, and typed literals.

## Null Semantics

Resolved predicates use Iceberg two-valued semantics. For a null field and a non-null literal:

| Predicate | Result |
| --- | --- |
| `is-null` | `true` |
| `not-null` | `false` |
| `eq`, `gt`, `gt-eq`, or `in` | `false` |
| `not-eq`, `lt`, or `lt-eq` | `true` |

Therefore, `col("region") != "US"` retains rows where `region` is null. Authors who want to
exclude nulls must add `col("region") != null`.

## Column Masks

The action vocabulary is the Iceberg read-restriction action profile:

- `mask-alphanum`;
- `mask-to-fixed-value`;
- `replace-with-null`;
- `show-first-4`;
- `show-last-4`;
- `truncate-to-year`;
- `truncate-to-month`;
- `sha-256-global`; and
- `sha-256-query-local`.

Applicable types, fixed values, output encodings, Unicode behavior, and null behavior follow the
pinned Iceberg specification. Unknown actions and unsupported action/type pairs fail closed.
`replace-with-null` is invalid for a required field.

The server returns the logical Iceberg action, and the reader owns execution. For
`sha-256-query-local`, the reader also owns generation and lifecycle of the per-query salt defined
by Iceberg.

Only top-level fields are supported in the first version. A row filter evaluates original values,
and masks apply afterward to surviving rows.

## Resolution and Conflicts

For one authenticated subject and table load:

1. Resolve effective tags for the table and all top-level columns.
2. Resolve enabled policy-on-tag matches.
3. Check each selected definition's kind, language, engine scope, and adapter capability.
4. Bind each common or safely lowered row expression and each selected mask to the table schema and
   Iceberg field IDs.
5. Canonicalize resolved restrictions and compute signatures.
6. Deduplicate equal signatures while retaining policy and tag provenance.
7. Reject multiple distinct row-filter signatures for one table.
8. Reject multiple distinct mask-action signatures for one field.

A constant-true row filter is omitted. Constant false remains a deny-all filter. Any loading,
parsing, context, schema, type, action, or conflict error aborts the governed load.

Canonicalization binds context and literals, normalizes comparisons with the field reference first,
rewrites null comparisons, folds Boolean constants, sorts and deduplicates commutative children and
`in` values, and validates the final closed Iceberg profile. Canonicalization is deterministic and
idempotent.

## Iceberg REST Response

When restrictions resolve successfully, the server adds the standard `read-restrictions` object to
the Iceberg load-table response. The response contains at most one required row filter and at most
one required projection per field ID.

The experimental implementation may use Gravitino-owned DTOs and serializers, but its JSON must
match the merged Iceberg REST schema exactly. Experimental Java types use the
`org.apache.gravitino.iceberg.experimental.restrictions` namespace and must not add classes under
`org.apache.iceberg`.

Response reconstruction for credentials, snapshot filtering, federation, and other load-table
features must preserve read restrictions.

### Caching

A load-table response can vary without a table metadata commit. Cache identity and ETags for a
governed response must include at least:

- the table metadata representation;
- authenticated subject and identity revision;
- effective policy and tag revisions;
- schema revision; and
- canonical read-restriction signature.

The existing metadata-location-only conditional-GET fast path must not return `304 Not Modified`
before restriction resolution. A response resolved for one subject must never be reused for another
subject.

## Experimental Delivery

The first implementation is delivered by the
`iceberg:iceberg-read-restrictions-experimental` module and is not a stable public API. It is
disabled by default.

An operator enables the feature with an experimental configuration, and a client opts in with the
`read-restrictions` experiment token. Both are required. The client token is capability negotiation,
not authorization.

When an active restriction applies:

- a client that has not opted in is rejected rather than given an unrestricted response;
- a missing, unsupported, or malformed restriction is rejected;
- the server never assumes that an unknown client enforces an unknown response field.

The experimental reader package pins the Iceberg implementation revision used for compatibility
tests. It is not published as a stable Iceberg replacement. When official Iceberg runtime support is
available, Gravitino replaces the experimental DTO and reader integration with official types and
runs the same conformance fixtures against both implementations.

## Persistence and Administration

Policy revisions store the single authored restriction definition, enabled state, and normal policy
audit information.
Resolved field IDs, identity values, and load-table responses are request-scoped and are not stored
as policy content.

Administrators should create a policy disabled, associate it with a tag, preview representative
subjects and tables, verify that the experimental reader is deployed, and then enable it. Enabling a
policy does not make an incompatible reader safe.

Explain output should include selected policies, matching tags, definition kinds and profiles,
canonical signatures, omission reasons, and conflicts. Query users receive a stable error code and
request ID; policy details remain subject to policy-view authorization.

## Failure and Security Requirements

1. Applicable restrictions fail closed on parse, lookup, binding, serialization, or enforcement
   capability errors.
2. Only the authenticated end user is bound to `session_user()`.
3. Policy enforcement uses the unfiltered internal policy result and is independent of whether the
   subject may view policy metadata.
4. Restriction errors and user-facing responses do not reveal policy, tag, or group names without
   the corresponding metadata privileges.
5. Unknown groups and identity lookup failures do not become non-membership.
6. An old client that may ignore `read-restrictions` is not a trusted enforcement target.
7. An engine-scoped expression is accepted only for an exact engine, language, and adapter
   capability match; no implicit translation or fallback is allowed.
8. A `function` definition remains invalid until its complete function profile and enforcement
   capability are enabled.

## Testing

Versioned conformance fixtures cover:

```text
source
  -> unresolved expression
  -> subject and schema binding
  -> canonical Iceberg predicate and actions
  -> loadTable JSON
  -> experimental reader result or fail-closed error
```

Coverage includes:

- every supported operator/type pair and numeric boundary;
- Unicode, escaping, negative zero, temporal precision, and null semantics;
- source limits and invalid operand shapes;
- session user, group membership, unknown groups, and identity lookup failure;
- common keyword boundaries, nested JSON escaping, keyword-named columns, and invalid identifiers;
- engine and language mismatch, unsupported adapters, and prohibited fallback;
- reserved function definitions, missing function versions, and invalid argument bindings;
- duplicate and conflicting row-filter definitions and mask actions;
- all nine mask actions and unsupported action/type pairs;
- table and column effective-tag selection;
- renamed, missing, required, and unsupported fields;
- subject, policy, tag, schema, and metadata cache changes;
- old-client rejection and experimental capability negotiation; and
- row filtering before masking in an end-to-end reader test.

## Implementation Plan

1. Add the common expression parser and typed row-filter and column-mask definition variants;
   reject unsupported engine and function variants explicitly.
2. Add an experimental Iceberg read-restriction model and exact wire serializer.
3. Implement policy-on-tag selection, subject binding, schema binding, canonicalization, and conflict
   detection.
4. Integrate restriction resolution with load-table responses and restriction-aware ETags.
5. Build the pinned experimental reader package and end-to-end conformance tests.
6. Replace experimental protocol and reader classes with official Iceberg types when available.

## References

- [Policy-on-tag design](policy-on-tag.md)
- [Apache Iceberg read-restrictions specification](https://github.com/apache/iceberg/pull/13879)
- [Pinned Iceberg REST schema](https://github.com/apache/iceberg/blob/6dec25e430b33a8b4f623b14940110d459581826/open-api/rest-catalog-open-api.yaml)
- [Iceberg read-restriction actions implementation](https://github.com/apache/iceberg/pull/16198)
- [Iceberg generic reader implementation](https://github.com/apache/iceberg/pull/16131)
- [Databricks ABAC core concepts](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/core-concepts)
- [Databricks ABAC policy management](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/policies)
