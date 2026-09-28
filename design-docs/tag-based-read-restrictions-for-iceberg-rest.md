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
provides a standard enforcement boundary without putting engine-specific expressions in Gravitino
policy content.

This design adds tag-based row-filter and column-mask policies and resolves them into Iceberg REST
`read-restrictions`. The first implementation is experimental while Iceberg reader support is
maturing. It is disabled by default and requires an explicit client opt-in.

## Goals

1. Define typed row-filter and column-mask policy content.
2. Select policies through the policy-on-tag model and effective tags.
3. Support a small, deterministic authoring language for row predicates and rule conditions.
4. Bind authored expressions to the authenticated subject and an Iceberg table schema.
5. Return only closed, typed Iceberg expressions and standard Iceberg mask actions.
6. Fail closed when an applicable restriction cannot be resolved or enforced.
7. Provide an experimental end-to-end path that can later move to official Iceberg runtime types
   without changing policy content.

## Non-Goals

1. Replacing table-level authorization or granting access through a read-restriction policy.
2. Supporting arbitrary expressions, user-defined functions, subqueries, or engine-specific syntax.
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

Policy content stores authored source and logical mask action names. It does not store a resolved
subject, group membership snapshot, table schema, field ID, or serialized load-table response.

### Row-filter content

A row-filter policy contains a non-empty ordered list of rules. Each rule has a required
`expression` and an optional context-only `when` condition.

```json
{
  "name": "restrict_orders",
  "comment": "Auditors see US orders; other users see their own orders",
  "policyType": "system_row_filter",
  "enabled": false,
  "content": {
    "rules": [
      {
        "when": "is_group_member(\"auditors\")",
        "expression": "col(\"region\") == \"US\""
      },
      {
        "expression": "col(\"owner\") == session_user()"
      }
    ]
  }
}
```

Conditions are evaluated in authored order against one identity snapshot. The first matching rule
selects its expression and later rules are not evaluated. A rule without `when` is unconditional.
If no rule matches in a selected policy, the policy contributes a constant-false predicate and
denies every row.

### Column-mask content

A column-mask policy contains a non-empty list of rules. Each rule has a required `action` and an
optional context-only `when` condition.

```json
{
  "name": "mask_phone_number",
  "comment": "Auditors see the final four characters",
  "policyType": "system_column_mask",
  "enabled": false,
  "content": {
    "rules": [
      {
        "when": "is_group_member(\"auditors\")",
        "action": "show-last-4"
      },
      {
        "when": "not is_group_member(\"auditors\")",
        "action": "replace-with-null"
      }
    ]
  }
}
```

Every mask condition is evaluated against the same identity snapshot. False conditions are omitted.
Equal active actions are deduplicated. More than one distinct active action for one field is a
conflict. A policy with no active mask rule contributes no mask.

Subject-dependent behavior is expressed through `when`.

## Expression Profile

The `gravitino-filter-v1` profile is the authoring language for row-filter expressions and `when`
conditions. A `when` condition uses the same grammar but cannot contain `col(...)`.

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
3. Select row-filter rules in authored order and evaluate every mask condition.
4. Bind the selected row expression and active masks to the table schema and Iceberg field IDs.
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

Policy revisions store the authored rule list, enabled state, and normal policy audit information.
Resolved field IDs, identity values, and load-table responses are request-scoped and are not stored
as policy content.

Administrators should create a policy disabled, associate it with a tag, preview representative
subjects and tables, verify that the experimental reader is deployed, and then enable it. Enabling a
policy does not make an incompatible reader safe.

Explain output should include selected policies, matching tags, selected rules, canonical
signatures, omission reasons, and conflicts. Query users receive a stable error code and request ID;
policy details remain subject to policy-view authorization.

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
- first-match row-filter rules and unmatched deny-all behavior;
- inactive, duplicate, and conflicting mask rules;
- all nine mask actions and unsupported action/type pairs;
- table and column effective-tag selection;
- renamed, missing, required, and unsupported fields;
- subject, policy, tag, schema, and metadata cache changes;
- old-client rejection and experimental capability negotiation; and
- row filtering before masking in an end-to-end reader test.

## Implementation Plan

1. Add the expression parser and typed row-filter and column-mask policy content.
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
