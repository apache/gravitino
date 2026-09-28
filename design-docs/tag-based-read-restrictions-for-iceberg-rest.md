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
reader applies a required row filter and required column projections before returning data. Without
a Gravitino policy model and resolver for this field, administrators cannot express portable
restrictions or safely deliver them to readers.

---

## Goals

1. **Typed policy content**: Define separate row-filter and column-mask policy types.
2. **Tag-based selection**: Select policies through the policy-on-tag model and effective tags.
3. **Deterministic authoring**: Provide a small expression syntax for row predicates and mask
   selection with one canonical result for equivalent input.
4. **Trusted binding**: Bind authored expressions to the authenticated subject and the loaded
   Iceberg table schema.
5. **Standard output**: Return only closed, typed Iceberg expressions and standard Iceberg mask
   actions.
6. **Fail-closed enforcement**: Reject a governed load when an applicable restriction cannot be
   resolved or enforced.
7. **Upgradable integration**: Allow the implementation to adopt official Iceberg runtime types
   without changing stored policy content.
8. **Reserved UDF model**: Define the compatibility boundary for future versioned function
   references without enabling them in the first version.

---

## Non-Goals

1. **Authorization replacement**: Read restrictions do not replace table-level authorization or
   grant access.
2. **Arbitrary execution**: The first Iceberg REST implementation does not execute arbitrary Rego,
   SQL, expressions, or UDFs.
3. **Extended attributes**: Nested-field masks, roles, identity attributes, nested groups, and
   general attribute expressions remain outside the first version.
4. **Direct policy association**: This design does not add a policy-to-metadata-object association
   model because policy-on-tag supplies policy selection.
5. **Legacy reader safety**: A reader without Iceberg read-restriction support is not a trusted
   enforcement target.
6. **Non-Iceberg adapters**: Trino OPA and other engine-specific renderers require separate designs
   and conformance contracts.

---

## Solution Investigations

| Approach | Benefits | Costs | Decision |
| --- | --- | --- | --- |
| Store arbitrary Rego, SQL, or UDF source | Maximum author flexibility | Couples policies to an execution engine, expands the attack surface, and cannot guarantee Iceberg-equivalent semantics | Rejected |
| Store structured `rules[].when` content | Simple DTO validation and explicit branch order | Makes control flow part of the policy schema and requires schema changes to extend the expression language | Rejected |
| Store one restricted Rego-compatible rule | Keeps one versioned source expression, supports familiar conditional branches, and compiles deterministically to Iceberg restrictions | Requires a dedicated parser and strict semantic validation | **Chosen** |
| Store versioned function references | Reuses governed functions and supports richer logic | Requires a function registry, execution authority, and an enforcement contract that the first Iceberg reader does not provide | Deferred |

The design uses the standard Iceberg REST restriction schema as its only first-version enforcement
format. Gravitino parses the restricted authoring syntax itself; it does not send the policy to OPA
or another Rego runtime. This preserves a path to other adapters without weakening the Iceberg
contract in this design.

---

## Proposal

### Architecture

This design adds tag-based row-filter and column-mask policies and resolves them into Iceberg REST
`read-restrictions`. The feature is disabled by default and requires both a trusted reader path and
an explicit client capability declaration while reader support is maturing.

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

### API and Policy Model

The design extends existing surfaces instead of adding a policy-resolution endpoint:

| Surface | Existing behavior | New behavior |
| --- | --- | --- |
| Policy create and update APIs | Accept registered built-in policy types and their typed content | Accept `system_row_filter` and `system_column_mask`, each with the content defined below |
| Policy-on-tag resolution | Selects enabled policies from effective tags | Supplies candidate restrictions for a table and its top-level columns |
| Iceberg REST `loadTable` | Returns table metadata and optional response fields | Adds the standard `read-restrictions` field after authorization and restriction resolution |

No new direct policy-association API is introduced. Policy create and update continue to use the
existing Gravitino policy APIs, including their normal audit and authorization checks.

This design introduces two built-in policy types:

| Policy type | Evaluation target | Effect |
| --- | --- | --- |
| `system_row_filter` | Table | Retains only rows matching one resolved predicate. |
| `system_column_mask` | Top-level column | Replaces visible values using one Iceberg mask action. |

Policies are associated with tags. The existing policy-on-tag resolver selects enabled policies
from effective tags. Row-filter resolution consumes effective policies for a table. Column-mask
resolution consumes effective policies for each top-level column. Direct policy
associations are not used.

First-version policy content stores one expression. It does not store parser names,
action-vocabulary names, resolved subjects, group membership snapshots, table schemas, field IDs,
or serialized load-table responses. The built-in policy type determines how the expression is
parsed.

Policy selection and policy effect are separate. Tags and policy-on-tag selectors decide whether a
policy is applicable; a row-filter or column-mask definition states what the selected policy does.
The effect content has no `rules` list and no `when` field. Conditional filter results are written
inside one restricted Rego expression. This follows the Databricks ABAC pattern in which a single
row-filter UDF can use conditional logic to return its Boolean result, while policy applicability
and function input binding remain separate. A future principal-aware selector belongs to the
selection model, not inside a restriction definition.

#### Row-filter content

The content of a row-filter policy contains exactly one `expression`.

```json
{
  "name": "restrict_orders",
  "comment": "Auditors see US orders; other users see their own orders",
  "policyType": "system_row_filter",
  "enabled": false,
  "content": {
    "expression": "filter := col(\"region\") == \"US\" if is_group_member(\"auditors\") else := col(\"owner\") == session_user()"
  }
}
```

The expression is one complete rule whose result is the row predicate. In the example, the Rego
assignment means “if the subject is an auditor, then use the region predicate; otherwise use the
owner predicate.” It does not select another policy rule. The expression may use trusted request
context and row values in either conditions or results. The first policy-on-tag selector version
does not select by principal, so subject-dependent filtering can remain inside this one expression.

#### Column-mask content

The content of a column-mask policy also contains exactly one `expression`. Its result is an
Iceberg mask action name.

```json
{
  "name": "mask_phone_number",
  "comment": "Auditors see the final four characters; other users see null",
  "policyType": "system_column_mask",
  "enabled": false,
  "content": {
    "expression": "mask := action(\"show-last-4\") if is_group_member(\"auditors\") else := action(\"replace-with-null\")"
  }
}
```

The rule result is an explicit `action("name")` value, and each condition must use the context-only
expression subset. A bare string is not a mask action and is invalid. A condition cannot contain
`col(...)` because an Iceberg projection selects one action for the complete column, not a different
action per row. Conditional results are resolved before the response is serialized. If more than
one distinct selected policy applies to the same field, resolution fails as a conflict. Matching
the same policy entity through more than one effective tag is not a conflict because the
policy-on-tag resolver deduplicates policy identity before restriction resolution.

#### Future function reference

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

### Restricted Rego Expressions

Both built-in policy types use the implicit `restricted-rego-v1` subset defined below. The profile
name is fixed by the first-version content schema rather than repeated in every policy. Its grammar,
binding, and evaluation semantics are immutable. A later profile must add an explicit schema
version, treat content without that field as v1, and retain the v1 parser for stored policies.

The subset supports only one complete rule named `filter` or `mask`; it is not an arbitrary Rego
module. A row-filter policy requires `filter`, whose result and conditions must be Boolean. A
column-mask policy requires `mask`, whose result must be an explicit action value and whose
condition must be Boolean and context-only.

#### Grammar

The implementation uses a Gravitino-owned ANTLR4 grammar and generated parser. It does not invoke
OPA's parser and does not accept a Rego module. The generated parse tree is translated immediately
to the restricted expression model and then to standard Iceberg expression nodes. The Iceberg JSON
expression grammar is an output format rather than an authoring grammar because it has no
conditional rule head, subject functions, or named column references.

The complete authoring grammar is:

```text
program     := filterRule | maskRule
filterRule  := unconditionalFilter | conditionalFilter
unconditionalFilter := "filter" ":=" expr
conditionalFilter := "filter" ":=" expr "if" expr filterElse* filterFallback
filterElse  := "else" ":=" expr "if" expr
filterFallback := "else" ":=" expr
maskRule    := unconditionalMask | conditionalMask
unconditionalMask := "mask" ":=" maskAction
conditionalMask := "mask" ":=" maskAction "if" contextExpr maskElse* maskFallback
maskElse    := "else" ":=" maskAction "if" contextExpr
maskFallback := "else" ":=" maskAction
maskAction  := "action" "(" string ")"
contextExpr := expr
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

`filter := value-a if condition else := value-b` and
`mask := action("action-a") if condition else := action("action-b")` have the semantic reading “if
condition, then value-a, otherwise value-b.” Conditional branches are evaluated from left to right
and the first true condition selects its value. A conditional rule requires an unconditional final
`else`, so a selected policy never becomes undefined. Unconditional forms omit `if` and `else`.

Strings use JSON double-quoted syntax. Packages, imports, additional rules, variables, rule bodies
in braces, comments, exponent notation, leading `+`, leading zeroes, trailing decimal points,
chained comparisons, and arbitrary functions are invalid. A row-filter root must be Boolean, and a
column-mask root must be an explicit `action(...)` value.

Operator precedence from highest to lowest is primary, comparison, `not`, `and`, and `or`.
Conditional `if` and `else` clauses are outside `expr` and bind to the complete rule. Therefore,
`not col("active") == true` means `not (col("active") == true)`.

#### Keywords, identifiers, and escaping

The restricted syntax reserves the lowercase keywords `filter`, `mask`, `if`, `else`, `and`, `or`,
`not`, `in`, `true`, `false`, and `null`. `:=` is the rule-result assignment operator; `then` is not
a literal token in Rego syntax because the result precedes `if`. The reserved built-in function
identifiers are `col`, `session_user`, and `is_group_member`. They are case-sensitive and are
recognized only as complete tokens. `action` is the reserved mask-action constructor. For example,
`notebook` is not `not` followed by an identifier. Bare identifiers are not part of the restricted
subset, so an unknown word is always invalid rather than an implicit column reference or function
call. Keywords, numbers, and function names must end at the end of input or before a valid token
delimiter. After optional whitespace, a function name must be followed by `(`. The lexer never
splits an alphanumeric or underscore sequence into adjacent tokens. The parser therefore rejects
prefix forms such as `1and`, `truefalse`, and `notcol("x")`.

`filter` and `mask` are Gravitino restricted-syntax keywords, not standard Rego keywords. `filter`
is valid only as the row-filter rule head, and `mask` is valid only as the column-mask rule head.

Column names, group names, and string values appear only as JSON string literals. A name equal to a
keyword needs no special keyword escape: `col("and")` references the column named `and`. Backticks,
single quotes, SQL delimited identifiers, and backslash escaping outside a JSON string are invalid.

There are two syntactic JSON layers in an API request. The HTTP JSON parser decodes the outer
`expression` field once, and the expression parser decodes each inner JSON string literal once. For
example, the request fragment
`"expression": "filter := col(\"and\") == \"open\""` becomes the source
`filter := col("and") == "open"`, whose decoded column name is `and`. No layer performs an
additional or implicit unescape.

After decoding, identifiers and values are preserved exactly. Gravitino performs no Unicode
normalization, case folding, whitespace trimming, environment expansion, URL decoding, or SQL
quoting. Adapters must bind typed AST nodes or parameters and must not concatenate decoded names or
values into rendered text. Canonical serialization applies JSON escaping; it does not change the
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

#### Limits

Save-time validation applies these limits to the decoded UTF-8 source and parsed source tree before
constant folding or canonicalization:

- source length: 16 KiB of UTF-8;
- operation depth: 8;
- AST nodes: 256;
- decoded string literal: 4 KiB of UTF-8; and
- array elements: 256.

An AST node is a rule branch, operator, call, reference, scalar literal, or array; every array
element counts as its scalar-literal node. Operation depth is the longest expression-node path from
a rule result or condition to a leaf; rule and branch containers do not add depth. The resolved
predicate also has maximum operation depth 8. Canonicalization cannot make an oversized expression
valid, even when folding would make it shallower.

### Context and Schema Binding

The expression subset provides two request-stable context functions:

| Function | Result | Binding |
| --- | --- | --- |
| `session_user()` | Non-null string | Authenticated effective user name. |
| `is_group_member("group")` | Non-null Boolean | Exact flat membership after verifying that the group exists in the current metalake. |

The resolver tracks group existence separately from membership. An unknown group, a backend that
cannot distinguish an unknown group from non-membership, a group lookup failure, a missing subject,
or an inconsistent identity snapshot is a resolution error. Only an existing group that does not
contain the subject returns `false`. Nested group expansion is not performed.

Adding a context function creates a new expression profile and requires versioned signatures,
capability checks, and conformance fixtures. Implementations must not silently accept a new
function under `restricted-rego-v1`.

Each column name is bound exactly once against the concrete table schema and converted to its stable
Iceberg field ID. Missing or ambiguous columns, incompatible types, schema drift, or unsupported
field-ID mapping fail closed.

All conditional results and conditions are parsed and type-checked before request binding. The
resolver then binds their context, columns, and literals and lowers an `if`/`else` chain to one
Boolean predicate. For example:

```text
filter := result1 if condition1
else := result2 if condition2
else := fallback
```

is lowered to:

```text
(condition1 and result1)
or (not condition1 and condition2 and result2)
or (not condition1 and not condition2 and fallback)
```

Request-context-only conditions are folded before the final predicate is built, but every branch
must still parse and type-check. For a column mask, all conditions are context-only: the resolver
evaluates them in order and selects the first action whose condition is true, or the final fallback
action. The conditional assignment, `if`, and `else` nodes never appear in the Iceberg wire
expression or projection. This preserves the first-matching-branch semantics of Rego's `else`
chain while producing the one closed Boolean predicate or one column action required by Iceberg.

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

Null tests may apply to any nullable top-level field with a stable field ID. No implicit conversion
is allowed, including numeric widening or narrowing, string-to-number conversion, temporal
conversion, collation changes, signedness changes, or timezone assumptions. A source literal is
assigned its expected type once from the comparison's column operand; this is literal typing, not a
conversion from a runtime string or numeric value. A literal that cannot represent that exact type
without reinterpretation, rounding, or loss is invalid.

Source numbers remain exact base-10 values until the comparison supplies a target type. Integral
bindings must be mathematical integers in range. Decimal bindings must fit the target precision and
scale without rounding. Float and double bindings preserve negative zero and reject overflow,
infinity, and NaN. Temporal strings use the Iceberg ISO-8601 representation and contain at most six
fractional-second digits for first-version time and timestamp values.

All context functions and named references are removed before a resolved predicate is serialized.
The Iceberg wire expression contains only Boolean constants, logical operators, supported
predicates, field-ID references, and typed literals.

### Null Semantics

Resolved predicates use Iceberg two-valued semantics. For a null field and a non-null literal:

| Predicate | Result |
| --- | --- |
| `is-null` | `true` |
| `not-null` | `false` |
| `eq`, `gt`, `gt-eq`, or `in` | `false` |
| `not-eq`, `lt`, or `lt-eq` | `true` |

Therefore, `col("region") != "US"` retains rows where `region` is null. Authors who want to
exclude nulls must add `col("region") != null`.

### Column Masks

The action vocabulary is defined by Iceberg read restrictions:

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
Iceberg specification at merge commit `6dec25e430b33a8b4f623b14940110d459581826`, which is pinned in
the references. Unknown actions and unsupported action/type pairs fail closed. `replace-with-null`
is invalid for a required field.

`action("name")` constructs a typed mask action during parsing. It is not a runtime UDF and does not
convert a string result into an action. A bare string, unknown action name, or action with an
unsupported input type is invalid.

The server returns the logical Iceberg action, and the reader owns execution. For
`sha-256-query-local`, the reader also owns generation and lifecycle of the per-query salt defined
by Iceberg.

Only top-level fields are supported in the first version. A row filter evaluates original values,
and masks apply afterward to surviving rows.

### Resolution and Conflicts

For one authenticated subject and table load:

1. Resolve effective tags for the table and all top-level columns.
2. Resolve enabled policy-on-tag matches and deduplicate multiple tag paths to the same policy
   entity.
3. Parse and type-check the expression required by each selected policy type.
4. Bind and resolve each row-filter expression and column-mask expression against the request
   context, table schema, and Iceberg field IDs.
5. Canonicalize resolved restrictions and compute signatures.
6. Retain policy and tag provenance for explain and audit output.
7. Reject more than one distinct row-filter policy entity for one table, even when two signatures
   are equal.
8. Reject more than one distinct column-mask policy entity for one field, even when two actions are
   equal.

A constant-true row filter is omitted. Constant false remains a deny-all filter. Any loading,
parsing, context, schema, type, action, or conflict error aborts the governed load.

#### Canonical form and signatures

Canonicalization is bottom-up and uses this order:

1. Bind context, field references, and literals, and rewrite null comparisons.
2. Canonicalize every child before its parent. Put the field reference first in comparisons and
   invert ordering operators when the operands are reversed.
3. Fold Boolean constants.
4. Flatten associative `and` or `or` nodes, sort their canonical children, remove duplicates, and
   rebuild a balanced binary tree. Recursively split `n` children after `floor(n / 2)`; the right
   side receives the extra child when `n` is odd.
5. Sort and deduplicate `in` values with Iceberg ordering for the bound type.
6. Validate the closed Iceberg profile and the resolved depth limit.

Canonical signature records use UTF-8 JSON without a byte-order mark or insignificant whitespace.
Object keys use the fixed order defined by the signature schema. Non-ASCII characters are emitted
without Unicode normalization; quotation marks, reverse solidus, and control characters use their
shortest JSON escapes. Version 1 uses these records and key orders:

| Record | Canonical key order |
| --- | --- |
| Row filter | `profile`, `predicate` |
| Column mask | `profile`, `field-id`, `action` |
| Logical predicate | `type`, `left`, `right` |
| Negation or unary predicate | `type`, `child` |
| Comparison predicate | `type`, `left`, `right` |
| Set predicate | `type`, `child`, `values` |
| Field reference | `type`, `id` |
| Typed literal | `type`, `value`, `data-type` |
| Typed literal set | `type`, `values`, `data-type` |

The `profile` value is `gravitino-read-restriction-v1`. Predicate, reference, literal, and action
values use the exact operator, type, and action names in the pinned Iceberg schema. Boolean
constants are bare JSON values. Typed literal records use these canonical values:

- Boolean and string values retain their exact logical value.
- Integer and long values use base-10 notation without a leading plus sign or leading zeroes; zero
  is `0`.
- A `DECIMAL(p,s)` value uses exactly `s` fractional digits and normalizes negative zero to positive
  zero. Therefore, `1`, `1.0`, and `1.00` bind to the same value for `DECIMAL(p,2)` and serialize as
  `1.00`.
- Float and double values use their fixed-width IEEE 754 bit pattern in unsigned hexadecimal, which
  preserves the distinction between positive and negative zero.
- Date, time, and timestamp values use their normalized Iceberg representation.

The same canonical bytes sort and deduplicate children, compare restriction equality, and feed the
SHA-256 signature. Repeating canonicalization produces the same tree and bytes. Wire serialization
remains the standard Iceberg JSON format and is not replaced by the internal signature record.

### Iceberg REST Response

When restrictions resolve successfully, the server adds the standard `read-restrictions` object to
the Iceberg load-table response. The response contains at most one required row filter and at most
one required projection per field ID.

The implementation may use Gravitino-owned DTOs and serializers, but its JSON must match the merged
Iceberg REST schema exactly and must not add classes under `org.apache.iceberg`.

Response reconstruction for credentials, snapshot filtering, federation, and other load-table
features must preserve read restrictions.

#### Caching

A load-table response can vary without a table metadata commit. Cache identity and ETags for a
governed response must include at least:

- the metalake, catalog, and table identity, catalog configuration revision, and table metadata
  representation;
- the authenticated subject, authentication scope, authorized delegation context, and identity
  revision;
- effective policy and tag revisions;
- schema revision;
- canonical read-restriction signature; and
- the read-restriction feature version and trusted-reader capability state.

The existing metadata-location-only conditional-GET fast path must not return `304 Not Modified`
before restriction resolution. A cache that cannot represent every key component must be disabled
for governed responses. A response resolved for one subject, authentication scope, resource, schema,
policy revision, or feature version must never be reused for another.

### Delivery and Trust Boundary

The implementation ships through the normal Gravitino Iceberg REST build and distribution.

The feature is disabled by default through a new Iceberg REST server setting. A compatible reader
sends `X-Gravitino-Read-Restrictions: v1` on `loadTable`; the Iceberg REST OpenAPI extension must
define this versioned request header. The server returns restrictions only when the feature is
enabled, the request arrives through an operator-configured trusted reader identity or channel, and
the header selects a supported version.

The header prevents accidental use by an incompatible client, but it is not authorization or proof
of enforcement. A caller can copy a header. The deployment trust policy must bind the authenticated
reader identity or mutually authenticated channel to a reviewed reader implementation, and must
carry the effective end user through an authorized delegation mechanism. The server must not vend
data credentials for a governed load when either the reader trust or end-user delegation check
fails.

When an active restriction applies:

- a client that has not opted in is rejected rather than given an unrestricted response;
- a missing, unsupported, or malformed restriction is rejected;
- the server never assumes that an unknown client enforces an unknown response field.

Compatibility tests pin the Iceberg implementation revision. When official Iceberg runtime support
is available, Gravitino replaces its compatibility DTOs and reader integration with official types
and runs the same conformance fixtures against both implementations.

### Persistence and Administration

Policy revisions store the single authored restriction definition, enabled state, and normal policy
audit information.

Resolved field IDs, identity values, and load-table responses are request-scoped and are not stored
as policy content.

Administrators create a policy disabled, associate it with a tag, preview representative subjects
and tables, verify that a compatible reader is deployed, and then enable it. Enabling a policy does
not make an incompatible reader safe.

Explain output includes selected policies, matching tags, selected conditional branches, canonical
signatures, omission reasons, and conflicts. Query users receive a stable error code and request ID;
policy details remain subject to policy-view authorization.

### User Process

1. An operator enables read restrictions, configures the trusted reader identity or channel, and
   deploys a reader that sends the versioned capability header and enforces the pinned Iceberg
   contract.
2. A governance administrator creates a disabled row-filter or column-mask policy through the
   existing policy API.
3. The administrator associates the policy with a tag and assigns that tag to the target table or
   top-level column.
4. The administrator previews representative subjects and tables, including the selected branch,
   bound field IDs, canonical signature, and reader capability result.
5. The administrator enables the policy after validation succeeds.
6. On `loadTable`, Gravitino authorizes the subject, resolves effective tags and policies, binds the
   restriction, and returns the standard `read-restrictions` field.
7. The trusted reader applies the row filter to original values and then applies masks to surviving
   rows. Any unsupported or malformed restriction aborts the read.

### Backward Compatibility

The policy types and optional Iceberg REST response field are additive. Existing policies and
ungoverned table loads keep their behavior while the feature remains disabled or no applicable
restriction exists. No stored-policy migration is required.

Enabling the feature intentionally changes governed loads: a client that does not declare and pass
the trusted-reader checks is rejected instead of receiving an unrestricted response. This is a
security boundary rather than a compatibility fallback. The temporary Gravitino-owned protocol
classes remain internal and are replaced by official Iceberg types without changing the wire format
or stored policy content.

### Failure and Security Requirements

1. Applicable restrictions fail closed on parse, lookup, binding, serialization, or enforcement
   capability errors.
2. Only the authenticated end user is bound to `session_user()`.
3. Policy enforcement uses the unfiltered internal policy result and is independent of whether the
   subject may view policy metadata.
4. Restriction errors and user-facing responses do not reveal policy, tag, or group names without
   the corresponding metadata privileges.
5. Unknown groups and identity lookup failures do not become non-membership.
6. An old client that may ignore `read-restrictions` is not a trusted enforcement target.
7. A `function` definition remains invalid until its complete contract and enforcement capability
   are enabled.
8. Governed data credentials and storage authorization must prevent reads that bypass the trusted
   restriction-enforcement path.

---

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
- keyword boundaries such as `1and`, `truefalse`, and `notcol("x")`, nested JSON escaping,
  keyword-named columns, and invalid identifiers;
- exact literal typing and rejection of every implicit-conversion path;
- unconditional filters, context and row-dependent conditions, multi-branch `else` chains, missing
  final fallbacks, and deterministic conditional lowering;
- unconditional masks, context-dependent mask branches, row-dependent mask-condition rejection,
  bare-string results, invalid action names, and deterministic action selection;
- reserved function definitions, missing function versions, exact argument and result types, and
  invalid argument bindings;
- duplicate tag paths to one policy and conflicts between distinct policy entities, including
  entities with equal resolved signatures or actions;
- all nine mask actions and unsupported action/type pairs;
- table and column effective-tag selection;
- renamed, missing, required, and unsupported fields;
- subject, authentication scope, resource, policy, tag, schema, feature-version, and metadata cache
  changes;
- old-client rejection, spoofed-header rejection, trusted-reader binding, and capability
  negotiation; and
- row filtering before masking in an end-to-end reader test.

---

## Task Breakdown

- [ ] Add the ANTLR4 grammar, generated parser, source limits, and parser conformance tests.
- [ ] Add typed `system_row_filter` and `system_column_mask` policy content and DTO support.
- [ ] Reject the reserved `function` form until its separate execution contract is implemented.
- [ ] Add a Gravitino-owned Iceberg read-restriction model and exact wire serializer.
- [ ] Implement context and schema binding, conditional lowering, canonicalization, and signatures.
- [ ] Integrate effective-tag policy selection and shared fail-closed conflict detection.
- [ ] Add restriction resolution to `loadTable`, including response reconstruction paths and
      restriction-aware ETags.
- [ ] Add the false-by-default server setting, trusted-reader binding, and versioned capability
      header to the Iceberg REST OpenAPI extension.
- [ ] Build the pinned compatible reader and add unit, conformance, and end-to-end enforcement tests.
- [ ] Document administration, deployment, preview, metrics, and error-code behavior.
- [ ] Replace compatibility protocol and reader classes with official Iceberg types when available.

---

## References

- [Policy-on-tag design](policy-on-tag.md)
- [Apache Iceberg read-restrictions specification](https://github.com/apache/iceberg/pull/13879)
- [Pinned Iceberg REST schema](https://github.com/apache/iceberg/blob/6dec25e430b33a8b4f623b14940110d459581826/open-api/rest-catalog-open-api.yaml)
- [Iceberg read-restriction actions implementation](https://github.com/apache/iceberg/pull/16198)
- [Iceberg generic reader implementation](https://github.com/apache/iceberg/pull/16131)
- [Databricks ABAC core concepts](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/core-concepts)
- [Databricks ABAC policy management](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/policies)
- [Databricks row-filter performance and conditional UDF examples](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/performance)
- [Open Policy Agent Rego policy language](https://www.openpolicyagent.org/docs/policy-language)
