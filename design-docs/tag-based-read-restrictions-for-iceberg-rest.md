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

| Policy type | Evaluation target | Effect |
| --- | --- | --- |
| `system_row_filter` | Table | Retains only rows matching one resolved predicate. |
| `system_column_mask` | Top-level column | Replaces visible values using one Iceberg mask action. |

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

```text
filter := col("region") == "US"
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
inside one restricted Rego expression. This follows the Databricks ABAC pattern in which a single
row-filter UDF can use conditional logic to return its Boolean result, while policy applicability
and function input binding remain separate. A future principal-aware selector belongs to the
selection model, not inside a restriction definition.

### Row-filter content

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

### Column-mask content

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
one selected policy resolves to a different mask for the same field, resolution fails as a
conflict.

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

## Restricted Rego Expressions

Both built-in policy types use the restricted Rego subset defined below. Its version is part of the
policy content schema rather than a field repeated in every policy. The subset supports only one
complete rule named `filter` or `mask`; it is not an arbitrary Rego module. A row-filter policy
requires `filter`, whose result and conditions must be Boolean. A column-mask policy requires
`mask`, whose result must be an explicit action value and whose condition must be Boolean and
context-only.

The grammar is:

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

### Keywords, identifiers, and escaping

The restricted syntax reserves the lowercase keywords `filter`, `mask`, `if`, `else`, `and`, `or`,
`not`, `in`, `true`, `false`, and `null`. `:=` is the rule-result assignment operator; `then` is not
a literal token in Rego syntax because the result precedes `if`. The reserved built-in function
identifiers are `col`, `session_user`, and `is_group_member`. They are case-sensitive and are
recognized only as complete tokens. `action` is the reserved mask-action constructor. For example,
`notebook` is not `not` followed by an identifier. Bare identifiers are not part of the restricted
subset, so an unknown word is always invalid rather than an implicit column reference or function
call.

`filter` and `mask` are Gravitino restricted-syntax keywords, not standard Rego keywords. `filter`
is valid only as the row-filter rule head, and `mask` is valid only as the column-mask rule head.

Column names, group names, and string values appear only as JSON string literals. A name equal to a
keyword needs no special keyword escape: `col("and")` references the column named `and`. Backticks,
single quotes, SQL delimited identifiers, and backslash escaping outside a JSON string are invalid.

For example, this expression is valid and unambiguous:

```text
filter := col("filter") == "mask"
```

The first `filter` is the rule head, `"filter"` is a column name, and `"mask"` is a string value.
Similarly, `col("action")` references a column named `action`; it is unrelated to the
`action(...)` mask constructor. A group name can also equal a keyword:

```text
mask := action("show-last-4") if is_group_member("mask")
else := action("replace-with-null")
```

User-provided names cannot appear as bare identifiers. The following forms are invalid:

```text
filter := region == "US"
filter := col(mask) == "US"
filter := filter == true
"filter" := col("region") == "US"
```

Keywords, numbers, and built-in function names must end at the end of input or before a valid token
delimiter. The lexer never splits an alphanumeric or underscore sequence into adjacent tokens. It
therefore rejects prefix forms such as `notcol("active")`, `notebook`, `truefalse`, and `1and`
rather than interpreting them as combinations of valid tokens.

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

The expression subset provides two request-stable context functions:

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
pinned Iceberg specification. Unknown actions and unsupported action/type pairs fail closed.
`replace-with-null` is invalid for a required field.

`action("name")` constructs a typed mask action during parsing. It is not a runtime UDF and does not
convert a string result into an action. A bare string, unknown action name, or action with an
unsupported input type is invalid.

The server returns the logical Iceberg action, and the reader owns execution. For
`sha-256-query-local`, the reader also owns generation and lifecycle of the per-query salt defined
by Iceberg.

Only top-level fields are supported in the first version. A row filter evaluates original values,
and masks apply afterward to surviving rows.

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
rewrites null comparisons, folds Boolean constants, sorts and deduplicates commutative children and
`in` values, and validates the final closed Iceberg expression. Canonicalization is deterministic
and idempotent.

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

| Active restriction | `read-restrictions.v1` | Trusted reader | Result |
| --- | --- | --- | --- |
| No | Missing or present | Any | Return the normal response without read restrictions. |
| Yes | Missing | Any | Return `406 Not Acceptable`; do not return unrestricted metadata. |
| Yes | Unsupported version | Any | Return `406 Not Acceptable`; do not return unrestricted metadata. |
| Yes | Present | No | Return `403 Forbidden`; the header does not establish trust. |
| Yes | Present | Yes | Return `200 OK` with the resolved read restrictions. |

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
2. Only the authenticated end user is bound to `session_user()`.
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
- unconditional filters, context and row-dependent conditions, multi-branch `else` chains, missing
  final fallbacks, and deterministic conditional lowering;
- unconditional masks, context-dependent mask branches, row-dependent mask-condition rejection,
  bare-string results, invalid action names, and deterministic action selection;
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

1. Add the restricted Rego parser, conditional lowering, and typed row-filter and column-mask
   content; reject unsupported function content explicitly.
2. Add a Gravitino-owned Iceberg read-restriction model and exact wire serializer.
3. Implement policy-on-tag selection, subject binding, schema binding, canonicalization, and conflict
   detection.
4. Integrate restriction resolution with load-table responses and restriction-aware ETags.
5. Add the false-by-default server setting, trusted-reader binding, and versioned
   `X-Gravitino-Client-Capabilities` handling.
6. Build the pinned compatible reader and add end-to-end conformance tests.
7. Replace compatibility protocol and reader classes with official Iceberg types when available.

## References

- [Policy-on-tag design](policy-on-tag.md)
- [Apache Iceberg read-restrictions specification](https://github.com/apache/iceberg/pull/13879)
- [Iceberg client-capabilities proposal](https://github.com/apache/iceberg/pull/16394)
- [Pinned Iceberg REST schema](https://github.com/apache/iceberg/blob/6dec25e430b33a8b4f623b14940110d459581826/open-api/rest-catalog-open-api.yaml)
- [Iceberg read-restriction actions implementation](https://github.com/apache/iceberg/pull/16198)
- [Iceberg generic reader implementation](https://github.com/apache/iceberg/pull/16131)
- [Databricks ABAC core concepts](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/core-concepts)
- [Databricks ABAC policy management](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/policies)
- [Databricks row-filter performance and conditional UDF examples](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/performance)
- [Open Policy Agent Rego policy language](https://www.openpolicyagent.org/docs/policy-language)
