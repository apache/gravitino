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
| `system_row_filter`                  | Row-filter expression                      | Not enforced yet          |
| `system_column_mask`                 | Column-mask expression                     | Not enforced yet          |
| `system_access_control`              | Privileges and applicable roles            | Not enforced yet          |
| `custom`                             | A free-form map that you define            | A system that you provide |

The row-filter, column-mask, and access-control policy types can be created and associated with
tags, but Gravitino does not enforce them yet. Until enforcement is available, these policy types
do not restrict data access.

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

The server validates complete definitions on policy creation and content updates. It does not
reparse content on reads, renames, or comment updates. The SQL parser rejects the previous
`filter :=` and `mask :=` syntax.
This validation does not enable enforcement. Schema binding, trusted context lookup, target
nullability checks, expression evaluation, and Iceberg translation require the enforcement path.

A row filter declares zero or more parameters and `RETURNS BOOLEAN`. Parameters bind to table
columns by name and must match their logical types. A mask declares exactly one parameter and
returns that exact type. Its parameter name is local: `(xxx STRING) RETURNS STRING -> show_last_4(xxx)`
can apply to a column named `phone_number`. No physical column alias or implicit `value` is injected.
Masks can return the input, a literal, NULL, a scalar function result, or CASE branches.

When the experimental Iceberg REST read-restriction loader resolves several row-filter policies
for one table, a filter that resolves to `true` adds no restriction and is ignored. Identical
effective filters are deduplicated. Distinct restrictive filters conflict and cause the load-table
request to fail; the loader does not combine them.

If the selected column-mask action is not applicable to a column type, the experimental loader
fails the load-table request rather than omitting the mask and exposing the original values.
Unselected actions do not need to be applicable to that column. For example, `replace-with-null`
cannot apply to a required column when that action is selected.

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

Conditional branches are evaluated from left to right. The first true condition selects its result,
and every conditional rule must end with an unconditional `else`. Row-filter conditions and results
must be Boolean. Column-mask conditions may use only request context; they cannot reference columns.
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

Context functions are `CURRENT_USER()`, `is_group_member('group')`, and `user_attribute('key')`.
Group and attribute names must be non-empty string literals. Context values come from the trusted
effective subject. Attribute types and key availability must be verified during context binding;
missing values of declared attributes are SQL NULL. Unknown keys or lookup failures are errors.
`is_nan(value)` accepts FLOAT or DOUBLE; `year(value)`, `month(value)`, and `day(value)` accept DATE
or supported TIMESTAMP values and return INTEGER.

The type-preserving mask functions each take one scalar argument:

| Function | Input type |
| --- | --- |
| `mask_alphanum`, `show_first_4`, `show_last_4` | STRING |
| `mask_to_fixed_value` | STRING, numeric, BOOLEAN, DATE or TIMESTAMP |
| `replace_with_null` | Any typed scalar |
| `truncate_to_year`, `truncate_to_month` | DATE or TIMESTAMP |
| `sha256_global`, `sha256_query_local` | STRING, signed INTEGER, signed LONG or BINARY |

Keywords and built-in function names are case-insensitive. Unquoted identifiers use ASCII
letters or underscore followed by letters, digits or underscores, and normalize to uppercase.
Double-quoted identifiers preserve case; double a quote to escape it. Parameter names must be
unique after normalization. String literals use single quotes, with doubled single quotes for
escaping. Backslashes are literal and Unicode is preserved; isolated surrogates and NUL are rejected.
Comments, statements, semicolons, casts, arithmetic, qualified names, nested fields, subqueries,
unknown functions, exponents, leading `+`, leading zeroes and trailing decimal points are rejected.

Limits apply to source before normalization: 16 KiB UTF-8 per complete definition, operation
depth 8 (including CASE and calls), 256 expression nodes, 4 KiB per decoded quoted token, 256
bytes per numeric literal, 256 total IN elements, and 256 parenthesis levels. The node limit
also bounds how many literal list elements can fit in a complete expression.

The complete grammar and execution semantics are in the
[read-restriction design](../design-docs/tag-based-read-restrictions-for-iceberg-rest.md).

### Policy-to-Tag Associations

Each association connects one policy to one tag and stores a selector. The selector determines
whether that association contributes the policy to an object's lookup result.

| Selector     | When it matches                                                          |
|--------------|--------------------------------------------------------------------------|
| `ALL_VALUES` | The effective tag is present, including an assignment without a value.   |
| `TAG_VALUE`  | One of the effective tag assignment values equals the specified value.   |

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
