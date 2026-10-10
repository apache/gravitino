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

| Type                                 | Rules                                | Consumer                  |
|--------------------------------------|--------------------------------------|---------------------------|
| `system_iceberg_compaction`          | Compaction thresholds and scheduling | Table maintenance service |
| `system_iceberg_orphan_file_removal` | Orphan-file cleanup options          | Table maintenance service |
| `system_row_filter`                  | Row-filter expression                | Not enforced yet          |
| `system_column_mask`                 | Column-mask expression               | Not enforced yet          |
| `custom`                             | A free-form map that you define      | A system that you provide |

The row-filter and column-mask policy types can be created and associated with tags, but Gravitino
does not enforce them yet. Until enforcement is available, these policy types do not restrict data
access.

### Read-Restriction Expressions

Row-filter and column-mask expressions use `restricted-rego-v1`, a small, fail-closed Rego-like
syntax owned by Gravitino. Each expression is one complete rule, not an arbitrary Rego module. A
row filter starts with `filter :=` and produces a Boolean predicate. A column mask starts with
`mask := action("...")` and produces an allowlisted masking action.

For example, a row-filter program can select the first matching predicate:

```restricted-rego-v1
filter := col("region") == "US" if is_group_member("auditors")
else := col("owner") == session_user()
```

This column-mask program selects a masking action in the same way:

```restricted-rego-v1
mask := action("show-last-4") if is_group_member("support")
else := action("replace-with-null")
```

Conditional branches are evaluated from left to right. The first true condition selects its result,
and every conditional rule must end with an unconditional `else`. Row-filter conditions and results
must be Boolean. Column-mask conditions may use only request context; they cannot reference columns.

The syntax supports these constructs:

- `col("name")`, `session_user()`, and `is_group_member("group")`;
- JSON string literals, exact decimal numbers, `true`, `false`, `null`, and homogeneous non-null
  literal arrays;
- comparisons `==`, `!=`, `<`, `<=`, `>`, `>=`, and `in`; and
- Boolean operators `and`, `or`, and `not`.

Keywords and built-in function names are case-sensitive. For example, only lowercase `in` is an
operator; `In`, `IN`, and `iN` are invalid.

Supported operand shapes are:

| Form | Operators | Requirements |
| --- | --- | --- |
| Boolean predicates | `and`, `or`, `not` | Every operand must be Boolean. |
| Column and non-null literal | `==`, `!=`, `<`, `<=`, `>`, `>=` | Either operand order; types must be compatible. Boolean literals support only `==` and `!=`. |
| String column and `session_user()` | `==`, `!=` | Either operand order. |
| `session_user()` and string literal | `==`, `!=` | Either operand order. |
| Column and `null` | `==`, `!=` | Either operand order. |
| Column and literal array | `in` | Column on the left; array must be non-empty and homogeneous. |
| `session_user()` and string array | `in` | `session_user()` on the left; array must be non-empty. |
| `is_group_member("group")` | none | Produces a Boolean request-context predicate. |

Column-to-column and literal-to-literal comparisons, ordering on `session_user()`, null array
elements, nested arrays, and comparisons on `is_group_member(...)` are invalid.

Column-mask actions are `mask-alphanum`, `mask-to-fixed-value`, `replace-with-null`, `show-first-4`,
`show-last-4`, `truncate-to-year`, `truncate-to-month`, `sha-256-global`, and
`sha-256-query-local`.

Validation applies these limits when a policy is created or its content is updated:

- 16 KiB of UTF-8 source;
- operation depth 8 for the source and lowered row-filter predicate;
- 256 nodes for the source AST and lowered row-filter predicate;
- 4 KiB of UTF-8 for each decoded string literal; and
- 256 bytes for each numeric literal;
- 256 elements in a literal array; and
- 256 array elements in total in the source and lowered row-filter predicate.

Bare identifiers, arbitrary functions, comments, packages, imports, variables, chained
comparisons, nested arrays, and a conditional rule without a final `else` are rejected. Read paths
do not re-parse stored policy expressions.

A custom policy's rules live in `customRules`. Gravitino stores them and returns them to clients;
it does not interpret their names or values. Built-in types have a defined content shape. See
[Iceberg compaction policy](./iceberg-compaction-policy.md) for the compaction rules and
[Table maintenance service](./table-maintenance-service/optimizer.md) for a worked example.

Policy content also has `properties` and `supportedObjectTypes`. Properties describe the policy
itself, such as its owner or consumer. `supportedObjectTypes` is required when creating a custom
policy. A custom policy content update replaces the whole content and can change this field; for a
built-in policy, the field cannot be changed after creation. Object policy lookup does not filter
by this field; each consumer decides whether a policy type applies to the object it is processing.

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
`MODEL`, `MODEL_VERSION`, and `FUNCTION`. A model version cannot carry a tag directly, but it can
inherit tags from its model and higher ancestors, so their policies appear in model version lookups.
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
