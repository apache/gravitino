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
   Choose `ALL_VALUES` to match tag presence or `TAG_VALUE` to match one exact assignment value.
3. [Assign the tag](./manage-tags-in-gravitino.md#object-operations) to an object or its ancestor.
4. [List the object's policies](./manage-policies-in-gravitino.md#list-policies-on-an-object) to
   confirm the result.

For example, associate `retention_30d` with `data_domain` using
`TAG_VALUE("finance")`. A table with an effective `data_domain=finance` assignment receives the
policy. A table with only `data_domain=risk` does not.

## The Policy Model

### Policy Types and Content

| Type | Rules | Consumer |
|------|-------|----------|
| `system_iceberg_compaction` | Compaction thresholds and scheduling | Table maintenance service |
| `custom` | A free-form map that you define | A system that you provide |

A custom policy's rules live in `customRules`. Gravitino stores them and returns them to clients;
it does not interpret their names or values. Built-in types have a defined content shape. See
[Iceberg compaction policy](./iceberg-compaction-policy.md) for the compaction rules and
[Table maintenance service](./table-maintenance-service/optimizer.md) for a worked example.

Policy content also has `properties` and `supportedObjectTypes`. Properties describe the policy
itself, such as its owner or consumer. `supportedObjectTypes` is required when creating a custom
policy and cannot be changed later. Object policy lookup does not filter by this field; each
consumer decides whether a policy type applies to the object it is processing.

### Policy-to-Tag Associations

Each association connects one policy to one tag and stores a selector. The selector determines
whether that association contributes the policy to an object's lookup result.

| Selector | When it matches |
|----------|-----------------|
| `ALL_VALUES` | The effective tag is present, including an assignment without a value. |
| `TAG_VALUE` | The effective tag has the specified exact assignment value. |

A policy may be associated with multiple tags. Association listings show those direct relations
and their selectors, even if no object currently matches them. An object policy lookup returns each
matching policy once.

A selector cannot be changed in place. Remove the policy-to-tag association and add it again with
the new selector. Removing an association leaves the policy, tag, and tag assignments intact.
Deleting a policy removes its associations.

### Effective Tags and Inheritance

An object receives tags assigned directly to it and tags inherited from its metadata object
ancestors. When both the object and an ancestor assign the same tag name, the object's direct
assignment wins, including its assignment values. Tag names themselves are flat; tags do not
inherit from other tags.

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
