---
title: "Manage Policies"
slug: "/manage-policies-in-gravitino"
keyword: "policy management, policy, policies, Gravitino, data governance"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

This page covers the Gravitino API for policies and policy-to-tag associations. For the policy
model and how object policy lookup works, see
[Policies](./policies.md). To move from direct object policy associations, follow
[Migration Guide](./migration-guide.md).

The Python client does not cover policies, so the examples below are REST and Java only.

## Policy Operations

### Create a Policy

A policy needs a name and a type. Content carries the rules, the object types the policy supports,
and optional properties. For a built-in policy, `supportedObjectTypes` cannot be changed after
creation. Updating custom policy content replaces the whole content and can change this field.
Object policy lookup does not filter by this field; consumers decide which policy types they can
use.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "retention_30d",
  "comment": "Thirty day retention",
  "policyType": "custom",
  "enabled": true,
  "content": {
    "customRules": {"retentionDays": 30},
    "supportedObjectTypes": ["CATALOG", "SCHEMA", "TABLE"],
    "properties": {"owner": "platform"}
  }
}' http://localhost:8090/api/metalakes/test/policies
```

</TabItem>
<TabItem value="java" label="Java">

```java
PolicyContent content = PolicyContents.custom(
    ImmutableMap.of("retentionDays", 30),
    ImmutableSet.of(
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.SCHEMA,
        MetadataObject.Type.TABLE),
    ImmutableMap.of("owner", "platform"));

Policy policy = client.createPolicy(
    "retention_30d", "custom", "Thirty day retention", true, content);
```

</TabItem>
</Tabs>

The built-in compaction policy has a fixed content shape, documented in
[Iceberg compaction policy](./iceberg-compaction-policy.md), and a helper that builds it with
defaults.

```java
Policy policy = client.createPolicy(
    "nightly_compaction",
    "system_iceberg_compaction",
    "Compaction defaults",
    true,
    PolicyContents.icebergDataCompaction());
```

### List Policies

Listing returns names, or full policy objects when `details=true` is set.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/test/policies

curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  "http://localhost:8090/api/metalakes/test/policies?details=true"
```

</TabItem>
<TabItem value="java" label="Java">

```java
String[] policyNames = client.listPolicies();
Policy[] policies = client.listPolicyInfos();
```

</TabItem>
</Tabs>

### Get a Policy

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/test/policies/retention_30d
```

</TabItem>
<TabItem value="java" label="Java">

```java
Policy policy = client.getPolicy("retention_30d");
```

</TabItem>
</Tabs>

### Alter a Policy

Changes are applied as a list in one request.

| Change             | JSON                                                                 | Java                                               |
|--------------------|----------------------------------------------------------------------|----------------------------------------------------|
| Rename             | `{"@type":"rename","newName":"policy_renamed"}`                      | `PolicyChange.rename("policy_renamed")`            |
| Update the comment | `{"@type":"updateComment","newComment":"new_comment"}`               | `PolicyChange.updateComment("new_comment")`        |
| Update the content | `{"@type":"updateContent","policyType":"custom","newContent":{...}}` | `PolicyChange.updateContent("custom", newContent)` |

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "updates": [
    {
      "@type": "updateContent",
      "policyType": "custom",
      "newContent": {
        "customRules": {"retentionDays": 90},
        "supportedObjectTypes": ["CATALOG", "SCHEMA", "TABLE"],
        "properties": {"owner": "platform"}
      }
    }
  ]
}' http://localhost:8090/api/metalakes/test/policies/retention_30d
```

</TabItem>
<TabItem value="java" label="Java">

```java
PolicyContent newContent = PolicyContents.custom(
    ImmutableMap.of("retentionDays", 90),
    ImmutableSet.of(
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.SCHEMA,
        MetadataObject.Type.TABLE),
    ImmutableMap.of("owner", "platform"));

Policy policy = client.alterPolicy(
    "retention_30d", PolicyChange.updateContent("custom", newContent));
```

</TabItem>
</Tabs>

### Enable or Disable a Policy

Disabling a policy keeps its tag associations, but removes it from object policy lookup results.
It remains available through policy and association listing APIs.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X PATCH -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{"enable": false}' \
  http://localhost:8090/api/metalakes/test/policies/retention_30d
```

</TabItem>
<TabItem value="java" label="Java">

```java
client.disablePolicy("retention_30d");
client.enablePolicy("retention_30d");
```

</TabItem>
</Tabs>

### Delete a Policy

Deleting a policy also removes its associations with tags.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/test/policies/retention_30d
```

</TabItem>
<TabItem value="java" label="Java">

```java
client.deletePolicy("retention_30d");
```

</TabItem>
</Tabs>

## Policy-to-Tag Associations

Create a policy and a tag in the same metalake before associating them. Each policy-to-tag
association has a selector:

| Selector     | Match condition                                                        |
|--------------|------------------------------------------------------------------------|
| `ALL_VALUES` | The effective tag is present, with or without assignment values.       |
| `TAG_VALUE`  | One effective tag assignment value equals the specified value.         |

The selector belongs to the association, not to the policy or the tag. An existing association
cannot be replaced by another add request. Remove it and add it again to change its selector.

### Associate a Policy with a Tag

This example applies `retention_30d` when the effective `data_domain` tag has the value `finance`.
Create the tag first if it does not exist; see
[Manage tags](./manage-tags-in-gravitino.md#create-a-tag-with-a-value-constraint).

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{"selector": {"type": "TAG_VALUE", "value": "finance"}}' \
  http://localhost:8090/api/metalakes/test/tags/data_domain/policies/retention_30d
```

Use `{"selector": {"type": "ALL_VALUES"}}` to match any assignment of `data_domain`.

</TabItem>
<TabItem value="java" label="Java">

```java
PolicyTagAssociation association = client.addPolicyForTag(
    "data_domain", "retention_30d", TagValueSelector.of("finance"));

// Match any assignment of data_domain instead:
// client.addPolicyForTag("data_domain", "retention_30d");
```

</TabItem>
</Tabs>

The request fails with a conflict if the policy is already associated with the tag.

### List Associations

List associations from either side. By default the response contains names. Set `details=true`
to get policy or tag details together with each association's selector. These lists show direct
associations even when their selectors do not match any object's current tag values.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  "http://localhost:8090/api/metalakes/test/tags/data_domain/policies?details=true"

curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  "http://localhost:8090/api/metalakes/test/policies/retention_30d/tags?details=true"
```

</TabItem>
<TabItem value="java" label="Java">

```java
PolicyTagAssociation[] policies = client.listPolicyAssociationsForTag("data_domain");
PolicyTagAssociation[] tags = client.listTagAssociationsForPolicy("retention_30d");
```

</TabItem>
</Tabs>

### Remove an Association

Removing the association stops this policy from being selected through the tag. It leaves the
policy, tag, and tag assignments in place.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/test/tags/data_domain/policies/retention_30d
```

</TabItem>
<TabItem value="java" label="Java">

```java
client.removePolicyFromTag("data_domain", "retention_30d");
```

</TabItem>
</Tabs>

## Object Operations

Object policies are read-only results derived from effective tags. To change the policies that apply
to an object, associate a policy with a tag and then assign or remove that tag on the object or one
of its ancestors. See [Manage tags in Gravitino](./manage-tags-in-gravitino.md) for tag assignment
operations.

For the `TAG_VALUE("finance")` association above, assign `data_domain=finance` to a table or one
of its ancestors. The nearest assignment of `data_domain` overrides farther assignments of the
same tag. For example, a schema assignment of `data_domain=risk` overrides
`data_domain=finance` on its catalog for every table in the schema. A direct table assignment
overrides both.

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v2+json" \
  -H "Content-Type: application/vnd.gravitino.v2+json" \
  -d '{"tagsToAdd": [{"name": "data_domain", "value": "finance"}]}' \
  http://localhost:8090/api/metalakes/test/objects/table/catalog1.schema1.customers/tags
```

### List Policies on an Object

The response includes policies derived from effective tags assigned to the object or its ancestors.
With `details=true`, the response returns full policy objects instead of policy names.
Each policy includes an `inherited` field, which is `true` when it matches only through a tag
assigned to an ancestor of the object. Disabled policies do not appear in this result.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  "http://localhost:8090/api/metalakes/test/objects/table/catalog1.schema1.customers/policies?details=true"
```

</TabItem>
<TabItem value="java" label="Java">

```java
Table customers =
    client.loadCatalog("catalog1")
        .asTableCatalog()
        .loadTable(NameIdentifier.of("schema1", "customers"));
String[] policyNames = customers.supportsPolicies().listPolicies();
Policy[] policies = customers.supportsPolicies().listPolicyInfos();
```

</TabItem>
</Tabs>
