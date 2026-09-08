---
title: "Manage Policies"
slug: "/manage-policies-in-gravitino"
keyword: "policy management, policy, policies, Gravitino, data governance"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

This page covers the Gravitino API for policies. For what a policy is, which object types can carry one, what goes in
policy content, how inheritance resolves, and how to work with policies in the UI, see
[Policies](./policies.md).

The Python client does not cover policies, so the examples below are REST and Java only.

## Policy Operations

### Create a Policy

A policy needs a name and a type. Content carries the rules, the object types the policy supports,
and optional properties. `supportedObjectTypes` cannot be changed after creation.

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

The flag is a marker for readers. Gravitino does not act on it, and disabling a policy neither
detaches it nor changes what a consumer receives.

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

Deleting a policy also removes it from every object it was attached to.

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

## Object Operations

### Attach and Detach Policies

Both happen in one request, and either list can be omitted. Catalogs, schemas, tables, filesets,
topics, models, views, and functions can carry a policy.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "policiesToAdd": ["retention_30d"],
  "policiesToRemove": ["retention_7d"]
}' http://localhost:8090/api/metalakes/test/objects/catalog/catalog1/policies
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = client.loadCatalog("catalog1");
catalog.supportsPolicies().associatePolicies(
    new String[] {"retention_30d"},
    new String[] {"retention_7d"});

Schema schema = catalog.asSchemas().loadSchema("schema1");
schema.supportsPolicies().associatePolicies(new String[] {"retention_30d"}, null);
```

</TabItem>
</Tabs>

### List Policies on an Object

The response includes policies inherited from ancestors. With `details=true` each policy carries an
`inherited` field, which a plain name listing does not.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  "http://localhost:8090/api/metalakes/test/objects/catalog/catalog1/policies?details=true"
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = client.loadCatalog("catalog1");
String[] policyNames = catalog.supportsPolicies().listPolicies();
Policy[] policies = catalog.supportsPolicies().listPolicyInfos();
```

</TabItem>
</Tabs>

### Get One Policy on an Object

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/test/objects/catalog/catalog1/policies/retention_30d
```

</TabItem>
<TabItem value="java" label="Java">

```java
Policy policy = catalog.supportsPolicies().getPolicy("retention_30d");
```

</TabItem>
</Tabs>

### List Objects Carrying a Policy

The response lists direct attachments only, so a policy attached to a catalog returns that catalog
rather than the objects beneath it.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/test/policies/retention_30d/objects
```

</TabItem>
<TabItem value="java" label="Java">

```java
Policy policy = client.getPolicy("retention_30d");
MetadataObject[] objects = policy.associatedObjects().objects();
int count = policy.associatedObjects().count();
```

</TabItem>
</Tabs>
