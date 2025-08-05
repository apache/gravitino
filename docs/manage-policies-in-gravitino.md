---
title: "Manage policies in Gravitino"
slug: /manage-policies-in-gravitino
date: 2025-08-04
keyword: policy management, policy, policies, Gravitino, data governance
license: This software is licensed under the Apache License version 2.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

Starting from 1.0.0, Gravitino introduces a new policy system that allows you to manage policies for
metadata objects. Policies are a set of rules that can be associated with a metadata
object for data governance and so on.

This document briefly introduces how to use policies in Gravitino by both Gravitino Java client and
REST APIs. If you want to know more about the policy system in Gravitino, please refer to the
Javadoc and REST API documentation.

:::info
1. Metadata objects are objects that are managed in Gravitino, such as `CATALOG`, `SCHEMA`, `TABLE`,
   `FILESET`, `TOPIC`, and `MODEL`. A metadata object is combined by a `type` and a dot-separated
   `name`. For example, a `CATALOG` object has a name "catalog1" with type "CATALOG", a `SCHEMA`
   object has a name "catalog1.schema1" with type "SCHEMA", a `TABLE` object has a name
   "catalog1.schema1.table1" with type "TABLE".
2. Policies can be associated with metadata objects like `CATALOG`, `SCHEMA`, `TABLE`, `FILESET`, `TOPIC`, and `MODEL`.
   However, the specific types of metadata objects a policy can be attached to are determined by its `supportedObjectTypes` field.
3. A policy's `inheritable` property determines if it can be inherited by child objects. For a policy to be inherited,
   its `inheritable` flag must be `true`, and the child object's type must be included in the policy's `supportedObjectTypes`.
4. A policy's `exclusive` property determines if multiple policies of the same type can be associated with the same metadata object. 
   If `exclusive` is `true`, another policy of the same type cannot be directly associated with the object. 
   An inherited policy of the same type will be overridden by a directly associated one.
5. The same policy can be associated with both parent and child metadata objects. When you list the
   associated policies of a child metadata object, this policy will be included only once in the result
   list with `inherited` value `false`.
:::

## Policy operations

### Create new policies

The first step to manage policies is to create new policies. You can create a new policy by providing a policy
name, type, and other optional fields like comment, enabled, etc.

Gravitino supports two kinds of policies: built-in policies and custom policies. 
For built-in policies, the `exclusive`, `inheritable`, and `supportedObjectTypes` attributes are predefined. 
For custom policies, you need to specify these attributes.

:::note
1. The fields `policyType`, `exclusive`, `inheritable`, and `supportedObjectTypes` are immutable after the policy is created.
2. The values of fields `exclusive`, `inheritable`, and `supportedObjectTypes` for policies of the same type must be the same.
:::

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
# Create a custom policy
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "my_policy1",
  "comment": "This is a test policy",
  "policyType": "test_type",
  "enabled": true,
  "exclusive": true,
  "inheritable": true,
  "supportedObjectTypes": [
    "CATALOG",
    "SCHEMA",
    "TABLE",
    "FILESET",
    "TOPIC",
    "MODEL"
  ],
  "content": {
    "customRules": {
      "rule1": 123
    },
    "properties": {
      "key1": "value1"
    }
  }
}' http://localhost:8090/api/metalakes/test/policies
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient client = ...

// Create a custom policy
PolicyContent content = PolicyContents.custom(
    ImmutableMap.of("rule1", 123),
    ImmutableMap.of("key1", "value1"));
Policy policy = client.createPolicy(
    "my_policy1",
    "test_type",
    "This is a test policy",
    true /* enabled */,
    true /* exclusive */,
    true /* inheritable */,
    ImmutableSet.of(
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.SCHEMA,
        MetadataObject.Type.TABLE,
        MetadataObject.Type.FILESET,
        MetadataObject.Type.TOPIC,
        MetadataObject.Type.MODEL),
    content);
```

</TabItem>
</Tabs>

### List created policies

You can list all the created policy names as well as policy objects in a metalake in Gravitino.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
# List policy names
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/metalakes/test/policies

# List policy details
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/metalakes/test/policies?details=true
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient client = ...
String[] policyNames = client.listPolicies();

Policy[] policies = client.listPolicyInfos();
```

</TabItem>
</Tabs>

### Get a policy by name

You can get a policy by its name.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/metalakes/test/policies/my_policy1
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient client = ...
Policy policy = client.getPolicy("my_policy1");
```

</TabItem>
</Tabs>

### Update a policy

Gravitino allows you to update a policy by providing changes.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "updates": [
    {
      "@type": "rename",
      "newName": "my_policy_new"
    },
    {
      "@type": "updateComment",
      "newComment": "This is my new policy comment"
    },
    {
      "@type": "updateContent",
      "policyType": "test_type",
      "newContent": {
        "customRules": {
          "rule1": 456
        },
        "properties": {
          "key1": "new_value1",
          "key2": "new_value2"
        }
      }
    }
  ]
}' http://localhost:8090/api/metalakes/test/policies/my_policy1
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient client = ...
PolicyContent newContent = PolicyContents.custom(
    ImmutableMap.of("rule1", 456),
    ImmutableMap.of("key1", "new_value1", "key2", "new_value2"));

Policy policy = client.alterPolicy(
    "my_policy1",
    PolicyChange.rename("my_policy_new"),
    PolicyChange.updateComment("This is my new policy comment"),
    PolicyChange.updateContent("test_type", newContent));
```

</TabItem>
</Tabs>

Currently, Gravitino supports the following policy changes:

| Supported modification | JSON                                                                    | Java                                                  |
|------------------------|-------------------------------------------------------------------------|-------------------------------------------------------|
| Rename a policy        | `{"@type":"rename","newName":"policy_renamed"}`                         | `PolicyChange.rename("policy_renamed")`               |
| Update a comment       | `{"@type":"updateComment","newComment":"new_comment"}`                  | `PolicyChange.updateComment("new_comment")`           |
| Update policy content  | `{"@type":"updateContent","policyType":"test_type","newContent":{...}}` | `PolicyChange.updateContent("test_type", newContent)` |

### Enable or disable a policy

You can enable or disable a policy.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
# Disable a policy
curl -X PATCH -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "enable": false
}' http://localhost:8090/api/metalakes/test/policies/my_policy_new

# Enable a policy
curl -X PATCH -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "enable": true
}' http://localhost:8090/api/metalakes/test/policies/my_policy_new
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient client = ...
// Disable a policy
client.disablePolicy("my_policy_new");

// Enable a policy
client.enablePolicy("my_policy_new");
```

</TabItem>
</Tabs>

### Delete a policy

You can delete a policy by its name.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/metalakes/test/policies/my_policy_new
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient client = ...
client.deletePolicy("my_policy_new");
```

</TabItem>
</Tabs>

## Policy associations

Gravitino allows you to associate and disassociate policies with metadata objects. Currently,
`CATALOG`, `SCHEMA`, `TABLE`, `FILESET`, `TOPIC`, and `MODEL` objects can have policies.

### Associate and disassociate policies with a metadata object

You can associate and disassociate policies with a metadata object by providing the object type, object
name and policy names.

The request path for REST API is `/api/metalakes/{metalake}/objects/{metadataObjectType}/{metadataObjectFullName}/policies`.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
# First, create some policies to associate
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
-d '{"name": "policy1", "policyType": "custom", "supportedObjectTypes": ["CATALOG", "SCHEMA"], "content": {}}' \
http://localhost:8090/api/metalakes/test/policies

curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
-d '{"name": "policy2", "policyType": "custom", "supportedObjectTypes": ["CATALOG"], "content": {}}' \
http://localhost:8090/api/metalakes/test/policies

curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
-d '{"name": "policy3", "policyType": "custom", "supportedObjectTypes": ["CATALOG"], "content": {}}' \
http://localhost:8090/api/metalakes/test/policies

# Associate and disassociate policies with a catalog
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "policiesToAdd": ["policy1", "policy2"],
  "policiesToRemove": ["policy3"]
}' http://localhost:8090/api/metalakes/test/objects/catalog/my_catalog/policies

# Associate policies with a schema
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "policiesToAdd": ["policy1"]
}' http://localhost:8090/api/metalakes/test/objects/schema/my_catalog.my_schema/policies
```

</TabItem>
<TabItem value="java" label="Java">

```java
// Assume catalog 'my_catalog' and schema 'my_catalog.my_schema' exist
Catalog catalog = client.loadCatalog("my_catalog");
catalog.supportsPolicies().associatePolicies(
    new String[] {"policy1", "policy2"},
    new String[] {"policy3"});

// You need to load the schema from the catalog
Schema schema = catalog.asSchemas().loadSchema("my_schema");
schema.supportsPolicies().associatePolicies(new String[] {"policy1"}, null);
```

</TabItem>
</Tabs>

### List associated policies for a metadata object

You can list all the policies associated with a metadata object. If a policy is inheritable, 
listing policies of a metadata object will also list the policies of its parent metadata objects.

The request path for REST API is `/api/metalakes/{metalake}/objects/{metadataObjectType}/{metadataObjectFullName}/policies`.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
# List policy names for a catalog
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/metalakes/test/objects/catalog/my_catalog/policies

# List policy details for a schema
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/metalakes/test/objects/schema/my_catalog.my_schema/policies?details=true
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = client.loadCatalog("my_catalog");
String[] policyNames = catalog.supportsPolicies().listPolicies();
Policy[] policies = catalog.supportsPolicies().listPolicyInfos();

Schema schema = catalog.asSchemas().loadSchema("my_schema");
String[] schemaPolicyNames = schema.supportsPolicies().listPolicies();
Policy[] schemaPolicies = schema.supportsPolicies().listPolicyInfos();
```

</TabItem>
</Tabs>

### Get an associated policy by name for a metadata object

You can get an associated policy by its name for a metadata object.

The request path for REST API is `/api/metalakes/{metalake}/objects/{metadataObjectType}/{metadataObjectFullName}/policies/{policy}`.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/metalakes/test/objects/catalog/my_catalog/policies/policy1

curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/metalakes/test/objects/schema/my_catalog.my_schema/policies/policy1
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = client.loadCatalog("my_catalog");
Policy policy = catalog.supportsPolicies().getPolicy("policy1");

Schema schema = catalog.asSchemas().loadSchema("my_schema");
Policy schemaPolicy = schema.supportsPolicies().getPolicy("policy1");
```

</TabItem>
</Tabs>

### List metadata objects associated with a policy

You can list all the metadata objects directly associated with a policy.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/metalakes/test/policies/policy1/objects
```

</TabItem>
<TabItem value="java" label="Java">

```java
Policy policy = client.getPolicy("policy1");
MetadataObject[] objects = policy.associatedObjects().objects();
int count = policy.associatedObjects().count();
```

</TabItem>
</Tabs>

