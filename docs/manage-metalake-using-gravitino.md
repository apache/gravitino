---
title: "Manage metalake using Apache Gravitino"
slug: /manage-metalake-using-gravitino
date: 2023-12-10
keyword: Gravitino metalake manage
license: This software is licensed under the Apache License version 2.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This page introduces how to manage metalake by Apache Gravitino. Metalake is a tenant-like concept in
Gravitino, all the catalogs, users and roles are under a metalake. Typically, a metalake is
mapping to a organization or a company.

Through Gravitino, you can create, edit, and delete metalake. This page includes the following
contents:

Assuming Gravitino has just started, and the host and port is [http://localhost:8090](http://localhost:8090).

## Metalake operations

### Create a metalake

You can create a metalake by sending a `POST` request to the `/api/metalakes` endpoint or just use the Gravitino Admin Java client.
The following is an example of creating a metalake:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{"name":"metalake","comment":"comment","properties":{}}' \
http://localhost:8090/api/metalakes
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoAdminClient gravitinoAdminClient = GravitinoAdminClient
    .builder("http://localhost:8090")
    .build();

GravitinoMetalake newMetalake = gravitinoAdminClient.createMetalake(
    NameIdentifier.of("metalake"),
    "This is a new metalake",
    new HashMap<>());
  // ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_admin_client: GravitinoAdminClient = GravitinoAdminClient(uri="http://localhost:8090")
gravitino_admin_client.create_metalake(name="metalake", 
                                       comment="This is a new metalake", 
                                       properties={})
```

</TabItem>
</Tabs>

### Load a metalake

You can create a metalake by sending a `GET` request to the `/api/metalakes/{metalake_name}` endpoint or just use the Gravitino Java client. The following is an example of loading a metalake:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json"  http://localhost:8090/api/metalakes/metalake
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
GravitinoMetalake loaded = gravitinoAdminClient.loadMetalake(
    NameIdentifier.of("metalake"));
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_admin_client.load_metalake("metalake")
```

</TabItem>
</Tabs>

### Alter a metalake

You can modify a metalake by sending a `PUT` request to the `/api/metalakes/{metalake_name}` endpoint or just use the Gravitino Java client. The following is an example of altering a metalake:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "updates": [
    {
      "@type": "rename",
      "newName": "metalake"
    },
    {
      "@type": "setProperty",
      "property": "key2",
      "value": "value2"
    }
  ]
}' http://localhost:8090/api/metalakes/new_metalake
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
GravitinoMetalake renamed = gravitinoAdminClient.alterMetalake(
    NameIdentifier.of("new_metalake"),
    MetalakeChange.rename("new_metalake_renamed")
);
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
changes = (
    MetalakeChange.rename("metalake_new_name"),
    MetalakeChange.update_comment("metalake_new_comment"),
    MetalakeChange.remove_property("metalake_properties_key1"),
    MetalakeChange.set_property("metalake_properties_key2", "metalake_properties_new_value"),
)

metalake = gravitino_admin_client.alter_metalake("metalake_name", *changes)
```

</TabItem>
</Tabs>


Currently, Gravitino supports the following changes to a metalake:

| Supported modification | JSON                                                         | Java                                            |
|------------------------|--------------------------------------------------------------|-------------------------------------------------|
| Rename metalake        | `{"@type":"rename","newName":"metalake_renamed"}`            | `MetalakeChange.rename("metalake_renamed")`     |
| Update comment         | `{"@type":"updateComment","newComment":"new_comment"}`       | `MetalakeChange.updateComment("new_comment")`   |
| Set a property         | `{"@type":"setProperty","property":"key1","value":"value1"}` | `MetalakeChange.setProperty("key1", "value1")`  |
| Remove a property      | `{"@type":"removeProperty","property":"key1"}`               | `MetalakeChange.removeProperty("key1")`         |


### Drop a metalake

You can remove a metalake by sending a `DELETE` request to the `/api/metalakes/{metalake_name}` endpoint or just use the Gravitino Java client. The following is an example of dropping a metalake:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" http://localhost:8090/api/metalakes/metalake
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
boolean success = gravitinoAdminClient.dropMetalake("metalake");
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_admin_client.drop_metalake("metalake")
```

</TabItem>
</Tabs>

:::note
Current Gravitino doesn't support dropping a metalake in cascade mode, which means all the 
catalogs, schemas and tables under the metalake need to be removed before dropping the metalake.
:::

### List all metalakes

You can list metalakes by sending a `GET` request to the `/api/metalakes` endpoint or just use the Gravitino Java client. The following is an example of listing all the metalake names:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json"  http://localhost:8090/api/metalakes
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
GravitinoMetalake[] allMetalakes = gravitinoAdminClient.listMetalakes();
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
metalake_list: List[GravitinoMetalake] = gravitino_admin_client.list_metalakes()
```

</TabItem>
</Tabs>
