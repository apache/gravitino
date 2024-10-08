---
title: "Manage metalake using Apache Gravitino"
slug: /manage-metalake-using-gravitino
date: 2023-12-10
keyword: Gravitino metalake manage
license: This software is licensed under the Apache License version 2.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This page introduces how to create, modify, view, and delete [metalakes](./glossary.md#metalake) by using Gravitino. 

## Prerequsites

You have installed and launched Gravitino. For more details, see [Get started](./getting-started.md).

Let's say, the access is [http://localhost:8090](http://localhost:8090).

## Create a metalake

To create a metalake, you can send a `POST` request to the `/api/metalakes` endpoint or use the Gravitino Admin client.

The following is an example of creating a metalake:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{"name":"metalake","comment":"This is a new metalake","properties":{}}' \
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

## Load a metalake

To load a metalake, you can send a `GET` request to the `/api/metalakes/{metalake_name}` endpoint or use the Gravitino Admin client.

The following is an example of loading a metalake:

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

## Alter a metalake

To alter a metalake, you can send a `PUT` request to the `/api/metalakes/{metalake_name}` endpoint or use the Gravitino Admin client.

The following is an example of renaming a metalake:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "updates": [
    {
      "@type": "rename",
      "newName": "metalake_renamed"
    }
  ]
}' http://localhost:8090/api/metalakes/metalake
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
GravitinoMetalake renamed = gravitinoAdminClient.alterMetalake(
    NameIdentifier.of("metalake"),
    MetalakeChange.rename("metalake_renamed")
);
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
changes = (
    MetalakeChange.rename("metalake_renamed"),
)

metalake = gravitino_admin_client.alter_metalake("metalake", *changes)
```

</TabItem>
</Tabs>


The following table outlines the supported modifications that you can make to a metalake:

| Supported modification | JSON                                                         | Java                                            | Python                                                                                    |
|------------------------|--------------------------------------------------------------|-------------------------------------------------|-------------------------------------------------------------------------------------------|
| Rename metalake        | `{"@type":"rename","newName":"metalake_renamed"}`            | `MetalakeChange.rename("metalake_renamed")`     | `MetalakeChange.rename("metalake_renamed")`                                               |
| Update comment         | `{"@type":"updateComment","newComment":"new_comment"}`       | `MetalakeChange.updateComment("new_comment")`   | `MetalakeChange.update_comment("new_comment")`                                    |
| Set property           | `{"@type":"setProperty","property":"key1","value":"value1"}` | `MetalakeChange.setProperty("key1", "value1")`  | `MetalakeChange.set_property("key1", "value1")` |
| Remove property        | `{"@type":"removeProperty","property":"key1"}`               | `MetalakeChange.removeProperty("key1")`         | `MetalakeChange.remove_property("key1")`                               |


## Drop a metalake

To drop a metalake, you can send a `DELETE` request to the `/api/metalakes/{metalake_name}` endpoint or use the Gravitino Admin client.

The following is an example of dropping a metalake:

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
Dropping a metalake in cascade mode is not allowed. That is, all the 
catalogs, schemas, and tables under a metalake must be removed before you can drop the metalake.
:::

## List all metalakes

To view all your metalakes, you can send a `GET` request to the `/api/metalakes` endpoint or use the Gravitino Admin client.

The following is an example of listing all metalakes:

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
