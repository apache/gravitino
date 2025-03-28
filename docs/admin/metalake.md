---
title: "Manage metalake"
slug: /manage-metalake
date: 2023-12-10
keyword: Gravitino metalake manage
license: This software is licensed under the Apache License version 2.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This page introduces how to create, modify, view, and delete [metalakes](../glossary.md#metalake) in Gravitino. 

## Prerequisites

You have installed and launched Gravitino.
For more details, see [Get started](../getting-started/index.md).
This page assumes that Gravitino server is serving at [http://localhost:8090](http://localhost:8090).

## Create a metalake

To create a metalake, you can send a `POST` request to the `/api/metalakes` endpoint
or use the Gravitino Admin client.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
cat <<EOF >metalake.json
{
  "name": "mymetalake",
  "comment": "This is a new metalake",
  "properties": {}
}
EOF

curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@metalake.json' \
  http://localhost:8090/api/metalakes
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoAdminClient gravitinoAdminClient = GravitinoAdminClient
    .builder("http://localhost:8090")
    .build();

GravitinoMetalake newMetalake = gravitinoAdminClient.createMetalake(
    NameIdentifier.of("mymetalake"),
    "This is a new metalake",
    new HashMap<>());
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoAdminClient(uri="http://localhost:8090")
admin_client.create_metalake(
    name="mymetalake", 
    comment="This is a new metalake", 
    properties={})
```

</TabItem>
</Tabs>

## Load a metalake

To load a metalake, you can send a `GET` request to the `/api/metalakes/{metalake}` endpoint
or use the Gravitino Admin client.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET \
  -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/mymetalake
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoMetalake loaded = gravitinoAdminClient.loadMetalake(
    NameIdentifier.of("mymetalake"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
client.load_metalake("mymetalake")
```
</TabItem>
</Tabs>

## Alter a metalake

To alter a metalake, you can send a `PUT` request to the `/api/metalakes/{metalake}` endpoint
or use the Gravitino Admin client.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
cat <<EOF >update.json
{
  "updates": [
    {
      "@type": "rename",
      "newName": "new-name"
    }
  ]
}
EOF

curl -X PUT \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@updata.json' \
  http://localhost:8090/api/metalakes/mymetalake
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoMetalake renamed = gravitinoAdminClient.alterMetalake(
    NameIdentifier.of("mymetalake"),
    MetalakeChange.rename("new-name")
);
```

</TabItem>
<TabItem value="python" label="Python">

```python
changes = (
    MetalakeChange.rename("new-name"),
)

metalake = client.alter_metalake("mymetalake", *changes)
```

</TabItem>
</Tabs>

The following table outlines the supported modifications that you can make to a metalake:

<table>
<thead>
<tr>
  <th>Supported modification</th>
  <th>JSON payload</th>
  <th>Java</th>
  <th>Python</th>
</tr>
</thead>
<tbody>
<tr>
  <td>Rename metalake</td>
  <td>`{"@type":"rename","newName":"metalake_renamed"}`</td>
  <td>`MetalakeChange.rename("metalake_renamed")`</td>
  <td>`MetalakeChange.rename("metalake_renamed")`</td>
</tr>
<tr>
  <td>Update metalake comment</td>
  <td>`{"@type":"updateComment","newComment":"new_comment"}`</td>
  <td>`MetalakeChange.updateComment("new_comment")`</td>
  <td>`MetalakeChange.update_comment("new_comment")`</td>
</tr>
<tr>
  <td>Set metalake property</td>
  <td>`{"@type":"setProperty","property":"key1","value":"value1"}`</td>
  <td>`MetalakeChange.setProperty("key1", "value1")`</td>
  <td>`MetalakeChange.set_property("key1", "value1")`</td>
</tr>
<tr>
  <td>Remove property</td>
  <td>`{"@type":"removeProperty","property":"key1"}`</td>
  <td>`MetalakeChange.removeProperty("key1")`</td>
  <td>`MetalakeChange.remove_property("key1")`</td>
</tr>
</tbody>
</table>

## Enable a metalake

Metalake has a reserved property `in-use` that indicates whether the metalake is available for use.
By default, the `in-use` property is set to `true`.
To enable a disabled metalake, you can send a `PATCH` request
to the `/api/metalakes/{metalake}` endpoint or use the Gravitino Admin client.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X PATCH \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{"inUse": true}' \
  http://localhost:8090/api/metalakes/mymetalake
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoAdminClient gravitinoAdminClient = GravitinoAdminClient
    .builder("http://localhost:8090")
    .build();
gravitinoAdminClient.enableMetalake("mymetalake");
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoAdminClient(uri="http://localhost:8090")
client.enable_metalake("mymetalake")
```

</TabItem>
</Tabs>

:::info
This operation does nothing if the metalake is already enabled.
:::

## Disable a metalake

Once a metalake is disabled:

- Users can only [list](#list-all-metalakes), [load](#load-a-metalake), [drop](#drop-a-metalake),
  or [enable](#enable-a-metalake) it.
- Any other operation on the metalake or its sub-entities will result in an error.

To disable a metalake, you can send a `PATCH` request to the `/api/metalakes/{metalake}` endpoint
or use the Gravitino Admin client.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X PATCH \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{"inUse": false}' \
  http://localhost:8090/api/metalakes/mymetalake
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoAdminClient gravitinoAdminClient = GravitinoAdminClient
    .builder("http://localhost:8090")
    .build();
gravitinoAdminClient.disableMetalake("mymetalake");
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoAdminClient(uri="http://localhost:8090")
client.disable_metalake("mymetalake")

```
</TabItem>
</Tabs>

:::info
This operation does nothing if the metalake is already disabled.
:::

## Drop a metalake

By default, Gravitino doesn't drop a metalake *by force*.
Before dropping a metalake, please make sure:

- There are no catalogs under the metalake.
  Otherwise, you will get an error unless you are dropping the metalake *by force*.
- The metalake is [disabled](#disable-a-metalake).
  Otherwise, you will get an error unless you are dropping the metalake *by force*.

Dropping a metalake *by force* will:

- Delete all sub-entities (tags, catalogs, schemas, etc.) hosted in the metalake.
- Delete the metalake itself even if it is enabled.

Dropping a metalake *by force* will not delete the external resources
(such as database, table, etc.) associated with sub-entities
unless they are managed (such as managed fileset).

To drop a metalake, you can send a `DELETE` request to the `/api/metalakes/{metalake}` endpoint
or use the Gravitino Admin client.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X DELETE \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  http://localhost:8090/api/metalakes/mymetalake?force=false
```

</TabItem>
<TabItem value="java" label="Java">

```java
// The second parameter 'force' can be true or false
boolean success = gravitinoAdminClient.dropMetalake("mymetalake", false);
```

</TabItem>
<TabItem value="python" label="Python">

```python
client.drop_metalake("mymetalake", force=True)
```

</TabItem>
</Tabs>

## List all metalakes

To view all your metalakes, you can send a `GET` request to the `/api/metalakes` endpoint
or use the Gravitino Admin client.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET \
  -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoMetalake[] allMetalakes = gravitinoAdminClient.listMetalakes();
```

</TabItem>
<TabItem value="python" label="Python">

```python
metalake_list = client.list_metalakes()
```

</TabItem>
</Tabs>

