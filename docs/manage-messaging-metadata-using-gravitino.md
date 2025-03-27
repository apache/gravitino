---
title: "Manage massaging metadata using Apache Gravitino"
slug: /manage-massaging-metadata-using-gravitino
date: 2024-4-22
keyword: Gravitino massaging metadata manage
license: This software is licensed under the Apache License version 2.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This page introduces how to manage messaging metadata using Apache Gravitino.
Messaging metadata refers to the topic metadata of the messaging system
such as *Apache Kafka*, *Apache Pulsar*, *Apache RocketMQ*, etc.
With Gravitino, you can create, update, delete, and list topics
using its RESTful APIs or the Java/Python client SDKs.

To use a messaging catalog, please make sure that:

- The Gravitino server has started and is serving at [http://localhost:8090](http://localhost:8090).
- A metalake has been created and [enabled](./manage-metalake-using-gravitino.md#enable-a-metalake).

## Catalog operations

### Create a catalog

:::tip
For a messaging catalog, you must set the `type` to `messaging` when creating a catalog.
:::

You can create a catalog by sending a `POST` request
to the `/api/metalakes/{metalake}/catalogs` endpoint
or just use the Gravitino client SDKs.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
cat <<EOF >catalog.json
{
  "name": "mycatalog",
  "type": "MESSAGING",
  "comment": "comment",
  "provider": "kafka",
  "properties": {
    "bootstrap.servers": "localhost:9092",
  }
}
EOF

curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@catalog.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://127.0.0.1:8090")
    .withMetalake("mymetalake")
    .build();

// Replace the bootstrap server with your own server that Gravitino can connect to.
Map<String, String> properties = ImmutableMap.<String, String>builder()
    .put("bootstrap.servers", "localhost:9092")
    .build();

// The 3rd parameter is the provder.
Catalog catalog = gravitinoClient.createCatalog(
    "mycatalog",
    Type.MESSAGING,
    "kafka",
    "This is a Kafka catalog",
    properties);
```

</TabItem>
</Tabs>

Currently, Gravitino supports the following catalog providers:

<table>
<thead>
<tr>
  <th>Catalog provider</th>
  <th>Catalog properties</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>kafka</tt></td>
  <td>[Link](./catalogs/messaging/kafka/index.md#catalog-properties)</td>
</tr>
</tbody>
</table>

### Load a catalog

Refer to [loading a catalog](./manage-relational-metadata-using-gravitino.md#load-a-catalog)
for a relational catalog.

### Alter a catalog

Refer to [altering a catalog](./manage-relational-metadata-using-gravitino.md#alter-a-catalog)
for a relational catalog.

### Drop a catalog

Refer to [dropping a catalog](./manage-relational-metadata-using-gravitino.md#drop-a-catalog)
for a relational catalog.

### List all catalogs in a metalake

Refer to [listing all catalogs](./manage-relational-metadata-using-gravitino.md#list-all-catalogs-in-a-metalake)
for relational catalogs.

### List all catalogs' information in a metalake

Refer to [list all catalogs' information](./manage-relational-metadata-using-gravitino.md#list-all-catalogs-information-in-a-metalake)
for relational catalogs.

## Schema operations

A *Schema* in a messaging catalogis a logical grouping of topics.
If the messaging system does not support topics grouping, schema operations are not supported.
But a *default* schema will be automatically created to host all topics.

:::caution note
Gravitino currently only supports the *Kafka* catalog.
Since Kafka does not support topic grouping, only *list* and *load* operations are supported for schema.
:::

### Create a schema

Refer to [creating a schema](./manage-relational-metadata-using-gravitino.md#create-a-schema)
for relational catalogs.

### Load a schema

Refer to [loading a schema](./manage-relational-metadata-using-gravitino.md#load-a-schema)
for a relational catalog.

### Alter a schema

Refer to [altering a schema](./manage-relational-metadata-using-gravitino.md#alter-a-schema)
for a relational catalog.

### Drop a schema

Refer to [dropping a schema](./manage-relational-metadata-using-gravitino.md#drop-a-schema)
for a relational catalog.

### List all schemas under a catalog

Refer to [listing all schemas](./manage-relational-metadata-using-gravitino.md#list-all-schemas-under-a-catalog)
for a relational catalog.

## Topic operations

:::tip
You will need a metalake, a catalog and a schema before operating on topics.
You have to ensure that the metalake and the catalog are enabled.
:::

### Create a topic

You can create a topic by sending a `POST` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/topics` endpoint
or use the Gravitino client SDKs.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
cat <<EOF >topic.json
{
  "name": "mytopic",
  "comment": "This is an example topic",
  "properties": {
    "partition-count": "3",
    "replication-factor": 1
  }
}
EOF

curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@topic.json' \
   http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/default/topics
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://127.0.0.1:8090")
    .withMetalake("mymetalake")
    .build();

Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
TopicCatalog topicCatalog = catalog.asTopicCatalog();

Map<String, String> propertiesMap = ImmutableMap.<String, String>builder()
        .put("partition-count": "3")
        .put("replication-factor": "1")
        .build();

topicCatalog.createTopic(
  NameIdentifier.of("default", "mytopic"),
  "This is an example topic",
  null, // The message schema of the topic object. Always null because it's not supported yet.
  propertiesMap,
);
```

</TabItem>
</Tabs>

### Alter a topic

You can modify a topic by sending a `PUT` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/topics/{topic}` endpoint
or use the Gravitino client SDKs.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
cat <<EOF >update.json
{
  "updates": [
    {
      "@type": "removeProperty",
      "property": "key2"
    }, {
      "@type": "setProperty",
      "property": "key3",
      "value": "value3"
    }
  ]
}
EOF

curl -X PUT \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@update.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/default/topics/mytopic
```

</TabItem>
<TabItem value="java" label="Java">

```java
// This assumes that you have a Kafka catalog named `mycatalog`,
// and a topic named `mytopic`.
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
TopicCatalog topicCatalog = catalog.asTopicCatalog();

Topic t = topicCatalog.alterTopic(
    NameIdentifier.of("default", "mytopic"),
    TopicChange.removeProperty("key2"),
    TopicChange.setProperty("key3", "value3")
);
```

</TabItem>
</Tabs>

Currently, Gravitino supports the following changes to a topic:

<table>
<thead>
<tr>
  <th>Supported modification</th>
  <th>JSON payload</th>
  <th>Java methods</th>
</tr>
</thead>
<tbody>
<tr>
  <td>Update topic comment</td>
  <td>`{"@type":"updateComment","newComment":"new comment"}`</td>
  <td>`TopicChange.updateComment("new_comment")`</td>
</tr>
<tr>
  <td>Set topic property</td>
  <td>`{"@type":"setProperty","property":"key1","value":"value1"}`</td>
  <td>`TopicChange.setProperty("key1", "value1")`</td>
</tr>
<tr>
  <td>Remove topic property</td>
  <td>`{"@type":"removeProperty","property":"key1"}`</td>
  <td>`TopicChange.removeProperty("key1")`</td>
</tr>
</tbody>
</table>

### Drop a topic

You can remove a topic by sending a `DELETE` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/topics/{topic}` endpoint
or by using the Gravitino client SDKs.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X DELETE \
  -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/default/topics/mytopic
```

</TabItem>
<TabItem value="java" label="Java">

```java
// This assumes that you have a Kafka catalog named `mycatalog`,
// and a topic named `mytopic`.
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
TopicCatalog topicCatalog = catalog.asTopicCatalog();
topicCatalog.dropTopic(NameIdentifier.of("default", "mytopic"));
```
</TabItem>
</Tabs>

### List all topics under a schema

You can list all topics in a schema by sending a `GET` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/topics` endpoint
or by using the Gravitino client SDKs.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET \
  -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/default/topics
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
TopicCatalog topicCatalog = catalog.asTopicCatalog();
NameIdentifier[] identifiers =
    topicCatalog.listTopics(Namespace.of("default"));
```
</TabItem>
</Tabs>

