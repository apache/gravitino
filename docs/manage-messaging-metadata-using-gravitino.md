---
title: "Manage massaging metadata using Apache Gravitino"
slug: /manage-massaging-metadata-using-gravitino
date: 2024-4-22
keyword: Gravitino massaging metadata manage
license: This software is licensed under the Apache License version 2.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This page introduces how to manage messaging metadata using Apache Gravitino. Messaging metadata refers to 
the topic metadata of the messaging system such as Apache Kafka, Apache Pulsar, Apache RocketMQ, etc.
Through Gravitino, you can create, update, delete, and list topics via unified RESTful APIs or Java client.

To use messaging catalog, please make sure that:

 - Gravitino server has started, and the host and port is [http://localhost:8090](http://localhost:8090).
 - A metalake has been created and [enabled](./manage-metalake-using-gravitino.md#enable-a-metalake).

## Catalog operations

### Create a catalog

:::tip
For a messaging catalog, you must specify the `type` as `messaging` when creating a catalog.
:::

You can create a catalog by sending a `POST` request to the `/api/metalakes/{metalake_name}/catalogs`
endpoint or just use the Gravitino Java client. The following is an example of creating a messaging catalog:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "catalog",
  "type": "MESSAGING",
  "comment": "comment",
  "provider": "kafka",
  "properties": {
    "bootstrap.servers": "localhost:9092",
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://127.0.0.1:8090")
    .withMetalake("metalake")
    .build();

Map<String, String> properties = ImmutableMap.<String, String>builder()
    // You should replace the following with your own Kafka bootstrap servers that Gravitino can connect to.
    .put("bootstrap.servers", "localhost:9092")
    .build();

Catalog catalog = gravitinoClient.createCatalog("catalog",
    Type.MESSAGING,
    "kafka", // provider, Gravitino only supports "kafka" for now.
    "This is a Kafka catalog",
    properties);
// ...
```

</TabItem>
</Tabs>

Currently, Gravitino supports the following catalog providers:

| Catalog provider | Catalog property                                                |
|------------------|-----------------------------------------------------------------|
| `kafka`          | [Kafka catalog property](./kafka-catalog.md#catalog-properties) |

### Load a catalog

Refer to [Load a catalog](./manage-relational-metadata-using-gravitino.md#load-a-catalog)
in relational catalog for more details. For a messaging catalog, the load operation is the same.

### Alter a catalog

Refer to [Alter a catalog](./manage-relational-metadata-using-gravitino.md#alter-a-catalog)
in relational catalog for more details. For a messaging catalog, the alter operation is the same.

### Drop a catalog

Refer to [Drop a catalog](./manage-relational-metadata-using-gravitino.md#drop-a-catalog)
in relational catalog for more details. For a messaging catalog, the drop operation is the same.

### List all catalogs in a metalake

Please refer to [List all catalogs in a metalake](./manage-relational-metadata-using-gravitino.md#list-all-catalogs-in-a-metalake)
in relational catalog for more details. For a messaging catalog, the list operation is the same.

### List all catalogs' information in a metalake

Please refer to [List all catalogs' information in a metalake](./manage-relational-metadata-using-gravitino.md#list-all-catalogs-information-in-a-metalake)
in relational catalog for more details. For a messaging catalog, the list operation is the same.

## Schema operations

`Schema` is a logical grouping of topics in a messaging catalog, if the messaging system does not support topics grouping, 
schema operations are not supported but a "default" schema will be automatically created to include all topics

:::caution note
Gravitino currently only supports the Kafka catalog. Since Kafka does not support topic grouping, only list and load operations are supported for schema.
:::

### Create a schema

Please refer to [Create a schema](./manage-relational-metadata-using-gravitino.md#create-a-schema)
in relational catalog for more details. For a messaging catalog, the create operation is the same.

### Load a schema

Please refer to [Load a schema](./manage-relational-metadata-using-gravitino.md#load-a-schema)
in relational catalog for more details. For a messaging catalog, the load operation is the same.

### Alter a schema

Please refer to [Alter a schema](./manage-relational-metadata-using-gravitino.md#alter-a-schema)
in relational catalog for more details. For a messaging catalog, the alter operation is the same.

### Drop a schema

Please refer to [Drop a schema](./manage-relational-metadata-using-gravitino.md#drop-a-schema)
in relational catalog for more details. For a messaging catalog, the drop operation is the same.

### List all schemas under a catalog

Please refer to [List all schemas under a catalog](./manage-relational-metadata-using-gravitino.md#list-all-schemas-under-a-catalog)
in relational catalog for more details. For a messaging catalog, the list operation is the same.

## Topic operations

:::tip
Users should create a metalake, a catalog and a schema, then ensure that the metalake and catalog are enabled before operating topics.
:::

### Create a topic

You can create a topic by sending a `POST` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/topics`
endpoint or just use the Gravitino Java client. The following is an example of creating a topic:


<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "example_topic",
  "comment": "This is an example topic",
  "properties": {
    "partition-count": "3",
    "replication-factor": 1
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/default/topics
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://127.0.0.1:8090")
    .withMetalake("metalake")
    .build();

Catalog catalog = gravitinoClient.loadCatalog("catalog");
TopicCatalog topicCatalog = catalog.asTopicCatalog();

Map<String, String> propertiesMap = ImmutableMap.<String, String>builder()
        .put("partition-count": "3")
        .put("replication-factor": "1")
        .build();

topicCatalog.createTopic(
  NameIdentifier.of("default", "example_topic"),
  "This is an example topic",
  null, // The message schema of the topic object. Always null because it's not supported yet.
  propertiesMap,
);
```

</TabItem>
</Tabs>

### Alter a topic

You can modify a topic by sending a `PUT` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/topics/{topic_name}`
endpoint or just use the Gravitino Java client. The following is an example of altering a topic:


<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
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
}' http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/default/topics/topic
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
// Assuming you have just created a Kafka catalog named `catalog`
Catalog catalog = gravitinoClient.loadCatalog("catalog");

TopicCatalog topicCatalog = catalog.asTopicCatalog();

Topic t = topicCatalog.alterTopic(NameIdentifier.of("default", "topic"),
    TopicChange.removeProperty("key2"), TopicChange.setProperty("key3", "value3"));
// ...
```

</TabItem>
</Tabs>

Currently, Gravitino supports the following changes to a topic:

| Supported modification  | JSON                                                         | Java                                        |
|-------------------------|--------------------------------------------------------------|---------------------------------------------|
| Update a comment        | `{"@type":"updateComment","newComment":"new_comment"}`       | `TopicChange.updateComment("new_comment")`  |
| Set a topic property    | `{"@type":"setProperty","property":"key1","value":"value1"}` | `TopicChange.setProperty("key1", "value1")` |
| Remove a topic property | `{"@type":"removeProperty","property":"key1"}`               | `TopicChange.removeProperty("key1")`        |

### Drop a topic

You can remove a topic by sending a `DELETE` request to the `/api/metalakes/{metalake_name}
/catalogs/{catalog_name}/schemas/{schema_name}/topics/{topic_name}` endpoint or by using the
Gravitino Java client. The following is an example of dropping a topic:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/default/topics/topic
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
// Assuming you have just created a Kafka catalog named `catalog`
Catalog catalog = gravitinoClient.loadCatalog("catalog");

TopicCatalog topicCatalog = catalog.asTopicCatalog();

// Drop a topic
topicCatalog.dropTopic(NameIdentifier.of("default", "topic"));
// ...
```

</TabItem>
</Tabs>

### List all topics under a schema

You can list all topics in a schema by sending a `GET` request to the `/api/metalakes/
{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/topics` endpoint or by using the
Gravitino Java client. The following is an example of listing all the topics in a schema:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema/topics
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
Catalog catalog = gravitinoClient.loadCatalog("catalog");

TopicCatalog topicCatalog = catalog.asTopicCatalog();
NameIdentifier[] identifiers =
    topicCatalog.listTopics(Namespace.of("default"));
// ...
```

</TabItem>
</Tabs>