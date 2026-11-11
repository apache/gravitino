---
title: "Manage Messaging Metadata"
slug: "/manage-messaging-metadata-using-gravitino"
keyword: "topic management, topic, messaging, Kafka, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

This page covers the Gravitino API for topics. For what a topic is, the default schema a messaging
catalog presents, topic properties, and what Gravitino does not cover, see [Topics](./topics.md). For
creating the catalog a topic lives in, see
[Manage Catalogs and Schemas](./manage-catalogs-and-schemas.md) and
[Apache Kafka catalog](./kafka-catalog.md).

A messaging catalog presents a single schema named `default`, so every path below uses it.

The Python client does not cover topics. `as_topic_catalog()` exists but raises
`UnsupportedOperationException`, so the examples below are REST and Java only.

## Topic Operations

### Create a Topic

A topic needs a name. `partition-count` and `replication-factor` are optional and fall back to the
broker's own defaults.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "orders",
  "comment": "Order events",
  "properties": {
    "partition-count": "3",
    "replication-factor": "1"
  }
}' http://localhost:8090/api/metalakes/example/catalogs/events/schemas/default/topics
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = client.loadCatalog("events");
TopicCatalog topics = catalog.asTopicCatalog();

Topic topic = topics.createTopic(
    NameIdentifier.of("default", "orders"),
    "Order events",
    null,
    ImmutableMap.of("partition-count", "3", "replication-factor", "1"));
```

</TabItem>
</Tabs>

The third argument to `createTopic` is the message schema, which is not supported and is always
`null`.

### Load a Topic

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/events/schemas/default/topics/orders
```

</TabItem>
<TabItem value="java" label="Java">

```java
Topic topic = topics.loadTopic(NameIdentifier.of("default", "orders"));
```

</TabItem>
</Tabs>

### Alter a Topic

Changes are applied as a list in one request. `partition-count` can be increased through a property
change; `replication-factor` is immutable once the topic exists.

| Change             | JSON                                                         | Java                                        |
|--------------------|--------------------------------------------------------------|---------------------------------------------|
| Update the comment | `{"@type":"updateComment","newComment":"new_comment"}`       | `TopicChange.updateComment("new_comment")`  |
| Set a property     | `{"@type":"setProperty","property":"key1","value":"value1"}` | `TopicChange.setProperty("key1", "value1")` |
| Remove a property  | `{"@type":"removeProperty","property":"key1"}`               | `TopicChange.removeProperty("key1")`        |

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "updates": [
    {"@type": "setProperty", "property": "partition-count", "value": "6"}
  ]
}' http://localhost:8090/api/metalakes/example/catalogs/events/schemas/default/topics/orders
```

</TabItem>
<TabItem value="java" label="Java">

```java
Topic topic = topics.alterTopic(
    NameIdentifier.of("default", "orders"),
    TopicChange.setProperty("partition-count", "6"));
```

</TabItem>
</Tabs>

### Drop a Topic

Dropping a topic through Gravitino deletes it in the cluster, along with its messages.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/events/schemas/default/topics/orders
```

</TabItem>
<TabItem value="java" label="Java">

```java
boolean dropped = topics.dropTopic(NameIdentifier.of("default", "orders"));
```

</TabItem>
</Tabs>

### List Topics

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/events/schemas/default/topics
```

</TabItem>
<TabItem value="java" label="Java">

```java
NameIdentifier[] identifiers = topics.listTopics(Namespace.of("default"));
```

</TabItem>
</Tabs>
