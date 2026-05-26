---
title: "Kafka Catalog"
slug: "/kafka-catalog"
date: 2024-4-22
keyword: "kafka catalog"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The Kafka catalog is a messaging catalog for managing Apache Kafka topic metadata. Each Kafka catalog corresponds to one Kafka cluster.

## Catalog

### Catalog Properties

Besides the [common catalog properties](./gravitino-server-config.md#catalog-properties), the Kafka catalog has the following properties:

| Property       | Description                                                                                                                                                                                                   | Default | Required | Since |
|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `bootstrap.servers` | The Kafka broker(s) to connect to, allowing for multiple brokers by comma-separating them.                                                                                                                    | (none)        | Yes      | 0.5.0         |
| `gravitino.bypass.` | Properties with this prefix are passed down to the underlying Kafka Admin client. See [Kafka Admin configs](https://kafka.apache.org/34/documentation.html#adminclientconfigs) for the supported keys.        | (none)        | No       | 0.5.0         |

### Catalog Operations

Refer to [Catalog operations](./manage-messaging-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

When a Kafka catalog is created, Gravitino automatically creates a `default` schema that contains all topics in the Kafka cluster.

### Schema Capabilities

The `default` schema is read-only and supports only loading and listing.

### Schema Properties

None.

### Schema Operations

Refer to [Schema operation](./manage-messaging-metadata-using-gravitino.md#schema-operations) for more details.

## Topic

### Topic Capabilities

The Kafka catalog supports creating, updating, deleting, and listing topics.

### Topic Properties

| Property name        | Description                              | Default                                                                       | Required | Since |
|----------------------|------------------------------------------|-------------------------------------------------------------------------------------|----------|---------------|
| `partition-count`    | Number of partitions for the topic.      | Falls back to `num.partition` from the broker.                                      | No       | 0.5.0         |
| `replication-factor` | Number of replications for the topic.    | Falls back to `default.replication.factor` from the broker.                         | No       | 0.5.0         |

Any other Kafka topic configuration can be passed through topic properties. See [Topic configs](https://kafka.apache.org/34/documentation.html#topicconfigs) for the full list.

### Topic Operations

Refer to [Topic operation](./manage-messaging-metadata-using-gravitino.md#topic-operations) for more details.
