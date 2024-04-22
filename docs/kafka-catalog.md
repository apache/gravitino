---
title: "Kafka catalog"
slug: /kafka-catalog
date: 2024-4-22
keyword: kafka catalog
license: "Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

## Introduction

Kafka catalog is a messaging catalog that offers the ability to manage Kafka topics metadata.
One Kafka catalog corresponds to one Kafka cluster.

## Catalog

### Catalog properties

| Property Name       | Description                                                                                                                                                                                                   | Default Value | Required | Since Version |
|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `bootstrap.servers` | The Kafka broker(s) to connect to, allowing for multiple brokers by comma-separating them.                                                                                                                    | (none)        | Yes      | 0.5.0         |
| `gravitino.bypass.` | Property name with this prefix passed down to the underlying Kafka Admin client for use. (refer to [Kafka Admin Configs](https://kafka.apache.org/34/documentation.html#adminclientconfigs) for more details) | (none)        | No       | 0.5.0         |

### Catalog operations

Refer to [Catalog operations](./manage-messaging-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

A "default" schema, which includes all the topics in the Kafka cluster, will be automatically created when catalog is created.

### Schema capabilities

- Since the "default" schema is read-only, it only supports loading and listing schema.

### Schema properties

None.

### Schema operations

Refer to [Schema operation](./manage-messaging-metadata-using-gravitino.md#schema-operations) for more details.

## Topic

### Topic capabilities

- The Kafka catalog supports creating, updating, deleting, and listing topics.

### Topic properties

| Property name        | Description                              | Default value                                                                       | Required | Since Version |
|----------------------|------------------------------------------|-------------------------------------------------------------------------------------|----------|---------------|
| `partition-count`    | The number of partitions for the topic.  | if not specified, will use the `num.partition` property in the broker.              | No       | 0.5.0         |
| `replication-factor` | The number of replications for the topic | if not specified, will use the `default.replication.factor` property in the broker. | No       | 0.5.0         |

You can pass other topic configurations to the topic properties. Refer to [Topic Configs](https://kafka.apache.org/34/documentation.html#topicconfigs) for more details.

### Topic operations

Refer to [Topic operation](./manage-messaging-metadata-using-gravitino.md#topic-operations) for more details.