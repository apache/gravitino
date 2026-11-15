---
title: "Kafka Catalog"
slug: "/kafka-catalog"
date: 2024-4-22
keyword: "kafka catalog"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Kafka catalog is a messaging catalog that offers the ability to manage Apache Kafka topics' metadata.
One Kafka catalog corresponds to one Kafka cluster.

## Catalog

### Catalog Properties

Besides the [common catalog properties](./gravitino-server-config.md#catalog-properties-configuration), the Kafka catalog has the following properties:

| Property Name                 | Description                                                                                                                                                                                                   | Default Value | Required |
|-------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|
| `bootstrap.servers`           | The Kafka broker(s) to connect to, allowing for multiple brokers by comma-separating them.                                                                                                                    | (none)        | Yes      |
| `gravitino.bypass.`           | Property name with this prefix passed down to the underlying Kafka Admin client for use. (refer to [Kafka Admin Configs](https://kafka.apache.org/34/documentation.html#adminclientconfigs) for more details) | (none)        | No       |
| `table-topic.iceberg-catalog` | Optional name of the Iceberg catalog in the same metalake where AutoMQ Table Topics are materialized. Used to compute read-only Iceberg linkage properties on topics.                                          | (none)        | No       |
| `table-topic.default-namespace` | Default Iceberg namespace (schema) written to `automq.table.topic.namespace` when creating or enabling an AutoMQ Table Topic through Gravitino without specifying `automq.table.topic.namespace`.              | `default`     | No       |

### Catalog Operations

Refer to [Catalog operations](./manage-messaging-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

A "default" schema, which includes all the topics in the Kafka cluster, will be automatically created when catalog is created.

### Schema Capabilities

- Since the "default" schema is read-only, it only supports loading and listing schema.

### Schema Properties

None.

### Schema Operations

Refer to [Schema operation](./manage-messaging-metadata-using-gravitino.md#schema-operations) for more details.

## Topic

### Topic Capabilities

- The Kafka catalog supports creating, updating, deleting, and listing topics.

### Topic Naming Rules

When creating a topic through Gravitino, its name must contain 1 to 128 characters and use only
ASCII letters, digits, underscores (`_`), and hyphens (`-`). A leading hyphen is allowed. For example,
`orders`, `order_events`, and `-orders` are valid names; `a/b`, `a=b`, and `a.b` are not.

Gravitino excludes dots for authorization compatibility and limits names to 128 characters to fit
its metadata storage. These restrictions are stricter than Kafka's native naming rules.

Invalid names return HTTP `400 Bad Request` before reaching Kafka. A name that passes these checks
can still be rejected by Kafka, for example when `a_b` collides with an existing `a.b` in
ZooKeeper-mode Kafka. These broker-side name rejections also return HTTP `400 Bad Request`, with
Kafka's rejection reason included in the error message.

::::caution Topic names containing dots
When authorization is enabled, topic names containing dots are unsupported, and one such topic can
cause the entire topic list request to fail. See
[Names containing dots](./security/access-control.md#names-containing-dots) for details and the
workaround.
::::

### Topic Properties

| Property name                          | Description                                                                                     | Default value                                                                       | Required |
|----------------------------------------|-------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------|----------|
| `partition-count`                      | The number of partitions for the topic.                                                         | if not specified, will use the `num.partitions` property in the broker.             | No       |
| `replication-factor`                   | The number of replications for the topic                                                        | if not specified, will use the `default.replication.factor` property in the broker. | No       |
| `automq.table.topic.enable`            | Whether to materialize this Kafka topic as an AutoMQ Table Topic in Apache Iceberg.             | (none)                                                                              | No       |
| `automq.table.topic.commit.interval.ms`| Commit interval in milliseconds for materializing AutoMQ Table Topic records (must be `> 0`).   | (none)                                                                              | No       |
| `automq.table.topic.namespace`         | Target Iceberg namespace (schema) for the materialized Table Topic.                             | (none)                                                                              | No       |
| `automq.table.topic.schema.type`       | Schema mode for Table Topic conversion (`schemaless`, `schema`, `record_name`, or `topic_name`).| (none)                                                                              | No       |
| `automq.table.topic.id.columns`        | Comma-separated primary key column names for Iceberg equality deletes/upserts. Required when both `automq.table.topic.enable=true` and `automq.table.topic.upsert.enable=true`. | (none) | No |
| `automq.table.topic.partition.by`      | Iceberg partition transform expressions for the materialized Table Topic.                       | (none)                                                                              | No       |
| `automq.table.topic.upsert.enable`     | Whether upsert mode is enabled for the materialized AutoMQ Table Topic.                         | (none)                                                                              | No       |
| `automq.table.topic.cdc.field`         | Field name carrying the CDC operation code for AutoMQ Table Topic materialization.              | (none)                                                                              | No       |
| `automq.table.topic.errors.tolerance`  | Error tolerance policy when converting records (`none` or `all`).                               | (none)                                                                              | No       |

Pass other topic configurations to the topic properties. Refer to [Topic Configs](https://kafka.apache.org/34/documentation.html#topicconfigs) for more details.

#### AutoMQ Table Topic Iceberg Linkage Properties

When `automq.table.topic.enable` is set to `true`, Gravitino populates the following reserved read-only properties on `loadTopic`, `createTopic`, and `alterTopic` responses (these properties cannot be set, altered, or removed directly by clients):

| Property name                 | Description                                                                                                                |
|-------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| `table-topic.enabled`         | Set to `true` when AutoMQ Table Topic materialization is enabled on the topic.                                             |
| `table-topic.iceberg-catalog` | Linked Iceberg catalog name from catalog property `table-topic.iceberg-catalog`, if configured.                            |
| `table-topic.iceberg-namespace`| Effective Iceberg namespace from `automq.table.topic.namespace` on the topic (`default` if unset on the broker).          |
| `table-topic.iceberg-table`   | Target Iceberg table name (matching the Kafka topic name).                                                                 |
| `table-topic.iceberg-ident`   | Qualified Iceberg table identifier (`<metalake>.<iceberg-catalog>.<namespace>.<table>`, or `<namespace>.<table>` if catalog is unset). |

Gravitino's MCP server exposes `get_topic_iceberg_linkage` to retrieve this linked Iceberg identifier and Table Topic configuration for a topic.

### Topic Operations

Refer to [Topic operation](./manage-messaging-metadata-using-gravitino.md#topic-operations) for more details.
