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

| Property Name                   | Description                                                                                                                                                                                                                                                             | Default Value | Required |
|---------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|
| `bootstrap.servers`             | The Kafka broker(s) to connect to, allowing for multiple brokers by comma-separating them.                                                                                                                                                                              | (none)        | Yes      |
| `gravitino.bypass.`             | Property name with this prefix passed down to the underlying Kafka Admin client for use. (refer to [Kafka Admin Configs](https://kafka.apache.org/39/configuration/admin-configs/) for more details)                                                                    | (none)        | No       |
| `table-topic.iceberg-catalog`   | Optional name of the Iceberg catalog in the same metalake where AutoMQ Table Topics are materialized. Used to compute read-only Iceberg linkage properties on topics.                                                                                                   | (none)        | No       |
| `table-topic.default-namespace` | Default Iceberg namespace (schema) written to `automq.table.topic.namespace` when creating or enabling an AutoMQ Table Topic through Gravitino without specifying `automq.table.topic.namespace`. If unset, Gravitino does not write `automq.table.topic.namespace` and AutoMQ uses its built-in `default` namespace. | (none) | No |

The Kafka catalog rejects the Kafka config provider properties `gravitino.bypass.config.providers`
and `gravitino.bypass.config.providers.*` (for example `gravitino.bypass.config.providers.file.class`),
because a provider can read the Gravitino server's own environment and files. Creating a catalog,
testing a connection, or altering a catalog with these properties fails with an error naming them.
This includes properties that come from `kafka.conf` (`catalogs/kafka/conf/kafka.conf`, or
`<package>/conf/kafka.conf` for a catalog with a custom `package` property), so a provider property
in a `kafka.conf` file blocks the creation of every Kafka catalog that uses that file. A catalog
created before upgrading that still has them fails on schema and topic operations until they are
removed.

:::caution Credentials in bypass properties
Bypass properties can be returned in plaintext by the catalog REST API. Gravitino masks only keys
whose names match the
[sensitive key keywords](./gravitino-server-config.md#sensitive-property-key-matching). With the
default keywords, `gravitino.bypass.ssl.truststore.password` is masked but a password inside
`gravitino.bypass.sasl.jaas.config` is returned unchanged. Changing
`gravitino.secret.sensitiveKeyKeywords` replaces the default list, so what is masked depends on the
server configuration. Do not put credentials in such properties as plaintext, including in
`kafka.conf`: its values are copied into the properties of each catalog created with it. If a
Gravitino secret provider is configured, bind the whole property per catalog. Use
`CatalogChange.setSecretBinding` (REST update type `setSecretBinding`) with a provider that
supports versioned writes, or `CatalogChange.setSecretReference` (REST update type
`setSecretReference`) with an external secret that holds the whole property value.
:::

See [Kafka catalog client upgrade](./migration-guide.md#kafka-catalog-client-upgrade) for how to
migrate catalogs that used config providers or `LdapLoginModule`.

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
| `automq.table.topic.id.columns`        | Primary key column names (comma-separated `col1,col2` or bracketed `[col1, col2]`) for Iceberg equality deletes/upserts. Required when `automq.table.topic.enable=true` and either `automq.table.topic.upsert.enable=true` or `automq.table.topic.cdc.field` is set. | (none) | No |
| `automq.table.topic.partition.by`      | Iceberg partition transform expressions for the materialized Table Topic.                       | (none)                                                                              | No       |
| `automq.table.topic.upsert.enable`     | Whether upsert mode is enabled for the materialized AutoMQ Table Topic.                         | (none)                                                                              | No       |
| `automq.table.topic.cdc.field`         | Field name carrying the CDC operation code for AutoMQ Table Topic materialization.              | (none)                                                                              | No       |
| `automq.table.topic.errors.tolerance`  | Error tolerance policy when converting records (`none`, `invalid_data`, or `all`).              | (none)                                                                              | No       |
| `automq.table.topic.convert.key.type`  | Key conversion strategy for AutoMQ Table Topic records (`raw`, `string`, `by_schema_id`, or `by_latest_schema`). | (none)                                                                     | No       |
| `automq.table.topic.convert.value.type`| Value conversion strategy for AutoMQ Table Topic records (`raw`, `string`, `by_schema_id`, or `by_latest_schema`). | (none)                                                                   | No       |
| `automq.table.topic.transform.value.type` | Value transformation type applied when materializing AutoMQ Table Topic records (for example `debezium`). | (none)                                                                    | No       |

Pass other topic configurations to the topic properties. Refer to [Topic-Level Configs](https://kafka.apache.org/39/configuration/topic-level-configs/) for more details.

#### AutoMQ Table Topic Iceberg Linkage Properties

When `automq.table.topic.enable` is set to `true`, Gravitino populates the following reserved read-only properties on `loadTopic`, `createTopic`, and `alterTopic` responses (these properties cannot be set, altered, or removed directly by clients):

| Property name                 | Description                                                                                                                |
|-------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| `table-topic.enabled`         | Set to `true` when AutoMQ Table Topic materialization is enabled on the topic.                                             |
| `table-topic.iceberg-catalog` | Linked Iceberg catalog name from catalog property `table-topic.iceberg-catalog`, if configured.                            |
| `table-topic.iceberg-namespace`| Effective Iceberg namespace from `automq.table.topic.namespace` on the topic (`default` if unset on the broker).          |
| `table-topic.iceberg-table`   | Target Iceberg table name (matching the Kafka topic name).                                                                 |
| `table-topic.iceberg-ident`   | Qualified Iceberg table identifier (`<metalake>.<iceberg-catalog>.<namespace>.<table>`, or `<namespace>.<table>` if catalog is unset). |

Gravitino's MCP server exposes `get_topic_iceberg_linkage` to retrieve this linked Iceberg identifier and Table Topic configuration for a topic. Note that AutoMQ creates the target Iceberg table asynchronously on the first commit interval after valid records arrive, so `table-topic.iceberg-ident` may exist on the topic before `load_table` finds the Iceberg table.

### Topic Operations

Refer to [Topic operation](./manage-messaging-metadata-using-gravitino.md#topic-operations) for more details.
