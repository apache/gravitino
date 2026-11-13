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

| Property Name       | Description                                                                                                                                                                                                   | Default Value | Required |
|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|
| `bootstrap.servers` | The Kafka broker(s) to connect to, allowing for multiple brokers by comma-separating them.                                                                                                                    | (none)        | Yes      |
| `gravitino.bypass.` | Property name with this prefix passed down to the underlying Kafka Admin client for use. (refer to [Kafka Admin Configs](https://kafka.apache.org/39/documentation.html#adminclientconfigs) for more details) | (none)        | No       |

The Kafka catalog rejects the Kafka config provider properties `gravitino.bypass.config.providers`
and `gravitino.bypass.config.providers.*` (for example `gravitino.bypass.config.providers.file.class`),
because a provider can read the Gravitino server's own environment and files. Creating a catalog,
testing a connection, or altering a catalog with these properties fails with an error naming them.
A catalog that still has them, such as one created before upgrading or one affected by
`catalogs/kafka/conf/kafka.conf`, fails on schema and topic operations until they are removed.

:::caution Credentials in bypass properties
Bypass properties can be returned in plaintext by the catalog REST API. Gravitino masks only keys
whose names match [sensitive key keywords](./gravitino-server-config.md#sensitive-property-key-matching),
so `gravitino.bypass.ssl.truststore.password` is masked but a password inside
`gravitino.bypass.sasl.jaas.config` is returned unchanged. Do not put credentials in such properties
as plaintext. If a Gravitino secret provider is configured, bind the whole property with
`CatalogChange.setSecretBinding` (REST update type `setSecretBinding`).
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

| Property name        | Description                              | Default value                                                                       | Required |
|----------------------|------------------------------------------|-------------------------------------------------------------------------------------|----------|
| `partition-count`    | The number of partitions for the topic.  | if not specified, will use the `num.partitions` property in the broker.              | No       |
| `replication-factor` | The number of replications for the topic | if not specified, will use the `default.replication.factor` property in the broker. | No       |

Pass other topic configurations to the topic properties. Refer to [Topic Configs](https://kafka.apache.org/39/documentation.html#topicconfigs) for more details.

### Topic Operations

Refer to [Topic operation](./manage-messaging-metadata-using-gravitino.md#topic-operations) for more details.
