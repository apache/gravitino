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
| `gravitino.bypass.` | Property name with this prefix passed down to the underlying Kafka Admin client for use. (refer to [Kafka Admin Configs](https://kafka.apache.org/39/configuration/admin-configs/) for more details) | (none)        | No       |

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

The server start script `bin/gravitino.sh` also sets the Kafka client JVM properties
`org.apache.kafka.automatic.config.providers=none` and an empty
`org.apache.kafka.sasl.oauthbearer.allowed.urls`. Kafka's OAUTHBEARER login callback handler,
`org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler`, fetches a token from
`sasl.oauthbearer.token.endpoint.url` and refuses any URL that is not in the allow list. The legacy
class name `org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler`
is a subclass and behaves the same. With the empty list, a Kafka catalog cannot make the server read
a `file://` URL or call an internal HTTP endpoint through that property. A catalog that uses either
class needs its token endpoint URL in the list, for example
`-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=https://idp.example.com/oauth2/token`. Login
handlers that create the token themselves, such as the AWS MSK IAM handler, do not read the token
endpoint and work with the empty list. The script sets these as defaults: a value set in
`JAVA_OPTS`, `JAVA_TOOL_OPTIONS`, `JDK_JAVA_OPTIONS` or `_JAVA_OPTIONS` replaces them.

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

Pass other topic configurations to the topic properties. Refer to [Topic-Level Configs](https://kafka.apache.org/39/configuration/topic-level-configs/) for more details.

### Topic Operations

Refer to [Topic operation](./manage-messaging-metadata-using-gravitino.md#topic-operations) for more details.
