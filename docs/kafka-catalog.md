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
| `gravitino.bypass.` | Property name with this prefix passed down to the underlying Kafka Admin client for use. (refer to [Kafka Admin Configs](https://kafka.apache.org/34/documentation.html#adminclientconfigs) for more details) | (none)        | No       |

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

Pass other topic configurations to the topic properties. Refer to [Topic Configs](https://kafka.apache.org/34/documentation.html#topicconfigs) for more details.

### Topic Operations

Refer to [Topic operation](./manage-messaging-metadata-using-gravitino.md#topic-operations) for more details.

### Authorization

Set `authorization-provider` to `kafka` on a Kafka catalog to push Gravitino RBAC privileges and ownership changes directly to KRaft-based Kafka `StandardAuthorizer` (or compatible Kafka broker authorizers) via Kafka `AdminClient`.

#### Authorization Properties

By default, the plugin reuses the catalog's `bootstrap.servers` and `gravitino.bypass.*` settings (for example, SASL, OAuthBearer, or SSL client settings) to connect to Kafka. You can override or customize behavior with the properties below:

| Property Name                                     | Description                                                                                                          | Default Value | Required |
|---------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|---------------|----------|
| `authorization-provider`                          | Set to `kafka` to enable native Kafka ACL authorization push-down.                                                   | (none)        | Yes      |
| `authorization.kafka.bootstrap.servers`           | Optional override for Kafka bootstrap servers used by the ACL admin client (defaults to catalog `bootstrap.servers`). | (none)        | No       |
| `authorization.kafka.admin.*`                     | Kafka `AdminClient` settings (with prefix stripped) overriding `gravitino.bypass.*` for authorization RPCs.         | (none)        | No       |
| `authorization.kafka.user-principal-type`         | Kafka principal type prefix used for Gravitino users (`<type>:<name>`). Wildcard `*` is rejected.                    | `User`        | No       |
| `authorization.kafka.group-principal-type`        | Kafka principal type prefix used for Gravitino groups (`<type>:<name>`) when `authorization.kafka.group-acl.enabled=true`. Wildcard `*` is rejected. | `Group` | No |
| `authorization.kafka.group-acl.enabled`           | Whether Gravitino group role grants and group ownership are pushed to Kafka ACLs. Enable only when the Kafka broker uses a custom group-aware `Authorizer` or `KafkaPrincipalBuilder` (vanilla `StandardAuthorizer` does not expand group membership). | `false` | No |
| `authorization.kafka.consumer-group-pattern`      | Consumer group resource name pattern granted `READ` when `ALLOW CONSUME_TOPIC` or ownership is pushed down.          | `*`           | No       |
| `authorization.kafka.consumer-group-pattern-type` | Consumer group resource pattern type (`LITERAL` or `PREFIXED`).                                                      | `LITERAL`     | No       |
| `authorization.kafka.host`                        | Host filter written to Kafka ACL entries.                                                                            | `*`           | No       |
| `authorization.kafka.idempotent-write-acl.enabled` | Whether `ALLOW PRODUCE_TOPIC` and ownership emit `CLUSTER:IDEMPOTENT_WRITE` (`ALLOW`).                              | `true`        | No       |
| `authorization.kafka.request-timeout-ms`          | Request and default API timeout in milliseconds for Kafka `AdminClient` ACL operations.                              | `30000`       | No       |

::::note Group ACL Requirements
Apache Kafka's built-in `StandardAuthorizer` evaluates only the authenticated request `KafkaPrincipal` (`User:<name>` by default) and wildcard `User:*`; it does not expand group memberships on the broker, and Gravitino does not store user-to-group membership relations. By default (`authorization.kafka.group-acl.enabled=false`), Gravitino logs a warning and skips pushing `Group:<name>` ACLs to Kafka while still persisting Gravitino metadata/ownership changes and updating any user-bound ACLs. Set `authorization.kafka.group-acl.enabled=true` only when your Kafka cluster is configured with a group-aware `Authorizer` or `KafkaPrincipalBuilder` that resolves group principals.
::::

#### Privilege and Ownership Mapping

| Gravitino Privilege / Role    | Scope (`Metalake`/`Catalog`/`Schema` vs `Topic`) | Kafka `AclBinding`s (`ALLOW`)                                                                                         | Kafka `AclBinding`s (`DENY`)   |
|-------------------------------|--------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|--------------------------------|
| `PRODUCE_TOPIC`               | `*` (catalog/schema/metalake) or topic literal   | `TOPIC` (`WRITE`, `DESCRIBE`), plus `CLUSTER:kafka-cluster` (`IDEMPOTENT_WRITE`) if enabled                           | `TOPIC` (`WRITE`)              |
| `CONSUME_TOPIC`               | `*` (catalog/schema/metalake) or topic literal   | `TOPIC` (`READ`, `DESCRIBE`), plus configured `GROUP` (`READ`)                                                        | `TOPIC` (`READ`)               |
| `CREATE_TOPIC`                | `*` (catalog/schema/metalake)                    | `TOPIC:*` (`CREATE`, `DESCRIBE`)                                                                                      | `TOPIC:*` (`CREATE`)           |
| Ownership (`USER`, or `GROUP` when enabled) | `*` (catalog/schema/metalake) or topic literal | `TOPIC` (`READ`, `WRITE`, `CREATE`, `DELETE`, `ALTER`, `DESCRIBE`, `ALTER_CONFIGS`, `DESCRIBE_CONFIGS`), `GROUP` (`READ`), `CLUSTER` (`IDEMPOTENT_WRITE` if enabled) | (none) |

When revoking roles, updating role privileges, transferring ownership, dropping a topic, or dropping the catalog, shared ACLs still required by another active role or ownership on the same principal are preserved, and unmanaged external broker ACLs are left untouched.

#### Operational Notes

- **`DENY` precedence**: Kafka `StandardAuthorizer` evaluates `DENY` before `ALLOW`. In particular, a catalog-, schema-, or metalake-scoped `DENY` (emitted on `TOPIC:*`) overrides topic-literal `ALLOW` grants and topic ownership `ALLOW` ACLs for the same principal on that cluster.
- **Multi-tenant consumer groups**: The default `authorization.kafka.consumer-group-pattern=*` (`LITERAL`) allows any consumer group ID for principals granted `ALLOW CONSUME_TOPIC`. On shared multi-tenant clusters, consider restricting consumer group namespaces with `authorization.kafka.consumer-group-pattern-type=PREFIXED` and a tenant/catalog prefix (for example, `authorization.kafka.consumer-group-pattern=analytics-`).
- **Transactional producers**: Idempotent producers (`enable.idempotence=true`) are covered when `authorization.kafka.idempotent-write-acl.enabled=true`, whereas transactional producers using `transactional.id` additionally require `TRANSACTIONAL_ID` (`WRITE`, `DESCRIBE`) ACLs managed outside Gravitino's topic privilege model.

#### Using with `authorization-chain`

When `authorization-provider` is set to `chain`, `ChainedAuthorizationProperties` forwards only keys under `authorization.chain.<plugin-name>.*` to each chained plugin (stripping `chain.<plugin-name>.` into `authorization.*`). You must configure `authorization.chain.<plugin-name>.kafka.bootstrap.servers` explicitly (along with any `authorization.chain.<plugin-name>.kafka.admin.*` security settings), because catalog-level `bootstrap.servers` and `gravitino.bypass.*` properties are not forwarded by `authorization-chain`:

```properties
authorization-provider=chain
authorization.chain.plugins=kafka-acl
authorization.chain.kafka-acl.provider=kafka
authorization.chain.kafka-acl.kafka.bootstrap.servers=broker1:9093
authorization.chain.kafka-acl.kafka.admin.security.protocol=SASL_SSL
authorization.chain.kafka-acl.kafka.admin.sasl.mechanism=OAUTHBEARER
```
