---
title: Connect Flink via Iceberg REST
sidebar_label: Flink
---

# Connecting Apache Flink via Iceberg REST

Apache Gravitino exposes an [Iceberg REST catalog](../iceberg-rest-service.md) endpoint that any
Iceberg-compatible engine can connect to directly — without installing a Gravitino-specific
connector plugin. This page describes how to configure Apache Flink to use Gravitino's Iceberg REST
(IRC) endpoint.

:::note
This integration uses the standard Apache Iceberg REST catalog specification. Gravitino enforces
its full access-control model on all IRC requests.
:::

## Prerequisites

- Apache Gravitino running with the Iceberg REST service enabled. See
  [Iceberg REST catalog service](../iceberg-rest-service.md) for setup instructions.
- The Gravitino IRC endpoint is accessible from your Flink environment. The default port is `9001`.
- The following JAR files on your Flink classpath:
  - `iceberg-flink-runtime-1.18-1.7.1.jar` (or `iceberg-flink-runtime-1.19-1.7.1.jar` for Flink 1.19)
  - `iceberg-aws-bundle-1.7.1.jar`
  - `flink-shaded-hadoop-2-uber.jar`

This page uses **Flink 1.18** with **Iceberg 1.7.1**.

:::note
Unlike Spark and Trino, Flink requires S3 connection properties to be specified in the catalog
definition itself rather than in a separate configuration file.
:::

## Configuration

Flink uses a cluster configuration file (`flink-conf.yaml`) for general settings. The Iceberg
catalog is registered per-session using a `CREATE CATALOG` SQL statement.

### flink-conf.yaml

Set batch execution mode and result display in `$FLINK_HOME/conf/flink-conf.yaml`:

```yaml
execution.runtime-mode: batch
sql-client.execution.result-mode: tableau
```

:::tip
`tableau` mode prints query results inline in the terminal. Without it, Flink SQL Client opens
results in a full-screen pager.
:::

### Starting the Flink SQL Client

```bash
$FLINK_HOME/bin/sql-client.sh
```

## Registering the catalog

At the Flink SQL Client prompt, run the following `CREATE CATALOG` statement. Replace
`<gravitino-host>` with your Gravitino server address and supply your S3 credentials.

### Without authentication

```sql
CREATE CATALOG gravitino_irc WITH (
  'type'                 = 'iceberg',
  'catalog-type'         = 'rest',
  'uri'                  = 'http://<gravitino-host>:9001/iceberg',
  'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.region'            = 'us-east-1',
  's3.access-key-id'     = '<access-key>',
  's3.secret-access-key' = '<secret-key>'
);
```

### With OAuth2 authentication

```sql
CREATE CATALOG gravitino_irc WITH (
  'type'                 = 'iceberg',
  'catalog-type'         = 'rest',
  'uri'                  = 'http://<gravitino-host>:9001/iceberg',
  'rest.auth.type'       = 'oauth2',
  'rest.auth.oauth2.token' = '<your-token>',
  'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.region'            = 'us-east-1',
  's3.access-key-id'     = '<access-key>',
  's3.secret-access-key' = '<secret-key>'
);
```

See [How to authenticate](../security/how-to-authenticate.md) for Gravitino authentication
configuration options.

:::note
The catalog registration persists for the duration of the SQL Client session. You must re-run
`CREATE CATALOG` each time you start a new session.
:::

:::tip Local development
For local development with MinIO, add the following S3 properties to the catalog definition:

```sql
  's3.endpoint'          = 'http://<minio-host>:9000',
  's3.path-style-access' = 'true',
```

See [gravitino-irc-quickstart](https://github.com/markhoerth/gravitino-irc-quickstart) for a
complete local development environment using MinIO.
:::

## Usage examples

### Use the catalog

```sql
USE CATALOG gravitino_irc;
```

### List databases

```sql
SHOW DATABASES;
```

### List tables

```sql
SHOW TABLES IN <namespace>;
```

### Query a table

```sql
SELECT * FROM <namespace>.<table>;
```

### Create a table

```sql
CREATE TABLE gravitino_irc.<namespace>.new_table (
  id INT,
  name STRING,
  created_at TIMESTAMP
);
```

### Insert data

```sql
INSERT INTO gravitino_irc.<namespace>.new_table VALUES (1, 'example', CURRENT_TIMESTAMP);
```

## Gravitino connector vs Iceberg REST

| | Gravitino Engine Connector | Iceberg REST |
|---|---|---|
| Engine plugin required | Yes | No |
| Gravitino access control | Yes | Yes |
| Supported engines | Trino, Spark, Flink, Daft | Any Iceberg-compatible engine |
| Credential vending | Varies | Yes (S3, GCS, OSS, ADLS) |

## Related

- [Iceberg REST catalog service](../iceberg-rest-service.md)
- [Connect Spark via Iceberg REST](./spark.md)
- [Connect Trino via Iceberg REST](./trino.md)
- [Flink Gravitino connector](../flink-connector/flink-connector.md)
