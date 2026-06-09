---
title: "Connect Trino to Iceberg REST"
sidebar_label: "Trino"
---

## Introduction

Apache Gravitino exposes an [Iceberg REST catalog](../iceberg-rest-service.md) endpoint that any
Iceberg-compatible engine can connect to directly — without installing a Gravitino-specific
connector plugin. This page describes how to configure Trino to use Gravitino's Iceberg REST
(IRC) endpoint.

:::note
This integration uses the standard Apache Iceberg REST catalog specification. Gravitino enforces
its full access-control model on all IRC requests.
:::

## Prerequisites

- Apache Gravitino running with the Iceberg REST service enabled. See
  [Iceberg REST catalog service](../iceberg-rest-service.md) for setup instructions.
- The Gravitino IRC endpoint is accessible from the Trino coordinator and all workers. The default
  port is `9001`.
- Trino 469 or later recommended.

## Configuration

Create a catalog properties file in your Trino `etc/catalog/` directory. The filename determines
the catalog name in Trino — `gravitino_irc.properties` creates a catalog named `gravitino_irc`.

:::note
The `warehouse` property is managed by the Gravitino IRC server and does not need to be set in
the Trino catalog configuration.
:::

### No Authentication

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://<gravitino-host>:9001/iceberg

# Native S3 filesystem (Trino 430+)
fs.native-s3.enabled=true
s3.region=us-east-1
s3.aws-access-key=<access-key>
s3.aws-secret-key=<secret-key>

# Table defaults
iceberg.file-format=PARQUET
iceberg.compression-codec=ZSTD
```

### Basic Authentication

Requires Trino **481+**. Trino has no native Basic mode for Iceberg REST; pass `Authorization`
via HTTP headers.

```shell
echo -n '<username>:<password>' | base64
```

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://<gravitino-host>:9001/iceberg

# Basic authentication
iceberg.rest-catalog.http-headers=Authorization: Basic <base64-credentials>

# Native S3 filesystem (Trino 430+)
fs.native-s3.enabled=true
s3.region=us-east-1
s3.aws-access-key=<access-key>
s3.aws-secret-key=<secret-key>

# Table defaults
iceberg.file-format=PARQUET
iceberg.compression-codec=ZSTD
```

Replace `<base64-credentials>` with the output of `echo -n '<username>:<password>' | base64`.

### OAuth2 Authentication

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://<gravitino-host>:9001/iceberg

# OAuth2 authentication
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.token=<your-token>

# Native S3 filesystem (Trino 430+)
fs.native-s3.enabled=true
s3.region=us-east-1
s3.aws-access-key=<access-key>
s3.aws-secret-key=<secret-key>

# Table defaults
iceberg.file-format=PARQUET
iceberg.compression-codec=ZSTD
```

See [How to authenticate](../security/how-to-authenticate.md) for Gravitino authentication
configuration options.

:::tip Local development
For local development with MinIO, replace the S3 section with:

```properties
fs.native-s3.enabled=true
s3.endpoint=http://<minio-host>:9000
s3.path-style-access=true
s3.aws-access-key=<minio-access-key>
s3.aws-secret-key=<minio-secret-key>
s3.region=us-east-1
```

See [gravitino-irc-quickstart](https://github.com/markhoerth/gravitino-irc-quickstart) for a
complete local development environment using MinIO.
:::

## Start Trino

Trino is a server process — the catalog is picked up automatically when Trino starts. After
placing `gravitino_irc.properties` in `etc/catalog/`, restart Trino:

```bash
$TRINO_HOME/bin/launcher restart
```

Once Trino is running, connect using the Trino CLI:

```bash
trino --server http://<trino-host>:8080 --catalog gravitino_irc
```

Or connect without specifying a default catalog and qualify queries fully:

```bash
trino --server http://<trino-host>:8080
```

## Examples

Once connected, use the Trino CLI or any Trino-compatible client.

### List Schemas

```sql
SHOW SCHEMAS FROM gravitino_irc;
```

### List Tables

```sql
SHOW TABLES FROM gravitino_irc.<namespace>;
```

### Query a Table

```sql
SELECT * FROM gravitino_irc.<namespace>.<table> LIMIT 10;
```

### Create a Schema

When creating a schema in Trino, a storage location must be specified:

```sql
CREATE SCHEMA gravitino_irc.<namespace>
WITH (location = 's3://<bucket>/<namespace>/');
```

### Create a Table

```sql
CREATE TABLE gravitino_irc.<namespace>.new_table (
  id INTEGER,
  name VARCHAR,
  created_at TIMESTAMP
)
WITH (
  format         = 'PARQUET',
  format_version = 2
);
```

## Known Issues

### `TIMESTAMP WITH TIME ZONE` Values Are Not Adjusted to the Client Session Time Zone

For `TIMESTAMP WITH TIME ZONE` values, Trino does not adjust query results according to the client
session time zone. Unlike Spark and Flink, Trino displays these values based on the stored
timestamp-with-time-zone value.

To convert a `TIMESTAMP WITH TIME ZONE` value to the current client session time zone, use
`at_timezone` together with `current_timezone()`:

```sql
SELECT
  id,
  at_timezone(timestamp_with_timezone_column, current_timezone())
FROM <catalog>.<namespace>.<table>;
```

## Gravitino Connector vs. Iceberg REST

| Feature                  | Gravitino Engine Connector  | Iceberg REST                  |
|:-------------------------|:----------------------------|:------------------------------|
| Engine plugin required   | Yes                         | No                            |
| Gravitino access control | Yes                         | Yes                           |
| Supported engines        | Trino, Spark, Flink, Daft   | Any Iceberg-compatible engine |
| Credential vending       | Varies                      | Yes (S3, GCS, OSS, ADLS)      |

### Trino Identifiers Are Not Treated as Case Sensitive

Trino identifiers are not treated as case sensitive. As a result, metadata names that differ
only by letter case cannot be distinguished. See [Trino identifier
documentation](https://trino.io/docs/current/language/reserved.html#language-identifiers). This
limitation comes from Trino itself and is not specific to Gravitino.

For the best compatibility with Trino:

- Use lowercase metadata names.
- Avoid creating objects whose names differ only by letter case.

## Related

- [Iceberg REST catalog service](../iceberg-rest-service.md)
- [Connect Spark to Iceberg REST](./spark.md)
- [Connect Flink to Iceberg REST](./flink.md)
- [Trino Gravitino connector](../trino-connector/trino-connector.md)
