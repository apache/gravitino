---
title: Connect StarRocks via Iceberg REST
sidebar_label: StarRocks
---

# Connecting StarRocks via Iceberg REST

Apache Gravitino exposes an [Iceberg REST catalog](../iceberg-rest-service.md) endpoint that any
Iceberg-compatible engine can connect to directly. This page describes how to configure StarRocks
to use Gravitino's Iceberg REST (IRC) endpoint.

## Prerequisites

- Apache Gravitino running with the Iceberg REST service enabled. See
  [Iceberg REST catalog service](../iceberg-rest-service.md) for setup instructions.
- The Gravitino IRC endpoint is accessible from your StarRocks environment. The default port is `9001`.

## Configuration

Create an external Iceberg catalog in StarRocks pointing at the Gravitino IRC endpoint:

```sql
CREATE EXTERNAL CATALOG iceberg
COMMENT "Gravitino Iceberg REST catalog"
PROPERTIES
(
  "type"                          = "iceberg",
  "iceberg.catalog.type"          = "rest",
  "iceberg.catalog.uri"           = "http://<gravitino-host>:9001/iceberg",
  "aws.s3.access_key"             = "<access-key>",
  "aws.s3.secret_key"             = "<secret-key>",
  "aws.s3.endpoint"               = "http://<s3-host>:9000",
  "aws.s3.enable_path_style_access" = "true",
  "client.factory"                = "com.starrocks.connector.iceberg.IcebergAwsClientFactory"
);
```

:::note
`client.factory` must be set explicitly for StarRocks to correctly initialize the Iceberg AWS client.
:::

## Usage examples

```sql
SET CATALOG iceberg;
CREATE DATABASE db;
USE db;
CREATE TABLE t(a int);
INSERT INTO t VALUES (1);
SELECT * FROM t;
```

## Gravitino connector vs Iceberg REST

| Feature | Gravitino Engine Connector | Iceberg REST |
|:---|:---|:---|
| Engine plugin required | Yes | No |
| Gravitino access control | Yes | Yes |
| Supported engines | Trino, Spark, Flink, Daft | Any Iceberg-compatible engine |
| Credential vending | Varies | Yes (S3, GCS, OSS, ADLS) |

## Related

- [Iceberg REST catalog service](../iceberg-rest-service.md)
- [Connect Spark via Iceberg REST](./spark.md)
- [Connect Trino via Iceberg REST](./trino.md)
