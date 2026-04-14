---
title: Connect Doris via Iceberg REST
sidebar_label: Doris
---

# Connecting Apache Doris via Iceberg REST

Apache Gravitino exposes an [Iceberg REST catalog](../iceberg-rest-service.md) endpoint that any
Iceberg-compatible engine can connect to directly. This page describes how to configure Apache Doris
to use Gravitino's Iceberg REST (IRC) endpoint.

## Prerequisites

- Apache Gravitino running with the Iceberg REST service enabled. See
  [Iceberg REST catalog service](../iceberg-rest-service.md) for setup instructions.
- The Gravitino IRC endpoint is accessible from your Doris environment. The default port is `9001`.

## Configuration

Create an Iceberg catalog in Doris pointing at the Gravitino IRC endpoint:

```sql
CREATE CATALOG iceberg PROPERTIES (
    "uri"                  = "http://<gravitino-host>:9001/iceberg/",
    "type"                 = "iceberg",
    "iceberg.catalog.type" = "rest",
    "s3.endpoint"          = "http://s3.<region>.amazonaws.com",
    "s3.region"            = "<region>",
    "s3.access_key"        = "<access-key>",
    "s3.secret_key"        = "<secret-key>"
);
```

## Usage examples

```sql
SWITCH iceberg;
CREATE DATABASE db;
USE db;
CREATE TABLE t(a int);
INSERT INTO t VALUES (1);
SELECT * FROM t;
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
