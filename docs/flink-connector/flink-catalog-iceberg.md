---
title: "Flink Connector: Iceberg Catalog"
slug: "/flink-connector/flink-catalog-iceberg"
keyword: "flink connector iceberg catalog"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The Apache Gravitino Flink connector can be used to read and write Iceberg tables, with the metadata managed by the Gravitino server.
To enable the Flink connector, you must download the Iceberg Flink runtime JAR and place it in the Flink classpath.

## Capabilities

### DML and DDL Operations

- `CREATE CATALOG`
- `CREATE DATABASE`
- `CREATE TABLE`
- `DROP TABLE`
- `ALTER TABLE`
- `INSERT INTO & OVERWRITE`
- `SELECT`

### Unsupported Operations

- Partition operations
- Metadata tables, like:
  - `{iceberg_catalog}.{iceberg_database}.{iceberg_table}&snapshots`
- Query UDF
- `UPDATE` clause
- `DELETE` clause
- `CREATE TABLE LIKE` clause
- Computed columns, metadata columns and watermarks

## Getting Started

### Prerequisites

Place the Iceberg Flink runtime JAR and the Gravitino Flink connector runtime JAR in the `lib` directory of your Flink installation.

Flink clients use a different Iceberg version than the Gravitino server (1.11.0). Use the table below to choose the correct JARs for your Flink version.

| Flink version | Scala | Iceberg version | Iceberg client runtime artifact         | Gravitino connector runtime artifact                                   |
|---------------|-------|-----------------|-----------------------------------------|------------------------------------------------------------------------|
| 1.19          | 2.12  | 1.10.2          | `iceberg-flink-runtime-1.19-1.10.2.jar` | `gravitino-flink-connector-runtime-1.19_2.12-${gravitino-version}.jar` |
| 1.20          | 2.12  | 1.11.0          | `iceberg-flink-runtime-1.20-1.11.0.jar` | `gravitino-flink-connector-runtime-1.20_2.12-${gravitino-version}.jar` |
| 2.1           | n/a   | 1.11.0          | `iceberg-flink-runtime-2.1-1.11.0.jar`  | `gravitino-flink-connector-runtime-2.1-${gravitino-version}.jar`       |
| 2.2           | n/a   | 1.12.0          | `iceberg-flink-runtime-2.2-1.12.0.jar`  | `gravitino-flink-connector-runtime-2.2-${gravitino-version}.jar`       |

Replace `${gravitino-version}` with your Gravitino release version.

:::caution
Use only the JARs from the matching table row. Mixing Iceberg JARs from different versions on the client classpath is not compatible and may cause runtime errors.
:::

## SQL Example

```sql

-- Suppose iceberg_a is the Iceberg catalog name managed by Gravitino

USE CATALOG iceberg_a;

CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;

CREATE TABLE sample (
    id BIGINT COMMENT 'unique id',
    data STRING NOT NULL
) PARTITIONED BY (data) 
WITH ('format-version'='2');

INSERT INTO sample
VALUES (1, 'A'), (2, 'B');

SELECT * FROM sample WHERE data = 'B';

```

## View

### View Capabilities

- Supports `CREATE VIEW`, `DROP VIEW`, `ALTER VIEW` (rename and replace view definition), list, load, and rename views managed by the underlying Iceberg backend.
- When creating a view, the connector stores the SQL with the `flink` dialect.
- When loading a view, the connector tries the `flink` dialect first, then falls back to the `hive` dialect.
- Multiple SQL representations per view (e.g. also a `spark` dialect) can coexist and are preserved through Gravitino.

### View SQL Example

```sql
USE CATALOG iceberg_a;
USE mydb;

CREATE VIEW order_view AS SELECT id, amount FROM orders WHERE status = 'completed';

SHOW VIEWS;

SELECT * FROM order_view;

DROP VIEW order_view;
```

## Catalog Properties

The Gravitino Flink connector transforms the following properties in a catalog to Flink connector configuration.


| Gravitino catalog property name | Flink Iceberg connector configuration | Description                                                                                                       |
|---------------------------------|---------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| `catalog-backend`               | `catalog-type`                        | Catalog backend type, currently, `Hive` and `REST` catalogs are supported, and `JDBC` is in continuous validation |
| `uri`                           | `uri`                                 | Catalog backend URI                                                                                               |
| `warehouse`                     | `warehouse`                           | Catalog backend warehouse                                                                                         |
| `io-impl`                       | `io-impl`                             | The IO implementation for `FileIO` in Iceberg.                                                                    |
| `oss-endpoint`                  | `oss.endpoint`                        | The endpoint of Aliyun OSS service.                                                                               |
| `oss-access-key-id`             | `client.access-key-id`                | The static access key ID used to access OSS data.                                                                 |
| `oss-secret-access-key`         | `client.access-key-secret`            | The static secret access key used to access OSS data.                                                             |

Gravitino catalog property names with the prefix `flink.bypass.` are passed to Flink iceberg connector. For example, using `flink.bypass.clients` to pass the `clients` to the Flink iceberg connector.

## Storage

### Credential Vending

Unless the catalog uses a `REST` backend with `data-access` set to `vended-credentials`, the Flink connector fetches catalog-level credentials from Gravitino once, when the catalog is opened, and never refreshes them. Only static credential providers, such as `s3-secret-key`, `oss-secret-key` and `azure-account-key`, are vended to Flink:

- `s3-token`, `gcs-token`, `adls-token`, `oss-token` and `cos-token` vend no credential at catalog level. Flink logs a warning and Iceberg uses the credentials available on the Flink nodes, such as an instance profile.
- `aws-irsa` is rejected when the catalog is loaded, because its credential expires and is never refreshed.

For temporary credentials vended per table and refreshed automatically, use a `REST` backend with `data-access` set to `vended-credentials`.

### OSS

Additionally, download the Aliyun OSS SDK version that matches the Iceberg runtime from the table above
([3.18.5](https://repo1.maven.org/maven2/com/aliyun/oss/aliyun-sdk-oss/3.18.5/aliyun-sdk-oss-3.18.5.jar)
for Iceberg 1.11.0 and 1.12.0, [3.10.2](https://repo1.maven.org/maven2/com/aliyun/oss/aliyun-sdk-oss/3.10.2/aliyun-sdk-oss-3.10.2.jar)
for Iceberg 1.10.2) together with its [jdom2](https://repo1.maven.org/maven2/org/jdom/jdom2/2.0.6.1/jdom2-2.0.6.1.jar)
dependency, and put `aliyun-sdk-oss-<version>.jar` and `jdom2-2.0.6.1.jar` in the Flink `lib` directory.
Iceberg 1.11.0 and later expect `OSS#deleteObject(String, String)` to return `VoidResult`, which is only
true from SDK 3.12.0 onward; older SDKs fail with `NoSuchMethodError` on OSS deletes.
