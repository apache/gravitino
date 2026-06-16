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

## Getting Started

### Prerequisites

Place the Iceberg Flink runtime JAR and the Gravitino Flink connector runtime JAR in the `lib` directory of your Flink installation.

Flink clients use a different Iceberg version than the Gravitino server (1.11.0). Use the table below to choose the correct JARs for your Flink version.

| Flink version | Scala | Iceberg version | Iceberg client runtime artifact        | Gravitino connector runtime artifact                                   |
|---------------|-------|-----------------|----------------------------------------|------------------------------------------------------------------------|
| 1.18          | 2.12  | 1.9.2           | `iceberg-flink-runtime-1.18-1.9.2.jar` | `gravitino-flink-connector-runtime-1.18_2.12-${gravitino-version}.jar` |
| 1.19          | 2.12  | 1.10.2          | `iceberg-flink-runtime-1.19-1.10.2.jar` | `gravitino-flink-connector-runtime-1.19_2.12-${gravitino-version}.jar` |
| 1.20          | 2.12  | 1.11.0          | `iceberg-flink-runtime-1.20-1.11.0.jar` | `gravitino-flink-connector-runtime-1.20_2.12-${gravitino-version}.jar` |

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


| Gravitino catalog property name | Flink Iceberg connector configuration | Description                                                                                                   | Since Version    |
|---------------------------------|---------------------------------------|---------------------------------------------------------------------------------------------------------------|------------------|
| `catalog-backend`               | `catalog-type`                        | Catalog backend type, currently, `Hive` and `REST` catalogs are supported, and `JDBC` is in continuous validation | 0.8.0-incubating |
| `uri`                           | `uri`                                 | Catalog backend URI                                                                                           | 0.8.0-incubating |
| `warehouse`                     | `warehouse`                           | Catalog backend warehouse                                                                                     | 0.8.0-incubating |
| `io-impl`                       | `io-impl`                             | The IO implementation for `FileIO` in Iceberg.                                                                | 0.8.0-incubating |
| `oss-endpoint`                  | `oss.endpoint`                        | The endpoint of Aliyun OSS service.                                                                           | 0.8.0-incubating |
| `oss-access-key-id`             | `client.access-key-id`                | The static access key ID used to access OSS data.                                                             | 0.8.0-incubating |
| `oss-secret-access-key`         | `client.access-key-secret`            | The static secret access key used to access OSS data.                                                         | 0.8.0-incubating |

Gravitino catalog property names with the prefix `flink.bypass.` are passed to Flink iceberg connector. For example, using `flink.bypass.clients` to pass the `clients` to the Flink iceberg connector.

## Storage

### OSS

Additionally, you need download the [Aliyun OSS SDK](https://gosspublic.alicdn.com/sdks/java/aliyun_java_sdk_3.10.2.zip), and copy `aliyun-sdk-oss-3.10.2.jar`, `hamcrest-core-1.1.jar`, `jdom2-2.0.6.jar` to the Flink classpath.
