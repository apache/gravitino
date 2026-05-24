---
title: "Flink connector Iceberg catalog"
slug: /flink-connector/flink-catalog-iceberg
keyword: flink connector iceberg catalog
license: "This software is licensed under the Apache License version 2."
---

The Apache Gravitino Flink connector can be used to read and write Iceberg tables, with the metadata managed by the Gravitino server.
To enable the Flink connector, you must download the Iceberg Flink runtime JAR and place it in the Flink classpath.

## Capabilities

#### Supported DML and DDL Operations

- `CREATE CATALOG`
- `CREATE DATABASE`
- `CREATE TABLE`
- `DROP TABLE`
- `ALTER TABLE`
- `INSERT INTO & OVERWRITE`
- `SELECT`

#### Unsupported Operations

- Partition operations
- View operations
- Metadata tables, like:
  - `{iceberg_catalog}.{iceberg_database}.{iceberg_table}&snapshots`
- Query UDF
- `UPDATE` clause
- `DELETE` clause
- `CREATE TABLE LIKE` clause

## Getting Started

### Prerequisites

Place the following JAR files in the lib directory of your Flink installation:

- The Iceberg Flink runtime JAR that matches your Flink minor version
- The Gravitino Flink connector runtime JAR that matches your Flink minor version

| Flink version | Iceberg runtime artifact | Gravitino runtime artifact |
|---------------|--------------------------|----------------------------|
| 1.18          | `iceberg-flink-runtime-1.18-${iceberg-version}.jar` | `gravitino-flink-connector-runtime-1.18_2.12-${gravitino-version}.jar` |
| 1.19          | `iceberg-flink-runtime-1.19-${iceberg-version}.jar` | `gravitino-flink-connector-runtime-1.19_2.12-${gravitino-version}.jar` |
| 1.20          | `iceberg-flink-runtime-1.20-${iceberg-version}.jar` | `gravitino-flink-connector-runtime-1.20_2.12-${gravitino-version}.jar` |

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
