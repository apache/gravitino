---
title: "Flink connector Iceberg catalog"
slug: /flink-connector/flink-catalog-iceberg
keyword: flink connector iceberg catalog
license: "This software is licensed under the Apache License version 2."
---

The Apache Gravitino Flink connector can be used to read and write Iceberg tables, with the metadata managed by the Gravitino server.
To enable the Flink connector, you must download the Iceberg Flink runtime JAR and place it in the Flink classpath.

## Capabilities

#### Supported DML and DDL operations:

- `CREATE CATALOG`
- `CREATE DATABASE`
- `CREATE TABLE`
- `DROP TABLE`
- `ALTER TABLE`
- `INSERT INTO & OVERWRITE`
- `SELECT`

#### Operations not supported:

- Partition operations
- View operations
- Metadata tables, like:
  - `{iceberg_catalog}.{iceberg_database}.{iceberg_table}&snapshots`
- Query UDF
- `UPDATE` clause
- `DELETE` clause
- `CREATE TABLE LIKE` clause

## SQL example

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

## Catalog properties

The Gravitino Flink connector transforms the following properties in a catalog to Flink connector configuration.


| Gravitino catalog property name | Flink Iceberg connector configuration | Description                                                                                                   | Since Version    |
|---------------------------------|---------------------------------------|---------------------------------------------------------------------------------------------------------------|------------------|
| `catalog-backend`               | `catalog-type`                        | Catalog backend type, currently, only `Hive` Catalog is supported, `JDBC` and `Rest` in Continuous Validation | 0.8.0-incubating |
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
