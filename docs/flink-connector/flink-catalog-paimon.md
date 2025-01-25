---
title: "Flink connector paimon catalog"
slug: /flink-connector/flink-catalog-paimon
keyword: flink connector paimon catalog
license: "This software is licensed under the Apache License version 2."
---

This document provides a comprehensive guide on configuring and using Apache Gravitino Flink connector to access the Paimon catalog managed by the Gravitino server.

## Capabilities

### Supported Paimon Table Types

* AppendOnly Table

### Supported Operation Types

Supports most DDL and DML operations in Flink SQL, except such operations:

- Function operations
- Partition operations
- View operations
- Querying UDF
- `LOAD` clause
- `UNLOAD` clause
- `CREATE TABLE LIKE` clause
- `TRUCATE TABLE` clause
- `UPDATE` clause
- `DELETE` clause
- `CALL` clause

## Requirement

* Paimon 0.8

Higher version like 0.9 or above may also support but have not been tested fully.

## Getting Started

### Prerequisites

Place the following JAR files in the lib directory of your Flink installation:

- `paimon-flink-1.18-${paimon-version}.jar`
- `gravitino-flink-connector-runtime-1.18_2.12-${gravitino-version}.jar`

### SQL Example

```sql

-- Suppose paimon_catalog is the Paimon catalog name managed by Gravitino
USE CATALOG paimon_catalog;
-- Execute statement succeed.

SHOW DATABASES;
-- +---------------------+
-- |       database name |
-- +---------------------+
-- |             default |
-- | gravitino_paimon_db |
-- +---------------------+

SET 'execution.runtime-mode' = 'batch';
-- [INFO] Execute statement succeed.

SET 'sql-client.execution.result-mode' = 'tableau';
-- [INFO] Execute statement succeed.

CREATE TABLE paimon_table_a (
    aa BIGINT,
    bb BIGINT
);

SHOW TABLES;
-- +----------------+
-- |     table name |
-- +----------------+
-- | paimon_table_a |
-- +----------------+


SELECT * FROM paimon_table_a;
-- Empty set

INSERT INTO paimon_table_a(aa,bb) VALUES(1,2);
-- [INFO] Submitting SQL update statement to the cluster...
-- [INFO] SQL update statement has been successfully submitted to the cluster:
-- Job ID: 74c0c678124f7b452daf08c399d0fee2

SELECT * FROM paimon_table_a;
-- +----+----+
-- | aa | bb |
-- +----+----+
-- |  1 |  2 |
-- +----+----+
-- 1 row in set
```

## Catalog properties

Gravitino Flink connector will transform below property names which are defined in catalog properties to Flink Paimon connector configuration.

| Gravitino catalog property name | Flink Paimon connector configuration | Description                                                                                                                                                                                                | Since Version    |
|---------------------------------|--------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|
| `catalog-backend`               | `metastore`                          | Catalog backend of Gravitino Paimon catalog. Supports `filesystem`.                                                                                                                                        | 0.8.0-incubating |
| `warehouse`                     | `warehouse`                          | Warehouse directory of catalog. `file:///user/hive/warehouse-paimon/` for local fs, `hdfs://namespace/hdfs/path` for HDFS , `s3://{bucket-name}/path/` for S3 or `oss://{bucket-name}/path` for Aliyun OSS | 0.8.0-incubating |

Gravitino catalog property names with the prefix `flink.bypass.` are passed to Flink Paimon connector. For example, using `flink.bypass.clients` to pass the `clients` to the Flink Paimon connector.
