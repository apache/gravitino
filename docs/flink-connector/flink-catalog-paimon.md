---
title: "Flink Connector: Paimon Catalog"
slug: "/flink-connector/flink-catalog-paimon"
keyword: "flink connector paimon catalog"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

This document provides a comprehensive guide on configuring and using Apache Gravitino Flink connector to access the Paimon catalog managed by the Gravitino server.

## Capabilities

### Paimon Table Types

* AppendOnly Table
* Primary Key Table (with bucket distribution)

### Distribution

* HASH distribution via `bucket-key` and `bucket` table properties.
* Only HASH strategy is supported. Range or other strategies are not applicable.
* When `bucket-key` is specified without `bucket`, the bucket number defaults to auto.

### Operation Types

Supports most DDL and DML operations in Flink SQL, except such operations:

- Function operations
- Partition operations
- Querying UDF
- `LOAD` clause
- `UNLOAD` clause
- `CREATE TABLE LIKE` clause
- `TRUCATE TABLE` clause
- `UPDATE` clause
- `DELETE` clause
- `CALL` clause

## Prerequisites

* Paimon 1.2.0 is fully tested.

Other Paimon versions may also work but have not been tested fully.

## Getting Started

### Prerequisites

Place the following JAR files in the lib directory of your Flink installation:

- The Paimon Flink connector JAR that matches your Flink minor version
- The Gravitino Flink connector runtime JAR that matches your Flink minor version

| Flink version | Paimon connector artifact | Gravitino runtime artifact |
|---------------|---------------------------|----------------------------|
| 1.18          | `paimon-flink-1.18-${paimon-version}.jar` | `gravitino-flink-connector-runtime-1.18_2.12-${gravitino-version}.jar` |
| 1.19          | `paimon-flink-1.19-${paimon-version}.jar` | `gravitino-flink-connector-runtime-1.19_2.12-${gravitino-version}.jar` |
| 1.20          | `paimon-flink-1.20-${paimon-version}.jar` | `gravitino-flink-connector-runtime-1.20_2.12-${gravitino-version}.jar` |

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

#### Distribution Example

```sql
-- Create a primary key table with HASH distribution on the 'id' column with 4 buckets
-- The distribution metadata is persisted in Gravitino and can be verified via the Gravitino API or client.
CREATE TABLE paimon_bucketed_table (
    id BIGINT,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket-key' = 'id',
    'bucket' = '4'
);
```

## View

### View Capabilities

- Supports `CREATE VIEW`, `DROP VIEW`, `ALTER VIEW` (rename and replace view definition), list, load, and rename views stored in the Paimon catalog.
- When creating a view, the connector stores two SQL representations: one with the `flink` dialect and one with the `query` dialect (Paimon's canonical dialect), both using the same expanded SQL text.
- When loading a view, the connector tries dialects in order: `flink` → `hive` → `query`. The first available representation wins.
- View support depends on the selected Paimon backend; not all backends implement the Paimon view API.

### View SQL Example

```sql
USE CATALOG paimon_a;
USE mydb;

CREATE VIEW summary_view AS SELECT category, SUM(amount) AS total FROM orders GROUP BY category;

SHOW VIEWS;

SELECT * FROM summary_view;

DROP VIEW summary_view;
```

## Catalog Properties

Gravitino Flink connector will transform below property names which are defined in catalog properties to Flink Paimon connector configuration.

| Gravitino catalog property name | Flink Paimon connector configuration | Description                                                                                                                                                                                                | Since Version    |
|---------------------------------|--------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|
| `catalog-backend`               | `metastore`                          | Catalog backend of Gravitino Paimon catalog. Supports `filesystem`.                                                                                                                                        | 0.8.0-incubating |
| `warehouse`                     | `warehouse`                          | Warehouse directory of catalog. `file:///user/hive/warehouse-paimon/` for local fs, `hdfs://namespace/hdfs/path` for HDFS , `s3://{bucket-name}/path/` for S3 or `oss://{bucket-name}/path` for Aliyun OSS | 0.8.0-incubating |

Gravitino catalog property names with the prefix `flink.bypass.` are passed to Flink Paimon connector. For example, using `flink.bypass.clients` to pass the `clients` to the Flink Paimon connector.
