---
title: "Flink connector paimon catalog"
slug: /flink-connector/flink-catalog-paimon
keyword: flink connector paimon catalog
license: "This software is licensed under the Apache License version 2."
---

This document provides a guide on configuring and using Apache Gravitino Flink connector.
Ths Apache Gravitino Flink connector can be used to access the Paimon catalog
managed by the Gravitino server.

## Capabilities

### Supported Paimon Table Types

* AppendOnly Table

### Supported Operation Types

Supports most DDL and DML operations in Flink SQL, except for the following operations:

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

Higher version like 0.9 or above may be supported but have not been tested fully.

## Getting Started

### Prerequisites

Place the following JAR files into the 'lib' directory of your Flink installation:

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

The Gravitino Flink connector will transform some properties defined in catalog properties
into Flink Paimon connector configurations.

<table>
<thead>
<tr>
  <th>Gravitino catalog property name</th>
  <th>Flink Paimon connector configuration</th>
  <th>Description</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>catalog-backend</tt></td>
  <td><tt>metastore</tt></td>
  <td>
    Catalog backend of Gravitino Paimon catalog.
    `filesystem` is supported.
  </td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>warehouse</tt></td>
  <td><tt>warehouse</tt></td>
  <td>
    The warehouse directory for the catalog.
    For local file systems, this is something like `file:///user/hive/warehouse-paimon/`.
    For HDFS, this is something like `hdfs://namespace/hdfs/path`.
    For S3, this looks like `s3://{bucket-name}/path/`.
    For Aliyun OSS, this looks like `oss://{bucket-name}/path`.
  </td>
  <td>`0.8.0-incubating`</td>
</tr>
</tbody>
</table>

Gravitino catalog property names with the prefix `flink.bypass.` are passed through
to the Flink Paimon connector.
For example, `flink.bypass.clients` will be translated into `clients`
by the Flink Paimon connector.

