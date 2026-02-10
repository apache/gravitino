---
title: "Flink connector hive catalog"
slug: /flink-connector/flink-catalog-hive
keyword: flink connector hive catalog
license: "This software is licensed under the Apache License version 2."
---

With the Apache Gravitino Flink connector, accessing data or managing metadata in Hive catalogs becomes straightforward, enabling seamless federation queries across different Hive catalogs.

## Capabilities

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

* Hive metastore 2.x
* HDFS 2.x or 3.x

## SQL example


```sql

// Suppose hive_a is the Hive catalog name managed by Gravitino
USE CATALOG hive_a;

CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;

SET 'execution.runtime-mode' = 'batch';
-- [INFO] Execute statement succeed.

SET 'sql-client.execution.result-mode' = 'tableau';
-- [INFO] Execute statement succeed.

// Create table
CREATE TABLE IF NOT EXISTS employees (
    id INT,
    name STRING,
    dt INT
)
PARTITIONED BY (dt);

DESC EXTENDED employees;

INSERT INTO employees VALUES (1, 'John Doe', 20240101), (2, 'Jane Smith', 20240101);
SELECT * FROM employees WHERE dt = 20240101;
```

## Catalog properties

The configuration of Flink Hive Connector is the same with the original Flink Hive connector.
Gravitino catalog property names with the prefix `flink.bypass.` are passed to Flink Hive connector. For example, using `flink.bypass.hive-conf-dir` to pass the `hive-conf-dir` to the Flink Hive connector.
The validated catalog properties are listed below. Any other properties with the prefix `flink.bypass.` in Gravitino Catalog will be ignored by Gravitino Flink Connector.

| Property name in Gravitino catalog properties | Flink Hive connector configuration | Description           | Since Version    |
|-----------------------------------------------|------------------------------------|-----------------------|------------------|
| `flink.bypass.default-database`               | `default-database`                 | Hive default database | 0.6.0-incubating |
| `flink.bypass.hive-conf-dir`                  | `hive-conf-dir`                    | Hive conf dir         | 0.6.0-incubating |
| `flink.bypass.hive-version`                   | `hive-version`                     | Hive version          | 0.6.0-incubating |
| `flink.bypass.hadoop-conf-dir`                | `hadoop-conf-dir`                  | Hadoop conf dir       | 0.6.0-incubating |
| `metastore.uris`                              | `hive.metastore.uris`              | Hive metastore uri    | 0.6.0-incubating |

:::caution
You can set other hadoop properties (with the prefix `hadoop.`, `dfs.`, `fs.`, `hive.`) in Gravitino Catalog properties. If so, it will override
the configuration from the `hive-conf-dir` and `hadoop-conf-dir`.
:::