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

## Generic table support

Flink generic tables are non-Hive tables. Their schema and partition keys are stored in table
properties in Hive metastore. Gravitino Flink connector follows the Flink Hive catalog behavior:

- If `connector=hive`, the table is treated as a Hive table and stored with a normal Hive schema.
- If the connector is missing or not `hive`, the table is treated as a generic table. Gravitino
  stores an empty Hive schema and serializes schema and partition keys into `flink.*` properties.
  It also uses the `is_generic` flag when needed for compatibility.

When loading or altering a table, Gravitino Flink connector detects generic tables by the
`is_generic`, `flink.connector`, and `flink.connector.type` properties. Generic tables are
reconstructed from the serialized `flink.*` properties. Hive tables continue to use the native
Hive schema.

:::note
You must set `connector=hive` explicitly when creating a raw Hive table. Otherwise, the table is
created as a generic table by default in HiveCatalog. Starting from Apache Flink 1.18,
ManagedTable-related APIs are deprecated, so avoid relying on managed table behavior. Prefer
Hive-compatible tables (use Hive dialect or set `connector=hive`) or external generic tables (set
an explicit `connector`). For details, see the Flink documentation on Hive generic tables:
https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/hive/hive_catalog/#generic-tables
:::

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

// Create a raw hive table, please make sure setting 'connector'='hive'.
CREATE TABLE IF NOT EXISTS employees (
    id INT,
    name STRING,
    dt INT
)
PARTITIONED BY (dt) WITH (
  'connector'='hive'
);

// Create a generic jdbc table
CREATE TABLE IF NOT EXISTS jdbc_table (
  id INT,
  name STRING
) WITH (
  'connector'='jdbc',
  'url'='jdbc:postgresql://127.0.0.1:5432/postgres',
  'table-name'='jdbc_table',
  'username'='xx',
  'password'='xx',
  'driver'='org.postgresql.Driver'
);

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
