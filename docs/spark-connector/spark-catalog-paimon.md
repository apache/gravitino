---
title: "Spark Connector: Paimon Catalog"
slug: "/spark-connector/spark-catalog-paimon"
keyword: "spark connector paimon catalog"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The Apache Gravitino Spark connector offers the capability to read and write Paimon tables, with the metadata managed by the Gravitino server.

## Preparation

1. Set `spark.sql.gravitino.enablePaimonSupport` to `true` in Spark configuration.
2. Download Paimon Spark runtime jar to Spark classpath.

:::info
The Paimon catalog is available on Spark 3.5 only. Paimon first published `paimon-spark-4.0` in
Paimon 1.3.0, above the version Gravitino currently depends on, so the Spark 4 connector builds
without the Paimon classes.
:::

## Capabilities

### DDL and DML Operations

- `CREATE NAMESPACE`
- `DROP NAMESPACE`
- `LIST NAMESPACE`
- `LOAD NAMESPACE`
  - Returning user-specified configs is not supported; the spark-connector currently only supports FilesystemCatalog.
- `CREATE TABLE`
  - Supports primary keys and bucket options through `TBLPROPERTIES`.
  - Supports fixed buckets through `CLUSTERED BY ... INTO ... BUCKETS`.
  - Doesn't support sort orders.
- `DROP TABLE`
- `ALTER TABLE`
- `LIST TABLE`
- `DESRICE TABLE`
- `SELECT`
- `INSERT INTO & OVERWRITE`
- `Schema Evolution`
- `PARTITION MANAGEMENT`, such as `LIST PARTITIONS`, `ALTER TABLE ... DROP PARTITION ...`

:::info
Only supports Paimon FilesystemCatalog on HDFS now.
:::

#### Unsupported Operations

- `ALTER NAMESPACE`
  - Paimon does not support alter namespace.
- Row Level operations, such as `MERGE INTO`, `DELETE`, `UPDATE`, `TRUNCATE`
- Metadata tables, such as `{paimon_catalog}.{paimon_database}.{paimon_table}$snapshots`
- Other Paimon extension SQLs, such as `Tag`
- Call Statements
- View
- Time Travel
- Hive and Jdbc backend, and Object Storage for FilesystemCatalog

## SQL Example

```sql
-- Suppose paimon_catalog is the Paimon catalog name managed by Gravitino
USE paimon_catalog;

CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;

CREATE TABLE IF NOT EXISTS employee (
  id bigint,
  name string,
  department string,
  hire_date timestamp
) PARTITIONED BY (name);

SHOW TABLES;
DESC TABLE EXTENDED employee;

INSERT INTO employee
VALUES
(1, 'Alice', 'Engineering', TIMESTAMP '2021-01-01 09:00:00'),
(2, 'Bob', 'Marketing', TIMESTAMP '2021-02-01 10:30:00'),
(3, 'Charlie', 'Sales', TIMESTAMP '2021-03-01 08:45:00');

SELECT * FROM employee WHERE name = 'Alice';

SHOW PARTITIONS employee;
ALTER TABLE employee DROP PARTITION (`name`='Alice');
```

## Creating primary-key tables

Specify Paimon primary keys and buckets in `TBLPROPERTIES`. For example, this creates a table with a composite primary key and dynamic buckets:

```sql
CREATE TABLE source_events (
  source STRING NOT NULL,
  event_id BIGINT NOT NULL,
  payload STRING,
  dt STRING
)
PARTITIONED BY (dt)
TBLPROPERTIES (
  'primary-key' = 'source,event_id',
  'bucket' = '-1',
  'merge-engine' = 'deduplicate'
);
```

Primary-key column order is preserved, and primary-key columns become non-nullable, matching Paimon's behavior. Other column definitions are preserved. Use `bucket = '-1'` for dynamic buckets without an explicit `bucket-key`. For fixed buckets, specify a positive bucket count. An optional `bucket-key` specifies comma-separated top-level bucket columns. Append tables with fixed buckets require explicit bucket keys.

Column names must exist and must not be repeated. Bucket keys must not include partition columns. For primary-key tables, explicit bucket keys must be a subset of the primary-key columns, and fixed buckets require the primary key to include all partition columns.

Use `PARTITIONED BY` for identity partitions. Fixed buckets can also be specified with `CLUSTERED BY`:

```sql
CREATE TABLE bucketed_events (
  event_id BIGINT,
  payload STRING,
  dt STRING
) USING paimon
PARTITIONED BY (dt)
CLUSTERED BY (event_id) INTO 4 BUCKETS;
```

If `CLUSTERED BY` and `TBLPROPERTIES` both specify a bucket count or bucket keys, their values must match. Bucket columns are represented as Paimon bucket metadata, separately from identity partitions. `SORTED BY` and non-identity partition transforms are not supported.

## Catalog Properties

Gravitino spark connector will transform below property names which are defined in catalog properties to Spark Paimon connector configuration.

| Gravitino catalog property name | Spark Paimon connector configuration | Description               |
|---------------------------------|--------------------------------------|---------------------------|
| `catalog-backend`               | `metastore`                          | Catalog backend type      |
| `uri`                           | `uri`                                | Catalog backend uri       |
| `warehouse`                     | `warehouse`                          | Catalog backend warehouse |

Gravitino catalog property names with the prefix `spark.bypass.` are passed to Spark Paimon connector. For example, using `spark.bypass.client-pool-size` to pass the `client-pool-size` to the Spark Paimon connector.
