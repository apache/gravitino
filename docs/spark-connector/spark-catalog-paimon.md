---
title: "Spark connector Paimon catalog"
slug: /spark-connector/spark-catalog-paimon
keyword: spark connector paimon catalog
license: "This software is licensed under the Apache License version 2."
---

The Apache Gravitino Spark connector offers the capability to read and write Paimon tables, with the metadata managed by the Gravitino server.

## Preparation

1. Set `spark.sql.gravitino.enablePaimonSupport` to `true` in Spark configuration.
2. Download Paimon Spark runtime jar to Spark classpath.

## Capabilities

### Support DDL and DML operations:

- `CREATE NAMESPACE`
- `DROP NAMESPACE`
- `LIST NAMESPACE`
- `LOAD NAMESPACE`
  - It can not return any user-specified configs now, as we only support FilesystemCatalog in spark-connector now.
- `CREATE TABLE`
  - Doesn't support distribution and sort orders.
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

#### Not supported operations:

- `ALTER NAMESPACE`
  - Paimon does not support alter namespace.
- Row Level operations, such as `MERGE INTO`, `DELETE`, `UPDATE`, `TRUNCATE`
- Metadata tables, such as `{paimon_catalog}.{paimon_database}.{paimon_table}$snapshots`
- Other Paimon extension SQLs, such as `Tag`
- Call Statements
- View
- Time Travel
- Hive and Jdbc backend, and Object Storage for FilesystemCatalog

## SQL example

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

## Catalog properties

Gravitino spark connector will transform below property names which are defined in catalog properties to Spark Paimon connector configuration.

| Gravitino catalog property name | Spark Paimon connector configuration | Description                                                                                                                                                                                                         | Since Version     |
|---------------------------------|--------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------|
| `catalog-backend`               | `metastore`                          | Catalog backend type                                                                                                                                                                                                | 0.8.0-incubating  |
| `uri`                           | `uri`                                | Catalog backend uri                                                                                                                                                                                                 | 0.8.0-incubating  |
| `warehouse`                     | `warehouse`                          | Catalog backend warehouse                                                                                                                                                                                           | 0.8.0-incubating  |

Gravitino catalog property names with the prefix `spark.bypass.` are passed to Spark Paimon connector. For example, using `spark.bypass.client-pool-size` to pass the `client-pool-size` to the Spark Paimon connector.
