---
title: "Spark connector Paimon catalog"
slug: /spark-connector/spark-catalog-paimon
keyword: spark connector paimon catalog
license: "This software is licensed under the Apache License version 2."
---

The Apache Gravitino Spark connector enables users to read and write Paimon tables
with their metadata managed by the Gravitino server.
To enable using Paimon catalogs via the Spark connector, you need to download
the [Paimon Spark runtime JAR](https://paimon.apache.org/docs/0.8/spark/quick-start/#preparation)
into the Spark class path.

## Capabilities

### Supported DDL and DML operations

- `CREATE NAMESPACE`
- `DROP NAMESPACE`
- `LIST NAMESPACE`
- `LOAD NAMESPACE`
  - The connector doesn't return any user-defined configs,
     as we only support FilesystemCatalog at the moment.
- `CREATE TABLE`
  - Distribution and sort orders are not supported yet.
- `DROP TABLE`
- `ALTER TABLE`
- `LIST TABLE`
- `DESRICE TABLE`
- `SELECT`
- `INSERT INTO & OVERWRITE`
- `Schema Evolution`
- Partition management such as `LIST PARTITIONS`, `ALTER TABLE ... DROP PARTITION ...`

:::info
The connector only supports Paimon FilesystemCatalog on HDFS now.
:::

#### Unupported operations

- `ALTER NAMESPACE`
- Row-level operations such as `MERGE INTO`, `DELETE`, `UPDATE`, `TRUNCATE`
- Metadata tables such as `<paimon-catalog>.<paimon-database>.<paimon-table>$snapshots`
- Other Paimon extension SQLs, such as `Tag`
- Call Statements
- Views
- Time travel
- Hive and JDBC backend, or object storage for FilesystemCatalog

## SQL example

```sql
-- Suppose paimon_catalog is a Paimon catalog managed by Gravitino
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

The Gravitino Spark connector transforms the properties defined in catalog properties
into Spark Paimon connector configurations.

<table>
<thead>
<tr>
  <th>Gravitino catalog property name</th>
  <th>Spark Paimon connector configuration</th>
  <th>Description</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>catalog-backend</tt></td>
  <td><tt>metastore</tt></td>
  <td>Catalog backend type</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>uri</tt></td>
  <td><tt>uri</tt></td>
  <td>Catalog backend URI</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>warehouse</tt></td>
  <td><tt>warehouse</tt></td>
  <td>Catalog backend warehouse</td>
  <td>`0.8.0-incubating`</td>
</tr>
</tbody>
</table>

Gravitino catalog property names prefixed with `spark.bypass.` are passed to the Spark Paimon connector.
For example, `spark.bypass.client-pool-size` is tranformed to `client-pool-size`
for the Spark Paimon connector.

