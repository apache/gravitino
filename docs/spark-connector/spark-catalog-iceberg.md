---
title: "Spark connector Iceberg catalog"
slug: /spark-connector/spark-catalog-iceberg
keyword: spark connector iceberg catalog
license: "This software is licensed under the Apache License version 2."
---

The Apache Gravitino Spark connector offers the capability to read and write Iceberg tables, with the metadata managed by the Gravitino server. To enable the use of the Iceberg catalog within the Spark connector, you must set the configuration `spark.sql.gravitino.enableIcebergSupport` to `true` and download Iceberg Spark runtime jar to Spark classpath.

## Capabilities

#### Support DML and DDL operations:

- `CREATE TABLE`
  - `Supports basic create table clause including table schema, properties, partition, does not support distribution and sort orders.`
- `DROP TABLE`
- `ALTER TABLE`
- `INSERT INTO&OVERWRITE`
- `SELECT`
- `MERGE INTO`
- `DELETE FROM`
- `UPDATE`
- `CALL`
- `TIME TRAVEL QUERY`
- `DESCRIBE TABLE`

#### Not supported operations:

- View operations.
- Metadata tables, like:
  - `{iceberg_catalog}.{iceberg_database}.{iceberg_table}.snapshots`
- Other Iceberg extension SQL, like:
  - `ALTER TABLE prod.db.sample ADD PARTITION FIELD xx`
  - `ALTER TABLE ... WRITE ORDERED BY`
  - `ALTER TABLE prod.db.sample CREATE BRANCH branchName`
  - `ALTER TABLE prod.db.sample CREATE TAG tagName`
- AtomicCreateTableAsSelect&AtomicReplaceTableAsSelect

## SQL example

```sql
-- Suppose iceberg_a is the Iceberg catalog name managed by Gravitino
USE iceberg_a;

CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;

CREATE TABLE IF NOT EXISTS employee (
  id bigint,
  name string,
  department string,
  hire_date timestamp
) USING iceberg
PARTITIONED BY (days(hire_date));
DESC TABLE EXTENDED employee;

INSERT INTO employee
VALUES
(1, 'Alice', 'Engineering', TIMESTAMP '2021-01-01 09:00:00'),
(2, 'Bob', 'Marketing', TIMESTAMP '2021-02-01 10:30:00'),
(3, 'Charlie', 'Sales', TIMESTAMP '2021-03-01 08:45:00');

SELECT * FROM employee WHERE date(hire_date) = '2021-01-01';

UPDATE employee SET department = 'Jenny' WHERE id = 1;

DELETE FROM employee WHERE id < 2;

MERGE INTO employee
USING (SELECT 4 as id, 'David' as name, 'Engineering' as department, TIMESTAMP '2021-04-01 09:00:00' as hire_date) as new_employee
ON employee.id = new_employee.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO employee
USING (SELECT 4 as id, 'David' as name, 'Engineering' as department, TIMESTAMP '2021-04-01 09:00:00' as hire_date) as new_employee
ON employee.id = new_employee.id
WHEN MATCHED THEN DELETE
WHEN NOT MATCHED THEN INSERT *;

-- Suppose that the first snapshotId of employee is 1L and the second snapshotId is 2L
-- Rollback the snapshot for iceberg_a.mydatabase.employee to 1L
CALL iceberg_a.system.rollback_to_snapshot('iceberg_a.mydatabase.employee', 1);
-- Set the snapshot for iceberg_a.mydatabase.employee to 2L
CALL iceberg_a.system.set_current_snapshot('iceberg_a.mydatabase.employee', 2);

-- Suppose that the commit timestamp of the first snapshot is older than '2024-05-27 01:01:00'
-- Time travel to '2024-05-27 01:01:00'
SELECT * FROM employee TIMESTAMP AS OF '2024-05-27 01:01:00';
SELECT * FROM employee FOR SYSTEM_TIME AS OF '2024-05-27 01:01:00';

-- Show the details of employee, such as schema and reserved properties(like location, current-snapshot-id, provider, format, format-version, etc)
DESC EXTENDED employee;
```

For more details about `CALL`, please refer to the [Spark Procedures description](https://iceberg.apache.org/docs/1.5.2/spark-procedures/#spark-procedures) in Iceberg official document. 

## Apache Iceberg backend-catalog support
- HiveCatalog
- JdbcCatalog
- RESTCatalog

### Catalog properties

Gravitino spark connector will transform below property names which are defined in catalog properties to Spark Iceberg connector configuration.

#### HiveCatalog

| Gravitino catalog property name | Spark Iceberg connector configuration | Default Value | Required | Description               | Since Version |
|---------------------------------|---------------------------------------|---------------|----------|---------------------------|---------------|
| `catalog-backend`               | `type`                                | `memory`      | Yes      | Catalog backend type      | 0.5.0         |
| `uri`                           | `uri`                                 | (none)        | Yes      | Catalog backend uri       | 0.5.0         |
| `warehouse`                     | `warehouse`                           | (none)        | Yes      | Catalog backend warehouse | 0.5.0         |

#### JdbcCatalog

| Gravitino catalog property name | Spark Iceberg connector configuration | Default Value | Required | Description               | Since Version |
|---------------------------------|---------------------------------------|---------------|----------|---------------------------|---------------|
| `catalog-backend`               | `type`                                | `memory`      | Yes      | Catalog backend type      | 0.5.0         |
| `uri`                           | `uri`                                 | (none)        | Yes      | Catalog backend uri       | 0.5.0         |
| `warehouse`                     | `warehouse`                           | (none)        | Yes      | Catalog backend warehouse | 0.5.0         |
| `jdbc-user`                     | `jdbc.user`                           | (none)        | Yes      | JDBC user name            | 0.5.0         |
| `jdbc-password`                 | `jdbc.password`                       | (none)        | Yes      | JDBC password             | 0.5.0         |

#### RESTCatalog

| Gravitino catalog property name | Spark Iceberg connector configuration | Default Value | Required | Description               | Since Version |
|---------------------------------|---------------------------------------|---------------|----------|---------------------------|---------------|
| `catalog-backend`               | `type`                                | `memory`      | Yes      | Catalog backend type      | 0.5.1         |
| `uri`                           | `uri`                                 | (none)        | Yes      | Catalog backend uri       | 0.5.1         |
| `warehouse`                     | `warehouse`                           | (none)        | No       | Catalog backend warehouse | 0.5.1         |

Gravitino catalog property names with the prefix `spark.bypass.` are passed to Spark Iceberg connector. For example, using `spark.bypass.io-impl` to pass the `io-impl` to the Spark Iceberg connector.

:::info
Iceberg catalog property `cache-enabled` is setting to `false` internally and not allowed to change.
:::

