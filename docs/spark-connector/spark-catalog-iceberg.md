---
title: "Spark connector Iceberg catalog"
slug: /spark-connector/spark-catalog-iceberg
keyword: spark connector iceberg catalog
license: "Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

## Capabilities

#### Support basic DML and DDL operations:

- `CREATE TABLE` 
 
Supports basic create table clause including table schema, properties, partition, does not support distribution and sort orders.

- `DROP TABLE`
- `ALTER TABLE`
- `INSERT INTO&OVERWRITE`
- `SELECT`
- `DELETE` 
 
Supports file delete only.

#### Not supported operations:

- Row level operations. like `MERGE INOT`, `DELETE FROM`, `UPDATE`
- View operations.
- Branching and tagging operations.
- Spark procedures.
- Other Iceberg extension SQL, like:
  - `ALTER TABLE prod.db.sample ADD PARTITION FIELD xx`
  - `ALTER TABLE ... WRITE ORDERED BY`

## SQL example

```sql
// Suppose iceberg_a is the Iceberg catalog name managed by Gravitino
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

SELECT * FROM employee WHERE date(hire_date) = '2021-01-01'
```

## Catalog properties

Gravitino spark connector will transform below property names which are defined in catalog properties to Spark Iceberg connector configuration.

| Gravitino catalog property name | Spark Iceberg connector configuration | Description               | Since Version |
|---------------------------------|---------------------------------------|---------------------------|---------------|
| `catalog-backend`               | `type`                                | Catalog backend type      | 0.5.0         |
| `uri`                           | `uri`                                 | Catalog backend uri       | 0.5.0         |
| `warehouse`                     | `warehouse`                           | Catalog backend warehouse | 0.5.0         |
| `jdbc-user`                     | `jdbc.user`                           | JDBC user name            | 0.5.0         |
| `jdbc-password`                 | `jdbc.password`                       | JDBC password             | 0.5.0         |

Gravitino catalog property names with the prefix `spark.bypass.` are passed to Spark Iceberg connector. For example, using `spark.bypass.io-impl` to pass the `io-impl` to the Spark Iceberg connector.
