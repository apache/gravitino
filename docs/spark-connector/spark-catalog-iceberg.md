---
title: "Spark connector Iceberg catalog"
slug: /spark-connector/spark-catalog-iceberg
keyword: spark connector iceberg catalog
license: "Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

## Capabilities

#### Support basic DML and DDL operations:

* `CREATE TABLE` Supports basic create table clause including table schema, properties, partition, does not support distribution and sort orders.
* `DROP TABLE`
* `ALTER TABLE`
* `INSERT INTO&OVERWRITE`
* `SELECT`
* `DELETE` Supports file delete.

#### Not supported operations:

* Row level operations. like `MERGE INOT`, `DELETE FROM`, `UPDATE`
* View operations.
* Branching and tagging operations.
* Spark procedures.
* Other Iceberg extension SQL, like:
  * `ALTER TABLE prod.db.sample ADD PARTITION FIELD xx`
  * `ALTER TABLE ... WRITE ORDERED BY`

## SQLs

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
