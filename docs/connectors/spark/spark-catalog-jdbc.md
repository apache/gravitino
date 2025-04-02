---
title: "Spark connector JDBC catalog"
slug: /spark-connector/spark-catalog-jdbc
keyword: spark connector jdbc catalog
license: "This software is licensed under the Apache License version 2."
---

The Apache Gravitino Spark connector enable users to read JDBC tables,
with the metadata managed by the Gravitino server.
To use the JDBC catalog via the Spark connector,
you must download the JDBC driver JAR into the Spark class path.

## Capabilities

This connector supports MySQL and PostgreSQL.
For OceanBase which is compatible with Mysql Dialects,
you can use MySQL driver and Mysql Dialects as a workaround.
Doris does not support MySQL Dialects, so it is currently not supported.

#### Support DML and DDL operations:

- `CREATE TABLE`
- `DROP TABLE`
- `ALTER TABLE`
- `SELECT`
- `INSERT`

  :::info
  JDBCTable does not support distributed transaction.
  When writing data to RDBMS, each task is an independent transaction.
  If some Spark tasks succeed while others fail, some data may be corrupted.
  :::

#### Unsupported operations

- `DELETE`
- `UPDATE`
- `TRUNCATE`

## SQL example

```sql
-- Suppose mysql_a is a mysql catalog managed by Gravitino
USE mysql_a;

CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;

CREATE TABLE IF NOT EXISTS employee (
  id bigint,
  name string,
  department string,
  hire_date timestamp
)
DESC TABLE EXTENDED employee;

INSERT INTO employee
VALUES
(1, 'Alice', 'Engineering', TIMESTAMP '2021-01-01 09:00:00'),
(2, 'Bob', 'Marketing', TIMESTAMP '2021-02-01 10:30:00'),
(3, 'Charlie', 'Sales', TIMESTAMP '2021-03-01 08:45:00');

SELECT * FROM employee WHERE date(hire_date) = '2021-01-01';
```

## Catalog properties

The Gravitino Spark connector translates the following properties defined in catalog properties
into Spark JDBC connector configurations.

<table>
<thead>
<tr>
  <th>Gravitino catalog property name</th>
  <th>Spark JDBC connector configuration</th>
  <th>Description</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>jdbc-url</tt></td>
  <td><tt>url</tt></td>
  <td>
    JDBC URL for connecting to the database.
    For example, jdbc:mysql://localhost:3306
  </td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>jdbc-user</tt></td>
  <td><tt>jdbc.user</tt></td>
  <td>JDBC user name</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>jdbc-password</tt></td>
  <td><tt>jdbc.password</tt></td>
  <td>JDBC password</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>jdbc-driver</tt></td>
  <td><tt>driver</tt></td>
  <td>
    The driver for the JDBC connection.
    For example, `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver`.
  </td>
  <td>`0.3.0`</td>
</tr>
</tbody>
</table>

Gravitino catalog property names with the prefix `spark.bypass.` are passed
to the Spark JDBC connector.

