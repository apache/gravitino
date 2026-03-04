---
title: "Spark connector JDBC catalog"
slug: /spark-connector/spark-catalog-jdbc
keyword: spark connector jdbc catalog
license: "This software is licensed under the Apache License version 2."
---

The Apache Gravitino Spark connector offers the capability to read JDBC tables, with the metadata managed by the Gravitino server.

## Preparation

1. Download the corresponding jdbc driver jar to Spark classpath.

## Capabilities

Supports MySQL and PostgreSQL. For OceanBase which is compatible with Mysql Dialects could use Mysql driver and Mysql Dialects as a trackoff way. But for Doris which do not support MySQL Dialects, are not currently supported.

#### Support DML and DDL operations:

- `CREATE TABLE`
- `DROP TABLE`
- `ALTER TABLE`
- `SELECT`
- `INSERT`

  :::info
  JDBCTable does not support distributed transaction. When writing data to RDBMS, each task is an independent transaction. If some tasks of spark succeed and some tasks fail, dirty data is generated.
  :::

#### Not supported operations:

- `UPDATE`
- `DELETE`
- `TRUNCATE`

## SQL example

```sql
-- Suppose mysql_a is the mysql catalog name managed by Gravitino
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

Gravitino spark connector will transform below property names which are defined in catalog properties to Spark JDBC connector configuration.

| Gravitino catalog property name | Spark JDBC connector configuration | Description                                                                                                                                                                                                         | Since Version |
|---------------------------------|------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `jdbc-url`                      | `url`                              | JDBC URL for connecting to the database. For example, jdbc:mysql://localhost:3306                                                                                                                                   | 0.3.0         |
| `jdbc-user`                     | `jdbc.user`                        | JDBC user name                                                                                                                                                                                                      | 0.3.0         |
| `jdbc-password`                 | `jdbc.password`                    | JDBC password                                                                                                                                                                                                       | 0.3.0         |
| `jdbc-driver`                   | `driver`                           | The driver of the JDBC connection. For example, com.mysql.jdbc.Driver or com.mysql.cj.jdbc.Driver                                                                                                                   | 0.3.0         |

Gravitino catalog property names with the prefix `spark.bypass.` are passed to Spark JDBC connector.

