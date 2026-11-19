---
title: "Spark Connector: JDBC Catalog"
slug: "/spark-connector/spark-catalog-jdbc"
keyword: "spark connector jdbc catalog"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The Apache Gravitino Spark connector offers the capability to read JDBC tables, with the metadata managed by the Gravitino server.

## Preparation

1. Download the corresponding jdbc driver jar to Spark classpath.

## Capabilities

Supports MySQL and PostgreSQL. OceanBase, which is MySQL-compatible, can use the MySQL driver as a workaround. Doris, which does not support MySQL dialects, is not supported.

### DML and DDL Operations

- `CREATE TABLE`
- `DROP TABLE`
- `ALTER TABLE`
- `SELECT`
- `INSERT`

  :::info
  JDBCTable does not support distributed transaction. When writing data to RDBMS, each task is an independent transaction. If some tasks of spark succeed and some tasks fail, dirty data is generated.
  :::

### Unsupported Operations

- `UPDATE`
- `DELETE`
- `TRUNCATE`

## SQL Example

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

Connection option names after `spark.bypass.` are case-insensitive. If both `spark.bypass.URL`
and `jdbc-url` are configured, `jdbc-url` takes precedence. Earlier connector versions could
select the bypass value when the names differed in case. Check for conflicting settings before
upgrading. Explicit Spark catalog options take precedence over catalog properties, and vended
JDBC credentials take precedence over configured user names and passwords.

## Changes during a Spark session

Spark keeps a JDBC catalog's connection settings for the lifetime of the session. The connector
checks the effective URL, user, password, and catalog creation time when resolving a JDBC table,
before creating or altering a table, and when building a scan or write or accessing table indexes.
If the settings change or the catalog is recreated, restart the Spark session before retrying.

A DataFrame resolved before the change is checked when Spark builds its scan. A physical plan or
writer already built can continue using the original connection. Finish in-flight jobs before
rotating credentials or switching databases. The metadata check and the JDBC operation are separate
requests, so a concurrent catalog change can also occur between them.

## Catalog Properties

Gravitino spark connector will transform below property names which are defined in catalog properties to Spark JDBC connector configuration.

| Gravitino catalog property name | Spark JDBC connector configuration | Description                                                                                       |
|---------------------------------|------------------------------------|---------------------------------------------------------------------------------------------------|
| `jdbc-url`                      | `url`                              | JDBC URL for connecting to the database. For example, jdbc:mysql://localhost:3306                 |
| `jdbc-user`                     | `jdbc.user`                        | JDBC user name                                                                                    |
| `jdbc-password`                 | `jdbc.password`                    | JDBC password                                                                                     |
| `jdbc-driver`                   | `driver`                           | The driver of the JDBC connection. For example, com.mysql.jdbc.Driver or com.mysql.cj.jdbc.Driver |

Gravitino catalog property names with the prefix `spark.bypass.` are passed to Spark JDBC connector.

