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

Supports MySQL and PostgreSQL. OceanBase, which is MySQL-compatible, can use the MySQL driver as a workaround. Apache Doris supports an opt-in, read-only governed path described below; when that path is disabled, `jdbc-doris` keeps the generic JDBC behavior.

### Governed Apache Doris batch reads

Set `spark.sql.gravitino.enableDorisSupport=true` to select the specialized Spark 3.5 / Scala 2.12 Doris read path for a `jdbc-doris` catalog. The specialized path requires a catalog-managed `jdbc-url` and `jdbc-driver`, plus a vended JDBC credential. The generic JDBC credential behavior remains unchanged for other providers and when specialized Doris support is disabled.

The first read baseline validates the Gravitino logical schema against Doris FE and JDBC metadata, supports ordinary scalar batch reads, rejects schema drift and unsupported Doris types, and keeps Gravitino authorization ahead of specialized physical access. The specialized table is read-only and rejects Spark writes and catalog DDL. Aggregate, Top-N, limit, offset, partitioned-read, native-tablet, and special-type normalization lanes are added only by follow-up contributions.

This baseline uses the MySQL Connector/J driver for Doris FE's MySQL protocol; it does not bundle a separate Doris Spark connector. MySQL Connector/J and any required external runtime dependencies must be available on the Spark driver and executors. Do not pass connection credentials or protected catalog settings through Spark catalog options.

### DML and DDL Operations

- `CREATE TABLE`
- `DROP TABLE`
- `ALTER TABLE`
- `SELECT`
- `INSERT`

For the specialized Doris path, only `SELECT` batch reads are supported. Spark writes and catalog DDL are rejected. The generic MySQL and PostgreSQL JDBC paths retain their existing behavior.

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

## Catalog Properties

Gravitino spark connector will transform below property names which are defined in catalog properties to Spark JDBC connector configuration.

| Gravitino catalog property name | Spark JDBC connector configuration | Description                                                                                       |
|---------------------------------|------------------------------------|---------------------------------------------------------------------------------------------------|
| `jdbc-url`                      | `url`                              | JDBC URL for connecting to the database. For example, jdbc:mysql://localhost:3306                 |
| `jdbc-user`                     | `jdbc.user`                        | JDBC user name                                                                                    |
| `jdbc-password`                 | `jdbc.password`                    | JDBC password                                                                                     |
| `jdbc-driver`                   | `driver`                           | The driver of the JDBC connection. For example, com.mysql.jdbc.Driver or com.mysql.cj.jdbc.Driver |

Gravitino catalog property names with the prefix `spark.bypass.` are passed to Spark JDBC connector.
