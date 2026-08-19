---
title: "Spark Connector: JDBC Catalog"
slug: "/spark-connector/spark-catalog-jdbc"
keyword: "spark connector jdbc catalog"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The Apache Gravitino Spark connector offers the capability to read JDBC tables, with the metadata managed by the Gravitino server.
For the specialized Apache Doris batch-read path on Spark 3.5, see
[Spark Connector: Apache Doris Batch Read](spark-catalog-doris.md).

## Preparation

1. Download the corresponding jdbc driver jar to Spark classpath.

## Capabilities

Supports MySQL and PostgreSQL. OceanBase, which is MySQL-compatible, can use the MySQL driver as a workaround.
The existing generic JDBC path remains available for `jdbc-doris`; it does not provide the
Doris-specific schema and query-semantics contract. The opt-in Doris batch-read adapter is
documented separately and is currently limited to Spark 3.5.3+ in the Spark 3.5 line / Scala 2.12.

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

## Catalog Properties

Gravitino spark connector will transform below property names which are defined in catalog properties to Spark JDBC connector configuration.

| Gravitino catalog property name | Spark JDBC connector configuration | Description                                                                                       |
|---------------------------------|------------------------------------|---------------------------------------------------------------------------------------------------|
| `jdbc-url`                      | `url`                              | JDBC URL for connecting to the database. For example, jdbc:mysql://localhost:3306                 |
| `jdbc-user`                     | `jdbc.user`                        | JDBC user name                                                                                    |
| `jdbc-password`                 | `jdbc.password`                    | JDBC password                                                                                     |
| `jdbc-driver`                   | `driver`                           | The driver of the JDBC connection. For example, com.mysql.jdbc.Driver or com.mysql.cj.jdbc.Driver |

Gravitino catalog property names with the prefix `spark.bypass.` are passed to Spark JDBC connector.

The generic JDBC write operations described above do not apply to the specialized Doris adapter.
When `spark.sql.gravitino.enableDorisSupport=true`, the Doris adapter exposes batch reads only.
