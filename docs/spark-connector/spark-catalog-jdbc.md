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

The specialized path validates the Gravitino logical schema against Doris FE and JDBC metadata, rejects schema drift and unsupported Doris types, and checks Gravitino authorization before physical access. Queries use the existing Spark JDBC V2 delegate for supported pushdowns and partitioned reads. Doris-specific optimizations, native tablet reads, and special-type normalization are not provided.

Ordinary scalar reads require agreement among the FE, JDBC, Gravitino, and Spark schemas. Supported type families are `BIGINT`, `BOOLEAN`, `DATE`/`DATEV2`, `DATETIME`/`DATETIMEV2`, `DECIMAL`/`DECIMALV3`, `DOUBLE`, `INT`/`INTEGER`, `SMALLINT`, `STRING`/`TEXT`, `TINYINT`, and `VARCHAR`. Precision, scale, length, and nullability must match the available metadata. `CHAR` and `FLOAT` are rejected because their Gravitino logical types differ from the types exposed by Spark JDBC. `DATETIME` uses Spark `TimestampType`, so string rendering follows `spark.sql.session.timeZone`.

With Spark 3.5.3, MySQL Connector/J 8.0.33, and Doris 3.0.6.2, `SMALLINT` retains Spark `ShortType` for reading despite the initial JDBC schema using `IntegerType`. This compatibility rule is limited to matching FE and JDBC `SMALLINT` metadata. See Spark's versioned [JDBC type mapping](https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala) and [MySQL dialect](https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/scala/org/apache/spark/sql/jdbc/MySQLDialect.scala).

This baseline uses the MySQL Connector/J driver for Doris FE's MySQL protocol; it does not bundle a separate Doris Spark connector. MySQL Connector/J and any required external runtime dependencies must be available on the Spark driver and executors. Do not pass connection credentials or protected catalog settings through Spark catalog options.

Catalog properties prefixed with `spark.bypass.` supply JDBC defaults. Application catalog options take precedence when keys match ignoring case. Spark read-option names are case-insensitive, but native Connector/J property names must use the driver's exact spelling, such as `connectTimeout` and `sslMode`; their spelling is preserved when forwarded. For example, application option `spark.sql.catalog.<catalog>.connectTimeout=20000` overrides catalog property `spark.bypass.connectTimeout=10000`.

Application catalog/scan options and catalog bypass properties cannot override the connection URL, driver, credentials, or table/query target, or supply Spark's `prepareQuery`/`sessionInitStatement` or the driver's `dbname`/`createDatabaseIfNotExist`. Read-only support rejects Spark writes and catalog DDL. Use vended database credentials with only the required read and metadata permissions; this API boundary does not make arbitrary JDBC driver configuration free of connection or session side effects.

Physical schema loading and validation errors emitted by the Doris adapter omit raw connection details and exception causes, retaining the failure stage and available safe SQLState/vendor diagnostics. Applications must keep Spark's JDBC connection-provider logging at `INFO` or higher and leave JDBC driver connection/protocol tracing disabled on both drivers and executors. For Log4j 2, the provider category can be configured as follows:

```properties
logger.jdbcConnectionProvider.name = org.apache.spark.sql.execution.datasources.jdbc.connection
logger.jdbcConnectionProvider.level = info
```

Spark and driver diagnostic logs and executor failures are controlled by the application and third-party components. Adapter exception redaction does not sanitize arbitrary third-party logs or exception chains, including connection details emitted when JDBC debugging is enabled.

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

