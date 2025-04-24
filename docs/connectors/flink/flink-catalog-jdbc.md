---
title: "Flink connector jdbc catalog"
slug: /flink-connector/flink-catalog-jdbc
keyword: flink connector jdbc catalog
license: "This software is licensed under the Apache License version 2."
---

This document provides a guide on configuring and using the Apache Gravitino Flink connector
to access the JDBC catalog managed by the Gravitino server.

## Capabilities

### Supported JDBC types

* MYSQL

## Getting started

### Prerequisites

Place the following JAR files in the lib directory for your Flink installation:

- [flink-connector-jdbc-${flinkJdbcConnectorVersion}.jar](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/connectors/table/jdbc/)
- [gravitino-flink-connector-runtime-1.18_2.12-${gravitino-version}.jar](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-flink-connector-runtime-1.18)
- JDBC driver

### SQL Example

```sql
-- Suppose jdbc_catalog is the JDBC catalog name managed by Gravitino

USE CATALOG jdbc_catalog;

SHOW DATABASES;
-- +------------------+
-- |    database name |
-- +------------------+
-- |          mysql   |
-- +------------------+
     
CREATE DATABASE jdbc_database;
-- [INFO] Execute statement succeed.

SHOW DATABASES;
-- +------------------+
-- |    database name |
-- +------------------+
-- |          mysql   |
-- |  jdbc_database   |
-- +------------------+

USE jdbc_database;
-- [INFO] Execute statement succeed.
    
SET 'execution.runtime-mode' = 'batch';
-- [INFO] Execute statement succeed.

SET 'sql-client.execution.result-mode' = 'tableau';
-- [INFO] Execute statement succeed.
     
USE jdbc_database;
-- [INFO] Execute statement succeed.

SHOW TABLES;
-- Empty set

CREATE TABLE jdbc_table_a (
   aa BIGINT NOT NULL PRIMARY KEY NOT ENFORCED,
   bb BIGINT
);
-- [INFO] Execute statement succeed.

SHOW TABLES;
-- +--------------+
-- |   table name |
-- +--------------+
-- | jdbc_table_a |
-- +--------------+
-- 1 row in set

INSERT INTO jdbc_table_a VALUES(1,2);

SELECT * FROM jdbc_table_a;
-- +----+----+
-- | aa | bb |
-- +----+----+
-- |  1 |  2 |
-- +----+----+
-- 1 row in set

INSERT INTO jdbc_table_a VALUES(2,3);
-- [INFO] Submitting SQL update statement to the cluster...
-- [INFO] SQL update statement has been successfully submitted to the cluster:
-- Job ID: bc320828d49b97b684ed9f622f1b8aca

INSERT INTO jdbc_table_a VALUES(1,4);
-- [INFO] Submitting SQL update statement to the cluster...
-- [INFO] SQL update statement has been successfully submitted to the cluster:
-- Job ID: bc320828d49b97b684ed9f622f1b8aca

SELECT * FROM jdbc_table_a;
-- +----+----+
-- | aa | bb |
-- +----+----+
-- |  1 |  4 |
-- |  2 |  3 |
-- +----+----+
-- 2 rows in set
     
```

## Catalog properties

Gravitino Flink connector will transform the follwing property names which are defined in catalog properties
into Flink JDBC connector configuration.

<table>
<thead>
<tr>
  <th>Gravitino catalog property name</th>
  <th>Flink JDBC connector configuration</th>
  <th>Description</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>jdbc-url</tt></td>
  <td><tt>base-url</tt></td>
  <td>JDBC URL for MySQL</td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>username</tt></td>
  <td><tt>username</tt></td>
  <td>Username for the MySQL account</td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>password</tt></td>
  <td><tt>password</tt></td>
  <td>Password for the MySQL account</td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>flink.bypass.default-database</tt></td>
  <td><tt>default-database</tt></td>
  <td>Default database to connect to</td>
  <td>`0.9.0-incubating`</td>
</tr>
</tbody>
</table>

