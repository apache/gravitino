---
title: "Flink connector jdbc catalog"
slug: /flink-connector/flink-catalog-jdbc
keyword: flink connector jdbc catalog
license: "This software is licensed under the Apache License version 2."
---

This document provides a comprehensive guide on configuring and using Apache Gravitino Flink connector to access the JDBC catalog managed by the Gravitino server.

## Capabilities

### Supported JDBC Types

* MYSQL

## Getting Started

### Prerequisites

Place the following JAR files in the lib directory of your Flink installation:

- [flink-connector-jdbc-${flinkJdbcConnectorVersion}.jar](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/connectors/table/jdbc/)
- [gravitino-flink-connector-runtime-1.18_2.12-${gravitino-version}.jar](flink-connector.md)
- JDBC driver

### SQL Example

```sql
show databases;
-- +------------------+
-- |    database name |
-- +------------------+
-- |          mysql   |
-- +------------------+
     
create database jdbc_database;
-- [INFO] Execute statement succeed.

show databases;
-- +------------------+
-- |    database name |
-- +------------------+
-- |          mysql   |
-- |  jdbc_database   |
-- +------------------+

use jdbc_database;
-- [INFO] Execute statement succeed.
    
SET 'execution.runtime-mode' = 'batch';
-- [INFO] Execute statement succeed.

SET 'sql-client.execution.result-mode' = 'tableau';
-- [INFO] Execute statement succeed.
     
use jdbc_database;
-- [INFO] Execute statement succeed.

show tables;
-- Empty set

CREATE TABLE jdbc_table_a (
   aa BIGINT NOT NULL PRIMARY KEY NOT ENFORCED,
   bb BIGINT
);
-- [INFO] Execute statement succeed.

show tables;
-- +--------------+
-- |   table name |
-- +--------------+
-- | jdbc_table_a |
-- +--------------+
-- 1 row in set

insert into jdbc_table_a values(1,2);

select * from jdbc_table_a;
-- +----+----+
-- | aa | bb |
-- +----+----+
-- |  1 |  2 |
-- +----+----+
-- 1 row in set

insert into jdbc_table_a values(2,3);
-- [INFO] Submitting SQL update statement to the cluster...
-- [INFO] SQL update statement has been successfully submitted to the cluster:
-- Job ID: bc320828d49b97b684ed9f622f1b8aca

insert into jdbc_table_a values(1,4);
-- [INFO] Submitting SQL update statement to the cluster...
-- [INFO] SQL update statement has been successfully submitted to the cluster:
-- Job ID: bc320828d49b97b684ed9f622f1b8aca

select * from jdbc_table_a;
-- +----+----+
-- | aa | bb |
-- +----+----+
-- |  1 |  4 |
-- |  2 |  3 |
-- +----+----+
-- 2 rows in set
     
```

## Catalog properties

Gravitino Flink connector will transform below property names which are defined in catalog properties to Flink JDBC connector configuration.

| Gravitino catalog property name | Flink JDBC connector configuration | Description                    | Since Version    |
|:--------------------------------|------------------------------------|--------------------------------|------------------|
| `jdbc-url`                      | `base-url`                         | JDBC URL for MYSQL             | 0.9.0-incubating |
| `username`                      | `username`                         | Username of MySQL account      | 0.9.0-incubating |
| `password`                      | `password`                         | Password of the account        | 0.9.0-incubating |
| `flink.bypass.default-database` | `default-database`                 | Default database to connect to | 0.9.0-incubating |

