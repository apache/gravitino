---
title: "Trino Connector SQL Support"
slug: "/trino-connector/sql-support"
keyword: "gravitino connector trino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The connector provides read access and write access to data and metadata stored in Apache Gravitino.

### Globally Available Statements

- [EXPLAIN](https://trino.io/docs/current/sql/explain.html)
- [EXPLAIN ANALYZE](https://trino.io/docs/current/sql/explain-analyze.html)
- [PREPARE](https://trino.io/docs/current/sql/prepare.html)
- [USE](https://trino.io/docs/current/sql/use.html)

### Read Operations

- [SELECT](https://trino.io/docs/current/sql/select.html)
- [DESCRIBE](https://trino.io/docs/current/sql/describe.html)
- [SHOW CATALOGS](https://trino.io/docs/current/sql/show-catalogs.html)
- [SHOW COLUMNS](https://trino.io/docs/current/sql/show-columns.html)
- [SHOW CREATE SCHEMA](https://trino.io/docs/current/sql/show-create-schema.html)
- [SHOW CREATE TABLE](https://trino.io/docs/current/sql/show-create-table.html)
- [SHOW SCHEMAS](https://trino.io/docs/current/sql/show-schemas.html)
- [SHOW TABLES](https://trino.io/docs/current/sql/show-tables.html)

### Write Operations

- [INSERT](https://trino.io/docs/current/sql/insert.html)
- [INSERT INTO SELECT](https://trino.io/docs/current/sql/insert.html)
- [UPDATE](https://trino.io/docs/current/sql/update.html)
- [DELETE](https://trino.io/docs/current/sql/delete.html)
- [MERGE](https://trino.io/docs/current/sql/merge.html)

### Schema and Table Management

- [CREATE TABLE](https://trino.io/docs/current/sql/create-table.html)
- [CREATE TABLE AS SELECT](https://trino.io/docs/current/sql/create-table-as.html) (`CREATE OR REPLACE TABLE AS SELECT` is not supported)
- [DROP TABLE](https://trino.io/docs/current/sql/drop-table.html)
- [ALTER TABLE](https://trino.io/docs/current/sql/alter-table.html)
- [CREATE SCHEMA](https://trino.io/docs/current/sql/create-schema.html)
- [DROP SCHEMA](https://trino.io/docs/current/sql/drop-schema.html)
- [COMMENT](https://trino.io/docs/current/sql/comment.html)

### View Management

- [CREATE VIEW](https://trino.io/docs/current/sql/create-view.html)
- [CREATE OR REPLACE VIEW](https://trino.io/docs/current/sql/create-view.html)
- [SHOW CREATE VIEW](https://trino.io/docs/current/sql/show-create-view.html)
- [DROP VIEW](https://trino.io/docs/current/sql/drop-view.html)
- [ALTER VIEW ... RENAME TO](https://trino.io/docs/current/sql/alter-view.html)

View management is only supported for catalogs backed by Hive or Iceberg; other catalogs
(e.g. Glue, JDBC, Memory) do not support view operations. A view stored by Gravitino may carry SQL
representations for multiple engines (Hive, Spark, Flink, Trino); the Trino connector only reads and
writes the Trino dialect representation. A view that has no Trino SQL representation is silently
invisible to Trino (it does not appear in `SHOW TABLES`/`information_schema` and cannot be loaded),
rather than causing an error. View owner is not supported; views are always shown with `SECURITY
INVOKER` (Gravitino does not track an owner, so `SECURITY DEFINER` cannot be represented).

Some catalogs (e.g. Iceberg) can store a view with a default schema but no default catalog. In
single-metalake mode this is resolved against the current Trino catalog. In multi-metalake mode it
cannot be resolved reliably, so loading such a view fails with an error instead of silently
resolving to a possibly incorrect catalog.

For Hive-backed catalogs, Trino dialect views are stored using Trino's own native "Presto View"
Hive Metastore format, so views are interoperable with a native Trino (or Presto) Hive connector
pointed at the same Hive Metastore: a view created directly through a native Trino Hive connector
is visible and queryable through Gravitino, and a view created through Gravitino is visible and
queryable through a native Trino Hive connector.

### Transactions

- [START TRANSACTION](https://trino.io/docs/current/sql/start-transaction.html)
- [COMMIT](https://trino.io/docs/current/sql/commit.html)
- [ROLLBACK](https://trino.io/docs/current/sql/rollback.html)

For more information, refer to Trino [SQL statements support](https://trino.io/docs/current/language/sql-support.html#sql-globally-available)
