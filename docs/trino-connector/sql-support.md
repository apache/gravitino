---
title: "Apache Gravitino Trino connector SQL support"
slug: /trino-connector/sql-support
keyword: gravitino connector trino
license: "This software is licensed under the Apache License version 2."
---

The connector provides read access and write access to data and metadata stored in Apache Gravitino.

### Globally available statements

- [EXPLAIN](https://trino.io/docs/current/sql/explain.html)
- [EXPLAIN ANALYZE](https://trino.io/docs/current/sql/explain-analyze.html)
- [PREPARE](https://trino.io/docs/current/sql/prepare.html)
- [USE](https://trino.io/docs/current/sql/use.html)

### Read operations

- [SELECT](https://trino.io/docs/current/sql/select.html)
- [DESCRIBE](https://trino.io/docs/current/sql/describe.html)
- [SHOW CATALOGS](https://trino.io/docs/current/sql/show-catalogs.html)
- [SHOW COLUMNS](https://trino.io/docs/current/sql/show-columns.html)
- [SHOW CREATE SCHEMA](https://trino.io/docs/current/sql/show-create-schema.html)
- [SHOW CREATE TABLE](https://trino.io/docs/current/sql/show-create-table.html)
- [SHOW SCHEMAS](https://trino.io/docs/current/sql/show-schemas.html)
- [SHOW TABLES](https://trino.io/docs/current/sql/show-tables.html)

### Write operations

- [INSERT](https://trino.io/docs/current/sql/insert.html)
- [INSERT INTO SELECT](https://trino.io/docs/current/sql/insert.html)
- [UPDATE](https://trino.io/docs/current/sql/update.html)
- [DELETE](https://trino.io/docs/current/sql/delete.html)

### Schema and table management

- [CREATE TABLE](https://trino.io/docs/current/sql/create-table.html)
- [DROP TABLE](https://trino.io/docs/current/sql/drop-table.html)
- [ALTER TABLE](https://trino.io/docs/current/sql/alter-table.html)
- [CREATE SCHEMA](https://trino.io/docs/current/sql/create-schema.html)
- [DROP SCHEMA](https://trino.io/docs/current/sql/drop-schema.html)
- [COMMENT](https://trino.io/docs/current/sql/comment.html)

### Transactions

- [START TRANSACTION](https://trino.io/docs/current/sql/start-transaction.html)
- [COMMIT](https://trino.io/docs/current/sql/commit.html)
- [ROLLBACK](https://trino.io/docs/current/sql/rollback.html)

For more information, please refer to Trino [SQL statements support](https://trino.io/docs/current/language/sql-support.html#sql-globally-available)
