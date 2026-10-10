---
title: "JDBC Catalog Connection Validation"
slug: "/jdbc-catalog-connection-validation"
keywords:
- jdbc
- catalog
- connection-pool
license: "This software is licensed under the Apache License version 2."
---

This page describes connection validation for JDBC catalogs' datasource pools. The settings apply
to the MySQL, PostgreSQL, Doris, StarRocks, ClickHouse, Hologres, and OceanBase catalogs.

## Connection Validation

JDBC catalogs validate connections on borrow by default using the JDBC driver's
`Connection.isValid()` method. Gravitino does not force a validation SQL query, so the driver
can validate the connection after catalog or schema changes.

The default avoids a connection-reuse problem with MySQL URLs that have no default database.
DBCP caches SQL validation as a prepared statement. MySQL Connector/J retains the database used
when that statement was prepared and tries to switch back to it when executing the statement.
After an application calls `setCatalog()`, switching back to an empty original database can fail
even though the connection is still healthy. DBCP then discards that connection and opens another.
Connector/J's `Connection.isValid()` uses a ping instead of the cached statement, so validation
does not need to restore the statement's original database.

The benefit is preserving healthy pooled connections and avoiding repeated connection setup and
authentication caused by failed validation. Validation remains enabled and disconnected connections
are still replaced. This does not establish a general throughput improvement: other drivers may
implement `isValid()` using SQL, and validation can still require a network round trip.

For a driver that does not support `Connection.isValid()`, set the catalog property
`gravitino.bypass.validationQuery` to a SQL SELECT statement that returns at least one row.
Explicit validation queries are passed to DBCP without being overwritten. The
`jdbc.pool.test-on-borrow` property controls validation on borrow. See the
[DBCP configuration reference](https://commons.apache.org/proper/commons-dbcp/configuration.html)
for other pool validation settings. DBCP-specific properties require the `gravitino.bypass.`
prefix in catalog configuration; the prefix is removed before passing them to DBCP.

For example, to use SQL validation with a driver that does not support `Connection.isValid()`:

```json
{
  "jdbc.pool.test-on-borrow": "true",
  "gravitino.bypass.validationQuery": "SELECT 1",
  "gravitino.bypass.validationQueryTimeout": "5"
}
```

Choose a validation query supported by the driver and database. Disabling validation on borrow
alone does not make a driver without `Connection.isValid()` support compatible: DBCP also validates
a connection when initializing its connection factory.

For MySQL drivers affected by the catalog-switching problem, explicitly setting `SELECT 1` restores
the cached-statement validation path and can reproduce connection churn. Leave the query unset when
the driver supports `Connection.isValid()`.
