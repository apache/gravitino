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

## Backend verification at create time

A relational JDBC catalog verifies that its backend is reachable when the catalog is created,
rather than deferring the first connection to a later operation. The verification opens a dedicated,
time-bounded one-off probe connection (separate from the pool used by normal operations), confirms
it, and closes it. If it fails, the create is rejected and rolled back (the catalog entity is not
persisted), and the caller receives a connection error that names the backend `host:port` and the
underlying driver message.

To register a catalog whose backend is temporarily unavailable, set the hidden catalog property
`verify-on-create` to `false`, which skips the create-time check:

```json
{
  "verify-on-create": "false"
}
```

The default is `true` (verify on create). The connection is still opened lazily on first use when
the check is skipped.

### Bounded timeouts

So that create-time verification fails fast instead of hanging, the timeout
`jdbc.pool.connect-timeout-ms` (default `30000` ms, maximum `2147483647`) bounds a JDBC connection.
Gravitino translates this canonical millisecond value into each driver family's own parameters and
unit:

- MySQL Connector/J — also used by the Doris, StarRocks, and OceanBase catalogs: `connectTimeout`
  and `socketTimeout` in milliseconds.
- PostgreSQL — also used by the Hologres catalog: `connectTimeout`, `socketTimeout` and
  `loginTimeout` in seconds (rounded up, minimum 1).
- ClickHouse: `connect_timeout` and `socket_timeout` in milliseconds (note the underscore names).

It is applied differently to the two kinds of connection:

- **Pooled connections** (normal operations) get only the **connect** timeout. A socket-read
  timeout is deliberately not imposed there, because it would also abort legitimate long-running
  operations on a healthy backend (a large `ALTER TABLE`, a slow metadata query).
- **The create-time verification probe** gets both the **connect** and the **read** (and, for
  PostgreSQL, **login**) timeout. The probe runs under the metalake write lock, so it must be fully
  bounded: a backend that accepts the TCP connection but never speaks the database protocol (a load
  balancer with a dead backend, a wrong port, a proxy that accepts and never forwards) would
  otherwise block a connect-only bound forever and stall the whole metalake.

A timeout value you set directly on the connection or in the `jdbc-url` query string is never
overridden; drivers whose family is not recognized are left unbounded by Gravitino. The pooled
timeout is applied as a per-connection bound, so the pool stays lazy and no connection is opened
until the first real round trip.
