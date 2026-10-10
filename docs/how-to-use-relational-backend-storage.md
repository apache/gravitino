---
title: "Relational Backend Storage"
slug: "/how-to-use-relational-backend-storage"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Apache Gravitino stores its metadata in a relational database through a JDBC backend. H2 is the
default and needs no configuration, but Gravitino makes no consistency or durability guarantee for
metadata held in H2. Use it for local development and tests only. A server that holds metadata you
care about should use MySQL or PostgreSQL.

This page covers pointing a Gravitino server at MySQL or PostgreSQL. For the connection pool and
version retention properties that apply whichever database you use, and for the H2 storage path,
see [Gravitino Server Configuration](gravitino-server-config.md#storage-backend).

## Quick Start

Setting up MySQL or PostgreSQL comes down to four properties in the server configuration file,
`${GRAVITINO_HOME}/conf/gravitino.conf`:

```text
# conf/gravitino.conf
gravitino.entity.store.relational.jdbcUrl      = {jdbc_url}
gravitino.entity.store.relational.jdbcDriver   = {driver_class}
gravitino.entity.store.relational.jdbcUser     = {username}
gravitino.entity.store.relational.jdbcPassword = {password}
```

`gravitino.entity.store` and `gravitino.entity.store.relational` already default to `relational`
and `JDBCBackend`. Leave them alone.

For concurrent metadata reads, tune `gravitino.entity.store.relational.maxIdleConnections`
alongside `maxConnections`. The default retains up to 10 idle connections per server; the
effective limit never exceeds `maxConnections`. Count this limit once per server when calculating
the database connection budget, and also count the pools of any JDBC catalogs that point at the
same database instance.

This limit, not idle-time eviction, is what decides whether connections are reused. When a request
returns a connection while the pool already holds `maxIdleConnections` idle connections, the pool
closes it at once, and a later request has to open a new one. When the number of connections in
use keeps swinging by more than this limit, physical connections keep being closed and reopened,
which adds latency. Setting it at or above the number of connections in use at steady peak
concurrency avoids that. For example, with 64 concurrent clients listing metalakes against MySQL,
a limit of `10` reopened about 1,400 to 2,700 connections every 15 seconds, `32` about 270 to 440,
and `64` almost none. Raise it only as far as the database connection budget of all servers allows.

Idle connections are released slowly. The pool's evictor runs every ten minutes and checks at most
three idle connections per run, closing those idle for more than 30 seconds. After a burst, a server
can therefore keep close to `maxIdleConnections` open for a long time; with a limit of `64`, it
takes more than three hours to shrink back to the minimum of five. Budget for the full value on
every server rather than treating it as a short-lived peak.

The values to use, and the driver each one needs:

| Database            | JDBC URL                                                          | Driver Class               | Driver Jar                    |
|---------------------|-------------------------------------------------------------------|----------------------------|-------------------------------|
| H2 (default)        | `jdbc:h2`                                                         | `org.h2.Driver`            | Bundled with the distribution |
| MySQL 5.7 or 8.0    | `jdbc:mysql://{host}:3306/{database}`                             | `com.mysql.cj.jdbc.Driver` | `com.mysql:mysql-connector-j` |
| PostgreSQL 12 to 16 | `jdbc:postgresql://{host}:5432/{database}?currentSchema={schema}` | `org.postgresql.Driver`    | `org.postgresql:postgresql`   |

Other PostgreSQL versions have not been tested by the community and may not work.

Gravitino initializes its schema automatically on H2 only, by running
`scripts/h2/schema-{version}-h2.sql` at startup. For MySQL and PostgreSQL you create the database
and run the script yourself, before starting the server for the first time. Work through the
steps below.

## Setting Up MySQL

**1. Create the database.** Gravitino will not create it for you.

```sql
CREATE DATABASE {database};
```

**2. Run the schema script.** Download and unpack the Gravitino distribution package if you have
not already; see [How to Install Gravitino](how-to-install.md). The MySQL scripts are in
`${GRAVITINO_HOME}/scripts/mysql/`. Run the `schema-*-mysql.sql` file matching your Gravitino
version against the database you just created:

```shell
mysql -h {host} -u {username} -p {database} < ${GRAVITINO_HOME}/scripts/mysql/schema-{version}-mysql.sql
```

If you are moving an existing deployment forward rather than starting fresh, run the
`upgrade-{old_version}-to-{new_version}-mysql.sql` scripts in order instead, one per version step.

**3. Install the driver.** Download the MySQL Connector/J jar matching your MySQL version from
[Maven Central](https://central.sonatype.com/artifact/com.mysql/mysql-connector-j) and place it in
`${GRAVITINO_HOME}/libs/`. The artifact was renamed from `mysql:mysql-connector-java` to
`com.mysql:mysql-connector-j`, so the jar is named `mysql-connector-j-{version}.jar`. Older
`mysql-connector-java-{version}.jar` builds still work but no longer receive fixes.

**4. Configure the server.** In `${GRAVITINO_HOME}/conf/gravitino.conf`:

```text
# conf/gravitino.conf
gravitino.entity.store.relational.jdbcUrl      = jdbc:mysql://{host}:3306/{database}
gravitino.entity.store.relational.jdbcDriver   = com.mysql.cj.jdbc.Driver
gravitino.entity.store.relational.jdbcUser     = {username}
gravitino.entity.store.relational.jdbcPassword = {password}
```

**5. Start the server.**

```shell
${GRAVITINO_HOME}/bin/gravitino.sh start
```

## Setting Up PostgreSQL

PostgreSQL follows the same five steps, with one difference: Gravitino addresses a schema inside
the database, so you create both, and the JDBC URL names both. Neither is optional.

**1. Create the database and schema.**

```postgresql
psql --username=postgres --password

CREATE DATABASE {database};
\c {database}
CREATE SCHEMA {schema};
```

**2. Run the schema script.** The PostgreSQL scripts are in `${GRAVITINO_HOME}/scripts/postgresql/`.
Set the search path first so the tables land in your schema rather than `public`:

```postgresql
\c {database}
SET search_path TO {schema};
\i ${GRAVITINO_HOME}/scripts/postgresql/schema-{version}-postgresql.sql
```

To move an existing deployment forward, run the
`upgrade-{old_version}-to-{new_version}-postgresql.sql` scripts in order instead.

**3. Install the driver.** Download the current pgJDBC driver from
[jdbc.postgresql.org](https://jdbc.postgresql.org/download/) and place `postgresql-{version}.jar`
in `${GRAVITINO_HOME}/libs/`. Take the latest release rather than pinning an old one; several
earlier versions carry published CVEs.

**4. Configure the server.** Note the `currentSchema` parameter:

```text
# conf/gravitino.conf
gravitino.entity.store.relational.jdbcUrl      = jdbc:postgresql://{host}:5432/{database}?currentSchema={schema}
gravitino.entity.store.relational.jdbcDriver   = org.postgresql.Driver
gravitino.entity.store.relational.jdbcUser     = {username}
gravitino.entity.store.relational.jdbcPassword = {password}
```

**5. Start the server.**

```shell
${GRAVITINO_HOME}/bin/gravitino.sh start
```

## Verifying the Connection

The readiness endpoint probes the entity store, so it answers the question directly. A server that
reached its database returns `UP`:

```shell
curl http://{host}:8090/health/ready
```

If the backend is unreachable or slow the endpoint returns 503 with an `entityStore` check in the
`DOWN` state. See
[Health Check Endpoints](gravitino-server-config.md#health-check-endpoints) for the response
format and the probe timeout.
