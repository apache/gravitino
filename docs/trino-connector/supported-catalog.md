---
title: "Trino Connector Catalog Support"
slug: "/trino-connector/supported-catalog"
keyword: "gravitino connector trino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The catalogs currently supported by the Apache Gravitino Trino connector are as follows:

- [Hive](catalog-hive.md)
- [Iceberg](catalog-iceberg.md)
- [MySQL](catalog-mysql.md)
- [PostgreSQL](catalog-postgresql.md)
- [AWS Glue](catalog-glue.md)

## Create Catalog

Users can create catalogs through the Gravitino Trino connector and then load them into Trino.
The Gravitino Trino connector provides the following stored procedures to create, delete, and alter catalogs.
User can also use the system table `catalog` to describe all the catalogs.

Create catalog:

```sql
create_catalog(CATALOG varchar, PROVIDER varchar, PROPERTIES MAP(VARCHAR, VARCHAR), IGNORE_EXIST boolean);
```

- CATALOG: The catalog name to be created.
- PROVIDER: The catalog provider. Supported values: `hive`, `lakehouse-iceberg`, `jdbc-mysql`, `jdbc-postgresql`, `glue`.
- PROPERTIES: The properties of the catalog.
- IGNORE_EXIST: The flag to ignore the error if the catalog already exists. It's optional, the default value is `false`.

The type of catalog properties reference:
- [Hive catalog](../apache-hive-catalog.md#catalog-properties)
- [Iceberg catalog](../lakehouse-iceberg-catalog.md#catalog-properties)
- [MySQL catalog](../jdbc-mysql-catalog.md#catalog-properties)
- [PostgreSQL catalog](../jdbc-postgresql-catalog.md#catalog-properties)
- [AWS Glue catalog](../aws-glue-catalog.md#catalog-properties)


Drop catalog:

```sql
drop_catalog(CATALOG varchar, IGNORE_NOT_EXIST boolean);
```

- CATALOG: The catalog name to be deleted.
- IGNORE_NOT_EXIST: The flag to ignore the error if the catalog does not exist. It's optional, the default value is `false`.


Alter catalog:

```sql
alter_catalog(CATALOG varchar, SET_PROPERTIES MAP(VARCHAR, VARCHAR), REMOVE_PROPERTIES ARRY[VARCHAR]);
```

- CATALOG: The catalog name to be altered.
- SET_PROPERTIES: The properties to be set.
- REMOVE_PROPERTIES: The properties to be removed.

These stored procedures are under the `gravitino` connector and the `system` schema.
So you need to use the following SQL to call them in the `trino-cli`:


Describe catalogs:

The system table `gravitino.system.catalog` is used to describe all the catalogs.

```sql
select * from gravitino.system.catalog;
```

The result is like:

```test
     name     | provider |                                                 properties
--------------+----------+-------------------------------------------------------------------------------------------------------------
 gt_hive      | hive     | {gravitino.bypass.hive.metastore.client.capability.check=false, metastore.uris=thrift://trino-ci-hive:9083}
```

Check catalog registration status:

`gravitino.system.catalog` lists the relational catalogs the Gravitino server knows about, minus any
that match `gravitino.trino.skip-catalog-patterns`. A catalog listed there is not necessarily usable
in Trino: registering it is a separate step that can fail. `gravitino.system.catalog_status` covers
every catalog the connector considered, including the ones `catalog` filters out, and says why each
one is or is not registered.

```sql
select catalog_name, status, last_error from gravitino.system.catalog_status;
```

The result is like:

```test
 catalog_name | status     | last_error
--------------+------------+-------------------------------------------------
 gt_hive      | REGISTERED | NULL
 gt_iceberg   | FAILED     | Access Denied: Cannot create catalog gt_iceberg
 gt_files     | UNSUPPORTED| Only relational catalogs are supported, the catalog type is FILESET
```

| Column               | Description                                                                                  |
|----------------------|----------------------------------------------------------------------------------------------|
| `metalake`           | The metalake the catalog belongs to.                                                           |
| `catalog_name`       | The name of the catalog in Gravitino.                                                          |
| `trino_catalog_name` | The name the catalog is registered under in Trino, as it appears in `SHOW CATALOGS`.           |
| `provider`           | The catalog provider, for example `hive` or `lakehouse-iceberg`.                               |
| `status`             | One of `REGISTERED`, `FAILED`, `UNSUPPORTED` or `SKIPPED`. See the table below.                |
| `last_error`         | The reason the catalog is not registered, `NULL` when it is.                                   |
| `last_attempt_time`  | When the catalog was last processed, as an ISO-8601 UTC timestamp.                             |
| `last_success_time`  | When the catalog was last registered successfully, `NULL` if it never was. Retained when a catalog later fails or becomes unsupported. |
| `failure_count`      | The number of consecutive failed attempts, `0` when the last attempt succeeded.                |

| Status        | Meaning                                                                                       |
|---------------|-----------------------------------------------------------------------------------------------|
| `REGISTERED`  | The catalog is registered in Trino and appears in `SHOW CATALOGS`.                              |
| `FAILED`      | The last registration attempt failed, `last_error` carries the reason. Retried every refresh.   |
| `UNSUPPORTED` | The catalog is not relational, or its provider is not supported by the connector.               |
| `SKIPPED`     | The catalog matches `gravitino.trino.skip-catalog-patterns` and is deliberately not registered. |

A failure that stops the connector before it can list catalogs at all, such as an unreachable
Gravitino server, leaves no row to attach itself to. `gravitino.system.load_status` reports the
health of the loop itself, and always has exactly one row.

```sql
select * from gravitino.system.load_status;
```

| Column                 | Description                                                                             |
|------------------------|-------------------------------------------------------------------------------------------|
| `trino_started`        | Whether the Trino server has been observed reachable over JDBC. No catalog is registered until it is. Latched: once true it stays true, so it is not a liveness probe. |
| `last_attempt_time`    | When the loop last ran, as an ISO-8601 UTC timestamp.                                       |
| `last_success_time`    | When the loop last completed successfully, `NULL` if it never did.                          |
| `consecutive_failures` | The number of consecutive failed runs, `0` when the last run succeeded.                     |
| `last_error`           | The reason the last run did not complete, including waiting for Trino to start, `NULL` when it succeeded. |
| `metalake_errors`      | A JSON map of metalake name to its last error, `NULL` when every metalake loaded. A metalake that fails here also fails the run as a whole. |

Both tables are served by the coordinator and reflect the last refresh, which runs every
`gravitino.metadata.refresh-interval-seconds` seconds (10 by default). A catalog created moments ago
may not have been processed yet.

Example:
Run the following SQL to create a catalog named `mysql` with `jdbc-mysql` provider.

```sql
-- Call stored procedures with position.
call gravitino.system.create_catalog(
    'mysql',
    'jdbc-mysql',
    Map(
        Array['jdbc-url', 'jdbc-user', 'jdbc-password', 'jdbc-driver'],
        Array['jdbc:mysql://192.168.164.4:3306?useSSL=false', 'trino', 'ds123', 'com.mysql.cj.jdbc.Driver']
    )
);
call gravitino.system.drop_datalog('mysql');

-- Call stored procedures with name.
call gravitino.system.create_catalog(
    catalog =>'mysql',
    provider => 'jdbc-mysql',
    properties => Map(
        Array['jdbc-url', 'jdbc-user', 'jdbc-password', 'jdbc-driver'],
        Array['jdbc:mysql://192.168.164.4:3306?useSSL=false', 'trino', 'ds123', 'com.mysql.cj.jdbc.Driver']
    ),
    ignore_exist => true
);

call gravitino.system.drop_datalog(
    catalog => 'mysql'
    ignore_not_exist => true
);

call gravitino.system.alter_catalog(
    catalog => 'mysql',
    set_properties=> Map(
        Array['jdbc-url'],
        Array['jdbc:mysql://127.0.0.1:3306?useSSL=false']
    ),
    remove_properties => Array['jdbc-driver']
);
```

If you need more information about catalog, refer to:
[Create a Catalog](../manage-catalogs-and-schemas.md#create-a-catalog).

## Pass Trino Connector Configuration

A Gravitino catalog is implemented by the Trino connector, so you can pass the Trino connector configuration to the Gravitino catalog.
For example, you want to set the `hive.config.resources` configuration for the Hive catalog, you can pass the configuration to the
Gravitino catalog like this:

```sql
call gravitino.system.create_catalog(
    'gt_hive',
    'hive',
    map(
        array['metastore.uris', 'trino.bypass.hive.config.resources'],
        array['thrift://trino-ci-hive:9083', '/tmp/hive-site.xml,/tmp/core-site.xml']
    )
);
```

A prefix with `trino.bypass.` in the configuration key is used to indicate Gravitino Trino connector to pass the Trino connector configuration to the Gravitino catalog in the Trino runtime.

Note that if Trino connector properties directly inherit values from a Gravitino catalog, these configurations cannot be overridden through any `trino.bypass.*` properties.
For example, the Trino MySQL connector properties `connection-url`, `connection-user` and `connection-password` directly inherit the `jdbc-url`, `jdbc-user` and `jdbc-password` values defined in the Gravitino MySQL catalog.
Therefore, defining `trino.bypass.connection-url`, `trino.bypass.connection-user` or `trino.bypass.connection-password` will be ommitted and does not take effect.

More Trino connector configurations can refer to:
- [Hive catalog](https://trino.io/docs/current/connector/hive.html#hive-general-configuration-properties)
- [Iceberg catalog](https://trino.io/docs/current/connector/iceberg.html#general-configuration)
- [MySQL catalog](https://trino.io/docs/current/connector/mysql.html#general-configuration-properties)
- [PostgreSQL catalog](https://trino.io/docs/current/connector/postgresql.html#general-configuration-properties)

## Data Type Mapping Between Trino and Apache Gravitino

Gravitino Trino connector supports the following data type conversions between Trino and Gravitino currently. Depending on the detailed catalog, Gravitino may not support some data types conversion for this specific catalog, for example,
Hive does not support `TIME` data type.

| Gravitino Type        | Trino Type               |
|-----------------------|--------------------------|
| Boolean               | BOOLEAN                  |
| Byte                  | TINYINT                  |
| Short                 | SMALLINT                 |
| Integer               | INTEGER                  |
| Long                  | BIGINT                   |
| Float                 | REAL                     |
| Double                | DOUBLE                   |
| Decimal               | DECIMAL                  |
| String                | VARCHAR                  |
| Varchar               | VARCHAR                  |
| FixedChar             | CHAR                     |
| Binary                | VARBINARY                |
| Date                  | DATE                     |
| Time                  | TIME                     |
| Timestamp             | TIMESTAMP                |
| TimestampWithTimezone | TIMESTAMP WITH TIME ZONE |
| List                  | ARRAY                    |
| Map                   | MAP                      |
| Struct                | ROW                      |

For more about Trino data types, refer to [Trino data types](https://trino.io/docs/current/language/types.html) and Gravitino data types, refer to [Gravitino data types](../tables-and-views.md#table-column-type).

## Troubleshooting

Registration happens in the background, so a catalog that fails to register simply never appears in
`SHOW CATALOGS`. Start from `gravitino.system.catalog_status` rather than the coordinator log.

| Symptom                                                            | Likely cause                                                                                                      |
|--------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| A catalog is missing from `SHOW CATALOGS`                            | Query `gravitino.system.catalog_status` and read `status` and `last_error`, then follow the rows below              |
| `status = FAILED`, `last_error` mentions `Access Denied`             | The `trino.jdbc.user` lacks a Trino system role permitted to run `CREATE CATALOG`                              |
| `status = FAILED`, `last_error` mentions a configuration property    | A `trino.bypass.` property is not accepted by the underlying Trino connector                                        |
| `status = UNSUPPORTED`                                               | The catalog is not relational, or its provider is outside the supported list. `last_error` names the supported providers |
| `status = SKIPPED`                                                   | The catalog matches `gravitino.trino.skip-catalog-patterns`                                                         |
| The catalog has no row in `catalog_status` at all                    | The load loop never reached it. Check `gravitino.system.load_status`                                                |
| `load_status.trino_started = false`                                  | The connector cannot reach Trino over JDBC. `last_error` carries the connection error. Check `discovery.uri`, `trino.jdbc.user` and `trino.jdbc.password` |
| `load_status.last_error` mentions connection refused                 | The Gravitino server is unreachable. Check `gravitino.uri`                                                          |
| `load_status.metalake_errors` names a metalake                       | That metalake could not be listed, the other metalakes are unaffected                                               |
| Querying `gravitino.system.catalog_status` itself fails              | The entry catalog did not initialise. The error is reported when the entry catalog is created, check the Trino server log at startup |
