---
title: "How to use Gravitino to manage PostgreSQL metadata"
slug: /jdbc-postgresql-catalog
keywords:
- jdbc
- PostgreSQL
- metadata
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

## Introduction
Gravitino provides the ability to manage PostgreSQL metadata.

## Catalog
### Catalog capabilities
- Supports DDL operation for PostgreSQL schemas and tables.
- Doesn't support table index operations.
- Doesn't support setting certain column properties, such as default value and check constraints

### Catalog properties
Any properties not defined by Gravitino with `gravitino.bypass` prefix will pass to PostgreSQL catalog data srouce properties. For example, if specify `gravitino.bypass.maxWaitMillis`, `maxWaitMillis` will pass to PostgreSQL catalog data source properties.
You can check the relevant data source configuration in [data source properties](https://commons.apache.org/proper/commons-dbcp/configuration.html)

If you are using JDBC catalog, you must provide `jdbc-url`, `jdbc-driver`, `jdbc-database`, `jdbc-user` and `jdbc-password` to catalog properties.

| Configuration item   | Description                                                                                               | Example value                                                  | Since Version |
|----------------------|-----------------------------------------------------------------------------------------------------------|----------------------------------------------------------------|---------------|
| `jdbc-url`           | JDBC URL for connecting to the database. You need to specify the database in the URL.                     | `jdbc:postgresql://localhost:3306/pg_database?sslmode=require` | 0.3.0         |
| `jdbc-driver`        | `org.postgresql.Driver` for PostgreSQL.                                                                   | `org.postgresql.Driver`                                        | 0.3.0         |
| `jdbc-database`      | The database of the JDBC connection. Needs to be configured with the same value as database in `jdbc-url` | `pg_database`                                                  | 0.3.0         |
| `jdbc-user`          | The JDBC user name.                                                                                       | `root`                                                         | 0.3.0         |
| `jdbc-password`      | The JDBC password.                                                                                        | `root`                                                         | 0.3.0         |
| `jdbc.pool.min-size` | The minimum number of connections in the pool. `2` by default.                                            | `2`                                                            | 0.3.0         |
| `jdbc.pool.max-size` | The maximum number of connections in the pool. `10` by default.                                           | `10`                                                           | 0.3.0         |

:::caution
You must download the corresponding JDBC driver to the `catalogs/jdbc-postgresql/libs` directory.
You must explicitly specify the database in both `jdbc-url` and `jdbc-database`. The values in both must be consistent; otherwise, an error will occur.
:::
:::info
In PostgreSQL, the database corresponds to the Gravitino catalog, and the schema corresponds to the Gravitino schema.
:::

### Catalog operations
see [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino#catalogs-operations).

## Schema
### Schema capabilities
- Support create schema with comments.
- Support drop schema.
- Support cascade drop schema.

### Schema properties
- Doesn't are schema property settings.

### Schema operations
see [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino#schemas-operations).

## Table
### Table capabilities
- Supports DDL operation for PostgreSQL tables.
- Doesn't support setting certain column properties, such as default value and check constraints.
- Doesn't support index definition.
- Doesn't support table property settings.

#### Table column types
| Gravitino Type | PostgreSQL Type               |
|----------------|-------------------------------|
| `Boolean`      | `boolean`                     |
| `Byte`         | `Tinyint`                     |
| `Short`        | `Smallint`                    |
| `Integer`      | `Integer`                     |
| `Long`         | `Bigint`                      |
| `Float`        | `Real`                        |
| `Double`       | `Double precision`            |
| `String`       | `Text`                        |
| `Date`         | `Date`                        |
| `Time`         | `Time without time zone`      |
| `Timestamp`    | `Timestamp without time zone` |
| `Timestamp_tz` | `Timestamp with time zone`    |
| `Decimal`      | `Numeric`                     |
| `VarChar`      | `Character varying`           |
| `FixedChar`    | `FixedChar`                   |
| `Binary`       | `Bytea`                       |

:::info
PostgreSQL doesn't support Gravitino `Fixed` `Struct` `List` `Map` `IntervalDay` `IntervalYear` `Union` `UUID` type.
:::

### Table properties
- Doesn't support table properties.

### Table operations
see [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino#tables-operations).