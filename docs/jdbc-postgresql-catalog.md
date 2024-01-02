---
title: "PostgreSQL catalog"
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

- Gravitino catalog corresponds to the PostgreSQL database.
- Supports metadata management of PostgreSQL (9.2, 12.0, 13.0).
- Supports DDL operation for PostgreSQL schemas and tables.
- Doesn't support table index operations.
- Doesn't support setting certain column properties, such as default value and check constraints

### Catalog properties

Any property that isn't defined by Gravitino can pass to MySQL data source by adding `gravitino.bypass` prefix as a catalog property. For example, catalog property `gravitino.bypass.maxWaitMillis` will pass `maxWaitMillis` to the data source property.
You can check the relevant data source configuration in [data source properties](https://commons.apache.org/proper/commons-dbcp/configuration.html)

If you use JDBC catalog, you must provide `jdbc-url`, `jdbc-driver`, `jdbc-database`, `jdbc-user` and `jdbc-password` to catalog properties.

| Configuration item   | Description                                                                                                                                                       | Default value | Required | Since Version |
|----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `jdbc-url`           | JDBC URL for connecting to the database. You need to specify the database in the URL. For example `jdbc:postgresql://localhost:3306/pg_database?sslmode=require`. | (none)        | Yes      | 0.3.0         |
| `jdbc-driver`        | The driver of the JDBC connection. For example `org.postgresql.Driver`.                                                                                           | (none)        | Yes      | 0.3.0         |
| `jdbc-database`      | The database of the JDBC connection. Configure it with the same value as the database in the `jdbc-url`. For example `pg_database`.                             | (none)        | Yes      | 0.3.0         |
| `jdbc-user`          | The JDBC user name.                                                                                                                                               | (none)        | Yes      | 0.3.0         |
| `jdbc-password`      | The JDBC password.                                                                                                                                                | (none)        | Yes      | 0.3.0         |
| `jdbc.pool.min-size` | The minimum number of connections in the pool. `2` by default.                                                                                                    | `2`           | No       | 0.3.0         |
| `jdbc.pool.max-size` | The maximum number of connections in the pool. `10` by default.                                                                                                   | `10`          | No       | 0.3.0         |

:::caution
You must download the corresponding JDBC driver to the `catalogs/jdbc-postgresql/libs` directory.
You must explicitly specify the database in both `jdbc-url` and `jdbc-database`. An error may occur if the values in both aren't consistent.
:::
:::info
In PostgreSQL, the database corresponds to the Gravitino catalog, and the schema corresponds to the Gravitino schema.
:::

### Catalog operations

Please refer to [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino.md#catalogs-operations) for more details.

## Schema

### Schema capabilities

- Gravitino schema corresponds to the PostgreSQL schema.
- Supports create schema with comments.
- Supports drop schema.
- Supports cascade drop schema.

### Schema properties

- Doesn't are schema property settings.

### Schema operations

Please refer to [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino.md#schemas-operations) for more details.

## Table

### Table capabilities

- Gravitino table corresponds to the PostgreSQL table.
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

Please refer to [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino#tables-operations) for more details.

#### Alter table operations

Supports operations:

- `RenameTable`
- `UpdateComment`
- `AddColumn`
- `DeleteColumn`
- `RenameColumn`
- `UpdateColumnType`
- `UpdateColumnNullability`
- `UpdateColumnComment`

:::info
You can't submit the `RenameTable` operation at the same time as other operations.
:::

:::caution
PostgreSQL doesn't support the `UpdateColumnPosition` operation, so you can only use `ColumnPosition.defaultPosition()` when `AddColumn`.
If you update a nullability column to non nullability, there may be compatibility issue.
:::
