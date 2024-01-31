---
title: "MySQL catalog"
slug: /jdbc-mysql-catalog
keywords:
- jdbc
- MySQL
- metadata
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

## Introduction

Gravitino provides the ability to manage MySQL metadata.

## Catalog

### Catalog capabilities

- Supports metadata management of MySQL (5.6, 5.7, 8.0).
- Supports DDL operation for MySQL databases and tables.
- Doesn't support table index operations.
- Doesn't support setting certain column properties, such as default value and check constraints.

### Catalog properties

You can pass to a MySQL data source any property that isn't defined by Gravitino by adding `gravitino.bypass` prefix as a catalog property. For example, catalog property `gravitino.bypass.maxWaitMillis` will pass `maxWaitMillis` to the data source property.

Check the relevant data source configuration in [data source properties](https://commons.apache.org/proper/commons-dbcp/configuration.html)

If you use a JDBC catalog, you must provide `jdbc-url`, `jdbc-driver`, `jdbc-user` and `jdbc-password` to catalog properties.

| Configuration item      | Description                                                                                                | Default value | Required | Since Version |
|-------------------------|------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `jdbc-url`              | JDBC URL for connecting to the database. For example, `jdbc:mysql://localhost:3306`                         | (none)        | Yes      | 0.3.0         |
| `jdbc-driver`           | The driver of the JDBC connection. For example, `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver`.      | (none)        | Yes      | 0.3.0         |
| `jdbc-user`             | The JDBC user name.                                                                                        | (none)        | Yes      | 0.3.0         |
| `jdbc-password`         | The JDBC password.                                                                                         | (none)        | Yes      | 0.3.0         |
| `jdbc.pool.min-size`    | The minimum number of connections in the pool. `2` by default.                                             | `2`           | No       | 0.3.0         |
| `jdbc.pool.max-size`    | The maximum number of connections in the pool. `10` by default.                                            | `10`          | No       | 0.3.0         |

:::caution
You must download the corresponding JDBC driver to the `catalogs/jdbc-mysql/libs` directory.
:::

### Catalog operations

Refer to [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema capabilities

The Gravitino schema:

- corresponds to the MySQL database.
- Ssupports create schema with comments.
- supports drop schema.
- doesn't support cascade drop database.

### Schema properties

- Doesn't support any database property settings.

### Schema operations

Refer to [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino.md#schema-operations) for more details.

## Table

### Table capabilities

The Gravitino table:

- corresponds to the MySQL table.
- supports DDL operation for MySQL tables.
- doesn't support setting certain column properties, such as default value and check constraints.
- doesn't support index definition.
- doesn't support table property settings.

#### Table column types

| Gravitino Type   | MySQL Type  |
|------------------|-------------|
| `Byte`           | `Tinyint`   |
| `Short`          | `Smallint`  |
| `Integer`        | `Int`       |
| `Long`           | `Bigint`    |
| `Float`          | `Float`     |
| `Double`         | `Double`    |
| `String`         | `Text`      |
| `Date`           | `Date`      |
| `Time`           | `Time`      |
| `Timestamp`      | `Timestamp` |
| `Decimal`        | `Decimal`   |
| `VarChar`        | `VarChar`   |
| `FixedChar`      | `FixedChar` |
| `Binary`         | `Binary`    |

:::info
MySQL doesn't support Gravitino `Boolean`, `Fixed`, `Struct`, `List`, `Map`, `Timestamp_tz`, `IntervalDay`, `IntervalYear`, `Union`, or `UUID` types.
:::

### Table properties

The Gravitino table doesn't support table properties.

### Table operations

Refer to [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino.md#table-operations) for more details.

#### Alter table operations

The Gravitino table Supports these operations:
- `RenameTable`
- `UpdateComment`
- `AddColumn`
- `DeleteColumn`
- `RenameColumn`
- `UpdateColumnType`
- `UpdateColumnPosition`
- `UpdateColumnNullability`
- `UpdateColumnComment`

:::info
You cannot submit the `RenameTable` operation at the same time as other operations.
:::

:::caution
If you update a nullability column to non-nullability, there may be compatibility issues.
:::
