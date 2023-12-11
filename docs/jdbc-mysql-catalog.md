---
title: "How to use Gravitino to manage MySQL metadata"
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
- Supports DDL operation for MySQL databases and tables.
- Doesn't support table index operations.
- Doesn't support setting certain column properties, such as default value and check constraints.

### Catalog properties
Any properties not defined by Gravitino with `gravitino.bypass` prefix will pass to MySQL catalog data srouce properties. For example, if specify `gravitino.bypass.maxWaitMillis`, `maxWaitMillis` will pass to MySQL catalog data source properties.
You can check the relevant datasource configuration in [datasource properties](https://commons.apache.org/proper/commons-dbcp/configuration.html)

If you are using JDBC catalog, you must provide `jdbc-url`, `jdbc-driver`, `jdbc-user` and `jdbc-password` to catalog properties.

| Configuration item      | Description                                                      | Example value                 | Since Version |
|-------------------------|------------------------------------------------------------------|-------------------------------|---------------|
| `jdbc-url`              | JDBC URL for connecting to the database.                         | `jdbc:mysql://localhost:3306` | 0.3.0         |
| `jdbc-driver`           | `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver` for MySQL. | `com.mysql.jdbc.Driver`       | 0.3.0         |
| `jdbc-user`             | The JDBC user name.                                              | `root`                        | 0.3.0         |
| `jdbc-password`         | The JDBC password.                                               | `root`                        | 0.3.0         |
| `jdbc.pool.min-size`    | The minimum number of connections in the pool. `2` by default.   | `2`                           | 0.3.0         |
| `jdbc.pool.max-size`    | The maximum number of connections in the pool. `10` by default.  | `10`                          | 0.3.0         |

### Catalog operations
see [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino#catalogs-operations).

### Catalog other
:::caution
You must download the corresponding JDBC driver to the `catalogs/jdbc-mysql/libs` directory.
:::

## Schema
### Schema capabilities
- Doesn't support cascade drop database.

### Schema properties
- Doesn't support are database property settings.

### Schema operations
see [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino#schemas-operations).

## Table
### Table capabilities
- Supports DDL operation for MySQL tables.
- Doesn't support setting certain column properties, such as default value and check constraints.
- Doesn't support index definition.
- Doesn't support table property settings.

#### Table column types
| Gravitino Type   | Mysql Type  |
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
MySQL doesn't support Gravitino `Boolean` `Fixed` `Struct` `List` `Map` `Timestamp_tz` `IntervalDay` `IntervalYear` `Union` `UUID` type.
:::

### Table properties
- Doesn't support table properties.

### Table operations
see [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino#tables-operations).