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

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

Gravitino provides the ability to manage MySQL metadata.

:::caution
Gravitino saves some system information in schema and table comment, like `(From Gravitino, DO NOT EDIT: gravitino.v1.uid1078334182909406185)`, please don't change or remove this message.
:::

## Catalog

### Catalog capabilities

- Gravitino catalog corresponds to the MySQL instance.
- Supports metadata management of MySQL (5.7, 8.0).
- Supports DDL operation for MySQL databases and tables.
- Supports table index.
- Supports [column default value](./manage-metadata-using-gravitino.md#table-column-default-value) and [auto-increment](./manage-metadata-using-gravitino.md#table-column-auto-increment).
- Supports managing MySQL table features though table properties, like using `engine` to set MySQL storage engine.

### Catalog properties

Any property that isn't defined by Gravitino can pass to MySQL data source by adding `gravitino.bypass` prefix as a catalog property. For example, catalog property `gravitino.bypass.maxWaitMillis` will pass `maxWaitMillis` to the data source property.
You can check the relevant data source configuration in [data source properties](https://commons.apache.org/proper/commons-dbcp/configuration.html)

If you use JDBC catalog, you must provide `jdbc-url`, `jdbc-driver`, `jdbc-user` and `jdbc-password` to catalog properties.

| Configuration item      | Description                                                                                                | Default value | Required | Since Version |
|-------------------------|------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `jdbc-url`              | JDBC URL for connecting to the database. For example `jdbc:mysql://localhost:3306`                         | (none)        | Yes      | 0.3.0         |
| `jdbc-driver`           | The driver of the JDBC connection. For example `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver`.      | (none)        | Yes      | 0.3.0         |
| `jdbc-user`             | The JDBC user name.                                                                                        | (none)        | Yes      | 0.3.0         |
| `jdbc-password`         | The JDBC password.                                                                                         | (none)        | Yes      | 0.3.0         |
| `jdbc.pool.min-size`    | The minimum number of connections in the pool. `2` by default.                                             | `2`           | No       | 0.3.0         |
| `jdbc.pool.max-size`    | The maximum number of connections in the pool. `10` by default.                                            | `10`          | No       | 0.3.0         |

:::caution
You must download the corresponding JDBC driver to the `catalogs/jdbc-mysql/libs` directory.
:::

### Catalog operations

Please refer to [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema capabilities

- Gravitino schema corresponds to the MySQL database.
- Supports creating schema, but does not support setting comment.
- Supports dropping schema.
- Doesn't support cascade dropping schema.

### Schema properties

- Doesn't support any schema property settings.

### Schema operations

Please refer to [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino.md#schema-operations) for more details.

## Table

### Table capabilities

- Gravitino table corresponds to the MySQL table.
- Supports DDL operation for MySQL tables.
- Supports index.
- Supports [column default value](./manage-metadata-using-gravitino.md#table-column-default-value) and [auto-increment](./manage-metadata-using-gravitino.md#table-column-auto-increment)..
- Supports managing MySQL table features though table properties, like using `engine` to set MySQL storage engine.

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
MySQL doesn't support Gravitino `Boolean` `Fixed` `Struct` `List` `Map` `Timestamp_tz` `IntervalDay` `IntervalYear` `Union` `UUID` type.
Meanwhile, the data types other than listed above are mapped to Gravitino **[Unparsed Type](./manage-metadata-using-gravitino.md#unparsed-type)** that represents an unresolvable data type since 0.5.0.
:::

#### Table column auto-increment

:::note
MySQL setting an auto-increment column requires simultaneously setting a unique index; otherwise, an error will occur.
:::

<Tabs>
<TabItem value="json" label="Json">

```json
{
  "columns": [
    {
      "name": "id",
      "type": "integer",
      "comment": "id column comment",
      "nullable": false,
      "autoIncrement": true
    },
    {
      "name": "name",
      "type": "varchar(500)",
      "comment": "name column comment",
      "nullable": true,
      "autoIncrement": false
    }
  ],
  "indexes": [
    {
      "indexType": "primary_key",
      "name": "PRIMARY",
      "fieldNames": [["id"]]
    }
  ]
}
```

</TabItem>
<TabItem value="java" label="Java">

```java
Column[] cols = new Column[] {
    Column.of("id", Types.IntegerType.get(), "id column comment", false, true, null),
    Column.of("name", Types.VarCharType.of(500), "Name of the user", true, false, null)
};
Index[] indexes = new Index[] {
    Indexes.of(IndexType.PRIMARY_KEY, "PRIMARY", new String[][]{{"id"}})
}
```

</TabItem>
</Tabs>

### Table properties

Although MySQL itself does not support table properties, Gravitino offers table property management for MySQL tables through the `jdbc-mysql` catalog, enabling control over table features. The supported properties are listed as follows:

| Property Name           | Description                                                                                                                                                                             | Required  | Since version |
|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|---------------|
| `engine`                | The engine used by the table. The default value is `InnoDB`. For example `MyISAM`, `MEMORY`, `CSV`, `ARCHIVE`, `BLACKHOLE`, `FEDERATED`, `ndbinfo`, `MRG_MYISAM`, `PERFORMANCE_SCHEMA`. | No        | 0.4.0         |
| `auto-increment-offset` | Used to specify the starting value of the auto-increment field.                                                                                                                         | No        | 0.4.0         |

- Doesn't support remove table properties. You can only modify values, not delete properties.

### Table indexes

- Supports PRIMARY_KEY and UNIQUE_KEY.

:::note
The index name of the PRIMARY_KEY must be PRIMARY
[Create table index](https://dev.mysql.com/doc/refman/8.0/en/create-table.html)
:::

<Tabs>
<TabItem value="json" label="Json">

```json
{
  "indexes": [
    {
      "indexType": "primary_key",
      "name": "PRIMARY",
      "fieldNames": [["id"]]
    },
    {
      "indexType": "unique_key",
      "name": "id_name_uk",
      "fieldNames": [["id"] ,["name"]]
    }
  ]
}
```

</TabItem>
<TabItem value="java" label="Java">

```java
Index[] indexes = new Index[] {
    Indexes.of(IndexType.PRIMARY_KEY, "PRIMARY", new String[][]{{"id"}}),
    Indexes.of(IndexType.UNIQUE_KEY, "id_name_uk", new String[][]{{"id"} , {"name"}}),
}
```

</TabItem>
</Tabs>

### Table operations

Please refer to [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino.md#table-operations) for more details.

#### Alter table operations

Supports operations:
- `RenameTable`
- `UpdateComment`
- `AddColumn`
- `DeleteColumn`
- `RenameColumn`
- `UpdateColumnType`
- `UpdateColumnPosition`
- `UpdateColumnNullability`
- `UpdateColumnComment`
- `SetProperty`

:::info
You cannot submit the `RenameTable` operation at the same time as other operations.
:::

:::caution
If you update a nullability column to non nullability, there may be compatibility issues.
:::
