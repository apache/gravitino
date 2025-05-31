---
title: "OceanBase catalog"
slug: /jdbc-oceanbase-catalog
keywords:
- jdbc
- OceanBase
- metadata
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

Apache Gravitino provides the ability to manage OceanBase metadata.

:::caution
Gravitino saves some system information in schema and table comment, like `(From Gravitino, DO NOT EDIT: gravitino.v1.uid1078334182909406185)`, please don't change or remove this message.
:::

## Catalog

### Catalog capabilities

- Gravitino catalog corresponds to the OceanBase instance.
- Supports metadata management of OceanBase (4.x).
- Supports DDL operation for OceanBase databases and tables.
- Supports table index.
- Supports [column default value](./manage-relational-metadata-using-gravitino.md#table-column-default-value) and [auto-increment](./manage-relational-metadata-using-gravitino.md#table-column-auto-increment).

### Catalog properties

You can pass to a OceanBase data source any property that isn't defined by Gravitino by adding `gravitino.bypass.` prefix as a catalog property. For example, catalog property `gravitino.bypass.maxWaitMillis` will pass `maxWaitMillis` to the data source property.

Check the relevant data source configuration in [data source properties](https://commons.apache.org/proper/commons-dbcp/configuration.html)

If you use a JDBC catalog, you must provide `jdbc-url`, `jdbc-driver`, `jdbc-user` and `jdbc-password` to catalog properties.
Besides the [common catalog properties](./gravitino-server-config.md#apache-gravitino-catalog-properties-configuration), the OceanBase catalog has the following properties:

| Configuration item   | Description                                                                                                                           | Default value | Required | Since Version    |
|----------------------|---------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `jdbc-url`           | JDBC URL for connecting to the database. For example, `jdbc:mysql://localhost:2881` or `jdbc:oceanbase://localhost:2881`              | (none)        | Yes      | 0.7.0-incubating |
| `jdbc-driver`        | The driver of the JDBC connection. For example, `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver` or `com.oceanbase.jdbc.Driver`. | (none)        | Yes      | 0.7.0-incubating |
| `jdbc-user`          | The JDBC user name.                                                                                                                   | (none)        | Yes      | 0.7.0-incubating |
| `jdbc-password`      | The JDBC password.                                                                                                                    | (none)        | Yes      | 0.7.0-incubating |
| `jdbc.pool.min-size` | The minimum number of connections in the pool. `2` by default.                                                                        | `2`           | No       | 0.7.0-incubating |
| `jdbc.pool.max-size` | The maximum number of connections in the pool. `10` by default.                                                                       | `10`          | No       | 0.7.0-incubating |

:::caution
Before using the OceanBase Catalog, you must download the corresponding JDBC driver to the `catalogs/jdbc-oceanbase/libs` directory.
Gravitino doesn't package the JDBC driver for OceanBase due to licensing issues.
:::

### Catalog operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema capabilities

- Gravitino's schema concept corresponds to the OceanBase database.
- Supports creating schema, but does not support setting comment.
- Supports dropping schema.
- Supports cascade dropping schema.

### Schema properties

- Doesn't support any schema property settings.

### Schema operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more details.

## Table

### Table capabilities

- Gravitino's table concept corresponds to the OceanBase table.
- Supports DDL operation for OceanBase tables.
- Supports index.
- Supports [column default value](./manage-relational-metadata-using-gravitino.md#table-column-default-value) and [auto-increment](./manage-relational-metadata-using-gravitino.md#table-column-auto-increment)..

### Table properties

- Doesn't support table properties.

### Table column types

| Gravitino Type    | OceanBase Type      |
|-------------------|---------------------|
| `Byte`            | `Tinyint`           |
| `Byte(false)`     | `Tinyint Unsigned`  |
| `Short`           | `Smallint`          |
| `Short(false)`    | `Smallint Unsigned` |
| `Integer`         | `Int`               |
| `Integer(false)`  | `Int Unsigned`      |
| `Long`            | `Bigint`            |
| `Long(false)`     | `Bigint Unsigned`   | 
| `Float`           | `Float`             |
| `Double`          | `Double`            |
| `String`          | `Text`              |
| `Date`            | `Date`              |
| `Time`            | `Time`              |
| `Timestamp`       | `Timestamp`         |
| `Decimal`         | `Decimal`           |
| `VarChar`         | `VarChar`           |
| `FixedChar`       | `FixedChar`         |
| `Binary`          | `Binary`            |

:::info
OceanBase doesn't support Gravitino `Boolean` `Fixed` `Struct` `List` `Map` `Timestamp_tz` `IntervalDay` `IntervalYear` `Union` `UUID` type.
Meanwhile, the data types other than listed above are mapped to Gravitino **[External Type](./manage-relational-metadata-using-gravitino.md#external-type)** that represents an unresolvable data type since 0.6.0-incubating.
:::

### Table column auto-increment

:::note
OceanBase setting an auto-increment column requires simultaneously setting a unique index; otherwise, an error will occur.
:::

<Tabs groupId='language' queryString>
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


### Table indexes

- Supports PRIMARY_KEY and UNIQUE_KEY.

<Tabs groupId='language' queryString>
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

:::note
The OceanBase catalog does not support creating partitioned tables in the current version.
:::

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for more details.

#### Alter table operations

Gravitino supports these table alteration operations:

- `RenameTable`
- `UpdateComment`
- `AddColumn`
- `DeleteColumn`
- `RenameColumn`
- `UpdateColumnType`
- `UpdateColumnPosition`
- `UpdateColumnNullability`
- `UpdateColumnComment`
- `UpdateColumnDefaultValue`
- `SetProperty`

:::info
 - You cannot submit the `RenameTable` operation at the same time as other operations.
 - If you update a nullability column to non-nullability, there may be compatibility issues.
:::
