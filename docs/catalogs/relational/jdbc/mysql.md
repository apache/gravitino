---
title: "MySQL catalog"
slug: /jdbc-mysql-catalog
keywords:
- jdbc
- MySQL
- metadata
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

Apache Gravitino provides the ability to manage MySQL metadata.

:::caution
Gravitino saves some system information in schema and table comment,
like `(From Gravitino, DO NOT EDIT: gravitino.v1.uid1078334182909406185)`,
please don't change or remove this message.
:::

## Catalog

### Catalog capabilities

- Gravitino catalog corresponds to the MySQL instance.
- Supports metadata management of MySQL (5.7, 8.0).
- Supports DDL operation for MySQL databases and tables.
- Supports table index.
- Supports [column default value](../../../manage-relational-metadata-using-gravitino.md#table-column-default-value)
  and [auto-increment](../../../manage-relational-metadata-using-gravitino.md#table-column-auto-increment).
- Supports managing MySQL table features though table properties, like using `engine` to set MySQL storage engine.

### Catalog properties

You can pass to a MySQL data source any property that isn't defined by Gravitino
by adding `gravitino.bypass.` prefix as a catalog property.
For example, catalog property `gravitino.bypass.maxWaitMillis` will pass `maxWaitMillis`
to the data source property.

Check the relevant data source configuration in
[data source properties](https://commons.apache.org/proper/commons-dbcp/configuration.html)

When you use the Gravitino with Trino, you can pass the Trino MySQL connector configuration using prefix `trino.bypass.`.
For example, using `trino.bypass.join-pushdown.strategy` to pass the `join-pushdown.strategy`
to the Gravitino MySQL catalog in Trino runtime.

If you use a JDBC catalog, you must provide `jdbc-url`, `jdbc-driver`, `jdbc-user`
and `jdbc-password` to catalog properties.
Besides the [common catalog properties](../../../gravitino-server-config.md#gravitino-catalog-properties-configuration),
the MySQL catalog has the following properties:

| Configuration item   | Description                                                                                            | Default value | Required | Since Version |
|----------------------|--------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `jdbc-url`           | JDBC URL for connecting to the database. For example, `jdbc:mysql://localhost:3306`                    | (none)        | Yes      | 0.3.0         |
| `jdbc-driver`        | The driver of the JDBC connection. For example, `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver`. | (none)        | Yes      | 0.3.0         |
| `jdbc-user`          | The JDBC user name.                                                                                    | (none)        | Yes      | 0.3.0         |
| `jdbc-password`      | The JDBC password.                                                                                     | (none)        | Yes      | 0.3.0         |
| `jdbc.pool.min-size` | The minimum number of connections in the pool. `2` by default.                                         | `2`           | No       | 0.3.0         |
| `jdbc.pool.max-size` | The maximum number of connections in the pool. `10` by default.                                        | `10`          | No       | 0.3.0         |

:::caution
You must download the corresponding JDBC driver to the `catalogs/jdbc-mysql/libs` directory.
:::

### Catalog operations

Refer to [managing relational metadata](../../../manage-relational-metadata-using-gravitino.md#catalog-operations).

## Schema

### Schema capabilities

- Gravitino's schema concept corresponds to the MySQL database.
- Supports creating schema, but does not support setting comment.
- Supports dropping schema.
- Supports cascade dropping schema.

### Schema properties

- Doesn't support any schema property settings.

### Schema operations

Refer to [manage relational metadata](../../../manage-relational-metadata-using-gravitino.md#schema-operations).

## Table

### Table capabilities

- Gravitino's table concept corresponds to the MySQL table.
- Supports DDL operation for MySQL tables.
- Supports index.
- Supports [column default value](../../../manage-relational-metadata-using-gravitino.md#table-column-default-value)
  and [auto-increment](../../../manage-relational-metadata-using-gravitino.md#table-column-auto-increment).
- Supports managing MySQL table features though table properties,
  like using `engine` to set MySQL storage engine.

### Table column types

| Gravitino Type     | MySQL Type          |
|--------------------|---------------------|
| `BOOLEAN`          | `BIT`               |
| `Binary`           | `Binary`            |
| `Byte`             | `Tinyint`           |
| `Date`             | `Date`              |
| `Decimal`          | `Decimal`           |
| `Double`           | `Double`            |
| `FixedChar`        | `FixedChar`         |
| `Float`            | `Float`             |
| `Integer`          | `Int`               |
| `Long`             | `Bigint`            |
| `Short`            | `Smallint`          |
| `String`           | `Text`              |
| `Time`             | `Time`              |
| `Timestamp`        | `Timestamp`         |
| `Unsigned Byte`    | `Tinyint Unsigned`  |
| `Unsigned Integer` | `Int Unsigned`      |
| `Unsigned Long`    | `Bigint Unsigned`   |
| `Unsigned Short`   | `Smallint Unsigned` |
| `VarChar`          | `VarChar`           |

:::info
MySQL doesn't support Gravitino `Fixed`, `Struct`, `List`, `Map`, `Timestamp_tz`,
`IntervalDay`, `IntervalYear`, `Union`, or `UUID` type.
Meanwhile, the data types other than listed above are mapped to Gravitino
**[External Type](../../../manage-relational-metadata-using-gravitino.md#external-type)**
that represents an unresolvable data type since 0.6.0-incubating.
:::

### Table column auto-increment

:::note
MySQL setting an auto-increment column requires simultaneously setting a unique index;
otherwise, an error will occur.
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
};
```

</TabItem>
</Tabs>

### Table properties

Although MySQL itself does not support table properties,
Gravitino offers table property management for MySQL tables
through the `jdbc-mysql` catalog, enabling control over table features.
The supported properties are listed as follows:

:::note
**Reserved**: Fields that cannot be passed to the Gravitino server.

**Immutable**: Fields that cannot be modified once set.
:::

:::caution
- Doesn't support remove table properties. You can only add or modify properties, not delete properties.
:::

| Property Name           | Description                                                                                                                                              | Default Value | Required  | Reserved   | Immutable | Since version |
|-------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|-----------|------------|-----------|---------------|
| `engine`                | The engine used by the table. For example `MyISAM`, `MEMORY`, `CSV`, `ARCHIVE`, `BLACKHOLE`, `FEDERATED`, `ndbinfo`, `MRG_MYISAM`, `PERFORMANCE_SCHEMA`. | `InnoDB`      | No        | No         | Yes       | 0.4.0         |
| `auto-increment-offset` | Used to specify the starting value of the auto-increment field.                                                                                          | (none)        | No        | No         | Yes       | 0.4.0         |


:::note
Some MySQL storage engines, such as FEDERATED, are not enabled by default and require additional configuration to use.
For example, to enable the FEDERATED engine, set federated=1 in the MySQL configuration file.
Similarly, engines like ndbinfo, MRG_MYISAM, and PERFORMANCE_SCHEMA may also require specific prerequisites
or configurations. For detailed instructions, refer to the
[MySQL documentation](https://dev.mysql.com/doc/refman/8.0/en/federated-storage-engine.html).
:::

### Table indexes

- Supports PRIMARY_KEY and UNIQUE_KEY.

:::note
The index name of the PRIMARY_KEY must be PRIMARY
[Create table index](https://dev.mysql.com/doc/refman/8.0/en/create-table.html)
:::

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
};
```

</TabItem>
</Tabs>

### Table operations

Refer to [managing relational metadata](../../../manage-relational-metadata-using-gravitino.md#table-operations).

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
