---
title: JDBC MySQL catalog
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

Apache Gravitino can be used to manage MySQL metadata.

:::caution
Gravitino saves some system information in schema and table comment,
like `(From Gravitino, DO NOT EDIT: gravitino.v1.uid1078334182909406185)`.

**Please don't change or remove this message.**
:::

## Catalog

### Catalog capabilities

- Gravitino catalog corresponds to the MySQL instance.
- This catalog supports managing metadata for MySQL (5.7, 8.0).
- The catalog supports DDL operation for MySQL databases and tables.
- Indexed tables are supported.
- [Column default value](../../../metadata/relational.md#table-column-default-value)
  and [auto-increment columns](../../../metadata/relational.md#table-column-auto-increment).
  are supported.
- This catalog supports managing MySQL table features through table properties,
  like using `engine` to set MySQL storage engine.

### Catalog properties

You can pass to a MySQL data source any property that isn't defined by Gravitino
by adding `gravitino.bypass.` as a catalog property name prefix.
For example, catalog property `gravitino.bypass.maxWaitMillis` will pass `maxWaitMillis`
to the data source property.

Check the relevant data source configuration in
[data source properties](https://commons.apache.org/proper/commons-dbcp/configuration.html)

When you use the Gravitino with Trino, you can pass the Trino MySQL connector configuration
using prefix `trino.bypass.`.
For example, using `trino.bypass.join-pushdown.strategy` to pass the `join-pushdown.strategy`
to the Gravitino MySQL catalog in Trino runtime.

If you use a JDBC catalog, you must provide `jdbc-url`, `jdbc-driver`, `jdbc-user`
and `jdbc-password` to catalog properties.
Besides the [common catalog properties](../../../admin/server-config.md#gravitino-catalog-properties-configuration),
the MySQL catalog has the following properties:

<table>
<thead>
<tr>
  <th>Property name</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>jdbc-url</tt></td>
  <td>
    JDBC URL for connecting to the database.
    For example, `jdbc:mysql://localhost:3306`.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>jdbc-driver</tt></td>
  <td>
    The driver of the JDBC connection.
    For example, `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver`.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>jdbc-user</tt></td>
  <td>The JDBC user name.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>jdbc-password</tt></td>
  <td>The JDBC password.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>jdbc.pool.min-size</tt></td>
  <td>
    The minimum number of connections in the pool.
  </td>
  <td>`2`</td>
  <td>No</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>jdbc.pool.max-size</tt></td>
  <td>
    The maximum number of connections in the pool.
  </td>
  <td>`10`</td>
  <td>No</td>
  <td>`0.3.0`</td>
</tr>
</tbody>
</table>

:::caution
You must download the corresponding JDBC driver to the `catalogs/jdbc-mysql/libs` directory.
:::

### Catalog operations

Refer to [managing relational metadata](../../../metadata/relational.md#catalog-operations).

## Schema

### Schema capabilities

- Gravitino's schema concept corresponds to the MySQL database.
- This catalog supports creating schema, but does not support setting comment.
- Dropping schema is supported.
- Cascaded dropping of schemas is supported.

### Schema properties

- No support to schema property settings.

### Schema operations

Refer to [manage relational metadata](../../../metadata/relational.md#schema-operations).

## Table

### Table capabilities

- Gravitino's table concept corresponds to the MySQL table.
- DDL operation for MySQL tables are supported.
- Indexed tables are supported.
- [Column default value](../../../metadata/relational.md#table-column-default-value)
  and [auto-increment columns](../../../metadata/relational.md#table-column-auto-increment).
  are supported.
- This catalog supports managing MySQL table features through table properties,
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
**[External Type](../../../metadata/relational.md#external-type)**
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

<table>
<thead>
<tr>
  <th>Property Name</th>
  <th>Description</th>
  <th>Default Value</th>
  <th>Required</th>
  <th>Reserved</th>
  <th>Immutable</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>engine</tt></td>
  <td>
    The engine used by the table.
    For example `MyISAM`, `MEMORY`, `CSV`, `ARCHIVE`, `BLACKHOLE`, `FEDERATED`,
    `ndbinfo`, `MRG_MYISAM`, `PERFORMANCE_SCHEMA`.
  </td>
  <td>`InnoDB`</td>
  <td>No</td>
  <td>No</td>
  <td>Yes</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>auto-increment-offset</tt></td>
  <td>
    The starting value of the auto-increment field.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>No</td>
  <td>Yes</td>
  <td>`0.4.0`</td>
</tr>
</tbody>
</table>

:::note
Some MySQL storage engines, such as "FEDERATED", are not enabled by default
and require additional configuration to use.
For example, to enable the "FEDERATED" engine, set `federated=1` in the MySQL configuration file.

Similarly, engines like "ndbinfo", "MRG_MYISAM", and "PERFORMANCE_SCHEMA"
may also require specific prerequisites or configurations.
For detailed instructions, refer to the
[MySQL documentation](https://dev.mysql.com/doc/refman/8.0/en/federated-storage-engine.html).
:::

### Table indexes

Indexed tables with `PRIMARY_KEY` and `UNIQUE_KEY` are supported.

:::note
The index name of the <tt>PRIMARY_KEY</tt> must be `PRIMARY`.
See [create table index](https://dev.mysql.com/doc/refman/8.0/en/create-table.html).
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

Refer to [managing relational metadata](../../../metadata/relational.md#table-operations).

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

