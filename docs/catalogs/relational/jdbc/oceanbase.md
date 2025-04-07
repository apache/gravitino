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

Apache Gravitino can be used to manage OceanBase metadata.

:::caution
Gravitino saves some system information in schema and table comment, like
`(From Gravitino, DO NOT EDIT: gravitino.v1.uid1078334182909406185)`.
**Please don't change or remove this message.**
:::

## Catalog

### Catalog capabilities

- Gravitino catalog corresponds to the OceanBase instance.
- This catalog supports managing metadata for OceanBase (4.x).
- DDL operation for OceanBase databases and tables are supported.
- Indexed tables are supported.
- [Column default value](../../../metadata/relational.md#table-column-default-value)
  and [auto-increment columns](../../../metadata/relational.md#table-column-auto-increment)
  are supported.

### Catalog properties

You can pass to a OceanBase data source any property that isn't defined by Gravitino
by adding `gravitino.bypass.` as a catalog property name prefix.
For example, catalog property `gravitino.bypass.maxWaitMillis` passes `maxWaitMillis`
to the data source property.

Check the relevant data source configuration in
[data source properties](https://commons.apache.org/proper/commons-dbcp/configuration.html)

If you use a JDBC catalog, you must provide `jdbc-url`, `jdbc-driver`, `jdbc-user`,
 and `jdbc-password` to catalog properties.
Besides the [common catalog properties](../../../admin/server-config.md#gravitino-catalog-properties-configuration),
the OceanBase catalog has the following properties:

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
<tr>
  <td><tt>jdbc-url</tt></td>
  <td>
    The JDBC URL for connecting to the database.
    For example, `jdbc:mysql://localhost:2881` or `jdbc:oceanbase://localhost:2881`.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>jdbc-driver</tt></td>
  <td>
    The driver of the JDBC connection.
    For example, `com.mysql.jdbc.Driver`, `com.mysql.cj.jdbc.Driver`
    or `com.oceanbase.jdbc.Driver`.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>jdbc-user</tt></td>
  <td>The JDBC user name.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>jdbc-password</tt></td>
  <td>The JDBC password.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>jdbc.pool.min-size</tt></td>
  <td>The minimum number of connections in the pool.</td>
  <td>`2`</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>jdbc.pool.max-size</tt></td>
  <td>The maximum number of connections in the pool.</td>
  <td>`10`</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
</tbody>
</table>

:::caution
Before using the OceanBase Catalog, you must download the corresponding JDBC driver
to the `catalogs/jdbc-oceanbase/libs` directory.
Gravitino doesn't package the JDBC driver for OceanBase due to licensing issues.
:::

### Catalog operations

Refer to [managing relational metadata](../../../metadata/relational.md#catalog-operations).

## Schema

### Schema capabilities

- Gravitino's schema concept corresponds to the OceanBase database.
- The catalog supports creating schema, but does not support setting comment.
- Dropping schemas is supported..
- Cascaded dropping of schemas are supported.

### Schema properties

- Schema property settings are not supported.

### Schema operations

Refer to [managing relational metadata](../../../metadata/relational.md#schema-operations).

## Table

### Table capabilities

- Gravitino's table concept corresponds to the OceanBase table.
- DDL operation for OceanBase tables are supported
- Indexed tables are supported.
- [Column default value](../../../metadata/relational.md#table-column-default-value)
  and [auto-increment columns](../../../metadata/relational.md#table-column-auto-increment)
  are supported.

### Table properties

- Table properties are not supported.

### Table column types

| Gravitino Type    | OceanBase Type      |
|-------------------|---------------------|
| `Binary`          | `Binary`            |
| `Byte`            | `Tinyint`           |
| `Byte(false)`     | `Tinyint Unsigned`  |
| `Date`            | `Date`              |
| `Decimal`         | `Decimal`           |
| `Double`          | `Double`            |
| `FixedChar`       | `FixedChar`         |
| `Float`           | `Float`             |
| `Integer`         | `Int`               |
| `Integer(false)`  | `Int Unsigned`      |
| `Long`            | `Bigint`            |
| `Long(false)`     | `Bigint Unsigned`   | 
| `Short`           | `Smallint`          |
| `Short(false)`    | `Smallint Unsigned` |
| `String`          | `Text`              |
| `Time`            | `Time`              |
| `Timestamp`       | `Timestamp`         |
| `VarChar`         | `VarChar`           |

:::info
OceanBase doesn't support Gravitino `Boolean`, `Fixed`, `Struct`, `List`,`Map`,`Timestamp_tz`,
`IntervalDay` `IntervalYear` `Union` `UUID` type.
Meanwhile, the data types other than listed above are mapped to Gravitino
**[External Type](../../../metadata/relational.md#external-type)**
that represents an unresolvable data type since 0.6.0-incubating.
:::

### Table column auto-increment

:::note
OceanBase setting an auto-increment column requires simultaneously setting a unique index;
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
}
```

</TabItem>
</Tabs>


### Table indexes

Indexed tables with primary keys and unique keys are supported.

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

Refer to [managing relational metadata](../../../metadata/relational.md#table-operations).

#### Alter table operations

Gravitino supports the following table alteration operations:

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
