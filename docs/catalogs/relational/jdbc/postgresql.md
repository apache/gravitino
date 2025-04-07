---
title: "PostgreSQL catalog"
slug: /jdbc-postgresql-catalog
keywords:
- jdbc
- PostgreSQL
- metadata
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

Apache Gravitino can be used to manage PostgreSQL metadata.

:::caution
Gravitino saves some system information in schema and table comment, like
`(From Gravitino, DO NOT EDIT: gravitino.v1.uid1078334182909406185)`.
**Please don't change or remove this message.**
:::

## Catalog

### Catalog capabilities

- Gravitino catalog corresponds to the PostgreSQL database.
- This catalog supports metadata management of PostgreSQL (12.x, 13.x, 14.x, 15.x, 16.x).
- DDL operation for PostgreSQL schemas and tables are supported.
- Indexed tables are supported.
- [Column default value](../../../metadata/relational.md#table-column-default-value) and
  [auto-increment columns](../../../metadata/relational.md#table-column-auto-increment).
  are suuported.

### Catalog properties

Any property that isn't defined by Gravitino can pass to PostgreSQL data source
by adding `gravitino.bypass.` prefix as a catalog property.
For example, catalog property `gravitino.bypass.maxWaitMillis` will pass `maxWaitMillis` to the data source property.
You can check the relevant data source configuration in
[data source properties](https://commons.apache.org/proper/commons-dbcp/configuration.html)

You can pass the PostgreSQL connector configuration using prefix `trino.bypass.`.
For example, using `trino.bypass.join-pushdown.strategy` to pass the `join-pushdown.strategy`
to the Gravitino PostgreSQL catalog in Trino runtime.

If you use JDBC catalog, you must provide `jdbc-url`, `jdbc-driver`, `jdbc-database`,
`jdbc-user` and `jdbc-password` to catalog properties.
Besides the [common catalog properties](../../../admin/server-config.md#gravitino-catalog-properties-configuration),
the PostgreSQL catalog has the following properties:

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
    The JDBC URL for connecting to the database.
    You need to specify the database in the URL.
    For example `jdbc:postgresql://localhost:3306/pg_database?sslmode=require`.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>jdbc-driver</tt></td>
  <td>
    The driver of the JDBC connection. For example `org.postgresql.Driver`.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>jdbc-database</tt></td>
  <td>
    The database for the JDBC connection.
    Configure it with the same value as the database in the `jdbc-url`.
    For example `pg_database`.
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
  <td>The minimum number of connections in the pool.</td>
  <td>`2`</td>
  <td>No</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>jdbc.pool.max-size</tt></td>
  <td>The maximum number of connections in the pool.</td>
  <td>`10`</td>
  <td>No</td>
  <td>`0.3.0`</td>
</tr>
</tbody>
</table>

:::caution
You must download the corresponding JDBC driver
to the `catalogs/jdbc-postgresql/libs` directory.
You must explicitly specify the database in both `jdbc-url` and `jdbc-database`.
An error may occur if the values in both aren't consistent.
:::

:::info
In PostgreSQL, the database corresponds to the Gravitino catalog,
and the schema corresponds to the Gravitino schema.
:::

### Catalog operations

Refer to [managing relational metadata](../../../metadata/relational.md#catalog-operations).

## Schema

### Schema capabilities

- Gravitino schema corresponds to the PostgreSQL schema.
- Creating schema with comments is supported.
- Dropping schemas is supported.
- Cascaded dropping of schemas is supported.

### Schema properties

- Schema property settings are not supported.

### Schema operations

Refer to [managing relational metadata](../../../metadata/relational.md#schema-operations).

## Table

### Table capabilities

- The Gravitino table corresponds to the PostgreSQL table.
- DDL operations for PostgreSQL tables are supported
- lndexed tables are supported.
- [Column default value](../../../metadata/relational.md#table-column-default-value) and
  [auto-increment columns](../../../metadata/relational.md#table-column-auto-increment)
  are suuported.
- Table property settings are not supported.

### Table column types

| Gravitino Type | PostgreSQL Type |
|----------------|-----------------|
| `Binary`       | `Bytea`         |
| `Boolean`      | `Bool`          |
| `Date`         | `Date`          |
| `Decimal`      | `Numeric`       |
| `Double`       | `Float8`        |
| `FixedChar`    | `Bpchar`        |
| `Float`        | `Float4`        |
| `Integer`      | `Int4`          |
| `List`         | `Array`         |
| `Long`         | `Int8`          |
| `Short`        | `Int2`          |
| `String`       | `Text`          |
| `Time`         | `Time`          |
| `Timestamp`    | `Timestamp`     |
| `Timestamp_tz` | `Timestamptz`   |
| `VarChar`      | `Varchar`       |

:::info
PostgreSQL doesn't support Gravitino `Fixed`,`Struct`, `Map`,`IntervalDay`, `IntervalYear`,`Union`, or `UUID` type.
Meanwhile, the data types other than listed above are mapped to Gravitino
**[External Type](../../../metadata/relational.md#external-type)**
that represents an unresolvable data type since 0.6.0-incubating.
:::

### Table column auto-increment

- Auto-increment columns are supported.

### Table properties

- Table properties are not supported.

### Table indexes

Indexed tables with primary keys and/or unique keys are supported.

<Tabs groupId='language' queryString>
<TabItem value="json" label="Json">

```json
{
  "indexes": [
    {
      "indexType": "primary_key",
      "name": "id_pk",
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

Refer to [manage relational metadata](../../../metadata/relational.md#table-operations).

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
- `UpdateColumnDefaultValue`

:::info
You can't submit the `RenameTable` operation at the same time as other operations.
:::

:::caution
PostgreSQL doesn't support the `UpdateColumnPosition` operation,
so you can only use `ColumnPosition.defaultPosition()` when `AddColumn`.
If you update a nullability column to non nullability, there may be compatibility issues.
:::

