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

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

Gravitino provides the ability to manage PostgreSQL metadata.

:::caution
Gravitino saves some system information in schema and table comment, like `(From Gravitino, DO NOT EDIT: gravitino.v1.uid1078334182909406185)`, please don't change or remove this message.
:::

## Catalog

### Catalog capabilities

- Gravitino catalog corresponds to the PostgreSQL database.
- Supports metadata management of PostgreSQL (12.x, 13.x, 14.x, 15.x, 16.x).
- Supports DDL operation for PostgreSQL schemas and tables.
- Supports table index.
- Supports [column default value](./manage-metadata-using-gravitino.md#table-column-default-value). and [auto-increment](./manage-metadata-using-gravitino.md#table-column-auto-increment). 

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

Please refer to [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema capabilities

- Gravitino schema corresponds to the PostgreSQL schema.
- Supports creating schema with comments.
- Supports dropping schema.
- Supports cascade dropping schema.

### Schema properties

- Doesn't support any schema property settings.

### Schema operations

Please refer to [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino.md#schema-operations) for more details.

## Table

### Table capabilities

- The Gravitino table corresponds to the PostgreSQL table.
- Supports DDL operation for PostgreSQL tables.
- Supports index.
- Support [column default value](./manage-metadata-using-gravitino.md#table-column-default-value) and [auto-increment](./manage-metadata-using-gravitino.md#table-column-auto-increment).
- Doesn't support table property settings.

#### Table column types

| Gravitino Type | PostgreSQL Type |
|----------------|-----------------|
| `Boolean`      | `Bool`          |
| `Short`        | `Int2`          |
| `Integer`      | `Int4`          |
| `Long`         | `Int8`          |
| `Float`        | `Float4`        |
| `Double`       | `Float8`        |
| `String`       | `Text`          |
| `Date`         | `Date`          |
| `Time`         | `Time`          |
| `Timestamp`    | `Timestamp`     |
| `Timestamp_tz` | `Timestamptz`   |
| `Decimal`      | `Numeric`       |
| `VarChar`      | `Varchar`       |
| `FixedChar`    | `Bpchar`        |
| `Binary`       | `Bytea`         |

:::info
PostgreSQL doesn't support Gravitino `Fixed` `Struct` `List` `Map` `IntervalDay` `IntervalYear` `Union` `UUID` type.
Meanwhile, the data types other than listed above are mapped to Gravitino **[Unparsed Type](./manage-metadata-using-gravitino.md#unparsed-type)** that represents an unresolvable data type since 0.5.0.
:::

#### Table column auto-increment

- Supports setting auto-increment.

### Table properties

- Doesn't support table properties.

### Table indexes

- Supports PRIMARY_KEY and UNIQUE_KEY.

<Tabs>
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

Please refer to [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino.md#table-operations) for more details.

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
If you update a nullability column to non nullability, there may be compatibility issues.
:::
