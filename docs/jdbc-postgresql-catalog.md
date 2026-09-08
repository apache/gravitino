---
title: "PostgreSQL Catalog"
slug: "/jdbc-postgresql-catalog"
keywords:
- jdbc
- PostgreSQL
- metadata
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

Apache Gravitino provides the ability to manage PostgreSQL metadata.

:::caution
Gravitino saves some system information in schema and table comment, like `(From Gravitino, DO NOT EDIT: gravitino.v1.uid1078334182909406185)`, do not change or remove this message.
:::

## Catalog

### Catalog Capabilities

- Gravitino catalog corresponds to the PostgreSQL database.
- Supports metadata management of PostgreSQL (12.x, 13.x, 14.x, 15.x, 16.x).
- Supports DDL operation for PostgreSQL schemas and tables.
- Supports table index.
- Supports [column default value](./tables-and-views.md#table-column-default-value). and [auto-increment](./tables-and-views.md#table-column-auto-increment).

### Catalog Properties

Any property that isn't defined by Gravitino can pass to PostgreSQL data source by adding `gravitino.bypass.` prefix as a catalog property. For example, catalog property `gravitino.bypass.maxWaitMillis` will pass `maxWaitMillis` to the data source property.
Check the relevant data source configuration in [data source properties](https://commons.apache.org/proper/commons-dbcp/configuration.html)

When using Gravitino with Trino, pass the Trino PostgreSQL connector configuration using the `trino.bypass.` prefix. For example, using `trino.bypass.join-pushdown.strategy` to pass the `join-pushdown.strategy` to the Gravitino PostgreSQL catalog in Trino runtime.

If you use JDBC catalog, you must provide `jdbc-url`, `jdbc-driver`, `jdbc-user` and `jdbc-password` to catalog properties.
Besides the [common catalog properties](./gravitino-server-config.md#catalog-properties-configuration), the PostgreSQL catalog has the following properties:

| Configuration item      | Description                                                                                                                                                       | Default value | Required |
|-------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|
| `jdbc-url`              | A valid PostgreSQL JDBC URL, for example `jdbc:postgresql://localhost:5432/pg_database?sslmode=require`. If the URL omits the database, set `jdbc-database`. | (none)        | Yes      |
| `jdbc-driver`           | The driver of the JDBC connection. For example `org.postgresql.Driver`.                                                                                           | (none)        | Yes      |
| `jdbc-database`         | The database of the JDBC connection. Derived from `jdbc-url` when omitted. An explicit value must be nonblank and match the URL database when both are provided. | (none)        | Only if absent from `jdbc-url` |
| `jdbc-user`             | The JDBC user name.                                                                                                                                               | (none)        | Yes      |
| `jdbc-password`         | The JDBC password.                                                                                                                                                | (none)        | Yes      |
| `jdbc.pool.min-size`    | The minimum number of connections in the pool. `2` by default.                                                                                                    | `2`           | No       |
| `jdbc.pool.max-size`    | The maximum number of connections in the pool. `10` by default.                                                                                                   | `10`          | No       |
| `jdbc.pool.max-wait-ms` | The maximum Duration that the pool will wait for a connection to be returned. `30000` by default.                                                                 | `30000`       | No       |

:::caution
Download the corresponding JDBC driver to the `catalogs/jdbc-postgresql/libs` directory.
When `jdbc-database` is omitted, the catalog derives it from `jdbc-url`. Catalog creation rejects invalid PostgreSQL URLs, even when `jdbc-database` is set, and fails if neither provides a nonblank database name. When both `jdbc-database` and the URL specify a database, their names must match; catalog creation rejects conflicting values.
:::
:::info
In PostgreSQL, the database corresponds to the Gravitino catalog, and the schema corresponds to the Gravitino schema.
:::

### Catalog Operations

Refer to [Manage Catalogs and Schemas](./manage-catalogs-and-schemas.md#catalog-operations) for more details.

:::note
Sensitive catalog properties such as `jdbc-password` are hidden from the default load catalog response (`jdbc-user` is returned in plaintext). Retrieve secret-manager-backed properties (including `jdbc-password` when stored as a secret URN) via `getSecrets` / `GET .../objects/{type}/{fullName}/secrets`. The [credential vending API](security/credential-vending.md) (`getCredentials` / `JdbcCredential`) remains available for typed credential delivery.
:::

## Schema

### Schema Capabilities

- Gravitino schema corresponds to the PostgreSQL schema.
- Supports creating schema with comments.
- Supports dropping schema.
- Supports cascade dropping schema.

### Schema Properties

- Doesn't support any schema property settings.

### Schema Operations

Refer to [Manage Catalogs and Schemas](./manage-catalogs-and-schemas.md#schema-operations) for more details.

## Table

### Table Capabilities

- The Gravitino table corresponds to the PostgreSQL table.
- Supports DDL operation for PostgreSQL tables.
- Supports index.
- Support [column default value](./tables-and-views.md#table-column-default-value) and [auto-increment](./tables-and-views.md#table-column-auto-increment).
- Doesn't support table property settings.

### Table Column Types

| Gravitino Type    | PostgreSQL Type  |
|-------------------|------------------|
| `Boolean`         | `Bool`           |
| `Short`           | `Int2`           |
| `Integer`         | `Int4`           |
| `Long`            | `Int8`           |
| `Float`           | `Float4`         |
| `Double`          | `Float8`         |
| `String`          | `Text`           |
| `Date`            | `Date`           |
| `Time(p)`         | `Time(p)`        |
| `Timestamp(p)`    | `Timestamp(p)`   |
| `Timestamp_tz(p)` | `Timestamptz(p)` |
| `Decimal`         | `Numeric`        |
| `VarChar`         | `Varchar`        |
| `FixedChar`       | `Bpchar`         |
| `Binary`          | `Bytea`          |
| `UUID`            | `Uuid`           |
| `List`            | `Array`          |

:::info
PostgreSQL doesn't support Gravitino `Fixed` `Struct` `Map` `IntervalDay` `IntervalYear` `Union` type.
Meanwhile, the data types other than listed above are mapped to Gravitino **[External Type](./tables-and-views.md#external-type)** that represents an unresolvable data type.
:::

### Table Column Auto-Increment

- Supports setting auto-increment.

### Table Properties

- Doesn't support table properties.

### Table Indexes

- Supports PRIMARY_KEY and UNIQUE_KEY.

<Tabs groupId='language' queryString>
<TabItem value="json" label="JSON">

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
    Indexes.of(IndexType.PRIMARY_KEY, "PRIMARY", new String[][]{{"id"}}, Map.of()),
    Indexes.of(IndexType.UNIQUE_KEY, "id_name_uk", new String[][]{{"id"} , {"name"}}, Map.of()),
}
```

</TabItem>
</Tabs>

### Table Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for more details.

#### Alter Table Operations

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
PostgreSQL doesn't support the `UpdateColumnPosition` operation, so you can only use `ColumnPosition.defaultPosition()` when `AddColumn`.
If you update a nullability column to non nullability, there may be compatibility issues.
:::
