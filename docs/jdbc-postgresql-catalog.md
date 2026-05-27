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

The PostgreSQL catalog enables Apache Gravitino to manage PostgreSQL metadata, including schemas, tables, columns, indexes, and column-level defaults. Use it when you want a single Gravitino-managed access surface over a PostgreSQL instance, with the option to federate it alongside other relational, lakehouse, and fileset catalogs.

:::caution
Gravitino saves some system information in schema and table comment, like `(From Gravitino, DO NOT EDIT: gravitino.v1.uid1078334182909406185)`, do not change or remove this message.
:::

### Requirements and Limitations

- **Supported PostgreSQL versions:** 12.x, 13.x, 14.x, 15.x, 16.x.
- **JDBC driver required.** Place the PostgreSQL JDBC driver (`org.postgresql.Driver`) in `catalogs/jdbc-postgresql/libs` on the Gravitino server. Gravitino does not bundle the driver.
- **One PostgreSQL database per catalog.** A Gravitino PostgreSQL catalog corresponds to exactly one PostgreSQL database. To manage multiple databases, create one Gravitino catalog per database.
- **Schema-to-schema mapping.** A Gravitino schema corresponds directly to a PostgreSQL schema inside the configured database.

## Quick Start

Create a minimum-viable PostgreSQL catalog and confirm it is reachable. The example assumes a Gravitino server at `http://localhost:8090`, a metalake named `test`, and a PostgreSQL instance at `localhost:5432` with a database named `pg_database`. Adjust the values for your environment.

### Create the Catalog

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "pg_catalog",
    "type": "RELATIONAL",
    "comment": "PostgreSQL catalog",
    "provider": "jdbc-postgresql",
    "properties": {
      "jdbc-url": "jdbc:postgresql://localhost:5432/pg_database",
      "jdbc-driver": "org.postgresql.Driver",
      "jdbc-database": "pg_database",
      "jdbc-user": "<your-user>",
      "jdbc-password": "<your-password>"
    }
  }' \
  http://localhost:8090/api/metalakes/test/catalogs
```

The response is a JSON object describing the created catalog.

### Verify the Catalog

```bash
# List catalogs in the metalake. pg_catalog should appear.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs" | jq

# Load the catalog directly and inspect its properties.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/pg_catalog" | jq

# List schemas. The response should include the PostgreSQL schemas in pg_database, typically `public` at minimum.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/pg_catalog/schemas" | jq
```

**Success check:** the catalog-list response includes `pg_catalog`, the load-catalog response shows `"provider":"jdbc-postgresql"`, and the schema-list response includes at least the `public` schema. If the schema-list call returns an authentication or connection error, verify the `jdbc-url`, `jdbc-user`, and `jdbc-password` values, and confirm the PostgreSQL JDBC driver is present in `catalogs/jdbc-postgresql/libs` on the Gravitino server.

## Catalog

### Catalog Capabilities

- A Gravitino catalog corresponds to one PostgreSQL database.
- Supports DDL operations on PostgreSQL schemas and tables.
- Supports table indexes.
- Supports [column default values](./manage-relational-metadata-using-gravitino.md#table-column-default-value) and [auto-increment](./manage-relational-metadata-using-gravitino.md#table-column-auto-increment).

### Catalog Properties

Any property that isn't defined by Gravitino can pass to PostgreSQL data source by adding `gravitino.bypass.` prefix as a catalog property. For example, catalog property `gravitino.bypass.maxWaitMillis` will pass `maxWaitMillis` to the data source property.
Check the relevant data source configuration in [data source properties](https://commons.apache.org/proper/commons-dbcp/configuration.html)

When using Gravitino with Trino, pass the Trino PostgreSQL connector configuration using the `trino.bypass.` prefix. For example, using `trino.bypass.join-pushdown.strategy` to pass the `join-pushdown.strategy` to the Gravitino PostgreSQL catalog in Trino runtime.

If you use JDBC catalog, you must provide `jdbc-url`, `jdbc-driver`, `jdbc-database`, `jdbc-user` and `jdbc-password` to catalog properties.
Besides the [common catalog properties](./gravitino-server-config.md#catalog-properties), the PostgreSQL catalog has the following properties:

| Property      | Description                                                                                                                                                       | Default | Required | Since |
|-------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `jdbc-url`              | JDBC URL for connecting to the database. You need to specify the database in the URL. For example `jdbc:postgresql://localhost:5432/pg_database?sslmode=require`. | (none)        | Yes      | 0.3.0         |
| `jdbc-driver`           | The driver of the JDBC connection. For example `org.postgresql.Driver`.                                                                                           | (none)        | Yes      | 0.3.0         |
| `jdbc-database`         | The database of the JDBC connection. Configure it with the same value as the database in the `jdbc-url`. For example `pg_database`.                               | (none)        | Yes      | 0.3.0         |
| `jdbc-user`             | The JDBC user name.                                                                                                                                               | (none)        | Yes      | 0.3.0         |
| `jdbc-password`         | The JDBC password.                                                                                                                                                | (none)        | Yes      | 0.3.0         |
| `jdbc.pool.min-size`    | The minimum number of connections in the pool. `2` by default.                                                                                                    | `2`           | No       | 0.3.0         |
| `jdbc.pool.max-size`    | The maximum number of connections in the pool. `10` by default.                                                                                                   | `10`          | No       | 0.3.0         |
| `jdbc.pool.max-wait-ms` | The maximum Duration that the pool will wait for a connection to be returned. `30000` by default.                                                                 | `30000`       | No       | 1.1.0         |

:::caution
Download the corresponding JDBC driver to the `catalogs/jdbc-postgresql/libs` directory.
Explicitly specify the database in both `jdbc-url` and `jdbc-database`. An error may occur if the values in both aren't consistent.
:::
:::info
In PostgreSQL, the database corresponds to the Gravitino catalog, and the schema corresponds to the Gravitino schema.
:::

### Catalog Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema Capabilities

- A Gravitino schema corresponds to a PostgreSQL schema in the configured database.
- Supports creating schemas with comments.
- Supports dropping schemas, including `CASCADE` drops of non-empty schemas.

### Schema Properties

- Doesn't support any schema property settings.

### Schema Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more details.

## Table

### Table Capabilities

- A Gravitino table corresponds to a PostgreSQL table in the configured schema.
- Supports DDL operations on PostgreSQL tables.
- Supports indexes.
- Supports [column default values](./manage-relational-metadata-using-gravitino.md#table-column-default-value) and [auto-increment](./manage-relational-metadata-using-gravitino.md#table-column-auto-increment).
- Does not support Gravitino-managed table properties. Configure storage and tuning options directly in PostgreSQL.

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
| `List`            | `Array`          |

:::info
PostgreSQL doesn't support Gravitino `Fixed` `Struct` `Map` `IntervalDay` `IntervalYear` `Union` `UUID` type.
Data types other than those listed above are mapped to the Gravitino **[External Type](./manage-relational-metadata-using-gravitino.md#external-type)**, which represents an unresolvable data type. Available since 0.6.0.
:::

### Table Column Auto-Increment

- Supports setting auto-increment.

### Table Properties

The PostgreSQL catalog does not accept Gravitino-managed table properties. Configure storage and tuning options directly in PostgreSQL using standard PostgreSQL DDL or server settings.

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
