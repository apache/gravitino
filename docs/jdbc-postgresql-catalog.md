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

See [JDBC catalog connection validation](./jdbc-catalog-connection-validation.md) for the default
validation behavior and SQL validation configuration for drivers without `Connection.isValid()` support.

Any property that isn't defined by Gravitino can pass to PostgreSQL data source by adding `gravitino.bypass.` prefix as a catalog property. For example, catalog property `gravitino.bypass.maxWaitMillis` will pass `maxWaitMillis` to the data source property.
Check the relevant data source configuration in [data source properties](https://commons.apache.org/proper/commons-dbcp/configuration.html)

When using Gravitino with Trino, pass the Trino PostgreSQL connector configuration using the `trino.bypass.` prefix. For example, using `trino.bypass.join-pushdown.strategy` to pass the `join-pushdown.strategy` to the Gravitino PostgreSQL catalog in Trino runtime.

If you use JDBC catalog, you must provide `jdbc-url`, `jdbc-driver`, `jdbc-user` and `jdbc-password` to catalog properties.
Besides the [common catalog properties](./gravitino-server-config.md#catalog-properties-configuration), the PostgreSQL catalog has the following properties:

| Configuration item      | Description                                                                                                                                                                              | Default value | Required                       |
|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|--------------------------------|
| `jdbc-url`              | A valid PostgreSQL JDBC URL, for example `jdbc:postgresql://localhost:5432/pg_database?sslmode=require`. If the URL omits the database, set `jdbc-database`.                             | (none)        | Yes                            |
| `jdbc-driver`           | The driver of the JDBC connection. For example `org.postgresql.Driver`.                                                                                                                  | (none)        | Yes                            |
| `jdbc-database`         | The database of the JDBC connection. Derived from `jdbc-url` when omitted. An explicit value must be nonblank and match the URL database when both are provided.                         | (none)        | Only if absent from `jdbc-url` |
| `jdbc-user`             | The JDBC user name.                                                                                                                                                                      | (none)        | Yes                            |
| `jdbc-password`         | The JDBC password.                                                                                                                                                                       | (none)        | Yes                            |
| `jdbc.pool.min-size`    | The minimum number of connections in the pool. `2` by default.                                                                                                                           | `2`           | No                             |
| `jdbc.pool.max-size`    | The maximum number of connections in the pool. `10` by default.                                                                                                                          | `10`          | No                             |
| `jdbc.pool.max-idle`    | Maximum idle connections retained per catalog per server; capped by `jdbc.pool.max-size`; takes precedence over `gravitino.bypass.maxIdle`. Idle connections are not evicted by default. | `8`           | No                             |
| `jdbc.pool.max-wait-ms` | The maximum Duration that the pool will wait for a connection to be returned; takes precedence over `gravitino.bypass.maxWaitMillis`.                                                    | `30000`       | No                             |

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
Sensitive catalog properties such as `jdbc-password` are hidden from the default load catalog response (`jdbc-user` is returned in plaintext). Recover `jdbc-user` / `jdbc-password` via the [credential vending API](security/credential-vending.md) (`getCredentials` / `JdbcCredential`); `jdbc-user` also remains in `properties()` when not hidden. Other non-credential secrets (secret-manager URNs, declared `hidden` properties, undeclared sensitive-named keys) use `getSecrets` / `GET .../objects/{type}/{fullName}/secrets`.
:::

## Schema

### Schema Capabilities

- Gravitino schema corresponds to the PostgreSQL schema.
- Supports creating schema with comments.
- Supports dropping schema.
- Non-empty schema drops, including requests with `cascade=true`, are refused with a schema-not-empty conflict (HTTP 409). PostgreSQL native `CASCADE` can delete objects in other schemas, and a client-side dependency check cannot prevent concurrent DDL from adding such dependencies. Remove the schema objects explicitly, then drop the empty schema.

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

An unconstrained `Numeric` column, that is one declared without precision and scale, accepts values of up to
131072 digits before and 16383 digits after the decimal point, and its precision and scale vary per row.
Gravitino `Decimal` caps precision at 38 and is fixed per column, so such a column is mapped to the External
Type `numeric` instead. A `Numeric(p, s)` column maps to `Decimal(p, s)` when `p` is at most 38
and `s` is between 0 and `p`. Other constrained declarations, such as `numeric(39,0)`,
`numeric(2,-3)`, and `numeric(3,5)`, map to External Types preserving their precision and scale.
A `Numeric(p)` column follows the same rules with scale 0.

The Spark JDBC, Trino PostgreSQL, and Flink connectors map these External Types to strings
(`StringType`, unbounded `VARCHAR`, and `STRING`, respectively), using the fallback added in
[PR #13042](https://github.com/apache/gravitino/pull/13042). This keeps schema type conversion
from failing, but does not expose native decimal semantics in those engines. Consumers needing
numeric operations must choose an explicit conversion compatible with their data and engine;
Gravitino does not clamp precision or round scale to fit `Decimal`. Supported `Numeric(p, s)`
columns continue to map to native decimal types.

PostgreSQL array elements always accept NULL and cannot be declared otherwise, so an `Array` column is always
mapped to a `List` whose elements are nullable. A `List` created with non-nullable elements is accepted and
produces an ordinary array whose elements accept NULL.
Array element types retain their character length, numeric precision and scale, and supported temporal
precision, following the same conversion rules as scalar columns. Unbounded `varchar` maps to
`String`, including array elements. Unbounded `bpchar` maps to `ExternalType("bpchar")` because its
trailing-space-insensitive comparison semantics cannot be represented by `String` or a bounded
`FixedChar`. Consumers that read external types as strings must not assume native `bpchar`
comparison semantics. Bounded character types retain their declared lengths, independently of pgjdbc's
`unknownLength` setting. Loading a table with character columns or sequence-backed defaults performs
one additional read of PostgreSQL's column declarations to distinguish unbounded types from driver
display lengths and identify column-owned serial sequences.

Native `smallserial`, `serial`, and `bigserial` columns map to `Short`, `Integer`, and `Long`, respectively,
and retain their auto-increment metadata. Standard non-null serial defaults that reference the column's own
sequence are represented by `autoIncrement`, without a separate default expression. Recreating such
columns creates an independent `GENERATED BY DEFAULT AS IDENTITY` sequence, as for loaded identity
columns. This preserves basic value generation, not the source sequence's current value, options, or
`GENERATED ALWAYS` mode. Shared sequence defaults, custom expressions, and serial columns altered
to allow NULL remain unparsed and are not supported for recreation.
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

### Native type declarations and column defaults

`ExternalType` on CREATE or ALTER accepts a single PostgreSQL type declaration: an unquoted
or double-quoted type name, optionally schema-qualified, with numeric or identifier type
modifiers (for example `geometry(Point,4326)`) and array suffixes (`[]` or `ARRAY`). Standard
multiword types such as `double precision`, `character varying`, `national character(3)`,
`timestamp with time zone`, and interval field declarations are also accepted. Quote type
names containing spaces or punctuation. Extra column definitions, constraints, defaults,
comments, and statements are rejected before DDL execution. Unsupported declaration syntax
must be expressed using one of the supported forms; arbitrary SQL fragments are not accepted.
The native type read path remains unchanged.

Function defaults accept an unquoted function identifier, optionally qualified, with literal
or nested-function arguments. Arguments are rendered as SQL values rather than Java object
strings. `CURRENT_TIMESTAMP` remains a no-argument keyword default. Raw SQL expressions and
SQL syntax in function names are rejected. String values containing backslashes use explicit
PostgreSQL escape literals so that `standard_conforming_strings` does not change their meaning.
On load, only complete supported constants are converted to literals; functions and other
expressions remain `UnparsedExpression` values. `CURRENT_TIMESTAMP` loads as the
`CURRENT_TIMESTAMP` default on any column type. A plain number on a string column, such as
`varchar(20) DEFAULT 10`, loads as the string `10`. Quoted defaults are decoded using the reading
connection's string mode without changing the session or adding a query. An unparsed default
is preserved for inspection, but cannot be submitted as a PostgreSQL write expression.
See [shared JDBC default restrictions](./tables-and-views.md#table-column-default-value) for the
other affected catalogs and the limits of this validation.

ALTER requests normalize explicit NULL default literals in the same way as CREATE requests.
A string literal containing `NULL` remains a string, and omitting a default when adding a
column still leaves the default unset.

### Deleting unique indexes

The table index API drops PostgreSQL primary-key and unique constraints. A standalone
`CREATE UNIQUE INDEX` is visible in table metadata, but deleting it through this API is
unsupported. Both strict and `ifExists` deletion requests fail explicitly before applying
any changes in the request. Remove standalone indexes directly in PostgreSQL.
