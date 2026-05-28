---
title: "Hologres Catalog"
slug: "/jdbc-hologres-catalog"
keywords:
- jdbc
- Hologres
- metadata
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

The Hologres catalog enables Apache Gravitino to manage [Hologres](https://help.aliyun.com/zh/hologres) metadata. Hologres is Alibaba Cloud's real-time data warehouse service designed for high-concurrency, low-latency OLAP. Hologres is PostgreSQL-protocol compatible and connects through the PostgreSQL JDBC driver. Use the catalog when you want a single Gravitino-managed access surface that covers Hologres alongside other relational, lakehouse, and fileset catalogs.

:::caution
Gravitino saves some system information in schema and table comment, like `(From Gravitino, DO NOT EDIT: gravitino.v1.uid1078334182909406185)`, do not change or remove this message.
:::

### Requirements and Limitations

- **PostgreSQL JDBC driver required.** Hologres uses the PostgreSQL JDBC driver (`org.postgresql.Driver`). Place version 42.3.2 or higher in `catalogs/jdbc-hologres/libs` on the Gravitino server. Gravitino does not bundle the driver.
- **One Hologres database per catalog.** The `jdbc-database` property is required. A Gravitino schema maps to a Hologres (PostgreSQL) schema inside that database.
- **System schemas filtered.** The following system schemas are hidden from `listSchemas`: `pg_toast`, `pg_catalog`, `information_schema`, `hologres`, `hg_internal`, `hg_recyclebin`, `hologres_object_table`, `hologres_sample`, `hologres_streaming_mv`, `hologres_statistic`.
- **Authentication.** Connect with either an AccessKey ID and Secret or a database username and password through `jdbc-user` and `jdbc-password`.
- **LIST partitioning is supported.** Both physical and logical Hologres partition tables can be created through Gravitino.
- **Hologres-specific table properties.** The `orientation`, `clustering_key`, `distribution_key`, and other Hologres WITH-clause properties are accepted as Gravitino table properties.
- **No auto-increment columns.**
- **No schema properties.** Hologres schemas do not accept Gravitino-managed schema properties.

## Quick Start

Create a minimum-viable Hologres catalog and confirm it is reachable. The example assumes a Gravitino server at `http://localhost:8090`, a metalake named `test`, and a Hologres instance at the endpoint shown. Substitute the actual Hologres endpoint, database name, and credentials for your deployment.

### Create the Catalog

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "hologres_catalog",
    "type": "RELATIONAL",
    "comment": "Hologres catalog",
    "provider": "jdbc-hologres",
    "properties": {
      "jdbc-url": "jdbc:postgresql://hgprecn-cn-xxx.hologres.aliyuncs.com:80/my_database",
      "jdbc-driver": "org.postgresql.Driver",
      "jdbc-database": "my_database",
      "jdbc-user": "<accesskey-id-or-user>",
      "jdbc-password": "<accesskey-secret-or-password>"
    }
  }' \
  http://localhost:8090/api/metalakes/test/catalogs
```

### Verify the Catalog

```bash
# List catalogs in the metalake. hologres_catalog should appear.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs" | jq

# Load the catalog directly and inspect its properties.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/hologres_catalog" | jq

# List schemas. System schemas are filtered out; the response typically includes at least `public`.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/hologres_catalog/schemas" | jq
```

**Success check:** the catalog-list response includes `hologres_catalog`, the load-catalog response shows `"provider":"jdbc-hologres"` with the configured `jdbc-url` and `jdbc-database`, and the schema-list response includes at least the `public` schema. If the schema-list call returns a connection error, verify the `jdbc-url`, `jdbc-database`, `jdbc-user`, and `jdbc-password` values, and confirm the PostgreSQL JDBC driver is present in `catalogs/jdbc-hologres/libs` on the Gravitino server.

## Catalog

### Catalog Capabilities

A Gravitino Hologres catalog corresponds to a Hologres database instance and provides:

- Metadata management.
- DDL operations on Hologres schemas and tables.
- Table indexes (`PRIMARY KEY` in `CREATE TABLE`).
- [Column default values](./manage-relational-metadata-using-gravitino.md#table-column-default-value).
- `LIST` partitioning (physical and logical partition tables).
- Hologres-specific table properties through the `WITH` clause (`orientation`, `clustering_key`, `distribution_key`, and others).

The Hologres catalog does not support [auto-increment](./manage-relational-metadata-using-gravitino.md#table-column-auto-increment).

### Catalog Properties

Pass to a Hologres data source any property that isn't defined by Gravitino by adding `gravitino.bypass.` prefix as a catalog property. For example, catalog property `gravitino.bypass.maxWaitMillis` will pass `maxWaitMillis` to the data source property.

Check the relevant data source configuration in [data source properties](https://commons.apache.org/proper/commons-dbcp/configuration.html)

If you use a JDBC catalog, you must provide `jdbc-url`, `jdbc-driver`, `jdbc-database`, `jdbc-user` and `jdbc-password` to catalog properties.
Besides the [common catalog properties](./gravitino-server-config.md#catalog-properties), the Hologres catalog has the following properties:

| Property      | Description                                                                                                                                                           | Default | Required | Since    |
|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `jdbc-url`              | JDBC URL for connecting to the database. For example, `jdbc:postgresql://hgprecn-cn-xxx.hologres.aliyuncs.com:80/my_database`                                        | (none)        | Yes      | 1.3.0 |
| `jdbc-driver`           | The driver of the JDBC connection. Must be `org.postgresql.Driver`.                                                                                                   | (none)        | Yes      | 1.3.0 |
| `jdbc-database`         | The database name. This is mandatory for Hologres.                                                                                                                    | (none)        | Yes      | 1.3.0 |
| `jdbc-user`             | The JDBC user name (AccessKey ID or database username).                                                                                                               | (none)        | Yes      | 1.3.0 |
| `jdbc-password`         | The JDBC password (AccessKey Secret or database password).                                                                                                            | (none)        | Yes      | 1.3.0 |
| `jdbc.pool.min-size`    | The minimum number of connections in the pool. `2` by default.                                                                                                        | `2`           | No       | 1.3.0 |
| `jdbc.pool.max-size`    | The maximum number of connections in the pool. `10` by default.                                                                                                       | `10`          | No       | 1.3.0 |

:::caution
Hologres uses the PostgreSQL JDBC Driver (version 42.3.2 or later recommended). You need to download the PostgreSQL JDBC Driver and place it in the `catalogs/jdbc-hologres/libs` directory under the Gravitino distribution (e.g., `distribution/package/catalogs/jdbc-hologres/libs` or `distribution/package-all/catalogs/jdbc-hologres/libs`).
:::

### Catalog Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema Capabilities

- A Gravitino schema corresponds to a Hologres (PostgreSQL) schema in the configured database.
- Supports creating schemas with comments.
- Supports dropping schemas.

### Schema Properties

The Hologres catalog does not support any schema-level properties.

### Schema Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more details.

## Table

### Table Capabilities

A Gravitino table corresponds to a Hologres table and supports:

- DDL operations on Hologres tables.
- `PRIMARY KEY` indexes in `CREATE TABLE`.
- [Column default values](./manage-relational-metadata-using-gravitino.md#table-column-default-value).
- Expression columns through `DEFAULT` expressions. (Gravitino maps these as column default values, not as true generated/computed columns in the Hologres sense.)
- `LIST` partitioning (physical and logical).

The Hologres catalog does not support [auto-increment](./manage-relational-metadata-using-gravitino.md#table-column-auto-increment). Creating auto-increment columns is rejected in both `CREATE TABLE` and `ALTER TABLE`.

### Table Properties

Hologres-specific table properties are set via the `WITH` clause during CREATE TABLE and read from the `hologres.hg_table_properties` system table. The following user-relevant properties are supported:

| Property                        | Description                       | Example                  |
|-------------------------------------|-----------------------------------|--------------------------------|
| `orientation`                       | Storage format                    | `column`, `row`, `row,column`  |
| `clustering_key`                    | Clustering key columns            | `id:asc`                       |
| `segment_key`                       | Event time column (segment key)   | `create_time`                  |
| `bitmap_columns`                    | Bitmap index columns              | `status,category`              |
| `dictionary_encoding_columns`       | Dictionary encoding columns       | `city,province`                |
| `time_to_live_in_seconds`           | Data TTL setting                  | `2592000`                      |
| `table_group`                       | Table group name                  | `my_table_group`               |
| `storage_format`                    | Internal storage format           | `orc`, `sst`                   |
| `binlog_level`                      | Binlog level                      | `replica`, `none`              |
| `binlog_ttl`                        | Binlog TTL                        | `86400`                        |

:::info
- Modifying table properties via ALTER TABLE `SetProperty` / `RemoveProperty` is not yet supported by Gravitino (Hologres natively supports property modification via the `CALL HG_UPDATE_TABLE_PROPERTY` or rebuild commands, but this is not yet implemented in Gravitino).
- The properties `distribution_key`, `is_logical_partitioned_table`, and `primary_key` are managed via their dedicated parameters (Distribution, Partitioning, Indexes) and should not be set directly in table properties.
:::

### Table Column Types

| Gravitino Type              | Hologres Type              | Notes                                              |
|-----------------------------|----------------------------|----------------------------------------------------|
| `Boolean`                   | `bool`                     |                                                    |
| `Short`                     | `int2` (SMALLINT)          |                                                    |
| `Integer`                   | `int4` (INTEGER)           |                                                    |
| `Long`                      | `int8` (BIGINT)            |                                                    |
| `Float`                     | `float4` (REAL)            |                                                    |
| `Double`                    | `float8` (DOUBLE PRECISION)|                                                    |
| `Decimal(p,s)`              | `numeric(p,s)`             |                                                    |
| `VarChar(n)`                | `varchar(n)`               | `varchar` without length maps to `String`          |
| `FixedChar(n)`              | `bpchar(n)` (CHAR)         |                                                    |
| `String`                    | `text`                     |                                                    |
| `Binary`                    | `bytea`                    |                                                    |
| `Date`                      | `date`                     |                                                    |
| `Time`                      | `time`                     | With optional precision                            |
| `Timestamp`                 | `timestamp`                | Always emitted without precision suffix             |
| `Timestamp_tz`              | `timestamptz`              | Always emitted without precision suffix             |
| `List(IntegerType, false)`  | `int4[]` (`_int4`)         | Array types via `_` prefix                          |
| `List(LongType, false)`     | `int8[]` (`_int8`)         |                                                    |
| `List(FloatType, false)`    | `float4[]` (`_float4`)     |                                                    |
| `List(DoubleType, false)`   | `float8[]` (`_float8`)     |                                                    |
| `List(BooleanType, false)`  | `bool[]` (`_bool`)         |                                                    |
| `List(StringType, false)`   | `text[]` (`_text`)         |                                                    |

:::info
- Hologres does not support precision syntax for `TIMESTAMP`/`TIMESTAMPTZ` (e.g., `timestamptz(6)` is invalid), so the type converter always emits the base type without precision.
- Array element types must be non-nullable (Hologres limitation). Multidimensional arrays are not supported.
- Types like `json`, `jsonb`, `uuid`, `inet`, `money`, `roaringbitmap` are mapped to Gravitino **[External Type](./manage-relational-metadata-using-gravitino.md#external-type)** with the original type name preserved.
:::

### Table Distribution

Hologres supports HASH distribution via the `distribution_key` property in the `WITH` clause.

<Tabs groupId='language' queryString>
<TabItem value="json" label="JSON">

```json
{
  "distribution": {
    "strategy": "hash",
    "number": 0,
    "funcArgs": [
      {
        "type": "field",
        "fieldName": ["id"]
      }
    ]
  }
}
```

</TabItem>
<TabItem value="java" label="Java">

```java
Distribution distribution = Distributions.hash(0, NamedReference.field("id"));
```

</TabItem>
</Tabs>

### Table Partitioning

Hologres supports LIST partitioning with two variants:

- **Physical partition tables**: `PARTITION BY LIST(column)` — supports exactly 1 partition column.
- **Logical partition tables** (Hologres V3.1+): `LOGICAL PARTITION BY LIST(col1[, col2])` — supports 1–2 partition columns. Enabled by setting property `is_logical_partitioned_table` to `true`.

<Tabs groupId='language' queryString>
<TabItem value="json" label="JSON">

```json
{
  "partitioning": [
    {
      "strategy": "list",
      "fieldNames": [["ds"]]
    }
  ]
}
```

</TabItem>
<TabItem value="java" label="Java">

```java
Transform[] partitioning = new Transform[] {
    Transforms.list(new String[][] {{"ds"}})
};
```

</TabItem>
</Tabs>

:::note
Creating partition child tables (e.g., `CREATE TABLE child PARTITION OF parent FOR VALUES IN ('value')`) is not yet supported through Gravitino.
:::

### Table Indexes

- Supports PRIMARY_KEY in CREATE TABLE.
- Adding or deleting indexes via ALTER TABLE is not yet supported by Gravitino (Hologres natively supports index modification via rebuild commands, but this is not yet implemented in Gravitino).

<Tabs groupId='language' queryString>
<TabItem value="json" label="JSON">

```json
{
  "indexes": [
    {
      "indexType": "primary_key",
      "name": "pk_id",
      "fieldNames": [["id"]]
    }
  ]
}
```

</TabItem>
<TabItem value="java" label="Java">

```java
Index[] indexes = new Index[] {
    Indexes.of(IndexType.PRIMARY_KEY, "pk_id", new String[][]{{"id"}}),
};
```

</TabItem>
</Tabs>

### Table Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for more details.

#### Alter Table Operations

Gravitino supports these table alteration operations for Hologres:

- `RenameTable`
- `UpdateComment`
- `AddColumn` (type and comment only; NOT NULL, default value, and auto-increment are not supported)
- `DeleteColumn`
- `RenameColumn`
- `UpdateColumnComment`

:::info
The following ALTER TABLE operations are **not supported** and will throw `IllegalArgumentException`:
- `UpdateColumnType`
- `UpdateColumnDefaultValue`
- `UpdateColumnNullability`
- `UpdateColumnPosition`
- `UpdateColumnAutoIncrement`
- `AddIndex`
- `DeleteIndex`
- `SetProperty`
- `RemoveProperty`
:::
