---
title: "Hologres catalog"
slug: /jdbc-hologres-catalog
keywords:
- jdbc
- Hologres
- metadata
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

Apache Gravitino provides the ability to manage [Hologres](https://help.aliyun.com/zh/hologres) metadata.

Hologres is a real-time data warehouse service provided by Alibaba Cloud, designed for high-concurrency and low-latency online analytical processing (OLAP). Hologres is fully compatible with the PostgreSQL protocol and uses the PostgreSQL JDBC Driver for connections.

:::caution
Gravitino saves some system information in schema and table comment, like `(From Gravitino, DO NOT EDIT: gravitino.v1.uid1078334182909406185)`, please don't change or remove this message.
:::

## Catalog

### Catalog capabilities

- Gravitino catalog corresponds to a Hologres database instance.
- Supports metadata management of Hologres.
- Supports DDL operation for Hologres schemas and tables.
- Supports table index (PRIMARY KEY in CREATE TABLE).
- Supports [column default value](./manage-relational-metadata-using-gravitino.md#table-column-default-value).
- Supports LIST partitioning (physical and logical partition tables).
- Supports Hologres-specific table properties via `WITH` clause (orientation, clustering_key, distribution_key, etc.).
- Does not support [auto-increment](./manage-relational-metadata-using-gravitino.md#table-column-auto-increment).

### Catalog properties

You can pass to a Hologres data source any property that isn't defined by Gravitino by adding `gravitino.bypass.` prefix as a catalog property. For example, catalog property `gravitino.bypass.maxWaitMillis` will pass `maxWaitMillis` to the data source property.

Check the relevant data source configuration in [data source properties](https://commons.apache.org/proper/commons-dbcp/configuration.html)

If you use a JDBC catalog, you must provide `jdbc-url`, `jdbc-driver`, `jdbc-database`, `jdbc-user` and `jdbc-password` to catalog properties.
Besides the [common catalog properties](./gravitino-server-config.md#apache-gravitino-catalog-properties-configuration), the Hologres catalog has the following properties:

| Configuration item      | Description                                                                                                                                                           | Default value | Required | Since Version    |
|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `jdbc-url`              | JDBC URL for connecting to the database. For example, `jdbc:postgresql://hgprecn-cn-xxx.hologres.aliyuncs.com:80/my_database`                                        | (none)        | Yes      | 0.9.0-incubating |
| `jdbc-driver`           | The driver of the JDBC connection. Must be `org.postgresql.Driver`.                                                                                                   | (none)        | Yes      | 0.9.0-incubating |
| `jdbc-database`         | The database name. This is mandatory for Hologres.                                                                                                                    | (none)        | Yes      | 0.9.0-incubating |
| `jdbc-user`             | The JDBC user name (AccessKey ID or database username).                                                                                                               | (none)        | Yes      | 0.9.0-incubating |
| `jdbc-password`         | The JDBC password (AccessKey Secret or database password).                                                                                                            | (none)        | Yes      | 0.9.0-incubating |
| `jdbc.pool.min-size`    | The minimum number of connections in the pool. `2` by default.                                                                                                        | `2`           | No       | 0.9.0-incubating |
| `jdbc.pool.max-size`    | The maximum number of connections in the pool. `10` by default.                                                                                                       | `10`          | No       | 0.9.0-incubating |

:::caution
Hologres uses the PostgreSQL JDBC Driver (version 42.3.2 or later recommended). Since the PostgreSQL JDBC Driver is already bundled with the Hologres catalog, you don't need to download it separately.
:::

### Catalog operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema capabilities

- Gravitino's schema concept corresponds to the Hologres (PostgreSQL) schema.
- Supports creating schema with comment.
- Supports dropping schema.
- System schemas are automatically filtered: `pg_toast`, `pg_catalog`, `information_schema`, `hologres`, `hg_internal`, `hg_recyclebin`, `hologres_object_table`, `hologres_sample`, `hologres_streaming_mv`, `hologres_statistic`.

### Schema properties

- Doesn't support any schema property settings.

### Schema operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more details.

## Table

### Table capabilities

- Gravitino's table concept corresponds to the Hologres table.
- Supports DDL operation for Hologres tables.
- Supports PRIMARY KEY index in CREATE TABLE.
- Supports [column default value](./manage-relational-metadata-using-gravitino.md#table-column-default-value).
- Supports generated (stored computed) columns.
- Supports LIST partitioning (physical and logical).
- Does not support [auto-increment](./manage-relational-metadata-using-gravitino.md#table-column-auto-increment). Creating auto-increment columns is rejected in both CREATE TABLE and ALTER TABLE.

### Table properties

Hologres-specific table properties are set via the `WITH` clause during CREATE TABLE and read from the `hologres.hg_table_properties` system table. The following user-relevant properties are supported:

| Property Key                        | Description                       | Example Value    |
|-------------------------------------|-----------------------------------|------------------|
| `orientation`                       | Storage format                    | `column`, `row`, `row,column` |
| `clustering_key`                    | Clustering key columns            | `id:asc`         |
| `segment_key`                       | Event time column (segment key)   | `create_time`    |
| `bitmap_columns`                    | Bitmap index columns              | `status,category`|
| `dictionary_encoding_columns`       | Dictionary encoding columns       | `city,province`  |
| `time_to_live_in_seconds`           | Data TTL setting                  | `2592000`        |
| `table_group`                       | Table group name                  | `my_table_group` |
| `storage_format`                    | Internal storage format           | `orc`, `sst`     |
| `binlog_level`                      | Binlog level                      | `replica`, `none`|
| `binlog_ttl`                        | Binlog TTL                        | `86400`          |

:::info
- Modifying table properties via ALTER TABLE `SetProperty` / `RemoveProperty` is not yet supported.
- The properties `distribution_key`, `is_logical_partitioned_table`, and `primary_key` are managed via their dedicated parameters (Distribution, Partitioning, Indexes) and should not be set directly in table properties.
:::

### Table column types

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

### Table distribution

Hologres supports HASH distribution via the `distribution_key` property in the `WITH` clause.

<Tabs groupId='language' queryString>
<TabItem value="json" label="Json">

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

### Table partitioning

Hologres supports LIST partitioning with two variants:

- **Physical partition tables**: `PARTITION BY LIST(column)` — supports exactly 1 partition column.
- **Logical partition tables** (Hologres V3.1+): `LOGICAL PARTITION BY LIST(col1[, col2])` — supports 1–2 partition columns. Enabled by setting property `is_logical_partitioned_table` to `true`.

<Tabs groupId='language' queryString>
<TabItem value="json" label="Json">

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

### Table indexes

- Supports PRIMARY_KEY in CREATE TABLE.
- Adding or deleting indexes via ALTER TABLE is not supported.

<Tabs groupId='language' queryString>
<TabItem value="json" label="Json">

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

### Table operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for more details.

#### Alter table operations

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
