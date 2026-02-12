---
title: "ClickHouse catalog"
slug: /jdbc-clickhouse-catalog
keywords:
- jdbc
- clickhouse
- metadata
license: "This software is licensed under the Apache License version 2.0."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

Apache Gravitino can manage ClickHouse metadata through a JDBC catalog. This document describes the capabilities and limitations of the ClickHouse catalog, as well as the supported operations and properties for catalogs, schemas, and tables.

## Catalog

### Catalog capabilities

| Item            | Description                                                             |
|-----------------|-------------------------------------------------------------------------|
| Scope           | One catalog maps to one ClickHouse instance                             |
| Metadata/DDL    | Supports JDBC-based metadata management and DDL                         |
| Column defaults | Supports column default values                                          |
| Drivers         | Requires user-provided ClickHouse JDBC driver in `catalogs-contrib/catalog-jdbc-clickhouse/libs` |

### Catalog properties

You can pass any JDBC pool property that Gravitino does not define by adding the `gravitino.bypass.` prefix (for example `gravitino.bypass.maxWaitMillis`). See [commons-dbcp configuration](https://commons.apache.org/proper/commons-dbcp/configuration.html) for details.

When using the JDBC catalog you must provide `jdbc-url`, `jdbc-driver`, `jdbc-user`, and `jdbc-password`. Common catalog properties are listed [here](./gravitino-server-config.md#apache-gravitino-catalog-properties-configuration); ClickHouse adds no extra catalog-scoped keys.

| Configuration item      | Description                                                                 | Default value | Required | Since Version |
|-------------------------|-----------------------------------------------------------------------------|---------------|----------|---------------|
| `jdbc-url`              | JDBC URL, for example `jdbc:clickhouse://localhost:8123`                    | (none)        | Yes      | 1.2.0         |
| `jdbc-driver`           | JDBC driver class, for example `com.clickhouse.jdbc.ClickHouseDriver`       | (none)        | Yes      | 1.2.0         |
| `jdbc-user`             | JDBC user name                                                              | (none)        | Yes      | 1.2.0         |
| `jdbc-password`         | JDBC password                                                               | (none)        | Yes      | 1.2.0         |
| `jdbc.pool.min-size`    | Minimum pool size                                                           | `2`           | No       | 1.2.0         |
| `jdbc.pool.max-size`    | Maximum pool size                                                           | `10`          | No       | 1.2.0         |
| `jdbc.pool.max-wait-ms` | Max wait time for a connection                                              | `30000`       | No       | 1.2.0         |

### Create a ClickHouse catalog

The following example creates a ClickHouse catalog with the required JDBC properties and optional connection pool settings. Note that the `jdbc-driver` class must be available in the Gravitino classpath (for example by placing the ClickHouse JDBC driver JAR in `catalogs/catalog-jdbc-clickhouse/libs`).
Description about some of the properties:
- provider: must be `jdbc-clickhouse` for Gravitino to recognize the catalog as ClickHouse;
- type: must be `RELATIONAL` since ClickHouse is a relational database; 


<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "ck",
  "type": "RELATIONAL",
  "comment": "ClickHouse catalog",
  "provider": "jdbc-clickhouse",
  "properties": {
    "jdbc-url": "jdbc:clickhouse://localhost:8123",
    "jdbc-driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "jdbc-user": "default",
    "jdbc-password": "password"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient client = GravitinoClient.builder("http://localhost:8090")
    .withMetalake("metalake")
    .build();

Map<String, String> ckProps = ImmutableMap.<String, String>builder()
    .put("jdbc-url", "jdbc:clickhouse://localhost:8123")
    .put("jdbc-driver", "com.clickhouse.jdbc.ClickHouseDriver")
    .put("jdbc-user", "default")
    .put("jdbc-password", "passw0rd")
    .build();

Catalog catalog =
    client.createCatalog("ck", Catalog.Type.RELATIONAL, "jdbc-clickhouse", "ClickHouse catalog", ckProps);
```

</TabItem>
</Tabs>

See [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for other catalog operations.

## Schema

### Schema capabilities

| Item         | Description                                                                                  |
|--------------|----------------------------------------------------------------------------------------------|
| Mapping      | Gravitino schema maps to a ClickHouse database                                               |
| Operations   | Create / drop (ClickHouse supports cascade drop)                                             |
| Comments     | Schema comments supported                                                                    |
| Cluster mode | Optional `ON CLUSTER` for creation when `cluster-name` is provided                           |

### Schema properties

| Property Name  | Description                                                                                     | Default Value | Required | Immutable | Since version |
|----------------|-------------------------------------------------------------------------------------------------|---------------|----------|-----------|---------------|
| `on-cluster`   | Use `ON CLUSTER` when creating the database                                                     | `false`       | No       | No        | 1.2.0         |
| `cluster-name` | Cluster name used with `ON CLUSTER` (must align with table-level cluster settings)             | (none)        | No       | No        | 1.2.0         |

### Create a schema

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "sales",
  "comment": "Sales database",
  "properties": {
    "on-cluster": "true",
    "cluster-name": "ck_cluster"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/ck/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = client.loadCatalog("ck");
Schema schema = catalog.asTableCatalog()
    .createSchema("sales", "Sales database",
        ImmutableMap.of("on-cluster", "true", "cluster-name", "ck_cluster"));
```

</TabItem>
</Tabs>

See [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more schema operations.

## Table

### Table capabilities

| Area                | Details                                                                                                                                                                                                                 |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Mapping             | Gravitino table maps to a ClickHouse table                                                                                                                                                                             |
| Engines             | Local engines: MergeTree family (`MergeTree` default, `ReplacingMergeTree`, `SummingMergeTree`, `AggregatingMergeTree`, `CollapsingMergeTree`, `VersionedCollapsingMergeTree`, `GraphiteMergeTree`), Tiny/Stripe/Log, Memory, File, Null, Set, Join, View, Buffer, KeeperMap, etc. Distributed engine supports cluster mode with remote database/table and sharding key. |
| Ordering/Partition  | MergeTree-family requires exactly one `ORDER BY` column; only single-column identity `PARTITION BY` is supported on MergeTree engines. Other engines reject `ORDER BY`/`PARTITION BY`.                                   |
| Indexes             | Primary key; data-skipping indexes `DATA_SKIPPING_MINMAX` and `DATA_SKIPPING_BLOOM_FILTER` (fixed granularities).                                                                                                      |
| Distribution        | Gravitino enforces `Distributions.NONE`; no custom distribution strategies.                                                                                                                                           |
| Column defaults     | Supported.                                                                                                                                                                                                             |
| Unsupported         | Engine change after creation; removing table properties; auto-increment columns.                                                                                                                                       |

### Table column types

| Gravitino Type      | ClickHouse Type                        |
|---------------------|----------------------------------------|
| `Byte`              | `Int8`                                 |
| `Unsigned Byte`     | `UInt8`                                |
| `Short`             | `Int16`                                |
| `Unsigned Short`    | `UInt16`                               |
| `Integer`           | `Int32`                                |
| `Unsigned Integer`  | `UInt32`                               |
| `Long`              | `Int64`                                |
| `Unsigned Long`     | `UInt64`                               |
| `Float`             | `Float32`                              |
| `Double`            | `Float64`                              |
| `Decimal(p,s)`      | `Decimal(p,s)`                         |
| `String`/`VarChar`  | `String`                               |
| `FixedChar(n)`      | `FixedString(n)`                       |
| `Date`              | `Date`                                 |
| `Timestamp[(p)]`    | `DateTime` (precision defaults to `0`) |
| `BOOLEAN`           | `Bool`                                 |
| `UUID`              | `UUID`                                 |

Other ClickHouse types are exposed as [External Type](./manage-relational-metadata-using-gravitino.md#external-type).

### Table properties

:::note
- `settings.*` keys are passed to the ClickHouse `SETTINGS` clause verbatim.  
- The `engine` value is immutable after creation.
:::

| Property Name              | Description                                                                                              | Default Value | Required | Reserved | Immutable | Since version |
|----------------------------|----------------------------------------------------------------------------------------------------------|---------------|----------|----------|-----------|---------------|
| `engine`                   | Table engine (for example `MergeTree`, `ReplacingMergeTree`, `Distributed`, `Memory`, etc.)              | `MergeTree`   | No       | No       | Yes       | 1.2.0         |
| `cluster-name`             | Cluster name used with `ON CLUSTER` and Distributed engine                                               | (none)        | No\*     | No       | No        | 1.2.0         |
| `on-cluster`               | Use `ON CLUSTER` when creating the table                                                                 | (none)        | No       | No       | No        | 1.2.0         |
| `cluster-remote-database`  | Remote database for `Distributed` engine                                                                 | (none)        | No\*\*   | No       | No        | 1.2.0         |
| `cluster-remote-table`     | Remote table for `Distributed` engine                                                                    | (none)        | No\*\*   | No       | No        | 1.2.0         |
| `cluster-sharding-key`     | Sharding key for `Distributed` engine (expression allowed; referenced columns must be non-null integral) | (none)        | No\*\*   | No       | No        | 1.2.0         |
| `settings.<name>`          | ClickHouse engine setting forwarded as `SETTINGS <name>=<value>`                                         | (none)        | No       | No       | No        | 1.2.0         |

\* Required when `on-cluster=true` or `engine=Distributed`.  
\*\* Required when `engine=Distributed`.

### Table indexes

- `PRIMARY_KEY`
- Data-skipping indexes:
  - `DATA_SKIPPING_MINMAX` (`GRANULARITY` fixed to 1)
  - `DATA_SKIPPING_BLOOM_FILTER` (`GRANULARITY` fixed to 3)

### Partitioning, sorting, and distribution

- `ORDER BY`: required for MergeTree-family engines and only columns identity are supported; for example `ORDER BY order_id` is supported, but `ORDER BY (order_id + 1)` is not supported. Other engines reject `ORDER BY` clause.
- `PARTITION BY`: single-column identity and some functions are supported only, and only for MergeTree-family engines. For example `PARTITION BY created_at` or `PARTITION BY toYYYYMM(created_at)` are supported, but `PARTITION BY (created_at + 1)` are not supported.
   In all, the following partitioning expressions are supported:
   - Identity: `PARTITION BY column_name`
   - Functions: `PARTITION BY toDate(column_name)`, `PARTITION BY toYear(column_name)`, `PARTITION BY toYYYYMM(column_name)`. Other functions are not supported.
   - Not support: `PARTITION BY (column_name + 1)`, `PARTITION BY (toYear(column_name) + 1)`, etc.
- Distribution: fixed to `Distributions.NONE` (no custom distribution). ClickHouse does not support custom distribution strategies since distribution is handled by the engine (for example `Distributed` engine with sharding key).

### Create a table

The following example creates a `MergeTree` table with `ORDER BY`, partitioning, indexes, comments, and properties including `ON CLUSTER`. Note that the `engine` property is required for MergeTree-family tables, and that the cluster properties must align with the schema-level cluster settings if `on-cluster=true`.

This is a create table statement that would be executed in ClickHouse to create the corresponding table:
```sql
CREATE TABLE sales.orders ON CLUSTER ck_cluster (
  order_id Int32,
  user_id Int32,
  amount Decimal(18,2),
  created_at DateTime,
  primary key (order_id),
) ENGINE = MergeTree order BY order_id PARTITION BY created_at;

```

The same table can be created through the API as follows:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "orders",
  "comment": "Orders table",
  "columns": [
    {"name": "order_id", "type": "int", "nullable": false},
    {"name": "user_id", "type": "int", "nullable": false},
    {"name": "amount", "type": "decimal(18,2)", "nullable": false},
    {"name": "created_at", "type": "timestamp", "nullable": false}
  ],
  "properties": {
    "engine": "MergeTree",
    "on-cluster": "true",
    "cluster-name": "ck_cluster"
  },
  "sortOrders": [
    {"expression": "order_id", "direction": "ASCENDING"}
  ],
  "partitioning": ["created_at"],
  "indexes": [
    {"indexType": "primary_key", "name": "pk_order", "fieldNames": [["order_id"]]}
  ]
}' http://localhost:8090/api/metalakes/metalake/catalogs/ck/schemas/sales/tables
```

</TabItem>
<TabItem value="java" label="Java">

```java
TableCatalog tableCatalog = client.loadCatalog("ck").asTableCatalog();

Column[] columns = new Column[] {
    Column.of("order_id", Types.IntegerType.get(), "Order ID", false),
    Column.of("user_id", Types.IntegerType.get(), "User ID", false),
    Column.of("amount", Types.DecimalType.of(18, 2), "Amount", false),
    Column.of("created_at", Types.TimestampType.withoutTimeZone(), "Created time", false)
};

Index[] indexes =
    new Index[] {Indexes.of(Index.IndexType.PRIMARY_KEY, "pk_order", new String[][] {{"order_id"}})};

SortOrder[] sortOrders =
    new SortOrder[] {SortOrder.builder("order_id").withDirection(SortDirection.ASCENDING).build()};

Transform[] partitions = new Transform[] {Transforms.identity("created_at")};

tableCatalog.createTable(
    NameIdentifier.of("sales", "orders"),
    columns,
    "Orders table",
    ImmutableMap.of("engine", "MergeTree", "on-cluster", "true", "cluster-name", "ck_cluster"),
    partitions,
    Distributions.NONE,
    indexes,
    sortOrders);
```

</TabItem>
</Tabs>

### Table operations

Supported:
- Create table with engine, `ORDER BY`, optional partition, indexes, comments, default values, and `SETTINGS`.
- Add column (with nullable flag, default, comment, position).
- Rename column.
- Update column type/comment/default/position/nullability.
- Delete columns (with `IF EXISTS` support).
- Add primary or data-skipping indexes; drop data-skipping indexes.
- Update table comment.

Unsupported:
- Changing engine after creation.
- Removing table properties or arbitrary `ALTER TABLE ... SETTINGS`.
- Auto-increment columns.

See [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for common JDBC semantics.
