---
title: "ClickHouse Catalog"
slug: "/jdbc-clickhouse-catalog"
keywords:
- jdbc
- clickhouse
- metadata
license: "This software is licensed under the Apache License version 2.0."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

The ClickHouse catalog enables Apache Gravitino to manage ClickHouse metadata through a JDBC connection, including databases (mapped to Gravitino schemas), tables, columns, engines (MergeTree family, Log family, Distributed, and others), partitioning, sort orders, indexes, and `ON CLUSTER` execution against ClickHouse clusters. Use it when you want a single Gravitino-managed access surface that covers ClickHouse alongside other relational, lakehouse, and fileset catalogs.

## Requirements and Limitations

- **ClickHouse catalog is not bundled.** The ClickHouse catalog is not included in the standard Gravitino server distribution due to the large size of the ClickHouse JDBC driver and licensing considerations. Build the catalog from source; see [How to Build](./how-to-build.md).
- **Supported ClickHouse versions.** All code is tested against ClickHouse `24.8.14`. Other `24.8.x` releases work with the same driver bundle. Newer ClickHouse versions (`25.x` and later) may work but are not thoroughly tested; report compatibility issues to the community.
- **JDBC driver required.** Place the ClickHouse JDBC driver in `${GRAVITINO_HOME}/catalogs/jdbc-clickhouse/libs` on the Gravitino server. For ClickHouse `24.8.x`, use driver versions `0.7.1` through `0.8.4`. For other ClickHouse versions, refer to the official ClickHouse JDBC documentation; the JAR is available from the [ClickHouse JDBC Maven repository](https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.7.1/).
- **One ClickHouse instance per catalog.** A Gravitino ClickHouse catalog corresponds to one ClickHouse server instance. To manage multiple instances, create one Gravitino catalog per instance.
- **Schema-to-database mapping.** A Gravitino schema maps to a ClickHouse database. Schema comments and `ON CLUSTER` creation are supported through schema properties.
- **Engine constraints.** MergeTree-family, Log-family, `Null`, `Set`, `Memory`, and `Distributed` engines are creatable through Gravitino. Engines that require parameterized ENGINE clauses or external dependencies (`Join`, `Buffer`, `View`, `KeeperMap`, `File`) are not creatable through Gravitino. The `engine` property is immutable after creation.
- **`Memory` engine is volatile.** Tables created with `engine=Memory` lose all data on a ClickHouse server restart. The table definition persists and Gravitino's `loadTable` continues to succeed, but data does not. Use `TinyLog`, `StripeLog`, or a MergeTree-family engine if durability is required.
- **ORDER BY, PARTITION BY constrained.** MergeTree-family engines require `ORDER BY` and accept only column-identity expressions: `id`, `(id, name)`, `(func(id), name)`, or `func(id)`. Composite expressions such as `(id + 1)` are rejected. `PARTITION BY` accepts single-column identity (`PARTITION BY column_name`) and a small set of functions (`toDate`, `toYear`, `toYYYYMM`); other functions and composite expressions such as `PARTITION BY (column_name + 1)` are rejected. ClickHouse itself supports arbitrary partition expressions; the Gravitino layer is intentionally more restrictive.
- **No custom distribution.** Distribution is fixed to `Distributions.NONE`. For sharded layouts, use the `Distributed` engine with the related cluster properties (`cluster-name`, `cluster-remote-database`, `cluster-remote-table`, `cluster-sharding-key`).
- **Cluster metadata is Gravitino-managed only.** ClickHouse does not persist `ON CLUSTER` information in `SHOW CREATE DATABASE` or `SHOW CREATE TABLE` output for non-Replicated objects, so Gravitino embeds the cluster name in the object's `COMMENT` at creation. Databases and tables created outside Gravitino will have `on-cluster=false` and no `cluster-name` when loaded, and Gravitino-issued `DROP` statements against such objects will not include `ON CLUSTER`. Recreate the object through Gravitino if you need accurate cluster metadata.
- **Alter limitations.** `engine` cannot be changed after creation; arbitrary `ALTER TABLE ... SETTINGS` and removal of table properties are not supported; auto-increment columns are not supported; primary key indexes cannot be added or dropped after creation (data-skipping indexes can be added and dropped).

## Quick Start

Create a minimum-viable ClickHouse catalog and confirm it is reachable. The example assumes a Gravitino server at `http://localhost:8090`, a metalake named `test`, and a ClickHouse instance at `localhost:8123`. Adjust the values for your environment. For a fuller create-catalog example with both shell and Java tabs, see [Create a ClickHouse Catalog](#create-a-clickhouse-catalog) below.

### Create the Catalog

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "clickhouse_catalog",
    "type": "RELATIONAL",
    "comment": "ClickHouse catalog",
    "provider": "jdbc-clickhouse",
    "properties": {
      "jdbc-url": "jdbc:clickhouse://localhost:8123",
      "jdbc-driver": "com.clickhouse.jdbc.ClickHouseDriver",
      "jdbc-user": "default",
      "jdbc-password": "<your-password>"
    }
  }' \
  http://localhost:8090/api/metalakes/test/catalogs
```

### Verify the Catalog

```bash
# List catalogs in the metalake. clickhouse_catalog should appear.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs" | jq

# Load the catalog directly and inspect its properties.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/clickhouse_catalog" | jq

# List schemas (ClickHouse databases). The response includes at least `default` and `system`.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/clickhouse_catalog/schemas" | jq
```

**Success check:** the catalog-list response includes `clickhouse_catalog`, the load-catalog response shows `"provider":"jdbc-clickhouse"` with the configured `jdbc-url`, and the schema-list response includes at least the `system` database. If the schema-list call returns a connection error, verify the `jdbc-url`, `jdbc-user`, and `jdbc-password`, and confirm the ClickHouse JDBC driver is present in `${GRAVITINO_HOME}/catalogs/jdbc-clickhouse/libs` on the Gravitino server.

## Catalog

### Catalog Capabilities

| Item              | Description                                                                                                                                                                                                   |
|-------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Scope             | One catalog maps to one ClickHouse instance.                                                                                                                                                                  |
| Metadata/DDL      | Supports JDBC-based metadata management and DDL.                                                                                                                                                              |
| Column defaults   | Supports column default values.                                                                                                                                                                               |

### Catalog Properties

Pass any JDBC pool property that Gravitino does not define by adding the `gravitino.bypass.` prefix (for example `gravitino.bypass.maxWaitMillis`). See [commons-dbcp configuration](https://commons.apache.org/proper/commons-dbcp/configuration.html) for details.

When using the JDBC catalog you must provide `jdbc-url`, `jdbc-driver`, `jdbc-user`, and `jdbc-password`. Common catalog properties are listed [here](./gravitino-server-config.md#catalog-properties); ClickHouse adds no extra catalog-scoped keys.

| Property      | Description                                                                 | Default | Required | Since |
|-------------------------|-----------------------------------------------------------------------------|---------------|----------|---------------|
| `jdbc-url`              | JDBC URL, for example `jdbc:clickhouse://localhost:8123`                    | (none)        | Yes      | 1.2.0         |
| `jdbc-driver`           | JDBC driver class, for example `com.clickhouse.jdbc.ClickHouseDriver`       | (none)        | Yes      | 1.2.0         |
| `jdbc-user`             | JDBC user name                                                              | (none)        | Yes      | 1.2.0         |
| `jdbc-password`         | JDBC password                                                               | (none)        | Yes      | 1.2.0         |
| `jdbc.pool.min-size`    | Minimum pool size                                                           | `2`           | No       | 1.2.0         |
| `jdbc.pool.max-size`    | Maximum pool size                                                           | `10`          | No       | 1.2.0         |
| `jdbc.pool.max-wait-ms` | Max wait time for a connection                                              | `30000`       | No       | 1.2.0         |

### Create a ClickHouse Catalog

The example below creates a ClickHouse catalog with the required JDBC properties. The `provider` value must be `jdbc-clickhouse` for Gravitino to recognize the catalog as ClickHouse, and `type` must be `RELATIONAL`. The `jdbc-driver` class must be available on the Gravitino classpath; place the ClickHouse JDBC driver JAR in `${GRAVITINO_HOME}/catalogs/jdbc-clickhouse/libs` on the Gravitino server.


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
    .put("jdbc-password", "password")
    .build();

Catalog catalog =
    client.createCatalog("ck", Catalog.Type.RELATIONAL, "jdbc-clickhouse", "ClickHouse catalog", ckProps);
```

</TabItem>
</Tabs>

See [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for other catalog operations.

## Schema

### Schema Capabilities

| Item         | Description                                                        |
|--------------|--------------------------------------------------------------------|
| Mapping      | Gravitino schema maps to a ClickHouse database                     |
| Operations   | Create / drop / load / list (ClickHouse supports cascade drop)     |
| Comments     | Schema comments supported                                          |
| Cluster mode | Optional `ON CLUSTER` for creation when `cluster-name` is provided |

### Schema Properties

| Property  | Description                                                                        | Default | Required | Immutable | Since |
|----------------|------------------------------------------------------------------------------------|---------------|----------|-----------|---------------|
| `on-cluster`   | Use `ON CLUSTER` when creating the database                                        | `false`       | No       | No        | 1.2.0         |
| `cluster-name` | Cluster name used with `ON CLUSTER` (must align with table-level cluster settings) | (none)        | No       | No        | 1.2.0         |

:::warning
**Cluster properties only reflect Gravitino-managed schemas.** Gravitino embeds the cluster name inside the schema's `COMMENT` field at creation time (because `SHOW CREATE DATABASE` does not include `ON CLUSTER` for standard Atomic databases). Schemas created outside Gravitino will not have this metadata, so `on-cluster` and `cluster-name` will be absent when loaded, and `DROP SCHEMA` will not propagate `ON CLUSTER` to other cluster nodes.
:::

### Create a Schema

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

### Table Capabilities

| Area                | Details                                                                                                                                                                                                       |
|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Mapping             | Gravitino table maps to a ClickHouse table.                                                                                                                                                                   |
| Engines             | See [Supported Engines](#supported-engines) below.                                                                                                                                                            |
| Ordering/Partition  | MergeTree-family requires exactly one `ORDER BY` column; only single-column identity `PARTITION BY` is supported on MergeTree engines. Other engines reject `ORDER BY` and `PARTITION BY`.                    |
| Indexes             | Primary key; data-skipping indexes `DATA_SKIPPING_MINMAX` and `DATA_SKIPPING_BLOOM_FILTER` with fixed granularities.                                                                                         |
| Distribution        | Gravitino enforces `Distributions.NONE`; no custom distribution strategies.                                                                                                                                   |
| Column defaults     | Supported.                                                                                                                                                                                                    |

#### Supported Engines

| Engine family or engine | Support |
|---|---|
| MergeTree family: `MergeTree` (default), `ReplacingMergeTree`, `SummingMergeTree`, `AggregatingMergeTree`, `CollapsingMergeTree`, `VersionedCollapsingMergeTree`, `GraphiteMergeTree` | Supported. Data and table definition persist across restarts. |
| Log family: `TinyLog`, `StripeLog`, `Log` | Supported. Data and table definition persist across restarts. |
| `Null` | Supported. Table definition persists; data is always discarded by design. |
| `Set` | Supported. Table definition persists. |
| `Memory` | Supported but volatile. Table definition persists; all data is lost on ClickHouse restart. See the warning under [Create a Table](#create-a-table). |
| `Distributed` | Supported. Cluster mode with remote database/table and sharding key configured through table properties. |
| `Join`, `Buffer`, `View`, `KeeperMap`, `File` | Not directly creatable through Gravitino; these engines require parameterized ENGINE clauses or external dependencies that the CREATE TABLE API does not expose. |

### Table Column Types

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

### Table Properties

:::note
- `settings.*` keys are passed to the ClickHouse `SETTINGS` clause verbatim.
- The `engine` value is immutable after creation.
:::

:::warning
**Cluster properties only reflect Gravitino-managed objects.**
ClickHouse does not persist `ON CLUSTER` information in `SHOW CREATE TABLE` or `SHOW CREATE DATABASE` output for non-Replicated objects. Gravitino works around this by embedding the cluster name in the object's `COMMENT` field at creation time and reading it back on load/drop.

This means:
- **Gravitino-created databases and tables**: `on-cluster` and `cluster-name` properties are accurate.
- **Databases or tables created outside Gravitino** (e.g., via ClickHouse client, migration scripts, or other tools): `on-cluster` will be `false` and `cluster-name` will be absent, regardless of whether the object was actually created `ON CLUSTER`. Subsequent `DROP DATABASE` / `DROP TABLE` operations performed through Gravitino will **not** include `ON CLUSTER`, which may leave orphan objects on non-coordinating cluster nodes.

If you need Gravitino to manage an existing cluster database or table, recreate it through the Gravitino API so the cluster metadata is properly embedded.
:::

:::warning
**Memory engine data volatility**: Tables created with `engine=Memory` store data in RAM only. After a ClickHouse server restart the table definition persists (Gravitino's `loadTable` succeeds), but all data is permanently lost. Gravitino metadata and ClickHouse remain consistent at the schema level, but users are responsible for repopulating data after restarts. Consider using `TinyLog`, `StripeLog`, or a MergeTree-family engine if data durability is required.
:::

| Property              | Description                                                                                              | Default | Required | Reserved | Immutable | Since |
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

### Table Indexes

- `PRIMARY_KEY`
- Data-skipping indexes:
  - `DATA_SKIPPING_MINMAX` (`GRANULARITY` fixed to 1)
  - `DATA_SKIPPING_BLOOM_FILTER` (`GRANULARITY` fixed to 3)

### Partitioning, Sorting, and Distribution

`ORDER BY` is required on MergeTree-family engines and accepts only column-identity expressions:

- Accepted: `id`, `(id, name)`, `(func(id), name)`, `func(id)`.
- Rejected: composite expressions such as `(id + 1)` or `(func(id) + 1)`.

`PARTITION BY` is supported only on MergeTree-family engines and accepts single-column identity or a small set of functions:

- Identity: `PARTITION BY column_name`.
- Functions: `PARTITION BY toDate(column_name)`, `PARTITION BY toYear(column_name)`, `PARTITION BY toYYYYMM(column_name)`. Other functions are not supported.
- Rejected: composite expressions such as `PARTITION BY (column_name + 1)` or `PARTITION BY (toYear(column_name) + 1)`.

ClickHouse itself supports arbitrary partition expressions. The Gravitino layer is intentionally more restrictive.

Distribution is fixed to `Distributions.NONE`. To shard data across nodes, use the `Distributed` engine and configure the sharding key and remote database/table through table properties (`cluster-sharding-key`, `cluster-remote-database`, `cluster-remote-table`).

### Create a Table

The following example creates a `MergeTree` table with `ORDER BY`, partitioning, indexes, comments, and properties including `ON CLUSTER`. Note that the `engine` property is required for MergeTree-family tables, and that the cluster properties must align with the schema-level cluster settings if `on-cluster=true`.

The equivalent ClickHouse SQL statement is:

```sql
CREATE TABLE sales.orders ON CLUSTER ck_cluster (
  order_id Int32,
  user_id Int32,
  amount Decimal(18,2),
  created_at DateTime,
  PRIMARY KEY (order_id)
) ENGINE = MergeTree
ORDER BY order_id
PARTITION BY created_at;
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

### Table Operations

Supported:

- Creating a table with engine, `ORDER BY`, optional partition, indexes, comments, default values, and `SETTINGS`.
- Adding a column with nullable flag, default, comment, and position.
- Renaming a column.
- Updating a column's type, comment, default, position, or nullability.
- Deleting columns.
- Adding and dropping data-skipping indexes.
- Updating the table comment.

Unsupported:

- Changing the engine after creation.
- Adding or dropping the primary key after creation.
- Removing table properties or arbitrary `ALTER TABLE ... SETTINGS`.
- Auto-increment columns.

See [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for common JDBC semantics.
