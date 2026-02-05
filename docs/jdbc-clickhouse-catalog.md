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

Apache Gravitino can manage ClickHouse metadata through a JDBC catalog.

:::caution
Gravitino writes system markers into table comments (for example `(From Gravitino, DO NOT EDIT: ...)`). Do not change or remove them.
:::

## Catalog

### Catalog capabilities

- A catalog maps to one ClickHouse instance.
- Supports JDBC-based metadata management and DDL.
- Supports column default values.

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

:::caution
Download the ClickHouse JDBC driver yourself and place it under `catalogs-contrib/catalog-jdbc-clickhouse/libs`.
:::

### Catalog operations

See [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations).

## Schema

### Schema capabilities

- Gravitino schema maps to a ClickHouse database.
- Supports create and drop (cascade supported by ClickHouse).
- Supports schema comments.

### Schema properties

| Property Name | Description                                                                                      | Default Value | Required | Immutable | Since version |
|---------------|--------------------------------------------------------------------------------------------------|---------------|----------|-----------|---------------|
| `on-cluster`  | Use `ON CLUSTER` when creating the database (cluster name must also be provided at table level)  | `false`       | No       | No        | 1.2.0         |
| `cluster-name`| Cluster name used with `ON CLUSTER` when creating the database                                   | (none)        | No       | No        | 1.2.0         |

Please note that both `on-cluster` and `cluster-name` are optional. If we want to create a schema on a cluster, we need to set `on-cluster` to `true` and provide `cluster-name`.

### Schema operations

See [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations).

## Table

### Table capabilities

- Gravitino table maps to a ClickHouse table.
- Supports comments, column default values, and primary/data-skipping indexes.
- Requires `ORDER BY` for MergeTree-family engines; distribution strategies are not supported.
- Supports single-column identity partitioning on MergeTree-family engines only.

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

| Property Name                | Description                                                                                    | Default Value | Required | Reserved | Immutable | Since version |
|------------------------------|------------------------------------------------------------------------------------------------|---------------|----------|----------|-----------|---------------|
| `engine`                     | Table engine (for example `MergeTree`, `ReplacingMergeTree`, `Distributed`, `Memory`, etc.)    | `MergeTree`   | No       | No       | Yes       | 1.2.0         |
| `cluster-name`               | Cluster name used with `ON CLUSTER` and Distributed engine                                     | (none)        | No\*     | No       | No        | 1.2.0         |
| `on-cluster`                 | Use `ON CLUSTER` when creating the table                                                       | (none)        | No       | No       | No        | 1.2.0         |
| `cluster-remote-database`    | Remote database for `Distributed` engine                                                       | (none)        | No\*\*   | No       | No        | 1.2.0         |
| `cluster-remote-table`       | Remote table for `Distributed` engine                                                          | (none)        | No\*\*   | No       | No        | 1.2.0         |
| `cluster-sharding-key`       | Sharding key for `Distributed` engine                                                          | (none)        | No\*\*   | No       | No        | 1.2.0         |
| `settings.<name>`            | ClickHouse engine setting forwarded as `SETTINGS <name>=<value>`                               | (none)        | No       | No       | No        | 1.2.0         |

\* Required when `on-cluster=true` or `engine=Distributed`.  
\*\* Required when `engine=Distributed`.

### Table indexes

- Supports `PRIMARY_KEY`.
- Supports data-skipping indexes: `DATA_SKIPPING_MINMAX` and `DATA_SKIPPING_BLOOM_FILTER` (granularity fixed internally).

### Partitioning and distribution

- Partitioning: only identity transform on a single column, and only for MergeTree-family engines.
- Distribution: not supported (`Distributions.NONE` is enforced).
- Sort order: MergeTree-family engines require exactly one `ORDER BY` column; other engines do not accept `ORDER BY`.

### Table operations

See [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations).
