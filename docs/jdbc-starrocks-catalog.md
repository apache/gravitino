---
title: "StarRocks catalog"
slug: /jdbc-starrocks-catalog
keywords:
- jdbc
- starrocks
- metadata
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

Apache Gravitino provides the ability to manage [StarRocks](https://www.starrocks.io/) metadata through JDBC connection.

:::caution
Gravitino saves some system information in table comments, like
`(From Gravitino, DO NOT EDIT: gravitino.v1.uid1078334182909406185)`, please don't change or remove this message.
:::

## Catalog

### Catalog capabilities

- Gravitino catalog corresponds to the StarRocks instance.
- Supports metadata management of StarRocks (3.3.x).
- Supports [column default value](./manage-relational-metadata-using-gravitino.md#table-column-default-value).

### Catalog properties

You can pass to a StarRocks data source any property that isn't defined by Gravitino by adding
`gravitino.bypass.` prefix as a catalog property. For example, catalog property
`gravitino.bypass.maxWaitMillis` will pass `maxWaitMillis` to the data source property.

You can check the relevant data source configuration in
[data source properties](https://commons.apache.org/proper/commons-dbcp/configuration.html) for
more details.

Besides the [common catalog properties](./gravitino-server-config.md#apache-gravitino-catalog-properties-configuration), the StarRocks catalog has the following properties:

| Configuration item   | Description                                                                                                                                                                                                                                                                                                                                                                                                      | Default value | Required | Since Version    |
|----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `jdbc-url`           | JDBC URL for connecting to the database. For example, `jdbc:mysql://localhost:9030`                                                                                                                                                                                                                                                                                                                              | (none)        | Yes      | 1.0.0            |
| `jdbc-driver`        | The driver of the JDBC connection. For example, `com.mysql.jdbc.Driver`.                                                                                                                                                                                                                                                                                                                                         | (none)        | Yes      | 1.0.0            |
| `jdbc-user`          | The JDBC user name.                                                                                                                                                                                                                                                                                                                                                                                              | (none)        | Yes      | 1.0.0            |
| `jdbc-password`      | The JDBC password.                                                                                                                                                                                                                                                                                                                                                                                               | (none)        | Yes      | 1.0.0            |
| `jdbc.pool.min-size` | The minimum number of connections in the pool. `2` by default.                                                                                                                                                                                                                                                                                                                                                   | `2`           | No       | 1.0.0            |
| `jdbc.pool.max-size` | The maximum number of connections in the pool. `10` by default.                                                                                                                                                                                                                                                                                                                                                  | `10`          | No       | 1.0.0            |
| `jdbc.pool.max-size` | The maximum number of connections in the pool. `10` by default.                                                                                                                                                                                                                                                                                                                                                  | `10`          | No       | 1.0.0            |


Before using the StarRocks Catalog, you must download the corresponding JDBC driver to the `catalogs/jdbc-starrocks/libs` directory.
Gravitino doesn't package the JDBC driver for StarRocks due to licensing issues.

### Driver Version Compatibility

The StarRocks catalog includes driver version compatibility checks for datetime precision calculation:

- **MySQL Connector/J versions >= 8.0.16**: Full support for datetime precision calculation
- **MySQL Connector/J versions < 8.0.16**: Limited support - datetime precision calculation returns `null` with a warning log

This limitation affects the following datetime types:
- `DATETIME(p)` - datetime precision

When using an unsupported driver version, the system will:
1. Continue to work normally with default precision (0)
2. Log a warning message indicating the driver version limitation
3. Return `null` for precision calculations to avoid incorrect results

**Example warning log:**
```
WARN: MySQL driver version mysql-connector-java-8.0.11 is below 8.0.16, 
columnSize may not be accurate for precision calculation. 
Returning null for DATETIME type precision. Driver version: mysql-connector-java-8.0.11
```

**Recommended driver versions:**
- `mysql-connector-java-8.0.16` or higher

### Catalog operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema capabilities

- Gravitino's schema concept corresponds to the StarRocks database.
- Supports creating schema.
- Supports dropping schema.

### Schema properties

As StarRocks can't get thr properties after set, So now we do not support set Schema properties.  

### Schema operations

Please refer to
[Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more details.

## Table

### Table capabilities

- Gravitino's table concept corresponds to the StarRocks table.
- Supports [column default value](./manage-relational-metadata-using-gravitino.md#table-column-default-value).

#### Table column types

| Gravitino Type | StarRocks Type |
|----------------|----------------|
| `Boolean`      | `Boolean`      |
| `Byte`         | `TinyInt`      |
| `Short`        | `SmallInt`     |
| `Integer`      | `Int`          |
| `Long`         | `BigInt`       |
| `Float`        | `Float`        |
| `Double`       | `Double`       |
| `Decimal`      | `Decimal`      |
| `Date`         | `Date`         |
| `Timestamp`    | `Datetime`     |
| `VarChar`      | `VarChar`      |
| `FixedChar`    | `Char`         |
| `String`       | `String`       |
| `Binary`       | `Binary`       |


StarRocks doesn't support Gravitino `Fixed` `Timestamp_tz` `IntervalDay` `IntervalYear` `Union` `UUID` type.
The data types other than those listed above are mapped to Gravitino's **[Unparsed Type](./manage-relational-metadata-using-gravitino.md#unparsed-type)** that represents an unresolvable data type since 1.0.0.

:::note
Gravitino can not load StarRocks `array`, `map` and `struct` type correctly, because StarRocks doesn't support these types in JDBC.
:::


### Table column auto-increment

Unsupported for now.

### Table properties

- StarRocks supports table properties, and you can set them in the table properties.
- Only supports StarRocks table properties and doesn't support user-defined properties.

### Table indexes

Unsupported

### Table partitioning

The StarRocks catalog supports partitioned tables. 
Users can create partitioned tables in the StarRocks catalog with specific partitioning attributes. It is also supported to pre-assign partitions when creating StarRocks tables. 
Note that although Gravitino supports several partitioning strategies, StarRocks inherently only supports these two partitioning strategies:

- `RANGE`
- `LIST`

:::caution
The `fieldName` specified in the partitioning attributes must be the name of columns defined in the table.
:::

### Table distribution

Users can also specify the distribution strategy when creating tables in the StarRocks catalog. Currently, the StarRocks catalog supports the following distribution strategies:
- `HASH`
- `RANDOM`

For the `RANDOM` distribution strategy, Gravitino uses the `EVEN` to represent it. More information about the distribution strategy defined in Gravitino can be found [here](./table-partitioning-distribution-sort-order-indexes.md#table-distribution).


### Table operations

Please refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for more details.

#### Alter table operations

Gravitino supports these table alteration operations:

- `RenameTable`
- `UpdateComment`
- `AddColumn`
- `DeleteColumn`
- `UpdateColumnType`
- `UpdateColumnPosition`
- `SetProperty`

Please be aware that:

 - Not all table alteration operations can be processed in batches.
 - Schema changes, such as adding/modifying/dropping columns can be processed in batches.
 - The schema alteration in StarRocks is asynchronous. You might get an outdated schema if you
   execute a schema query immediately after the alteration. It is recommended to pause briefly
   after the schema alteration. Gravitino will add the schema alteration status into
   the schema information in the upcoming version to solve this problem.
- StarRocks has limited support for [alert table properties](https://docs.starrocks.io/docs/3.3/sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE/#modify-table-properties), And it suggests modify one property at a time.  