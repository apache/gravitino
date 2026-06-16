---
title: "Doris Catalog"
slug: "/jdbc-doris-catalog"
keywords:
- jdbc
- Apache Doris
- metadata
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

Apache Gravitino provides the ability to manage [Apache Doris](https://doris.apache.org/) metadata through JDBC connection.

:::caution
Gravitino saves some system information in schema and table comments, like
`(From Gravitino, DO NOT EDIT: gravitino.v1.uid1078334182909406185)`, do not change or remove this message.
:::

## Catalog

### Catalog Capabilities

- Gravitino catalog corresponds to the Doris instance.
- Supports metadata management of Doris (1.2.x).
- Supports table index.
- Supports [column default value](./manage-relational-metadata-using-gravitino.md#table-column-default-value).

### Catalog Properties

Pass to a Doris data source any property that isn't defined by Gravitino by adding
`gravitino.bypass.` prefix as a catalog property. For example, catalog property
`gravitino.bypass.maxWaitMillis` will pass `maxWaitMillis` to the data source property.

Check the relevant data source configuration in
[data source properties](https://commons.apache.org/proper/commons-dbcp/configuration.html) for
more details.

Besides the [common catalog properties](./gravitino-server-config.md#catalog-properties-configuration), the Doris catalog has the following properties:

| Configuration item      | Description                                                                                                                                                                                                                                                                                                                                                                                                      | Default value | Required | Since Version    |
|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `jdbc-url`              | JDBC URL for connecting to the database. For example, `jdbc:mysql://localhost:9030`                                                                                                                                                                                                                                                                                                                              | (none)        | Yes      | 0.5.0            |
| `jdbc-driver`           | The driver of the JDBC connection. For example, `com.mysql.jdbc.Driver`.                                                                                                                                                                                                                                                                                                                                         | (none)        | Yes      | 0.5.0            |
| `jdbc-user`             | The JDBC user name.                                                                                                                                                                                                                                                                                                                                                                                              | (none)        | Yes      | 0.5.0            |
| `jdbc-password`         | The JDBC password.                                                                                                                                                                                                                                                                                                                                                                                               | (none)        | Yes      | 0.5.0            |
| `jdbc.pool.min-size`    | The minimum number of connections in the pool. `2` by default.                                                                                                                                                                                                                                                                                                                                                   | `2`           | No       | 0.5.0            |
| `jdbc.pool.max-size`    | The maximum number of connections in the pool. `10` by default.                                                                                                                                                                                                                                                                                                                                                  | `10`          | No       | 0.5.0            |
| `replication_num`       | The number of replications for the table. If not specified and the number of backend servers less than 3, then the default value is 1; If not specified and the number of backend servers greater or equals to 3, the default value (3) in Doris server will be used. For more, see the [doc](https://doris.apache.org/docs/1.2/sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE/)        | `1` or `3`    | No       | 0.6.0-incubating |
| `jdbc.pool.max-wait-ms` | The maximum Duration that the pool will wait for a connection to be returned. `30000` by default.                                                                                                                                                                                                                                                                                                                | `30000`       | No       | 1.1.0            |

Before using the Doris Catalog, you must download the corresponding JDBC driver to the `catalogs/jdbc-doris/libs` directory.
Gravitino doesn't package the JDBC driver for Doris due to licensing issues.

### Driver Version Compatibility

The Doris catalog includes driver version compatibility checks for datetime precision calculation:

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

### Catalog Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for more details.

:::note
Sensitive catalog properties such as `jdbc-user` and `jdbc-password` are hidden from the load catalog response since Gravitino 1.3.0. Use the [credential vending API](security/credential-vending.md) to retrieve them at runtime.
:::

## Schema

### Schema Capabilities

- Gravitino's schema concept corresponds to the Doris database.
- Supports creating schema.
- Supports dropping schema.

### Schema Properties

- Support schema properties, including Doris database properties and user-defined properties.

### Schema Operations

Refer to
[Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more details.

## Table

### Table Capabilities

- Gravitino's table concept corresponds to the Doris table.
- Supports index.
- Supports [column default value](./manage-relational-metadata-using-gravitino.md#table-column-default-value).

#### Table Column Types

| Gravitino Type   | Doris Type      |
|------------------|-----------------|
| `Boolean`        | `Boolean`       |
| `Byte`           | `TinyInt`       |
| `Short`          | `SmallInt`      |
| `Integer`        | `Int`           |
| `Long`           | `BigInt`        |
| `Float`          | `Float`         |
| `Double`         | `Double`        |
| `Decimal`        | `Decimal`       |
| `Date`           | `Date`          |
| `Timestamp[(p)]` | `Datetime[(p)]` |
| `VarChar`        | `VarChar`       |
| `FixedChar`      | `Char`          |
| `String`         | `String`        |


Doris doesn't support Gravitino `Fixed` `Timestamp_tz` `IntervalDay` `IntervalYear` `Union` `UUID` type.
The data types other than those listed above are mapped to Gravitino's **[Unparsed Type](./manage-relational-metadata-using-gravitino.md#unparsed-type)** that represents an unresolvable data type since 0.5.0.

:::note
Gravitino cannot load Doris `array`, `map` and `struct` type correctly, because Doris doesn't support these types in JDBC.
:::


### Table Column Auto-Increment

Unsupported for now.

### Table Properties

- Doris supports table properties, and you can set them in the table properties.
- Only supports Doris table properties and doesn't support user-defined properties.

### Table Indexes

- Supports PRIMARY_KEY

    Please be aware that the index can only apply to a single column.

    <Tabs groupId='language' queryString>
    <TabItem value="json" label="JSON">

    ```json
    {
      "indexes": [
        {
          "indexType": "primary_key",
          "name": "PRIMARY",
          "fieldNames": [["id"]]
        }
      ]
    }
    ```

    </TabItem>
    <TabItem value="java" label="Java">

    ```java
    Index[] indexes = new Index[] {
        Indexes.of(IndexType.PRIMARY_KEY, "PRIMARY", new String[][]{{"id"}}, Map.of())
    }
    ```

    </TabItem>
    </Tabs>

### Table Partitioning

The Doris catalog supports partitioned tables. 
Users can create partitioned tables in the Doris catalog with specific partitioning attributes. It is also supported to pre-assign partitions when creating Doris tables. 
Note that although Gravitino supports several partitioning strategies, Apache Doris inherently only supports these two partitioning strategies:

- `RANGE`
- `LIST`

:::caution
The `fieldName` specified in the partitioning attributes must be the name of columns defined in the table.
:::

### Table Distribution

Users can also specify the distribution strategy when creating tables in the Doris catalog. The Doris catalog supports the following distribution strategies:
- `HASH`
- `RANDOM`

For the `RANDOM` distribution strategy, Gravitino uses the `EVEN` to represent it. More information about the distribution strategy defined in Gravitino can be found [here](./table-partitioning-distribution-sort-order-indexes.md#table-distribution).


### Table Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for more details.

#### Alter Table Operations

Gravitino supports these table alteration operations:

- `RenameTable`
- `UpdateComment`
- `AddColumn`
- `DeleteColumn`
- `UpdateColumnType`
- `UpdateColumnPosition`
- `UpdateColumnComment`
- `SetProperty`

Please be aware that:

 - Not all table alteration operations can be processed in batches.
 - Schema changes, such as adding/modifying/dropping columns can be processed in batches.
 - Supports modifying multiple column comments at the same time.
 - Doesn't support modifying the column type and column comment at the same time.
 - The schema alteration in Doris is asynchronous. You might get an outdated schema if you
   execute a schema query immediately after the alteration. Pause briefly
   after the alteration. Gravitino will surface the schema-alteration status in the
   schema information in an upcoming release to solve this.
