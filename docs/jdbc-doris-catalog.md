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

The Doris catalog enables Apache Gravitino to manage [Apache Doris](https://doris.apache.org/) metadata through a JDBC connection, including databases (mapped to Gravitino schemas), tables, columns, indexes, and column-level defaults. Use it when you want a single Gravitino-managed access surface that covers Doris alongside other relational, lakehouse, and fileset catalogs.

:::caution
Gravitino saves some system information in schema and table comments, like
`(From Gravitino, DO NOT EDIT: gravitino.v1.uid1078334182909406185)`, do not change or remove this message.
:::

### Requirements and Limitations

- **Supported Doris versions:** 1.2.x.
- **JDBC driver required.** Doris uses the MySQL JDBC driver. Place the driver in `catalogs/jdbc-doris/libs` on the Gravitino server. Use `mysql-connector-java-8.0.16` or higher to get accurate datetime precision values; see [Driver Version Compatibility](#driver-version-compatibility) below.
- **One Doris instance per catalog.** A Gravitino Doris catalog corresponds to one Doris instance. A Gravitino schema maps to a Doris database on that instance.
- **Indexes and column defaults are supported.** Table indexes and column default values are managed through Gravitino.
- **No auto-increment columns.**

## Quick Start

Create a minimum-viable Doris catalog and confirm it is reachable. The example assumes a Gravitino server at `http://localhost:8090`, a metalake named `test`, and a Doris instance with the MySQL-compatible frontend at `localhost:9030`. Adjust the values for your environment.

### Create the Catalog

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "doris_catalog",
    "type": "RELATIONAL",
    "comment": "Doris catalog",
    "provider": "jdbc-doris",
    "properties": {
      "jdbc-url": "jdbc:mysql://localhost:9030",
      "jdbc-driver": "com.mysql.cj.jdbc.Driver",
      "jdbc-user": "<your-user>",
      "jdbc-password": "<your-password>"
    }
  }' \
  http://localhost:8090/api/metalakes/test/catalogs
```

### Verify the Catalog

```bash
# List catalogs in the metalake. doris_catalog should appear.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs" | jq

# Load the catalog directly and inspect its properties.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/doris_catalog" | jq

# List schemas (Doris databases). The response includes at least `information_schema`.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/doris_catalog/schemas" | jq
```

**Success check:** the catalog-list response includes `doris_catalog`, the load-catalog response shows `"provider":"jdbc-doris"` with the configured `jdbc-url`, and the schema-list response includes at least the `information_schema` database. If the schema-list call returns a connection error, verify the `jdbc-url`, `jdbc-user`, and `jdbc-password` values, and confirm the MySQL JDBC driver is present in `catalogs/jdbc-doris/libs` on the Gravitino server.

## Catalog

### Catalog Capabilities

A Gravitino Doris catalog corresponds to a Doris instance (1.2.x) and provides:

- Metadata management.
- Table indexes.
- [Column default values](./manage-relational-metadata-using-gravitino.md#table-column-default-value).

### Catalog Properties

Pass to a Doris data source any property that isn't defined by Gravitino by adding
`gravitino.bypass.` prefix as a catalog property. For example, catalog property
`gravitino.bypass.maxWaitMillis` will pass `maxWaitMillis` to the data source property.

Check the relevant data source configuration in
[data source properties](https://commons.apache.org/proper/commons-dbcp/configuration.html) for
more details.

Besides the [common catalog properties](./gravitino-server-config.md#catalog-properties), the Doris catalog has the following properties:

| Property      | Description                                                                                                                                                                                                                                                                                                                                                                                                      | Default | Required | Since    |
|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `jdbc-url`              | JDBC URL for connecting to the database. For example, `jdbc:mysql://localhost:9030`                                                                                                                                                                                                                                                                                                                              | (none)        | Yes      | 0.5.0            |
| `jdbc-driver`           | The driver of the JDBC connection. For example, `com.mysql.jdbc.Driver`.                                                                                                                                                                                                                                                                                                                                         | (none)        | Yes      | 0.5.0            |
| `jdbc-user`             | The JDBC user name.                                                                                                                                                                                                                                                                                                                                                                                              | (none)        | Yes      | 0.5.0            |
| `jdbc-password`         | The JDBC password.                                                                                                                                                                                                                                                                                                                                                                                               | (none)        | Yes      | 0.5.0            |
| `jdbc.pool.min-size`    | The minimum number of connections in the pool. `2` by default.                                                                                                                                                                                                                                                                                                                                                   | `2`           | No       | 0.5.0            |
| `jdbc.pool.max-size`    | The maximum number of connections in the pool. `10` by default.                                                                                                                                                                                                                                                                                                                                                  | `10`          | No       | 0.5.0            |
| `replication_num`       | The number of replications for the table. If not specified and the number of backend servers less than 3, then the default value is 1; If not specified and the number of backend servers greater or equals to 3, the default value (3) in Doris server will be used. For more, see the [doc](https://doris.apache.org/docs/1.2/sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE/) | `1` or `3`    | No       | 0.6.0-incubating |
| `jdbc.pool.max-wait-ms` | The maximum Duration that the pool will wait for a connection to be returned. `30000` by default.                                                                                                                                                                                                                                                                                                                | `30000`       | No       | 1.1.0            |

Before using the Doris Catalog, you must download the corresponding JDBC driver to the `catalogs/jdbc-doris/libs` directory.
Gravitino doesn't package the JDBC driver for Doris due to licensing issues.

### Driver Version Compatibility

Datetime precision calculation for `DATETIME(p)` columns depends on the MySQL Connector/J driver version:

- **MySQL Connector/J 8.0.16 and later:** Full support. Precision is read from the driver and round-trips correctly through Gravitino.
- **MySQL Connector/J earlier than 8.0.16:** Limited support. Gravitino logs a warning and returns `null` for the precision value rather than risk reporting an incorrect one. All other catalog operations continue to work; only the reported precision for the `DATETIME(p)` type above is affected.

Use `mysql-connector-java-8.0.16` or higher to avoid the limitation.

Example warning log:

```
WARN: MySQL driver version mysql-connector-java-8.0.11 is below 8.0.16,
columnSize may not be accurate for precision calculation.
Returning null for DATETIME type precision. Driver version: mysql-connector-java-8.0.11
```

### Catalog Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema Capabilities

A Gravitino schema corresponds to a Doris database. The Doris catalog supports creating and dropping schemas.

### Schema Properties

- Support schema properties, including Doris database properties and user-defined properties.

### Schema Operations

Refer to
[Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more details.

## Table

### Table Capabilities

- A Gravitino table corresponds to a Doris table in the configured database.
- Supports DDL operations on Doris tables.
- Supports table indexes.
- Supports [column default values](./manage-relational-metadata-using-gravitino.md#table-column-default-value).
- Does not support auto-increment.

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

For the `RANDOM` distribution strategy, Gravitino uses the `EVEN` to represent it. More information about the distribution strategy defined in Gravitino can be found [here](./table-partitioning-distribution-sort-order-indexes.md#distribution).


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
