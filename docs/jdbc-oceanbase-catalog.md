---
title: "OceanBase Catalog"
slug: "/jdbc-oceanbase-catalog"
keywords:
- jdbc
- OceanBase
- metadata
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

The OceanBase catalog enables Apache Gravitino to manage OceanBase metadata through a JDBC connection, including databases (mapped to Gravitino schemas), tables, columns, indexes, column-level defaults, and auto-increment columns. Use it when you want a single Gravitino-managed access surface that covers OceanBase alongside other relational, lakehouse, and fileset catalogs.

:::caution
Gravitino saves some system information in schema and table comment, like `(From Gravitino, DO NOT EDIT: gravitino.v1.uid1078334182909406185)`, do not change or remove this message.
:::

### Requirements and Limitations

- **OceanBase catalog is not bundled since 1.2.0.** The OceanBase catalog is not included in the standard Gravitino distribution. Build it from source if you need it; see [How to Build](./how-to-build.md).
- **Supported OceanBase versions:** 4.x.
- **JDBC driver required.** OceanBase accepts either the MySQL JDBC driver (`com.mysql.cj.jdbc.Driver` or the legacy `com.mysql.jdbc.Driver`) or the OceanBase native JDBC driver (`com.oceanbase.jdbc.Driver`). Place the driver in `catalogs/jdbc-oceanbase/libs` on the Gravitino server. Use `mysql-connector-java-8.0.16` or higher to get accurate datetime precision values; see [Driver Version Compatibility](#driver-version-compatibility) below.
- **One OceanBase instance per catalog.** A Gravitino schema maps to an OceanBase database on the instance.
- **No schema comments.** OceanBase databases do not carry comments, so creating a schema with a comment is not supported.
- **No schema properties.** OceanBase schemas do not accept Gravitino-managed schema properties.
- **No Gravitino-managed table properties.** OceanBase tables do not accept Gravitino-managed table properties; configure storage and tuning options directly in OceanBase.

## Quick Start

Create a minimum-viable OceanBase catalog and confirm it is reachable. The example assumes a Gravitino server at `http://localhost:8090`, a metalake named `test`, and an OceanBase instance at `localhost:2881`. Adjust the values for your environment.

### Create the Catalog

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "oceanbase_catalog",
    "type": "RELATIONAL",
    "comment": "OceanBase catalog",
    "provider": "jdbc-oceanbase",
    "properties": {
      "jdbc-url": "jdbc:mysql://localhost:2881",
      "jdbc-driver": "com.mysql.cj.jdbc.Driver",
      "jdbc-user": "<your-user>",
      "jdbc-password": "<your-password>"
    }
  }' \
  http://localhost:8090/api/metalakes/test/catalogs
```

### Verify the Catalog

```bash
# List catalogs in the metalake. oceanbase_catalog should appear.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs" | jq

# Load the catalog directly and inspect its properties.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/oceanbase_catalog" | jq

# List schemas (OceanBase databases). The response includes at least `oceanbase` and `information_schema`.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/oceanbase_catalog/schemas" | jq
```

**Success check:** the catalog-list response includes `oceanbase_catalog`, the load-catalog response shows `"provider":"jdbc-oceanbase"` with the configured `jdbc-url`, and the schema-list response includes at least the `oceanbase` system database. If the schema-list call returns a connection error, verify the `jdbc-url`, `jdbc-user`, and `jdbc-password` values, and confirm an accepted JDBC driver is present in `catalogs/jdbc-oceanbase/libs` on the Gravitino server.

## Catalog

### Catalog Capabilities

A Gravitino OceanBase catalog corresponds to an OceanBase instance (4.x) and provides:

- Metadata management.
- DDL operations on OceanBase databases and tables.
- Table indexes.
- [Column default values](./manage-relational-metadata-using-gravitino.md#table-column-default-value) and [auto-increment](./manage-relational-metadata-using-gravitino.md#table-column-auto-increment).

### Catalog Properties

Pass to a OceanBase data source any property that isn't defined by Gravitino by adding `gravitino.bypass.` prefix as a catalog property. For example, catalog property `gravitino.bypass.maxWaitMillis` will pass `maxWaitMillis` to the data source property.

Check the relevant data source configuration in [data source properties](https://commons.apache.org/proper/commons-dbcp/configuration.html)

If you use a JDBC catalog, you must provide `jdbc-url`, `jdbc-driver`, `jdbc-user` and `jdbc-password` to catalog properties.
Besides the [common catalog properties](./gravitino-server-config.md#catalog-properties), the OceanBase catalog has the following properties:

| Property      | Description                                                                                                                           | Default | Required | Since    |
|-------------------------|---------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `jdbc-url`              | JDBC URL for connecting to the database. For example, `jdbc:mysql://localhost:2881` or `jdbc:oceanbase://localhost:2881`              | (none)        | Yes      | 0.7.0-incubating |
| `jdbc-driver`           | The driver of the JDBC connection. For example, `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver` or `com.oceanbase.jdbc.Driver`. | (none)        | Yes      | 0.7.0-incubating |
| `jdbc-user`             | The JDBC user name.                                                                                                                   | (none)        | Yes      | 0.7.0-incubating |
| `jdbc-password`         | The JDBC password.                                                                                                                    | (none)        | Yes      | 0.7.0-incubating |
| `jdbc.pool.min-size`    | The minimum number of connections in the pool. `2` by default.                                                                        | `2`           | No       | 0.7.0-incubating |
| `jdbc.pool.max-size`    | The maximum number of connections in the pool. `10` by default.                                                                       | `10`          | No       | 0.7.0-incubating |
| `jdbc.pool.max-wait-ms` | The maximum Duration that the pool will wait for a connection to be returned. `30000` by default.                                     | `30000`       | No       | 1.1.0            |

:::caution
Before using the OceanBase Catalog, you must download the corresponding JDBC driver to the `catalogs/jdbc-oceanbase/libs` directory.
Gravitino doesn't package the JDBC driver for OceanBase due to licensing issues.
:::

### Driver Version Compatibility

Datetime precision calculation for `TIME(p)`, `TIMESTAMP(p)`, and `DATETIME(p)` columns depends on the MySQL Connector/J driver version when the MySQL JDBC driver is in use:

- **MySQL Connector/J 8.0.16 and later:** Full support. Precision is read from the driver and round-trips correctly through Gravitino.
- **MySQL Connector/J earlier than 8.0.16:** Limited support. Gravitino logs a warning and returns `null` for the precision value rather than risk reporting an incorrect one. All other catalog operations continue to work; only the reported precision for the three datetime types above is affected.

Use `mysql-connector-java-8.0.16` or higher to avoid the limitation.

### Catalog Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema Capabilities

A Gravitino schema corresponds to an OceanBase database. The OceanBase catalog supports creating, dropping, and cascade-dropping schemas, but does not support setting a schema comment.

### Schema Properties

The OceanBase catalog does not accept Gravitino-managed schema properties.

### Schema Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more details.

## Table

### Table Capabilities

A Gravitino table corresponds to an OceanBase table and supports:

- DDL operations on OceanBase tables.
- Indexes.
- [Column default values](./manage-relational-metadata-using-gravitino.md#table-column-default-value) and [auto-increment](./manage-relational-metadata-using-gravitino.md#table-column-auto-increment).

### Table Properties

The OceanBase catalog does not accept Gravitino-managed table properties. Configure storage and tuning options directly in OceanBase using standard OceanBase DDL.

### Table Column Types

| Gravitino Type      | OceanBase Type      |
|---------------------|---------------------|
| `Byte`              | `Tinyint`           |
| `Byte(false)`       | `Tinyint Unsigned`  |
| `Short`             | `Smallint`          |
| `Short(false)`      | `Smallint Unsigned` |
| `Integer`           | `Int`               |
| `Integer(false)`    | `Int Unsigned`      |
| `Long`              | `Bigint`            |
| `Long(false)`       | `Bigint Unsigned`   | 
| `Float`             | `Float`             |
| `Double`            | `Double`            |
| `String`            | `Text`              |
| `Date`              | `Date`              |
| `Time[(p)]`         | `Time[(p)]`         |
| `Timestamp_tz[(p)]` | `Timestamp[(p)]`    |
| `Timestamp[(p)]`    | `Datetime[(p)]`     |
| `Decimal`           | `Decimal`           |
| `VarChar`           | `VarChar`           |
| `FixedChar`         | `FixedChar`         |
| `Binary`            | `Binary`            |

:::info
OceanBase doesn't support Gravitino `Boolean` `Fixed` `Struct` `List` `Map` `IntervalDay` `IntervalYear` `Union` `UUID` type.
Meanwhile, the data types other than listed above are mapped to Gravitino **[External Type](./manage-relational-metadata-using-gravitino.md#external-type)** that represents an unresolvable data type since 0.6.0-incubating.
:::

### Table Column Auto-Increment

:::note
OceanBase setting an auto-increment column requires simultaneously setting a unique index; otherwise, an error will occur.
:::

<Tabs groupId='language' queryString>
<TabItem value="json" label="JSON">

```json
{
  "columns": [
    {
      "name": "id",
      "type": "integer",
      "comment": "id column comment",
      "nullable": false,
      "autoIncrement": true
    },
    {
      "name": "name",
      "type": "varchar(500)",
      "comment": "name column comment",
      "nullable": true,
      "autoIncrement": false
    }
  ],
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
Column[] cols = new Column[] {
    Column.of("id", Types.IntegerType.get(), "id column comment", false, true, null),
    Column.of("name", Types.VarCharType.of(500), "Name of the user", true, false, null)
};
Index[] indexes = new Index[] {
    Indexes.of(IndexType.PRIMARY_KEY, "PRIMARY", new String[][]{{"id"}}, Map.of())
};
```

</TabItem>
</Tabs>


### Table Indexes

- Supports PRIMARY_KEY and UNIQUE_KEY.

<Tabs groupId='language' queryString>
<TabItem value="json" label="JSON">

```json
{
  "indexes": [
    {
      "indexType": "primary_key",
      "name": "PRIMARY",
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
};
```

</TabItem>
</Tabs>

### Table Operations

:::note
The OceanBase catalog does not support creating partitioned tables in the current version.
:::

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for more details.

#### Alter Table Operations

Gravitino supports these table alteration operations:

- `RenameTable`
- `UpdateComment`
- `AddColumn`
- `DeleteColumn`
- `RenameColumn`
- `UpdateColumnType`
- `UpdateColumnPosition`
- `UpdateColumnNullability`
- `UpdateColumnComment`
- `UpdateColumnDefaultValue`
- `SetProperty`

:::info
 - You cannot submit the `RenameTable` operation at the same time as other operations.
 - If you update a nullability column to non-nullability, there may be compatibility issues.
:::
