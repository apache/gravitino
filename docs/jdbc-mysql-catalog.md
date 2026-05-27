---
title: "MySQL Catalog"
slug: "/jdbc-mysql-catalog"
keywords:
- jdbc
- MySQL
- metadata
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

The MySQL catalog enables Apache Gravitino to manage MySQL metadata, including databases (mapped to Gravitino schemas), tables, columns, indexes, column-level defaults, and a small set of MySQL-specific table properties such as `engine`. Use it when you want a single Gravitino-managed access surface over a MySQL instance, with the option to federate it alongside other relational, lakehouse, and fileset catalogs.

:::caution
Gravitino saves some system information in schema and table comments, such as `(From Gravitino, DO NOT EDIT: gravitino.v1.uid1078334182909406185)`. Do not edit or remove this message.
:::

### Requirements and Limitations

- **Supported MySQL versions:** 5.7 and 8.0.
- **JDBC driver required.** Place the MySQL Connector/J driver in `catalogs/jdbc-mysql/libs` on the Gravitino server. Gravitino does not bundle the driver. Use `mysql-connector-java-8.0.16` or higher to get accurate datetime precision values; see [Driver Version Compatibility](#driver-version-compatibility) below.
- **One MySQL instance per catalog.** A Gravitino MySQL catalog corresponds to one MySQL server instance. A Gravitino schema corresponds to a MySQL database on that instance.
- **Schema comments not supported.** MySQL databases do not carry comments, so creating a schema with a comment is not supported.
- **Table properties are add-or-modify only.** Once set, MySQL table properties managed through Gravitino cannot be removed; they can only be added or modified.

## Quick Start

Create a minimum-viable MySQL catalog and confirm it is reachable. The example assumes a Gravitino server at `http://localhost:8090`, a metalake named `test`, and a MySQL instance at `localhost:3306`. Adjust the values for your environment.

### Create the Catalog

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mysql_catalog",
    "type": "RELATIONAL",
    "comment": "MySQL catalog",
    "provider": "jdbc-mysql",
    "properties": {
      "jdbc-url": "jdbc:mysql://localhost:3306",
      "jdbc-driver": "com.mysql.cj.jdbc.Driver",
      "jdbc-user": "<your-user>",
      "jdbc-password": "<your-password>"
    }
  }' \
  http://localhost:8090/api/metalakes/test/catalogs
```

The response is a JSON object describing the created catalog.

### Verify the Catalog

```bash
# List catalogs in the metalake. mysql_catalog should appear.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs" | jq

# Load the catalog directly and inspect its properties.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/mysql_catalog" | jq

# List schemas. The response should include the databases on the MySQL instance, typically `mysql` and `information_schema` at minimum.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/mysql_catalog/schemas" | jq
```

**Success check:** the catalog-list response includes `mysql_catalog`, the load-catalog response shows `"provider":"jdbc-mysql"`, and the schema-list response includes at least the `mysql` system database. If the schema-list call returns an authentication or connection error, verify the `jdbc-url`, `jdbc-user`, and `jdbc-password` values, and confirm the MySQL Connector/J driver is present in `catalogs/jdbc-mysql/libs` on the Gravitino server.

## Catalog

### Catalog Capabilities

A Gravitino MySQL catalog corresponds to a MySQL instance and provides:

- DDL operations on MySQL databases and tables.
- Table indexes.
- [Column default values](./manage-relational-metadata-using-gravitino.md#table-column-default-value) and [auto-increment](./manage-relational-metadata-using-gravitino.md#table-column-auto-increment).
- Management of MySQL table features through table properties (for example, `engine` to set the storage engine).

### Catalog Properties

Pass to a MySQL data source any property that isn't defined by Gravitino by adding `gravitino.bypass.` prefix as a catalog property. For example, catalog property `gravitino.bypass.maxWaitMillis` will pass `maxWaitMillis` to the data source property.

Check the relevant data source configuration in [data source properties](https://commons.apache.org/proper/commons-dbcp/configuration.html)

When you use Gravitino with Trino, pass Trino MySQL connector configuration through the `trino.bypass.` prefix. For example, set `trino.bypass.join-pushdown.strategy` to forward `join-pushdown.strategy` to the Gravitino MySQL catalog at Trino runtime.

If you use a JDBC catalog, you must provide `jdbc-url`, `jdbc-driver`, `jdbc-user` and `jdbc-password` to catalog properties.
Besides the [common catalog properties](./gravitino-server-config.md#catalog-properties), the MySQL catalog has the following properties:

| Property      | Description                                                                                            | Default | Required | Since |
|-------------------------|--------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `jdbc-url`              | JDBC URL for connecting to the database. For example, `jdbc:mysql://localhost:3306`                    | (none)        | Yes      | 0.3.0         |
| `jdbc-driver`           | The driver of the JDBC connection. For example, `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver`. | (none)        | Yes      | 0.3.0         |
| `jdbc-user`             | The JDBC user name.                                                                                    | (none)        | Yes      | 0.3.0         |
| `jdbc-password`         | The JDBC password.                                                                                     | (none)        | Yes      | 0.3.0         |
| `jdbc.pool.min-size`    | The minimum number of connections in the pool. `2` by default.                                         | `2`           | No       | 0.3.0         |
| `jdbc.pool.max-size`    | The maximum number of connections in the pool. `10` by default.                                        | `10`          | No       | 0.3.0         |
| `jdbc.pool.max-wait-ms` | The maximum Duration that the pool will wait for a connection to be returned. `30000` by default.      | `30000`       | No       | 1.1.0         |

:::caution
Download the corresponding JDBC driver to the `catalogs/jdbc-mysql/libs` directory.
:::

### Driver Version Compatibility

Datetime precision calculation for `TIME(p)`, `TIMESTAMP(p)`, and `DATETIME(p)` columns depends on the MySQL Connector/J driver version:

- **MySQL Connector/J 8.0.16 and later:** Full support. Precision is read from the driver and round-trips correctly through Gravitino.
- **MySQL Connector/J earlier than 8.0.16:** Limited support. Gravitino logs a warning and returns `null` for the precision value rather than risk reporting an incorrect one. All other catalog operations continue to work; only the reported precision for the three datetime types above is affected.

Use `mysql-connector-java-8.0.16` or higher to avoid the limitation.

Example warning log:

```
WARN: MySQL driver version mysql-connector-java-8.0.11 is below 8.0.16,
columnSize may not be accurate for precision calculation.
Returning null for TIMESTAMP type precision. Driver version: mysql-connector-java-8.0.11
```

### Catalog Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema Capabilities

A Gravitino schema corresponds to a MySQL database. The MySQL catalog supports creating, dropping, and cascade-dropping schemas, but does not support setting a schema comment.

### Schema Properties

The MySQL catalog does not support any schema-level properties.

### Schema Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more details.

## Table

### Table Capabilities

A Gravitino table corresponds to a MySQL table and supports:

- DDL operations on MySQL tables.
- Indexes.
- [Column default values](./manage-relational-metadata-using-gravitino.md#table-column-default-value) and [auto-increment](./manage-relational-metadata-using-gravitino.md#table-column-auto-increment).
- Management of MySQL table features through table properties (for example, `engine` to set the storage engine).

### Table Column Types

| Gravitino Type       | MySQL Type          |
|----------------------|---------------------|
| `Byte`               | `Tinyint`           |
| `Unsigned Byte`      | `Tinyint Unsigned`  |
| `Short`              | `Smallint`          |
| `Unsigned Short`     | `Smallint Unsigned` |
| `Integer`            | `Int`               |
| `Unsigned Integer`   | `Int Unsigned`      |
| `Long`               | `Bigint`            |
| `Unsigned Long`      | `Bigint Unsigned`   |
| `Float`              | `Float`             |
| `Double`             | `Double`            |
| `String`             | `Text`              |
| `Date`               | `Date`              |
| `Time[(p)]`          | `Time[(p)]`         |
| `Timestamp_tz[(p)]`  | `Timestamp(p)`      |
| `Timestamp[(p)]`     | `Datetime[(p)]`     |
| `Decimal`            | `Decimal`           |
| `VarChar`            | `VarChar`           |
| `FixedChar`          | `FixedChar`         |
| `Binary`             | `Binary`            |
| `BOOLEAN`            | `BIT`               |

:::info
MySQL doesn't support Gravitino `Fixed` `Struct` `List` `Map` `IntervalDay` `IntervalYear` `Union` `UUID` type.
Data types other than those listed above are mapped to the Gravitino **[External Type](./manage-relational-metadata-using-gravitino.md#external-type)**, which represents an unresolvable data type. Available since 0.6.0.
:::

### Table Column Auto-Increment

:::note
MySQL setting an auto-increment column requires simultaneously setting a unique index; otherwise, an error will occur.
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

### Table Properties

Although MySQL itself does not support table properties, Gravitino offers table property management for MySQL tables through the `jdbc-mysql` catalog, enabling control over table features. The supported properties are listed as follows:

:::note
**Reserved**: Fields that cannot be passed to the Gravitino server.

**Immutable**: Fields that cannot be modified once set.
:::

:::caution
Table properties on a Gravitino-managed MySQL table cannot be removed once set. You can add new properties or modify existing values, but not delete them.
:::

| Property           | Description                                                                                                                                              | Default | Required  | Reserved   | Immutable | Since |
|-------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|-----------|------------|-----------|---------------|
| `engine`                | The engine used by the table. For example `MyISAM`, `MEMORY`, `CSV`, `ARCHIVE`, `BLACKHOLE`, `FEDERATED`, `ndbinfo`, `MRG_MYISAM`, `PERFORMANCE_SCHEMA`. | `InnoDB`      | No        | No         | Yes       | 0.4.0         |
| `auto-increment-offset` | Used to specify the starting value of the auto-increment field.                                                                                          | (none)        | No        | No         | Yes       | 0.4.0         |


:::note
Some MySQL storage engines, such as FEDERATED, are not enabled by default and require additional configuration to use. For example, to enable the FEDERATED engine, set federated=1 in the MySQL configuration file. Similarly, engines like ndbinfo, MRG_MYISAM, and PERFORMANCE_SCHEMA may also require specific prerequisites or configurations. For detailed instructions, 
refer to the [MySQL documentation](https://dev.mysql.com/doc/refman/8.0/en/federated-storage-engine.html).
:::

### Table Indexes

- Supports PRIMARY_KEY and UNIQUE_KEY.

:::note
The index name of a `PRIMARY_KEY` must be `PRIMARY`. See [Create Table](https://dev.mysql.com/doc/refman/8.0/en/create-table.html) in the MySQL documentation for the underlying constraint.
:::

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
