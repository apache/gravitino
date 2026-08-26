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

Apache Gravitino provides the ability to manage OceanBase metadata.

:::caution
1. Gravitino saves some system information in schema and table comment, like `(From Gravitino, DO NOT EDIT: gravitino.v1.uid1078334182909406185)`, do not change or remove this message.
2. OceanBase catalog is not included in standard Gravitino distribution, but you can still build it from source if you need it. Check [build from source](./how-to-build.md) for more details.
:::

## Catalog

### Catalog Capabilities

- Gravitino catalog corresponds to the OceanBase instance.
- Supports metadata management of OceanBase (4.x).
- Supports DDL operation for OceanBase databases and tables.
- Supports table index.
- Supports [column default value](./tables-and-views.md#table-column-default-value) and [auto-increment](./tables-and-views.md#table-column-auto-increment).

### Catalog Properties

Pass to a OceanBase data source any property that isn't defined by Gravitino by adding `gravitino.bypass.` prefix as a catalog property. For example, catalog property `gravitino.bypass.maxWaitMillis` will pass `maxWaitMillis` to the data source property.

Check the relevant data source configuration in [data source properties](https://commons.apache.org/proper/commons-dbcp/configuration.html)

If you use a JDBC catalog, you must provide `jdbc-url`, `jdbc-driver`, `jdbc-user` and `jdbc-password` to catalog properties.
Besides the [common catalog properties](./gravitino-server-config.md#catalog-properties-configuration), the OceanBase catalog has the following properties:

| Configuration item      | Description                                                                                                                           | Default value | Required |
|-------------------------|---------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|
| `jdbc-url`              | JDBC URL for connecting to the database. For example, `jdbc:mysql://localhost:2881` or `jdbc:oceanbase://localhost:2881`              | (none)        | Yes      |
| `jdbc-driver`           | The driver of the JDBC connection. For example, `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver` or `com.oceanbase.jdbc.Driver`. | (none)        | Yes      |
| `jdbc-user`             | The JDBC user name.                                                                                                                   | (none)        | Yes      |
| `jdbc-password`         | The JDBC password.                                                                                                                    | (none)        | Yes      |
| `jdbc.pool.min-size`    | The minimum number of connections in the pool. `2` by default.                                                                        | `2`           | No       |
| `jdbc.pool.max-size`    | The maximum number of connections in the pool. `10` by default.                                                                       | `10`          | No       |
| `jdbc.pool.max-wait-ms` | The maximum Duration that the pool will wait for a connection to be returned. `30000` by default.                                     | `30000`       | No       |

:::caution
Before using the OceanBase Catalog, you must download the corresponding JDBC driver to the `catalogs/jdbc-oceanbase/libs` directory.
Gravitino doesn't package the JDBC driver for OceanBase due to licensing issues.
:::

### Driver Version Compatibility

The OceanBase catalog includes driver version compatibility checks for datetime precision calculation:

- **MySQL Connector/J versions >= 8.0.16**: Full support for datetime precision calculation
- **MySQL Connector/J versions < 8.0.16**: Limited support - datetime precision calculation returns `null` with a warning log

This limitation affects the following datetime types:
- `TIME(p)` - time precision
- `TIMESTAMP(p)` - timestamp precision  
- `DATETIME(p)` - datetime precision

When using an unsupported driver version, the system will:
1. Continue to work normally with default precision (0)
2. Log a warning message indicating the driver version limitation
3. Return `null` for precision calculations to avoid incorrect results

**Example warning log:**
```
WARN: MySQL driver version mysql-connector-java-8.0.11 is below 8.0.16, 
columnSize may not be accurate for precision calculation. 
Returning null for TIMESTAMP type precision. Driver version: mysql-connector-java-8.0.11
```

**Recommended driver versions:**
- `mysql-connector-java-8.0.16` or higher
- `com.oceanbase.jdbc.Driver` (OceanBase official driver)

### Catalog Operations

Refer to [Manage Catalogs and Schemas](./manage-catalogs-and-schemas.md#catalog-operations) for more details.

:::note
Sensitive catalog properties such as `jdbc-user` and `jdbc-password` are hidden from the default load catalog response. Retrieve secret-manager-backed properties (including `jdbc-password` when stored as a secret URN) via `getSecrets` / `GET .../objects/{type}/{fullName}/secrets`. The [credential vending API](security/credential-vending.md) (`getCredentials` / `JdbcCredential`) remains available for typed credential delivery.
:::

## Schema

### Schema Capabilities

- Gravitino's schema concept corresponds to the OceanBase database.
- Supports creating schema, but does not support setting comment.
- Supports dropping schema.
- Supports cascade dropping schema.

### Schema Properties

- Doesn't support any schema property settings.

### Schema Operations

Refer to [Manage Catalogs and Schemas](./manage-catalogs-and-schemas.md#schema-operations) for more details.

## Table

### Table Capabilities

- Gravitino's table concept corresponds to the OceanBase table.
- Supports DDL operation for OceanBase tables.
- Supports index.
- Supports [column default value](./tables-and-views.md#table-column-default-value) and [auto-increment](./tables-and-views.md#table-column-auto-increment)..

### Table Properties

- Doesn't support table properties.

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
Meanwhile, the data types other than listed above are mapped to Gravitino **[External Type](./tables-and-views.md#external-type)** that represents an unresolvable data type.
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
