---
title: "Doris catalog"
slug: /jdbc-doris-catalog
keywords:
- jdbc
- Doris
- metadata
license: "Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

Gravitino provides the ability to manage Doris metadata.

:::caution
Gravitino saves some system information in schema and table comment, like `(From Gravitino, DO NOT EDIT: gravitino.v1.uid1078334182909406185)`, please don't change or remove this message.
:::

## Catalog

### Catalog capabilities

- Gravitino catalog corresponds to the Doris instance.
- Supports metadata management of Doris (1.2.x).
- Supports table index.
- Supports [column default value](./manage-relational-metadata-using-gravitino.md#table-column-default-value).

:::note
Doris 2.x supports more advanced metadata features such as auto-increment column. We will be adding support for it in the future.
:::

### Catalog properties

You can pass to a Doris data source any property that isn't defined by Gravitino by adding `gravitino.bypass` prefix as a catalog property. For example, catalog property `gravitino.bypass.maxWaitMillis` will pass `maxWaitMillis` to the data source property.

Check the relevant data source configuration in [data source properties](https://commons.apache.org/proper/commons-dbcp/configuration.html)

If you use a Doris catalog, you must provide `jdbc-url`, `jdbc-driver`, `jdbc-user` and `jdbc-password` to catalog properties.

| Configuration item   | Description                                                                         | Default value | Required | Since Version |
|----------------------|-------------------------------------------------------------------------------------|---------------|----------|---------------|
| `jdbc-url`           | JDBC URL for connecting to the database. For example, `jdbc:mysql://localhost:9030` | (none)        | Yes      | 0.5.0         |
| `jdbc-driver`        | The driver of the JDBC connection. For example, `com.mysql.jdbc.Driver`.            | (none)        | Yes      | 0.5.0         |
| `jdbc-user`          | The JDBC user name.                                                                 | (none)        | Yes      | 0.5.0         |
| `jdbc-password`      | The JDBC password.                                                                  | (none)        | Yes      | 0.5.0         |
| `jdbc.pool.min-size` | The minimum number of connections in the pool. `2` by default.                      | `2`           | No       | 0.5.0         |
| `jdbc.pool.max-size` | The maximum number of connections in the pool. `10` by default.                     | `10`          | No       | 0.5.0         |

:::caution
You must download the corresponding JDBC driver to the `catalogs/jdbc-doris/libs` directory.
:::

### Catalog operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema capabilities

- Gravitino's schema concept corresponds to the Doris database.
- Supports creating schema.
- Supports dropping schema.

### Schema properties

- Support schema properties

### Schema operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more details.

## Table

### Table capabilities

- Gravitino's table concept corresponds to the Doris table.
- Supports index.
- Supports [column default value](./manage-relational-metadata-using-gravitino.md#table-column-default-value) and [auto-increment](./manage-relational-metadata-using-gravitino.md#table-column-auto-increment)..

#### Table column types

| Gravitino Type | Doris Type |
|----------------|------------|
| `Boolean`      | `Boolean`  |
| `Byte`         | `TinyInt`  |
| `Short`        | `SmallInt` |
| `Integer`      | `Int`      |
| `Long`         | `BigInt`   |
| `Float`        | `Float`    |
| `Double`       | `Double`   |
| `Decimal`      | `Decimal`  |
| `Date`         | `Date`     |
| `Timestamp`    | `Datetime` |
| `VarChar`      | `VarChar`  |
| `FixedChar`    | `Char`     |
| `String`       | `String`   |

:::info
MySQL doesn't support Gravitino `Fixed` `Struct` `List` `Map` `Timestamp_tz` `IntervalDay` `IntervalYear` `Union` `UUID` type.
Meanwhile, the data types other than listed above are mapped to Gravitino **[Unparsed Type](./manage-relational-metadata-using-gravitino.md#unparsed-type)** that represents an unresolvable data type since 0.5.0.
:::

#### Table column auto-increment

:::note
Gravitino does not support the creation of an auto-increment column for a Doris table now.
:::


### Table properties

Doris supports table properties, and you can set them in the table properties.

### Table indexes

- Supports PRIMARY_KEY

:::note
Index can only apply to a single column
:::

<Tabs>
<TabItem value="json" label="Json">

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
    Indexes.of(IndexType.PRIMARY_KEY, "PRIMARY", new String[][]{{"id"}})
}
```

</TabItem>
</Tabs>

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
- `UpdateColumnComment`
- `SetProperty`

:::info
 - Not all table alteration operations can be processed in batch. 
 - Schema change, such as add/modify/drop columns can be processed in batch
 - Can modify multi column comments at same time
 - You can't modify the column type and column comment at the same time
 - The schema alteration in Doris is an asynchronous operation. You might retrieve an outdated schema if you execute a schema query immediately following the alteration. It is recommended to pause briefly following the schema alteration, and Gravitino will incorporate the schema alteration status into the schema information in the upcoming version.
:::