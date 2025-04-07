---
title: Apache Doris catalog
slug: /jdbc-doris-catalog
keywords:
- jdbc
- Apache Doris
- metadata
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

Apache Gravitino can be used to manage [Apache Doris](https://doris.apache.org/)
metadata through JDBC connection.

:::caution
Gravitino saves some system information in schema and table comments,
like `(From Gravitino, DO NOT EDIT: gravitino.v1.uid1078334182909406185)`.
**Please don't change or remove this message.**
:::

## Catalog

### Catalog capabilities

- Gravitino catalog corresponds to the Doris instance.
- This can supports managing metadata for Doris (1.2.x).
- Indexed tables are supported
- [Column default value](../../../metadata/relational.md#table-column-default-value)
  are supported.

### Catalog properties

You can pass to a Doris data source any property that isn't defined by Gravitino
by adding the `gravitino.bypass.` prefix as a catalog property.
For example, catalog property `gravitino.bypass.maxWaitMillis` will pass `maxWaitMillis`
to the data source property.

For more details, you can check the relevant data source configuration in
[data source properties](https://commons.apache.org/proper/commons-dbcp/configuration.html).

Besides the [common catalog properties](../../../admin/server-config.md#gravitino-catalog-properties-configuration),
the Doris catalog has the following properties:

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>jdbc-url</tt></td>
  <td>
    The JDBC URL to use when connecting to the database.
    For example, `jdbc:mysql://localhost:9030`.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.5.0`</td>
</tr>
<tr>
  <td><tt>jdbc-driver</tt></td>
  <td>
    The driver of the JDBC connection. For example, `com.mysql.jdbc.Driver`.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.5.0`</td>
</tr>
<tr>
  <td><tt>jdbc-user</tt></td>
  <td>The JDBC user name.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.5.0`</td>
</tr>
<tr>
  <td><tt>jdbc-password</tt></td>
  <td>The JDBC password.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.5.0`</td>
</tr>
<tr>
  <td><tt>jdbc.pool.min-size</tt></td>
  <td>The minimum number of connections in the pool.</td>
  <td>`2`</td>
  <td>No</td>
  <td>`0.5.0`</td>
</tr>
<tr>
  <td><tt>jdbc.pool.max-size</tt></td>
  <td>The maximum number of connections in the pool.</td>
  <td>`10`</td>
  <td>No</td>
  <td>`0.5.0`</td>
</tr>
<tr>
  <td><tt>jdbc.pool.max-size</tt></td>
  <td>The maximum number of connections in the pool.</td>
  <td>`10`</td>
  <td>No</td>
  <td>`0.5.0`</td>
</tr>
<tr>
  <td><tt>replication_num</tt></td>
  <td>
    The number of replications for the table.

    - If not specified and the number of backend servers less than 3, the default value is 1;
    - If not specified and the number of backend servers greater or equals to 3,
      the default value (`3`) in Doris server will be used.

    For more details, check the
    [Doris reference](https://doris.apache.org/docs/1.2/sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE/).
  </td>
  <td>`1` or `3`</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
</tbody>
</table>

Before using the Doris Catalog, you must download the corresponding JDBC driver
to the `catalogs/jdbc-doris/libs` directory.
Gravitino doesn't package the JDBC driver for Doris due to licensing issues.

### Catalog operations

Refer to [managing relational metadata](../../../metadata/relational.md#catalog-operations).

## Schema

### Schema capabilities

- Gravitino's schema concept corresponds to the Doris database.
- Creating schema is supported.
- Dropping schema is supported.

### Schema properties

- This catalog supports schema properties, including Doris database properties and user-defined properties.

### Schema operations

Refer to [managing relational metadata](../../../metadata/relational.md#schema-operations).

## Table

### Table capabilities

- Gravitino's table concept corresponds to the Doris table.
- Indexed tables are supported.
- [Column default value](../../../metadata/relational.md#table-column-default-value)
  is supported.

#### Table column types

| Gravitino Type | Doris Type |
|----------------|------------|
| `Boolean`      | `Boolean`  |
| `Byte`         | `TinyInt`  |
| `Date`         | `Date`     |
| `Decimal`      | `Decimal`  |
| `Double`       | `Double`   |
| `FixedChar`    | `Char`     |
| `Float`        | `Float`    |
| `Integer`      | `Int`      |
| `Long`         | `BigInt`   |
| `Short`        | `SmallInt` |
| `String`       | `String`   |
| `Timestamp`    | `Datetime` |
| `VarChar`      | `VarChar`  |

Doris doesn't support Gravitino `Fixed`, `Timestamp_tz`, `IntervalDay`, `IntervalYear`, `Union`, or `UUID` type.
The data types other than those listed above are mapped to Gravitino's
**[Unparsed Type](../../../metadata/relational.md#unparsed-type)**
that represents an unresolvable data type since 0.5.0.

:::note
Gravitino can not load Doris `array`, `map` and `struct` type correctly,
because Doris doesn't support these types in JDBC.
:::

### Table column auto-increment

Auto-increment columns are not supported for now.

### Table properties

- Doris supports table properties, and you can set them in the table properties.
- Only supports Doris table properties and doesn't support user-defined properties.

### Table indexes

PRIMARY_KEY is supported

Please be aware that the index can only apply to a single column.

<Tabs groupId='language' queryString>
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

### Table partitioning

The Doris catalog supports partitioned tables. 
Users can create partitioned tables in the Doris catalog with specific partitioning attributes.
It is also supported to pre-assign partitions when creating Doris tables. 
Note that although Gravitino supports several partitioning strategies,
Apache Doris inherently only supports these two partitioning strategies:

- `RANGE`
- `LIST`

:::caution
The `fieldName` specified in the partitioning attributes must be the name of columns defined in the table.
:::

### Table distribution

Users can also specify the distribution strategy when creating tables in the Doris catalog.
Currently, the Doris catalog supports the following distribution strategies:

- `HASH`
- `RANDOM`

For the `RANDOM` distribution strategy, Gravitino uses the `EVEN` to represent it.
More information about the distribution strategy can be found [here](../distributed-table.md#distribution-strategies).

### Table operations

Please refer to [managing relational metadata](../../../metadata/relational.md#table-operations).

#### Alter table operations

Gravitino supports the following table alteration operations:

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

- This catalog supports modifying multiple column comments at the same time.

- This catalog doesn't support modifying the column type and column comment at the same time.

- The schema alteration in Doris is asynchronous.
  You might get an outdated schema if you execute a schema query immediately after the alteration.
  It is recommended to pause briefly after the schema alteration.
  Gravitino will add the schema alteration status into
  the schema information in the upcoming version to solve this problem.

