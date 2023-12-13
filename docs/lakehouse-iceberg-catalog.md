---
title: "Lakehouse Iceberg catalog"
slug: /lakehouse-iceberg-catalog
keywords:
  - lakehouse
  - iceberg
  - metadata
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

## Introduction

Gravitino provides the ability to manage Apache Iceberg metadata.

### Requirements and limitations

:::info
Builds with Apache Iceberg `1.3.1`. The Apache Iceberg table format version is `1` by default.
Builds with Hadoop 2.10.x, there may compatibility issue when accessing Hadoop 3.x clusters.
:::

## Catalog

### Catalog capabilities

- Works as a catalog proxy, supporting `HiveCatalog` and `JdbcCatalog`.
- Supports DDL operations for Iceberg schemas and tables.
- Doesn't support snapshot or table management operations.

### Catalog properties

| Property name     | Description                                                                                                                                                          | Default value | Required | Since Version |
|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `catalog-backend` | Catalog backend of Gravitino Iceberg catalog. Supports `hive` or `jdbc`.                                                                                             | (none)        | Yes      | 0.2.0         |
| `uri`             | The URI configuration of the Iceberg catalog. `thrift://127.0.0.1:9083` or `jdbc:postgresql://127.0.0.1:5432/db_name` or `jdbc:mysql://127.0.0.1:3306/metastore_db`. | (none)        | Yes      | 0.2.0         |
| `warehouse`       | Warehouse directory of catalog. `file:///user/hive/warehouse-hive/` for local fs or `hdfs://namespace/hdfs/path` for HDFS.                                           | (none)        | Yes      | 0.2.0         |

Any properties not defined by Gravitino with `gravitino.bypass` prefix will pass to Iceberg catalog properties and HDFS configuration. For example, if specify `gravitino.bypass.list-all-tables`, `list-all-tables` will pass to Iceberg catalog properties.

#### JDBC catalog

If you are using JDBC catalog, you must provide `jdbc-user`, `jdbc-password` and `jdbc-driver` to catalog properties.

| Property name     | Description                                                                                             | Default value | Required | Since Version |
|-------------------|---------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `jdbc-user`       | JDBC user name                                                                                          | (none)        | Yes      | 0.2.0         |
| `jdbc-password`   | JDBC password                                                                                           | (none)        | Yes      | 0.2.0         |
| `jdbc-driver`     | `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver` for MySQL, `org.postgresql.Driver` for PostgreSQL | (none)        | Yes      | 0.3.0         |
| `jdbc-initialize` | Whether to initialize meta tables when create JDBC catalog                                              | `true`        | No       | 0.2.0         |

:::caution
You must download the corresponding JDBC driver to the `catalogs/lakehouse-iceberg/libs` directory.
:::

### Catalog operations

Please refer to [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino.md#catalogs-operations) for more details.

## Schema 

### Schema capabilities

- doesn't support cascade drop schema.

### Schema properties

You could put properties except `comment`.

### Schema operations

Please refer to [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino.md#schemas-operations) for more details.

## Table 

### Table capabilities

#### Table partitions

Supports transforms:

  - `IdentityTransform`
  - `BucketTransform`
  - `TruncateTransform`
  - `YearTransform`
  - `MonthTransform`
  - `DayTransform`
  - `HourTransform`

:::info
Iceberg doesn't support multi fields in `BucketTransform`.
Iceberg doesn't support `ApplyTransform`, `RangeTransform` and `ListTransform`.
:::

### Table sort orders

supports expressions:

- `FieldReference`
- `FunctionExpression`
  - `bucket`
  - `truncate`
  - `year`
  - `month`
  - `day`
  - `hour`

:::info
For `bucket` and `truncate`, the first argument must be integer literal, the second argument must be field reference.
:::

### Table distributions

- Doesn't support `Distribution`, you should use `BucketPartition` instead.

:::info
If you load Iceberg tables, the table distribution strategy is `hash` with num 0, which means no distribution.
:::

### Table column types

| Gravitino Type              | Apache Iceberg Type         |
|-----------------------------|-----------------------------|
| `Struct`                    | `Struct`                    |
| `Map`                       | `Map`                       |
| `Array`                     | `Array`                     |
| `Boolean`                   | `Boolean`                   |
| `Integer`                   | `Integer`                   |
| `Long`                      | `Long`                      |
| `Float`                     | `Float`                     |
| `Double`                    | `Double`                    |
| `String`                    | `String`                    |
| `Date`                      | `Date`                      |
| `Time`                      | `Time`                      |
| `TimestampType withZone`    | `TimestampType withZone`    |
| `TimestampType withoutZone` | `TimestampType withoutZone` |
| `Decimal`                   | `Decimal`                   |
| `Fixed`                     | `Fixed`                     |
| `BinaryType`                | `Binary`                    |
| `UUID`                      | `UUID`                      |

:::info
Apache Iceberg doesn't support Gravitino `Varchar` `Fixedchar` `Byte` `Short` `Union` type.
:::

### Table properties

You can pass [Iceberg table properties](https://iceberg.apache.org/docs/1.3.1/configuration/) to Gravitino when creating Iceberg table.

The Gravitino server doesn't allow passing the following reserved fields.

| Configuration item        | Description                                             |
|---------------------------|---------------------------------------------------------|
| `comment`                 | The table comment.                                      |
| `creator`                 | The table creator.                                      |
| `location`                | Iceberg location for table storage.                     |
| `current-snapshot-id`     | The snapshot represents the current state of the table. |
| `cherry-pick-snapshot-id` | Selecting a specific snapshot in a merge operation.     |
| `sort-order`              | Selecting a specific snapshot in a merge operation.     |
| `identifier-fields`       | The identifier fields for defining the table.           |

### Table operations

Please refer to [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino.md#tables-operations) for more details.

#### Alter table operations

Supports operations:

- `RenameTable`
- `SetProperty`
- `RemoveProperty`
- `UpdateComment`
- `AddColumn`
- `DeleteColumn`
- `RenameColumn`
- `UpdateColumnType`
- `UpdateColumnPosition`
- `UpdateColumnNullability`
- `UpdateColumnComment`

:::info
The default column position is `LAST` when you add a column. If you add a non nullability column, there may be compatibility issues.
Iceberg just supports updating primitive types.
:::

:::caution
If you update a nullability column to non nullability, there may be compatibility issues.
:::

## HDFS configuration

You can place `core-site.xml` and `hdfs-site.xml` in the `catalogs/lakehouse-iceberg/conf` directory to automatically load as the default HDFS configuration.

:::caution
When writing to HDFS, the Gravitino Iceberg REST server can only operate as the specified HDFS user and doesn't support proxying to other HDFS users. See [How to access Apache Hadoop](gravitino-server-config.md) for more details.
:::
