---
title: "Lakehouse Apache Iceberg catalog"
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

### Capabilities

- Works as a catalog proxy, supporting `HiveCatalog` and `JdbcCatalog`.
- Supports DDL operation for Iceberg schemas and tables.
- Doesn't support snapshot or table management operations.
- When writing to HDFS, the Gravitino Iceberg REST server can only operate as the specified HDFS user and
  doesn't support proxying to other HDFS users. See [How to access Apache Hadoop](gravitino-server-config) for more details.

:::info
Builds with Apache Iceberg `1.3.1`. The Apache Iceberg table format version is `1` by default.
:::
:::notice
Builds with hadoop 2.10.x, there may compatibility issue when accessing hadoop 3.x clusters.
:::

## Catalog info

### Catalog properties

| Configuration item | Description                                   | value                                                                                                                 | Since Version |
|--------------------|-----------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|---------------|
| `catalog-backend`  | Catalog backend of Gravitino Iceberg catalog. | `hive` or `jdbc`                                                                                                      | 0.2.0         |
| `uri`              | The URI configuration of the Iceberg catalog. | `thrift://127.0.0.1:9083` or `jdbc:postgresql://127.0.0.1:5432/db_name` or `jdbc:mysql://127.0.0.1:3306/metastore_db` | 0.2.0         |
| `warehouse`        | Warehouse directory of catalog.               | `file:///user/hive/warehouse-hive/` for localfs or `hdfs://namespace/hdfs/path` for HDFS                              | 0.2.0         |

Any properties not defined by Gravitino with `gravitino.bypass` prefix will pass to Iceberg catalog properties and HDFS configuration. For example, if specify `gravitino.bypass.list-all-tables`, `list-all-tables` will pass to Iceberg catalog properties.

#### JDBC catalog

If you are using JDBC catalog, you must provide `jdbc-user`, `jdbc-password` and `jdbc-driver` to catalog properties.

| Configuration item | Description                                                                                             | Default value | Since Version |
|--------------------|---------------------------------------------------------------------------------------------------------|---------------|---------------|
| `jdbc-user`        | JDBC user name                                                                                          | ` `           | 0.2.0         |
| `jdbc-password`    | JDBC password                                                                                           | ` `           | 0.2.0         |
| `jdbc-driver`      | `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver` for MySQL, `org.postgresql.Driver` for PostgreSQL | ` `           | 0.3.0         |
| `jdbc-initialize`  | Whether to initialize meta tables when create Jdbc catalog                                              | `true`        | 0.2.0         |

:::caution
Your must download the corresponding JDBC driver to the `catalogs/lakehouse-iceberg/libs` directory.
:::

## Schema info

### Schema capabilities

- Not support cascade drop schema.

### Schema properties

You could put properties except `comment`.

## Table info

### Table partitions

Supports transforms:
  - `IdentityTransform`
  - `BucketTransform`
:::info
Iceberg doesn't support multi fields in `BucketTransform`
:::
  - `TruncateTransform`
  - `YearTransform`
  - `MonthTransform`
  - `DayTransform`
  - `HourTransform`
:::info
Iceberg doesn't support `ApplyTransform`, `RangeTransform` and `ListTransform`
:::

### Table sort orders

supports expressions:
- `FieldReference`
- `FunctionExpression`
  - `bucket`
:::info
The first argument must be integer literal,the second argument must be field reference.
:::
  - `truncate`
:::info
The first argument must be integer literal,the second argument must be field reference.
:::
  - `year`
  - `month`
  - `day`
  - `hour`

### Table distribution

- Doesn't support `Distribution`, you should use `BucketPartition` instead.

### Table column type 

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

Gravitino server reserves the following fields which can't be passed.

| Configuration item        | Description                                             |
|---------------------------|---------------------------------------------------------|
| `comment`                 | The table comment.                                      |
| `creator`                 | The table creator.                                      |
| `location`                | Iceberg location for table storage.                     |
| `current-snapshot-id`     | The snapshot represents the current state of the table. |
| `cherry-pick-snapshot-id` | Selecting a specific snapshot in a merge operation.     |
| `sort-order`              | Selecting a specific snapshot in a merge operation.     |
| `identifier-fields`       | The identifier fields for defining the table.           |


### Alter table operations

supports operations:
- `RenameTable`
- `SetProperty`
- `RemoveProperty`
- `UpdateComment`
- `AddColumn`
:::info
The default column position is `LAST` if not specifying column position.
:::
:::notice
If you add a non nullability column, there may be compatibility issue.
:::
- `DeleteColumn`
- `RenameColumn`
- `UpdateColumnType`
:::info
Iceberg supports update primitive type. 
:::
- `UpdateColumnPosition`
- `UpdateColumnNullability`
:::notice
If you update a nullability column to non nullability, there may be compatibility issue.
:::
- `UpdateColumnComment`

## HDFS configuration

You can place `core-site.xml` and `hdfs-site.xml` in the `catalogs/lakehouse-iceberg/conf` directory to automatically load as the default HDFS configuration.