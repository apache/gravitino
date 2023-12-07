---
title: How to use Gravitino to manage Iceberg metadata
slug: /lakehouse-iceberg-catalog
keywords:
  - lakehouse
  - iceberg
  - metadata
---

## Introduction

Gravitino provides the ability to manage Iceberg metadata.

### Capabilities

* Worked as a catalog proxy, supports `HiveCatalog` and `JdbcCatalog`.
* Support DDL operation for schema and Iceberg table.
* Not support snapshot or table management operations.
* When writing to HDFS, the Gravitino Iceberg REST server can only operate as the specified HDFS user and
  it doesn't support proxying to other HDFS users. See **How to access Apache Hadoop** in the **How to customize Gravitino server configurations** document for more details.


## Catalog info
### Catalog Properties

| Configuration item | Description                                   | value                                                                                                  |
|-----------------------|-----------------------------------------------|--------------------------------------------------------------------------------------------------------|
| `catalog-backend`  | Catalog backend of Gravitino Iceberg catalog. | `hive` or `jdbc`                                                                                       |
| `uri`              | The uri config of the Iceberg catalog.        | `thrift://127.0.0.1:9083` or `jdbc:postgresql://127.0.0.1:5432/db_name` or `jdbc:mysql://127.0.0.1:3306/metastore_db` |
| `warehouse`        | Warehouse directory of catalog.               | `file:///user/hive/warehouse-hive/` for localfs or `hdfs://namespace/hdfs/path` for HDFS

Any properties not defined by Gravitino with `gravitino.bypass` prefix will pass to Iceberg catalog properites and HDFS configuration. For example, if specify `gravitino.bypass.list-all-tables`, `list-all-tables` will pass to Iceberg catalog properites.

#### JDBC catalog

If you are using JDBC catalog, you should provide `jdbc-user`, `jdbc-password` and `jdbc-driver` to catalog properties and download corresponding JDBC driver to the `catalogs/lakehouse-iceberg/libs` directory.

| Configuration item | Description                                   | Default value                                                                                                  |
|--------------------|-----------------------------------------------|--------------------------------------------------------------------------------------------------------|
| `jdbc-user`  | JDBC user name  | ``|
| `jdbc-password`  | JDBC password | ``|
| `jdbc-driver`  | `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver` for MySQL, `org.postgresql.Driver` for PostgreSQL | ``|
| `jdbc-initialize`  | Whether to initialize meta tables when create Jdbc catalog  | `true`|

## Schema info
### Capabilities
* Not support cascade drop schema.

### Schema properties
You could put any properties except `comment`.

## Table info

### Capabilities
* Not suppport `Distribution`, you should use `BucketPartition` instead.
* Built with Iceberg `1.3.1`. The Iceberg table format version is `1` by default.

### Type Info

| Gravitino Type | Iceberg Type |
|----------------|-----------------|
| `Struct`       |`Struct`         |
| `Map`          |`Map` |
| `Array`        |`Array` |
| `Boolean`      |`Boolean` |
| `Integer`             |`Integer` |
| `Long`             |`Long` |
| `Float`             |`Float` |
| `Double`             |`Double` |
| `String`             |`String` |
| `Date`             |`Date` |
| `Time`             |`Time` |
| `TimestampType withZone`             |`TimestampType withZone` |
| `TimestampType withoutZone`             |`TimestampType withoutZone` |
| `Decimal`             |`Decimal` |
| `Fixed`             |`Fixed` |
| `BinaryType`             |`Binary` |
| `UUID`             |`UUID` |

Iceberg does not support Gravitino `Varchar` `Fixedchar` `Byte` `Short` `Union` type.


### Table properties
You call pass [Iceberg table properties](https://iceberg.apache.org/docs/1.3.1/configuration/) to Gravitino when creating Iceberg table.

Gravitino server reserves the following fields, you can't pass them.

| Configuration item        | Description                                             |
|---------------------------|---------------------------------------------------------|
| `comment`                 | The table comment.                                      |
| `creator`                 | The table creator.                                      |
| `location`                | Iceberg location for table storage.                     |
| `current-snapshot-id`     | The snapshot represents the current state of the table. |
| `cherry-pick-snapshot-id` | Selecting a specific snapshot in a merge operation.     |
| `sort-order`              | Selecting a specific snapshot in a merge operation.     |
| `identifier-fields`       | The identifier field(s) for defining the table.         |

### HDFS configuration
You can place `core-site.xml` and `hdfs-site.xml` in the `catalogs/lakehouse-iceberg/conf` directory, and it will be automatically loaded as the default HDFS configuration. 
