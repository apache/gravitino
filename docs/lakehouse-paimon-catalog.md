---
title: "Paimon catalog"
slug: /lakehouse-paimon-catalog
keywords:
  - lakehouse
  - Paimon
  - metadata
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Apache Gravitino provides the ability to manage Apache Paimon metadata.

### Requirements

:::info
Builds with Apache Paimon `1.2`.
:::

## Catalog

### Catalog capabilities

- Works as a catalog proxy, supporting `FilesystemCatalog`, `JdbcCatalog` and `HiveCatalog`.
- Supports DDL operations for Paimon schemas and tables.

- Doesn't support alterSchema.

### Catalog properties

| Property name                                      | Description                                                                                                                                                                                                 | Default value                                                                  | Required                                                                                                                                                             | Since Version    |
|----------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|
| `catalog-backend`                                  | Catalog backend of Gravitino Paimon catalog. Supports `filesystem`, `jdbc` and `hive`.                                                                                                                      | (none)                                                                         | Yes                                                                                                                                                                  | 0.6.0-incubating |
| `uri`                                              | The URI configuration of the Paimon catalog. `thrift://127.0.0.1:9083` or `jdbc:postgresql://127.0.0.1:5432/db_name` or `jdbc:mysql://127.0.0.1:3306/metastore_db`. It is optional for `FilesystemCatalog`. | (none)                                                                         | required if the value of `catalog-backend` is not `filesystem`.                                                                                                      | 0.6.0-incubating |
| `warehouse`                                        | Warehouse directory of catalog. `file:///user/hive/warehouse-paimon/` for local fs, `hdfs://namespace/hdfs/path` for HDFS , `s3://{bucket-name}/path/` for S3 or `oss://{bucket-name}/path` for Aliyun OSS  | (none)                                                                         | Yes                                                                                                                                                                  | 0.6.0-incubating |
| `catalog-backend-name`                             | The catalog name passed to underlying Paimon catalog backend.                                                                                                                                               | The property value of `catalog-backend`, like `jdbc` for JDBC catalog backend. | No                                                                                                                                                                   | 0.8.0-incubating |
| `authentication.type`                              | The type of authentication for Paimon catalog backend, currently Gravitino only supports `Kerberos` and `simple`.                                                                                           | `simple`                                                                       | No                                                                                                                                                                   | 0.6.0-incubating |
| `hive.metastore.sasl.enabled`                      | Whether to enable SASL authentication protocol when connect to Kerberos Hive metastore. This is a raw Hive configuration                                                                                    | `false`                                                                        | No, This value should be true in most case(Some will use SSL protocol, but it rather rare) if the value of `gravitino.iceberg-rest.authentication.type` is Kerberos. | 0.6.0-incubating |
| `authentication.kerberos.principal`                | The principal of the Kerberos authentication.                                                                                                                                                               | (none)                                                                         | required if the value of `authentication.type` is Kerberos.                                                                                                          | 0.6.0-incubating |
| `authentication.kerberos.keytab-uri`               | The URI of The keytab for the Kerberos authentication.                                                                                                                                                      | (none)                                                                         | required if the value of `authentication.type` is Kerberos.                                                                                                          | 0.6.0-incubating |
| `authentication.kerberos.check-interval-sec`       | The check interval of Kerberos credential for Paimon catalog.                                                                                                                                               | 60                                                                             | No                                                                                                                                                                   | 0.6.0-incubating |
| `authentication.kerberos.keytab-fetch-timeout-sec` | The fetch timeout of retrieving Kerberos keytab from `authentication.kerberos.keytab-uri`.                                                                                                                  | 60                                                                             | No                                                                                                                                                                   | 0.6.0-incubating |
| `oss-endpoint`                                     | The endpoint of the Aliyun OSS.                                                                                                                                                                             | (none)                                                                         | required if the value of `warehouse` is a OSS path                                                                                                                   | 0.7.0-incubating |
| `oss-access-key-id`                                | The access key of the Aliyun OSS.                                                                                                                                                                           | (none)                                                                         | required if the value of `warehouse` is a OSS path                                                                                                                   | 0.7.0-incubating |
| `oss-access-key-secret`                            | The secret key the Aliyun OSS.                                                                                                                                                                              | (none)                                                                         | required if the value of `warehouse` is a OSS path                                                                                                                   | 0.7.0-incubating |
| `s3-endpoint`                                      | The endpoint of the AWS S3.                                                                                                                                                                                 | (none)                                                                         | required if the value of `warehouse` is a S3 path                                                                                                                    | 0.7.0-incubating |
| `s3-access-key-id`                                 | The access key of the AWS S3.                                                                                                                                                                               | (none)                                                                         | required if the value of `warehouse` is a S3 path                                                                                                                    | 0.7.0-incubating |
| `s3-secret-access-key`                             | The secret key of the AWS S3.                                                                                                                                                                               | (none)                                                                         | required if the value of `warehouse` is a S3 path                                                                                                                    | 0.7.0-incubating |

:::note
If you want to use the `oss` or `s3` warehouse, you need to place related jars in the `catalogs/lakehouse-paimon/lib` directory, more information can be found in the [Paimon S3](https://paimon.apache.org/docs/master/filesystems/s3/).
:::

:::note
The hive backend does not support the kerberos authentication now.
:::

Any properties not defined by Gravitino with `gravitino.bypass.` prefix will pass to Paimon catalog properties and HDFS configuration. For example, if specify `gravitino.bypass.table.type`, `table.type` will pass to Paimon catalog properties.

#### JDBC backend

If you are using JDBC backend, you must specify the properties like `jdbc-user`, `jdbc-password` and `jdbc-driver`.

| Property name   | Description                                                                                               | Default value   | Required                                              | Since Version    |
|-----------------|-----------------------------------------------------------------------------------------------------------|-----------------|-------------------------------------------------------|------------------|
| `jdbc-user`     | Jdbc user of Gravitino Paimon catalog for `jdbc` backend.                                                 | (none)          | required if the value of `catalog-backend` is `jdbc`. | 0.7.0-incubating |
| `jdbc-password` | Jdbc password of Gravitino Paimon catalog for `jdbc` backend.                                             | (none)          | required if the value of `catalog-backend` is `jdbc`. | 0.7.0-incubating |
| `jdbc-driver`   | `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver` for MySQL, `org.postgresql.Driver` for PostgreSQL   | (none)          | required if the value of `catalog-backend` is `jdbc`. | 0.7.0-incubating |

:::caution
You must download the corresponding JDBC driver and place it to the `catalogs/lakehouse-paimon/libs` directory If you are using JDBC backend.
:::

### Catalog operations

Please refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema capabilities

- Supporting createSchema, dropSchema, loadSchema and listSchema.
- Supporting cascade drop schema.

- Doesn't support alterSchema.

### Schema properties

- Doesn't support specify location and store any schema properties when createSchema for FilesystemCatalog.
- Doesn't return any schema properties when loadSchema for FilesystemCatalog.
- Doesn't support store schema comment for FilesystemCatalog.

### Schema operations

Please refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more details.

## Table

### Table capabilities

- Supporting createTable, purgeTable, alterTable, loadTable and listTable.
- Supporting Column default value through table properties, such as `fields.{columnName}.default-value`, not column expression.

- Doesn't support dropTable.
- Doesn't support table distribution and sort orders.

:::info
Gravitino Paimon Catalog does not support dropTable, because the dropTable in Paimon will both remove the table metadata and the table location from the file system and skip the trash, we should use purgeTable instead in Gravitino.
:::

:::info
Paimon does not support auto increment column.
:::

### Table changes

- RenameTable
- AddColumn
- DeleteColumn
- RenameColumn
- UpdateColumnComment
- UpdateColumnNullability
- UpdateColumnPosition
- UpdateColumnType
- UpdateComment
- SetProperty
- RemoveProperty

### Table partitions

- Only supports Identity partitions, such as `day`, `hour`, etc.

Please refer to [Paimon DDL Create Table](https://paimon.apache.org/docs/0.8/spark/sql-ddl/#create-table) for more details.

### Table sort orders

- Doesn't support table sort orders.

### Table distributions

- Doesn't support table distributions.

### Table indexes

- Only supports primary key Index.

:::info
We cannot specify more than one primary key Index, and a primary key Index can contain multiple fields as a joint primary key.
:::

:::info
Paimon Table primary key constraint should not be same with partition fields, this will result in only one record in a partition.
:::

### Table column types

| Gravitino Type    | Apache Paimon Type           |
|-------------------|------------------------------|
| `Struct`          | `Row`                        |
| `Map`             | `Map`                        |
| `List`            | `Array`                      |
| `Boolean`         | `Boolean`                    |
| `Byte`            | `TinyInt`                    |
| `Short`           | `SmallInt`                   |
| `Integer`         | `Int`                        |
| `Long`            | `BigInt`                     |
| `Float`           | `Float`                      |
| `Double`          | `Double`                     |
| `Decimal`         | `Decimal`                    |
| `String`          | `VarChar(Integer.MAX_VALUE)` |
| `VarChar`         | `VarChar`                    |
| `FixedChar`       | `Char`                       |
| `Date`            | `Date`                       |
| `Time(p)`         | `Time(p)`                    |
| `Timestamp(p)`    | `LocalZonedTimestamp(p)`     |
| `Timestamp_tz(p)` | `Timestamp(p)`               |
| `Fixed`           | `Binary`                     |
| `Binary`          | `VarBinary`                  |

:::info
Gravitino doesn't support Paimon `MultisetType` type.
:::

### Table properties

You can pass [Paimon table properties](https://paimon.apache.org/docs/0.8/maintenance/configurations/) to Gravitino when creating a Paimon table.

:::note
**Reserved**: Fields that cannot be passed to the Gravitino server.

**Immutable**: Fields that cannot be modified once set.
:::

| Configuration item | Description               | Default Value | Required  | Reserved | Immutable | Since version     |
|--------------------|---------------------------|---------------|-----------|----------|-----------|-------------------|
| `merge-engine`     | The table merge-engine.   | (none)        | No        | No       | Yes       | 0.6.0-incubating  |
| `sequence.field`   | The table sequence.field. | (none)        | No        | No       | Yes       | 0.6.0-incubating  |
| `rowkind.field`    | The table rowkind.field.  | (none)        | No        | No       | Yes       | 0.6.0-incubating  |
| `comment`          | The table comment.        | (none)        | No        | Yes      | No        | 0.6.0-incubating  |
| `owner`            | The table owner.          | (none)        | No        | Yes      | No        | 0.6.0-incubating  |
| `bucket-key`       | The table bucket-key.     | (none)        | No        | Yes      | No        | 0.6.0-incubating  |
| `primary-key`      | The table primary-key.    | (none)        | No        | Yes      | No        | 0.6.0-incubating  |
| `partition`        | The table partition.      | (none)        | No        | Yes      | No        | 0.6.0-incubating  |

### Table operations

Please refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for more details.

## HDFS configuration

You can place `core-site.xml` and `hdfs-site.xml` in the `catalogs/lakehouse-paimon/conf` directory to automatically load as the default HDFS configuration.

:::caution
When reading and writing to HDFS, the Gravitino server can only operate as the specified Kerberos user and doesn't support proxying to other Kerberos users now.
:::
