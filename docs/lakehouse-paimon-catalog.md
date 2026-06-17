---
title: "Paimon Catalog"
slug: "/lakehouse-paimon-catalog"
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

### Catalog Capabilities

- Works as a catalog proxy, supporting `FilesystemCatalog`, `JdbcCatalog` and `HiveCatalog`.
- Supports DDL operations for Paimon schemas and tables.

- Doesn't support alterSchema.

### Catalog Properties

| Property name                                      | Description                                                                                                                                                                                                 | Default value                                                                  | Required                                                                                                                                                             | Since Version    |
|----------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|
| `catalog-backend`                                  | Catalog backend of Gravitino Paimon catalog. Supports `filesystem`, `jdbc`, `hive` and `rest`.                                                                                                              | (none)                                                                         | Yes                                                                                                                                                                  | 0.6.0-incubating |
| `uri`                                              | The URI configuration of the Paimon catalog. `thrift://127.0.0.1:9083` or `jdbc:postgresql://127.0.0.1:5432/db_name` or `jdbc:mysql://127.0.0.1:3306/metastore_db`. It is optional for `FilesystemCatalog`. | (none)                                                                         | required if the value of `catalog-backend` is not `filesystem`.                                                                                                      | 0.6.0-incubating |
| `warehouse`                                        | Warehouse directory of catalog. `file:///user/hive/warehouse-paimon/` for local fs, `hdfs://namespace/hdfs/path` for HDFS , `s3://{bucket-name}/path/` for S3 or `oss://{bucket-name}/path` for Aliyun OSS  | (none)                                                                         | Yes                                                                                                                                                                  | 0.6.0-incubating |
| `catalog-backend-name`                             | The catalog name passed to underlying Paimon catalog backend.                                                                                                                                               | The property value of `catalog-backend`, like `jdbc` for JDBC catalog backend. | No                                                                                                                                                                   | 0.8.0-incubating |
| `authentication.type`                              | The type of authentication for Paimon catalog backend, Gravitino only supports `Kerberos` and `simple`.                                                                                                     | `simple`                                                                       | No                                                                                                                                                                   | 0.6.0-incubating |
| `hive.metastore.sasl.enabled`                      | Whether to enable SASL authentication protocol when connect to Kerberos Hive metastore. This is a raw Hive configuration                                                                                    | `false`                                                                        | No, This value should be true in most case(Some will use SSL protocol, but it rather rare) if the value of `gravitino.iceberg-rest.authentication.type` is Kerberos. | 0.6.0-incubating |
| `authentication.kerberos.principal`                | The principal of the Kerberos authentication.                                                                                                                                                               | (none)                                                                         | required if the value of `authentication.type` is Kerberos.                                                                                                          | 0.6.0-incubating |
| `authentication.kerberos.keytab-uri`               | The URI of The keytab for the Kerberos authentication.                                                                                                                                                      | (none)                                                                         | required if the value of `authentication.type` is Kerberos.                                                                                                          | 0.6.0-incubating |
| `authentication.kerberos.check-interval-sec`       | The check interval of Kerberos credential for Paimon catalog.                                                                                                                                               | 60                                                                             | No                                                                                                                                                                   | 0.6.0-incubating |
| `authentication.kerberos.keytab-fetch-timeout-sec` | The fetch timeout of retrieving Kerberos keytab from `authentication.kerberos.keytab-uri`.                                                                                                                  | 60                                                                             | No                                                                                                                                                                   | 0.6.0-incubating |
| `oss-endpoint`                                     | The endpoint of the Aliyun OSS.                                                                                                                                                                             | (none)                                                                         | required if the value of `warehouse` is a OSS path                                                                                                                   | 0.7.0-incubating |
| `oss-access-key-id`                                | The access key of the Aliyun OSS.                                                                                                                                                                           | (none)                                                                         | required if the value of `warehouse` is a OSS path                                                                                                                   | 0.7.0-incubating |
| `oss-secret-access-key`                            | The secret key the Aliyun OSS.                                                                                                                                                                              | (none)                                                                         | required if the value of `warehouse` is a OSS path                                                                                                                   | 0.7.0-incubating |
| `s3-endpoint`                                      | The endpoint of the AWS S3.                                                                                                                                                                                 | (none)                                                                         | required if the value of `warehouse` is a S3 path                                                                                                                    | 0.7.0-incubating |
| `s3-access-key-id`                                 | The access key of the AWS S3.                                                                                                                                                                               | (none)                                                                         | required if the value of `warehouse` is a S3 path                                                                                                                    | 0.7.0-incubating |
| `s3-secret-access-key`                             | The secret key of the AWS S3.                                                                                                                                                                               | (none)                                                                         | required if the value of `warehouse` is a S3 path                                                                                                                    | 0.7.0-incubating |
| `token-provider`                                   | The token provider type for Paimon catalog backend.                                                                                                                                                         | Token provider could be `bearer` or `dlf`.                                     | required if the value of `catalog-backend` is `rest`.                                                                                                                | 1.2.0            |
| `token`                                            | The bearer token for Paimon REST catalog authentication.                                                                                                                                                    | (none)                                                                         | required if the value of `token-provider` is `bearer`.                                                                                                               | 1.2.0            |
| `dlf-access-key-id`                                | The access key ID for Aliyun DLF (Data Lake Formation).                                                                                                                                                     | (none)                                                                         | required if the value of `catalog-backend` is `rest` and accessing Aliyun DLF Paimon REST server.                                                                    | 1.2.0            |
| `dlf-access-key-secret`                            | The access key secret for Aliyun DLF.                                                                                                                                                                       | (none)                                                                         | required if the value of `catalog-backend` is `rest` and accessing Aliyun DLF Paimon REST server.                                                                    | 1.2.0            |
| `dlf-security-token`                               | The security token for Aliyun DLF.                                                                                                                                                                          | (none)                                                                         | No                                                                                                                                                                   | 1.2.0            |
| `dlf-token-path`                                   | The token path for Aliyun DLF.                                                                                                                                                                              | (none)                                                                         | No                                                                                                                                                                   | 1.2.0            |
| `dlf-token-loader`                                 | The token loader for Aliyun DLF.                                                                                                                                                                            | (none)                                                                         | No                                                                                                                                                                   | 1.2.0            |

:::note
- If you want to use the `oss` or `s3` warehouse, you need to place related jars in the `catalogs/lakehouse-paimon/lib` directory, more information can be found in the [Paimon S3](https://paimon.apache.org/docs/1.2/maintenance/filesystems/#s3).
- If you use an S3 warehouse, also download [`gravitino-aws-<version>.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aws) and place it in the `catalogs/lakehouse-paimon/libs` directory to enable credential vending. For OSS, use [`gravitino-aliyun-<version>.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun) instead.
- If you want to use REST backend, Gravitino Paimon catalog supports Aliyun DLF (Data Lake Formation) as the REST catalog service. You need to configure the DLF-related properties eg:
```
{
  "name": "dlf_paimon",
  "type": "RELATIONAL",
  "provider": "lakehouse-paimon",
  "properties": {
    "catalog-backend": "rest",
    "uri": "<catalog server url>",
    "warehouse": "gravitino",
    "token-provider": "dlf",
    "dlf-access-key-id": "<access-key-id>",
    "dlf-access-key-secret": "<access-key-secret>"
  }
}
```
connect to Aliyun DLF, more information can be found in the [Paimon REST Catalog](https://paimon.apache.org/docs/master/concepts/rest/overview/).
- The hive backend does not support the kerberos authentication now.
:::

Any properties not defined by Gravitino with `gravitino.bypass.` prefix will pass to Paimon catalog properties and HDFS configuration. For example, if specify `gravitino.bypass.table.type`, `table.type` will pass to Paimon catalog properties.

#### JDBC Backend

If you are using JDBC backend, you must specify the properties like `jdbc-user`, `jdbc-password` and `jdbc-driver`.

| Property name   | Description                                                                                               | Default value   | Required                                              | Since Version    |
|-----------------|-----------------------------------------------------------------------------------------------------------|-----------------|-------------------------------------------------------|------------------|
| `jdbc-user`     | Jdbc user of Gravitino Paimon catalog for `jdbc` backend.                                                 | (none)          | required if the value of `catalog-backend` is `jdbc`. | 0.7.0-incubating |
| `jdbc-password` | Jdbc password of Gravitino Paimon catalog for `jdbc` backend.                                             | (none)          | required if the value of `catalog-backend` is `jdbc`. | 0.7.0-incubating |
| `jdbc-driver`   | `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver` for MySQL, `org.postgresql.Driver` for PostgreSQL   | (none)          | required if the value of `catalog-backend` is `jdbc`. | 0.7.0-incubating |

:::caution
Download the corresponding JDBC driver and place it to the `catalogs/lakehouse-paimon/libs` directory If you are using JDBC backend.
:::

### Catalog Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for more details.

:::note
Sensitive catalog properties such as `s3-access-key-id`, `s3-secret-access-key`, `jdbc-user`, and `jdbc-password` are hidden from the load catalog response since Gravitino 1.3.0. Use the [credential vending API](security/credential-vending.md) to retrieve them at runtime.
:::

## Schema

### Schema Capabilities

- Supporting createSchema, dropSchema, loadSchema and listSchema.
- Supporting cascade drop schema.

- Doesn't support alterSchema.

### Schema Properties

- Doesn't support specify location and store any schema properties when createSchema for FilesystemCatalog.
- Doesn't return any schema properties when loadSchema for FilesystemCatalog.
- Doesn't support store schema comment for FilesystemCatalog.

### Schema Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more details.

## Table

### Table Capabilities

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

### Table Changes

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

### Table Partitions

- Only supports Identity partitions, such as `day`, `hour`, etc.

Refer to [Paimon DDL Create Table](https://paimon.apache.org/docs/0.8/spark/sql-ddl/#create-table) for more details.

### Table Sort Orders

- Doesn't support table sort orders.

### Table Distributions

- Doesn't support table distributions.

### Table Indexes

- Only supports primary key Index.

:::info
We cannot specify more than one primary key Index, and a primary key Index can contain multiple fields as a joint primary key.
:::

:::info
Paimon Table primary key constraint should not be same with partition fields, this will result in only one record in a partition.
:::

### Table Column Types

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

### Table Properties

Pass [Paimon table properties](https://paimon.apache.org/docs/0.8/maintenance/configurations/) to Gravitino when creating a Paimon table.

:::note
**Reserved**: Fields that cannot be passed to the Gravitino server.

**Immutable**: Fields that cannot be modified once set.
:::

Bucket settings are defined via Gravitino table distribution (HASH strategy). The `bucket` and
`bucket-key` options are reserved and derived from the distribution instead of being set directly.

| Configuration item | Description               | Default Value | Required  | Reserved | Immutable | Since version     |
|--------------------|---------------------------|---------------|-----------|----------|-----------|-------------------|
| `merge-engine`     | The table merge-engine.   | (none)        | No        | No       | Yes       | 0.6.0-incubating  |
| `sequence.field`   | The table sequence.field. | (none)        | No        | No       | Yes       | 0.6.0-incubating  |
| `rowkind.field`    | The table rowkind.field.  | (none)        | No        | No       | Yes       | 0.6.0-incubating  |
| `comment`          | The table comment.        | (none)        | No        | Yes      | No        | 0.6.0-incubating  |
| `owner`            | The table owner.          | (none)        | No        | Yes      | No        | 0.6.0-incubating  |
| `bucket-key`       | The table bucket-key.     | (none)        | No        | Yes      | No        | 0.6.0-incubating  |
| `bucket`           | The table bucket number.  | (none)        | No        | Yes      | No        | 1.2.0  |
| `primary-key`      | The table primary-key.    | (none)        | No        | Yes      | No        | 0.6.0-incubating  |
| `partition`        | The table partition.      | (none)        | No        | Yes      | No        | 0.6.0-incubating  |

### Table Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for more details.

## View

### View Capabilities

- Supports list, create, load, alter, and drop for views stored in the Paimon catalog.
- Each view must include exactly one SQL representation with dialect `query`, which serves as the canonical view definition. At most one representation per dialect is allowed.
- Additional dialect-specific SQL representations (for example, `flink`, `spark`) can be provided alongside the required `query` representation.
- The `defaultCatalog` and `defaultSchema` fields are stored as Paimon view options and can be used to resolve unqualified identifiers in the SQL text.
- View support depends on the selected Paimon backend and requires backend view API support.

:::note
Rename cannot be combined with other changes in a single `alterView` call. Submit rename as a standalone request.
:::

### View Operations

Refer to [Manage view metadata using Gravitino](./manage-view-metadata-using-gravitino.md) for more details.

## HDFS Configuration

Place `core-site.xml` and `hdfs-site.xml` in the `catalogs/lakehouse-paimon/conf` directory to automatically load as the default HDFS configuration.

:::caution
When reading and writing to HDFS, the Gravitino server can only operate as the specified Kerberos user and doesn't support proxying to other Kerberos users now.
:::
