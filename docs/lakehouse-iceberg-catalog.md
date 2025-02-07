---
title: "Iceberg catalog"
slug: /lakehouse-iceberg-catalog
keywords:
  - lakehouse
  - iceberg
  - metadata
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

Apache Gravitino provides the ability to manage Apache Iceberg metadata.

### Requirements and limitations

:::info
Builds with Apache Iceberg `1.6.1`. The Apache Iceberg table format version is `2` by default.
:::

## Catalog

### Catalog capabilities

- Works as a catalog proxy, supporting `Hive`, `JDBC` and `REST` as catalog backend.
- Supports DDL operations for Iceberg schemas and tables.
- Doesn't support snapshot or table management operations.
- Supports multi storage, including S3, GCS, ADLS, OSS and HDFS.
- Supports Kerberos or simple authentication for Iceberg catalog with Hive backend.

### Catalog properties

| Property name                                      | Description                                                                                                                                                                                                                                      | Default value                                                                  | Required                                                    | Since Version |
|----------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|-------------------------------------------------------------|---------------|
| `catalog-backend`                                  | Catalog backend of Gravitino Iceberg catalog. Supports `hive` or `jdbc` or `rest`.                                                                                                                                                               | (none)                                                                         | Yes                                                         | 0.2.0         |
| `uri`                                              | The URI configuration of the Iceberg catalog. `thrift://127.0.0.1:9083` or `jdbc:postgresql://127.0.0.1:5432/db_name` or `jdbc:mysql://127.0.0.1:3306/metastore_db` or `http://127.0.0.1:9001`.                                                  | (none)                                                                         | Yes                                                         | 0.2.0         |
| `warehouse`                                        | Warehouse directory of catalog. `file:///user/hive/warehouse-hive/` for local fs or `hdfs://namespace/hdfs/path` for HDFS.                                                                                                                       | (none)                                                                         | Yes                                                         | 0.2.0         |
| `catalog-backend-name`                             | The catalog name passed to underlying Iceberg catalog backend. Catalog name in JDBC backend is used to isolate namespace and tables.                                                                                                             | The property value of `catalog-backend`, like `jdbc` for JDBC catalog backend. | No                                                          | 0.5.2         |


Any property not defined by Gravitino with `gravitino.bypass.` prefix will pass to Iceberg catalog properties and HDFS configuration. For example, if specify `gravitino.bypass.list-all-tables`, `list-all-tables` will pass to Iceberg catalog properties.

If you are using the Gravitino with Trino, you can pass the Trino Iceberg connector configuration using prefix `trino.bypass.`. For example, using `trino.bypass.iceberg.table-statistics-enabled` to pass the `iceberg.table-statistics-enabled` to the Gravitino Iceberg catalog in Trino runtime.

If you are using the Gravitino with Spark, you can pass the Spark Iceberg connector configuration using prefix `spark.bypass.`. For example, using `spark.bypass.io-impl` to pass the `io-impl` to the Spark Iceberg connector in Spark runtime.


#### JDBC backend

If you are using JDBC backend, you must provide properties like `jdbc-user`, `jdbc-password` and `jdbc-driver`.

| Property name     | Description                                                                                             | Default value | Required | Since Version |
|-------------------|---------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `jdbc-user`       | JDBC user name                                                                                          | (none)        | Yes      | 0.2.0         |
| `jdbc-password`   | JDBC password                                                                                           | (none)        | Yes      | 0.2.0         |
| `jdbc-driver`     | `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver` for MySQL, `org.postgresql.Driver` for PostgreSQL | (none)        | Yes      | 0.3.0         |
| `jdbc-initialize` | Whether to initialize meta tables when create JDBC catalog                                              | `true`        | No       | 0.2.0         |

If you have a JDBC Iceberg catalog prior, you must set `catalog-backend-name` to keep consistent with your Jdbc Iceberg catalog name to operate the prior namespace and tables.

:::caution
You must download the corresponding JDBC driver and place it to the `catalogs/lakehouse-iceberg/libs` directory If you are using JDBC backend.
:::

#### S3

Supports using static access-key-id and secret-access-key to access S3 data.

| Configuration item     | Description                                                                                                                                                                                                         | Default value | Required | Since Version    |
|------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `io-impl`              | The io implementation for `FileIO` in Iceberg, use `org.apache.iceberg.aws.s3.S3FileIO` for s3.                                                                                                                     | (none)        | No       | 0.6.0-incubating |
| `s3-access-key-id`     | The static access key ID used to access S3 data.                                                                                                                                                                    | (none)        | No       | 0.6.0-incubating |
| `s3-secret-access-key` | The static secret access key used to access S3 data.                                                                                                                                                                | (none)        | No       | 0.6.0-incubating |
| `s3-endpoint`          | An alternative endpoint of the S3 service, This could be used for S3FileIO with any s3-compatible object storage service that has a different endpoint, or access a private S3 endpoint in a virtual private cloud. | (none)        | No       | 0.6.0-incubating |
| `s3-region`            | The region of the S3 service, like `us-west-2`.                                                                                                                                                                     | (none)        | No       | 0.6.0-incubating |

For other Iceberg s3 properties not managed by Gravitino like `s3.sse.type`, you could config it directly by `gravitino.bypass.s3.sse.type`.

:::info
To configure the JDBC catalog backend, set the `warehouse` parameter to `s3://{bucket_name}/${prefix_name}`. For the Hive catalog backend, set `warehouse` to `s3a://{bucket_name}/${prefix_name}`. Additionally, download the [Iceberg AWS bundle](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-aws-bundle) and place it in the `catalogs/lakehouse-iceberg/libs/` directory.
:::

#### OSS

Gravitino Iceberg REST service supports using static access-key-id and secret-access-key to access OSS data.

| Configuration item      | Description                                                                                           | Default value | Required | Since Version    |
|-------------------------|-------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `io-impl`               | The IO implementation for `FileIO` in Iceberg, use `org.apache.iceberg.aliyun.oss.OSSFileIO` for OSS. | (none)        | No       | 0.6.0-incubating |
| `oss-access-key-id`     | The static access key ID used to access OSS data.                                                     | (none)        | No       | 0.7.0-incubating |
| `oss-secret-access-key` | The static secret access key used to access OSS data.                                                 | (none)        | No       | 0.7.0-incubating |
| `oss-endpoint`          | The endpoint of Aliyun OSS service.                                                                   | (none)        | No       | 0.7.0-incubating |

For other Iceberg OSS properties not managed by Gravitino like `client.security-token`, you could config it directly by `gravitino.bypass.client.security-token`.

:::info
Please set the `warehouse` parameter to `oss://{bucket_name}/${prefix_name}`. Additionally, download the [Aliyun OSS SDK](https://gosspublic.alicdn.com/sdks/java/aliyun_java_sdk_3.10.2.zip) and copy `aliyun-sdk-oss-3.10.2.jar`, `hamcrest-core-1.1.jar`, `jdom2-2.0.6.jar` in the `catalogs/lakehouse-iceberg/libs/` directory.
:::

#### GCS

Supports using google credential file to access GCS data.

| Configuration item | Description                                                                                        | Default value | Required | Since Version    |
|--------------------|----------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `io-impl`          | The io implementation for `FileIO` in Iceberg, use `org.apache.iceberg.gcp.gcs.GCSFileIO` for GCS. | (none)        | No       | 0.6.0-incubating |

For other Iceberg GCS properties not managed by Gravitino like `gcs.project-id`, you could config it directly by `gravitino.bypass.gcs.project-id`.

Please make sure the credential file is accessible by Gravitino, like using `export GOOGLE_APPLICATION_CREDENTIALS=/xx/application_default_credentials.json` before Gravitino server is started.

:::info
Please set `warehouse` to `gs://{bucket_name}/${prefix_name}`, and download [Iceberg GCP bundle jar](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-gcp-bundle) and place it to `catalogs/lakehouse-iceberg/libs/`.
:::

#### ADLS 

Supports using Azure account name and secret key to access ADLS data.

| Configuration item           | Description                                                                                               | Default value | Required | Since Version    |
|------------------------------|-----------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `io-impl`                    | The io implementation for `FileIO` in Iceberg, use `org.apache.iceberg.azure.adlsv2.ADLSFileIO` for ADLS. | (none)        | No       | 0.6.0-incubating |
| `azure-storage-account-name` | The static storage account name used to access ADLS data.                                                 | (none)        | No       | 0.8.0-incubating |
| `azure-storage-account-key`  | The static storage account key used to access ADLS data.                                                  | (none)        | No       | 0.8.0-incubating |

For other Iceberg ADLS properties not managed by Gravitino like `adls.read.block-size-bytes`, you could config it directly by `gravitino.iceberg-rest.adls.read.block-size-bytes`.

:::info
Please set `warehouse` to `abfs[s]://{container-name}@{storage-account-name}.dfs.core.windows.net/{path}`, and download the [Iceberg Azure bundle](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-azure-bundle) and place it to `catalogs/lakehouse-iceberg/libs/`.
:::

#### Other storages

For other storages that are not managed by Gravitino directly, you can manage them through custom catalog properties.

| Configuration item | Description                                                                             | Default value | Required | Since Version    |
|--------------------|-----------------------------------------------------------------------------------------|---------------|----------|------------------|
| `io-impl`          | The IO implementation for `FileIO` in Iceberg, please use the full qualified classname. | (none)        | No       | 0.6.0-incubating |

To pass custom properties such as `security-token` to your custom `FileIO`, you can directly configure it by `gravitino.bypass.security-token`. `security-token` will be included in the properties when the initialize method of `FileIO` is invoked.

:::info
Please set the `warehouse` parameter to `{storage_prefix}://{bucket_name}/${prefix_name}`. Additionally, download corresponding jars in the `catalogs/lakehouse-iceberg/libs/` directory.
:::

#### Catalog backend security

Users can use the following properties to configure the security of the catalog backend if needed. For example, if you are using a Kerberos Hive catalog backend, you must set `authentication.type` to `Kerberos` and provide `authentication.kerberos.principal` and `authentication.kerberos.keytab-uri`.

| Property name                                      | Description                                                                                                                                                                                                                                      | Default value | Required                                                                                                                                                             | Since Version    |
|----------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|
| `authentication.type`                              | The type of authentication for Iceberg catalog backend. This configuration only applicable for for Hive backend, and only supports `Kerberos`, `simple` currently. As for JDBC backend, only username/password authentication was supported now. | `simple`      | No                                                                                                                                                                   | 0.6.0-incubating |
| `authentication.impersonation-enable`              | Whether to enable impersonation for the Iceberg catalog                                                                                                                                                                                          | `false`       | No                                                                                                                                                                   | 0.6.0-incubating |
| `hive.metastore.sasl.enabled`                      | Whether to enable SASL authentication protocol when connect to Kerberos Hive metastore. This is a raw Hive configuration                                                                                                                         | `false`       | No, This value should be true in most case(Some will use SSL protocol, but it rather rare) if the value of `gravitino.iceberg-rest.authentication.type` is Kerberos. | 0.6.0-incubating |
| `authentication.kerberos.principal`                | The principal of the Kerberos authentication                                                                                                                                                                                                     | (none)        | required if the value of `authentication.type` is Kerberos.                                                                                                          | 0.6.0-incubating |
| `authentication.kerberos.keytab-uri`               | The URI of The keytab for the Kerberos authentication.                                                                                                                                                                                           | (none)        | required if the value of `authentication.type` is Kerberos.                                                                                                          | 0.6.0-incubating |
| `authentication.kerberos.check-interval-sec`       | The check interval of Kerberos credential for Iceberg catalog.                                                                                                                                                                                   | 60            | No                                                                                                                                                                   | 0.6.0-incubating |
| `authentication.kerberos.keytab-fetch-timeout-sec` | The fetch timeout of retrieving Kerberos keytab from `authentication.kerberos.keytab-uri`.                                                                                                                                                       | 60            | No                                                                                                                                                                   | 0.6.0-incubating |

### Catalog operations

Please refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema capabilities

- doesn't support cascade drop schema.

### Schema properties

You could put properties except `comment`.

### Schema operations

Please refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more details.

## Table

### Table capabilities

- Doesn't support column default value.

### Table partitions

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
Iceberg doesn't support `ApplyTransform`, `RangeTransform`, and `ListTransform`.
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
For `bucket` and `truncate`, the first argument must be integer literal, and the second argument must be field reference.
:::

### Table distributions

- Support `HashDistribution`, which distribute data by partition key.
- Support `RangeDistribution`, which distribute data by partition key or sort key for a SortOrder table.
- Doesn't support `EvenDistribution`.

:::info
If you doesn't specify distribution expressions, the table distribution will be adjusted to `RangeDistribution` for a sort order table, to `HashDistribution` for a partition table.
:::

### Table column types

| Gravitino Type              | Apache Iceberg Type         |
|-----------------------------|-----------------------------|
| `Struct`                    | `Struct`                    |
| `Map`                       | `Map`                       |
| `List`                      | `Array`                     |
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
| `Binary`                    | `Binary`                    |
| `UUID`                      | `UUID`                      |

:::info
Apache Iceberg doesn't support Gravitino `Varchar` `Fixedchar` `Byte` `Short` `Union` type.
Meanwhile, the data types other than listed above are mapped to Gravitino **[External Type](./manage-relational-metadata-using-gravitino.md#external-type)** that represents an unresolvable data type since 0.6.0-incubating.
:::

### Table properties

You can pass [Iceberg table properties](https://iceberg.apache.org/docs/1.5.2/configuration/) to Gravitino when creating an Iceberg table.

:::note
**Reserved**: Fields that cannot be passed to the Gravitino server.

**Immutable**: Fields that cannot be modified once set.
:::

| Configuration item        | Description                                                                           | Default value | Required | Reserved | Immutable | Since Version |
|---------------------------|---------------------------------------------------------------------------------------|---------------|----------|----------|-----------|---------------|
| `location`                | Iceberg location for table storage.                                                   | (none)        | No       | No       | Yes       | 0.2.0         |
| `provider`                | The storage provider for table storage.                                               | (none)        | No       | No       | Yes       | 0.2.0         |
| `format`                  | The format of table storage.                                                          | (none)        | No       | No       | Yes       | 0.2.0         |
| `format-version`          | The format version of table storage.                                                  | (none)        | No       | No       | Yes       | 0.2.0         |
| `comment`                 | The table comment, please use `comment` field in table meta instead.                  | (none)        | No       | Yes      | No        | 0.2.0         |
| `creator`                 | The table creator.                                                                    | (none)        | No       | Yes      | No        | 0.2.0         |
| `current-snapshot-id`     | The snapshot represents the current state of the table.                               | (none)        | No       | Yes      | No        | 0.2.0         |
| `cherry-pick-snapshot-id` | Selecting a specific snapshot in a merge operation.                                   | (none)        | No       | Yes      | No        | 0.2.0         |
| `sort-order`              | Iceberg table sort order, please use `SortOrder` in table meta instead.               | (none)        | No       | Yes      | No        | 0.2.0         |
| `identifier-fields`       | The identifier fields for defining the table.                                         | (none)        | No       | Yes      | No        | 0.2.0         |
| `write.distribution-mode` | Defines distribution of write data, please use `distribution` in table meta instead.  | (none)        | No       | Yes      | No        | 0.2.0         |

### Table indexes

- Doesn't support table indexes.

### Table operations

Please refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for more details.

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
:::

:::caution
If you update a nullability column to non nullability, there may be compatibility issues.
:::

## HDFS configuration

You can place `core-site.xml` and `hdfs-site.xml` in the `catalogs/lakehouse-iceberg/conf` directory to automatically load as the default HDFS configuration.

:::info
Builds with Hadoop 2.10.x, there may be compatibility issues when accessing Hadoop 3.x clusters.
When writing to HDFS, the Gravitino Iceberg REST server can only operate as the specified HDFS user and doesn't support proxying to other HDFS users. See [How to access Apache Hadoop](gravitino-server-config.md) for more details.
:::
