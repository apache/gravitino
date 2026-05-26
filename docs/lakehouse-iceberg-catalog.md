---
title: "Iceberg Catalog"
slug: "/lakehouse-iceberg-catalog"
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

### Requirements and Limitations

:::info
Builds with Apache Iceberg `1.10.0`. The Apache Iceberg table format version is `2` by default.
:::

## Catalog

### Catalog Capabilities

The Iceberg catalog:

- Works as a catalog proxy, supporting `Hive`, `JDBC`, and `REST` as metadata backend options.
- Supports DDL operations on Iceberg schemas and tables.
- Does not support snapshot or table management operations.
- Supports multiple object storage providers (S3, GCS, ADLS, OSS, and HDFS).
- Supports Kerberos or simple authentication when using Hive as the metadata backend.
- Caches table metadata.

### Catalog Properties

| Property name          | Description                                                                                                                                                                                             | Default                                                                  | Required                                  | Since |
|------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|-------------------------------------------|---------------|
| `catalog-backend`      | Metadata backend type for the catalog. Supports `hive`, `jdbc`, or `rest`.                                                                                                                      | (none)                                                                         | Yes                                       | 0.2.0         |
| `uri`                  | The URI configuration of the Iceberg catalog. `thrift://127.0.0.1:9083` or `jdbc:postgresql://127.0.0.1:5432/db_name` or `jdbc:mysql://127.0.0.1:3306/metastore_db` or `http://127.0.0.1:9001/iceberg`. | (none)                                                                         | Yes                                       | 0.2.0         |
| `warehouse`            | Object storage location for the catalog's data files. Use a physical S3 or HDFS location when `catalog-backend` is `hive` or `jdbc`; use a catalog name when `catalog-backend` is `rest`.                                                      | (none)                                                                         | Yes for `hive` and `jdbc` catalog backend | 0.2.0         |
| `catalog-backend-name` | The name passed to the underlying metadata backend. In a JDBC store, this name isolates namespaces and tables.                                                                    | The property value of `catalog-backend` (e.g., `jdbc` when using JDBC). | No                                        | 0.5.2         |


Any property not defined by Gravitino with `gravitino.bypass.` prefix will pass to Iceberg catalog properties and HDFS configuration. For example, if specify `gravitino.bypass.list-all-tables`, `list-all-tables` will pass to Iceberg catalog properties.

When using Gravitino with Trino, pass Trino Iceberg connector configuration through the `trino.bypass.` prefix. For example, set `trino.bypass.iceberg.table-statistics-enabled` to forward `iceberg.table-statistics-enabled` to the Gravitino Iceberg catalog at Trino runtime.

When using Gravitino with Spark, pass Spark Iceberg connector configuration through the `spark.bypass.` prefix. For example, set `spark.bypass.io-impl` to forward `io-impl` to the Spark Iceberg connector at Spark runtime.


#### JDBC

When using JDBC as the metadata backend, you must provide properties like `jdbc-user`, `jdbc-password`, and `jdbc-driver`.

| Property name     | Description                                                                                             | Default | Required | Since |
|-------------------|---------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `jdbc-user`       | JDBC user name                                                                                          | (none)        | Yes      | 0.2.0         |
| `jdbc-password`   | JDBC password                                                                                           | (none)        | Yes      | 0.2.0         |
| `jdbc-driver`     | `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver` for MySQL, `org.postgresql.Driver` for PostgreSQL | (none)        | Yes      | 0.3.0         |
| `jdbc-initialize` | Whether to initialize meta tables when create JDBC catalog                                              | `true`        | No       | 0.2.0         |

If you have an existing JDBC Iceberg catalog, set `catalog-backend-name` to match your existing catalog name so its prior namespaces and tables remain accessible.

:::caution
Download the corresponding JDBC driver and place it in `catalogs/lakehouse-iceberg/libs` when using JDBC as the metadata backend.
If you have multiple JDBC metadata backends, setting `jdbc-initialize` to true may not take effect for RDBMS like `MySQL`; create Iceberg meta tables explicitly in that case.
:::

#### REST

When using REST as the metadata backend, `warehouse` identifies the catalog in the Iceberg REST spec. In the Gravitino Iceberg REST server, `warehouse` maps to the catalog name; an empty value means the default catalog.

`data-access` controls how the Iceberg REST client accesses table data when using REST as the metadata backend:

| Property name  | Description                                                                                                             | Default | Required | Since |
|----------------|-------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `data-access`  | Data access mode when using REST as the metadata backend. Supported values are `vended-credentials` and `remote-signing`.              | (none)        | No       | 1.3.0         |

- `vended-credentials`: request credential vending from the Iceberg REST server.
- `remote-signing`: Gravitino doesn't support this mode yet.

Example: create an Iceberg catalog with REST as the metadata backend. This targets the default catalog and uses a REST path like `http://127.0.0.1:9001/iceberg/v1/namespaces/db/tables/table`.

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-d '{
  "name": "iceberg_rest",
  "type": "RELATIONAL",
  "comment": "Iceberg REST catalog",
  "provider": "lakehouse-iceberg",
  "properties": {
    "catalog-backend": "rest",
    "uri": "http://localhost:9001/iceberg",
    "data-access": "vended-credentials"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs
```

To access a non-default catalog, set `warehouse` to the catalog name. This uses a REST path like `http://127.0.0.1:9001/iceberg/v1/catalog/namespaces/db/tables/table`. See [Multi catalog](./iceberg-rest-service.md#multiple-metadata-backends) for details.

#### S3

The Iceberg catalog supports static `access-key-id` and `secret-access-key` for S3.

| Property     | Description                                                                                                                                                                                                         | Default | Required | Since    |
|------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `io-impl`              | The io implementation for `FileIO` in Iceberg, use `org.apache.iceberg.aws.s3.S3FileIO` for s3.                                                                                                                     | (none)        | No       | 0.6.0-incubating |
| `s3-access-key-id`     | The static access key ID used to access S3 data.                                                                                                                                                                    | (none)        | No       | 0.6.0-incubating |
| `s3-secret-access-key` | The static secret access key used to access S3 data.                                                                                                                                                                | (none)        | No       | 0.6.0-incubating |
| `s3-endpoint`          | An alternative endpoint of the S3 service, This could be used for S3FileIO with any s3-compatible object storage service that has a different endpoint, or access a private S3 endpoint in a virtual private cloud. | (none)        | No       | 0.6.0-incubating |
| `s3-region`            | The region of the S3 service, like `us-west-2`.                                                                                                                                                                     | (none)        | No       | 0.6.0-incubating |
| `s3-path-style-access` | Whether to use path style access for S3.                                                                                                                                                                            | false         | No       | 0.9.0-incubating |


For other Iceberg s3 properties not managed by Gravitino like `s3.sse.type`, you could config it directly by `gravitino.bypass.s3.sse.type`.

:::info
 - When `catalog-backend` is `jdbc`, set `warehouse` to `s3://{bucket_name}/${prefix_name}`.
 - When `catalog-backend` is `hive`, set `warehouse` to `s3a://{bucket_name}/${prefix_name}`. 
 - Additionally, download the [Gravitino Iceberg AWS bundle](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-iceberg-aws-bundle) and place it in the `catalogs/lakehouse-iceberg/libs/` directory.
:::

:::note
Since Gravitino 1.1.0, the Gravitino Iceberg AWS bundle jar has already included the Iceberg AWS bundle jar, no need to download and include it separately.
:::

#### OSS

The Iceberg catalog supports static `access-key-id` and `secret-access-key` for OSS.

| Property      | Description                                                                                           | Default | Required | Since    |
|-------------------------|-------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `io-impl`               | The IO implementation for `FileIO` in Iceberg, use `org.apache.iceberg.aliyun.oss.OSSFileIO` for OSS. | (none)        | No       | 0.6.0-incubating |
| `oss-access-key-id`     | The static access key ID used to access OSS data.                                                     | (none)        | No       | 0.7.0-incubating |
| `oss-secret-access-key` | The static secret access key used to access OSS data.                                                 | (none)        | No       | 0.7.0-incubating |
| `oss-endpoint`          | The endpoint of Aliyun OSS service.                                                                   | (none)        | No       | 0.7.0-incubating |

For other Iceberg OSS properties not managed by Gravitino like `client.security-token`, you could config it directly by `gravitino.bypass.client.security-token`.

:::info
Set the `warehouse` parameter to `oss://{bucket_name}/${prefix_name}`. Additionally, download the [Gravitino Iceberg Aliyun bundle](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-iceberg-aliyun-bundle) and place it in the `catalogs/lakehouse-iceberg/libs/` directory.
:::

:::note
Since Gravitino 1.1.0, the Gravitino Iceberg aliyun bundle jar has already included the Iceberg aliyun necessary dependency jars, no need to download and include them separately.
:::

#### GCS

The Iceberg catalog supports a Google credential file for GCS access.

| Property | Description                                                                                        | Default | Required | Since    |
|--------------------|----------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `io-impl`          | The io implementation for `FileIO` in Iceberg, use `org.apache.iceberg.gcp.gcs.GCSFileIO` for GCS. | (none)        | No       | 0.6.0-incubating |

For other Iceberg GCS properties not managed by Gravitino like `gcs.project-id`, you could config it directly by `gravitino.bypass.gcs.project-id`.

Make sure the credential file is accessible by Gravitino, for example by setting `export GOOGLE_APPLICATION_CREDENTIALS=/xx/application_default_credentials.json` before starting the Gravitino server.

:::info
Set `warehouse` to `gs://{bucket_name}/${prefix_name}`, and download the [Gravitino Iceberg GCP bundle jar](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-iceberg-gcp-bundle) and place it in `catalogs/lakehouse-iceberg/libs/`.
:::

:::note
Since Gravitino 1.1.0, the Gravitino Iceberg GCP bundle jar has already included the Iceberg GCP bundle jar, no need to download and include it separately.
:::

#### ADLS

The Iceberg catalog supports an Azure storage account name and key for ADLS access.

| Property           | Description                                                                                               | Default | Required | Since    |
|------------------------------|-----------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `io-impl`                    | The io implementation for `FileIO` in Iceberg, use `org.apache.iceberg.azure.adlsv2.ADLSFileIO` for ADLS. | (none)        | No       | 0.6.0-incubating |
| `azure-storage-account-name` | The static storage account name used to access ADLS data.                                                 | (none)        | No       | 0.8.0-incubating |
| `azure-storage-account-key`  | The static storage account key used to access ADLS data.                                                  | (none)        | No       | 0.8.0-incubating |

For other Iceberg ADLS properties not managed by Gravitino like `adls.read.block-size-bytes`, you could config it directly by `gravitino.iceberg-rest.adls.read.block-size-bytes`.

:::info
Set `warehouse` to `abfs[s]://{container-name}@{storage-account-name}.dfs.core.windows.net/{path}`, and download the [Gravitino Iceberg Azure bundle](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-iceberg-azure-bundle) and place it in `catalogs/lakehouse-iceberg/libs/`.
:::

:::note
Since Gravitino 1.1.0, the Gravitino Iceberg Azure bundle jar has already included the Iceberg Azure bundle jar, no need to download and include it separately.
:::

#### Other Storage

For other storages that are not managed by Gravitino directly, you can manage them through custom catalog properties.

| Property | Description                                                                             | Default | Required | Since    |
|--------------------|-----------------------------------------------------------------------------------------|---------------|----------|------------------|
| `io-impl`          | The IO implementation for `FileIO` in Iceberg; use the fully qualified classname. | (none)        | No       | 0.6.0-incubating |

To pass custom properties such as `security-token` to your custom `FileIO`, you can directly configure it by `gravitino.bypass.security-token`. `security-token` will be included in the properties when the initialize method of `FileIO` is invoked.

:::info
Set the `warehouse` parameter to `{storage_prefix}://{bucket_name}/${prefix_name}`, and place the corresponding JARs in the `catalogs/lakehouse-iceberg/libs/` directory.
:::

#### Metadata Backend Security

Use the following properties to configure metadata backend security as needed. For example, when using a Kerberos-protected Hive metadata backend, set `authentication.type` to `Kerberos` and provide `authentication.kerberos.principal` and `authentication.kerberos.keytab-uri`.

| Property name                                      | Description                                                                                                                                                                                                                                      | Default | Required                                                                                                                                                             | Since    |
|----------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|
| `authentication.type`                              | The type of authentication for the metadata backend. Applies only to Hive and supports `Kerberos` and `simple`. For JDBC, only username/password authentication is supported.                                                                          | `simple`      | No                                                                                                                                                                   | 0.6.0-incubating |
| `authentication.impersonation-enable`              | Whether to enable impersonation for the Iceberg catalog                                                                                                                                                                                          | `false`       | No                                                                                                                                                                   | 0.6.0-incubating |
| `hive.metastore.sasl.enabled`                      | Whether to enable SASL when connecting to a Kerberos Hive metastore. This is a raw Hive configuration.                                                                                                                                           | `false`       | Should be `true` for most Kerberos setups (SSL is the rarer alternative) when `gravitino.iceberg-rest.authentication.type` is `Kerberos`.                            | 0.6.0-incubating |
| `authentication.kerberos.principal`                | The principal of the Kerberos authentication                                                                                                                                                                                                     | (none)        | required if the value of `authentication.type` is Kerberos.                                                                                                          | 0.6.0-incubating |
| `authentication.kerberos.keytab-uri`               | The URI of The keytab for the Kerberos authentication.                                                                                                                                                                                           | (none)        | required if the value of `authentication.type` is Kerberos.                                                                                                          | 0.6.0-incubating |
| `authentication.kerberos.check-interval-sec`       | The check interval of Kerberos credential for Iceberg catalog.                                                                                                                                                                                   | 60            | No                                                                                                                                                                   | 0.6.0-incubating |
| `authentication.kerberos.keytab-fetch-timeout-sec` | The fetch timeout of retrieving Kerberos keytab from `authentication.kerberos.keytab-uri`.                                                                                                                                                       | 60            | No                                                                                                                                                                   | 0.6.0-incubating |

#### Table Metadata Cache

Gravitino includes a pluggable cache system for table metadata. It validates the cached metadata location against the metadata backend before returning a hit, so cached data stays correct.

| Property                    | Description                                 | Default | Required | Since |
|---------------------------------------|---------------------------------------------|---------------|----------|---------------|
| `table-metadata-cache-impl`           | The implement of the cache.                 | (none)        | No       | 1.1.0         |
| `table-metadata-cache-capacity`       | The capacity of table metadata cache.       | 200           | No       | 1.1.0         |
| `table-metadata-cache-expire-minutes` | The expire minutes of table metadata cache. | 60            | No       | 1.1.0         |

Gravitino provides the built-in `org.apache.gravitino.iceberg.common.cache.LocalTableMetadataCache`, which stores cached data in memory. To plug in a custom cache, implement the `org.apache.gravitino.iceberg.common.cache.TableMetadataCache` interface.

### Catalog Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema Capabilities

The Iceberg catalog does not support cascade-dropping schemas.

### Schema Properties

Any schema property is allowed except `comment`.

### Schema Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more details.

## Table

### Table Capabilities

The Iceberg catalog does not support column default values.

### Table Partitions

The Iceberg catalog supports the following partition transforms:

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

### Table Sort Orders

The Iceberg catalog supports the following sort-order expressions:

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

### Table Distributions

The Iceberg catalog supports:

- `HashDistribution`, which distributes data by partition key.
- `RangeDistribution`, which distributes data by partition key (or sort key, for a sort-ordered table).

`EvenDistribution` is not supported.

:::info
If no distribution expression is specified, the table distribution defaults to `RangeDistribution` for a sort-ordered table and `HashDistribution` for a partitioned table.
:::

### Table Column Types

| Gravitino Type    | Apache Iceberg Type         |
|-------------------|-----------------------------|
| `Struct`          | `Struct`                    |
| `Map`             | `Map`                       |
| `List`            | `Array`                     |
| `Boolean`         | `Boolean`                   |
| `Integer`         | `Integer`                   |
| `Long`            | `Long`                      |
| `Float`           | `Float`                     |
| `Double`          | `Double`                    |
| `String`          | `String`                    |
| `Date`            | `Date`                      |
| `Time(6)`         | `Time`                      |
| `Timestamp(6)`    | `TimestampType withZone`    |
| `Timestamp_tz(6)` | `TimestampType withoutZone` |
| `Decimal`         | `Decimal`                   |
| `Fixed`           | `Fixed`                     |
| `Binary`          | `Binary`                    |
| `UUID`            | `UUID`                      |

:::info
Apache Iceberg does not support the Gravitino `Varchar`, `Fixedchar`, `Byte`, `Short`, or `Union` types.
Data types other than those listed above are mapped to the Gravitino **[External Type](./manage-relational-metadata-using-gravitino.md#external-type)**, which represents an unresolvable data type. (Since 0.6.0-incubating.)
:::

### Table Properties

Pass [Iceberg table properties](https://iceberg.apache.org/docs/1.5.2/configuration/) to Gravitino when creating an Iceberg table.

:::note
**Reserved**: Fields that cannot be passed to the Gravitino server.

**Immutable**: Fields that cannot be modified once set.
:::

| Property        | Description                                                                           | Default | Required | Reserved | Immutable | Since |
|---------------------------|---------------------------------------------------------------------------------------|---------------|----------|----------|-----------|---------------|
| `location`                | Iceberg location for table storage.                                                   | (none)        | No       | No       | Yes       | 0.2.0         |
| `provider`                | The storage provider for table storage.                                               | (none)        | No       | No       | Yes       | 0.2.0         |
| `format`                  | The format of table storage.                                                          | (none)        | No       | No       | Yes       | 0.2.0         |
| `format-version`          | The format version of table storage.                                                  | (none)        | No       | No       | Yes       | 0.2.0         |
| `comment`                 | The table comment; use the `comment` field in table meta instead.                  | (none)        | No       | Yes      | No        | 0.2.0         |
| `creator`                 | The table creator.                                                                    | (none)        | No       | Yes      | No        | 0.2.0         |
| `current-snapshot-id`     | The snapshot represents the current state of the table.                               | (none)        | No       | Yes      | No        | 0.2.0         |
| `cherry-pick-snapshot-id` | Selecting a specific snapshot in a merge operation.                                   | (none)        | No       | Yes      | No        | 0.2.0         |
| `sort-order`              | Iceberg table sort order; use `SortOrder` in table meta instead.               | (none)        | No       | Yes      | No        | 0.2.0         |
| `identifier-fields`       | The identifier fields for defining the table.                                         | (none)        | No       | Yes      | No        | 0.2.0         |
| `write.distribution-mode` | Defines distribution of write data; use `distribution` in table meta instead.  | (none)        | No       | Yes      | No        | 0.2.0         |

### Table Indexes

The Iceberg catalog does not support table indexes.

### Table Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for more details.

#### Alter Table Operations

The Iceberg catalog supports the following alter-table operations:

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
The default column position when adding a column is `LAST`. Adding a non-nullable column may cause compatibility issues with existing data.
:::

:::caution
Changing a nullable column to non-nullable may cause compatibility issues with existing data.
:::

## View

### View Capabilities

- Supports list, create, load, alter, and drop for views managed by the underlying Iceberg REST, JDBC, or Hive metadata backend.
- Supports dialects such as `trino`, `spark`, and `hive`.
- Can preserve multiple SQL representations for the same logical view.

### View Operations

Refer to [Manage view metadata using Gravitino](./manage-view-metadata-using-gravitino.md) for more details.

## HDFS Configuration

Place `core-site.xml` and `hdfs-site.xml` in the `catalogs/lakehouse-iceberg/conf` directory to automatically load as the default HDFS configuration.

:::info
Builds with Hadoop 2.10.x, there may be compatibility issues when accessing Hadoop 3.x clusters.
When writing to HDFS, the Gravitino Iceberg REST server can only operate as the specified HDFS user and doesn't support proxying to other HDFS users. See [How to access Apache Hadoop](gravitino-server-config.md) for more details.
:::
