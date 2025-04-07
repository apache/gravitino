---
title: Lakehouse Iceberg catalog
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

Apache Gravitino can be used to manage Apache Iceberg metadata.

### Requirements and limitations

This catalog is built with Apache Iceberg `1.6.1`.
The Apache Iceberg table format version is `2` by default.

## Catalog

### Catalog capabilities

- This catalog works as a proxy, supporting `Hive`, `JDBC` and `REST` as the backends.
- The catalog supports DDL operations for Iceberg schemas and tables.
- The catalog supports multi storage, including S3, GCS, ADLS, OSS and HDFS.
- Snapshot or table management operations are not supported.
- The catalog supports Kerberos or simple authentication for Iceberg catalog with Hive backend.

### Catalog properties

<table>
<thead>
<tr>
  <th>Property name</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>catalog-backend</tt></td>
  <td>
    Catalog backend of Gravitino Iceberg catalog.
    Valid values are `hive`, `jdbc` and `rest`.
   </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>uri</tt></td>
  <td>
    The URI for the Iceberg catalog. Examples:
    - `thrift://127.0.0.1:9083`
    - `jdbc:postgresql://127.0.0.1:5432/db_name`
    - `jdbc:mysql://127.0.0.1:3306/metastore_db`
    - `http://127.0.0.1:9001`.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>warehouse</tt></td>
  <td>
    Warehouse directory of the catalog.

    - For local filesystem, use `file:///user/hive/warehouse-hive/` format;
    - For HDFS, use `hdfs://namespace/hdfs/path` format.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>catalog-backend-name</tt></td>
  <td>
    The catalog name passed to underlying Iceberg catalog backend.
    Catalog name in JDBC backend is used to isolate namespace and tables.

    The default value is the property value of `catalog-backend`.
    For JDBC catalog backend, the defaule value is `jdbc`.
  </td>
  <td>(see description)</td>
  <td>No</td>
  <td>`0.5.2`</td>
</tr>
</tbody>
</table>

Any property not defined by Gravitino, but prefixed with `gravitino.bypass.`
are passed to Iceberg catalog properties and HDFS configuration.
For example, for `gravitino.bypass.list-all-tables`,
the configuration `list-all-tables` is passed to Iceberg catalog.

If you are using the Gravitino with Trino,
you can pass the Trino Iceberg connector configuration using prefix `trino.bypass.`.
For example, `trino.bypass.iceberg.table-statistics-enabled`
can be used to pass the `iceberg.table-statistics-enabled` configuration
to the Gravitino Iceberg catalog in Trino runtime.

If you are using the Gravitino with Spark,
you can pass the Spark Iceberg connector configuration
using prefix `spark.bypass.`.
For example, `spark.bypass.io-impl` can be used
to pass the `io-impl` to the Spark Iceberg connector in Spark runtime.

#### JDBC backend

If you are using JDBC backend, you must provide properties
like `jdbc-user`, `jdbc-password` and `jdbc-driver`.

<table>
<thead>
<tr>
  <th>Property name</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>jdbc-user</tt></td>
  <td>The JDBC user name</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>jdbc-password</tt></td>
  <td>The JDBC password</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>jdbc-driver</tt></td>
  <td>
    `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver` for MySQL;
    `org.postgresql.Driver` for PostgreSQL.
  </td>
  <td>(see description)</td>
  <td>Yes</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>jdbc-initialize</tt></td>
  <td>
    Whether to initialize meta tables when creating a JDBC catalog.
  </td>
  <td>`true`</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
</tbody>
</table>

If you have a JDBC Iceberg catalog, you must set `catalog-backend-name`
to keep it consistent with your JDBC Iceberg catalog name
to operate the existing namespaces and tables.

:::caution
You must download the corresponding JDBC driver and place it
to the `catalogs/lakehouse-iceberg/libs` directory
if you are using the JDBC backend.
:::

#### S3

This catalog supports using static `access-key-id` and `secret-access-key` to access S3 data.

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>io-impl</tt></td>
  <td>
    The I/O implementation for `FileIO` in Iceberg.
    Use `org.apache.iceberg.aws.s3.S3FileIO` for S3.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>s3-access-key-id</tt></td>
  <td>The static access key ID used to access S3 data.</td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>s3-secret-access-key</tt></td>
  <td>The static secret access key used to access S3 data.</td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>s3-endpoint</tt></td>
  <td>
    An alternative endpoint of the S3 service.
    This can be used for S3FileIO with any s3-compatible object storage service
    that has a different endpoint.
    It can be used to access a private S3 endpoint in a virtual private cloud.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>s3-region</tt></td>
  <td>The region of the S3 service, like `us-west-2`.</td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>s3-path-style-access</tt></td>
  <td>Whether to use path style access for S3.</td>
  <td>false</td>
  <td>No</td>
  <td>`0.9.0-incubating`</td>
</tr>
</tbody>
</table>

For other Iceberg s3 properties not managed by Gravitino like `s3.sse.type`,
you can config it directly using `gravitino.bypass.s3.sse.type`.

:::info
To configure the JDBC catalog backend, set the `warehouse` parameter
to `s3://${bucket_name}/${prefix_name}`.
For the Hive catalog backend, set `warehouse` to
`s3a://${bucket_name}/${prefix_name}`.
In addition to this, you need to download the [Iceberg AWS bundle](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-aws-bundle)
and place it into the `catalogs/lakehouse-iceberg/libs/` directory.
:::

#### OSS

Gravitino Iceberg REST service supports using static `access-key-id` and `secret-access-key`
to access OSS data.

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
  <td><tt>io-impl</tt></td>
  <td>
    The I/O implementation for `FileIO` in Iceberg.
    Use `org.apache.iceberg.aliyun.oss.OSSFileIO` for OSS.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-access-key-id</tt></td>
  <td>The static access key ID used to access OSS data.</td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-secret-access-key</tt></td>
  <td>The static secret access key used to access OSS data.</td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-endpoint</tt></td>
  <td>The endpoint of Aliyun OSS service.</td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
</tbody>
</table>

For other Iceberg OSS properties not managed by Gravitino like `client.security-token`,
you can config it directly using `gravitino.bypass.client.security-token`.

:::info
Please set the `warehouse` parameter to `oss://${bucket_name}/${prefix_name}`.
You need to download the [Aliyun OSS SDK](https://gosspublic.alicdn.com/sdks/java/aliyun_java_sdk_3.10.2.zip)
and copy `aliyun-sdk-oss-3.10.2.jar`, `hamcrest-core-1.1.jar`, `jdom2-2.0.6.jar`
into the `catalogs/lakehouse-iceberg/libs/` directory.
:::

#### GCS

Supports using google credential file to access GCS data.

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
  <td><tt>io-impl</tt></td>
  <td>
    The I/O implementation for `FileIO` in Iceberg.
    Use `org.apache.iceberg.gcp.gcs.GCSFileIO` for GCS.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
</tbody>
</table>

For other Iceberg GCS properties not managed by Gravitino like `gcs.project-id`,
you can config it directly using `gravitino.bypass.gcs.project-id`.

Please make sure the credential file is accessible by Gravitino,
like using `export GOOGLE_APPLICATION_CREDENTIALS=/xx/application_default_credentials.json`
before the Gravitino server is started.

:::info
Please set `warehouse` to `gs://${bucket_name}/${prefix_name}`.
You need to download the [Iceberg GCP bundle JAR](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-gcp-bundle)
and place it to `catalogs/lakehouse-iceberg/libs/`.
:::

#### ADLS 

Supports using Azure account name and secret key to access ADLS data.

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
  <td><tt>io-impl</tt></td>
  <td>
    The I/O implementation for `FileIO` in Iceberg.
    Use `org.apache.iceberg.azure.adlsv2.ADLSFileIO` for ADLS.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>azure-storage-account-name</tt></td>
  <td>The static storage account name used to access ADLS data.</td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>azure-storage-account-key</tt></td>
  <td>The static storage account key used to access ADLS data.</td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.8.0-incubating`</td>
</tr>
</tbody>
</table>

For other Iceberg ADLS properties not managed by Gravitino like `adls.read.block-size-bytes`,
you can config it directly by setting `gravitino.iceberg-rest.adls.read.block-size-bytes`.

:::info
Please set `warehouse` to `abfs[s]://{container-name}@{storage-account-name}.dfs.core.windows.net/{path}`.
You need to download the [Iceberg Azure bundle](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-azure-bundle)
and place it to `catalogs/lakehouse-iceberg/libs/`.
:::

#### Other storages

For other storages that are not managed by Gravitino directly,
you can manage them through custom catalog properties.

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
  <td><tt>io-impl</tt></td>
  <td>
    The I/O implementation for `FileIO` in Iceberg.
    Please use the full qualified classname.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
</tbody>
</table>

To pass custom properties such as `security-token` to your custom `FileIO`,
you can directly configure it by setting `gravitino.bypass.security-token`.
`security-token` will be included in the properties
when the initialize method of `FileIO` is invoked.

:::info
Please set the `warehouse` parameter to `{storage_prefix}://{bucket_name}/${prefix_name}`.
You need to ownload corresponding JARs and place them
in the `catalogs/lakehouse-iceberg/libs/` directory.
:::

#### Catalog backend security

Users can use the following properties to configure the security of the catalog backend if needed.
For example, if you are using a Kerberos Hive catalog backend,
you must set `authentication.type` to `Kerberos` and provide `authentication.kerberos.principal`
and `authentication.kerberos.keytab-uri`.

<table>
<thead>
<tr>
  <th>Property name</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>authentication.type</tt></td>
  <td>
    The type of authentication for Iceberg catalog backend.
    This configuration only applicable for for Hive backend.
    It only supports `Kerberos`, `simple` currently.
    As for the JDBC backend, only username/password authentication is supported now.
  </td>
  <td>`simple`</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>authentication.impersonation-enable</tt></td>
  <td>When `true`, enable impersonation for the Iceberg catalog.</td>
  <td>`false`</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>hive.metastore.sasl.enabled</tt></td>
  <td>
    Whether to enable SASL authentication protocol when connect to Kerberos Hive metastore.
   This is a raw Hive configuration.

   This property should be `true` in most cases
   if the value of `gravitino.iceberg-rest.authentication.type` is Kerberos.
   Some will use SSL protocol, but it's very rare.
  </td>
  <td>`false`</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>authentication.kerberos.principal</tt></td>
  <td>
    The principal of the Kerberos authentication.
    This field is required if the value of `authentication.type` is Kerberos.
   </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>authentication.kerberos.keytab-uri</tt></td>
  <td>
    The URI of The keytab for the Kerberos authentication.

    This field is required if the value of `authentication.type` is Kerberos.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>authentication.kerberos.check-interval-sec</tt></td>
  <td>The checking interval seconds of Kerberos credential for Iceberg catalog.</td>
  <td>`60`</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>authentication.kerberos.keytab-fetch-timeout-sec</tt></td>
  <td>
    The fetch timeout seconds when retrieving Kerberos keytab
    from `authentication.kerberos.keytab-uri`.
   </td>
  <td>60</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
</tbody>
</table>

### Catalog operations

For more details, please refer to [manage relational metadata](../../../metadata/relational.md#catalog-operations).

## Schema

### Schema capabilities

- Cascaded drop for schema is not supported.

### Schema properties

You can set properties except `comment`.

### Schema operations

For more details, please refer to [managing relational metadata](../../../metadata/relational.md#schema-operations).

## Table

### Table capabilities

- Column default values is not supported.

### Table partitions

The following transforms are supported:

- `BucketTransform`
- `DayTransform`
- `HourTransform`
- `IdentityTransform`
- `MonthTransform`
- `TruncateTransform`
- `YearTransform`

:::info
Iceberg doesn't support multi fields in `BucketTransform`.
Iceberg doesn't support `ApplyTransform`, `RangeTransform`, and `ListTransform`.
:::

### Table sort orders

The followingexpressions are supported for sorting:

- `FieldReference`
- `FunctionExpression`
  - `bucket`
  - `day`
  - `hour`
  - `month`
  - `truncate`
  - `year`

:::info
For `bucket` and `truncate`, the first argument must be an integer literal,
and the second argument must be a field reference.
:::

### Table distributions

- This catalog suuports `HashDistribution` for distributing data
  based on the partition key specified.
- This catalog supports `RangeDistribution` that distributes data
  by a partitioning key or a sorting key for a SortOrder table.
- This catalog doesn't support `EvenDistribution`.

:::info
If you doesn't specify distribution expressions,
the table distribution will be defaulted to `RangeDistribution`
for a sorted table, or `HashDistribution` for a partitioned table.
:::

### Table column types

| Gravitino Type              | Apache Iceberg Type         |
|-----------------------------|-----------------------------|
| `Binary`                    | `Binary`                    |
| `Boolean`                   | `Boolean`                   |
| `Date`                      | `Date`                      |
| `Decimal`                   | `Decimal`                   |
| `Double`                    | `Double`                    |
| `Fixed`                     | `Fixed`                     |
| `Float`                     | `Float`                     |
| `Integer`                   | `Integer`                   |
| `List`                      | `Array`                     |
| `Long`                      | `Long`                      |
| `Map`                       | `Map`                       |
| `String`                    | `String`                    |
| `Struct`                    | `Struct`                    |
| `Time`                      | `Time`                      |
| `TimestampType withZone`    | `TimestampType withZone`    |
| `TimestampType withoutZone` | `TimestampType withoutZone` |
| `UUID`                      | `UUID`                      |

:::info
Apache Iceberg doesn't support Gravitino `Varchar`, `Fixedchar`, `Byte`, `Short`, and `Union` type.
Meanwhile, the data types other than listed above are mapped to Gravitino
**[External Type](../../../metadata/relational.md#external-type)**
that represents an unresolvable data type since *0.6.0-incubating*.
:::

### Table properties

You can pass [Iceberg table properties](https://iceberg.apache.org/docs/1.5.2/configuration/)
to Gravitino when creating an Iceberg table.

:::note
- *Reserved*: Fields that cannot be passed to the Gravitino server.
 -*Immutable*: Fields that cannot be modified once set.
:::

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Reserved</th>
  <th>Immutable</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>location</tt></td>
  <td>Iceberg location for table storage.</td>
  <td>(none)</td>
  <td>No</td>
  <td>No</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>provider</tt></td>
  <td>The storage provider for table storage.</td>
  <td>(none)</td>
  <td>No</td>
  <td>No</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>format</tt></td>
  <td>The format of table storage.</td>
  <td>(none)</td>
  <td>No</td>
  <td>No</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>format-version</tt></td>
  <td>The format version of table storage.</td>
  <td>(none)</td>
  <td>No</td>
  <td>No</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>comment</tt></td>
  <td>The table comment. Please use `comment` field in table meta instead.</td>
  <td>(none)</td>
  <td>No</td>
  <td>Yes</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>creator</tt></td>
  <td>The table creator.</td>
  <td>(none)</td>
  <td>No</td>
  <td>Yes</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>current-snapshot-id</tt></td>
  <td>The snapshot represents the current state of the table.</td>
  <td>(none)</td>
  <td>No</td>
  <td>Yes</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>cherry-pick-snapshot-id</tt></td>
  <td>Selecting a specific snapshot in a merge operation.</td>
  <td>(none)</td>
  <td>No</td>
  <td>Yes</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>sort-order</tt></td>
  <td>Iceberg table sort order. Please use `SortOrder` in table meta instead.</td>
  <td>(none)</td>
  <td>No</td>
  <td>Yes</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>identifier-fields</tt></td>
  <td>The identifier fields for defining the table.</td>
  <td>(none)</td>
  <td>No</td>
  <td>Yes</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>write.distribution-mode</tt></td>
  <td>
    The distribution of write data.
    Please use `distribution` in table meta instead.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>Yes</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
</tbody>
</table>

### Table indexes

- Indexed tables are not supported.

### Table operations

For more details, please refer to [managing relational metadata](../../../metadata/relational.md#table-operations).

#### Alter table operations

Supports operations:

- `AddColumn`
- `DeleteColumn`
- `RemoveProperty`
- `RenameTable`
- `RenameColumn`
- `SetProperty`
- `UpdateColumnType`
- `UpdateColumnPosition`
- `UpdateColumnNullability`
- `UpdateColumnComment`
- `UpdateComment`

:::info
The default column position is `LAST` when you add a column.
If you add a non nullability column, there may be compatibility issues.
:::

:::caution
If you update a nullability column to non nullability, there may be compatibility issues.
:::

## HDFS configuration

You can place `core-site.xml` and `hdfs-site.xml` in the `catalogs/lakehouse-iceberg/conf` directory
to automatically load as the default HDFS configuration.

:::info
- The implementation is built with Hadoop *2.10.x*,
  there may be compatibility issues when accessing Hadoop *3.x* clusters.

- When writing to HDFS, the Gravitino Iceberg REST server can only operate as the specified HDFS user
 and doesn't support proxying to other HDFS users.

- See [accessing Apache Hadoop](../../../admin/server-config.md) for more details.
:::

