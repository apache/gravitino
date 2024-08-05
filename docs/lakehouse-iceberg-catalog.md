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
Builds with Apache Iceberg `1.5.2`. The Apache Iceberg table format version is `2` by default.
:::

## Catalog

### Catalog capabilities

- Works as a catalog proxy, supporting `HiveCatalog`, `JdbcCatalog` and `RESTCatalog`.
- Supports DDL operations for Iceberg schemas and tables.
- Doesn't support snapshot or table management operations.
- Supports S3 and HDFS storage.
- Supports Kerberos or simple authentication for Iceberg catalog with Hive backend. 

### Catalog properties

| Property name                                      | Description                                                                                                                                                                                     | Default value          | Required                                                    | Since Version |
|----------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------|-------------------------------------------------------------|---------------|
| `catalog-backend`                                  | Catalog backend of Gravitino Iceberg catalog. Supports `hive` or `jdbc` or `rest`.                                                                                                              | (none)                 | Yes                                                         | 0.2.0         |
| `uri`                                              | The URI configuration of the Iceberg catalog. `thrift://127.0.0.1:9083` or `jdbc:postgresql://127.0.0.1:5432/db_name` or `jdbc:mysql://127.0.0.1:3306/metastore_db` or `http://127.0.0.1:9001`. | (none)                 | Yes                                                         | 0.2.0         |
| `warehouse`                                        | Warehouse directory of catalog. `file:///user/hive/warehouse-hive/` for local fs or `hdfs://namespace/hdfs/path` for HDFS.                                                                      | (none)                 | Yes                                                         | 0.2.0         |
| `catalog-backend-name`                             | The catalog name passed to underlying Iceberg catalog backend. Catalog name in JDBC backend is used to isolate namespace and tables.                                                            | Gravitino catalog name | No                                                          | 0.5.2         |
| `authentication.type`                              | The type of authentication for Iceberg catalog backend, currently Gravitino only supports `Kerberos`, `simple`.                                                                                 | `simple`               | No                                                          | 0.6.0         |
| `authentication.impersonation-enable`              | Whether to enable impersonation for the Iceberg catalog                                                                                                                                         | `false`                | No                                                          | 0.6.0         |
| `authentication.kerberos.principal`                | The principal of the Kerberos authentication                                                                                                                                                    | (none)                 | required if the value of `authentication.type` is Kerberos. | 0.6.0         |
| `authentication.kerberos.keytab-uri`               | The URI of The keytab for the Kerberos authentication.                                                                                                                                          | (none)                 | required if the value of `authentication.type` is Kerberos. | 0.6.0         |
| `authentication.kerberos.check-interval-sec`       | The check interval of Kerberos credential for Iceberg catalog.                                                                                                                                  | 60                     | No                                                          | 0.6.0         |
| `authentication.kerberos.keytab-fetch-timeout-sec` | The fetch timeout of retrieving Kerberos keytab from `authentication.kerberos.keytab-uri`.                                                                                                      | 60                     | No                                                          | 0.6.0         |


Any properties not defined by Gravitino with `gravitino.bypass.` prefix will pass to Iceberg catalog properties and HDFS configuration. For example, if specify `gravitino.bypass.list-all-tables`, `list-all-tables` will pass to Iceberg catalog properties.

When you use the Gravitino with Trino. You can pass the Trino Iceberg connector configuration using prefix `trino.bypass.`. For example, using `trino.bypass.iceberg.table-statistics-enabled` to pass the `iceberg.table-statistics-enabled` to the Gravitino Iceberg catalog in Trino runtime.

When you use the Gravitino with Spark. You can pass the Spark Iceberg connector configuration using prefix `spark.bypass.`. For example, using `spark.bypass.io-impl` to pass the `io-impl` to the Spark Iceberg connector in Spark runtime.


#### JDBC catalog

If you are using JDBC catalog, you must provide `jdbc-user`, `jdbc-password` and `jdbc-driver` to catalog properties.

| Property name     | Description                                                                                             | Default value | Required | Since Version |
|-------------------|---------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `jdbc-user`       | JDBC user name                                                                                          | (none)        | Yes      | 0.2.0         |
| `jdbc-password`   | JDBC password                                                                                           | (none)        | Yes      | 0.2.0         |
| `jdbc-driver`     | `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver` for MySQL, `org.postgresql.Driver` for PostgreSQL | (none)        | Yes      | 0.3.0         |
| `jdbc-initialize` | Whether to initialize meta tables when create JDBC catalog                                              | `true`        | No       | 0.2.0         |

If you have a JDBC Iceberg catalog prior, you must set `catalog-backend-name` to keep consistent with your Jdbc Iceberg catalog name to operate the prior namespace and tables.

:::caution
You must download the corresponding JDBC driver to the `catalogs/lakehouse-iceberg/libs` directory.
:::

#### S3

Supports using static access-key-id and secret-access-key to access S3 data.

| Configuration item     | Description                                                                                                                                                                                                         | Default value | Required | Since Version |
|------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `io-impl`              | The io implementation for `FileIO` in Iceberg, use `org.apache.iceberg.aws.s3.S3FileIO` for s3.                                                                                                                     | (none)        | No       | 0.6.0         |
| `s3-access-key-id`     | The static access key ID used to access S3 data.                                                                                                                                                                    | (none)        | No       | 0.6.0         |
| `s3-secret-access-key` | The static secret access key used to access S3 data.                                                                                                                                                                | (none)        | No       | 0.6.0         |
| `s3-endpoint`          | An alternative endpoint of the S3 service, This could be used for S3FileIO with any s3-compatible object storage service that has a different endpoint, or access a private S3 endpoint in a virtual private cloud. | (none)        | No       | 0.6.0         |
| `s3-region`            | The region of the S3 service, like `us-west-2`.                                                                                                                                                                     | (none)        | No       | 0.6.0         |

For other Iceberg s3 properties not managed by Gravitino like `s3.sse.type`, you could config it directly by `gravitino.bypass.s3.sse.type`.

:::info
Please set `gravitino.iceberg-rest.warehouse` to `s3://{bucket_name}/${prefix_name}` for JDBC catalog backend, `s3a://{bucket_name}/${prefix_name}` for Hive catalog backend.
:::

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

- Gravitino used by default `NoneDistribution`.

<Tabs groupId='language' queryString>
<TabItem value="json" label="JSON">

```json
{
  "strategy": "none",
  "number": 0,
  "expressions": []
}
```

</TabItem>
<TabItem value="java" label="Java">

```java
Distributions.NONE;
```

</TabItem>
</Tabs>

- Support `HashDistribution`, Hash distribute by partition key.

<Tabs groupId='language' queryString>
<TabItem value="json" label="JSON">

```json
{
  "strategy": "hash",
  "number": 0,
  "expressions": []
}
```
</TabItem>
<TabItem value="java" label="Java">

```java
Distributions.HASH;
```

</TabItem>
</Tabs>

- Support `RangeDistribution`, You can pass `range` as values through the API. Range distribute by partition key or sort key if table has an SortOrder.

<Tabs groupId='language' queryString>
<TabItem value="json" label="JSON">

```json
{
  "strategy": "range",
  "number": 0,
  "expressions": []
}
```

</TabItem>
<TabItem value="java" label="Java">

```java
Distributions.RANGE;
```

</TabItem>
</Tabs>

:::info
Iceberg automatically distributes the data according to the partition or table sort order. It is forbidden to specify distribution expressions.
:::
:::info
Apache Iceberg doesn't support Gravitino `EvenDistribution` type.
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
Meanwhile, the data types other than listed above are mapped to Gravitino **[External Type](./manage-relational-metadata-using-gravitino.md#external-type)** that represents an unresolvable data type since 0.6.0.
:::

### Table properties

You can pass [Iceberg table properties](https://iceberg.apache.org/docs/1.5.2/configuration/) to Gravitino when creating an Iceberg table.

The Gravitino server doesn't allow passing the following reserved fields.

| Configuration item              | Description                                             |
|---------------------------------|---------------------------------------------------------|
| `comment`                       | The table comment.                                      |
| `creator`                       | The table creator.                                      |
| `location`                      | Iceberg location for table storage.                     |
| `current-snapshot-id`           | The snapshot represents the current state of the table. |
| `cherry-pick-snapshot-id`       | Selecting a specific snapshot in a merge operation.     |
| `sort-order`                    | Selecting a specific snapshot in a merge operation.     |
| `identifier-fields`             | The identifier fields for defining the table.           |
| `write.distribution-mode`       | Defines distribution of write data                      |

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
