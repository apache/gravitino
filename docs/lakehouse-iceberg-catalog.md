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
Builds with Apache Iceberg `1.11.0`. The Apache Iceberg table format version is `2` by default.
:::

Flink and Spark clients may use a different Iceberg version than the server.

- [Flink Iceberg catalog](flink-connector/flink-catalog-iceberg.md) — client JAR requirements
- [Spark Iceberg catalog](spark-connector/spark-catalog-iceberg.md) — client JAR requirements

:::caution
Mixing Iceberg JARs from different versions on the client classpath is not compatible and may cause runtime errors.
:::

## Catalog

### Catalog Capabilities

- Works as a catalog proxy, supporting `Hive`, `JDBC` and `REST` as catalog backend.
- Supports DDL operations for Iceberg schemas and tables.
- Doesn't support snapshot or table management operations.
- Supports multi storage, including S3, GCS, ADLS, OSS and HDFS.
- Supports Kerberos or simple authentication for Iceberg catalog with Hive backend.
- Supports table metadata cache.

### Catalog Properties

| Property name          | Description                                                                                                                                                                                             | Default value                                                                  | Required                                  | Since Version |
|------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|-------------------------------------------|---------------|
| `catalog-backend`      | Catalog backend of Gravitino Iceberg catalog. Supports `hive` or `jdbc` or `rest`.                                                                                                                      | (none)                                                                         | Yes                                       | 0.2.0         |
| `uri`                  | The URI configuration of the Iceberg catalog. `thrift://127.0.0.1:9083` or `jdbc:postgresql://127.0.0.1:5432/db_name` or `jdbc:mysql://127.0.0.1:3306/metastore_db` or `http://127.0.0.1:9001/iceberg`. | (none)                                                                         | Yes                                       | 0.2.0         |
| `warehouse`            | Warehouse location of catalog. Use a physical S3 or HDFS location for `hive` or `jdbc` catalog backend, use catalog name for REST catalog backend.                                                      | (none)                                                                         | Yes for `hive` and `jdbc` catalog backend | 0.2.0         |
| `catalog-backend-name` | The catalog name passed to underlying Iceberg catalog backend. Catalog name in JDBC backend is used to isolate namespace and tables.                                                                    | The property value of `catalog-backend`, like `jdbc` for JDBC catalog backend. | No                                        | 0.5.2         |


Any property not defined by Gravitino with `gravitino.bypass.` prefix will pass to Iceberg catalog properties and HDFS configuration. For example, if specify `gravitino.bypass.list-all-tables`, `list-all-tables` will pass to Iceberg catalog properties.

If you are using the Gravitino with Trino, you can pass the Trino Iceberg connector configuration using prefix `trino.bypass.`. For example, using `trino.bypass.iceberg.table-statistics-enabled` to pass the `iceberg.table-statistics-enabled` to the Gravitino Iceberg catalog in Trino runtime.

If you are using the Gravitino with Spark, you can pass the Spark Iceberg connector configuration using prefix `spark.bypass.`. For example, using `spark.bypass.io-impl` to pass the `io-impl` to the Spark Iceberg connector in Spark runtime.


#### JDBC Backend

If you are using JDBC backend, you must provide properties like `jdbc-user`, `jdbc-password` and `jdbc-driver`.

| Property name     | Description                                                                                             | Default value | Required | Since Version |
|-------------------|---------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `jdbc-user`       | JDBC user name                                                                                          | (none)        | Yes      | 0.2.0         |
| `jdbc-password`   | JDBC password                                                                                           | (none)        | Yes      | 0.2.0         |
| `jdbc-driver`     | `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver` for MySQL, `org.postgresql.Driver` for PostgreSQL | (none)        | Yes      | 0.3.0         |
| `jdbc-initialize` | Whether to initialize meta tables when create JDBC catalog                                              | `true`        | No       | 0.2.0         |

If you have a JDBC Iceberg catalog prior, you must set `catalog-backend-name` to keep consistent with your Jdbc Iceberg catalog name to operate the prior namespace and tables.

:::caution
Download the corresponding JDBC driver and place it to the `catalogs/lakehouse-iceberg/libs` directory If you are using JDBC backend.
If you are using multiple JDBC catalog backends, setting `jdbc-initialize` to true may not take effect for RDMS like `Mysql`, you should create Iceberg meta tables explicitly.
:::

#### REST Catalog Backend

For the REST catalog backend, `warehouse` identifies the catalog in the Iceberg REST spec. In the Gravitino Iceberg REST server, `warehouse` maps to the catalog name. An empty value means the default catalog.

The following properties tune REST backend behavior:

| Property name                         | Description                                                                                                | Default value | Required | Since Version |
|---------------------------------------|------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `data-access`                         | Data access mode for REST catalog backend. Supported values are `vended-credentials` and `remote-signing`. | (none)        | No       | 1.3.0         |
| `rest-client-connection-timeout-ms`   | The HTTP connection timeout in milliseconds for requests to the REST catalog backend.                      | 10000         | No       | 1.3.0         |
| `rest-client-socket-timeout-ms`       | The HTTP socket timeout in milliseconds for requests to the REST catalog backend.                          | 60000         | No       | 1.3.0         |

- `vended-credentials`: request credential vending from the Iceberg REST server.
- `remote-signing`: Gravitino doesn't support this mode yet.

Example: create an Iceberg catalog with the REST backend. This targets the default catalog and uses a REST path like `http://127.0.0.1:9001/iceberg/v1/namespaces/db/tables/table`.

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
    "rest-client-connection-timeout-ms": "10000",
    "rest-client-socket-timeout-ms": "60000",
    "data-access": "vended-credentials"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs
```

To access a non-default catalog, set `warehouse` to the catalog name. This uses a REST path like `http://127.0.0.1:9001/iceberg/v1/catalog/namespaces/db/tables/table`. See [Multi-Catalog Configuration](./iceberg-rest-service.md#multi-catalog-configuration) for details.

#### S3

If `io-impl` is not configured, the Iceberg catalog uses
`org.apache.iceberg.io.ResolvingFileIO`, which selects a `FileIO` implementation based
on the URI scheme:

- S3: `s3`, `s3a`, or `s3n`
- OSS: `oss`
- GCS: `gs` or `gcs`
- ADLS: `abfs`, `abfss`, `wasb`, or `wasbs`
- To override the default, explicitly configure `io-impl`.
- Ensure that the corresponding storage bundle is available in the Iceberg catalog classpath.

Supports using static access-key-id and secret-access-key to access S3 data.

| Configuration item     | Description                                                                                                                                                                                                         | Default value                           | Required | Since Version    |
|------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|----------|------------------|
| `io-impl`              | The IO implementation for `FileIO` in Iceberg. Set it to `org.apache.iceberg.aws.s3.S3FileIO` to explicitly use S3FileIO.                                                                                           | `org.apache.iceberg.io.ResolvingFileIO` | No       | 0.6.0-incubating |
| `s3-access-key-id`     | The static access key ID used to access S3 data.                                                                                                                                                                    | (none)                                  | No       | 0.6.0-incubating |
| `s3-secret-access-key` | The static secret access key used to access S3 data.                                                                                                                                                                | (none)                                  | No       | 0.6.0-incubating |
| `s3-endpoint`          | An alternative endpoint of the S3 service, This could be used for S3FileIO with any s3-compatible object storage service that has a different endpoint, or access a private S3 endpoint in a virtual private cloud. | (none)                                  | No       | 0.6.0-incubating |
| `s3-region`            | The region of the S3 service, like `us-west-2`.                                                                                                                                                                     | (none)                                  | No       | 0.6.0-incubating |
| `s3-path-style-access` | Whether to use path style access for S3.                                                                                                                                                                            | false                                   | No       | 0.9.0-incubating |


For other Iceberg s3 properties not managed by Gravitino like `s3.sse.type`, you could config it directly by `gravitino.bypass.s3.sse.type`.

:::info
 - For the JDBC catalog backend, set the `warehouse` parameter to `s3://{bucket_name}/${prefix_name}`. 
 - For the Hive catalog backend, set `warehouse` to `s3a://{bucket_name}/${prefix_name}`. 
 - Additionally, download the [Gravitino Iceberg AWS bundle](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-iceberg-aws-bundle) and place it in the `catalogs/lakehouse-iceberg/libs/` directory.
:::

:::note
Since Gravitino 1.1.0, the Gravitino Iceberg AWS bundle jar has already included the Iceberg AWS bundle jar, no need to download and include it separately.
:::

#### OSS

Gravitino Iceberg REST service supports using static access-key-id and secret-access-key to access OSS data.

| Configuration item      | Description                                                                                                                     | Default value                           | Required | Since Version    |
|-------------------------|---------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|----------|------------------|
| `io-impl`               | The IO implementation for `FileIO` in Iceberg. Set it to `org.apache.iceberg.aliyun.oss.OSSFileIO` to explicitly use OSSFileIO. | `org.apache.iceberg.io.ResolvingFileIO` | No       | 0.6.0-incubating |
| `oss-access-key-id`     | The static access key ID used to access OSS data.                                                                               | (none)                                  | No       | 0.7.0-incubating |
| `oss-secret-access-key` | The static secret access key used to access OSS data.                                                                           | (none)                                  | No       | 0.7.0-incubating |
| `oss-endpoint`          | The endpoint of Aliyun OSS service.                                                                                             | (none)                                  | No       | 0.7.0-incubating |

For other Iceberg OSS properties not managed by Gravitino like `client.security-token`, you could config it directly by `gravitino.bypass.client.security-token`.

:::info
Please set the `warehouse` parameter to `oss://{bucket_name}/${prefix_name}`. Additionally, download the [Gravitino Iceberg Aliyun bundle](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-iceberg-aliyun-bundle) and place it in the `catalogs/lakehouse-iceberg/libs/` directory.
:::

:::note
Since Gravitino 1.1.0, the Gravitino Iceberg aliyun bundle jar has already included the Iceberg aliyun necessary dependency jars, no need to download and include them separately.
:::

#### GCS

Supports using google credential file to access GCS data.

| Configuration item | Description                                                                                                                  | Default value                           | Required | Since Version    |
|--------------------|------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|----------|------------------|
| `io-impl`          | The IO implementation for `FileIO` in Iceberg. Set it to `org.apache.iceberg.gcp.gcs.GCSFileIO` to explicitly use GCSFileIO. | `org.apache.iceberg.io.ResolvingFileIO` | No       | 0.6.0-incubating |

For other Iceberg GCS properties not managed by Gravitino like `gcs.project-id`, you could config it directly by `gravitino.bypass.gcs.project-id`.

Please make sure the credential file is accessible by Gravitino, like using `export GOOGLE_APPLICATION_CREDENTIALS=/xx/application_default_credentials.json` before Gravitino server is started.

:::info
Please set `warehouse` to `gs://{bucket_name}/${prefix_name}`, and download [Gravitino Iceberg GCP bundle jar](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-iceberg-gcp-bundle) and place it to `catalogs/lakehouse-iceberg/libs/`.
:::

:::note
Since Gravitino 1.1.0, the Gravitino Iceberg GCP bundle jar has already included the Iceberg GCP bundle jar, no need to download and include it separately.
:::

#### ADLS

Supports using Azure account name and secret key to access ADLS data.

| Configuration item           | Description                                                                                                                         | Default value                           | Required | Since Version    |
|------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|----------|------------------|
| `io-impl`                    | The IO implementation for `FileIO` in Iceberg. Set it to `org.apache.iceberg.azure.adlsv2.ADLSFileIO` to explicitly use ADLSFileIO. | `org.apache.iceberg.io.ResolvingFileIO` | No       | 0.6.0-incubating |
| `azure-storage-account-name` | The static storage account name used to access ADLS data.                                                                           | (none)                                  | No       | 0.8.0-incubating |
| `azure-storage-account-key`  | The static storage account key used to access ADLS data.                                                                            | (none)                                  | No       | 0.8.0-incubating |

For other Iceberg ADLS properties not managed by Gravitino like `adls.read.block-size-bytes`, you could config it directly by `gravitino.iceberg-rest.adls.read.block-size-bytes`.

:::info
Please set `warehouse` to `abfs[s]://{container-name}@{storage-account-name}.dfs.core.windows.net/{path}`, and download the [Gravitino Iceberg Azure bundle](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-iceberg-azure-bundle) and place it to `catalogs/lakehouse-iceberg/libs/`.
:::

:::note
Since Gravitino 1.1.0, the Gravitino Iceberg Azure bundle jar has already included the Iceberg Azure bundle jar, no need to download and include it separately.
:::

#### Other Storage

For other storages that are not managed by Gravitino directly, you can manage them through custom catalog properties.

| Configuration item | Description                                                                                                               | Default value                           | Required | Since Version    |
|--------------------|---------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|----------|------------------|
| `io-impl`          | The IO implementation for `FileIO` in Iceberg. Use the fully qualified class name to override the default implementation. | `org.apache.iceberg.io.ResolvingFileIO` | No       | 0.6.0-incubating |

To pass custom properties such as `security-token` to your custom `FileIO`, you can directly configure it by `gravitino.bypass.security-token`. `security-token` will be included in the properties when the initialize method of `FileIO` is invoked.

:::info
Please set the `warehouse` parameter to `{storage_prefix}://{bucket_name}/${prefix_name}`. Additionally, download corresponding jars in the `catalogs/lakehouse-iceberg/libs/` directory.
:::

#### Catalog Backend Security

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

#### Table Metadata Cache

Gravitino features a pluggable cache system for updating or retrieving table metadata in the cache. It validates the location of table metadata against the catalog backend to ensure the correctness of cached data.

| Configuration item                    | Description                                                                                                                                                                           | Default value                                                       | Required | Since Version |
|---------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------|----------|---------------|
| `table-metadata-cache-impl`           | The implementation of the table metadata cache. Set to empty string("") if `catalog-backend` is `rest` catalog, or `custom` catalog without the `SupportsMetadataLocation` interface. | `org.apache.gravitino.iceberg.common.cache.LocalTableMetadataCache` | No       | 1.1.0         |
| `table-metadata-cache-capacity`       | The capacity of the table metadata cache.                                                                                                                                             | 1000                                                                | No       | 1.1.0         |
| `table-metadata-cache-expire-minutes` | The expiration time (in minutes) of the table metadata cache.                                                                                                                         | 60                                                                  | No       | 1.1.0         |

Gravitino provides the build-in `org.apache.gravitino.iceberg.common.cache.LocalTableMetadataCache` to store the cached data in the memory. You could also implement your custom table metadata cache by implementing the `org.apache.gravitino.iceberg.common.cache.TableMetadataCache` interface.

### Catalog Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for more details.

:::note
Sensitive catalog properties such as `s3-access-key-id`, `s3-secret-access-key`, `oss-access-key-id`, and `oss-secret-access-key` are hidden from the load catalog response since Gravitino 1.3.0. Use the [credential vending API](security/credential-vending.md) to retrieve them at runtime.
:::

## Schema

### Schema Capabilities

- doesn't support cascade drop schema.
- supports hierarchical (multi-level) schemas, mapping each level to an Iceberg namespace level. See [Hierarchical schema](#hierarchical-schema).

### Schema Properties

You could put properties except `comment`.

### Schema Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more details.

### Hierarchical schema

The Iceberg catalog supports a hierarchical (multi-level) schema, where a schema can be nested under
another schema, mapping each level to an Iceberg multi-level namespace.

A hierarchical schema name is a path whose levels are joined by the configured separator
`gravitino.schema.separator` (default `:`, see [Gravitino server configuration](./gravitino-server-config.md#schema-configuration)).
For example, with the default separator the name `a:b:c` denotes a schema `c` nested under `a:b`,
which in turn is nested under `a`. The separator is only used at the API boundary; Gravitino stores
the name internally using a physical separator that never collides with user input.

To create a hierarchical schema, just supply its full hierarchical name. Any missing ancestor schemas are
created automatically, so creating `a:b:c` also creates `a` and `a:b` if they don't already exist.
The following example creates the schema `a:b:c`:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "a:b:c",
  "comment": "a hierarchical schema",
  "properties": {}
}' http://localhost:8090/api/metalakes/metalake/catalogs/iceberg_catalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
// Assuming you have just created an Iceberg catalog named `iceberg_catalog`
Catalog catalog = gravitinoClient.loadCatalog("iceberg_catalog");

SupportsSchemas supportsSchemas = catalog.asSchemas();
// missing ancestors `a` and `a:b` are created automatically
Schema schema = supportsSchemas.createSchema("a:b:c", "a hierarchical schema", Collections.emptyMap());
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://127.0.0.1:8090", metalake_name="metalake")
catalog: Catalog = gravitino_client.load_catalog(name="iceberg_catalog")
# missing ancestors `a` and `a:b` are created automatically
catalog.as_schemas().create_schema(name="a:b:c", comment="a hierarchical schema", properties={})
```

</TabItem>
</Tabs>

To list the schemas directly under a parent schema, pass the parent schema name. Over REST this is
the optional `parentSchema` query parameter; in the clients it is an argument to the list-schemas
method. Given the schemas `a`, `a:b` and `a:b:c`, listing the children of `a:b` returns `[a:b:c]`.
When the parent is omitted, only the top-level schemas under the catalog are returned (the direct
children of the catalog root, e.g. `a`), not the nested ones.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
"http://localhost:8090/api/metalakes/metalake/catalogs/iceberg_catalog/schemas?parentSchema=a:b"
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("iceberg_catalog");
SupportsSchemas supportsSchemas = catalog.asSchemas();
String[] children = supportsSchemas.listSchemas("a:b");
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://127.0.0.1:8090", metalake_name="metalake")
catalog: Catalog = gravitino_client.load_catalog(name="iceberg_catalog")
children: List[str] = catalog.as_schemas().list_schemas(parent_schema="a:b")
```

</TabItem>
</Tabs>

## Table

### Table Capabilities

- Doesn't support column default value.

### Table Partitions

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

### Table Sort Orders

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

### Table Distributions

- Support `HashDistribution`, which distribute data by partition key.
- Support `RangeDistribution`, which distribute data by partition key or sort key for a SortOrder table.
- Doesn't support `EvenDistribution`.

:::info
If you doesn't specify distribution expressions, the table distribution will be adjusted to `RangeDistribution` for a sort order table, to `HashDistribution` for a partition table.
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
Apache Iceberg doesn't support Gravitino `Varchar` `Fixedchar` `Byte` `Short` `Union` type.
Meanwhile, the data types other than listed above are mapped to Gravitino **[External Type](./manage-relational-metadata-using-gravitino.md#external-type)** that represents an unresolvable data type since 0.6.0-incubating.
:::

### Table Properties

Pass [Iceberg table properties](https://iceberg.apache.org/docs/1.5.2/configuration/) to Gravitino when creating an Iceberg table.

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
| `comment`                 | The table comment; use the `comment` field in table meta instead.                     | (none)        | No       | Yes      | No        | 0.2.0         |
| `creator`                 | The table creator.                                                                    | (none)        | No       | Yes      | No        | 0.2.0         |
| `current-snapshot-id`     | The snapshot represents the current state of the table.                               | (none)        | No       | Yes      | No        | 0.2.0         |
| `cherry-pick-snapshot-id` | Selecting a specific snapshot in a merge operation.                                   | (none)        | No       | Yes      | No        | 0.2.0         |
| `sort-order`              | Iceberg table sort order; use `SortOrder` in table meta instead.                      | (none)        | No       | Yes      | No        | 0.2.0         |
| `identifier-fields`       | The identifier fields for defining the table.                                         | (none)        | No       | Yes      | No        | 0.2.0         |
| `write.distribution-mode` | Defines distribution of write data; use `distribution` in table meta instead.         | (none)        | No       | Yes      | No        | 0.2.0         |

### Table Indexes

- Doesn't support table indexes.

### Table Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for more details.

#### Alter Table Operations

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

## View

### View Capabilities

- Supports list, create, load, alter, and drop for views managed by the underlying Iceberg backend.
- Accepts any dialect name (e.g. `trino`, `spark`, `flink`, `hive`). No restriction on which dialects are used.
- Can preserve multiple SQL representations for the same logical view; the full set of representations round-trips through Gravitino.
- `defaultCatalog` and `defaultSchema` are stored and returned as-is by the backend.
- View support depends on the Iceberg catalog backend: REST and Hive backends generally support views; JDBC backend support is in continuous validation.

:::note
Rename cannot be combined with other changes in a single `alterView` call. Submit rename as a standalone request.
:::

### View Operations

Refer to [Manage view metadata using Gravitino](./manage-view-metadata-using-gravitino.md) for more details.

## HDFS Configuration

Place `core-site.xml` and `hdfs-site.xml` in the `catalogs/lakehouse-iceberg/conf` directory to automatically load as the default HDFS configuration.

:::info
Builds with Hadoop 2.10.x, there may be compatibility issues when accessing Hadoop 3.x clusters.
When writing to HDFS, the Gravitino Iceberg REST server can only operate as the specified HDFS user and doesn't support proxying to other HDFS users. See [How to access Apache Hadoop](gravitino-server-config.md) for more details.
:::
