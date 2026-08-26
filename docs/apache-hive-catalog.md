---
title: "Hive Catalog"
slug: "/apache-hive-catalog"
date: 2023-12-10
keyword: "hive catalog"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Apache Gravitino offers the capability to utilize [Apache Hive](https://hive.apache.org) as a catalog for metadata management.

### Requirements and Limitations

* The Hive catalog requires a Hive Metastore Service (HMS), or a compatible implementation of the HMS, such as AWS Glue.
* Gravitino must have network access to the Hive metastore service using the Thrift protocol.

:::note
The Hive catalog supports HMS versions 2.x and 3.x. it can automatically detect the HMS version.
:::

## Catalog

### Catalog Capabilities

The Hive catalog supports creating, updating, and deleting databases and tables in the HMS.

### Catalog Properties

Besides the [common catalog properties](./gravitino-server-config.md#catalog-properties-configuration), the Hive catalog has the following properties:

| Property Name                            | Description                                                                                                                                                                                                                                         | Default Value | Required                     |
|------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|------------------------------|
| `metastore.uris`                         | The Hive metastore service URIs, separate multiple addresses with commas. Such as `thrift://127.0.0.1:9083`                                                                                                                                         | (none)        | Yes                          |
| `client.pool-size`                       | The maximum number of Hive metastore clients in the pool for Gravitino.                                                                                                                                                                             | 1             | No                           |
| `gravitino.bypass.`                      | Property name with this prefix passed down to the underlying HMS client for use. Such as `gravitino.bypass.hive.metastore.failure.retries = 3` indicate 3 times of retries upon failure of Thrift metastore calls                                   | (none)        | No                           |
| `client.pool-cache.eviction-interval-ms` | The cache pool eviction interval.                                                                                                                                                                                                                   | 300000        | No                           |
| `impersonation-enable`                   | Enable user impersonation for Hive catalog.                                                                                                                                                                                                         | false         | No                           |
| `kerberos.principal`                     | The Kerberos principal for the catalog. You should configure `gravitino.bypass.hadoop.security.authentication`, `gravitino.bypass.hive.metastore.kerberos.principal` and `gravitino.bypass.hive.metastore.sasl.enabled`if you want to use Kerberos. | (none)        | required if you use kerberos |
| `kerberos.keytab-uri`                    | The uri of key tab for the catalog. Now supported protocols are `https`, `http`, `ftp`, `file`.                                                                                                                                                     | (none)        | required if you use kerberos |
| `kerberos.check-interval-sec`            | The interval to check validity of the principal                                                                                                                                                                                                     | 60            | No                           |
| `kerberos.keytab-fetch-timeout-sec`      | The timeout to fetch key tab                                                                                                                                                                                                                        | 60            | No                           |
| `list-all-tables`                        | Whether to list all tables in a database, including non-Hive tables such as Iceberg, Paimon, and Hudi. When false, non-Hive tables are filtered out on a best-effort basis; see the note below for known limitations.                               | false         | No                           |
| `default.catalog`                        | The default catalog name for the Hive3 metastore backend; this configuration is ignored when using a Hive2 metastore.                                                                                                                               | hive          | No                           |

:::note
When `list-all-tables=false`, the Hive catalog removes the following on a best-effort basis:
- Iceberg tables (table property `table_type=ICEBERG`)
- Paimon tables (table property `table_type=PAIMON`)
- Hudi tables (table property `provider=hudi`), together with their `_ro` and `_rt` siblings

**Known limitation.** Filtering is performed server-side via the Hive Metastore, which only
supports exact-key lookups on dot-free property keys. Hudi tables registered directly by Spark
(e.g. via `saveAsTable`) typically only set `spark.sql.sources.provider=hudi` without also
setting `provider=hudi`, so they cannot be filtered out and will appear in the listing.

**Workaround.** Add a dot-free `provider=hudi` property to such tables so the server-side
filter can match them. Either after creation:

```sql
ALTER TABLE <db>.<table> SET TBLPROPERTIES ('provider'='hudi');
```

or at write time via Hudi's Hive sync option:

```scala
df.write.format("hudi")
  .option("hoodie.datasource.hive_sync.table_properties", "provider=hudi")
  .saveAsTable("<db>.<table>")
```

The corresponding `_ro` / `_rt` siblings are removed automatically based on the base table name.
:::

When using Gravitino with Trino, pass the Trino Hive connector configuration using the `trino.bypass.` prefix. For example, using `trino.bypass.hive.config.resources` to pass the `hive.config.resources` to the Gravitino Hive catalog in Trino runtime.

When using Gravitino with Spark, pass the Spark Hive connector configuration using the `spark.bypass.` prefix. For example, using `spark.bypass.hive.exec.dynamic.partition.mode` to pass the `hive.exec.dynamic.partition.mode` to the Spark Hive connector in Spark runtime.

When using Gravitino authorization for Hive with Apache Ranger, see the [Authorization Hive with Ranger properties](security/authorization-pushdown.md#configure-the-ranger-hadoop-sql-plugin)

### Catalog Operations

Refer to [Manage Catalogs and Schemas](./manage-catalogs-and-schemas.md#catalog-operations) for more details.

:::note
Sensitive catalog properties such as credential-vending keys are hidden from the default load catalog response. Retrieve secret-manager-backed properties (including keys that overlap with credential vending) via `getSecrets` / `GET .../objects/{type}/{fullName}/secrets`. The [credential vending API](security/credential-vending.md) remains available for typed credential delivery.
:::

## Schema

### Schema Capabilities

The Hive catalog supports creating, updating, and deleting databases in the HMS.

### Schema Properties

Schema properties supply or set metadata for the underlying Hive database.
The following table lists predefined schema properties for the Hive database. Additionally, you can define your own key-value pair properties and transmit them to the underlying Hive database.

| Property name | Description                                                              | Default value                                                                           | Required |
|---------------|--------------------------------------------------------------------------|-----------------------------------------------------------------------------------------|----------|
| `location`    | The directory for Hive database storage, such as `/user/hive/warehouse`. | HMS uses the value of `hive.metastore.warehouse.dir` in the `hive-site.xml` by default. | No       |

### Schema Operations

see [Manage Catalogs and Schemas](./manage-catalogs-and-schemas.md#schema-operations).

## Table

### Table Capabilities

- The Hive catalog supports creating, updating, and deleting tables in the HMS.
- Doesn't support column default value.

### Table Partitioning

The Hive catalog supports [partitioned tables](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-PartitionedTables). Users can create partitioned tables in the Hive catalog with the specific partitioning attribute.
Although Gravitino supports several partitioning strategies, Apache Hive inherently only supports a single partitioning strategy (partitioned by column). Therefore, the Hive catalog only supports `Identity` partitioning.

:::caution
The `fieldName` specified in the partitioning attribute must be the name of a column defined in the table.
:::

### Table Sort Orders and Distributions

The Hive catalog supports [bucketed sorted tables](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-BucketedSortedTables). Users can create bucketed sorted tables in the Hive catalog with specific `distribution` and `sortOrders` attributes.
Although Gravitino supports several distribution strategies, Apache Hive inherently only supports a single distribution strategy (clustered by column). Therefore the Hive catalog only supports `Hash` distribution.

:::caution
The `fieldName` specified in the `distribution` and `sortOrders` attribute must be the name of a column defined in the table.
:::

### Table Column Types

The Hive catalog supports all data types defined in the [Hive Language Manual](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types).
The following table lists the data types mapped from the Hive catalog to Gravitino.

| Hive Data Type              | Gravitino Data Type |
|-----------------------------|---------------------|
| `boolean`                   | `boolean`           |
| `tinyint`                   | `byte`              |
| `smallint`                  | `short`             |
| `int`/`integer`             | `integer`           |
| `bigint`                    | `long`              |
| `float`                     | `float`             |
| `double`/`double precision` | `double`            |
| `decimal`                   | `decimal`           |
| `string`                    | `string`            |
| `char`                      | `char`              |
| `varchar`                   | `varchar`           |
| `timestamp`                 | `timestamp`         |
| `date`                      | `date`              |
| `interval_year_month`       | `interval_year`     |
| `interval_day_time`         | `interval_day`      |
| `binary`                    | `binary`            |
| `array`                     | `list`              |
| `map`                       | `map`               |
| `struct`                    | `struct`            |
| `uniontype`                 | `union`             |

:::info
1. The data types other than listed above are mapped to Gravitino **[External Type](./tables-and-views.md#external-type)** that represents an unresolvable data type from the Hive catalog.
2. Using the `struct` data type with field comments will throw an error, as it does not work for Hive tables (see [HIVE-26593](https://issues.apache.org/jira/browse/HIVE-26593)).
:::

### Table Properties

Table properties supply or set metadata for the underlying Hive tables.
The following table lists predefined table properties for a Hive table. Additionally, you can define your own key-value pair properties and transmit them to the underlying Hive database.

:::note
**Reserved**: Fields that cannot be passed to the Gravitino server.

**Immutable**: Fields that cannot be modified once set.
:::

| Property Name           | Description                                                                                                                                | Default Value                                                                                                                                       | Required | Reserved | Immutable |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|----------|----------|-----------|
| `location`              | The location for table storage, such as `/user/hive/warehouse/test_table`.                                                                 | HMS uses the database location as the parent directory by default.                                                                                  | No       | No       | Yes       |
| `table-type`            | Type of the table. Valid values include `MANAGED_TABLE` and `EXTERNAL_TABLE`.                                                              | `MANAGED_TABLE`                                                                                                                                     | No       | No       | Yes       |
| `format`                | The table file format. Valid values include `TEXTFILE`, `SEQUENCEFILE`, `RCFILE`, `ORC`, `PARQUET`, `AVRO`, `JSON`, `CSV`, and `REGEX`.    | `TEXTFILE`                                                                                                                                          | No       | No       | Yes       |
| `input-format`          | The input format class for the table, such as `org.apache.hadoop.hive.ql.io.orc.OrcInputFormat`.                                           | The property `format` sets the default value `org.apache.hadoop.mapred.TextInputFormat` and can change it to a different default.                   | No       | No       | Yes       |
| `output-format`         | The output format class for the table, such as `org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat`.                                         | The property `format` sets the default value `org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat` and can change it to a different default. | No       | No       | Yes       |
| `serde-lib`             | The serde library class for the table, such as `org.apache.hadoop.hive.ql.io.orc.OrcSerde`.                                                | The property `format` sets the default value `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe` and can change it to a different default.         | No       | No       | Yes       |
| `serde.parameter.`      | The prefix of the serde parameter, such as `"serde.parameter.orc.create.index" = "true"`, indicating `ORC` serde lib to create row indexes | (none)                                                                                                                                              | No       | No       | Yes       |
| `serde-name`            | The name of the serde                                                                                                                      | Table name by default.                                                                                                                              | No       | No       | Yes       |
| `comment`               | Used to store a table comment.                                                                                                             | (none)                                                                                                                                              | No       | Yes      | No        |
| `numFiles`              | Used to store the number of files in the table.                                                                                            | (none)                                                                                                                                              | No       | Yes      | No        |
| `totalSize`             | Used to store the total size of the table.                                                                                                 | (none)                                                                                                                                              | No       | Yes      | No        |
| `EXTERNAL`              | Indicates whether the table is external.                                                                                                   | (none)                                                                                                                                              | No       | Yes      | No        |
| `transient_lastDdlTime` | Used to store the last DDL time of the table.                                                                                              | (none)                                                                                                                                              | No       | Yes      | No        |

### Table Indexes

- Doesn't support table indexes.

### Table Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for more details.

#### Alter Operations

Gravitino has already defined a unified set of [metadata operation interfaces](./manage-relational-metadata-using-gravitino.md#alter-a-table), and almost all [Hive Alter operations](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-AlterTable/Partition/Column) have corresponding table update requests which enable you to change the struct of an existing table.
The following table lists the mapping relationship between Hive Alter operations and Gravitino table update requests.

##### Alter table

| Hive Alter Operation                          | Gravitino Table Update Request |
|-----------------------------------------------|--------------------------------|
| `Rename Table`                                | `Rename table`                 |
| `Alter Table Properties`                      | `Set a table property`         |
| `Alter Table Comment`                         | `Update comment`               |
| `Alter SerDe Properties`                      | `Set a table property`         |
| `Remove SerDe Properties`                     | `Remove a table property`      |
| `Alter Table Storage Properties`              | Unsupported                    |
| `Alter Table Skewed or Stored as Directories` | Unsupported                    |
| `Alter Table Constraints`                     | Unsupported                    |

:::note
As Gravitino has a separate interface for updating the comment of a table, the Hive catalog sets `comment` as a reserved property for the table, preventing users from setting the comment property. Apache Hive can modify the comment property of the table.
:::

##### Alter column

| Hive Alter Operation     | Gravitino Table Update Request    |
|--------------------------|-----------------------------------|
| `Change Column Name`     | `Rename a column`                 |
| `Change Column Type`     | `Update the type of a column`     |
| `Change Column Position` | `Update the position of a column` |
| `Change Column Comment`  | `Update the column comment`       |

##### Alter partition

:::note
Support for altering partitions is under development.
:::

## View

### View Capabilities

- Supports list, create, load, alter, and drop for views stored in the Hive Metastore Service as `VIRTUAL_VIEW`.
- Each view must contain exactly one SQL representation.
- Supports creating views with the `hive`, `trino`, `flink`, or `spark` dialect.
- When loading an existing HMS view, Gravitino automatically detects whether the view uses the `hive`, `trino`, `flink`, or `spark` dialect.
- For the `hive` and `flink` dialects, `defaultCatalog` and `defaultSchema` must be `null`.
- For the `trino` dialect, `defaultSchema` requires `defaultCatalog` to also be set (a schema without a catalog cannot be represented).
- The `trino` dialect requires at least one output column, and is stored using Trino's own native "Presto View" Hive Metastore encoding, so a view created through Gravitino is interoperable with a native Trino/Presto Hive connector pointed at the same Hive Metastore, and vice versa. The HMS `presto_view` property this relies on is reserved and managed internally based on the view's dialect; it cannot be set or removed directly. Gravitino's view model cannot represent a native Trino view's owner, `runAsInvoker`, or SQL path, so replacing an existing native view that has a non-default value for any of them is rejected rather than silently discarding it.
- The `flink` dialect requires at least one view property with the prefix `flink.` to be set. The Flink connector automatically sets `flink.schema.num-columns`; when using the REST API directly, set at least one `flink.*` property explicitly.
- The `spark` dialect requires the view property `spark.sql.create.version` to be set; without it the view round-trips as the `hive` dialect on reload.

### View Operations

Refer to [Manage view metadata using Gravitino](./manage-view-metadata-using-gravitino.md) for more details.

## Hive Catalog with S3 Storage

To create a Hive catalog with S3 storage, you can refer to the [Hive catalog with S3](./hive-catalog-with-cloud-storage.md) documentation. No special configurations are required for the Hive catalog to work with S3 storage.
The only difference is the storage location of the files, which is in S3. Use `location` to specify the S3 path for the database or table.
