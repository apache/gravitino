---
title: "Apache Hive catalog"
slug: /apache-hive-catalog
date: 2023-12-10
keyword: hive catalog
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Apache Gravitino can use [Apache Hive](https://hive.apache.org) as a catalog for metadata management.

### Requirements and limitations

* The Hive catalog requires a Hive Metastore Service (HMS),
  or a compatible implementation such as AWS Glue.
* Gravitino must have network access to the Hive metastore service using the Thrift protocol.

:::note
Although the Hive catalog uses the Hive2 metastore client,
it can be compatible with the Hive3 metastore service
because the HMS APIs used are still available in Hive3.
If there is any compatibility issue, please file an [issue](https://github.com/apache/gravitino/issues).
:::

## Catalog

### Catalog capabilities

The Hive catalog supports creating, updating, and deleting databases and tables in the HMS.

### Catalog properties

Besides the [common catalog properties](../../../admin/server-config.md#gravitino-catalog-properties-configuration),
the Hive catalog has the following properties:

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
  <td><tt>metastore.uris</tt></td>
  <td>
    The Hive metastore service URIs.
    Multiple addresses can be specified using a comma-separated list.
    E.g. `thrift://127.0.0.1:9083`.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>client.pool-size</tt></td>
  <td>The maximum number of Hive metastore clients in the pool for Gravitino.</td>
  <td>1</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.bypass.</tt></td>
  <td>
    Property name with this prefix passed down to the underlying HMS client for use.
    Such as `gravitino.bypass.hive.metastore.failure.retries = 3`
    means 3 times of retries upon failure of Thrift metastore calls.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>client.pool-cache.eviction-interval-ms</tt></td>
  <td>The cache pool eviction interval in milliseconds.</td>
  <td>`300000`</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>impersonation-enable</tt></td>
  <td>Enable user impersonation for Hive catalog.</td>
  <td>false</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>kerberos.principal</tt></td>
  <td>
    The Kerberos principal for the catalog.
    You should configure `gravitino.bypass.hadoop.security.authentication`,
    `gravitino.bypass.hive.metastore.kerberos.principal`, and
    `gravitino.bypass.hive.metastore.sasl.enabled`
    if you want to use Kerberos.

    This property is required if Kerberos is used.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>kerberos.keytab-uri</tt></td>
  <td>
    The URI of key tab for the catalog.
    Currently supported protocols are `https`, `http`, `ftp`, `file`.

    This property is required if Kerberos is used.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>kerberos.check-interval-sec</tt></td>
  <td>The interval seconds for checking the validity of the principal.</td>
  <td>`60`</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>kerberos.keytab-fetch-timeout-sec</tt></td>
  <td>The timeout in seconds when fetching the keytab.</td>
  <td>`60`</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>list-all-tables</tt></td>
  <td>
    Lists all tables in a database, including non-Hive tables, such as Iceberg, Hudi, etc.
  </td>
  <td>false</td>
  <td>No</td>
  <td>`0.5.1`</td>
</tr>
</tbody>
</table>


:::note

For `list-all-tables=false`, the Hive catalog will filter out:

- Iceberg tables by table property `table_type=ICEBERG`
- Paimon tables by table property `table_type=PAIMON`
- Hudi tables by table property `provider=hudi`
:::

When you use the Gravitino with Trino, you can pass the Trino Hive connector configuration
using prefix `trino.bypass.`.
For example, using `trino.bypass.hive.config.resources` to pass the `hive.config.resources`
to the Gravitino Hive catalog in Trino runtime.

When you use the Gravitino with Spark, you can pass the Spark Hive connector configuration
using prefix `spark.bypass.`.
For example, using `spark.bypass.hive.exec.dynamic.partition.mode`
to pass the `hive.exec.dynamic.partition.mode` to the Spark Hive connector in Spark runtime.

When you use the Gravitino authorization Hive with Apache Ranger.
For more details, you can check
[Authorization Hive with Ranger properties](../../../security/authorization-pushdown.md#authorization-hive-with-ranger-properties).

### Catalog operations

Refer to [managing relational metadata](../../../metadata/relational.md#catalog-operations).

## Schema

### Schema capabilities

The Hive catalog supports creating, updating, and deleting databases in the HMS.

### Schema properties

Schema properties supply or set metadata for the underlying Hive database.
The following table lists predefined schema properties for the Hive database.
Additionally, you can define your own key-value pair properties
and transmit them to the underlying Hive database.

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
  <td><tt>location</tt></td>
  <td>
    The directory for Hive database storage, such as `/user/hive/warehouse`.
    HMS uses the value of `hive.metastore.warehouse.dir` in the `hive-site.xml` by default.
  </td>
  <td>`hive.metastore.warehouse.dir` from Hive.</td>
  <td>No</td>
  <td>`0.1.0`</td>
</tr>
</tbody>
</table>

### Schema operations

See [managing relational metadata](../../../metadata/relational.md#schema-operations).

## Table

### Table capabilities

- The Hive catalog supports creating, updating, and deleting tables in the HMS.
- Column default value is not supported yet.

### Table partitioning

The Hive catalog supports [partitioned tables](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-PartitionedTables).
Users can create partitioned tables in the Hive catalog
with the specific partitioning attributes.
Although Gravitino supports several partitioning strategies.
Apache Hive inherently only supports a single partitioning strategy (partitioned by column).
Therefore, the Hive catalog only supports `identity` partitioning.

:::caution
The `fieldName` specified in the partitioning attribute must be the name of a column defined in the table.
:::

### Table sort orders and distributions

The Hive catalog supports [bucketed sorted tables](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-BucketedSortedTables).
Users can create bucketed sorted tables in the Hive catalog
with specific `distribution` and `sortOrders` attributes.
Although Gravitino supports several distribution strategies,
Apache Hive inherently only supports a single distribution strategy (clustered by column).
Therefore the Hive catalog only supports `Hash` distribution.

:::caution
The `fieldName` specified in the `distribution` and `sortOrders` attribute must be the name of a column defined in the table.
:::

### Table column types

The Hive catalog supports all data types defined in the
[Hive Language Manual](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types).
The following table lists the data types mapped from the Hive catalog to Gravitino.

| Hive Data Type              | Gravitino Data Type | Since Version |
|-----------------------------|---------------------|---------------|
| `array`                     | `list`              | `0.2.0`       |
| `bigint`                    | `long`              | `0.2.0`       |
| `binary`                    | `binary`            | `0.2.0`       |
| `boolean`                   | `boolean`           | `0.2.0`       |
| `char`                      | `char`              | `0.2.0`       |
| `date`                      | `date`              | `0.2.0`       |
| `decimal`                   | `decimal`           | `0.2.0`       |
| `double`/`double precision` | `double`            | `0.2.0`       |
| `float`                     | `float`             | `0.2.0`       |
| `int`/`integer`             | `integer`           | `0.2.0`       |
| `interval_year_month`       | `interval_year`     | `0.2.0`       |
| `interval_day_time`         | `interval_day`      | `0.2.0`       |
| `map`                       | `map`               | `0.2.0`       |
| `smallint`                  | `short`             | `0.2.0`       |
| `string`                    | `string`            | `0.2.0`       |
| `struct`                    | `struct`            | `0.2.0`       |
| `tinyint`                   | `byte`              | `0.2.0`       |
| `timestamp`                 | `timestamp`         | `0.2.0`       |
| `uniontype`                 | `union`             | `0.2.0`       |
| `varchar`                   | `varchar`           | `0.2.0`       |

:::info
Since 0.6.0-incubating, the data types other than listed above are mapped to Gravitino
**[External Type](../../../metadata/relational.md#external-type)**
that represents an unresolvable data type from the Hive catalog.
:::

### Table properties

Table properties supply or set metadata for the underlying Hive tables.
The following table lists predefined table properties for a Hive table.
Additionally, you can define your own key-value pair properties
and transmit them to the underlying Hive database.

:::note
- *Reserved*: Fields that cannot be passed to the Gravitino server.
- *Immutable*: Fields that cannot be modified once set.
:::

<table>
<thead>
<tr>
  <th>Property name</th>
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
  <td>
    The location for table storage, such as `/user/hive/warehouse/test_table`.
    HMS uses the database location as the parent directory by default.
  </td>
  <td>Database location of the partent directory</td>
  <td>No</td>
  <td>No</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>table-type</tt></td>
  <td>
    Type of the table.
    Valid values include `MANAGED_TABLE` and `EXTERNAL_TABLE`.
  </td>
  <td>`MANAGED_TABLE`</td>
  <td>No</td>
  <td>No</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>format</tt></td>
  <td>
    The table file format.
    Valid values include `TEXTFILE`, `SEQUENCEFILE`, `RCFILE`, `ORC`,
    `PARQUET`, `AVRO`, `JSON`, `CSV`, and `REGEX`.
  </td>
  <td>`TEXTFILE`</td>
  <td>No</td>
  <td>No</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>input-format</tt></td>
  <td>
    The input format class for the table,
    such as `org.apache.hadoop.hive.ql.io.orc.OrcInputFormat`.
    The property `format` sets the default value `org.apache.hadoop.mapred.TextInputFormat`
    and can change it to a different default.
  </td>
  <td>(see description)</td>
  <td>No</td>
  <td>No</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>output-format</tt></td>
  <td>
    The output format class for the table,
    such as `org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat`.
    The property `format` sets the default value
    `org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat`
    and can change it to a different default.
  </td>
  <td>(see description)</td>
  <td>No</td>
  <td>No</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>serde-lib</tt></td>
  <td>
    The serde library class for the table, such as
    `org.apache.hadoop.hive.ql.io.orc.OrcSerde`.
    The property `format` sets the default value
    `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe`
    and can change it to a different default.
  </td>
  <td>(see description)</td>
  <td>No</td>
  <td>No</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>serde.parameter.</tt></td>
  <td>
    The prefix of the serde parameter, such as
    `"serde.parameter.orc.create.index" = "true"`,
    indicating `ORC` serde lib to create row indexes
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>No</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>serde-name</tt></td>
  <td>The name of the serde</td>
  <td>Table name by default.</td>
  <td>No</td>
  <td>No</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>comment</tt></td>
  <td>Used to store a table comment.</td>
  <td>(none)</td>
  <td>No</td>
  <td>Yes</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>numFiles</tt></td>
  <td>The number of files in the table.</td>
  <td>(none)</td>
  <td>No</td>
  <td>Yes</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>totalSize</tt></td>
  <td>The total size of the table.</td>
  <td>(none)</td>
  <td>No</td>
  <td>Yes</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>EXTERNAL</tt></td>
  <td>Flag indicating whether the table is external.</td>
  <td>(none)</td>
  <td>No</td>
  <td>Yes</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>transient_lastDdlTime</tt></td>
  <td>The last DDL time of the table.</td>
  <td>(none)</td>
  <td>No</td>
  <td>Yes</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
</tbody>
</table>

### Table indexes

- Table indexing is not supported.

### Table operations

Refer to [managing relational metadata](../../../metadata/relational.md#table-operations).

#### Alter operations

Gravitino has already defined a unified set of
[metadata operation interfaces](../../../metadata/relational.md#alter-a-table).
Almost all [Hive Alter operations](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-AlterTable/Partition/Column)
have corresponding table update requests which enable you
to change the struct of an existing table.
The following table lists the mapping relationship
between Hive Alter operations and Gravitino table update requests.

##### Alter table

<table>
<thead>
<tr>
  <th>Hive alter operation</th>
  <th>Gravitino table update request</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td>Rename table</td>
  <td>Rename table</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td>Alter table properties</td>
  <td>Set a table property</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td>Alter table comment</td>
  <td>Update comment</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td>Alter SerDe properties</td>
  <td>Set a table property</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td>Remove SerDe properties</td>
  <td>Remove a table property</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td>Alter table storage properties</td>
  <td>Unsupported</td>
  <td>-</td>
</tr>
<tr>
  <td>Alter Table Skewed or Stored as Directories</td>
  <td>Unsupported</td>
  <td>-</td>
</tr>
<tr>
  <td>Alter Table Constraints</td>
  <td>Unsupported</td>
  <td>-</td>
</tr>
</tbody>
</table>

:::note
As Gravitino has a separate interface for updating the comment of a table,
the Hive catalog sets `comment` as a reserved property for the table,
preventing users from setting the comment property.
Apache Hive can modify the comment property of the table.
:::

##### Alter column

<table>
<thead>
<tr>
  <th>Hive alter operation</th>
  <th>Gravitino table update request</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td>Change column name</td>
  <td>Rename a column</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td>Change column type</td>
  <td>Update the type of a column</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td>Change column position</td>
  <td>Update the position of a column</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td>Change column cmment</td>
  <td>Update the column comment</td>
  <td>`0.2.0`</td>
</tr>
</tbody>
</table>

##### Alter partition

:::note
Support for altering partitions is under development.
:::

## Hive catalog with S3 storage

To create a Hive catalog with S3 storage, you can refer to the
[Hive catalog with S3](./cloud-storage.md) documentation.
No special configurations are required for the Hive catalog to work with S3 storage.
The only difference is the storage location of the files, which is in S3.
You can use `location` to specify the S3 path for the database or table.

