---
title: "Apache Hive catalog"
slug: /apache-hive-catalog
date: 2023-12-10
keyword: hive catalog
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

## Introduction

Gravitino offers the capability to utilize [Apache Hive](https://hive.apache.org) as a catalog for metadata management.

### Requirements and limitations

* The Hive catalog requires a Hive Metastore Service (HMS), or a compatible implementation of the HMS, such as AWS Glue.
* Gravitino must have network access to the Hive metastore service using the Thrift protocol.

:::note
The Hive catalog is available for Apache Hive **2.x** only. Support for Apache Hive 3.x is under development.
:::

## Catalog

### Catalog capabilities

The Hive catalog supports creating, updating, and deleting databases and tables in the HMS.

### Catalog properties

| Property Name                            | Description                                                                                                                                                                                                       | Default Value | Required                     | Since Version |
|------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|------------------------------|---------------|
| `metastore.uris`                         | The Hive metastore service URIs, separate multiple addresses with commas. Such as `thrift://127.0.0.1:9083`                                                                                                       | (none)        | Yes                          | 0.2.0         |
| `client.pool-size`                       | The maximum number of Hive metastore clients in the pool for Gravitino.                                                                                                                                           | 1             | No                           | 0.2.0         |
| `gravitino.bypass.`                      | Property name with this prefix passed down to the underlying HMS client for use. Such as `gravitino.bypass.hive.metastore.failure.retries = 3` indicate 3 times of retries upon failure of Thrift metastore calls | (none)        | No                           | 0.2.0         |
| `client.pool-cache.eviction-interval-ms` | The cache pool eviction interval.                                                                                                                                                                                 | 300000        | No                           | 0.4.0         |
| `impersonation-enable`                   | Enable user impersonation for Hive catalog.                                                                                                                                                                       | false         | No                           | 0.4.0         |
| `kerberos.principal`                     | The Kerberos principal for the catalog. You should configure `gravitino.bypass.hadoop.security.authentication` and `gravitino.bypass.hive.metastore.sasl.enabled`if you want to use Kerberos.                     | (none)        | required if you use kerberos | 0.4.0         |
| `kerberos.keytab-uri`                    | The uri of key tab for the catalog. Now supported protocols are `https`, `http`, `ftp`, `file`.                                                                                                                   | (none)        | required if you use kerberos | 0.4.0         |
| `kerberos.check-interval-sec`            | The interval to check validness of the principal                                                                                                                                                                  | 60            | No                           | 0.4.0         |
| `kerberos.keytab-fetch-timeout-sec`      | The timeout to fetch key tab                                                                                                                                                                                      | 60            | No                           | 0.4.0         |

### Catalog operations

Refer to [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema capabilities

The Hive catalog supports creating, updating, and deleting databases in the HMS.

### Schema properties

Schema properties supply or set metadata for the underlying Hive database.
The following table lists predefined schema properties for the Hive database. Additionally, you can define your own key-value pair properties and transmit them to the underlying Hive database.

| Property name | Description                                                              | Default value                                                                           | Required | Since Version |
|---------------|--------------------------------------------------------------------------|-----------------------------------------------------------------------------------------|----------|---------------|
| `location`    | The directory for Hive database storage, such as `/user/hive/warehouse`. | HMS uses the value of `hive.metastore.warehouse.dir` in the `hive-site.xml` by default. | No       | 0.1.0         |

### Schema operations

see [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino.md#schema-operations).

## Table

### Table capabilities

- The Hive catalog supports creating, updating, and deleting tables in the HMS.
- Doesn't support column default value.

#### Table partitions

The Hive catalog supports [partitioned tables](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-PartitionedTables). Users can create partitioned tables in the Hive catalog with the specific partitioning attribute.
Although Gravitino supports several partitioning strategies, Apache Hive inherently only supports a single partitioning strategy (partitioned by column). Therefore, the Hive catalog only supports `Identity` partitioning.

:::caution
The `fieldName` specified in the partitioning attribute must be the name of a column defined in the table.
:::

#### Table sort orders and distributions

The Hive catalog supports [bucketed sorted tables](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-BucketedSortedTables). Users can create bucketed sorted tables in the Hive catalog with specific `distribution` and `sortOrders` attributes.
Although Gravitino supports several distribution strategies, Apache Hive inherently only supports a single distribution strategy (clustered by column). Therefore the Hive catalog only supports `Hash` distribution.

:::caution
The `fieldName` specified in the `distribution` and `sortOrders` attribute must be the name of a column defined in the table.
:::

#### Table column types

The Hive catalog supports all data types defined in the [Hive Language Manual](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types).
The following table lists the data types mapped from the Hive catalog to Gravitino.

| Hive Data Type              | Gravitino Data Type | Since Version |
|-----------------------------|---------------------|---------------|
| `boolean`                   | `boolean`           | 0.2.0         |
| `tinyint`                   | `byte`              | 0.2.0         |
| `smallint`                  | `short`             | 0.2.0         |
| `int`/`integer`             | `integer`           | 0.2.0         |
| `bigint`                    | `long`              | 0.2.0         |
| `float`                     | `float`             | 0.2.0         |
| `double`/`double precision` | `double`            | 0.2.0         |
| `decimal`                   | `decimal`           | 0.2.0         |
| `string`                    | `string`            | 0.2.0         |
| `char`                      | `char`              | 0.2.0         |
| `varchar`                   | `varchar`           | 0.2.0         |
| `timestamp`                 | `timestamp`         | 0.2.0         |
| `date`                      | `date`              | 0.2.0         |
| `interval_year_month`       | `interval_year`     | 0.2.0         |
| `interval_day_time`         | `interval_day`      | 0.2.0         |
| `binary`                    | `binary`            | 0.2.0         |
| `array`                     | `array`             | 0.2.0         |
| `map`                       | `map`               | 0.2.0         |
| `struct`                    | `struct`            | 0.2.0         |
| `uniontype`                 | `uniontype`         | 0.2.0         |

:::info
Since 0.5.0, the data types other than listed above are mapped to Gravitino **[Unparsed Type](./manage-metadata-using-gravitino.md#unparsed-type)** that represents an unresolvable data type from the Hive catalog.
:::

### Table properties

Table properties supply or set metadata for the underlying Hive tables.
The following table lists predefined table properties for a Hive table. Additionally, you can define your own key-value pair properties and transmit them to the underlying Hive database.

| Property Name      | Description                                                                                                                             | Default Value                                                                                                                                       | Required | Since version |
|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------|
| `location`         | The location for table storage, such as `/user/hive/warehouse/test_table`.                                                              | HMS uses the database location as the parent directory by default.                                                                                  | No       | 0.2.0         |
| `table-type`       | Type of the table. Valid values include `MANAGED_TABLE` and `EXTERNAL_TABLE`.                                                           | `MANAGED_TABLE`                                                                                                                                     | No       | 0.2.0         |
| `format`           | The table file format. Valid values include `TEXTFILE`, `SEQUENCEFILE`, `RCFILE`, `ORC`, `PARQUET`, `AVRO`, `JSON`, `CSV`, and `REGEX`. | `TEXTFILE`                                                                                                                                          | No       | 0.2.0         |
| `input-format`     | The input format class for the table, such as `org.apache.hadoop.hive.ql.io.orc.OrcInputFormat`.                                        | The property `format` sets the default value `org.apache.hadoop.mapred.TextInputFormat` and can change it to a different default.                   | No       | 0.2.0         |
| `output-format`    | The output format class for the table, such as `org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat`.                                      | The property `format` sets the default value `org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat` and can change it to a different default. | No       | 0.2.0         |
| `serde-lib`        | The serde library class for the table, such as `org.apache.hadoop.hive.ql.io.orc.OrcSerde`.                                             | The property `format` sets the default value `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe` and can change it to a different default.         | No       | 0.2.0         |
| `serde.parameter.` | The prefix of the serde parameter, such as `"serde.parameter.orc.create.index" = "true"`, indicating `ORC` serde lib to create row indexes | (none)                                                                                                                                              | No       | 0.2.0         |

Hive automatically adds and manages some reserved properties. Users aren't allowed to set these properties.

| Property Name           | Description                                       | Since Version |
|-------------------------|---------------------------------------------------|---------------|
| `comment`               | Used to store a table comment.                  | 0.2.0         |
| `numFiles`              | Used to store the number of files in the table.   | 0.2.0         |
| `totalSize`             | Used to store the total size of the table.        | 0.2.0         |
| `EXTERNAL`              | Indicates whether the table is external.          | 0.2.0         |
| `transient_lastDdlTime` | Used to store the last DDL time of the table.     | 0.2.0         |

### Table indexes

- Doesn't support table indexes.

### Table operations

Refer to [Manage Metadata Using Gravitino](./manage-metadata-using-gravitino.md#table-operations) for more details.

#### Alter operations

Gravitino has already defined a unified set of [metadata operation interfaces](./manage-metadata-using-gravitino.md#alter-a-table), and almost all [Hive Alter operations](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-AlterTable/Partition/Column) have corresponding table update requests which enable you to change the struct of an existing table.
The following table lists the mapping relationship between Hive Alter operations and Gravitino table update requests.

##### Alter table

| Hive Alter Operation                          | Gravitino Table Update Request | Since Version |
|-----------------------------------------------|--------------------------------|---------------|
| `Rename Table`                                | `Rename table`                 | 0.2.0         |
| `Alter Table Properties`                      | `Set a table property`         | 0.2.0         |
| `Alter Table Comment`                         | `Update comment`               | 0.2.0         |
| `Alter SerDe Properties`                      | `Set a table property`         | 0.2.0         |
| `Remove SerDe Properties`                     | `Remove a table property`      | 0.2.0         |
| `Alter Table Storage Properties`              | Unsupported                    | -             |
| `Alter Table Skewed or Stored as Directories` | Unsupported                    | -             |
| `Alter Table Constraints`                     | Unsupported                    | -             |

:::note
As Gravitino has a separate interface for updating the comment of a table, the Hive catalog sets `comment` as a reserved property for the table, preventing users from setting the comment property. Apache Hive can modify the comment property of the table.
:::

##### Alter column

| Hive Alter Operation     | Gravitino Table Update Request    | Since Version |
|--------------------------|-----------------------------------|---------------|
| `Change Column Name`     | `Rename a column`                 | 0.2.0         |
| `Change Column Type`     | `Update the type of a column`     | 0.2.0         |
| `Change Column Position` | `Update the position of a column` | 0.2.0         |
| `Change Column Comment`  | `Update the column comment`       | 0.2.0         |

##### Alter partition

:::note
Support for altering partitions is under development.
:::
