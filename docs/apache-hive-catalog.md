---
title: "Apache Hive catalog of Gravitino"
slug: /apache-hive-catalog
date: 2023-12-10
keyword: hive catalog
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

## Introduction

Gravitino offers the capability to utilize [Apache Hive](https://hive.apache.org) as a catalog for metadata management.

## Requirements

* The Hive catalog requires a Hive Metastore Service (HMS), or a compatible implementation of the HMS, such as AWS Glue.
* The Gravitino must have network access to the Hive metastore service with the Thrift protocol.
* Support is available for Apache Hive 2.x.

## Properties

### Catalog properties

| Property name       | Description                                                                                                                  | example value                                                                                                            | Since version |
|---------------------|------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|---------------|
| `metastore.uris`    | This is a required configuration, and it should be the Hive metastore service URIs, separate multiple addresses with commas. | `thrift://127.0.0.1:9083`                                                                                                | 0.2.0         |
| `client.pool-size`  | The maximum number of Hive metastore clients in the pool for Gravitino. 1 by default.                                        | 1                                                                                                                        | 0.2.0         |
| `gravitino.bypass.` | Property name with this prefix passed down to the underlying HMS client for use. Empty by default.                           | `gravitino.bypass.hive.metastore.failure.retries = 3` indicate 3 times of retries upon failure of Thrift metastore calls | 0.2.0         |

### Schema properties

Schema properties supply or set metadata for the underlying Hive database.
The following table lists predefined schema properties for the Hive database. In addition, you can also define your own key-value pair properties, which can also be transmitted to the underlying Hive database.

| Property name       | Description                                                                                                                                  | example value                            | Since version |
|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------|---------------|
| `location`          | The directory for Hive database storage. Not required, HMS uses the value of `hive.metastore.warehouse.dir` in the hive-site.xml by default. | `/user/hive/warehouse`                   | 0.1.0         |

### Table properties

Table properties supply or set metadata for underlying Hive tables.
The following table lists predefined table properties for the Hive table. In addition, you can also define your own key-value pair properties, which can also be transmitted to the underlying Hive table.

| Property name      | Description                                                                                                                                                                                | example value                                                                                | Since version |
|--------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------|---------------|
| `location`         | The location for table storage. Not required, HMS will use the database location as the parent directory by default.                                                                       | `/user/hive/warehouse/test_table`                                                            | 0.2.0         |
| `table-type`       | Type of the table. Valid values include `MANAGED_TABLE` and `EXTERNAL_TABLE`. `MANAGED_TABLE` by default.                                                                                  | `MANAGED_TABLE`                                                                              | 0.2.0         |
| `format`           | The table file format. Valid values include `TEXTFILE`, `SEQUENCEFILE`, `RCFILE`, `ORC`, `PARQUET`, `AVRO`, `JSON`, `CSV`, and `REGEX`. `TEXTFILE` by default.                             | `ORC`                                                                                        | 0.2.0         |
| `input-format`     | The input format class for the table. The property `format` sets the default value `org.apache.hadoop.mapred.TextInputFormat` and can change it to a different default.                    | `org.apache.hadoop.hive.ql.io.orc.OrcInputFormat`                                            | 0.2.0         |
| `output-format`    | The output format class for the table. The property `format` sets the default value `org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat` and can change it to a different default. | `org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat`                                           | 0.2.0         |
| `serde-lib`        | The serde library class for the table. The property `format` sets the default value `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe` and can change it to a different default.         | `org.apache.hadoop.hive.ql.io.orc.OrcSerde`                                                  | 0.2.0         |
| `serde.parameter.` | The prefix of the serde parameter, empty by default.                                                                                                                                       | `"serde.parameter.orc.create.index" = "true"` indicate `ORC` serde lib to create row indexes | 0.2.0         |

Some properties are reserved, which are automatically added and managed by Hive. Users are not allowed to set these properties.

| Property name           | Description                                       | Since version |
|-------------------------|---------------------------------------------------|---------------|
| `comment`               | Used to store the table comment.                  | 0.2.0         |
| `numFiles`              | Used to store the number of files in the table.   | 0.2.0         |
| `totalSize`             | Used to store the total size of the table.        | 0.2.0         |
| `EXTERNAL`              | Indicates whether the table is an external table. | 0.2.0         |
| `transient_lastDdlTime` | Used to store the last DDL time of the table.     | 0.2.0         |
