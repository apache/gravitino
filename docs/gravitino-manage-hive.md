---
title: "How to use Gravitino to manage Hive metadata"
date: 2023-10-20
license: "Copyright 2023 Datastrato.
This software is licensed under the Apache License version 2."
---
## Using Hive as a Catalog in Gravitino

Gravitino offers the functionality to utilize Hive as a catalog for metadata management. This guide will lead you through the process of creating a catalog using Hive in Gravitino.

### Requirements:

* The Hive catalog requires a Hive Metastore Service(HMS), or a compatible implementation of the HMS, such as AWS Glue.
* The Gravitino must have network access to the Hive metastore service with the Thrift protocol.
* Apache Hive 2.x is supported.
* Before you create a Hive catalog, make sure you have already created a Metalake. If you haven't done so, please follow the Metalake creation steps.

## Creating a Hive Catalog

To create a Hive catalog, use the following steps:

Submit a catalog JSON example to the Gravitino server using the URL format:

```shell
http://{GravitinoServerHost}:{GravitinoServerPort}/api/metalakes/{Your_metalake_name}/catalogs
```

Example JSON:

```json
   {
       "name": "test_hive_catalog",
       "comment": "my test Hive catalog",
       "type": "RELATIONAL",
       "provider": "hive",
       "properties": {
           "metastore.uris": "thrift://127.0.0.1:9083"
       }
   }
```

* `name`: The name of the Hive catalog to be created.
* `comment`:  Optional, user custom catalog comment.
* `provider`: Must set this to "hive" in order to use Hive as the catalog provider.
* `type`: Must set this to "RELATIONAL" because Hive has a relational data structure, like `db.table`.
* `properties`: The properties of the Hive catalog. More properties information see the following catalog properties table.

### catalog properties

| Property name       | Description                                                                                                                  | example value                                                                                                            | Since version |
|---------------------|------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|---------------|
| `metastore.uris`    | This is a required configuration, and it should be the Hive metastore service URIs, separate multiple addresses with commas. | `thrift://127.0.0.1:9083`                                                                                                | 0.2.0         |
| `client.pool-size`  | The maximum number of Hive metastore clients in the pool for Gravitino. 1 by default value.                                  | 1                                                                                                                        | 0.2.0         |
| `gravitino.bypass.` | Property name with this prefix will be passed down to the underlying HMS client for use. Empty by default value.             | `gravitino.bypass.hive.metastore.failure.retries = 3` indicate 3 times of retries upon failure of Thrift metastore calls | 0.2.0         |

## Creating a Hive Schema

After the catalog is created, you can submit a schema JSON example to the Gravitino server using the URL format:

```shell
http://{GravitinoServerHost}:{GravitinoServerPort}/api/metalakes/{metalake}/catalogs/{catalog}/schemas
```

Example JSON:

```json
{
    "name": "test_schema",
    "comment": "my test schema",
    "properties": {
        "location": "/user/hive/warehouse"
    }
}
```

* `name`: The name of the Hive database to be created.
* `comment`: Optional, user custom Hive database comment.
* `properties`: The properties of the Hive database. More properties information see the following schema properties table. Other properties will be passed down to the underlying Hive database parameters.

### schema properties

| Property name       | Description                                                                                                                                                     | example value                            | Since version |
|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------|---------------|
| `location`          | The directory for Hive database storage. Not required, HMS will use the value of `hive.metastore.warehouse.dir` in the Hive conf file hive-site.xml by default. | `/user/hive/warehouse`                   | 0.1.0         |

## Creating a Hive Table

After the schema is created, you can submit a table JSON example to the Gravitino server using the URL format:

```shell
http://{GravitinoServerHost}:{GravitinoServerPort}/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables
```

Example JSON:

```json
{
    "name": "test_table",
    "comment": "my test table",
    "columns": [
        {
            "name": "id",
            "type": "i32?",
            "comment": "id column comment"
        },
        {
            "name": "name",
            "type": "string?",
            "comment": "name column comment"
        },
        {
            "name": "age",
            "type": "i32?",
            "comment": "age column comment"
        },
        {
            "name": "dt",
            "type": "date?",
            "comment": "dt column comment"
        }
    ],
    "partitions": [
        {
            "strategy": "identity",
            "fieldName": ["dt"]
        }
    ],
    "distribution": {
        "strategy": "hash",
        "number": 32,
        "expressions": [
            {
                "expressionType": "field",
                "fieldName": ["id"]
            }
        ]
    },
    "sortOrders": [
        {
            "expression": {
                "expressionType": "field",
                "fieldName": ["age"]
            },
            "direction": "asc",
            "nullOrdering": "first"
        }
    ],
    "properties": {
        "format": "ORC"
    }
}
```

* `name`: The name of the Hive table to be created.
* `comment`: Optional, user custom Hive table comment.
* `columns`: The columns of the Hive table.
* `partitions`: Optional, the partitions of the Hive table, above example is a partitioned table with `dt` column.
* `distribution`: Optional, equivalent to the `CLUSTERED BY` clause in Hive DDL, above example table is bucketed(cluster by) `id` column.
* `sortOrders`: Optional, equivalent to the `SORTED BY` clause in Hive DDL, above example table data is sorted in increasing order of `age` in each bucket.
* `properties`: The properties of the Hive table. More properties information see the following table properties table. Other properties will be passed down to the underlying Hive table parameters.

### table properties

| Configuration item | Description                                                                                                                                                                                | example value                                                                                | Since version |
|--------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------|---------------|
| `location`         | The location for table storage. Not required, HMS will use the database location as parent directory by default.                                                                           | `/user/hive/warehouse/test_table`                                                            | 0.2.0         |
| `table-type`       | Type of the table. Valid values include `MANAGED_TABLE` and `EXTERNAL_TABLE`. `MANAGED_TABLE` by default value.                                                                            | `MANAGED_TABLE`                                                                              | 0.2.0         |
| `format`           | The table file format. Valid values include `TEXTFILE`, `SEQUENCEFILE`, `RCFILE`, `ORC`, `PARQUET`, `AVRO`, `JSON`, `CSV`, and `REGEX`. `TEXTFILE` by default value.                       | `ORC`                                                                                        | 0.2.0         |
| `input-format`     | The input format class for the table. The property `format` sets the default value `org.apache.hadoop.mapred.TextInputFormat` and can change it to a different default.                    | `org.apache.hadoop.hive.ql.io.orc.OrcInputFormat`                                            | 0.2.0         |
| `output-format`    | The output format class for the table. The property `format` sets the default value `org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat` and can change it to a different default. | `org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat`                                           | 0.2.0         |
| `serde-lib`        | The serde library class for the table. The property `format` sets the default value `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe` and can change it to a different default.         | `org.apache.hadoop.hive.ql.io.orc.OrcSerde`                                                  | 0.2.0         |
| `serde.parameter.` | The prefix of serde parameter, empty by default.                                                                                                                                           | `"serde.parameter.orc.create.index" = "true"` indicate `ORC` serde lib to create row indexes | 0.2.0         |

