---
title: "Gravtino connnector - Hive catalog"
slug: /trino-connector/catalogs/hive
keyword: gravition connector trino
license: Copyright 2023 Datastrato Pvt. This software is licensed under the Apache License version 2.
---

The Hive catalog allows Trino querying data stored in an Apache Hive data warehouse. 

## Table Properties

In the Hive catalog, additional properties can be set for tables and schemas
using the "WITH" keyword in the "CREATE TABLE/SCHEMA" statement.
```sql
CREATE TABLE "metalake.catalog".dbname.tabname
(
  name varchar,
  salary int
) WITH (
  format = 'TEXTFILE'
);
```

| Property     | Description                               | Default                                                    |
| ------------ |-------------------------------------------| ---------------------------------------------------------- |
| format       | Hive storage format for the table         | TEXTFILE                                                   |
| total_size   | total size of the table                   | null                                                       |
| num_files    | number of files                           | 0                                                          |
| external     | Indicate whether it's an external table   | null                                                       |
| location     | HDFS location for table storage           | null                                                       |
| table_type   | The type of Hive table                    | null                                                       |
| input_format | The input format class for the table      | org.apache.hadoop.mapred.TextInputFormat                   |
| output_format| The output format class for the table     | org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat |
| serde_lib    | The serde library class for the table     | org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe         |
| serde_name   | Name of the serde, table name by default  | null                                                       |

## Schema Properties
| Property | Description                        | Default |
| -------- | ---------------------------------- | ------- |
| location | HDFS location for table storage    | null    |