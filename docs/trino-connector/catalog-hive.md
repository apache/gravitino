---
title: "Gravtino connnector - Hive catalog"
slug: /trino-connector/catalogs/hive
keyword: gravition connector trino
license: "Copyright 2023 Datastrato Pvt Ltd. This software is licensed under the Apache License version 2."
---

The Hive catalog allows Trino querying data stored in an Apache Hive data warehouse. 

## Create table

Currently, only basic Hive table creation statements are supported, including fields, null allowance, and comments.
Advanced features like partitioning, sorting, and distribution are not supported.

`CREATE TABLE AS SELECT` is not supported.

## Alter table

Support for the following alter table operations:
- Rename table
- Add a column
- Drop a column
- Rename a column
- Change a column type
- Set a table property

## Select

Most `SELECT` statements are supported, the queries can be executed successfully.
Some query optimizations such as pushdown and pruning functionalities are not yet supported.

## Table properties

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

## Schema properties

| Property | Description                        | Default |
| -------- | ---------------------------------- | ------- |
| location | HDFS location for table storage    | null    |

## Basic usage examples

First, you need to create a metalake and catalog in Gravitino.
For example, create a new metalake named `test` and create a new catalog named `hive_test` using the `hive` provider.

```bash
curl -X POST \
-H "Content-Type: application/json" \
-d '{
  "name": "hive_test",
  "type": "RELATIONAL",
  "comment": "comment",
  "provider": "hive",
  "properties": {
    "metastore.uris": "thrift://hive-host:9083"
  }
}' http://gravition-host:8090/api/metalakes/test/catalogs
```

Listing all Gravitino managed catalogs:

```sql 
show catalogs
```

The results are similar to:

```text
    Catalog
----------------
 gravitino
 jmx
 system
 test.hive_test
(4 rows)

Query 20231017_082503_00018_6nt3n, FINISHED, 1 node
```

The `gravitino` catalog is a catalog defined By Trino catalog configuration. 
The test.hive_test catalog is the catalog created by you in Gravitino.
Other catalogs are regular user-configured Trino catalogs.

### Creating tables and schemas

Create a new schema named `database_01` in `test.hive_test` catalog.

```sql
create schema "test.hive_test".database_01;
```

Create a new schema using hdfs location:

```sql
create schema "test.hive_test".database_01 {
  location = 'hdfs://hdfs-host:9000/user/hive/warehouse/database_01'
};
```

Create a new table named `table_01` in schema `"test.hive_test".database_01` and stored in a TEXTFILE format.

```sql
create table  "test.hive_test".database_01.table_01
(
name varchar,
salary int
)
WITH (
  format = 'TEXTFILE'
);
```

Drop a schema:

```sql
drop schema "test.hive_test".database_01;
```

Drop a table:

```sql
drop table "test.hive_test".database_01.table_01;
```

### Writing data

Insert data into the table `table_01`:

```sql
insert into  "test.hive_test".database_01.table_01 (name, salary) values ('ice', 12);
```

Insert data into the table `table_01` from select:

```sql
insert into  "test.hive_test".database_01.table_01 (name, salary) select * from "test.hive_test".database_01.table_01;
```

### Querying data

Query the `table_01` table:

```sql
select * from "test.hive_test".database_01.table_01;
```

### Modify a table

Add a new column `age` to the `table_01` table:

```sql
alter table "test.hive_test".database_01.table_01 add column age int;
```

Drop a column `age` from the `table_01` table:

```sql
alter table "test.hive_test".database_01.table_01 drop column age;
```

rename the `table_01` table to `table_02`:

```sql
alter table "test.hive_test".database_01.table_01 rename to "test.hive_test".database_01.table_02;
```

## HDFS config and permissions

For basic setups, Gravitino connector configures the HDFS client automatically and does not require any configuration files.
Gravitino connector is not support user to config the `hdfs-site.xml` and `core-site.xml` files to the HDFS client.

Before running any `Insert` statements for Hive tables in Trino, 
you must check that the user Trino is using to access HDFS has access to the Hive warehouse directory.
You can override this username by setting the HADOOP_USER_NAME system property in the Trino JVM config, 
replacing hdfs_user with the appropriate username:

```text
-DHADOOP_USER_NAME=hdfs_user
```