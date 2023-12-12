---
title: "Gravtino connnector - Hive catalog"
slug: /trino-connector/catalogs/hive
keyword: gravition connector trino
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

The Hive catalog allows Trino querying data stored in an Apache Hive data warehouse. 

## Requirements

The Hive connector requires a Hive metastore service (HMS), or a compatible implementation of the Hive metastore, such as AWS Glue.

Apache Hadoop HDFS 2.x and 3.x are supported.

Many distributed storage systems including HDFS, Amazon S3 or S3-compatible systems,
Google Cloud Storage, Azure Storage, and IBM Cloud Object Storage can be queried with the Hive connector.

The coordinator and all workers must have network access to the Hive metastore and the storage system. 

Hive metastore access with the Thrift protocol defaults to using port 9083.

Data files must be in a supported file format. Some file formats can be configured using file format configuration properties 
per catalog:
  - ORC
  - Parquet
  - Avro
  - RCText (RCFile using ColumnarSerDe)
  - RCBinary (RCFile using LazyBinaryColumnarSerDe)
  - SequenceFile
  - JSON (using org.apache.hive.hcatalog.data.JsonSerDe)
  - CSV (using org.apache.hadoop.hive.serde2.OpenCSVSerde)
  - TextFile

## Create table

The Gravitino connector currently supports basic Hive table creation statements, such as defining fields, 
allowing null values, and adding comments. 
However, it does not support advanced features like partitioning, sorting, and distribution.

The Gravitino connector does not support `CREATE TABLE AS SELECT`.

## Alter table

Support for the following alter table operations:
- Rename table
- Add a column
- Drop a column
- Rename a column
- Change a column type
- Set a table property

## Select

The Gravitino connector supports most SELECT statements, allowing the execution of queries successfully.
Currently, it doesn't support certain query optimizations, such as pushdown and pruning functionalities.

## Table properties

You can set additional properties for tables and schemas in the Hive catalog using "WITH" keyword in the "CREATE TABLE" statement.

```sql
CREATE TABLE "metalake.catalog".dbname.tabname
(
  name varchar,
  salary int
) WITH (
  format = 'TEXTFILE'
);
```

| Property      | Description                              | Default                                                    | Required | Since Version |
|---------------|------------------------------------------|------------------------------------------------------------|----------|---------------|
| format        | Hive storage format for the table        | TEXTFILE                                                   | No       | 0.2.0         |
| total_size    | Total size of the table                  | (none)                                                     | No       | 0.2.0         |
| num_files     | Number of files                          | 0                                                          | No       | 0.2.0         |
| external      | Indicate whether it's an external table  | (none)                                                     | No       | 0.2.0         |
| location      | HDFS location for table storage          | (none)                                                     | No       | 0.2.0         |
| table_type    | The type of Hive table                   | (none)                                                     | No       | 0.2.0         |
| input_format  | The input format class for the table     | org.apache.hadoop.mapred.TextInputFormat                   | No       | 0.2.0         |
| output_format | The output format class for the table    | org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat | No       | 0.2.0         |
| serde_lib     | The serde library class for the table    | org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe         | No       | 0.2.0         |
| serde_name    | Name of the serde, table name by default | (none)                                                     | No       | 0.2.0         |

## Schema properties

| Property | Description                     | Default | Required | Since Version |
|----------|---------------------------------|---------|----------|---------------|
| location | HDFS location for table storage | (none)  | No       | 0.2.0         |

## Basic usage examples

First, you need to create a metalake and catalog in Gravitino.
For example, create a new metalake named `test` and create a new catalog named `hive_test` using the `hive` provider.
For More information about the Hive catalog, please refer to [Hive catalog](../apache-hive-catalog).

```bash
curl -X POST -H "Content-Type: application/json" \
-d '{
  "name": "test",
  "comment": "comment",
  "properties": {}
}' http://gravition-host:8090/api/metalakes

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
SHOW CATALOGS
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
The `test.hive_test` catalog is the catalog created by you in Gravitino.
Other catalogs are regular user-configured Trino catalogs.

### Creating tables and schemas

Create a new schema named `database_01` in `test.hive_test` catalog.

```sql
CREATE SCHEMA "test.hive_test".database_01;
```

Create a new schema using HDFS location:

```sql
CREATE SCHEMA "test.hive_test".database_01 {
  location = 'hdfs://hdfs-host:9000/user/hive/warehouse/database_01'
};
```

Create a new table named `table_01` in schema `"test.hive_test".database_01` and stored in a TEXTFILE format.

```sql
CREATE TABLE  "test.hive_test".database_01.table_01
(
name varchar,
salary int
)
WITH (
  format = 'TEXTFILE'
);
```

### Writing data

Insert data into the table `table_01`:

```sql
INSERT INTO "test.hive_test".database_01.table_01 (name, salary) VALUES ('ice', 12);
```

Insert data into the table `table_01` from select:

```sql
INSERT INTO "test.hive_test".database_01.table_01 (name, salary) SELECT * FROM "test.hive_test".database_01.table_01;
```

### Querying data

Query the `table_01` table:

```sql
SELECT * FROM "test.hive_test".database_01.table_01;
```

### Modify a table

Add a new column `age` to the `table_01` table:

```sql
ALTER TABLE "test.hive_test".database_01.table_01 ADD COLUMN age int;
```

Drop a column `age` from the `table_01` table:

```sql
ALTER TABLE "test.hive_test".database_01.table_01 DROP COLUMN age;
```

Rename the `table_01` table to `table_02`:

```sql
ALTER TABLE "test.hive_test".database_01.table_01 RENAME TO "test.hive_test".database_01.table_02;
```

# DROP 

Drop a schema:

```sql
DROP SCHEMA "test.hive_test".database_01;
```

Drop a table:

```sql
DROP TABLE "test.hive_test".database_01.table_01;
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