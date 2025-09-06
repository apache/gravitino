---
title: "Apache Gravitino Trino connector - Hive catalog"
slug: /trino-connector/catalog-hive
keyword: gravitino connector trino
license: "This software is licensed under the Apache License version 2."
---

The Hive catalog allows Trino querying data stored in an Apache Hive data warehouse.

## Requirements

The Hive connector requires a Hive metastore service (HMS), or a compatible implementation of the Hive metastore, such
as AWS Glue.

Apache Hadoop HDFS 2.x supported.

Many distributed storage systems including HDFS, Amazon S3 or S3-compatible systems,
Google Cloud Storage, Azure Storage, and IBM Cloud Object Storage can be queried with the Hive connector.

The coordinator and all workers must have network access to the Hive metastore and the storage system.

Hive metastore access with the Thrift protocol defaults to using port 9083.

Data files must be in a supported file format. Some file formats can be configured using file format configuration
properties
per catalog:

- ORC
- PARQUET
- AVRO
- RCFILE
- SEQUENCEFILE
- JSON
- CSV
- TEXTFILE


## Schema operations

### Create a schema 

Users can create a schema with properties through Apache Gravitino Trino connector as follows:

```SQL
CREATE SCHEMA catalog.schema_name 
```

## Table operations

### Create table

The Gravitino Trino connector currently supports basic Hive table creation statements, such as defining fields,
allowing null values, and adding comments. The Gravitino Trino connector does not support `CREATE TABLE AS SELECT`.

The following example shows how to create a table in the Hive catalog:

```shell
CREATE TABLE catalog.schema_name.table_name
(
  name varchar,
  salary int
)
```


### Alter table

Support for the following alter table operations:

- Rename table
- Add a column
- Drop a column
- Rename a column
- Change a column type
- Set a table property

### Select

The Gravitino Trino connector supports most SELECT statements, allowing the execution of queries successfully.
Currently, it doesn't support certain query optimizations, such as pushdown and pruning functionalities.

### Update

`UPDATE` is only supported for transactional Hive tables with format ORC. `UPDATE` of partition or bucket columns is not supported.

### Delete

`DELETE` applied to non-transactional tables is only supported if the table is partitioned and the `WHERE` clause matches entire partitions. 
Transactional Hive tables with ORC format support "row-by-row" deletion, in which the `WHERE` clause may match arbitrary sets of rows.

### Merge

`MERGE` is only supported for ACID tables.

See also [more limitation](https://trino.io/docs/current/connector/hive.html#data-management).

## Schema and table properties

You can set additional properties for tables and schemas in the Hive catalog using "WITH" keyword in the "CREATE"
statement.


### Create a schema with properties

Users can use the following example to create a schema with properties: 

```sql
CREATE SCHEMA catalog.dbname
WITH (
  location = 'hdfs://hdfs-host:9000/user/hive/warehouse/dbname'
);
```

The following tables are the properties supported by the Hive schema:

| Property | Description                     | Default Value | Required | Reserved | Since Version |
|----------|---------------------------------|---------------|----------|----------|---------------|
| location | HDFS location for table storage | (none)        | No       | No       | 0.2.0         |

Reserved properties: A reserved property is one can't be set by users but can be read by users.


### Create a table with properties

Users can use the following example to create a table with properties: 

```sql
CREATE TABLE catalog.dbname.tablename
(
  name varchar,
  salary int
) WITH (
  format = 'TEXTFILE',
  KEY = 'VALUE',
  ...      
);
```

The following tables are the properties supported by the Hive table:

| Property       | Description                             | Default Value                                              | Required | Reserved | Since Version |
|----------------|-----------------------------------------|------------------------------------------------------------|----------|----------|---------------|
| format         | Hive storage format for the table       | TEXTFILE                                                   | No       | No       | 0.2.0         |
| location       | HDFS location for table storage         | (none)                                                     | No       | No       | 0.2.0         |
| input_format   | The input format class for the table    | org.apache.hadoop.mapred.TextInputFormat                   | No       | No       | 0.2.0         |
| output_format  | The output format class for the table   | org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat | No       | No       | 0.2.0         |
| serde_lib      | The serde library class for the table   | org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe         | No       | No       | 0.2.0         |
| serde_name     | Name of the serde                       | table name by default                                      | No       | No       | 0.2.0         |
| partitioned_by | Partition columns for the table         | (none)                                                     | No       | No       | 0.4.0         |   
| bucketed_by    | Bucket columns for the table            | (none)                                                     | No       | No       | 0.4.0         |
| bucket_count   | Number of buckets for the table         | (none)                                                     | No       | No       | 0.4.0         |
| sorted_by      | Sorted columns for the table            | (none)                                                     | No       | No       | 0.4.0         |

The following properties are automatically added and managed as reserved properties. Users are not allowed to set these properties.

| Property       | Description                             | Since Version |
|----------------|-----------------------------------------|---------------|
| total_size     | Total size of the table                 | 0.2.0         |
| num_files      | Number of files                         | 0.2.0         |
| external       | Indicate whether it's an external table | 0.2.0         |
| table_type     | The type of Hive table                  | 0.2.0         |

## Basic usage examples

You need to do the following steps before you can use the Hive catalog in Trino through Gravitino.

- Create a metalake and catalog in Gravitino. Assuming that the metalake name is `test` and the catalog name is `hive_test`,
then you can use the following code to create them in Gravitino:

```bash
curl -X POST -H "Content-Type: application/json" \
-d '{
  "name": "test",
  "comment": "comment",
  "properties": {}
}' http://gravitino-host:8090/api/metalakes

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
}' http://gravitino-host:8090/api/metalakes/test/catalogs
```

For More information about the Hive catalog, please refer to [Hive catalog](../apache-hive-catalog.md).

- Set the value of configuration `gravitino.metalake` to the metalake you have created, named 'test', and start the Trino container.

Use the Trino CLI to connect to the Trino container and run a query.

Listing all Gravitino managed catalogs:

```sql 
SHOW CATALOGS;
```

The results are similar to:

```text
    Catalog
----------------
 gravitino
 jmx
 system
 hive_test
(4 rows)

Query 20231017_082503_00018_6nt3n, FINISHED, 1 node
```

The `gravitino` catalog is a catalog defined By Trino catalog configuration.
The `hive_test` catalog is the catalog created by you in Gravitino.
Other catalogs are regular user-configured Trino catalogs.

### Creating tables and schemas

Create a new schema named `database_01` in `hive_test` catalog.

```sql
CREATE SCHEMA hive_test.database_01;
```

Create a new schema using HDFS location:

```sql
CREATE SCHEMA hive_test.database_01 WITH (
  location = 'hdfs://hdfs-host:9000/user/hive/warehouse/database_01'
);
```

Create a new table named `table_01` in schema `hive_test.database_01` and stored in a TEXTFILE format, partitioning by `salary`, bucket by `name` and sorted by `salary`.

```sql
CREATE TABLE  hive_test.database_01.table_01
(
name varchar,
salary int,
month int    
)
WITH (
  format = 'TEXTFILE',
  partitioned_by = ARRAY['month'],
  bucketed_by = ARRAY['name'],
  bucket_count = 2,
  sorted_by = ARRAY['salary']  
);
```

### Writing data

Insert data into the table `table_01`:

```sql
INSERT INTO hive_test.database_01.table_01 (name, salary, month) VALUES ('ice', 12, 22);
```

Insert data into the table `table_01` from select:

```sql
INSERT INTO hive_test.database_01.table_01 (name, salary, month) SELECT * FROM hive_test.database_01.table_01;
```

Delete data from the table `table_01` with an entire partition:

```sql
DELETE FROM hive_test.database_01.table_01 WHERE month=22;
```

If an ACID table is defined in the schema `hive_test.database_01` as follows: 

```sql
CREATE TABLE database_01.test_acid
(
    id INT,
    name STRING,
    salary INT
)
CLUSTERED BY (id) INTO 4 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true');
```

Update data into table `test_acid`:

```sql
UPDATE hive_test.database_01.test_acid SET name='bob' WHERE id=1;
```

Delete data from table `test_acid`:

```sql
DELETE FROM hive_test.database_01.test_acid WHERE id=1;
```

Merge data into table `test_acid`:

```sql
MERGE INTO hive_test.database_01.test_acid t USING hive_test.database_01.table_01 s
    ON (t.name = s.name)
    WHEN MATCHED AND s.name = 'bob'
        THEN DELETE
    WHEN MATCHED
        THEN UPDATE
            SET salary = s.salary + t.salary
    WHEN NOT MATCHED
        THEN INSERT (id, name, salary)
              VALUES (3, s.name, s.salary);
```

### Querying data

Query the `table_01` table:

```sql
SELECT * FROM hive_test.database_01.table_01;
```

### Modify a table

Add a new column `age` to the `table_01` table:

```sql
ALTER TABLE hive_test.database_01.table_01 ADD COLUMN age int;
```

Drop a column `age` from the `table_01` table:

```sql
ALTER TABLE hive_test.database_01.table_01 DROP COLUMN age;
```

Rename the `table_01` table to `table_02`:

```sql
ALTER TABLE hive_test.database_01.table_01 RENAME TO hive_test.database_01.table_02;
```

### DROP

Drop a schema:

```sql
DROP SCHEMA hive_test.database_01;
```

Drop a table:

```sql
DROP TABLE hive_test.database_01.table_01;
```

## HDFS config and permissions

For basic setups, the Apache Gravitino Trino connector configures the HDFS client
using catalog configurations. It supports configuring the HDFS client with `hdfs-site.xml`
and `core-site.xml` files via the `trino.bypass.hive.config.resources` setting in the catalog configurations.

Before running any `Insert` statements for Hive tables in Trino,
you must check that the user Trino is using to access HDFS has access to the Hive warehouse directory.
You can override this username by setting the HADOOP_USER_NAME system property in the Trino JVM config,
replacing hdfs_user with the appropriate username:

```text
-DHADOOP_USER_NAME=hdfs_user
```

## S3

When using AWS S3 within the Hive catalog, users need to configure the Trino Hive connector's
AWS S3-related properties in the catalog's properteis. Please refer to the documentation
of [Hive connector with Amazon S3](https://trino.io/docs/435/connector/hive-s3.html).

To create a Hive catalog with AWS S3 configuration in the Trino CLI, use the following command:

```sql
call gravitino.system.create_catalog(
  'gt_hive',
  'hive',
  map(
    array['metastore.uris',
        'trino.bypass.hive.s3.aws-access-key', 'trino.bypass.hive.s3.aws-secret-key', 'trino.bypass.hive.s3.region'
    ],
    array['thrift://hive:9083', '<aws-access-key>', '<aws-secret-key>', '<region>']
  )
);
```

- The settings for `trino.bypass.hive.s3.aws-access-key`, `trino.bypass.hive.s3.aws-secret-key` and `trino.bypass.hive.s3.region`
are required by the Apache Gravitino Trino connector.

Once the Hive catalog is successfully created, users can create schemas and tables as follows:

```sql
CREATE SCHEMA gt_hive.gt_db02
WITH (location = 's3a://trino-test/dw/gt_db02');

CREATE TABLE gt_hive.gt_db02.tb01 (
    name varchar,
    salary int
);
```

The `location` specifies the AWS S3 storage path.

After running the command, the tables are ready for data reading and writing operations on AWS S3.

:::note
Ensure the Hive Metastore service used by the Hive catalog supports AWS S3.
:::
