---
title: "Apache Gravitino Trino connector - Iceberg catalog"
slug: /trino-connector/catalog-iceberg
keyword: gravitino connector trino
license: "This software is licensed under the Apache License version 2."
---

Apache Iceberg is an open table format for huge analytic datasets. 
The Iceberg catalog allows Trino querying data stored in files written in Iceberg format, 
as defined in the Iceberg Table Spec. The catalog supports Apache Iceberg table spec versions 1 and 2.

## Requirements

To use Iceberg, you need:
- Network access from the Trino coordinator and workers to the distributed object storage.
- Access to a Hive metastore service (HMS), an AWS Glue catalog, a JDBC catalog, a REST catalog, or a Nessie server.
- Data files stored in a supported file format. These can be configured using file format configuration properties per catalog:
  - ORC
  - Parquet (default)

## Schema operations

### Create a schema

Users can create a schema through Apache Gravitino Trino connector as follows:

```SQL
CREATE SCHEMA catalog.schema_name
```

## Table operations

### Create table

The Apache Gravitino Trino connector currently supports basic Iceberg table creation statements, such as defining fields,
allowing null values, and adding comments. The Apache Gravitino Trino connector does not support `CREATE TABLE AS SELECT`.

The following example shows how to create a table in the Iceberg catalog:

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

The Apache Gravitino Trino connector supports most SELECT statements, allowing the execution of queries successfully.
Currently, it doesn't support certain query optimizations, such as pushdown and pruning functionalities.

### Update

`UPDATE` is only supported for table using v2 or higher of the Iceberg specification. 

### Delete

Support the deletion of entire partitions and deletion of individual rows for table using v2 or higher of the Iceberg specification.
See also [Delete limitation](https://trino.io/docs/current/connector/iceberg.html#data-management).

### Merge

`MERGE` is only supported for table using v2 or higher of the Iceberg specification.

## Table and Schema properties

### Create a schema with properties

Iceberg schema does not support properties.

### Create a table with properties

Users can use the following example to create a table with properties:

```sql
CREATE TABLE catalog.dbname.tablename
(
  name varchar,
  salary int
) WITH (
  KEY = 'VALUE',
  ...      
);
```

The following tables are the properties supported by the Iceberg table:

| Property       | Description                             | Default Value                                              | Required | Reserved | Since Version |
|----------------|-----------------------------------------|------------------------------------------------------------|----------|----------|---------------|
| partitioning   | Partition columns for the table         | (none)                                                     | No       | No       | 0.4.0         |
| sorted_by      | Sorted columns for the table            | (none)                                                     | No       | No       | 0.4.0         | 

Reserved properties: A reserved property is one can't be set by users but can be read by users. 

## Basic usage examples

You need to do the following steps before you can use the Iceberg catalog in Trino through Apache Gravitino.

- Create a metalake and catalog in Apache Gravitino. Assuming that the metalake name is `test` and the catalog name is `iceberg_test`,
then you can use the following code to create them in Apache Gravitino:

```bash
curl -X POST -H "Content-Type: application/json" \
-d '{
  "name": "test",
  "comment": "comment",
  "properties": {}
}' http://gravitino-host:8090/api/metalakes

curl -X POST -H "Content-Type: application/json" \
-d '{
  "name": "iceberg_test",
  "type": "RELATIONAL",
  "comment": "comment",
  "provider": "lakehouse-iceberg",
  "properties": {
    "uri": "thrift://hive-host:9083",
    "catalog-backend": "hive",
    "warehouse": "hdfs://hdfs-host:9000/user/iceberg/warehouse"
  }
}' http://gravitino-host:8090/api/metalakes/test/catalogs
```

For More information about the Iceberg catalog, please refer to [Iceberg catalog](../lakehouse-iceberg-catalog.md).

- Set the value of configuration `gravitino.metalake` to the metalake you have created, named 'test', and start the Trino container.

Use the Trino CLI to connect to the Trino container and run a query.

Listing all Apache Gravitino managed catalogs:

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
 iceberg_test
(4 rows)

Query 20231017_082503_00018_6nt3n, FINISHED, 1 node
```

The `gravitino` catalog is a catalog defined By Trino catalog configuration. 
The `iceberg_test` catalog is the catalog created by you in Apache Gravitino.
Other catalogs are regular user-configured Trino catalogs.

### Creating tables and schemas

Create a new schema named `database_01` in `test.iceberg_test` catalog.

```sql
CREATE SCHEMA iceberg_test.database_01;
```

Create a new table named `table_01` in schema `iceberg_test.database_01`.

```sql
CREATE TABLE iceberg_test.database_01.table_01
(
name varchar,
salary int
) with (
  partitioning = ARRAY['salary'],
  sorted_by = ARRAY['name']
);
```

### Writing data

Insert data into the table `table_01`:

```sql
INSERT INTO iceberg_test.database_01.table_01 (name, salary) VALUES ('ice', 12);
```

Insert data into the table `table_01` from select:

```sql
INSERT INTO iceberg_test.database_01.table_01 (name, salary) SELECT * FROM iceberg_test.database_01.table_01;
```

Update data into the table `table_01`:

```sql
UPDATE iceberg_test.database_01.table_01 SET name='ice_update' WHERE salary=12;
```

Delete data into the table `table_01`:

```sql
DELETE FROM iceberg_test.database_01.table_01 WHERE name='ice';
```

Merge data into the table `table_01`:

```sql
MERGE INTO iceberg_test.database_01.table_01 t USING iceberg_test.database_01.table_02 s
    ON (t.name = s.name)
    WHEN MATCHED AND s.name = 'bob'
        THEN DELETE
    WHEN MATCHED
        THEN UPDATE
            SET salary = s.salary + t.salary
    WHEN NOT MATCHED
        THEN INSERT (name, salary)
              VALUES (s.name, s.salary);
```

### Querying data

Query the `table_01` table:

```sql
SELECT * FROM iceberg_test.database_01.table_01;
```

### Modify a table

Add a new column `age` to the `table_01` table:

```sql
ALTER TABLE iceberg_test.database_01.table_01 ADD COLUMN age int;
```

Drop a column `age` from the `table_01` table:

```sql
ALTER TABLE iceberg_test.database_01.table_01 DROP COLUMN age;
```

Rename the `table_01` table to `table_02`:

```sql
ALTER TABLE iceberg_test.database_01.table_01 RENAME TO iceberg_test.database_01.table_02;
```

### Drop

Drop a schema:

```sql
DROP SCHEMA iceberg_test.database_01;
```

Drop a table:

```sql
DROP TABLE iceberg_test.database_01.table_01;
```

## HDFS username and permissions

Before running any `Insert` statements for Iceberg tables in Trino, 
you must check that the user Trino is using to access HDFS has access to the warehouse directory.
You can override this username by setting the HADOOP_USER_NAME system property in the Trino JVM config, 
replacing hdfs_user with the appropriate username:

```text
-DHADOOP_USER_NAME=hdfs_user
```

## S3

When using AWS S3 within the Iceberg catalog, users need to configure the Trino Iceberg connector's
AWS S3-related properties in the catalog's properties. Please refer to the documentation
of [Hive connector with Amazon S3](https://trino.io/docs/435/connector/hive-s3.html).
These configurations must use the `trino.bypass.` prefix in the Iceberg catalog's attributes to be effective.

To create an Iceberg catalog with AWS S3 configuration in the Trino CLI, use the following command:

```sql
call gravitino.system.create_catalog(
    'gt_iceberg',
    'lakehouse-iceberg',
    map(
        array['uri', 'catalog-backend', 'warehouse',
          'trino.bypass.hive.s3.aws-access-key', 'trino.bypass.hive.s3.aws-secret-key', 'trino.bypass.hive.s3.region',
          's3-access-key-id', 's3-secret-access-key', 's3-region', 'io-impl'
        ],
        array['thrift://hive:9083', 'hive', 's3a://trino-test-ice/dw2',
        '<aws-access-key>', '<aws-secret-key>', '<region>',
        '<aws-access-key>', '<aws-secret-key>', '<region>', 'org.apache.iceberg.aws.s3.S3FileIO']
    )
);
```

- The configurations of `trino.bypass.hive.s3.aws-access-key`, `trino.bypass.hive.s3.aws-secret-key`, `trino.bypass.hive.s3.region`
are the required the configurations for the Apache Gravitino Trino connector.
- The configurations of `s3-access-key-id`, `s3-secret-access-key`, `io-impl` and `s3-region`.
are the required the configurations for the [Apache Gravitino Iceberg catalog](../lakehouse-iceberg-catalog.md#S3).
- The `location` specifies the storage path on AWS S3. Ensure that the specified directory exists on AWS S3 before proceeding.

Once the Iceberg catalog is successfully created, users can create schemas and tables as follows:

```sql
CREATE SCHEMA gt_iceberg.gt_db03;

CREATE TABLE gt_iceberg.gt_db03.tb01 (
    name varchar,
    salary int
);
```

After running the command, the tables are ready for data reading and writing operations on AWS S3.

:::note
TThe Iceberg catalog module in the Apache Gravitino server should add AWS S3 support.
Please refer to [Apache Gravitino Iceberg catalog](../lakehouse-iceberg-catalog.md#S3).
:::
