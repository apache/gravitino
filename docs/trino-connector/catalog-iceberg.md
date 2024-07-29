---
title: "Apache Gravitino connector - Iceberg catalog"
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
CREATE SCHEMA "metalake.catalog".schema_name 
```

## Table operations

### Create table

The Gravitino connector currently supports basic Iceberg table creation statements, such as defining fields,
allowing null values, and adding comments. The Gravitino connector does not support `CREATE TABLE AS SELECT`.

The following example shows how to create a table in the Iceberg catalog:

```shell
CREATE TABLE "metalake.catalog".schema_name.table_name
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

## Select

The Gravitino connector supports most SELECT statements, allowing the execution of queries successfully.
Currently, it doesn't support certain query optimizations, such as pushdown and pruning functionalities.

## Table and Schema properties

### Create a schema with properties

Iceberg schema does not support properties.

### Create a table with properties

Users can use the following example to create a table with properties:

```sql
CREATE TABLE "metalake.catalog".dbname.tablename
(
  name varchar,
  salary int
) WITH (
  format = 'TEXTFILE',
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

You need to do the following steps before you can use the Iceberg catalog in Trino through Gravitino.

- Create a metalake and catalog in Gravitino. Assuming that the metalake name is `test` and the catalog name is `iceberg_test`,
then you can use the following code to create them in Gravitino:

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
 iceberg_test
(4 rows)

Query 20231017_082503_00018_6nt3n, FINISHED, 1 node
```

The `gravitino` catalog is a catalog defined By Trino catalog configuration. 
The `test.iceberg_test` catalog is the catalog created by you in Gravitino.
Other catalogs are regular user-configured Trino catalogs.

### Creating tables and schemas

Create a new schema named `database_01` in `test.iceberg_test` catalog.

```sql
CREATE SCHEMA iceberg_test.database_01;
```

Create a new table named `table_01` in schema `"test.iceberg_test".database_01`.

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
INSERT INTO iceberg_test.database_01.table_01 (name, salary) SELECT * FROM "test.iceberg_test".database_01.table_01;
```

### Querying data

Query the `table_01` table:

```sql
SELECT * FROM "test.iceberg_test".database_01.table_01;
```

### Modify a table

Add a new column `age` to the `table_01` table:

```sql
ALTER TABLE "test.iceberg_test".database_01.table_01 ADD COLUMN age int;
```

Drop a column `age` from the `table_01` table:

```sql
ALTER TABLE "test.iceberg_test".database_01.table_01 DROP COLUMN age;
```

Rename the `table_01` table to `table_02`:

```sql
ALTER TABLE "test.iceberg_test".database_01.table_01 RENAME TO "test.iceberg_test".database_01.table_02;
```

### Drop

Drop a schema:

```sql
DROP SCHEMA "test.iceberg_test".database_01;
```

Drop a table:

```sql
DROP TABLE "test.iceberg_test".database_01.table_01;
```

## HDFS username and permissions

Before running any `Insert` statements for Iceberg tables in Trino, 
you must check that the user Trino is using to access HDFS has access to the warehouse directory.
You can override this username by setting the HADOOP_USER_NAME system property in the Trino JVM config, 
replacing hdfs_user with the appropriate username:

```text
-DHADOOP_USER_NAME=hdfs_user
```