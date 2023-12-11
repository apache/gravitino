---
title: "Gravtino connnector - Iceberg catalog"
slug: /trino-connector/catalogs/iceberg
keyword: gravition connector trino
license: "Copyright 2023 Datastrato Pvt Ltd. This software is licensed under the Apache License version 2."
---

Apache Iceberg is an open table format for huge analytic datasets. 
The Iceberg catalog allows Trino querying data stored in files written in Iceberg format, 
as defined in the Iceberg Table Spec. The catalog supports Apache Iceberg table spec versions 1 and 2.

## Create table

Currently, only basic table creation statements are supported, including fields, null allowance, and comments.
Advanced features like partitioning, sorting, and distribution are not supported.

## Alter table

Support for the following alter table operations:
- Rename table
- Add a column
- Drop a column
- Rename a column
- Change a column type
- Set a table property

## Table and Schema properties

Iceberg's tables and schemas cannot support properties.

## Basic usage examples

First, you need to create a metalake and catalog in Gravitino.
For example, create a new metalake named `test` and create a new catalog named `iceberg_test` using the `iceberge` provider.

```bash
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
 test.iceberg_test
(4 rows)

Query 20231017_082503_00018_6nt3n, FINISHED, 1 node
```

The `gravitino` catalog is a catalog defined By Trino catalog configuration. 
The test.iceberg_test catalog is the catalog created by you in Gravitino.
Other catalogs are regular user-configured Trino catalogs.

### Creating tables and schemas

Create a new schema named `database_01` in `test.iceberg_test` catalog.

```sql
create schema "test.iceberg_test".database_01;
```

Create a new schema using hdfs location:

```sql
create schema "test.iceberg_test".database_01 {
  location = 'hdfs://hdfs-host:9000/user/iceberg/warehouse/database_01'
};
```

Create a new table named `table_01` in schema `"test.iceberg_test".database_01` and stored in a TEXTFILE format.

```sql
create table  "test.iceberg_test".database_01.table_01
(
name varchar,
salary int
);
```

Drop a schema:

```sql
drop schema "test.iceberg_test".database_01;
```

Drop a table:

```sql
drop table "test.iceberg_test".database_01.table_01;
```

### Writing data

Insert data into the table `table_01`:

```sql
insert into  "test.iceberg_test".database_01.table_01 (name, salary) values ('ice', 12);
```

Insert data into the table `table_01` from select:

```sql
insert into  "test.iceberg_test".database_01.table_01 (name, salary) select * from "test.iceberg_test".database_01.table_01;
```

### Querying data

Query the `table_01` table:

```sql
select * from "test.iceberg_test".database_01.table_01;
```

### Modify a table

Add a new column `age` to the `table_01` table:

```sql
alter table "test.iceberg_test".database_01.table_01 add column age int;
```

Drop a column `age` from the `table_01` table:

```sql
alter table "test.iceberg_test".database_01.table_01 drop column age;
```

rename the `table_01` table to `table_02`:

```sql
alter table "test.iceberg_test".database_01.table_01 rename to "test.iceberg_test".database_01.table_02;
```

## HDFS username and permissions

Before running any `Insert` statements for Iceberg tables in Trino, 
you must check that the user Trino is using to access HDFS has access to the Hive warehouse directory.
You can override this username by setting the HADOOP_USER_NAME system property in the Trino JVM config, 
replacing hdfs_user with the appropriate username:
```text
-DHADOOP_USER_NAME=hdfs_user
```
