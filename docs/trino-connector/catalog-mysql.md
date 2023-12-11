---
title: "Gravtino connnector - MySQL catalog"
slug: /trino-connector/catalogs/mysql
keyword: gravition connector trino
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

The MySQL catalog allows querying and creating tables in an external MySQL instance. 
This can be used to join data between different systems like MySQL and Hive, or between two different MySQL instances.

## Requirements

To connect to MySQL, you need:
- MySQL 5.7, 8.0 or higher.
- Network access from the Trino coordinator and workers to MySQL. Port 3306 is the default port.

## Create table

At present, the Gravitino connector only supports basic MySQL table creation statements, which involve fields, null allowances, and comments. 
However, it does not support advanced features like primary keys, indexes, default values, and auto-increment.

The Gravitino connector does not support `CREATE TABLE AS SELECT`.

## Alter table

Support for the following alter table operations:
- Rename table
- Add a column
- Drop a column
- Change a column type
- Set a table property

## Select

The Gravitino connector supports most SELECT statements, allowing the execution of queries successfully.
It's not implements certain query optimizations, such as indexing and pushdowns.

## Table and Schema properties

MySQL's tables and schemas cannot support properties.

## Basic usage examples

First, you need to create a metalake and catalog in Gravitino.
For example, create a new metalake named `test` and create a new catalog named `mysql_test` using the `mysql` provider.
For More information about the MySQL catalog, please refer to [MySql catalog](../docs/jdbc-mysql-catalog).

```bash
curl -X POST -H "Content-Type: application/json" \
-d '{
  "name": "test",
  "comment": "comment",
  "properties": {}
}' http://gravition-host:8090/api/metalakes

curl -X POST -H "Content-Type: application/json" \
-d '{
  "name": "mysql_test",
  "type": "RELATIONAL",
  "comment": "comment",
  "provider": "jdbc-mysql",
  "properties": {
    "jdbc-url": "jdbc:mysql://mysql-host:3306?useSSL=false",
    "jdbc-user": "root",
    "jdbc-password": "ds123"
    "jdbc-driver": "com.mysql.cj.jdbc.Driver"
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
 test.mysql_test
(4 rows)

Query 20231017_082503_00018_6nt3n, FINISHED, 1 node
```

The `gravitino` catalog is a catalog defined By Trino catalog configuration. 
The `1test.mysql_test` catalog is the catalog created by you in Gravitino.
Other catalogs are regular user-configured Trino catalogs.

### Creating tables and schemas

Create a new schema named `database_01` in `test.mysql_test` catalog.

```sql
create schema "test.mysql_test".database_01;
```

Create a new table named `table_01` in schema `"test.mysql_test".database_01` and stored in a TEXTFILE format.

```sql
create table  "test.mysql_test".database_01.table_01
(
name varchar,
salary int
);
```

Drop a schema:

```sql
drop schema "test.mysql_test".database_01;
```

Drop a table:

```sql
drop table "test.mysql_test".database_01.table_01;
```

### Writing data

Insert data into the table `table_01`:

```sql
insert into  "test.mysql_test".database_01.table_01 (name, salary) values ('ice', 12);
```

Insert data into the table `table_01` from select:

```sql
insert into  "test.mysql_test".database_01.table_01 (name, salary) select * from "test.mysql_test".database_01.table_01;
```

### Querying data

Query the `table_01` table:

```sql
select * from "test.mysql_test".database_01.table_01;
```

### Modify a table

Add a new column `age` to the `table_01` table:

```sql
alter table "test.mysql_test".database_01.table_01 add column age int;
```

Drop a column `age` from the `table_01` table:

```sql
alter table "test.mysql_test".database_01.table_01 drop column age;
```

Rename the `table_01` table to `table_02`:

```sql
alter table "test.mysql_test".database_01.table_01 rename to "test.mysql_test".database_01.table_02;
```