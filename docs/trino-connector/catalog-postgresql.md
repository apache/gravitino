---
title: "Gravtino connnector - PostgreSQL catalog"
slug: /trino-connector/catalogs/postgresql
keyword: gravition connector trino
license: "Copyright 2023 Datastrato Pvt Ltd. This software is licensed under the Apache License version 2."
---

The PostgreSQL catalog allows querying and creating tables in an external PostgreSQL database. 
This can be used to join data between different systems like PostgreSQL and Hive, or between different PostgreSQL instances.

## Create table

Currently, only basic mysql table creation statements are supported, including fields, null allowance, and comments.
Advanced features like primary key, index, default value, auto increment are not supported.


## Alter table

Support for the following alter table operations:
- Rename table
- Add a column
- Drop a column
- Rename a column
- Change a column type
- Set a table property

## Table and Schema properties

PostgreSQL's tables and schemas cannot support properties.


## Basic usage examples

First, you need to create a metalake and catalog in Gravitino.
For example, create a new metalake named `test` and create a new catalog named `postgresql_test` using the `postgresql` provider.

```bash
curl -X POST -H "Content-Type: application/json" \
-d '{
  "name": "jdbc-postgresql",
  "type": "RELATIONAL",
  "comment": "comment",
  "provider": "jdbc-postgresql",
  "properties": {
    "jdbc-url": "jdbc:postgresql://postgresql-host/mydb",
    "jdbc-user": "root",
    "jdbc-password": "ds123",
    "jdbc-database": "mydb",
    "jdbc-driver": "org.postgresql.Driver"
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
 test.postgresql_test
(4 rows)

Query 20231017_082503_00018_6nt3n, FINISHED, 1 node
```

The `gravitino` catalog is a catalog defined By Trino catalog configuration. 
The test.postgresql_test catalog is the catalog created by you in Gravitino.
Other catalogs are regular user-configured Trino catalogs.

### Creating tables and schemas

Create a new schema named `database_01` in `test.postgresql_test` catalog.

```sql
create schema "test.postgresql_test".database_01;
```

Create a new table named `table_01` in schema `"test.postgresql_test".database_01` and stored in a TEXTFILE format.

```sql
create table  "test.postgresql_test".database_01.table_01
(
name varchar,
salary int
);
```

Drop a schema:

```sql
drop schema "test.postgresql_test".database_01;
```

Drop a table:

```sql
drop table "test.postgresql_test".database_01.table_01;
```

### Writing data

Insert data into the table `table_01`:

```sql
insert into  "test.postgresql_test".database_01.table_01 (name, salary) values ('ice', 12);
```

Insert data into the table `table_01` from select:

```sql
insert into  "test.postgresql_test".database_01.table_01 (name, salary) select * from "test.postgresql_test".database_01.table_01;
```

### Querying data

Query the `table_01` table:

```sql
select * from "test.postgresql_test".database_01.table_01;
```

### Modify a table

Add a new column `age` to the `table_01` table:

```sql
alter table "test.postgresql_test".database_01.table_01 add column age int;
```

Drop a column `age` from the `table_01` table:

```sql
alter table "test.postgresql_test".database_01.table_01 drop column age;
```

rename the `table_01` table to `table_02`:

```sql
alter table "test.postgresql_test".database_01.table_01 rename to "test.postgresql_test".database_01.table_02;
```