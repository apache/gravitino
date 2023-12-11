---
title: "Trino using Gravitino examples"
slug: /trino-connector/example
keyword: gravition connector trino
license: Copyright 2023 Datastrato Pvt. This software is licensed under the Apache License version 2.
---
## Basic usage examples

First, you need to create a metalake and catalog in Gravitino.
For example, create a new metalake named `test` and create a new catalog named `hive_test` using the hive provider.

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