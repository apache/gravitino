---
title: "Gravitino connector - MySQL catalog"
slug: /trino-connector/catalog-mysql
keyword: gravitino connector trino
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

The MySQL catalog allows querying and creating tables in an external MySQL instance. 
You can join data between different systems like MySQL and Hive, or between two different MySQL instances by this.

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
Currently, it doesn't support certain query optimizations, such as indexes and pushdowns.

## Table and Schema properties

MySQL's tables and schemas cannot support properties.

## Basic usage examples

You need to do the following steps before you can use the MySQL catalog in Trino through Gravitino.

- Create a metalake and catalog in Gravitino. Assuming that the metalake name is `test` and the catalog name is `mysql_test`,
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
  "name": "mysql_test",
  "type": "RELATIONAL",
  "comment": "comment",
  "provider": "jdbc-mysql",
  "properties": {
    "jdbc-url": "jdbc:mysql://mysql-host:3306?useSSL=false",
    "jdbc-user": "<username>",
    "jdbc-password": "<password>"
    "jdbc-driver": "com.mysql.cj.jdbc.Driver"
  }
}' http://gravitino-host:8090/api/metalakes/test/catalogs
```

For More information about the MySQL catalog, please refer to [MySQL catalog](../jdbc-mysql-catalog.md).

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
 test.mysql_test
(4 rows)

Query 20231017_082503_00018_6nt3n, FINISHED, 1 node
```

The `gravitino` catalog is a catalog defined By Trino catalog configuration. 
The `test.mysql_test` catalog is the catalog created by you in Gravitino.
Other catalogs are regular user-configured Trino catalogs.

### Creating tables and schemas

Create a new schema named `database_01` in `test.mysql_test` catalog.

```sql
CREATE SCHEMA "test.mysql_test".database_01;
```

Create a new table named `table_01` in schema `"test.mysql_test".database_01`.

```sql
CREATE TABLE  "test.mysql_test".database_01.table_01
(
name varchar,
salary int
);
```

### Writing data

Insert data into the table `table_01`:

```sql
INSERT INTO "test.mysql_test".database_01.table_01 (name, salary) VALUES ('ice', 12);
```

Insert data into the table `table_01` from select:

```sql
INSERT INTO "test.mysql_test".database_01.table_01 (name, salary) SELECT * FROM "test.mysql_test".database_01.table_01;
```

### Querying data

Query the `table_01` table:

```sql
SELECT * FROM "test.mysql_test".database_01.table_01;
```

### Modify a table

Add a new column `age` to the `table_01` table:

```sql
ALTER TABLE "test.mysql_test".database_01.table_01 ADD COLUMN age int;
```

Drop a column `age` from the `table_01` table:

```sql
ALTER TABLE "test.mysql_test".database_01.table_01 DROP COLUMN age;
```

Rename the `table_01` table to `table_02`:

```sql
ALTER TABLE "test.mysql_test".database_01.table_01 RENAME TO "test.mysql_test".database_01.table_02;
```

### DROP

Drop a schema:

```sql
DROP SCHEMA "test.mysql_test".database_01;
```

Drop a table:

```sql
DROP TABLE "test.mysql_test".database_01.table_01;
```