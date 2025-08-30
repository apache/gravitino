---
title: "Apache Gravitino Trino connector - PostgreSQL catalog"
slug: /trino-connector/catalog-postgresql
keyword: gravitino connector trino
license: "This software is licensed under the Apache License version 2."
---

The PostgreSQL catalog allows querying and creating tables in an external PostgreSQL database. 
This can be used to join data between different systems like PostgreSQL and Hive, or between different PostgreSQL instances.

## Requirements

To connect to PostgreSQL, you need:
- PostgreSQL 10.x or higher.
- Network access from the Trino coordinator and workers to PostgreSQL. Port 5432 is the default port.

## Case sensitivity

PostgreSQL treats unquoted identifiers as case insensitive.
For example, the table name MyTable is equivalent to mytable and MYTABLE.

However, if you create a table with quoted identifiers, such as "MyTable", it becomes case sensitive and must be referenced exactly as "MyTable".

When using the Gravitino Trino connector with PostgreSQL, you must use unquoted identifiers to avoid case sensitivity issues.
Otherwise, schema names, table names or column names containing uppercase letters may not be found.

## Create table

At present, the Apache Gravitino Trino connector only supports basic PostgreSQL table creation statements, which involve fields, null allowances, and comments. However, it does not support advanced features like primary keys, indexes, default values, and auto-increment.

The Gravitino Trino connector does not support `CREATE TABLE AS SELECT`.

## Alter table

Gravitino Trino connector supports the following alter table operations:
- Rename table
- Add a column
- Drop a column
- Rename a column
- Change a column type
- Set a table property

## Select

The Gravitino Trino connector supports most SELECT statements, allowing the execution of queries successfully.
Currently, it doesn't support certain query optimizations, such as indexes and pushdowns.

## Update

Only `UPDATE` statements with constant assignments and predicates are supported. See also [UPDATE limitation](https://trino.io/docs/current/connector/postgresql.html#update-limitation).

## Delete

If the `WHERE` clause is specified, only the matching rows are deleted. Otherwise, all rows from the table are deleted. See also [DELETE limitation](https://trino.io/docs/current/connector/postgresql.html#delete-limitation).

## Table and Schema properties

PostgreSQL's tables and schemas cannot support properties.

## Basic usage examples

You need to do the following steps before you can use the PostgreSQL catalog in Trino through Gravitino.

- Create a metalake and catalog in Gravitino. Assuming that the metalake name is `test` and the catalog name is `postgresql_test`, then you can use the following code to create them in Gravitino:

```bash
curl -X POST -H "Content-Type: application/json" \
-d '{
  "name": "test",
  "comment": "comment",
  "properties": {}
}' http://gravitino-host:8090/api/metalakes

curl -X POST -H "Content-Type: application/json" \
-d '{
  "name": "postgresql_test",
  "type": "RELATIONAL",
  "comment": "comment",
  "provider": "jdbc-postgresql",
  "properties": {
    "jdbc-url": "jdbc:postgresql://postgresql-host/mydb",
    "jdbc-user": "<user>",
    "jdbc-password": "<password>",
    "jdbc-database": "mydb",
    "jdbc-driver": "org.postgresql.Driver"
  }
}' http://gravitino-host:8090/api/metalakes/test/catalogs
```
For more information about the PostgreSQL catalog, please refer to [PostgreSQL catalog](../jdbc-postgresql-catalog.md).

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
 postgresql_test
(4 rows)

Query 20231017_082503_00018_6nt3n, FINISHED, 1 node
```

The `gravitino` catalog is a catalog defined By Trino catalog configuration. 
The `postgresql_test` catalog is the catalog created by you in Gravitino.
Other catalogs are regular user-configured Trino catalogs.

### Creating tables and schemas

Create a new schema named `database_01` in `postgresql_test` catalog.

```sql
CREATE SCHEMA postgresql_test.database_01;
```

Create a new table named `table_01` in schema `postgresql_test.database_01`.

```sql
CREATE TABLE postgresql_test.database_01.table_01
(
name varchar,
salary int
);
```

### Writing data

Insert data into the table `table_01`:

```sql
INSERT INTO postgresql_test.database_01.table_01 (name, salary) VALUES ('ice', 12);
```

Insert data into the table `table_01` from select:

```sql
INSERT INTO postgresql_test.database_01.table_01 (name, salary) SELECT * FROM postgresql_test.database_01.table_01;
```

Update the table `table_01`:

```sql
UPDATE postgresql_test.database_01.table_01 SET name = 'ice_update' WHERE salary = 12;
```

Delete the table `table_01`:

```sql
DELETE FROM postgresql_test.database_01.table_01 WHERE salary = 12;
DELETE FROM postgresql_test.database_01.table_01;
```

### Querying data

Query the `table_01` table:

```sql
SELECT * FROM postgresql_test.database_01.table_01;
```

### Modify a table

Add a new column `age` to the `table_01` table:

```sql
ALTER TABLE postgresql_test.database_01.table_01 ADD COLUMN age int;
```

Drop a column `age` from the `table_01` table:

```sql
ALTER TABLE postgresql_test.database_01.table_01 DROP COLUMN age;
```

Rename the `table_01` table to `table_02`:

```sql
ALTER TABLE postgresql_test.database_01.table_01 RENAME TO postgresql_test.database_01.table_02;
```

### Drop

Drop a schema:

```sql
DROP SCHEMA postgresql_test.database_01;
```

Drop a table:

```sql
DROP TABLE postgresql_test.database_01.table_01;
```
