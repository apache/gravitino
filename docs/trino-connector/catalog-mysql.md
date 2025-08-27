---
title: "Apache Gravitino Trino connector - MySQL catalog"
slug: /trino-connector/catalog-mysql
keyword: gravitino connector trino
license: "This software is licensed under the Apache License version 2."
---

The MySQL catalog allows querying and creating tables in an external MySQL instance. 
You can join data between different systems like MySQL and Hive, or between two different MySQL instances by this.

## Requirements

To connect to MySQL, you need:
- MySQL 5.7, 8.0 or higher.
- Network access from the Trino coordinator and workers to MySQL. Port 3306 is the default port.

## Create table

At present, the Apache Gravitino Trino connector only supports basic MySQL table creation statements, which involve fields, null allowances, comments, primary keys, indexes, default values and auto-increment.

The Gravitino Trino connector does not support `CREATE TABLE AS SELECT`.

## Alter table

Support for the following alter table operations:
- Rename table
- Add a column
- Drop a column
- Change a column type
- Set a table property

## Select

The Gravitino Trino connector supports most SELECT statements, allowing the execution of queries successfully.
Currently, it doesn't support certain query optimizations, such as indexes and pushdowns.

## Update

Only `UPDATE` statements with constant assignments and predicates are supported. See also [UPDATE limitation](https://trino.io/docs/current/connector/mysql.html#update-limitation).

## Delete

If the `WHERE` clause is specified, only the matching rows are deleted. Otherwise, all rows from the table are deleted. See also [DELETE limitation](https://trino.io/docs/current/connector/mysql.html#delete-limitation).

## Merge

Not support.

## Table and Schema properties

MySQL's schemas cannot support properties.

The following are supported MySQL table properties:

| Property name                      | Type   | Default Value  | Description                                                                                                                             | Required | Since Version |
|------------------------------------|--------|----------------|-----------------------------------------------------------------------------------------------------------------------------------------|----------|---------------|
| engine                             | string | InnoDB         | The engine that MySQL table uses.                                                                                                       | No       | 0.4.0         |
| auto_increment_offset              | string | (none)         | The auto increment offset for the table.                                                                                                | No       | 0.4.0         |
| primary_key                        | list   | (none)         | The primary keys for the table, can choose multi columns as the table primary key. All key columns must be defined as `NOT NULL`.       | No       | 1.0.0         |
| unique_key                         | list   | (none)         | The unique keys for the table, can choose multi columns for multi unique key. Each unique key should be defined as `keyName:col1,col2`. | No       | 1.0.0         |

The following are supported MySQL column properties:

| Property name                      | Type    | Default Value | Description                                       | Required | Since Version |
|------------------------------------|---------|---------------|---------------------------------------------------|----------|---------------|
| auto_increment                     | boolean | false         | The auto increment column.                        | No       | 1.0.0         |
| default                            | string  | (none)        | The default value for column.                     | No       | 1.0.0         |

**Note:** Currently, creating tables only supports constant default values and does not support expression default values, 
and `show create table` also exclusively renders constant default values in its output.
The following are Trino type which support configuration of default values:

| Type name | Default Value example                   |
|-----------|-----------------------------------------|
| TINYINT   | 1                                       |
| SMALLINT  | 1                                       | 
| INT       | 1                                       | 
| BIGINT    | 1                                       | 
| REAL      | 1.0                                     | 
| DOUBLE    | 1.0                                     | 
| DECIMAL   | 1.0                                     | 
| VARCHAR   | abc                                     | 
| CHAR      | abc                                     | 
| DATE      | 2025-08-07                              | 
| TIME      | 01:01:01                                | 
| TIMESTAMP | 2025-08-07 01:01:01 (CURRENT_TIMESTAMP) | 

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
 mysql_test
(4 rows)

Query 20231017_082503_00018_6nt3n, FINISHED, 1 node
```

The `gravitino` catalog is a catalog defined By Trino catalog configuration. 
The `mysql_test` catalog is the catalog created by you in Gravitino.
Other catalogs are regular user-configured Trino catalogs.

### Creating tables and schemas

Create a new schema named `database_01` in `test.mysql_test` catalog.

```sql
CREATE SCHEMA mysql_test.database_01;
```

Create a new table named `table_01` in schema `mysql_test.database_01`.

```sql
CREATE TABLE mysql_test.database_01.table_01
(
name varchar,
salary int
);
```

Create a new table named `table_index` in schema `mysql_test.database_01` with primary keys and indexes.

```sql
CREATE TABLE mysql_test.database_01.table_index (
   key1 integer NOT NULL,
   key2 integer,
   key3 integer,
   key4 integer,
   key5 integer NOT NULL,
   col1 integer
)
COMMENT ''
WITH (
   engine = 'InnoDB',
   primary_key = ARRAY['key5','key1'],
   unique_key = ARRAY['unique_key1:key2','unique_key2:key4,key3']
);
```

Create a new table named `table_column_properties` in schema `mysql_test.database_01` with auto_increment and default.

```sql
CREATE TABLE mysql_test.database_01.table_column_properties(
    key1 INT NOT NULL WITH (auto_increment=true),
    f1 VARCHAR(200) WITH (default='VARCHAR'),
    f2 CHAR(20) WITH (default='CHAR') ,
    f4 DECIMAL(10, 3) WITH (default='0.3') ,
    f5 REAL WITH (default='0.3') ,
    f6 DOUBLE WITH (default='0.3') ,
    f8 TINYINT WITH (default='1') ,
    f9 SMALLINT WITH (default='1') ,
    f10 INT WITH (default='1') ,
    f11 INTEGER WITH (default='1') ,
    f12 BIGINT WITH (default='1'),
    f13 DATE WITH (default='2024-04-01'),
    f14 TIME WITH (default='08:00:00'),
    f15 TIMESTAMP WITH (default='2012-12-31 11:30:45'),
    f16 TIMESTAMP WITH TIME ZONE WITH (default='2012-12-31 11:30:45'),
    f17 TIMESTAMP WITH TIME ZONE WITH (default='CURRENT_TIMESTAMP')
)
WITH (
   primary_key = ARRAY['key1']
);
```

### Writing data

Insert data into the table `table_01`:

```sql
INSERT INTO mysql_test.database_01.table_01 (name, salary) VALUES ('ice', 12);
```

Insert data into the table `table_01` from select:

```sql
INSERT INTO mysql_test.database_01.table_01 (name, salary) SELECT * FROM "test.mysql_test".database_01.table_01;
```

Update data into the table `table_01`:

```sql
UPDATE mysql_test.database_01.table_01 SET name = 'ice_update' WHERE salary = 12;
```

Delete data from the table `table_01`:

```sql
DELETE FROM mysql_test.database_01.table_01 WHERE salary = 12;
DELETE FROM mysql_test.database_01.table_01;
```

### Querying data

Query the `table_01` table:

```sql
SELECT * FROM mysql_test.database_01.table_01;
```

### Modify a table

Add a new column `age` to the `table_01` table:

```sql
ALTER TABLE mysql_test.database_01.table_01 ADD COLUMN age int;
```

Drop a column `age` from the `table_01` table:

```sql
ALTER TABLE mysql_test.database_01.table_01 DROP COLUMN age;
```

Rename the `table_01` table to `table_02`:

```sql
ALTER TABLE mysql_test.database_01.table_01 RENAME TO mysql_test.database_01.table_02;
```

### DROP

Drop a schema:

```sql
DROP SCHEMA mysql_test.database_01;
```

Drop a table:

```sql
DROP TABLE mysql_test.database_01.table_01;
```
