---
title: "Apache Gravitino Trino connector - MySQL catalog"
slug: /trino-connector/catalog-mysql
keyword: gravitino connector trino
license: "This software is licensed under the Apache License version 2."
---

A MySQL catalog allows users to manage tables in an external MySQL instance. 
You can join data from different systems like MySQL and Hive,
or from two different MySQL instances using this kind of a catalog.

## Requirements

To connect to MySQL, you need:

- MySQL 5.7, 8.0 or higher.
- Network access from the Trino coordinator and workers to the MySQL server.
  Port 3306 is the default port.

## Create table

At present, the Apache Gravitino Trino connector only supports basic MySQL table creation statements,
which involve field definition, nullability, and comments.
It doesn't support advanced features like primary keys, indexes,
default values, or auto-increment fields.

The Gravitino Trino connector does not support `CREATE TABLE AS SELECT`.

## Alter table

The connector supports the following alter table operations:

- Rename table
- Add a column
- Drop a column
- Change a column type
- Set a table property

## Select

The connector supports most SELECT statements for running queries.
It doesn't support certain query optimizations, such as indexes or push-downs.

## Table and Schema properties

There is no support to properties for a MySQL table or schema.

## Basic usage examples

Before using a Gravitino MySQL catalog in Trino, you need to set up the environment.

- Create a metalake and a catalog in Gravitino.
  For example, the following commands creates a metalake named 'test'
  and a catalog named 'mysql_test':

  ```shell
  cat <<EOF >metalake.json
  {
    "name": "test",
    "comment": "comment",
    "properties": {}
  }
  EOF

  curl -X POST \
    -H "Content-Type: application/json" \
    -d '@metalake.json' \
    http://gravitino-host:8090/api/metalakes
 
  cat <<EOF >catalog.json
  {
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
  }
  EOF

  curl -X POST \
    -H "Content-Type: application/json" \
    -d '@catalog.json' \
    http://gravitino-host:8090/api/metalakes/test/catalogs
  ```

  For More information about MySQL catalogs, please refer to
  the [MySQL catalog documentation](../../catalogs/relational/jdbc/mysql.md).

- Set the configuration `gravitino.metalake` to the metalake you have created,
  and start the Trino container.

- Use the Trino CLI to connect to the Trino container and run a query.
  For example, the follwing command lists all Gravitino managed catalogs:

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

The `gravitino` catalog is a catalog defined by the Trino catalog configuration. 
The `mysql_test` catalog is the catalog you created in Gravitino.
Other catalogs are regular user-configured Trino catalogs.

### Creating tables and schemas

To create a new schema named `database_01` in the `test.mysql_test` catalog:

```sql
CREATE SCHEMA mysql_test.database_01;
```

To create a new table named `table_01` in schema `mysql_test.database_01`:

```sql
CREATE TABLE mysql_test.database_01.table_01
(
  name varchar,
  salary int
);
```

### Writing data

To insert data into the table `table_01`:

```sql
INSERT INTO mysql_test.database_01.table_01 (name, salary)
  VALUES ('ice', 12);
```

To insert data from a select clause into the table `table_01` :

```sql
INSERT INTO mysql_test.database_01.table_01 (name, salary)
  SELECT * FROM "test.mysql_test".database_01.table_01;
```

### Querying data

To query the `table_01` table:

```sql
SELECT * FROM mysql_test.database_01.table_01;
```

### Modifying a table

To add a new column `age` to the `table_01` table:

```sql
ALTER TABLE mysql_test.database_01.table_01 ADD COLUMN age int;
```

To drop a column `age` from the `table_01` table:

```sql
ALTER TABLE mysql_test.database_01.table_01 DROP COLUMN age;
```

To rename the `table_01` table to `table_02`:

```sql
ALTER TABLE mysql_test.database_01.table_01
  RENAME TO mysql_test.database_01.table_02;
```

### Dropping schemas or tables

To drop a schema:

```sql
DROP SCHEMA mysql_test.database_01;
```

To drop a table:

```sql
DROP TABLE mysql_test.database_01.table_01;
```

