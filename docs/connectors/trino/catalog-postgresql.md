---
title: "Apache Gravitino Trino connector - PostgreSQL catalog"
slug: /trino-connector/catalog-postgresql
keyword: gravitino connector trino
license: "This software is licensed under the Apache License version 2."
---

The PostgreSQL catalog allows querying and creating tables in an external PostgreSQL database. 
This can be used to join data from different systems like PostgreSQL and Hive,
or from different PostgreSQL instances.

## Requirements

To connect to PostgreSQL, you need:

- PostgreSQL 10.x or higher.
- Network access from the Trino coordinator and workers to PostgreSQL.
  Port 5432 is the default port.

## Create table

At present, the Apache Gravitino Trino connector only supports basic PostgreSQL table creation statements,
which involve field types, nullability, and comments.
It does not support advanced features like primary keys, indexes, default values,
or auto-increment fields.

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
Currently, it doesn't support certain query optimizations, such as indexes or pushdowns.

## Table and Schema properties

PostgreSQL's tables and schemas cannot support properties.

## Basic usage examples

You need to do some preparations before using the PostgreSQL catalog in Trino.

- Create a metalake and catalog in Gravitino.
  Assuming that the metalake name is `test` and the catalog name is `postgresql_test`,
  you can use the following code to create them in Gravitino:

  ```bash
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
  }
  EOF

  curl -X POST \
    -H "Content-Type: application/json" \
    -d '@catalog.json' \
    http://gravitino-host:8090/api/metalakes/test/catalogs
  ```

  For more information on the PostgreSQL catalog, please refer to
  [PostgreSQL catalog documentation](../../catalogs/relational/jdbc/postgresql.md).

- Set the configuration `gravitino.metalake` to the metalake you have created.

- Start the Trino container.

- Use the Trino CLI to connect to the Trino container and run a query.
  For example, to lsti all Gravitino managed catalogs:

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
The `postgresql_test` catalog is the catalog you just created.
Other catalogs are regular user-configured Trino catalogs.

### Creating tables and schemas

To create a new schema named `database_01` in `postgresql_test` catalog.

```sql
CREATE SCHEMA postgresql_test.database_01;
```

To create a new table named `table_01` in schema `postgresql_test.database_01`.

```sql
CREATE TABLE postgresql_test.database_01.table_01
(
name varchar,
salary int
);
```

### Writing data

To insert data into the table `table_01`:

```sql
INSERT INTO postgresql_test.database_01.table_01 (name, salary) VALUES ('ice', 12);
```

To insert data into the table `table_01` from select:

```sql
INSERT INTO postgresql_test.database_01.table_01 (name, salary)
  SELECT * FROM postgresql_test.database_01.table_01;
```

### Querying data

To query the `table_01` table:

```sql
SELECT * FROM postgresql_test.database_01.table_01;
```

### Modify a table

To add a new column `age` to the `table_01` table:

```sql
ALTER TABLE postgresql_test.database_01.table_01 ADD COLUMN age int;
```

To drop a column `age` from the `table_01` table:

```sql
ALTER TABLE postgresql_test.database_01.table_01 DROP COLUMN age;
```

To ename the `table_01` table to `table_02`:

```sql
ALTER TABLE postgresql_test.database_01.table_01
  RENAME TO postgresql_test.database_01.table_02;
```

### Drop

To drop a schema:

```sql
DROP SCHEMA postgresql_test.database_01;
```

To drop a table:

```sql
DROP TABLE postgresql_test.database_01.table_01;
```
