---
title: How to upgrade Apache Gravitino
slug: /how-to-upgrade
license: "This software is licensed under the Apache License version 2."
---

## Introduction
This document describes how to upgrade the schema of backend
Gravitino instance from one release version of Gravitino to another
release version of Gravitino. For example, by following the steps listed
below it is possible to upgrade a Gravitino 0.6.0 schema to a
Gravitino 0.7.0 schema. Before attempting this project we
strongly recommend that you read through all of the steps in this
document and familiarize yourself with the required tools.

## Upgrade Steps

### Step 1: Shutdown your Gravitino instance
Shutdown your Gravitino instance and restrict access to the
Apache Gravitino's database. It is very important that no one else
accesses or modifies the contents of database while you are
performing the schema upgrade.

### Step 2: Backup your Gravitino instance
Create a backup of your database. This will allow
you to revert any changes made during the upgrade process if
something goes wrong. 

#### MySQL

For MySQL, you can use the following command to backup your database:

```shell
mysqldump --opt <db_name> > backup.sql
```
Note that you may also need to specify a hostname and username
using the `--host` and `--user` command line switches.

#### H2
The easiest way of accomplishing this task is
by creating a copy of the directory containing your database.

#### PostgreSQL
For PostgreSQL, you can use the following command to backup your database:

```shell
pg_dump -U username -h hostname -d database_name -n schema_name -Fc -f data_backup.dump
```
The `-Fc` option produces a compressed binary dump in PostgreSQL's
custom format, which can be restored with `pg_restore`.

### Step 3: Dump your Gravitino database
Dump your Gravitino database schema to a file

#### MySQL
You can use the mysqldump utility to dump the database schema to a file:

```shell
mysqldump --skip-add-drop-table --no-data <db_name> > schema-x.y.z-mysql.sql
```

#### H2
For H2, you can use the `Script` tool to dump the database schema to a file:

```shell
wget https://repo1.maven.org/maven2/com/h2database/h2/1.4.200/h2-1.4.200.jar
java -cp h2-1.4.200.jar org.h2.tools.Script -url "jdbc:h2:file:<db_file>;DB_CLOSE_DELAY=-1;MODE=MYSQL" -user <user> -password <password> -script backup.sql
```
Note that you may need to specify your h2 file path, username and password

#### PostgreSQL
For PostgreSQL, you can use the following command to dump the database schema to a file:

```shell
pg_dump -U username -h hostname -d database_name -n schema_name -s -f schema-x.y.z-postgresql.sql
```

### Step 4: Determine differences between your schema and the official schema
The schema upgrade scripts assume that the schema you are upgrading
closely matches the official schema for your particular version of
Gravitino. The files in this directory with names like
`schema-x.y.z-<type>.sql` contain dumps of the official schemas
corresponding to each of the released versions of Gravitino. You can
determine differences between your schema and the official schema
by comparing the contents of the official dump with the schema dump
you created in the previous step.

Some differences are acceptable and will not interfere
with the upgrade process, such as differences in object ordering or
comments. However, differences in table structures, column types, or
missing tables/indexes need to be resolved manually
or the upgrade scripts will fail to complete.

### Step 5: Apply the upgrade scripts
You are now ready to run the schema upgrade scripts. If you are
upgrading from Gravitino 0.6.0 to Gravitino 0.7.0 you need to run the
`upgrade-0.6.0-to-0.7.0-<type>.sql` script, but if you are upgrading
from 0.6.0 to 0.8.0 you will need to run the 0.6.0 to 0.7.0 upgrade
script followed by the 0.7.0 to 0.8.0 upgrade script.

#### MySQL
Assuming you are upgrading the version of Gravitino server from 0.6.0 to 0.8.0

```shell
mysql --verbose
mysql> use <db_name>;
Database changed
mysql> source upgrade-0.6.0-to-0.7.0-mysql.sql
mysql> source upgrade-0.7.0-to-0.8.0-mysql.sql
```
#### H2
For H2, you can use the `RunScript` tool to apply the upgrade script:

```shell
java -cp h2-1.4.200.jar org.h2.tools.RunScript -url "jdbc:h2:file:<db_file>;DB_CLOSE_DELAY=-1;MODE=MYSQL" -user <user> -password <password> -script upgrade-0.6.0-to-0.7.0-h2.sql
java -cp h2-1.4.200.jar org.h2.tools.RunScript -url "jdbc:h2:file:<db_file>;DB_CLOSE_DELAY=-1;MODE=MYSQL" -user <user> -password <password> -script upgrade-0.7.0-to-0.8.0-h2.sql
```

#### PostgreSQL
For PostgreSQL, you can use the following command to apply the upgrade script:

```shell
psql -U username -h hostname -d database_name -c "SET search_path TO schema_name;" -f upgrade-0.6.0-to-0.7.0-postgresql.sql
psql -U username -h hostname -d database_name -c "SET search_path TO schema_name;" -f upgrade-0.7.0-to-0.8.0-postgresql.sql
```


These scripts should run to completion without any errors. If you
do encounter errors you need to analyze the cause and attempt to
trace it back to one of the preceding steps.

### Step 6: Verify the upgrade
The final step of the upgrade process is validating your freshly
upgraded schema against the official schema for your particular
version of Gravitino. This is accomplished by repeating steps (3) and
(4), but this time comparing against the official version of the
upgraded schema, e.g. if you upgraded the schema to Gravitino 0.8.0 then
you will want to compare your schema dump against the contents of
`schema-0.8.0-<type>.sql`

## Rollback if Upgrade Fails

If you encounter errors during the upgrade process, you can
restore your database from the backup created in Step 2.

### MySQL

Use the backup file to restore your database. The `mysqldump`
backup contains `DROP TABLE` statements by default (via `--opt`),
so it will replace the upgraded tables automatically:

```shell
mysql -u username -h hostname <db_name> < backup.sql
```

### H2

Replace the current database directory with the copy you made
during the backup step:

```shell
rm -rf <db_directory>
cp -r <db_directory_backup> <db_directory>
```

### PostgreSQL

Use `pg_restore` with `--clean` to drop existing objects before
restoring them in one transaction. This is safer than manually
dropping the schema first:

```shell
pg_restore -U username -h hostname -d database_name --clean --if-exists --single-transaction data_backup.dump
```

After restoring, verify that your Gravitino instance starts
successfully with the restored database before attempting the
upgrade again.

<img src="https://analytics.apache.org/matomo.php?idsite=62&rec=1&bots=1&action_name=HowToUpgrade" alt="" />
