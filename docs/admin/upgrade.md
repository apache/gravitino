---
title: Upgrade Apache Gravitino
slug: /how-to-upgrade
license: "This software is licensed under the Apache License version 2."
---

## Introduction

This document describes how to upgrade the schema of backend Gravitino instance
from one release to another,
for example, upgrading a Gravitino 0.6.0 schema to a 0.7.0 schema.
Before attempting this upgrade we strongly recommend you to read through the whole page
and familiarize yourself with the required tools.

## Upgrade Steps

### Step 1: Shutdown your Gravitino instance

Shutdown your Gravitino instance and stop all accesses to the Apache Gravitino's database.
It is very important that no one else accesses or modifies the contents of database
while you are performing the schema upgrade.

### Step 2: Backup your Gravitino instance

Create a backup of your database.
This will allow you to revert any changes made during the upgrade if things go wrong. 

#### H2

The easiest way of backing up the H2 data is by creating a full copy of the H2 data directory.

#### MySQL

For MySQL, you can use the following command to backup your database:

```shell
mysqldump --opt <db_name> > backup.sql
```

Note that you may need to specify a hostname and username
using the `--host` and `--user` command line options.

#### PostgreSQL

For PostgreSQL, you can use the following command to backup your database:

```shell
pg_dump -U username -h hostname \
  -d database_name -n schema_name -a -F c -f data_backup.sql
```

### Step 3: Dump your Gravitino database

Dump your Gravitino database schema to a file.

#### H2

For H2, you can use the `Script` tool to dump the database schema to a file:
Note that you need to specify your H2 database file path, username and password
in the example below.

```shell
wget https://repo1.maven.org/maven2/com/h2database/h2/1.4.200/h2-1.4.200.jar
java -cp h2-1.4.200.jar org.h2.tools.Script \
  -url "jdbc:h2:file:<db_file>;DB_CLOSE_DELAY=-1;MODE=MYSQL" \
  -user <user> \
  -password <password> \
  -script backup.sql
```

#### MySQL

You can use the *mysqldump* utility to dump the database schema to a file:

```shell
mysqldump --skip-add-drop-table --no-data <db_name> > schema-x.y.z-mysql.sql
```

#### PostgreSQL

For PostgreSQL, you can use the following command to dump the database schema to a file:

```shell
pg_dump -U username -h hostname \
  -d database_name \
  -n schema_name \
  -s -F c -f schema-x.y.z-postgresql.sql
```

### Step 4: Determine differences between your schema and the official schema

The schema upgrade scripts assume that the schema you are upgrading closely matches the official schema
for your particular version of Gravitino.
The files in this directory with names like `schema-x.y.z-<type>.sql` contain dumps of the official schemas
corresponding to each of the released versions of Gravitino.
You can determine differences between your schema and the official schema
by comparing the contents of the official dump with the schema dump you created in the previous step.
Some differences are acceptable and will not interfere with the upgrade process,
but others need to be resolved manually or the upgrade scripts will fail to complete.

### Step 5: Apply the upgrade scripts

You are now ready to run the schema upgrade scripts.
If you are upgrading from Gravitino 0.6.0 to Gravitino 0.7.0,
you need to run the `upgrade-0.6.0-to-0.7.0-<type>.sql` script;
but if you are upgrading from 0.6.0 to 0.8.0,
you will need to run the 0.6.0 to 0.7.0 upgrade script
followed by the 0.7.0 to 0.8.0 upgrade script.

#### H2

For H2, you can use the `RunScript` tool to apply the upgrade script:

```shell
java -cp h2-1.4.200.jar org.h2.tools.RunScript \
  -url "jdbc:h2:file:<db_file>;DB_CLOSE_DELAY=-1;MODE=MYSQL" \
  -user <user> \
  -password <password> \
  -script upgrade-0.6.0-to-0.7.0-h2.sql

java -cp h2-1.4.200.jar org.h2.tools.RunScript \
  -url "jdbc:h2:file:<db_file>;DB_CLOSE_DELAY=-1;MODE=MYSQL" \
  -user <user> \
  -password <password> \
  -script upgrade-0.7.0-to-0.8.0-h2.sql
```

#### MySQL

Assuming you are upgrading the version of Gravitino server from 0.6.0 to 0.8.0:

```shell
mysql --verbose
mysql> use <db_name>;
Database changed
mysql> source upgrade-0.6.0-to-0.7.0-mysql.sql
mysql> source upgrade-0.7.0-to-0.8.0-mysql.sql
```

#### PostgreSQL

For PostgreSQL, you can use the following command to apply the upgrade script:

```shell
psql -U username -h hostname -d database_name \
  -c "SET search_path TO schema_name;" \
  -f upgrade-0.6.0-to-0.7.0-postgresql.sql

psql -U username -h hostname -d database_name \
  -c "SET search_path TO schema_name;" \
  -f upgrade-0.7.0-to-0.8.0-postgresql.sql
```

These scripts should run to completion without any errors.
If you do encounter errors you need to analyze the cause
and attempt to trace it back to one of the preceding steps.

### Step 6: Verify the upgrade

The final step of the upgrade process is validating your freshly upgraded schema
against the official schema for your particular version of Gravitino.
This is accomplished by repeating steps (3) and (4),
but this time comparing against the official version of the upgraded schema.
For example, if you upgraded the schema to Gravitino 0.8.0,
you will want to compare your schema dump against the contents of `schema-0.8.0-<type>.sql`

<img src="https://analytics.apache.org/matomo.php?idsite=62&rec=1&bots=1&action_name=HowToUpgrade" alt="" />

