---
title: "Upgrade Gravitino"
slug: "/how-to-upgrade"
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

### Step 1: Shut Down the Gravitino Instance

Shut down the Gravitino instance and restrict access to the
Apache Gravitino's database. It is very important that no one else
accesses or modifies the contents of database while you are
performing the schema upgrade.

### Step 2: Back Up the Gravitino Instance

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
custom format, which can be restored with `pg_restore`. Unlike a
schema-only dump, this backup includes both schema and data, which
is necessary for a full rollback.

If you are running an Iceberg REST Catalog (IRC) service backed by a separate PostgreSQL database,
back it up as well:

```shell
pg_dump -U username -h hostname -d irc_database_name -n schema_name -Fc -f data_backup_irc.dump
```

### Step 3: Dump the Gravitino Database

Dump your Gravitino database schema to a file

#### MySQL

Use the mysqldump utility to dump the database schema to a file:

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

Note: The `-s` flag dumps schema-only in plain SQL format. This is
different from the `-Fc` custom format used in Step 2 for backup,
which produces a binary dump that must be restored with `pg_restore`.

### Step 4: Determine Differences Between the Existing Schema and the Official Schema

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

### Step 5: Apply the Upgrade Scripts

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

### Step 6: Verify the Upgrade

The final step of the upgrade process is validating your freshly
upgraded schema against the official schema for your particular
version of Gravitino. This is accomplished by repeating steps (3) and
(4), but this time comparing against the official version of the
upgraded schema, e.g. if you upgraded the schema to Gravitino 0.8.0 then
you will want to compare your schema dump against the contents of
`schema-0.8.0-<type>.sql`

## Upgrading via Helm Chart

:::note
The Gravitino Helm chart does not currently support automatic schema migration. Before running
`helm upgrade`, you must manually back up your database (see [Step 2](#step-2-backup-your-gravitino-instance))
and apply the appropriate SQL upgrade scripts (see [Step 5](#step-5-apply-the-upgrade-scripts)).
:::

This section describes how to upgrade a Gravitino deployment managed by the Gravitino Helm chart

### Step 1: Prepare the new values file

Create a new values file for the target version (e.g., `values-<new-version>.yaml`) based on
the previous one. Update `image.tag` to the target version and apply any version-specific field
changes listed in the sections below.

#### 1.2.0 → 1.3.0

| Field                                         | 1.2.0                  | 1.3.0                 |
| --------------------------------------------- | ---------------------- | --------------------- |
| `image.tag`                                   | `1.2.0`                | `1.3.0`               |
| `env.GRAVITINO_HOME`                          | `/root/gravitino`      | `/opt/gravitino`      |
| `extraVolumeMounts[gravitino-log].mountPath`  | `/root/gravitino/logs` | `/opt/gravitino/logs` |

Key points:
- The `GRAVITINO_HOME` path changed from `/root/gravitino` to `/opt/gravitino`. Make sure
  `extraVolumeMounts` and any other path references are updated consistently.

### Step 2: Run the Helm upgrade

```shell
helm upgrade gravitino <path-to-gravitino-helm-chart>/gravitino-helm-<new-version>.tgz \
  -n <namespace> \
  -f values-<new-version>.yaml
```

### Step 3: Verify the rollout

Check that all pods come up healthy with the new image:

```shell
kubectl rollout status deployment/<release-name>-gravitino-helm -n <namespace>
kubectl get pods -n <namespace>
```

Confirm the running image tag matches the target version:

```shell
kubectl get pods -n <namespace> -o jsonpath='{.items[*].spec.containers[*].image}'
```

## Rollback if Upgrade Fails

> **Important:** Stop the Gravitino server before restoring the database
> to avoid data corruption from concurrent writes.

If you encounter errors during the upgrade process, you can
restore your database from the backup created in Step 2.

### MySQL

Use the backup file to restore your database. The `mysqldump`
backup contains `DROP TABLE` statements by default (via `--opt`),
so it will replace the upgraded tables automatically:

```shell
mysql -u username -h hostname --database=db_name < backup.sql
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
pg_restore -U username -h hostname -d database_name -n schema_name --clean --if-exists --single-transaction data_backup.dump
```

Note: `pg_restore --clean` only drops objects that are present in the
dump file. If the upgrade scripts added new tables or sequences not
present in the backup, those objects will not be removed automatically.
Verify the schema matches the expected state after restoring.

After restoring, verify that your Gravitino instance starts
successfully with the restored database before attempting the
upgrade again.

<img src="https://analytics.apache.org/matomo.php?idsite=62&rec=1&bots=1&action_name=HowToUpgrade" alt="" />
