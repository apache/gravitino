---
title: How to use relational backend storage
slug: /how-to-use-relational-backend-storage
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Before the version `0.6.0-incubating`, Apache Gravitino supports KV and Relational backend storage to store metadata.
Since 0.6.0-incubating, Gravitino only supports using RDBMS as relational backend storage to store metadata. This doc will guide you on how to use the
relational backend storage in Gravitino.

Relational backend storage mainly aims to the users who are accustomed to using RDBMS to
store data or lack available a KV storage, and want to use Gravitino.

With relational backend storage, you can quickly deploy Gravitino in a production environment and
take advantage of relational storage to manage metadata.

### What kind of backend storage is supported

Currently, relational backend storage supports the `JDBCBackend`, and `H2`, `MySQL` and `PostgreSQL` are supported currently for `JDBCBackend`, `H2` is the
default storage for `JDBCBackend`.

## How to use H2

As mentioned above, `H2` is the default storage for `JDBCBackend`, so you can use `H2` directly without any additional configuration.

## How to use MySQL

### Prerequisites

+ MySQL 5.7 or 8.0.
+ Gravitino distribution package.
+ MySQL connector Jar (Should be compatible with the version of MySQL instance).

### Step 1: Get the initialization script

You need to `download` and `unzip` the distribution package firstly, please see
[How to install Gravitino](how-to-install.md).

Then you can get the initialization script in the directory:

```text
${GRAVITINO_HOME}/scripts/mysql/
```

The script name is like `schema-{version}-mysql.sql`, and the `version` depends on your Gravitino version.
For example, if your Gravitino version is `0.6.0-incubating`, then you can choose the **latest version** script.
If you used a legacy script, you can use `upgrade-{old version}-to-{new version}-mysql.sql` to upgrade the schema.

### Step 2: Initialize the database

Please create a database in MySQL in advance, and execute the initialization script obtained above in the database.

### Step 3: Place the MySQL connector Jar

You should **download** the MySQL connector Jar for the corresponding version of MySQL you use
(You can download it from the [maven-central-repo](https://repo1.maven.org/maven2/mysql/mysql-connector-java/)),
which is name like `mysql-connector-java-{version}.jar`.

Then please place it in the distribution package directory:

```text
${GRAVITINO_HOME}/libs/
```

### Step 4: Set up the Apache Gravitino server configs

Find the server configuration file which name is `gravitino.conf` in the distribution package directory:

```text
${GRAVITINO_HOME}/conf/
```

Then set up the following server configs:

```text
gravitino.entity.store = relational
gravitino.entity.store.relational = JDBCBackend
gravitino.entity.store.relational.jdbcUrl = ${your_jdbc_url}
gravitino.entity.store.relational.jdbcDriver = ${your_driver_name}
gravitino.entity.store.relational.jdbcUser = ${your_username}
gravitino.entity.store.relational.jdbcPassword = ${your_password}
```

### Step 5: Start the server

Finally, you can run the script in the distribution package directory to start the server:

```shell
./${GRAVITINO_HOME}/bin/gravitino.sh start
```

## How to use PostgreSQL

### Prerequisites

- PostgreSQL 12 ~ 16. For other versions, the community has not tested fully and may not be compatible.
- Gravitino distribution package with version `0.7.0` or above.
- PostgreSQL connector Jar (Should be compatible with the version of PostgreSQL instance).

### Step 1: Get the initialization script

You need to `download` and `unzip` the distribution package firstly, please see
[How to install Gravitino](how-to-install.md).

Then you can get the initialization script in the directory:

```text
${GRAVITINO_HOME}/scripts/postgresql/
```

The script name is like `schema-{version}-postgresql.sql`, and the `version` depends on your Gravitino version.
For example, if your Gravitino version is the latest release version, then you can choose the **latest version** script.
If you used a legacy script, you can use `upgrade-{old version}-to-{new version}-postgresql.sql` to upgrade the schema.

### Step 2: Initialize the database

Please create a database and schema in PostgreSQL in advance, and execute the initialization script obtained above in the database.

```postgresql
psql --username=postgres --password 
password:

create database your_database;
\c your_database
create schema your_schema;
set search_path to your_schema;
\i schema-{version}-postgresql.sql
```

:::note
Database and schema are required parameters in the Gravitino PostgreSQL JDBC URL, Users should
create them in advance and set the database and schema in the JDBC URL.
:::


### Step 3: Place the PostgreSQL connector Jar

You should **download** the PostgreSQL connector Jar for the corresponding version of PostgreSQL you use
(You can download it from the [PostgreSQL-driver-jar](https://jdbc.postgresql.org/download/postgresql-42.7.0.jar)),
which is name like `postgresql-{version}.jar`.

Then please place it in the distribution package directory:

```text
${GRAVITINO_HOME}/libs/
```

### Step 4: Set up the Apache Gravitino server configs

Find the server configuration file which name is `gravitino.conf` in the distribution package directory:

```text
${GRAVITINO_HOME}/conf/
```

Then set up the following server configs:

```text
gravitino.entity.store = relational
gravitino.entity.store.relational = JDBCBackend

## JDBC URL such as: jdbc:postgresql://localhost:5432/your_database?currentSchema=your_schema
gravitino.entity.store.relational.jdbcUrl = ${your_jdbc_url}

## JDBC driver name such as: org.postgresql.Driver
gravitino.entity.store.relational.jdbcDriver = ${your_driver_name}

gravitino.entity.store.relational.jdbcUser = ${your_username}
gravitino.entity.store.relational.jdbcPassword = ${your_password}
```

### Step 5: Start the server

Please see the above steps in the MySQL section.