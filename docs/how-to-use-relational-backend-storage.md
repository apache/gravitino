---
title: How to use relational backend storage
slug: /how-to-use-relational-backend-storage
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Before `0.6.0-incubating`, Apache Gravitino can use a KV store or a relational backend to store metadata.
Starting from `0.6.0-incubating`, Gravitino only supports using RDBMS to store its metadata.
This page shows you how to configure a relational backend storage for Gravitino.

The reason to only support relational backend was mainly due to the fact
most users are accustomed to using RDBMS and some users even don't have a KV storage.
The deployment of a relational backend is usually straightforward in a production environment,
so the users can quickly deploy Gravitino and begin their journey..

### Backend storage supported

Currently, relational backend storage supports the `JDBCBackend` and `H2`,
`MySQL` and `PostgreSQL` are supported currently for `JDBCBackend`,
`H2` is the default storage for `JDBCBackend`.

## H2

As mentioned above, `H2` is the default storage for `JDBCBackend`.
You can use `H2` directly without any additional configuration.

## MySQL

### Prerequisites

+ MySQL 5.7 or 8.0.
+ Gravitino distribution package.
+ MySQL connector JAR (should be compatible with the version of MySQL instance).

### Install

1. Get the initialization script

   Follow the [install guide](./how-to-install.md) to install the distribution package.
   You can then find the *initialization script* in the directory:

   ```text
   ${GRAVITINO_HOME}/scripts/mysql/
   ```

   The script name is like `schema-<version>-mysql.sql`,
   where `<version>` depends on your Gravitino version.
   If you were using an older version of the script, you can run the script
   `upgrade-<old version>-to-<new version>-mysql.sql` to upgrade the backend storage.

1. Initialize the database

   [Create a database in MySQL](https://www.mysqltutorial.org/mysql-basics/mysql-create-database/),
   and execute the initialization script.

1. Place the MySQL connector Jar

   You should download the MySQL connector JAR for the your MySQL version.
   The connector's name is like `mysql-connector-java-<version>.jar`.
   For example, you can download it from [maven-central-repo](https://repo1.maven.org/maven2/mysql/mysql-connector-java/)),
   and place it in the distribution package directory `${GRAVITINO_HOME}/libs/`.

1. Set up the Apache Gravitino server configs

   Find the [server configuration file](./admin/server-config.md)
   in the `${GRAVITINO_HOME}/conf/` directory, and set the following options:

   ```text
   gravitino.entity.store = relational
   gravitino.entity.store.relational = JDBCBackend
   # JDBC URL, e.g. jdbc:mysql://<host>:<port>/<DB_NAME>
   gravitino.entity.store.relational.jdbcUrl = <JDBC_URL>
   # JDBC driver name, e.g. com.mysql.cj.jdbc.Driver.
   gravitino.entity.store.relational.jdbcDriver = <DRIVER_NAME>
   gravitino.entity.store.relational.jdbcUser = <USER_NAME>
   gravitino.entity.store.relational.jdbcPassword = <PASSWORD>
   ```

1. Start the server

   Run the script in the distribution package directory to start the server:

   ```shell
   ./${GRAVITINO_HOME}/bin/gravitino.sh start
   ```

## PostgreSQL

### Prerequisites

- PostgreSQL 12~16. Other versions may work, but are fully tested for compatibility.
- Gravitino distribution package with version `0.7.0`+.
- PostgreSQL connector JAR (Should be compatible with your PostgreSQL version).

### Install

1. Get the initialization script

   Follow the [install guide](./how-to-install.md) to install the distribution package.
   You can then find the *initialization script* in the directory `${GRAVITINO_HOME}/scripts/postgresql/`.

   The script name is like `schema-<version>-postgresql.sql`,
   where the `<version>` depends on your Gravitino version.
   If you were using an older version of the script, you can use
   `upgrade-<old version>-to-<new version>-postgresql.sql` to upgrade the schema.

1. Initialize the database

   [Create a database](https://www.postgresql.org/docs/current/sql-createdatabase.html),
   and [a schema](https://www.postgresql.org/docs/current/sql-createschema.html) in PostgreSQL.
   and then execute the *initialization script*. For example:

   ```postgresql
   psql --username=postgres --password 
   password:

   create database YOUR_DATABASE;
   \c YOUR_DATABSE
   create schema YOUR_SCHEMA;
   set search_path to YOUR_SCHEMA;
   \i schema-{version}-postgresql.sql
   ```

   :::note
   Database and schema are required parameters in the Gravitino PostgreSQL JDBC URL.
   Users should create them in advance before setting the JDBC URL.
   :::

1. Prepare the PostgreSQL connector JAR

   Download and install the PostgreSQL connector JAR for the PostgreSQL version you use.
   The name of the JAR file is something like `postgresql-<version>.jar`.
   For example, you can download it from [PostgreSQL-driver-jar](https://jdbc.postgresql.org/download/postgresql-42.7.0.jar)),
   and place the JAR in the distribution package directory `${GRAVITINO_HOME}/libs/`.

1. Set up the Apache Gravitino server configs

   Find the [server configuration file](./admin/server-config.md)
   in the `${GRAVITINO_HOME}/conf/` directory, and set the following options:

   ```text
   gravitino.entity.store = relational
   gravitino.entity.store.relational = JDBCBackend
   # JDBC URL, e.g. jdbc:postgresql://<host>:5432/<DB_NAME>?currentSchema=<SCHEMA_NAME>
   gravitino.entity.store.relational.jdbcUrl = <JDBC_URL>
   # JDBC driver name, e.g. org.postgresql.Driver
   gravitino.entity.store.relational.jdbcDriver = <DRIVER_NAME>
   gravitino.entity.store.relational.jdbcUser = <USERNAME>
   gravitino.entity.store.relational.jdbcPassword = <PASSWORD>
   ```

1. Start the server

   Run the script in the distribution package directory to start the server:

   ```shell
   ./${GRAVITINO_HOME}/bin/gravitino.sh start
   ```

