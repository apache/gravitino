---
title: 'Apache Gravitino Command Line Interface'
slug: /cli
keyword: cli
last_update:
  date: 2024-10-23
  author: justinmclean
license: 'This software is licensed under the Apache License version 2.'
---

This document provides guidance on managing metadata within Apache Gravitino using the Command Line Interface (CLI). The CLI offers a terminal based alternative to using code or the REST interface for metadata management.

Currently, the CLI allows users to view metadata information for metalakes, catalogs, schemas, and tables. Future updates will expand on these capabilities to include roles, users, and tags.

## Running the CLI

You can configure an alias for the CLI for ease of use, with the following command:

```bash
alias gcli='java -jar ../../cli/build/libs/gravitino-cli-*-incubating-SNAPSHOT.jar'
```

Or you use the `gcli.sh` script found in the `clients/cli/bin/` directory to run the CLI.

## Usage

The general structure for running commands with the Gravitino CLI is `gcli entity command [options]`.

 ```bash
 usage: gcli [metalake|catalog|schema|table|column] [list|details|create|delete|update|set|remove|properties] [options]
 Options
 -b,--bootstrap <arg>   Kafka bootstrap servers
 -c,--comment <arg>     entity comment
 -d,--database <arg>    database name
 -h,--help              command help information
 -i,--ignore            Ignore client/sever version check
 -j,--jdbcurl <arg>     JDBC URL
 -l,--user <arg>        database username
 -m,--metastore <arg>   Hive metastore URI
 -n,--name <arg>        full entity name (dot separated)
 -p,--provider <arg>    provider one of hadoop, hive, mysql, postgres,
                        iceberg, kafka
 -r,--rename <arg>      new entity name
 -u,--url <arg>         Gravitino URL (default: http://localhost:8090)
 -v,--value <arg>       property value
 -w,--warehouse <arg>   warehouse name
 -z,--password <arg>    database password
 ```

## Commands

The following commands are used for entity management:

- list: List available entities
- details: Show detailed information about an entity
- create: Create a new entity
- delete: Delete an existing entity
- update: Update an existing entity
- set: Set a property on an entity
- remove: Remove a property from an entity
- properties: Display an entities properties

### Setting the Metalake name

As dealing with one Metalake is a typical scenario, you can set the Metalake name in several ways so it doesn't need to be passed on the command line.

1. Passed in on the command line via the `--metalake` parameter.
2. Set via the `GRAVITINO_METALAKE` environment variable.
3. Stored in the Gravitino CLI configuration file.

The command line option overrides the environment variable and the environment variable overrides the configuration file.

## Setting the Gravitino URL

As you need to set the Gravitino URL for every command, you can set the URL in several ways.

1. Passed in on the command line via the `--url` parameter.
2. Set via the 'GRAVITINO_URL' environment variable.
3. Stored in the Gravitino CLI configuration file.

The command line option overrides the environment variable and the environment variable overrides the configuration file.

## Gravitino CLI configuration file

The gravitino CLI can read commonly used CLI options from a configuration file. By default, the file is `.gravitino` in the user's home directory. The metalake, URL and ignore parameters can be set in this file.

```text
#
# Gravitino CLI configuration file
#

# Metalake to use
metalake=metalake_demo

# Gravitino server to connect to
URL=http://localhost:8090

# Ignore client/server version mismatch
ignore=true

```


## Manage metadata

All the commands are performed by using the [Java API](api/java-api) internally.

### Display help

To display help on command usage:

```bash
gcli --help
```

### Display client version

To display the client version:

```bash
gcli --version
```

### Display server version

To display the server version:

```bash
gcli --server
```

### Client/server version mismatch

If the client and server are running different versions of the Gravitino software then you may need to ignore the client/server version check for the command to run. This can be done in several ways:

1. Passed in on the command line via the `--ignore` parameter.
2. Set via the `GRAVITINO_IGNORE` environment variable.
3. Stored in the Gravitino CLI configuration file.

### Metalake commands

#### Show all metalakes

```bash
gcli metalake list
```

#### Show a metalake details

```bash
gcli metalake details --metalake metalake_demo
```

#### Create a metalake

```bash
gcli metalake create --metalake my_metalake --comment "This is my metalake"
```

#### Delete a metalake

```bash
gcli metalake delete --metalake my_metalake
```

#### Rename a metalake

```bash
gcli metalake update --metalake metalake_demo --rename demo
```

#### Update a metalake's comment

```bash
gcli metalake update --metalake metalake_demo --comment "new comment"
```

#### Display a metalake's properties

```bash
gcli metalake properties --metalake metalake_demo
```

#### Set a metalake's property

```bash
gcli metalake set --metalake metalake_demo --property test --value value
```

#### Remove a metalake's property

```bash
gcli metalake remove --metalake metalake_demo --property test
```

### Catalog

#### Show all catalogs in a metalake

```bash
gcli catalog list --metalake metalake_demo
```

#### Show a catalog details

```bash
gcli catalog details --metalake metalake_demo --name catalog_postgres
```

#### Creating a catalog

The type of catalog to be created is specified by the `--provider` option. Different catalogs require different options, for example, a Hive catalog requires a metastore URI to be passed in as an option.

##### Create a Hive catalog

```bash
gcli catalog create --metalake metalake_demo --name hive --provider hive --metastore thrift://hive-host:9083
```

##### Create an Iceberg catalog

```bash
gcli catalog create --metalake metalake_demo --name iceberg --provider iceberg --metastore thrift://hive-host:9083 --warehouse hdfs://hdfs-host:9000/user/iceberg/warehouse
```

##### Create a MySQL catalog

```bash
gcli catalog create --metalake metalake_demo --name mysql --provider mysql --jdbcurl "jdbc:mysql://mysql-host:3306?useSSL=false" --user user --password password
```

##### Create a Postgres catalog

```bash
gcli catalog create --metalake metalake_demo --name postgres --provider postgres --jdbcurl jdbc:postgresql://postgresql-host/mydb --user user --password password -database db
```

##### Create a Kafka catalog

```bash
gcli catalog create --metalake metalake_demo --name kafka --provider kafka --bootstrap 127.0.0.1:9092,127.0.0.2:9092
```

#### Delete a catalog

```bash
gcli catalog delete --metalake metalake_demo --name hive
```

#### Rename a catalog

```bash
gcli catalog update --metalake metalake_demo --name catalog_mysql --rename mysql
```

#### Change a catalog comment

```bash
gcli catalog update --metalake metalake_demo --name catalog_mysql --comment "new comment"
```

#### Display a catalog's properties

```bash
gcli catalog properties --metalake metalake_demo --name catalog_mysql
```

#### Set a catalog's property

```bash
gcli catalog set --metalake metalake_demo --name catalog_mysql --property test --value value
```

#### Remove a catalog's property

```bash
gcli catalog remove --metalake metalake_demo --name catalog_mysql --property test
```

### Schema

#### Show all schemas in a catalog

```bash
gcli schema list --metalake metalake_demo --name catalog_postgres
```

#### Show schema details

```bash
gcli schema details --metalake metalake_demo --name catalog_postgres.hr
```

#### Create a schema

```bash
gcli schema create --metalake metalake_demo --name catalog_postgres.new_db
```

#### Display schema properties

```bash
gcli schema properties --metalake metalake_demo --name catalog_postgres.hr -i
```

Setting and removing schema properties is not currently supported by the Java API or the Gravitino CLI.

### Table

#### Show all tables

```bash
gcli table list --metalake metalake_demo --name catalog_postgres.hr
```

#### Show tables details

```bash
gcli column list --metalake metalake_demo --name catalog_postgres.hr.departments
```

#### Delete a table

```bash
gcli table delete --metalake metalake_demo --name catalog_postgres.hr.salaries
```
