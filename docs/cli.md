---
title: 'Apache Gravitino Command Line Interface'
slug: /cli
keyword: cli
last_update:
  date: 2024-10-23
  author: justinmclean
license: 'This software is licensed under the Apache License version 2.'
---

This document primarily outlines how users can manage metadata within Apache Gravitino using the Command Line Interface (CLI). The CLI is accessible via a terminal window as an alternative to writing code or using the REST interface.

Currently, you can view basic metadata information for metalakes, catalogs, schema, and tables. The ability to create, update, and delete metalakes and support additional entities in planned in the near future.

## Running the CLI

You can set up an alias for the command like so:

```bash
alias gcli='java -jar ../../cli/build/libs/gravitino-cli-*-incubating-SNAPSHOT.jar'
```

Or use the `gcli.sh` script found in the `clients/cli/bin/` directory to run the CLI.

## Usage

 To run the Gravitino CLI, use the following command structure:

 ```bash
 usage: gcli [metalake|catalog|schema|table] [list|details|create|delete|update] [options]
 Options
 -f,--name <arg>       full entity name (dot separated)
 -h,--help             command help information
 -i,--ignore           Ignore client/sever version check
 -m,--metalake <arg>   Metalake name
 -r,--server           Gravitino server version
 -u,--url <arg>        Gravitino URL (default: http://localhost:8090)
 -v,--version          Gravitino client version
 ```

## Commands

The following commands are available for entity management:

- list: List available entities
- details: Show detailed information about an entity
- create: Create a new entity
- delete: Delete an existing entity
- update: Update an existing entity

### Setting the Metalake name

As dealing with one Metalake is a typical scenario, you can set the Metalake name in several ways.

1. Passed in on the command line via the `--metalake` parameter.
2. Set via the `GRAVITINO_METALAKE` environment variable.

The command line option overrides the environment variable.

## Setting the Gravitino URL

As you need to set the Gravitino URL for every command, you can set the URL in several ways.

1. Passed in on the command line via the `--url` parameter.
2. Set via the 'GRAVITINO_URL' environment variable.

The command line option overrides the environment variable.

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

If the client and server are running different versions of the Gravitino software then you need an additional `--ignore` option for the command to run.

### Metalake

#### Show all metalakes

```bash
gcli metalake list
```

#### Show a metalake details

```bash
gcli metalake details --metalake metalake_demo 
```

### Catalog

#### Show all catalogs in a metalake

```bash
gcli catalog list --metalake metalake_demo
```

#### Show a catalogs details

```bash
gcli catalog details --metalake metalake_demo --name catalog_postgres
```

### Schema

#### Show all schemas in a catalog

```bash
gcli schema list --metalake metalake_demo --name catalog_postgres
```

#### Show a schema details

```bash
gcli schema details --metalake metalake_demo --name catalog_postgres.hr
```

### Table

#### Show all tables

```bash
gcli table list --metalake metalake_demo --name catalog_postgres.hr
```

#### Show tables details

```bash
gcli column list --metalake metalake_demo --name catalog_postgres.hr.departments
```
