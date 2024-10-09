<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Gravitino CLI

Apache Gravitino CLI is a command-line tool that interacts with the Gravitino server to manage and query entities like metalakes, catalogs, schemas, and tables. The tool provides options for listing information about Gravitino entities and in future versions support creating, deleting, and updating these entities.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Commands](#commands)
- [Running Tests](#running-tests)
- [Contributing](#contributing)
- [License](#license)

## Features

- Retrieve server version
- Provide help on usage
- Manage Gravitino entities such as Metalakes, Catalogs, Schemas, and Tables
- List details about Graviotino entities

## Installation

### Prerequisites

Before you can build and run this project, it is suggested you have the following installed:

- Java 11 or higher

### Build the Project

1. Clone the entire Gravitino repository:

    ```bash
    git clone https://github.com/apache/gravitino
    ```

2. Build the CLI sub-project using Gradle:

    ```bash
    ./gradlew :clients:cli:build
    ```
3. Create an alias:

    ```bash
    alias gcli='java -jar clients/cli/build/libs/gravitino-cli-0.7.0-incubating-SNAPSHOT.jar'
    ```
3. Test the command:
    ```bash
    gcli --help
    ```

## Usage

To run the Gravitino CLI, use the following command structure:

```bash
usage: gcli <entity> <action> [options]
 -b,--bootstrap <arg>   Kafka bootstrap servers
 -c,--catalog <arg>     catalog name
 -C,--create            create an entity
 -D,--details           list details about an entity
 -d,--database <arg>    database name
 -e,--entity <arg>      entity type
 -h,--help              command help information
 -j,--jdbcurl <arg>     JDBC URL
 -L,--list              list entity children
 -m,--metastore <arg>   Hive metastore URI
 -n,--name <arg>        full entity name (dot separated)
 -P,--properties        show an entities properties
 -p,--password <arg>    database password
 -R,--delete            delete an entity
 -r,--rename <arg>      new entity name
 -s,--schema <arg>      schema name
 -t,--table <arg>       table name
 -u,--user <arg>        database username
 -U,--update            update an entity
 -v,--value <arg>       property value
 -w,--warehouse <arg>   warehouse name
 -x,--command <arg>     one of: list, details, create, delete, or update
```

The command line can be used in several ways to achieve the same results, depending on your preference.
```bash
gcli catalog details --name metalake_demo.catalog_postgres
gcli catalog --command details -name metalake_demo.catalog_postgres
gcli --entity catalog --command details -name metalake_demo.catalog_postgres
gcli catalog details --metalake metalake_demo --catalog catalog_postgres
gcli details --metalake metalake_demo --catalog catalog_postgres
gcli --metalake metalake_demo --catalog catalog_postgres
gcli --command details --metalake metalake_demo --catalog catalog_postgres
```
The form `gcli <entity> <action> [options]` is used in this document.

## Commands
The following commands are available for entity management:

list: List available entities
details: Show detailed information about an entity
create: Create a new entity
delete: Delete an existing entity
update: Update an existing entity

### Examples
List All Metalakes

```bash
gcli list
```

Get Details of a Specific Metalake

```bash
gcli metalake details -name my_metalake
```

List Tables in a Catalog

```bash
gcli metalake list -name my_metalake.my_catalog
```

Create a Metalake

```bash
gcli metalake create -name my_metalake -comment "This is my metalake"
```

Create a Catalog

```bash
gcli catalog create -name metalake_demo.iceberg --provider iceberg --metastore thrift://hive-host:9083 --warehouse hdfs://hdfs-host:9000/user/iceberg/warehouse
```

Delete a Catalog

```bash
gcli catalog delete -name my_metalake.my_catalog
```

Rename a Metalake

```bash
gcli metalake update -name metalake_demo -rename demo 
```

Update a Metalake's comment

```bash
gcli metalake update -name metalake_demo -comment "new comment" 
```

### Setting Metalake name

As dealing with one Metalake is a typical scenario, you can set the Metalake name in several ways.

1. Passed in on the command line either as the first part of the entities name or via the `--metalake` parameter.
2. Set via the 'GRAVITINO_METALAKE' environment variable.
3. Placed in the Gravitino configuration file `~/.gravitino` by adding a line like `metalake=metalake_demo`.

The command line option overrides the other options and the environment variable overrides the value in the configuration file.

## Running Tests

This project includes a suite of unit tests to verify its functionality.

To run the tests, execute the following command:

```bash
./gradlew :clients:cli:test
```

## Contributing

We welcome contributions to the Gravitino CLI!

## License

This project is licensed under the Apache License 2.0.
