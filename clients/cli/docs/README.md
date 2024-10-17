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
usage: gcli [metalake|catalog|schema|table] [list|details|create|delete|update] [options]
Options
 -b,--bootstrap <arg>   Kafka bootstrap servers
 -C,--create            create an entity
 -c,--comment <arg>     entity comment
 -D,--details           list details about an entity
 -d,--database <arg>    database name
 -h,--help              command help information
 -j,--jdbcurl <arg>     JDBC URL
 -L,--list              list entity children
 -l,--user <arg>        database username
 -m,--metastore <arg>   Hive metastore URI
 -n,--name <arg>        full entity name (dot separated)
 -P,--properties        show an entities properties
 -p,--provider <arg>    provider one of hadoop, hive, mysql, postgres, iceberg, kafka
 -r,--rename <arg>      new entity name
 -R,--delete            delete an entity
 -u,--url <arg>         Gravitino URL (default: http://localhost:8090)
 -U,--update            update an entity
 -v,--value <arg>       property value
 -w,--warehouse <arg>   warehouse name
 -z,--password <arg>    database password
```

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
gcli metalake details -name my-metalake
```

List Tables in a Catalog

```bash
gcli metalake list -name my-metalake.my-catalog
```

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
