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

# Apache Gravitino Command Line Interface

Apache Gravitino CLI is a command-line tool that interacts with the Gravitino server to manage and query entities like metalakes, catalogs, schemas, and tables. The tool provides options for listing information about Gravitino entities and in future versions support creating, deleting, and updating these entities.

## Table of Contents

- [Installation](#installation)
- [Running Tests](#running-tests)
- [Contributing](#contributing)
- [License](#license)

## Installation

### Prerequisites

Before you can build and run this project, it is suggested you have the following installed:

- Java 8 or higher

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
    alias gcli='java -jar clients/cli/build/libs/gravitino-cli-*-incubating-SNAPSHOT.jar'
    ```
3. Test the command:
    ```bash
    gcli --help
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
