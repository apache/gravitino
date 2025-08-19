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

# Apache Gravitino™

[![GitHub Actions Build](https://github.com/apache/gravitino/actions/workflows/build.yml/badge.svg)](https://github.com/apache/gravitino/actions/workflows/build.yml)
[![GitHub Actions Integration Test](https://github.com/apache/gravitino/actions/workflows/integration-test.yml/badge.svg)](https://github.com/apache/gravitino/actions/workflows/integration-test.yml)
[![License](https://img.shields.io/github/license/apache/gravitino)](https://github.com/apache/gravitino/blob/main/LICENSE)
[![Contributors](https://img.shields.io/github/contributors/apache/gravitino)](https://github.com/apache/gravitino/graphs/contributors)
[![Release](https://img.shields.io/github/v/release/apache/gravitino)](https://github.com/apache/gravitino/releases)
[![Open Issues](https://img.shields.io/github/issues-raw/apache/gravitino)](https://github.com/apache/gravitino/issues)
[![Last Committed](https://img.shields.io/github/last-commit/apache/gravitino)](https://github.com/apache/gravitino/commits/main/)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/8358/badge)](https://www.bestpractices.dev/projects/8358)

## Introduction

Apache Gravitino is a high-performance, geo-distributed, and federated metadata lake. It manages metadata directly in different sources, types, and regions, providing users with unified metadata access for data and AI assets.

![Gravitino Architecture](docs/assets/gravitino-architecture.png)

Gravitino aims to provide several key features:
* Unified Metadata Management: Gravitino provides a unified model and API to manage different types of metadata, including relational (e.g., Hive, MySQL) and file-based (e.g., HDFS, S3) metadata sources.
* End-to-End Data Governance: Gravitino offers a unified governance layer for managing metadata with features like access control, auditing, and discovery.
* Direct Metadata Management: Gravitino connects directly to metadata sources via connectors, ensuring changes are instantly reflected between Gravitino and the underlying systems.
* Geo-Distribution Support: Gravitino enables deployment across multiple regions or clouds, allowing instances to share metadata for a global cross-region view.
* Multi-Engine Support: Gravitino supports query engines enabling metadata access without modifying SQL dialects.
* AI Asset Management (WIP): Gravitino is expanding to manage both data and AI assets, with support for AI models and features currently in development.

## Contributing to Apache Gravitino

Gravitino is open source software available under the Apache 2.0 license. For information on contributing to Gravitino, please see the [Contribution guidelines](https://gravitino.apache.org/contrib/).

## Online documentation

The latest Gravitino documentation is available on our [official website](https://gravitino.apache.org/docs/latest/). This README file only contains basic setup instructions.

## Building Apache Gravitino

You can build Gravitino using Gradle. Currently, you can build Gravitino on Linux and macOS, and Windows isn't supported.

To build Gravitino, please run:

```shell
./gradlew clean build -x test
```

If you want to build a distribution package, please run:

```shell
./gradlew compileDistribution -x test
```

to build a distribution package.

Or:

```shell
./gradlew assembleDistribution -x test
```

to build a compressed distribution package.

The directory `distribution` contains the generated binary distribution package.

Please see [How to build Gravitino](https://gravitino.apache.org/docs/latest/how-to-build/) for details on building and testing Gravitino.

## Quick start

### Use Gravitino playground

This is the recommended approach. Gravitino provides a docker-compose-based playground where you can experience a whole system alongside other components. Clone or download the [Gravitino playground repository](https://github.com/apache/gravitino-playground) and then follow the [README](https://github.com/apache/gravitino-playground/blob/main/README.md), to get everything running.

### Configure and start Gravitino server in local

To start Gravitino on your machine, download a binary package from the [download page](https://gravitino.apache.org/downloads) and decompress the package.

Before starting the Gravitino server, configure its settings by editing the `gravitino.conf` file located in the `conf` directory. This file follows the standard properties file format, allowing you to modify the server configuration as needed.

To start the Gravitino server, please run:

```shell
./bin/gravitino.sh start
```

To stop the Gravitino server, please run:

```shell
./bin/gravitino.sh stop
```

Alternatively, to run the Gravitino server in the frontend, please run:

```shell
./bin/gravitino.sh run
```

And press `CTRL+C` to stop the Gravitino server.

### Gravitino Iceberg REST catalog service

Gravitino provides Iceberg REST catalog service to manage Iceberg efficiently. For more details, refer to [Gravitino Iceberg REST catalog service](https://gravitino.apache.org/docs/latest/iceberg-rest-service/).

### Using Trino with Apache Gravitino

Gravitino provides a Trino connector for accessing metadata within Gravitino. To use Trino with Gravitino, please follow the [trino-gravitino-connector doc](https://gravitino.apache.org/docs/latest/trino-connector/index/).

## Development guide

1. [How to build Gravitino](https://gravitino.apache.org/docs/latest/how-to-build/)
2. [How to test Gravitino](https://gravitino.apache.org/docs/latest/how-to-test/)
3. [How to publish Docker images](https://gravitino.apache.org/docs/latest/publish-docker-images)

## License

Gravitino is licensed under the Apache License Version 2.0. For details, see the [LICENSE](LICENSE).

<sub>Apache®, Apache Gravitino&trade;, Apache Hadoop&reg;, Apache Hive&trade;, Apache Iceberg&trade;, Apache Kafka&reg;, Apache Spark&trade;, Apache Submarine&trade;, Apache Thrift&trade; and Apache Zeppelin&trade; are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.</sub>

<img src="https://analytics.apache.org/matomo.php?idsite=62&rec=1&bots=1&action_name=ReadMe" style="border:0;" alt="" />
