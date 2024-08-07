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

# Apache Gravitino™ (incubating)

[![GitHub Actions Build](https://github.com/apache/gravitino/actions/workflows/build.yml/badge.svg)](https://github.com/apache/gravitino/actions/workflows/build.yml)
[![GitHub Actions Integration Test](https://github.com/apache/gravitino/actions/workflows/integration-test.yml/badge.svg)](https://github.com/apache/gravitino/actions/workflows/integration-test.yml)
[![License](https://img.shields.io/github/license/apache/gravitino)](https://github.com/apache/gravitino/blob/main/LICENSE)
[![Contributors](https://img.shields.io/github/contributors/apache/gravitino)](https://github.com/apache/gravitino/graphs/contributors)
[![Release](https://img.shields.io/github/v/release/apache/gravitino)](https://github.com/apache/gravitino/releases)
[![Open Issues](https://img.shields.io/github/issues-raw/apache/gravitino)](https://github.com/apache/gravitino/issues)
[![Last Committed](https://img.shields.io/github/last-commit/apache/gravitino)](https://github.com/apache/gravitino/commits/main/)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/8358/badge)](https://www.bestpractices.dev/projects/8358)

## Introduction

Apache Gravitino is a high-performance, geo-distributed, and federated metadata lake. It manages the metadata directly in different sources, types, and regions. It also provides users with unified metadata access for data and AI assets.

![Gravitino Architecture](docs/assets/gravitino-architecture.png)

Gravitino aims to provide several key features:

* Single Source of Truth for multi-regional data with geo-distributed architecture support.
* Unified Data and AI asset management for both users and engines.
* Security in one place, centralizing the security for different sources.
* Built-in data management and data access management.

## Contributing to Apache Gravitino

Gravitino is open source software available under the Apache 2.0 license. For information on how to contribute to Gravitino please see the [Contribution guidelines](CONTRIBUTING.md).

## Online documentation

You can find the latest Gravitino documentation in the [doc folder](docs). This README file only contains basic setup instructions.

## Building Apache Gravitino

You can build Gravitino using Gradle. Currently you can build Gravitino on Linux and macOS, Windows isn't supported.

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

For the details of building and testing Gravitino, please see [How to build Gravitino](docs/how-to-build.md).

## Quick start

### Configure and start the Apache Gravitino server

If you already have a binary distribution package, go to the directory of the decompressed package.

Before starting the Gravitino server, please configure the Gravitino server configuration file. The
configuration file, `gravitino.conf`, is in the `conf` directory and follows the standard property file format. You can modify the configuration within this file.

To start the Gravitino server, please run:

```shell
./bin/gravitino.sh start
```

To stop the Gravitino server, please run:

```shell
./bin/gravitino.sh stop
```

Alternatively, to run the Gravitino server in frontend, please run:

```shell
./bin/gravitino.sh run
```

And press `CTRL+C` to stop the Gravitino server.

### Gravitino Iceberg REST catalog service

Gravitino provides Iceberg REST catalog service to manage Iceberg, please refer to [Gravitino Iceberg REST catalog service](docs/iceberg-rest-service.md) for more details.

### Using Trino with Apache Gravitino

Gravitino provides a Trino connector to access the metadata in Gravitino. To use Trino with Gravitino, please follow the [trino-gravitino-connector doc](docs/trino-connector/index.md).

## Development guide

1. [How to build Gravitino](docs/how-to-build.md)
2. [How to test Gravitino](docs/how-to-test.md)
3. [How to publish Docker images](docs/publish-docker-images.md)

## License

Gravitino is under the Apache License Version 2.0, See the [LICENSE](LICENSE) for the details.

## ASF Incubator disclaimer

Apache Gravitino is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the Apache Incubator. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects. While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.

<sub>Apache®, Apache Gravitino&trade;, Apache Hadoop&reg;, Apache Hive&trade;, Apache Iceberg&trade;, Apache Kafka&reg;, Apache Spark&trade;, Apache Submarine&trade;, Apache Thrift&trade; and Apache Zeppelin&trade; are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.</sub>
