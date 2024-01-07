<!--
  Copyright 2023 Datastrato Pvt Ltd.
  This software is licensed under the Apache License version 2.
-->

# Gravitino

[![GitHub Actions Build](https://github.com/datastrato/gravitino/actions/workflows/build.yml/badge.svg)](https://github.com/datastrato/gravitino/actions/workflows/build.yml)
[![GitHub Actions Integration Test](https://github.com/datastrato/gravitino/actions/workflows/integration-test.yml/badge.svg)](https://github.com/datastrato/gravitino/actions/workflows/integration-test.yml)
[![License](https://img.shields.io/github/license/datastrato/gravitino)](https://github.com/datastrato/gravitino/blob/main/LICENSE)
[![Contributors](https://img.shields.io/github/contributors/datastrato/gravitino)](https://github.com/datastrato/gravitino/graphs/contributors)
[![Release](https://img.shields.io/github/v/release/datastrato/gravitino)](https://github.com/datastrato/gravitino/releases)
[![Open Issues](https://img.shields.io/github/issues-raw/datastrato/gravitino)](https://github.com/datastrato/gravitino/issues)
[![Last Committed](https://img.shields.io/github/last-commit/datastrato/gravitino)](https://github.com/datastrato/gravitino/commits/main/)

## Introduction

Gravitino is a high-performance, geo-distributed, and federated metadata lake. It manages the metadata directly in different sources, types, and regions. It also provides users with unified metadata access for data and AI assets.

![Gravitino Architecture](docs/assets/gravitino-architecture.png)

Gravitino aims to provide several key features:

* Single Source of Truth for multi-regional data with geo-distributed architecture support.
* Unified Data and AI asset management for both users and engines.
* Security in one place, centralizing the security for different sources.
* Built-in data management and data access management.

## Contributing to Gravitino

Gravitino is open source software available under the Apache 2.0 license. For information on how to contribute to Gravitino please see the [Contribution guidelines](CONTRIBUTING.md).

## Online documentation

You can find the latest Gravitino documentation in the [doc folder](docs). This README file only contains basic setup instructions.

## Building Gravitino

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

### Configure and start the Gravitino server

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

### Using Trino with Gravitino

Gravitino provides a Trino connector to access the metadata in Gravitino. To use Trino with Gravitino, please follow the [trino-gravitino-connector doc](docs/trino-connector/index.md).

## Development guide

1. [How to build Gravitino](docs/how-to-build.md)
2. [How to test Gravitino](docs/how-to-test.md)
3. [How to publish Docker images](docs/publish-docker-images.md)

## License

Gravitino is under the Apache License Version 2.0, See the [LICENSE](LICENSE) for the details.

<sub>Apache®, Apache Hadoop&reg;, Apache Hive&trade;, Apache Iceberg&trade;, Apache Kafka&reg;, Apache Spark&trade;, Apache Submarine&trade;, Apache Thrift&trade; and Apache Zeppelin&trade; are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.</sub>
