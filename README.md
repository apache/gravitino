<!--
  Copyright 2023 Datastrato.
  This software is licensed under the Apache License version 2.
-->

# Gravitino

[![GitHub Actions Build](https://github.com/datastrato/gravitino/actions/workflows/build.yml/badge.svg)](https://github.com/datastrato/gravitino/actions/workflows/build.yml)
[![GitHub Actions Integration Test](https://github.com/datastrato/gravitino/actions/workflows/integration-test.yml/badge.svg)](https://github.com/datastrato/gravitino/actions/workflows/integration-test.yml)

## Introduction

Gravitino is a high-performance, geo-distributed, and federated metadata lake. It manages the metadata directly in different sources, types, and regions. It also provides users unified access to the metadata for both data and AI assets.

![Gravitino Architecture](docs/assets/gravitino-architecture.png)

Gravitino aims to provide several key features:

* SSOT (Single Source of Truth) for multi-regional data with geo-distributed architecture support.
* Unified Data and AI asset management for both users and engines.
* Security in one place, centralizing the security for different sources.
* Built-in data management and data access management.

## Online Documentation

You can find the latest Gravitino documentation here in the [doc folder](docs), The README file only contains basic setup instructions.

## Building Gravitino

Gravitino is built using Gradle. To build Gravitino, please run:

```shell
./gradlew clean build
```

If you want to build a distribution package, please run:

```shell
./gradlew compileDistribution
```

to build a distribution package.

Or:

```shell
./gradlew assembleDistribution
```

to build a compressed distribution package.

Note:

1. Gravitino is built against JDK8, please make sure JDK8 is installed in your environment.
2. Gravitino trino-connector is built against JDK17, please also make sure JDK17 is installed in your environment.

For the details of building and testing Gravitino, please see [How to build Gravitino](docs/how-to-build.md).

## Quick Start

### Configure and start the Gravitino server

Gravitino server configuration file `gravitino.conf` is located under `conf` and follows the typical property file format, you can change the configuration in this file.

To start the Gravitino server, please run:

```shell
./bin/gravitino.sh start
```

To stop the Gravitino server, please run:

```shell
./bin/gravitino.sh stop
```

### Using Trino with Gravitino

Gravitino provides a Trino connector to access the metadata in Gravitino. To use Trino with Gravitino, please follow the [trino-gravitino-connector doc](docs/trino-gravitino-connector.md).

## Development Guide

1. [How to build Gravitino](docs/how-to-build.md)
2. [How to Run Integration Test](docs/integration-test.md)
3. [How to publish Docker images](docs/publish-docker-images.md)

## License

Gravitino is under the Apache License Version 2.0, See the [LICENSE](LICENSE) for the details.

