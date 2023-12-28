<!--
  Copyright 2023 Datastrato Pvt Ltd.
  This software is licensed under the Apache License version 2.
-->

![Gravitino Logo](docs/assets/gravitino-logo.png)

[![GitHub Actions Build](https://github.com/datastrato/gravitino/actions/workflows/build.yml/badge.svg)](https://github.com/datastrato/gravitino/actions/workflows/build.yml)
[![GitHub Actions Integration Test](https://github.com/datastrato/gravitino/actions/workflows/integration-test.yml/badge.svg)](https://github.com/datastrato/gravitino/actions/workflows/integration-test.yml)

## Introduction

Gravitino is a high-performance, geo-distributed, and federated metadata lake. It creates and manages metadata from multiple different database sources and cloud providers directly, regardless of type and region. Users can then access the unified metadata using supported engines for data management and advanced analytics.

For a detailed overview of features, please see the [official documentation](https://datastrato.ai/docs).

![Gravitino Architecture](docs/assets/gravitino-architecture.png)

<details>
<summary> Table of Contents </summary>   

- [Core Features](#Core-Features)
- [Documentation](#Documentation)
- [Installation](#Installation)
- [Quick Start](#Quick-Start)
- [Contributing](#Contributing-to-Gravitino)
- [License](#License)
</details>

## Core Features
<b>Gravitino aims to provide several key features:</b>

* Single Source of Truth for multi-regional data with geo-distributed architecture support.
* Unified Data and AI asset management for both users and engines.
* Security in one place, centralizing the security for different sources.
* Built-in data management and data access management.

## Documentation

You can find the latest Gravitino documentation in the [docs folder](docs). The official [website](https://datastrato.ai/docs) has docs for each official release.

## Installation

You can install Gravitino from the binary release package or Docker image, please follow the
[how-to-install](docs/how-to-install.md) to install Gravitino. Or, you can install Gravitino from scratch, please 
follow the documentation of
[how-to-build](docs/how-to-build.md) and [how-to-install](docs/how-to-install.md) to install Gravitino.


Gravitino requires **Java 8** to run. The optional trino-connector runs with Trino, and requires Java 17.

### Building from Source 

Gradle makes it easy to build Gravitino. To build Gravitino, please run:

```shell
./gradlew clean build -x test
```

If you want to build a distribution package, please run:

```shell
./gradlew compileDistribution -x test 
```
Or, you can also build a compressed distribution package with the following:
```shell
./gradlew assembleDistribution -x test 
```


The generated binary distribution package locates in `distribution` directory.

For the details of building and testing Gravitino, please see [How to build Gravitino](docs/how-to-build.md).

## Quick Start

### Configure and start the Gravitino server

If you already have a binary distribution package, please decompress the package (if required)
and go to the directory where the package locates.

Before starting the Gravitino server, please configure the Gravitino server configuration file. The
configuration file, `gravitino.conf`, located in the `conf` directory and follows the standard property file format. You can modify the configuration within this file.

To start the Gravitino server, please run:

```shell
./bin/gravitino.sh start
```

To stop the Gravitino server, please run:

```shell
./bin/gravitino.sh stop
```

For a more detailed step-by-step guide, see our [Getting Started](docs/getting-started.md) guide.

### Using Trino with Gravitino

Gravitino provides a Trino connector to access the metadata in Gravitino. To use Trino with Gravitino, please follow the [trino-gravitino-connector doc](docs/trino-connector/index.md).

## Contributing to Gravitino

Gravitino is open source software available under the Apache 2.0 license. For information of how to contribute to Gravitino please see the [Contribution guidelines](CONTRIBUTING.md).

### Development guide

If you are interested in developing with Gravitino, check out the following guides:

1. [How to build Gravitino](docs/how-to-build.md)
2. [How to test Gravitino](docs/how-to-test.md)
3. [How to publish Docker images](docs/publish-docker-images.md)

## Community
We have a [Discourse](http://gravitino.discourse.group) group that anyone can join and where we actively take feedback and feature requests.

## License

Gravitino is under the Apache License Version 2.0, See the [LICENSE](LICENSE) for the details.

<sub>Apache®, Apache Hadoop&reg;, Apache Hive&trade;, Apache Iceberg&trade;, Apache Kafka&reg;, Apache Spark&trade;, Apache Submarine&trade;, Apache Thrift&trade; and Apache Zeppelin&trade; are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.</sub>
