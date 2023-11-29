---
title: "How to Build Gravitino"
date: 2023-10-03T09:03:20-08:00
license: "Copyright 2023 Datastrato.
This software is licensed under the Apache License version 2."
---
## How to Build Gravitino

## Prerequisites

+ Linux or macOS operating system
+ Git
+ A JDK8 or later version installed in your environment to launch Gradle
+ Docker (optional) to run integration tests

Note:

* Gravitino itself uses JDK8 to build, Gravitino Trino connector uses JDK17 to build. You don't 
  have to preinstall JDK8 or JDK17, Gradle will detect the JDK version and download it automatically.
* Gravitino uses Gradle Java Toolchain to detect and manage JDK version, you can check the 
  installed JDK by running `./gradlew javaToolchains` command. For the details of Gradle Java 
  Toolchain, please see [Gradle Java Toolchain](https://docs.gradle.org/current/userguide/toolchains.html#sec:java_toolchain).
* Gravitino uses Docker to run integration tests, please make sure you have installed Docker in 
  your environment, otherwise some docker related tests will be skipped.
* On macOS, we will use "docker-connector" to make the Gravitino Trino connector work with Docker 
  for Mac. For the details of "docker-connector", please see [docker-connector](https://github.com/wenjunxiao/mac-docker-connector).

## Quick Start

1. Clone the Gravitino project.

    ```shell
    git clone git@github.com:datastrato/gravitino.git
    ```

2. Build the Gravitino project.

    ```shell
    cd gravitino
    ./gradlew build
    ```

   > Note: The first time you build the project, it may take a while to download the dependencies.

3. Deploy the Gravitino project in your local environment.

    ```shell
    ./gradlew compileDistribution
    ```

   The `compileDistribution` command will create a `distribution` directory in the Gravitino root directory.

   The directory structure of the `distribution` directory is as follows:

    ```text
    ├── ...
    └── distribution/package
        ├── bin/gravitino.sh            # Gravitino Server Launching scripts.
        ├── catalogs
        │   └── hive/                   # Hive catalog dependencies and configurations.
        │   └── lakehouse-iceberg/      # Iceberg catalog dependencies and configurations.
        ├── conf/                       # All configuration for Gravitino.
        |   ├── gravitino.conf          # Gravitino Server configuration.
        |   ├── gravitino-env.sh        # Environment variables, etc., JAVA_HOME, GRAVITINO_HOME, and more.
        |   └── log4j2.properties       # log4j configuration for Gravitino Server.
        ├── libs/                       # Gravitino Server dependencies libraries.
        └── logs/                       # Gravitino Server logs. Automatically created after the Gravitino server starts.
        └── data/                       # Default directory for Gravitino Server to store data.
    ```

   > Note: The `./gradlew clean` command will delete the `distribution` directory.

4. Run Gravitino Server.

    ```shell
    distribution/package/bin/gravitino.sh start
    ```
   You can access the Gravitino WEB UI by typing http://localhost:8090 in your browser

   > Note: If you need to debug the Gravitino Server, you can enable the `GRAVITINO_DEBUG_OPTS` environment variable in the `conf/gravitino-env.sh` file.
   Then you can create a `Remote JVM Debug` configuration in `IntelliJ IDEA` and debug `gravitino.server.main`.

5. Stop Gravitino Server.

    ```shell
    distribution/package/bin/gravitino.sh stop
    ```

6. Assemble the Gravitino distribution package.

    ```shell
    ./gradlew assembleDistribution
    ```

   The `assembleDistribution` command will create `gravitino-{version}-bin.tar.gz` and `gravitino-{version}-bin.tar.gz.sha256` under the `distribution` directory.

   You can deploy them to your production environment.

   > Note: The `gravitino-{version}-bin.tar.gz` file is the Gravitino Server distribution package, and the `gravitino-{version}-bin.tar.gz.sha256` file is the sha256 checksum file for the Gravitino Server distribution package.

7. Assemble Gravitino trino connector package

   ```shell
    ./gradlew assembleTrinoConnector
    ```

   or

    ```shell
    ./gradlew assembleDistribution
    ```

   It will create `gravitino-trino-connector-{version}.tar.gz` and `gravitino-trino-connector-{version}.tar.gz.sha256` under the `distribution` directory.
