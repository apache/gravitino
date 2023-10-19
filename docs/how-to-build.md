---
title: "How to Build Gravitino"
date: 2023-10-03T09:03:20-08:00
license: "Copyright 2023 Datastrato.
This software is licensed under the Apache License version 2."
---
# How to Build Gravitino

## Prerequisites
+ JDK 1.8
+ Git

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
    ```
    ├── ...
    └── distribution/package
        ├── bin/gravitino.sh            # Gravitino Server Launching scripts.
        ├── catalogs
        │   └── hive/                   # Hive catalog dependencies and configuratons.
        │   └── lakehouse-iceberg/      # Iceberg catalog dependencies and configuratons.
        ├── conf/                       # All configuration for Gravitino.
        |   ├── gravitino.conf          # Gravitino Server configuration.
        |   ├── gravitino-env.sh        # Environment variables, etc., JAVA_HOME, GRAVITINO_HOME, and more.
        |   └── log4j2.properties       # log4j configuration for Gravitino Server.
        ├── libs/                       # Gravitino Server dependencies libraries.
        └── logs/                       # Gravitino Server logs.
        └── data/                       # Default directory for Gravitino Server to store data.
    ```
   > Note: The `./gradlew clean` command will delete the `distribution` directory.

4. Run Gravitino Server.

    ```shell
    distribution/package/bin/gravitino.sh start
    ```
   > Note: If you need to debug the Gravitino Server, you can enable the `GRAVITINO_DEBUG_OPTS` environment variable in the `conf/gravitino-env.sh` file.
   Then you can create a `Remote JVM Debug` configuration in `IntelliJ IDEA` and debug `gravitino.server.main`.

5. Stop Gravitino Server.

    ```shell
    distribution/package/bin/gravitino.sh stop
    ```

6. Assemble Gravitino distribution package.

    ```shell
   ./gradlew assembleDistribution
   ```
   The `assembleDistribution` command will create `gravitino-{version}-bin.tar` and `gravitino-{version}-bin.tar.sha256` files in the `distribution/package` directory.
   You can deploy the `gravitino-{version}-bin.tar` file to your production environment.
   > Note: The `gravitino-{version}-bin.tar` file is the Gravitino Server distribution package, and the `gravitino-{version}-bin.tar.sha256` file is the sha256 checksum file for the Gravitino Server distribution package.