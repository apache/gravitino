<!--
  Copyright 2023 Datastrato.
  This software is licensed under the Apache License version 2.
-->
# How to Build Graviton

## Prerequisites
+ JDK 1.8
+ Git

## Quick Start
1. Clone the Graviton project.

    ```shell
    git clone git@github.com:datastrato/graviton.git
    ```

2. Build the Graviton project.

    ```shell
    cd graviton
    ./gradlew build
    ```
   > Note: The first time you build the project, it will take a long time to download the dependencies.

3. Deploy the Graviton project in your local environment.

    ```shell
    ./gradlew complieDistribution
    ```
    
    The `compileDistribution` command will create `distribution` directory in the Graviton root directory.
    
    The `distribution` directory structure is as follows:
    ```
    ├── ...
    └── distribution/package
        ├── bin/graviton.sh          # Graviton Server Launching scripts
        ├── catalogs
        │   └── hive/libs/           # Hive catalog dependencies
        ├── conf/                    # All configuration for Gravtion
        |   ├── graviton.conf        # Graviton Server configuration
        |   ├── graviton-env.sh      # Environment variables, etc., JAVA_HOME, GRAVITON_HOME and more.
        |   └── log4j2.properties    # log4j configuration for Gravtion Server.
        ├── libs/                    # Graviton Server dependencies lib
        └── logs/                    # Graviton Server logs
    ```
   > Note: `./gradlew clean` command will delete the `distribution` directory.

4. Run Graviton Server.

    ```shell
    distribution/package/bin/graviton.sh start
    ```
    > Note: If you need to debug the Graviton Server, you can enable `GRAVITON_DEBUG_OPTS` environment variable in the `conf/graviton-env.sh` file.
      Then you can create `Remote JVM Debug` configuration in the `IntelliJ IDEA` and debug `graviton.server.main`.

5. Stop Graviton Server.

    ```shell
    distribution/package/bin/graviton.sh stop
    ```

6. Assembly Graviton distribution package

    ```shell
   ./gradlew assembleDistribution
   ```
   The `assembleDistribution` command will create `graviton-{version}-bin.tar` and `graviton-{version}-bin.tar.sha256` files in the `distribution/package` directory.
   You can deploy the `graviton-{version}-bin.tar` file to your production environment.
   > Note: The `graviton-{version}-bin.tar` file is the Graviton Server distribution package, and the `graviton-{version}-bin.tar.sha256` file is the sha256 checksum file for the Graviton Server distribution package.