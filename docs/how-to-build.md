---
title: How to build Gravitino
slug: /how-to-build
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

## Prerequisites

+ Linux or macOS operating system
+ Git
+ A Java Development Kit, version 8 to 17, installed in your environment to launch Gradle
+ Optionally, Docker to run integration tests

:::info Please read the following notes before trying to build Gravitino.

+ Gravitino requires at least JDK8 and at most JDK17 to run Gradle, so you need to
  install a JDK, versions 8 to 17, to launch the build environment.
+ Gravitino itself supports using JDK8, 11, and 17 to build. The Gravitino Trino connector uses
  JDK17 to build (to avoid vendor-related issues on some platforms, Gravitino uses the specified Amazon Corretto OpenJDK 17 to build the Trino connector on macOS).
  You don't have to preinstall the specified JDK environment, as Gradle detects the JDK version needed and downloads it automatically.
+ Gravitino uses the Gradle Java Toolchain to detect and manage JDK versions, it checks the
  installed JDK by running the `./gradlew javaToolchains` command. See [Gradle Java Toolchain](https://docs.gradle.org/current/userguide/toolchains.html#sec:java_toolchain).
+ Make sure you have installed Docker in your environment if you plan to run integration texts. Without it, some Docker-related tests may not run.
+ macOS uses `docker-connector` to make the Gravitino Trino connector work with Docker
  for macOS. See [docker-connector](https://github.com/wenjunxiao/mac-docker-connector), `$GRAVITINO_HOME/dev/docker/tools/mac-docker-connector.sh`, and
  `$GRAVITINO_HOME/dev/docker/tools/README.md` for more details.
+ You can use OrbStack as a replacement for Docker for macOS. See
  [OrbStack](https://orbstack.dev/). With OrbStack you can run Gravitino integration tests
  without needing to install `docker-connector`.
:::

## Quick start

1. Clone the Gravitino project.

    ```shell
    git clone git@github.com:datastrato/gravitino.git
    ```

2. Build the Gravitino project.

    ```shell
    cd gravitino
    ./gradlew build
    ```

    The default specified JDK version is 8, if you want to use JDK 11 or 17 to build,
    modify the property `jdkVersion` to 11 or 17 in the `gradle.properties` file, or specify the version
    with `-P`, like:

    ```shell
    ./gradlew build -PjdkVersion=11
    ```

    Or:

    ```shell
    ./gradlew build -PjdkVersion=17
    ```

3. Build the Gravitino Python client.

   The default specified Python version is 3.8, if you want to use Python 3.9, 3.10 or 3.11 to build,
   modify the property `pythonVersion` to 3.9, 3.10 or 3.11 in the `gradle.properties` file, or specify the version
   with `-P`, like:

    ```shell
    ./gradlew build -PpythonVersion=3.9
    ```

   Or:

    ```shell
    ./gradlew build -PpythonVersion=3.10
    ```

   Or:

    ```shell
    ./gradlew build -PpythonVersion=3.11
    ```
   
4. Build Gravitino Spark connector

    ```shell
    ./gradlew spark-connector:spark-connector-runtime:build
    ```

   This creates `gravitino-spark-connector-runtime-{sparkVersion}_{scalaVersion}-{version}.jar` under the `spark-connector/spark-connector-runtime/build/libs` directory.


:::note
The first time you build the project, downloading the dependencies may take a while. You can add
`-x test` to skip the tests, by using `./gradlew build -x test`.

The built Gravitino libraries are Java 8 compatible, and verified under Java 8, 11, and 17
environments. You can use Java 8, 11, 17 runtimes to run the Gravitino server, no matter which
JDK version you use to build the project.
:::

3. Get the Gravitino binary package.

    ```shell
    ./gradlew compileDistribution
    ```

   The `compileDistribution` command creates a `distribution` directory in the Gravitino root directory.

   The directory structure of the `distribution` directory is as follows:

:::note
The `./gradlew clean` command deletes the `distribution` directory.
:::

4. Assemble the Gravitino distribution package.

    ```shell
    ./gradlew assembleDistribution
    ```

   The `assembleDistribution` command creates `gravitino-{version}-bin.tar.gz` and `gravitino-{version}-bin.tar.gz.sha256` under the `distribution` directory.

   You can deploy them to your production environment.

:::note
The `gravitino-{version}-bin.tar.gz` file is the Gravitino server distribution package, and the
`gravitino-{version}-bin.tar.gz.sha256` file is the sha256 checksum file for the Gravitino
server distribution package.
:::

5. Assemble the Gravitino Trino connector package

   ```shell
    ./gradlew assembleTrinoConnector
    ```

   or

    ```shell
    ./gradlew assembleDistribution
    ```

   This creates `gravitino-trino-connector-{version}.tar.gz` and `gravitino-trino-connector-{version}.tar.gz.sha256` under the `distribution` directory.
