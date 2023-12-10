---
title: How to build Gravitino
slug: /how-to-build
license: Copyright 2023 Datastrato Pvt. This software is licensed under the Apache License version 2.
---

## Prerequisites

+ Linux or macOS operating system
+ Git
+ A Java Development Kit version 8 to 17 installed in your environment to launch Gradle
+ Optionally Docker to run integration tests

:::info Please read the following notes first
+ Gravitino requires at least JDK8 and at most JDK17 to run Gradle, so you need to
  install JDK8 to 17 version to launch the build environment.

+ Gravitino itself uses JDK8 to build, Gravitino Trino connector uses JDK17 to build. You don't
  have to preinstall JDK8 or JDK17, Gradle detects the JDK version needed and downloads it automatically.

+ Gravitino uses Gradle Java Toolchain to detect and manage JDK versions, it checks the
  installed JDK by running `./gradlew javaToolchains` command. For the details of Gradle Java
  Toolchain, please see [Gradle Java Toolchain](https://docs.gradle.org/current/userguide/toolchains.html#sec:java_toolchain).

+ Make sure you have installed Docker in your environment as Gravitino uses it to run integration tests; without it, some Docker-related tests may not run.

+ macOS uses "docker-connector" to make the Gravitino Trino connector work with Docker
  for macOS. For the details of "docker-connector", please see [docker-connector](https://github.com/wenjunxiao/mac-docker-connector) and [README](../dev/docker/tools/README.md).
  Alternatively, you can use OrbStack to replace Docker for macOS, please see
  [OrbStack](https://orbstack.dev/), with OrbStack you can run Gravitino integration tests
  without needing to install "docker-connector".
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

:::note
The first time you build the project, downloading the dependencies may take a while. You can add `-x test` to skip the test, like `./gradlew build -x test`.
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
The `gravitino-{version}-bin.tar.gz` file is the Gravitino Server distribution package, and the `gravitino-{version}-bin.tar.gz.sha256` file is the sha256 checksum file for the Gravitino Server distribution package.
:::

5. Assemble Gravitino Trino connector package

   ```shell
    ./gradlew assembleTrinoConnector
    ```

   or

    ```shell
    ./gradlew assembleDistribution
    ```

   This creates `gravitino-trino-connector-{version}.tar.gz` and `gravitino-trino-connector-{version}.tar.gz.sha256` under the `distribution` directory.
