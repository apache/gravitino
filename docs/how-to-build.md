---
title: How to build Apache Gravitino
slug: /how-to-build
license: "This software is licensed under the Apache License version 2."
---

## Prerequisites

- Linux, macOS or Windows operating system
- Git
- JDK 8 (default), 11 or 17 for running Gradle, the build environment for Gravitino.
- Python 3.8 (default), 3.9, 3.10, or 3.11, for building the Gravitino Python client
- Docker (optional), for running the integration tests

:::note
Depending on how you deploy Gravitino, there may be known security vulnerabilities
in other software used in conjunction with Gravitino.
:::

### Install WSL on Windows

If you are building Gravitino on a Linux or macOS machine, you can proceed to the [next section](#install-jdk).
To build Apache Gravitino on Windows, you need to install the Windows Subsystem for Linux (WSL)
from Microsoft which provides you with a Linux-like environment for development.
To install WSL, refer to the [WSL Installation Guide](https://learn.microsoft.com/en-us/windows/wsl/install).
The following steps assume that you have installed Unbuntu, which is the default distribution for WSL.

*Note: Gravitino can run successfully on Ubuntu 22.04.*

Make sure you have fresh information about the latest versions of Ubuntu packages and their dependencies:

```shell
sudo apt update
```

Install the prerequisite packages required by later software installation:

```shell
sudo apt install apt-transport-https ca-certificates curl software-properties-common
```

### Install JDK

You don't have to pre-install the JDK environment.
The Gradle Java toolchain can detect and install JDK automatically by running the `./gradlew javaToolchains` command.
For more details, see [Gradle Java Toolchain](https://docs.gradle.org/current/userguide/toolchains.html#sec:java_toolchain).

If you want to install JDK manually, Java SDK 17 is recommended, and JDK 8 or JDK 11 also works.
After installing the JDK, you may want to set up your Shell environment to ensure that the correct JDK version will be used.

1. Edit the `~/.bashrc` file by adding the following lines at the end of the file.

   ```sh
   export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
   export PATH=$PATH:$JAVA_HOME/bin
   ```

   Replace `/usr/lib/jvm/java-11-openjdk-amd64` with your actual Java installation path.

1. Run `source ~/.bashrc` to update your Shell environment variables.

### Install Python

Python 3.8 is the default version for building the Gravitino Python client packages..
Python 3.9, 3.10, or 3.11 also works.

Add a repository for the latest Python versions and install Python 3.8:

```shell
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.8
```

### Install Docker

If you don't want to run tests, you can proceed to the [next step](#build-gravitino).

:::info
- Gravitino skips all Docker-related tests by default.

  To run Docker-related tests, make sure you have installed Docker, and
  (1) set `skipDockerTests=false` in the `gradle.properties` file, or
  (2) use `-PskipDockerTests=false` in the command line, or
  (3) `export SKIP_DOCKER_TESTS=false` in the Shell.

<!--TODO(Qiming): move the following two items elsewhere-->
- macOS uses [docker-connector](https://github.com/wenjunxiao/mac-docker-connector)
  to make the Gravitino Trino connector work. Refer to
  `$GRAVITINO_HOME/dev/docker/tools/mac-docker-connector.sh`, and
  `$GRAVITINO_HOME/dev/docker/tools/README.md` for more details.
  The Gravitino Trino connector uses JDK17 to build.
  For example, to avoid vendor-related issues on macOS,
  Gravitino uses the Amazon Corretto OpenJDK 17 to build the Trino connector.

- You can use [OrbStack](https://orbstack.dev) as an alternative for Docker for macOS.
  With OrbStack, you can run Gravitino integration tests without `docker-connector`.
:::


1. Add the apt repository for Docker-CE:

   ```shell
   curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
   sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
   ```

1. Install the `docker-ce` package and start the Docker daemon:

   ```shell
   sudo apt update
   sudo apt install docker-ce
   sudo service docker start
   ```

1. Verify the installation by running `hello-world`:

   ```shell
   sudo docker run hello-world
   ```

1. Add yourself to the `docker` group so that you can run Docker commands without `sudo` in the future.

   ```shell
   sudo usermod -aG docker $USER
   ```

## Build Gravitino

1. Clone the Gravitino project.

   ```shell
   git clone git@github.com:apache/gravitino.git
   ```

1. Build the Gravitino project. Running this for the first time can take 15 minutes or more.

   ```shell
   cd gravitino
   ./gradlew build
   ```

   You can customize the property `jdkVersion` in the `gradle.properties` file to use JDK 11 or JDK 17.
   Alternatively, you can specify the version using the `-P` Gradle flag. e.g.

   ```shell
   ./gradlew build -PjdkVersion=11
   ```

   The `./gradlew build` command builds all the Gravitino components,
   including the Gravitino server, Java and Python clients, Trino and Spark connectors etc.
 
   You can customize the `pythonVersion` property in the `gradle.properties` file
   to use Python 3.9, 3.10, or 3.11. Alternatively, you can set it on the command line
   using the `-P` Gradle flag. e.g.
 
   ```shell
   ./gradlew build -PpythonVersion=3.9
   ```

   :::note
   The Gravitino libraries built are Java 8 compatible and verified under Java 8, 11, and 17.
   You can run the Gravitino server using JRE 8, 11, or 17,
   regardless of which JDK version was used to build the project.
 
   The built JARs are under the modules `build/libs` directory.
   You can publish them to the Maven repository for your project.
   :::

1. Build the Gravitino server binary package.

   ```shell
   ./gradlew compileDistribution
   ```

   The `compileDistribution` command creates a `distribution` directory in the Gravitino root directory.
   You can use the `-x test` flag to skip tests explicitly.

   :::note
   The `./gradlew clean` command deletes the `distribution` directory.
   :::

1. Assemble the Gravitino server distribution package.

   ```shell
   ./gradlew assembleDistribution
   ```

   The `assembleDistribution` command creates the distribution packages for production deployment:
 
   - `distribution/gravitino-{version}-bin.tar.gz`
   - `distribution/gravitino-{version}-bin.tar.gz.sha256`
   - `distribution/gravitino-trino-connector-{version}.tar.gz`
   - `distribution/gravitino-trino-connector-{version}.tar.gz.sha256`
   - `distribution/gravitino-iceberg-rest-server-{version}.tar.gz`
   - `distribution/gravitino-iceberg-rest-server-{version}.tar.gz.sha256`
 
   :::note
   You can assemble the Gravitino Trino connector package alone by running
   the `assembleTrinoConnector` Gradle command.
   Similarly, you can assemble the Gravitino Iceberg REST server package alone
   by running the `assembleIcebergRESTServer` Gradle command.
 
   ```shell
   ./gradlew assembleTrinoConnector
   ./gradlew assembleIcebergRESTServer
   ```
   :::

1. Start the server for verification:

   ```shell
   cd distribution/package/
   ./bin/gravitino.sh start
   ```
   
   The Web UI for Gravitino can now be accessed at [http://localhost:8090](http://localhost:8090).


### On Building an Individual Module

If you want to build a module on its own, like the Spark connector,
you can use Gradle to build a module with a specific name:

```shell
./gradlew spark-connector:spark-runtime-3.4:build -PscalaVersion=2.12
```

This creates `gravitino-spark-connector-runtime-{sparkVersion}_{scalaVersion}-{version}.jar`
under the `spark-connector/v3.4/spark-runtime/build/libs` directory.
You can customize the Spark version (`3.4` here) and the Scala version (`2.12` here).
The default Scala version is `2.12` if `-PscalaVersion` is not specified.

<img src="https://analytics.apache.org/matomo.php?idsite=62&rec=1&bots=1&action_name=HowToBuild" alt="" />

