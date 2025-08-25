---
title: How to build Apache Gravitino
slug: /how-to-build
license: "This software is licensed under the Apache License version 2."
---

- [Prerequisites](#prerequisites)
- [Quick start](#quick-start)
- [How to Build Apache Gravitino on Windows (Using WSL)](#how-to-build-apache-gravitino-on-windows-using-wsl)

## Prerequisites

+ Linux or macOS operating system
+ Git
+ A Java Development Kit, version 17, installed in your environment to launch Gradle
+ Python 3.8, 3.9, 3.10, 3.11, or 3.12 to build the Gravitino Python client
+ Optionally, Docker to run integration tests

:::info Please read the following notes before trying to build Gravitino.

+ Gravitino requires a minimum of JDK17 to run Gradle, so you need to install a JDK17 to launch the build environment.
+ Gravitino itself supports using JDK 17 to build. The Gravitino Trino connector uses JDK17 to build (to avoid vendor-related issues on some platforms, It's recommended to use Amazon Corretto OpenJDK 17 to build Gravitino on macOS).
 You don't have to preinstall the specified JDK environment, as Gradle detects the JDK version needed and downloads it automatically.
+ Gravitino uses the Gradle Java Toolchain to detect and manage JDK versions, and it checks the installed JDK by running the `./gradlew javaToolchains` command. See [Gradle Java Toolchain](https://docs.gradle.org/current/userguide/toolchains.html#sec:java_toolchain).
+ Gravitino excludes all Docker-related tests by default. To run Docker-related tests, make sure you have installed Docker in your environment and either (1) set `skipDockerTests=false` in the `gradle.properties` file (or use `-PskipDockerTests=false` in the command) or (2) `export SKIP_DOCKER_TESTS=false` in the shell. Otherwise, all tests requiring Docker will be skipped.
+ macOS uses `docker-connector` to make the Gravitino Trino connector work with Docker for macOS. See [docker-connector](https://github.com/wenjunxiao/mac-docker-connector), `$GRAVITINO_HOME/dev/docker/tools/mac-docker-connector.sh`, and `$GRAVITINO_HOME/dev/docker/tools/README.md` for more details.
+ You can use OrbStack as a replacement for Docker for macOS. See [OrbStack](https://orbstack.dev/). With OrbStack, you can run Gravitino integration tests without needing to install `docker-connector`.
+ Depending on how you deploy Gravitino, other software used in conjunction with Gravitino may contain known security vulnerabilities.
:::

## Quick start

1. Clone the Gravitino project.

   If you want to contribute to this open-source project, please fork the project on GitHub first. After forking, clone the forked project to your local environment, make your changes, and submit a pull request (PR).

   ```shell
   git clone git@github.com:apache/gravitino.git
   ```

2. Build the Gravitino project. Running this for the first time can take 15 minutes or more.

   ```shell
   cd gravitino
   ./gradlew build
   ```

  The `./gradlew build` command builds all the Gravitino components, including the Gravitino server, Java and Python clients, Trino and Spark connectors, and more.

  For the Python client, the `./gradlew build` command builds the Python client with Python 3.8 by default. If you want to use Python 3.9, 3.10, 3.11, or 3.12 to build, please modify the property `pythonVersion` to 3.9, 3.10, 3.11, or 3.12 in the `gradle.properties` file, or specify the version with `-P` like:

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

  Or:
    
   ```shell
   ./gradlew build -PpythonVersion=3.12
   ```

  If you want to build a module on its own, like the Spark connector, you can use Gradle to build a module with a specific name, like so:

   ```shell
   ./gradlew spark-connector:spark-runtime-3.4:build -PscalaVersion=2.12
   ```

  This creates `gravitino-spark-connector-runtime-{sparkVersion}_{scalaVersion}-{version}.jar` under the `spark-connector/v3.4/spark-runtime/build/libs` directory. You could replace `3.4` with  `3.3` or `3.5` to specify different Spark versions and replace `2.12` with `2.13` for different Scala versions. The default Scala version is `2.12` if `-PscalaVersion` is not specified.

  :::info
  Gravitino Spark connector doesn't support Scala 2.13 for Spark 3.3.
  :::

  :::note
  The first time you build the project, downloading the dependencies may take a while.
 
  You can add `-x test` to skip the tests using `./gradlew build -x test`.

  The built Gravitino libraries are Java 17 compatible and verified under 17 environments. You can use Java 17 runtimes to run the Gravitino server, no matter which JDK version was used to build the project.

  The built jars are under the modules `build/libs` directory. You can publish them in your Maven repository for use in your project.
  :::

3. Get the Gravitino server binary package.

   ```shell
   ./gradlew compileDistribution
   ```

  The `compileDistribution` command creates a `distribution` directory in the Gravitino root directory.

  :::note
  The `./gradlew clean` command deletes the `distribution` directory.
  :::

4. Assemble the Gravitino server distribution package.

   ```shell
   ./gradlew assembleDistribution
   ```

  The `assembleDistribution` command creates `gravitino-{version}-bin.tar.gz` and `gravitino-{version}-bin.tar.gz.sha256` under the `distribution` directory.

  You can deploy these to your production environment.

  :::note
  The `gravitino-{version}-bin.tar.gz` file is the Gravitino **server** distribution package, and the `gravitino-{version}-bin.tar.gz.sha256` file is the sha256 checksum file for the Gravitino server distribution package.
  :::

5. Assemble the Gravitino Trino connector package

  ```shell
   ./gradlew assembleTrinoConnector
   ```

  or

   ```shell
   ./gradlew assembleDistribution
   ```

  This creates `gravitino-trino-connector-{version}.tar.gz` and
  `gravitino-trino-connector-{version}.tar.gz.sha256` under the `distribution` directory. You can uncompress and deploy it to Trino to use the Gravitino Trino connector.

6. Assemble the Gravitino Iceberg REST server package

  ```shell
   ./gradlew assembleIcebergRESTServer
   ```

  This creates `gravitino-iceberg-rest-server-{version}.tar.gz` and `gravitino-iceberg-rest-server-{version}.tar.gz.sha256` under the `distribution` directory. You can uncompress and deploy it to use the Gravitino Iceberg REST server.

## How to build Apache Gravitino on Windows (Using WSL)

### Download WSL (Ubuntu)

**On Windows:**

Refer to this guide for installation: [WSL Installation Guide](https://learn.microsoft.com/en-us/windows/wsl/install)

*Note: Gravitino can run successfully on Ubuntu 22.04*

This step involves setting up your Windows machine's Windows Subsystem for Linux (WSL). WSL allows you to run a Linux distribution alongside Windows, providing a Linux-like environment for development.

### Update package list and install necessary packages

**On Ubuntu (WSL):**

```shell
sudo apt update
sudo apt install apt-transport-https ca-certificates curl software-properties-common
```

Updating the package list ensures you have the latest information on the newest versions of packages and dependencies. Installing the necessary packages lets your system download and manage additional software securely.

### Download and setup Java SDK 17

**On Ubuntu (WSL):**

1. Edit your `~/.bashrc` file using any editor. Here, `vim` is used:

   ```shell
   vim ~/.bashrc
   ```

2. Add the following lines at the end of the file. Replace `/usr/lib/jvm/java-17-openjdk-amd64` with your actual Java installation path:

   ```sh
   export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
   export PATH=$PATH:$JAVA_HOME/bin
   ```

3. Save and quit in vim using `:wq`.

4. Run `source ~/.bashrc` to update your shell session's environment variables.

   Editing the `~/.bashrc` file allows you to set environment variables available in every terminal session. Setting `JAVA_HOME` and updating `PATH` ensures that your system uses the correct Java version for development.

### Install Docker

**On Ubuntu (WSL):**

```shell
curl -fsSL https
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
sudo apt update
sudo apt install docker-ce
sudo service docker start
sudo docker run hello-world
sudo usermod -aG docker $USER
```

These commands install Docker. Running `hello-world` verifies the installation. Adding your user to the Docker group allows you to run Docker commands without `sudo`.

### Install Python 3.11

**On Ubuntu (WSL):**

```shell
sudo apt update
sudo apt install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.11
python3.11 --version
```

These commands add a repository that provides the latest Python versions and installs Python 3.11.

### Download Apache Gravitino project to WSL

**On Ubuntu (WSL):**

```shell
git clone https://github.com/apache/gravitino.git
cd gravitino
./gradlew compileDistribution -x test
cd distribution/package/
./bin/gravitino.sh start
```

Access [http://localhost:8090](http://localhost:8090)

Building the Gravitino project compiles the necessary components, and starting the server allows you to access the application in your browser.

Please refer to [CONTRIBUTING.md](https://github.com/apache/gravitino/blob/main/CONTRIBUTING.md) for instructions on running the project using VSCode or IntelliJ on Windows.

<img src="https://analytics.apache.org/matomo.php?idsite=62&rec=1&bots=1&action_name=HowToBuild" alt="" />
