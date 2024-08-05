---
title: How to build Apache Gravitino
slug: /how-to-build
license: "This software is licensed under the Apache License version 2."
---

- [Prerequisites](#prerequisites)
- [Quick start](#quick-start)
- [How to Build Apache Gravitino on Windows (Using WSL)](#how-to-build-gravitino-on-windows-using-wsl)

## Prerequisites

+ Linux or macOS operating system
+ Git
+ A Java Development Kit, version 8 to 17, installed in your environment to launch Gradle
+ Python 3.8, 3.9, 3.10, or 3.11 to build the Gravitino Python client
+ Optionally, Docker to run integration tests

:::info Please read the following notes before trying to build Gravitino.

+ Gravitino requires at least JDK8 and at most JDK17 to run Gradle, so you need to
  install a JDK, versions 8 to 17, to launch the build environment.
+ Gravitino itself supports using JDK8, 11, and 17 to build. The Gravitino Trino connector uses
  JDK17 to build (to avoid vendor-related issues on some platforms, Gravitino uses the specified Amazon Corretto OpenJDK 17 to build the Trino connector on macOS).
  You don't have to preinstall the specified JDK environment, as Gradle detects the JDK version needed and downloads it automatically.
+ Gravitino uses the Gradle Java Toolchain to detect and manage JDK versions, it checks the
  installed JDK by running the `./gradlew javaToolchains` command. See [Gradle Java Toolchain](https://docs.gradle.org/current/userguide/toolchains.html#sec:java_toolchain).
+ Gravitino excludes all Docker-related tests by default. To run Docker related tests, make sure you have installed
  Docker in your environment and either (1) set `skipDockerTests=false` in the `gradle.properties` file (or
  use `-PskipDockerTests=false` in the command) or (2) `export SKIP_DOCKER_TESTS=false` in shell. Otherwise, all Docker required tests will skip.
+ macOS uses `docker-connector` to make the Gravitino Trino connector work with Docker
  for macOS. See [docker-connector](https://github.com/wenjunxiao/mac-docker-connector), `$GRAVITINO_HOME/dev/docker/tools/mac-docker-connector.sh`, and
  `$GRAVITINO_HOME/dev/docker/tools/README.md` for more details.
+ You can use OrbStack as a replacement for Docker for macOS. See
  [OrbStack](https://orbstack.dev/). With OrbStack you can run Gravitino integration tests
  without needing to install `docker-connector`.
:::

## Quick start

1. Clone the Gravitino project.

    If you want to contribute to this open-source project, please fork the project on GitHub first. After forking, clone the forked project to your local environment, make your changes, and submit a pull request (PR).

    ```shell
    git clone git@github.com:apache/gravitino.git
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

   The `./gradlew build` command builds all the Gravitino components, including Gravitino 
   server, Java and Python client, Trino and Spark connectors, and more.

   For Python client, the `./gradlew build` command builds the Python client with Python 3.8
   by default. If you want to use Python 3.9, 3.10 or 3.11 to build, please modify the property
   `pythonVersion` to 3.9, 3.10 or 3.11 in the `gradle.properties` file, or specify the version
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
   
   If you want to build some modules alone, like Spark connector, you can use Gradle build task 
   with specific module name, like:

    ```shell
    ./gradlew spark-connector:spark-runtime-3.4:build -PscalaVersion=2.12
    ```

   This creates `gravitino-spark-connector-runtime-{sparkVersion}_{scalaVersion}-{version}.jar`
   under the `spark-connector/v3.4/spark-runtime/build/libs` directory. You could replace `3.4` with 
   `3.3` or `3.5` to specify different Spark versions, and replace `2.12` with `2.13` for different Scala 
   versions. The default Scala version is `2.12` if not specifying `-PscalaVersion`.

   :::info
   Gravitino Spark connector doesn't support Scala 2.13 for Spark 3.3.
   :::

   :::note
   The first time you build the project, downloading the dependencies may take a while. You can add
   `-x test` to skip the tests, by using `./gradlew build -x test`.
   
   The built Gravitino libraries are Java 8 compatible, and verified under Java 8, 11, and 17
   environments. You can use Java 8, 11, 17 runtimes to run the Gravitino server, no matter which
   JDK version you use to build the project.

   The built jars are under the `build/libs` directory of the modules. You can publish them to your
   Maven repository to use them in your project.
   :::

3. Get the Gravitino server binary package.

    ```shell
    ./gradlew compileDistribution
    ```

   The `compileDistribution` command creates a `distribution` directory in the Gravitino root directory.

   The directory structure of the `distribution` directory is as follows:

   :::note
   The `./gradlew clean` command deletes the `distribution` directory.
   :::

4. Assemble the Gravitino server distribution package.

    ```shell
    ./gradlew assembleDistribution
    ```

   The `assembleDistribution` command creates `gravitino-{version}-bin.tar.gz` and `gravitino-{version}-bin.tar.gz.sha256` under the `distribution` directory.

   You can deploy them to your production environment.

   :::note
   The `gravitino-{version}-bin.tar.gz` file is the Gravitino **server** distribution package, and the
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

   This creates `gravitino-trino-connector-{version}.tar.gz` and
   `gravitino-trino-connector-{version}.tar.gz.sha256` under the `distribution` directory. You 
   can uncompress and deploy it to Trino to use the Gravitino Trino connector.

6. Assemble the Gravitino Iceberg REST server package

   ```shell
    ./gradlew assembleIcebergRESTServer
    ```

   This creates `gravitino-iceberg-rest-server-{version}.tar.gz` and `gravitino-iceberg-rest-server-{version}.tar.gz.sha256` under the `distribution` directory. You can uncompress and deploy it to use the Gravitino Iceberg REST server.

## How to Build Apache Gravitino on Windows (Using WSL)

### Download WSL (Ubuntu)

**On Windows:**

Refer to this guide for installation: [WSL Installation Guide](https://learn.microsoft.com/en-us/windows/wsl/install)

*Note: Ubuntu 22.04 can successfully run Gravitino*

This step involves setting up the Windows Subsystem for Linux (WSL) on your Windows machine. WSL allows you to run a Linux distribution alongside Windows, providing a Linux-like environment for development.

### Update Package List and Install Necessary Packages

**On Ubuntu (WSL):**

```shell
sudo apt update
sudo apt install apt-transport-https ca-certificates curl software-properties-common
```

Updating the package list ensures you have the latest information on the newest versions of packages and dependencies. Installing the necessary packages sets up your system to securely download and manage additional software.

### Download and Setup Java SDK 17 (11 or 8 also works)

**On Ubuntu (WSL):**

1. Edit your `~/.bashrc` file using any editor. Here, `vim` is used:

    ```shell
    vim ~/.bashrc
    ```

2. Add the following lines at the end of the file. Replace `/usr/lib/jvm/java-11-openjdk-amd64` with your actual Java installation path:

    ```sh
    export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
    export PATH=$PATH:$JAVA_HOME/bin
    ```

3. Save and quit in vim using `:wq`.

4. Run `source ~/.bashrc` to update the `.bashrc` file in your current shell session.

    Editing the `~/.bashrc` file allows you to set environment variables that will be available in every terminal session. Setting `JAVA_HOME` and updating `PATH` ensures that your system uses the correct Java version for development.

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

These commands add a repository that provides the latest Python versions and then installs Python 3.11.

### Download Apache Gravitino Project to WSL

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

For instructions on how to run the project using VSCode or IntelliJ on Windows, please refer to [CONTRIBUTING.md](CONTRIBUTING.md).
