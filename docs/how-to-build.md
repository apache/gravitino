---
title: How to build Gravitino
slug: /how-to-build
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

- [Prerequisites](#prerequisites)
- [Quick start](#quick-start)
- [How to Build Gravitino on Windows (Using WSL)](#how-to-build-gravitino-on-windows-using-wsl)

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
    ./gradlew spark-connector:spark-connector-runtime:build
    ```

   This creates `gravitino-spark-connector-runtime-{sparkVersion}_{scalaVersion}-{version}.jar`
   under the `spark-connector/spark-connector-runtime/build/libs` directory.

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

## How to Build Gravitino on Windows (Using WSL)
### Download WSL (Ubuntu)

**On Windows:**

Refer to this guide for installation: [WSL Installation Guide](https://learn.microsoft.com/en-us/windows/wsl/install)

*Note: Ubuntu 22.04 can successfully run Gravitino*


This step involves setting up the Windows Subsystem for Linux (WSL) on your Windows machine. WSL allows you to run a Linux distribution alongside Windows, providing a Linux-like environment for development.

### Update Package List and Install Necessary Packages

**On Ubuntu (WSL):**

```sh
sudo apt update
sudo apt install apt-transport-https ca-certificates curl software-properties-common
```

Updating the package list ensures you have the latest information on the newest versions of packages and their dependencies. Installing the necessary packages sets up your system to securely download and manage additional software.

### Download and Setup Java SDK 17 (11 or 8 also works)

**On Ubuntu (WSL):**

1. Edit your `~/.bashrc` file using any editor. Here, `vim` is used:

    ```sh
    vim ~/.bashrc
    ```

2. Add the following lines at the end of the file. Replace `/usr/lib/jvm/java-11-openjdk-amd64` with your actual Java installation path:

    ```sh
    export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
    export PATH=$PATH:$JAVA_HOME/bin
    ```

3. Save and quit in vim using `:wq`.

Editing the `~/.bashrc` file allows you to set environment variables that will be available in every terminal session. Setting `JAVA_HOME` and updating `PATH` ensures that your system uses the correct Java version for development.

### Install Docker

**On Ubuntu (WSL):**

Refer to chatGPT for specific instructions. Here are the steps:

```sh
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
sudo apt update
sudo apt install docker-ce
sudo service docker start
sudo docker run hello-world
sudo usermod -aG docker $USER
```

These commands install Docker, a platform for developing, shipping, and running applications inside containers. Running `hello-world` verifies the installation. Adding your user to the Docker group allows you to run Docker commands without `sudo`.

### Install Python 3.11

**On Ubuntu (WSL):**

```sh
sudo apt update
sudo apt install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.11
python3.11 --version
```

This installs Python 3.11, a programming language used for a variety of applications. The commands add a repository that provides the latest Python versions and then installs Python 3.11.


### Download Gravitino Project to WSL

**On Ubuntu (WSL):**

Remember the project download location for the next step.

```sh
git clone https://github.com/datastrato/gravitino.git
cd gravitino
./gradlew compileDistribution -x test
cd distribution/package/
./bin/gravitino.sh start
```

Access [http://localhost:8090](http://localhost:8090)


Building the Gravitino project compiles the necessary components, and starting the server allows you to access the application in your browser.

### Using IntelliJ (Optional)

**On Windows:**

1. Open the Gravitino project that was just downloaded.
2. Go to `File > Project Structure > Project` and change to the SDK you downloaded in WSL.
3. If the SDK does not appear, manually add the SDK.
4. To find the SDK location, run this command in WSL:

    **On Ubuntu (WSL):**

    ```sh
    which java
    ```


IntelliJ IDEA is an integrated development environment (IDE) for Java development. Setting the project SDK ensures that IntelliJ uses the correct Java version for building and running the project.

You can open up WSL in the IntelliJ terminal. Find the down arrow and select ubuntu.

### Using VS Code (Optional)

#### Set up WSL Extension in VSCode

**On Windows:**

1. Open VSCode extension marketplace, search for and install **WSL**.
2. On Windows, press `Ctrl+Shift+P` to open the command palette, and run `Shell Command: Install 'code' command in PATH`.

Installing the WSL extension in VSCode allows you to open and edit files in your WSL environment directly from VSCode. Adding the `code` command to your PATH enables you to open VSCode from the WSL terminal.

#### Verify and Configure Environment Variables

**On Windows:**

1. Add VSCode path to the environment variables. The default installation path for VSCode is usually:

    ```plaintext
    C:\Users\<Your-Username>\AppData\Local\Programs\Microsoft VS Code\bin
    ```

    Replace `<Your-Username>` with your actual Windows username.

    Example:

    ```plaintext
    C:\Users\epic\AppData\Local\Programs\Microsoft VS Code\bin
    ```


Adding VSCode to the environment variables ensures that you can open VSCode from any command prompt or terminal window.

**On Ubuntu (WSL):**

```sh
code --version
cd gravitino
code .
```

Running `code --version` verifies that the `code` command is available. Using `code .` opens the current directory in VSCode.

#### Open a WSL Project in Windows VSCode

**On Ubuntu (WSL):**

1. **Navigate to Your Project Directory**

   Use the terminal to navigate to the directory of your project. For example:

   ```sh
   cd gravitino
   ```

2. **Open the Project in VSCode**

   In the WSL terminal, type the following command to open the current directory in VSCode:

   ```sh
   code .
   ```

   This command will open the current WSL directory in VSCode on Windows. If you haven't added `code` to your path, follow these steps:
   
   - Open VSCode on Windows.
   - Press `Ctrl+Shift+P` to open the command palette.
   - Type and select `Shell Command: Install 'code' command in PATH`.

3. **Ensure Remote - WSL is Active**

   When VSCode opens, you should see a green bottom-left corner indicating that VSCode is connected to WSL. If it isn't, click on the green area and select `Remote-WSL: New Window` or `Remote-WSL: Reopen Folder in WSL`.

4. **Edit and Develop Your Project**

   You can now edit and develop your project files in VSCode as if they were local files. The Remote - WSL extension seamlessly bridges the file system between Windows and WSL.

