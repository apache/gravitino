---
title: "How to Run Integration Tests"
date: 2023-10-02T09:03:20-08:00
license: "Copyright 2023 Datastrato.
This software is licensed under the Apache License version 2."
---
## How to Run Integration Tests

## Introduction

The `integration-test` module contains integration test cases to ensure the correctness of the
Gravitino Server, API, and Client. These tests can be run locally or on GitHub Actions.

## Test Modes

### MiniGravitino Embedded Mode

The `MiniGravitino` is a lightweight Gravitino Server that is embedded in the integration test module.

1. Execute the `./gradlew build -x test` command to build the Gravitino project.
2. Use the `./gradlew test [--rerun-tasks]` or `./gradlew test [--rerun-tasks] -PtestMode=embedded` command to run the integration tests.
> Execute the `./gradlew build` command to build the Gravitino project and automatically run the integration tests in the embedded mode.

It provides full Gravitino Server functionality and supports the following abilities:

- Running a Gravitino Server in a single thread
- Starting a Jetty Server to provide the Gravitino Server API, using a random port to avoid port conflicts
- Using a random storage path to store backend data, which is deleted when MiniGravitino is stopped

### Deploying Gravitino Server Locally

The Gravitino Server can be deployed locally to run the integration tests. Follow these steps:

1. Execute the `./gradlew build -x test` command to build the Gravitino project.
2. Use the `./gradlew compileDistribution` command to compile and package the Gravitino project in the `distribution` directory.
3. Use the `./gradlew test [--rerun-tasks] -PtestMode=deploy` command to run the integration tests in the `distribution` directory.

### Skipping Tests

- You can skip unit tests by using the `./gradlew build -PskipTests` command.
- You can skip integration tests by using the `./gradlew build -PskipITs` command.
- You can both skip unit tests and integration tests by using the `./gradlew build -x test` or `./gradlew build -PskipTests -PskipITs` command.

## Docker Test Environment

Some integration test cases depend on the Gravitino CI Docker environment.

If an integration test relies on the specific Gravitino CI Docker environment,
you need to set the `@tag(CI-DOCKER-NAME)` annotation in the test class.
For example, the `integration-test/src/test/.../CatalogHiveIT.java` test needs to connect to
the `datastrato/gravitino-ci-hive` Docker container for testing the Hive data source.
Therefore, it should have the following `@tag` annotation:`@tag(CI-DOCKER-NAME)`, This annotation
helps identify the specific Docker container required for the integration test.
For example:

```java
@Tag("gravitino-ci-hive")
public class CatalogHiveIT extends AbstractIT {
...
}
```

If you have Docker installed and a special CI Docker container running, the `./gradlew test -PtestMode=[embedded|deploy]`
command will automatically execute all the test cases.

```text
------------------- Check Docker environment ------------------
Docker server status .......................................... [running]
Gravitino IT Docker container is already running ............... [yes]
Use Gravitino IT Docker container to run all integration test.   [embbeded|deploy test]
---------------------------------------------------------------
```

If Docker is not installed or the special CI Docker container is not running, the `./gradlew test -PtestMode=[embedded|deploy]`
command will skip the test cases that depend on the special Docker environment.

```text
------------------- Check Docker environment ------------------
Docker server status .......................................... [running]
Gravitino IT Docker container is already running ............... [no]
Run only test cases where tag is set `gravitino-docker-it`.      [embbeded|deploy test]
---------------------------------------------------------------
```

If Docker is not installed or the `mac docker connector` is not running, the `./gradlew test -PtestMode=[embedded|deploy]`
command will skip the test cases that depend on the `mac docker connector`.

```text
------------------- Check Docker environment ------------------
Docker server status .......................................... [running]
Gravitino IT Docker container is already running ............... [no]
Run only test cases where tag is set `gravitino-trino-it`.      [embbeded|deploy test]
---------------------------------------------------------------
```

> Gravitino will run all integration test cases in the GitHub Actions environment.

### Running Gravitino CI Docker Environment

Before running the tests, make sure Docker is installed.

#### Mac Docker connector
Because Docker Desktop for Mac does not provide access to container IP from host(macOS).
The [mac-docker-connector](https://github.com/wenjunxiao/mac-docker-connector) provides the ability for the macOS host to directly access the docker container IP.
This can result in host(macOS) and containers not being able to access each other's internal services directly over IPs.
Before running the integration tests, make sure to execute the `dev/docker/tools/mac-docker-connector.sh` script.
> Developing Gravitino in a linux environment does not have this limitation and does not require executing the `mac-docker-connector.sh` script ahead of time.

#### Running Gravitino Hive CI Docker Environment

1. Run a hive docker test environment container in the local using the `docker run --rm -d -p 8022:22 -p 8088:8088 -p 9000:9000 -p 9083:9083 -p 10000:10000 -p 10002:10002 -p 50010:50010 -p 50070:50070 -p 50075:50075 datastrato/gravitino-ci-hive` command.

Additionally, the Gravitino Server and third-party data source Docker runtime environments will use certain ports. Ensure that these ports are not already in use:

- Gravitino Server: Port `8090`
- Hive Docker runtime environment: Ports are `22`, `7180`, `8088`, `8888`, `9000`, `9083`, `10000`, `10002`, `50010`, `50070`, and `50075`

## Debugging Gravitino Server and Integration Tests

By default, the integration tests are run using MiniGravitino.
Debugging with MiniGravitino is simple and easy. You can modify any code in the Gravitino project and set breakpoints anywhere.

### How to Debug deploy mode Gravitino Server and Integration Tests

To debug the Gravitino Server and integration tests, you have two modes: `embedded` and `deploy` mode.

1. Embedded Mode (default):
    - This mode is simpler to debug but not as close to the actual environment
    - Debugging with `MiniGravitino` is simple and easy. You don't have to do any setup to debug directly

2. Deploy Mode:
    - This mode is closer to the actual environment but more complex to debug
    - To debug the Gravitino Server code, follow these steps:
        - Execute the `./gradlew build -x test` command to build the Gravitino project
        - Use the `./gradlew compileDistribution` command to republish the packaged project in the `distribution` directory
        - If you only debug integration test codes, You don't have to do any setup to debug directly
        - If you need to debug Gravitino server codes, follow these steps:
            - Enable the `GRAVITINO_DEBUG_OPTS` environment variable in the `distribution/package/conf/gravitino-env.sh` file to enable remote JVM debugging
            - Manually start the Gravitino Server using the `./distribution/package/bin/gravitino.sh start` command
            - Select `gravitino.server.main` module classpath in the `Remote JVM Debug` to attach the Gravitino Server process and debug it

## Running on GitHub Actions

- When you submit a pull request to the `main` branch, GitHub Actions will automatically run integration tests in the embedded and deploy mode.
- Test results can be viewed in the `Actions` tab of the pull request page.
- The integration tests are executed in several steps:

  - If you set the `build docker image` label in the pull request, GitHub Actions will trigger the build of all Docker
    images under the `./dev/docker/` directory. This step usually takes around 10 minutes. If you have made changes to the Dockerfile,
    you need to set the `build docker image` label in the pull request.
  - Otherwise, GitHub Actions will pull the Docker image `datastrato/gravitino-ci-hive` from the Docker Hub repository. This step usually takes around 15 seconds.
  - If you set the `debug action` label in the pull request, GitHub Actions will run an SSH server with
    `csexton/debugger-action@master`, allowing you to remotely log in to the Actions environment for debugging purposes.
  - The Gravitino project is compiled and packaged in the `distribution` directory using the `./gradlew compileDistribution` command.
  - Execution the `./gradlew test -PtestMode=[embedded|deploy]` command.
  - Stop the Docker image to clean up.
