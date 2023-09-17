<!--
  Copyright 2023 Datastrato.
  This software is licensed under the Apache License version 2.
-->
# How to Run Integration Tests

## Introduction
The `integration-test` module contains test cases that serve as integration tests to ensure the correctness of the 
Graviton Server, API, and Client. These tests can be run locally or on GitHub Actions.

## Test Modes
### Using MiniGraviton Embedded Server
The `MiniGraviton` is a lightweight Graviton Server that is embedded in the integration test module.
1. Execute the `./gradlew build` command to build the Graviton project.
3. Use the `./gradlew test [--rerun-tasks] -PtestMode=embedded` or `./gradlew test [--rerun-tasks] -PtestMode=embedded` command to run the integration tests.

It provides full Graviton Server functionality and supports the following abilities:
- Running a Graviton Server in a single thread
- Starting a Jetty Server to provide the Graviton Server API, using a random port to avoid port conflicts
- Using a random storage path to store backend data, which is deleted when MiniGraviton is stopped

## Running Locally
Before running the tests, make sure Docker is installed. 
Then, execute blow steps: 
1. Execute the `./gradlew clean build` command to build Graviton project.
2. The Graviton project is compiled and packaged in the `distribution` directory using the `./gradlew compileDistribution` command.
3. Run a hive docker test environment container in the local using the `docker run --rm -d -p 8022:22 -p 8088:8088 -p 9000:9000 -p 9083:9083 -p 10000:10000 -p 10002:10002 -p 50070:50070 -p 50075:50075 datastrato/graviton-ci-hive` command.
4. The integration test cases in the `integration-test` module are executed using the `./gradlew integrationTest` command.

Additionally, the Graviton Server and third-party data source Docker runtime environments will use certain ports. Ensure that these ports are not already in use:
- Graviton Server: Port `8090`
- Hive Docker runtime environment: Ports is `22`, `7180`, `8088`, `8888`, `9000`, `9083`, `10000`, `10002`, `50070`, and ` 50075`

#### Debugging Graviton Server and Integration Tests
By default, the integration tests are run using MiniGraviton. 
You can use the `./gradlew build` or `./gradlew build -PtestMode=embedded` command to run the integration tests. 
Debugging with MiniGraviton is simple and easy. You can modify any code in the Graviton project and set breakpoints anywhere.

### Deploying Graviton Server Locally
The Graviton Server can be deployed locally to run the integration tests. Follow these steps:

1. Execute the `./gradlew build` command to build the Graviton project.
2. Use the `./gradlew compileDistribution` command to compile and package the Graviton project in the `distribution` directory.
3. Use the `./gradlew test [--rerun-tasks] -PtestMode=deploy` command to run the integration tests in the `distribution` directory.

### How to Debug deploy mode Graviton Server and Integration Tests

To debug the Graviton Server and integration tests, you have two modes: `embedded` and `deploy` mode.

1. Embedded Mode:
    - This mode is simpler to debug but not as close to the actual environment
    - Debugging with `MiniGraviton` is simple and easy. You can modify any code in the Graviton project and set breakpoints anywhere.

2. Deploy Mode:
    - This mode is closer to the actual environment but more complex to debug
    - To debug the Graviton Server code, follow these steps:
        - Execute the `./gradlew build` command to build the Graviton project
        - Use the `./gradlew compileDistribution` command to republish the packaged project in the `distribution` directory
        - If you only debug integration test codes, You can modify all the Graviton's code and set breakpoints anywhere
        - If you need debug Graviton server codes, flow these steps:
            - Enable the `GRAVITON_DEBUG_OPTS` environment variable in the `distribution/package/conf/graviton-env.sh` file to enable remote JVM debugging
            - Manually start the Graviton Server using the `./distribution/package/bin/graviton-server.sh start` command
            - Use remote JVM debugging to attach to the Graviton Server process and set breakpoints

## Docker Test Environment:
Some integration test cases depend on the Graviton CI Docker environment.

If an integration test relies on the specific Graviton CI Docker environment, 
you need to set the `@tag(CI-DOCKER-NAME)` annotation in the test class. 
For example, the `integration-test/src/test/.../CatalogHiveIT.java` test needs to connect to 
the `datastrato/graviton-ci-hive` Docker container for testing the Hive data source. 
Therefore, it should have the following `@tag` annotation:`@tag(CI-DOCKER-NAME)`, This annotation 
helps identify the specific Docker container required for the integration test.
For example:
```
@Tag("graviton-ci-hive")
public class CatalogHiveIT extends AbstractIT {
...
}
```

If you have Docker installed and a special CI Docker container running, the `./gradlew test -PtestMode=[embedded|deploy]` 
command will automatically execute all the test cases.
```
-------------------- Check Docker environment --------------------
Docker server status ............................................. [running]
Graviton CI Hive container is already running .................... [yes]
Use exist Graviton CI Hive container to run all integration test.  [embbeded|deploy test]
------------------------------------------------------------------
```

If Docker is not installed or the special CI Docker container is not running, the `./gradlew test -PtestMode=[embedded|deploy]` 
command will skip the test cases that depend on the special Docker environment.
```
-------------------- Check Docker environment --------------------
Docker server status ............................................. [stop]
Graviton CI Hive container is already running .................... [no]
Run only test cases where tag is not set `CI-DOCKER-NAME`.         [embbeded|deploy test]
------------------------------------------------------------------
```

## Running on GitHub Actions:

- When you submit a pull request to the `main` branch, GitHub Actions will automatically run the integration tests.
- Test results can be viewed in the `Actions` tab of the pull request page.
- The integration tests are executed in several steps:

  + If you set the `build docker image` label in the pull request, GitHub Actions will trigger the build of all Docker 
    images under the `./dev/docker/` directory. This step usually takes around 10 minutes. If you have made changes to the Dockerfile, 
    you need to set the `build docker image` label in the pull request.
  + If you do not set the `build docker image` label in the pull request, GitHub Actions will pull the Docker image 
    `datastrato/graviton-ci-hive` from the Docker Hub repository. This step usually takes around 15 seconds.
  + The Docker image is then run in the GitHub Actions environment.
  + If you set the `debug action` label in the pull request, GitHub Actions will run an SSH server with 
    `csexton/debugger-action@master`, allowing you to remotely log in to the Actions environment for debugging purposes.
  + The Graviton project is compiled and packaged in the `distribution` directory using the `./gradlew compileDistribution` command.
  + The integration test cases in the `integration-test` module are executed using the `./gradlew test -PtestMode=[embedded|deploy]` command.
  + The Docker image is stopped.
  + The test environment is cleaned up.

## Running Locally:
- Before running the tests, ensure that Docker is installed.
- Follow these steps to run the tests locally:
  + Execute the `./gradlew build` command to build the Graviton project.
  + Use the `./gradlew compileDistribution` command to compile and package the Graviton project in the `distribution` directory.
  + Run a Hive Docker CI test environment container locally using the `docker run` command with the appropriate port mappings.
  + Execute the `./gradlew test -PtestMode=[embedded|deploy]` command to run the integration test cases in the `integration-test` module.

Note: Make sure that the necessary ports for the Graviton Server and third-party data source Docker runtime environments are not already in use.
- Graviton Server: Port `8088`
- Hive Docker runtime environment: Ports `50070`, `50075`, `10002`, `10000`, `8888`, `9083`, `7180`, and `22`
