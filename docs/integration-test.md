---
title: "How to Run Integration Tests"
date: 2023-10-02T09:03:20-08:00
license: "Copyright 2023 Datastrato.
This software is licensed under the Apache License version 2."
---
## How to Run Integration Tests

## Introduction

The `integration-test` module contains integration test cases to ensure the correctness of the
Gravitino server, API, and client. These tests can be run locally or on GitHub Actions.

## Test Modes

### MiniGravitino Embedded Mode

The `MiniGravitino` is a lightweight Gravitino server embedded in the integration test module.

1. Execute the `./gradlew build -x test` command to build the Gravitino project.
2. Use the `./gradlew test [--rerun-tasks]` or `./gradlew test [--rerun-tasks] -PtestMode=embedded` command to run the integration tests.
> Execute the `./gradlew build` command to build the Gravitino project and automatically run the integration tests in the embedded mode.

It provides full Gravitino server functionality and supports the following abilities:

- Running a Gravitino server in a single thread
- Starting a Jetty server to provide the Gravitino server API, using a random port to avoid port conflicts
- Using a random storage path to store backend data, which is deleted when MiniGravitino is stopped

### Deploying Gravitino Server Locally

The Gravitino server can be deployed locally to run the integration tests. Follow these steps:

1. Execute the `./gradlew build -x test` command to build the Gravitino project.
2. Use the `./gradlew compileDistribution` command to compile and package the Gravitino project in the `distribution` directory.
3. Use the `./gradlew test [--rerun-tasks] -PtestMode=deploy` command to run the integration tests in the `distribution` directory.

### Skipping Tests

- You can skip unit tests by using the `./gradlew build -PskipTests` command.
- You can skip integration tests by using the `./gradlew build -PskipITs` command.
- You can skip both unit tests and integration tests by using the `./gradlew build -x test` or `./gradlew build -PskipTests -PskipITs` command.

## Docker Test Environment

Some integration test cases depend on the Gravitino CI Docker environment.

If an integration test relies on the specific Gravitino CI Docker environment,
you need to set the `@tag(gravitino-docker-it)` annotation in the test class.
For example, the `integration-test/src/test/.../CatalogHiveIT.java` test needs to connect to
the `datastrato/gravitino-ci-hive` Docker container for testing the Hive data source.
Therefore, it should have the following `@tag` annotation:`@tag(gravitino-docker-it)`, This annotation
helps identify the specific Docker container required for the integration test.
For example:

```java
@Tag("gravitino-docker-it")
public class CatalogHiveIT extends AbstractIT {
...
}
```

### Running all integration tests
If you're running `Docker server` and `mac-docker-connector` (only macOS need to run `mac-docker-connector`), the `./gradlew test -PtestMode=[embedded|deploy]`
command will automatically execute all the test cases.

```text
------------------- Check Docker environment --------------------
Docker server status ............................................ [running]
mac-docker-connector server status .............................. [running]
Using Gravitino IT Docker container to run all integration tests. [embedded test]
-----------------------------------------------------------------
```

### Docker server or mac-docker-connector not running
If `Docker server` or `mac-docker-connector` is not running (only required to run in macOS), the `./gradlew test -PtestMode=[embedded|deploy]`
command will run test cases without `gravitino-docker-it` tag.

```text
------------------- Check Docker environment ------------------
Docker server status ............................................ [stop]
mac-docker-connector server status .............................. [stop]
Run test cases without `gravitino-docker-it` tag ................ [embedded test]
---------------------------------------------------------------
Tip: Please make sure to start the `Docker server before` running the integration tests.
Tip: Please make sure to execute the `dev/docker/tools/mac-docker-connector.sh` script before running the integration test in MacOS.
```

## Debugging Gravitino Server and Integration Tests

By default, the integration tests are run using MiniGravitino.
Debugging with MiniGravitino is simple and easy. You can modify any code in the Gravitino project and set breakpoints anywhere.

### How to Debug deploy mode Gravitino Server and Integration Tests

You have two modes to debug the Gravitino server and integration tests: `embedded` and `deploy` mode.

1. Embedded Mode (default):
    - This mode is simpler to debug but not as close to the actual environment
    - Debugging with `MiniGravitino` is simple and easy. You don't have to do any setup to debug directly

2. Deploy Mode:
    - This mode is closer to the actual environment but more complex to debug
    - To debug the Gravitino server code, follow these steps:
        - Execute the `./gradlew build -x test` command to build the Gravitino project
        - Use the `./gradlew compileDistribution` command to republish the packaged project in the `distribution` directory
        - If you only debug integration test codes, You don't have to do any setup to debug directly
        - If you need to debug Gravitino server codes, follow these steps:
            - Enable the `GRAVITINO_DEBUG_OPTS` environment variable in the `distribution/package/conf/gravitino-env.sh` file to enable remote JVM debugging
            - Manually start the Gravitino server using the `./distribution/package/bin/gravitino.sh start` command
            - Select `gravitino.server.main` module classpath in the `Remote JVM Debug` to attach the Gravitino server process and debug it

## Running on GitHub Actions

- GitHub Actions will automatically run integration tests in the embedded and deploy mode when you submit a pull request to the 'main' branch.
- Test results can be viewed in the `Actions` tab of the pull request page.
- The integration tests are executed in several steps:

  - Gravitino integration tests will pull the CI Docker image from the Docker Hub repository. This step usually takes around 15 seconds.
  - If you set the `debug action` label in the pull request, GitHub Actions will run an SSH server with
    `csexton/debugger-action@master`, allows you to log in to the Actions environment for remote debugging.
  - The Gravitino project is compiled and packaged in the `distribution` directory using the `./gradlew compileDistribution` command.
  - Execution the `./gradlew test -PtestMode=[embedded|deploy]` command.

## Test Failure
If a test fails, valuable information can be retrieved from the logs and test report. Test reports 
are stored in the `./build/reports` directory. The integration test logs are situated in the 
`./integrate-test/build` directory. In deploy mode, Gravitino server logs can be found at 
`./distribution/package/logs/` directory. In the event of a test failure within the GitHub workflow, 
both logs and the test report are archived. To obtain the archive, please follow these steps:
1. Click the `detail` link associated with the failed integrate test in the pull request (PR). This will redirect you to the job page.

   ![pr page Image](assets/test-fail-pr.png)

2. On the job page, locate the `Summary` button on the left-hand side and click it to access the workflow summary page.

   ![job page Image](assets/test-fail-job.png)

3. Look for the Artifacts item on the summary page and download the archive from there.

   ![summary page Image](assets/test-fail-summary.png)
