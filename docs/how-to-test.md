---
title: How to test Gravitino
slug: /how-to-test
license: Copyright 2023 Datastrato Pvt. This software is licensed under the Apache License version 2.
---

## How to test Gravitino

Gravitino has two types of tests: unit tests and integration tests. Unit tests are mainly 
focused on the functionalities of the specific class, module, or component. Integration tests 
are end-to-end tests which covers the whole system.

:::note before test

* If you want to run the complete integration test suites, you need to install Docker in your 
environment.
* Please refer to [How to build Gravitino](./how-to-build) for more details to make sure you have 
a build environment ready.
* We highly recommend you to use [OrbStack](https://orbstack.dev/) to replace Docker Desktop for 
  macOS. OrbStack automatically configures the Docker network.
* If you insist on using Docker Desktop for macOS, please launch 
  [mac-docker-connector](https://github.com/wenjunxiao/mac-docker-connector) before running tests, 
  you can check `$GRAVITINO_HOME/dev/docker/tools/README.md` and
  `$GRAVITINO_HOME/dev/docker/tools/mac-docker-connector.sh` for more details.

:::

## Run the unit tests

To run the unit test, you can simply run the following command:

```shell

./gradlew test -PskipITs
```

This command will run all the unit tests and skip the integration tests.

## Run the integration tests

Gravitino has two modes to run the integration tests, the `embedded` and `deploy` modes.

* With the `embedded` mode, the integration test starts an embedded `MiniGravitino` server 
  within the same process of the integration test to run the integration tests.
* With the `deploy` mode, the user has to start a local Gravitino server manually, and the 
  integration test connects to the local Gravitino server to run the integration tests.

### Run the integration tests in embedded mode

1. Run the `./gradlew build -x test` command to build the Gravitino project.

2. Use the `./gradlew test [--rerun-tasks] -PskipTests -PtestMode=embedded` commands to run the 
   integration tests.

:::note

Run the `./gradlew build` command will trigger the build and run the integration tests in embedded mode.

:::

### Deploy the Gravitino server and run the integration tests in deploy mode

Deploy the Gravitino server locally to run the integration tests. Follow these steps:

1. Run the `./gradlew build -x test` command to build the Gravitino project.
2. Use the `./gradlew compileDistribution` command to compile and package the Gravitino project 
   in the `distribution` directory.
3. Use the `./gradlew test [--rerun-tasks] -PskipTests -PtestMode=deploy` command to run the 
   integration tests in the `distribution` directory.

## Skip tests

- You can skip unit tests by using the `./gradlew build -PskipTests` command.
- You can skip integration tests by using the `./gradlew build -PskipITs` command.
- You can skip both unit tests and integration tests by using the `./gradlew build -x test` or `./gradlew build -PskipTests -PskipITs` commands.

## Docker test environment

Some integration test cases depend on the Gravitino CI Docker image.

If an integration test relies on the specific Gravitino CI Docker image,
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

## Running the complete integration tests

:::note

* You should make sure that the `Docker server` is running before running the complete 
  integration tests. Otherwise, it only runs the integration tests without the `gravitino-docker-it` tag.
* on macOS, please be sure to run the `${GRAVITION_HOME}/dev/docker/tools/mac-docker-connector.sh` 
  script before running the complete integration tests. Or, you should make use that
  [OrbStack](https://orbstack.dev/) is running.

:::

When integration tests run, it checks the whole environment and outputs the status of the 
required environment, for example:

```text
------------------ Check Docker environment ---------------------
Docker server status ............................................ [running]
mac-docker-connector status ..................................... [stop]
OrbStack status ................................................. [yes]
Using Gravitino IT Docker container to run all integration tests. [deploy test]
-----------------------------------------------------------------
```

## Debugging Gravitino server and integration tests

By default, using MiniGravitino runs the integration tests. Debugging with MiniGravitino is simple 
and easy. You can modify any code in the Gravitino project and set breakpoints anywhere.

## How to debug deploy mode Gravitino server and integration tests

You have two modes to debug the Gravitino server and integration tests, the `embedded` and `deploy` modes.

1. Embedded Mode is the default:
    - This mode is simpler to debug but not as close to the actual environment.
    - Debugging with `MiniGravitino` is simple and easy. You don't have to do any setup to debug directly.

2. Deploy Mode:
    - This mode is closer to the actual environment but more complex to debug.
    - To debug the Gravitino server code, follow these steps:
        - Run the `./gradlew build -x test` command to build the Gravitino project.
        - Use the `./gradlew compileDistribution` command to republish the packaged project in the `distribution` directory.
        - If you only debug integration test codes, You don't have to do any setup to debug directly.
        - If you need to debug Gravitino server codes, follow these steps:
            - Enable the `GRAVITINO_DEBUG_OPTS` environment variable in the `distribution/package/conf/gravitino-env.sh` file to enable remote JVM debugging.
            - Manually start the Gravitino server using the `./distribution/package/bin/gravitino.sh start` command.
            - Select `gravitino.server.main` module classpath in the `Remote JVM Debug` to attach the Gravitino server process and debug it.

## Running on GitHub actions

- GitHub Actions automatically run integration tests in the embedded and deploy mode when you submit a pull request to the 'main' branch.
- View the test results in the `Actions` tab of the pull request page.
- Run the integration tests in several steps:

  - The Gravitino integration tests pull the CI Docker image from the Docker Hub repository. This step typically takes around 15 seconds.
  - If you set the `debug action` label in the pull request, GitHub actions runs an SSH server with `csexton/debugger-action@master`, allowing you to log in to the actions environment for remote debugging.
  - The Gravitino project compiles and packages in the `distribution` directory using the `./gradlew compileDistribution` command.
  - Run the `./gradlew test -PtestMode=[embedded|deploy]` command.

## Test failure

If a test fails, you can retrieve valuable information from the logs and test report. Test reports are in the `./build/reports` directory. The integration test logs are in the `./integrate-test/build` directory. In deploy mode, Gravitino server logs are in the `./distribution/package/logs/` directory. In the event of a test failure within the GitHub workflow, the system generates archived logs and test reports. To obtain the archive, follow these steps:

1. Click the `detail` link associated with the failed integrate test in the pull request. This redirects you to the job page.

   ![pr page Image](assets/test-fail-pr.png)

2. On the job page, locate the `Summary` button on the left-hand side and click it to access the workflow summary page.

   ![job page Image](assets/test-fail-job.png)

3. Look for the Artifacts item on the summary page and download the archive from there.

   ![summary page Image](assets/test-fail-summary.png)
