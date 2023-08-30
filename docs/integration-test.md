<!--
  Copyright 2023 Datastrato.
  This software is licensed under the Apache License version 2.
-->
# How to Run Integration Test

## Introduction
The test cases under the `integration-test` module are integration tests to ensure the correctness of the Graviton Server, and API and Client.
You can run these tests either locally or on GitHub Actions.

## Run on GitHub Actions
When you submit a pull request to branch of `main`, GitHub Actions will automatically run the integration test.
You can check the test results in the `Actions` tab of the pull request page.
The Github Acitons will run the integration test in the following steps:
1. If you set `build docker image` label in the pull request, Then Github Acitons will trigger build the all docker image under the `./dev/docker/` directory, 
normal this step need about 10 minutes. If you modify the code of the dockerfile, you need to set `build docker image` label in the pull request.
2. If you not set `build docker image` label in the pull request, Then Github Acitons will pull docker image `datastrato/hive2:latest` from docker hub repository, normal this step need about 15 seconds.
3. Run the docker image in the Github Acitons environment.
4. Execute `./gradlew integrationTest` command to compile and package the Graviton project in `distribution` directory.
5. Run the integration test cases in the `integration-test` module.
6. Stop the docker image. 
7. Clean up the test environment.

## Run on Locally
Before running the tests, Docker must be installed.
Then, execute `./gradlew build compileDistribution integrationTest` command to compile and package the Graviton project in `distribution` directory.
In addition, Graviton Server and third-part data source docker runtime environment will use some port. Please make sure these ports are not occupied.

+ Graviton Server: `8088` port.
+ Hive Docker runtime environment: `50070`, `50075`, `10002`, `10000`, `8888`, `9083`, `7180`, and `22` port.
