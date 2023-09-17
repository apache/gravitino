<!--
  Copyright 2023 Datastrato.
  This software is licensed under the Apache License version 2.
-->
# How to Run Integration Tests

## Introduction
The `integration-test` module contains test cases that serve as integration tests to ensure the correctness of the Graviton Server, API, and Client. 
You can run these tests locally or on GitHub Actions.

## Running on GitHub Actions
When you submit a pull request to the `main` branch, GitHub Actions will automatically run the integration tests. 
You can view the test results in the `Actions` tab of the pull request page. 
The integration tests are executed in the following steps:

1. If you set the `build docker image` label in the pull request, GitHub Actions will trigger the build of all Docker images under the `./dev/docker/` directory. This step usually takes around 10 minutes. If you have made changes to the Dockerfile, you need to set the `build docker image` label in the pull request.
2. If you do not set the `build docker image` label in the pull request, GitHub Actions will pull the Docker image `datastrato/graviton-ci-hive:latest` from the Docker Hub repository. This step usually takes around 15 seconds.
3. The Docker image is then run in the GitHub Actions environment.
4. If you set the `debug action` label in the pull request, GitHub Actions will run an SSH server with `csexton/debugger-action@master`, allowing you to remotely log in to the Actions environment for debugging purposes.
5. The Graviton project is compiled and packaged in the `distribution` directory using the `./gradlew compileDistribution` command.
6. The integration test cases in the `integration-test` module are executed using the `./gradlew integrationTest` command.
7. The Docker image is stopped.
8. The test environment is cleaned up.

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