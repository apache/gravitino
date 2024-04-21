<!-- 
- Copyright 2024 Datastrato Pvt Ltd.
- This software is licensed under the Apache License version 2. 
--> 

# Quick Start

1. Install current library in your local machine. 
    ```bash
    pip install -e .
    ```

# Development Environment

1. Install dependency
    ```bash
    pip install -e '.[dev]'
    ```

2. Run integration tests
    ```bash
    cd gravitino
    ./gradlew compileDistribution -x test
    ./gradlew :clients:client-python:test
    ```

3. Test mode

    + Test Principle: Every Python ITs class base on the `IntegrationTestEnv`, `IntegrationTestEnv` will automatically start and stop Gravitino server to support ITs, But when you run multiple ITs class at same time, The first test class that finishes running will shut down the Gravitino server, which will cause other test classes to fail if they can't connect to the Gravitino server.
    + Run test in the IDE: Through `IntegrationTestEnv` class automatically start and stop Gravitino server to support ITs.
    + Run test in the GitHub Action or Gradle command `:client:client-python:test`: Gradle automatically start and stop Gravitino server, and set `EXTERNAL_START_GRAVITINO` environment variable.
