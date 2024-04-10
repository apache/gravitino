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
   
2. Run tests
    ```bash
    cd gravitino
    ./gradlew :clients:client-python:test
    ```

3. Run integration tests
    ```bash
    cd gravitino
    ./gradlew compileDistribution -x test
    ./gradlew :clients:client-python:integrationTest
    ```
