---
title: "How to use Gravitino Python client"
slug: /how-to-use-gravitino-python-client
date: 2024-05-09
keyword: Gravitino Python client
license: Copyright 2024 Datastrato Pvt Ltd. This software is licensed under the Apache License version 2.
---
## Introduction

Gravitino is a high-performance, geo-distributed, and federated metadata lake.
It manages the metadata directly in different sources, types, and regions, also provides users
the unified metadata access for data and AI assets.

Gravitino Python client helps data scientists easily manage metadata using Python language.

![gravitino-python-client-introduction](./assets/gravitino-python-client-introduction.png)

## Use Guidance

You can use Gravitino Python client library with Spark, PyTorch, Tensorflow, Ray and Python environment.

First of all, You must have a Gravitino server set up and run, You can refer document of
[How to install Gravitino](./how-to-install.md) to build Gravitino server from source code and
install it in your local.

### Gravitino Python client API

```shell
pip install gravitino
```

1. [Manage metalake using Gravitino Python API](./manage-metalake-using-gravitino.md)
2. [Manage fileset metadata using Gravitino Python API](./manage-fileset-metadata-using-gravitino.md)

### Gravitino Fileset Example

We offer a playground environment to help you quickly understand how to use Gravitino Python
client to manage non-tabular data on HDFS via Fileset in Gravitino. You can refer to the
document [How to use the playground#Launch AI components of playground](./how-to-use-the-playground.md#launch-ai-components-of-playground)
to launch a Gravitino server, HDFS and Jupyter notebook environment in you local Docker environment.

Waiting for the playground Docker environment to start, you can directly open
`http://localhost:8888/lab/tree/gravitino-fileset-sample.ipynb` in the browser and run the example.

The [gravitino-fileset-example](https://github.com/datastrato/gravitino-playground/blob/main/init/jupyter/gravitino-fileset-sample.ipynb)
contains the following code snippets:

1. Install HDFS Python client.
2. Create a HDFS client to connect HDFS and to do some test operations.
3. Install Gravitino Python client.
4. Initialize Gravitino admin client and create a Gravitino metalake.
5. Initialize Gravitino client and list metalakes.
6. Create a Gravitino `Catalog` and special `type` is `Catalog.Type.FILESET` and `provider` is
   [hadoop](./hadoop-catalog.md)
7. Create a Gravitino `Schema` with the `location` pointed to a HDFS path, and use `hdfs client` to
   check if the schema location is successfully created in HDFS.
8. Create a `Fileset` with `type` is [Fileset.Type.MANAGED](./manage-fileset-metadata-using-gravitino.md#fileset-operations),
   use `hdfs client` to check if the fileset location was successfully created in HDFS.
9. Drop this `Fileset.Type.MANAGED` type fileset and check if the fileset location was
   successfully deleted in HDFS.
10. Create a `Fileset` with `type` is [Fileset.Type.EXTERNAL](./manage-fileset-metadata-using-gravitino#fileset-operations)
    and `location` pointed to exist HDFS path
11. Drop this `Fileset.Type.EXTERNAL` type fileset and check if the fileset location was
    not deleted in HDFS.

## How to development Gravitino Python Client

You can ues any IDE to develop Gravitino Python Client. Directly open the client-python module project in the IDE.

### Prerequisites

+ Python 3.8+
+ Refer to [How to build Gravitino](./how-to-build.md#prerequisites) to have necessary build
  environment ready for building.

### Build and testing

1. Clone the Gravitino project.

    ```shell
    git clone git@github.com:datastrato/gravitino.git
    ```

2. Build the Gravitino Python client module

    ```shell
    ./gradlew :clients:client-python:build
    ```

3. Run unit tests

    ```shell
    ./gradlew :clients:client-python:test -PskipITs
    ```

4. Run integration tests

   Because Python client connects to Gravitino Server to run integration tests,
   So it runs `./gradlew compileDistribution -x test` command automatically to compile the
   Gravitino project in the `distribution` directory. When you run integration tests via Gradle
   command or IDE, Gravitino integration test framework (`integration_test_env.py`)
   will start and stop Gravitino server automatically.

    ```shell
    ./gradlew :clients:client-python:test
    ```

5. Distribute the Gravitino Python client module

    ```shell
    ./gradlew :clients:client-python:distribution
    ```

6. Deploy the Gravitino Python client to https://pypi.org/project/gravitino/

    ```shell
    ./gradlew :clients:client-python:deploy
    ```

## Resources

+ Official website https://datastrato.ai/
+ Project home on GitHub: https://github.com/datastrato/gravitino/
+ Playground with Docker: https://github.com/datastrato/gravitino-playground
+ User documentation: https://datastrato.ai/docs/
+ Videos on Youtube: https://www.youtube.com/@Datastrato
+ Twitter: https://twitter.com/datastrato
+ Linkedin: https://www.linkedin.com/company/datastrato
+ Slack Community: [https://join.slack.com/t/datastrato-community](https://join.slack.com/t/datastrato-community/shared_invite/zt-2a8vsjoch-cU_uUwHA_QU6Ab50thoq8w)
+ Discourse Community: https://gravitino.discourse.group/
