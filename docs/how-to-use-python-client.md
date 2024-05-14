---
title: "How to use Gravitino Python client"
slug: /how-to-use-gravitino-python-client
date: 2024-05-09
keyword: Gravitino Python client
license: Copyright 2024 Datastrato Pvt Ltd. This software is licensed under the Apache License version 2.
---
## Introduction

Gravitino is a high-performance, geo-distributed, and federated metadata lake.
It manages the metadata directly in different sources, types, and regions.
It also provides users with unified metadata access for data and AI assets.

Gravitino Python client helps data scientists easily manage metadata in Gravitino services.

![gravitino-python-client-introduction](https://raw.githubusercontent.com/datastrato/gravitino/main/docs/assets/gravitino-python-client-introduction.png)

## Use Guidance
You can use Gravitino Python client library in the Spark, PyTorch, Tensorflow, Ray and Python environment.
First of all, You must have a Gravitino Service, You can refer document of [How to install Gravitino](https://datastrato.ai/docs/latest/how-to-install)
to build Gravitino service from source code and install it in your local.

### Gravitino Python client API

```python
pip install gravitino
```

1. [Manage metalake using Gravitino Python API](https://datastrato.ai/docs/latest/manage-metalake-using-gravitino?language=python)
2. [Manage fileset metadata using Gravitino Python API](https://datastrato.ai/docs/latest/manage-fileset-metadata-using-gravitino?language=python)

### Gravitino Fileset Sample
We provided a Fileset playground environment to help you quickly understand how to use Gravitino Python client to manage HDFS in Gravitino service.
You can refer document of [How to use the playground#Launch AI components of playground](https://datastrato.ai/docs/latest/how-to-use-the-playground#launch-ai-components-of-playground) to launch a Gravitino service, HDFS and Jupyter notebook environment in you local Docker environment.

Waiting for the playground Docker environment to start, you can directly open http://localhost:8888/lab/tree/gravitino-fileset-sample.ipynb in the browser and execute it.

The [gravitino-fileset-sample.ipynb](https://github.com/datastrato/gravitino-playground/blob/main/init/jupyter/gravitino-fileset-sample.ipynb) contains the following code paragraphs:
1. Install HDFS Python client
2. Create a HDFS client to connect HDFS and to do some test operations
3. Install Gravitino Python client
4. Initialize Gravitino admin client and create a Gravitino metalake
5. Initialize Gravitino client and list metalakes
6. Create a Gravitino `Catalog` and special `type` is `Catalog.Type.FILESET` and `provider` is [hadoop](https://datastrato.ai/docs/latest/hadoop-catalog)
7. Create a Gravitino `Schema` and special `location`'s HDFS path, and use `hdfs client` to check if the schema location was successfully created in HDFS
8. Create a `Fileset` and special `type` is [Fileset.Type.MANAGED](https://datastrato.ai/docs/latest/manage-fileset-metadata-using-gravitino#fileset-operations) and `location`'s HDFS path, and use `hdfs client` to check if the fileset location was successfully created in HDFS
9. Drop this `Fileset.Type.MANAGED` type fileset and check if the fileset location was successfully deleted in HDFS
10. Create a `Fileset` and special `type` is [Fileset.Type.EXTERNAL](https://datastrato.ai/docs/latest/manage-fileset-metadata-using-gravitino#fileset-operations) and special `location` to exist HDFS path
11. Drop this `Fileset.Type.EXTERNAL` type fileset and check if the fileset location was not deleted in HDFS

## How to development Gravitino Python Client
You can ues any like IDE to develop Gravitino Python Client. Directly open the client-python module project in the IDE.

#### Prerequisites
+ Python 3.8+

### Compile tools
+ [Gradle](https://gradle.org/)

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
    ```bash
    ./gradlew :clients:client-python:test -PskipITs
    ```

4. Run integration tests
   Because Python client will connect Gravitino Server to execute integration tests,
   So we let Gradle automatic execute `./gradlew compileDistribution -x test` command to compile the Gravitino project in the `distribution` directory.
   When you execute integration test via Gradle command or IDE, Gravitino integration test environment framework (`integration_test_env.py`) Will
   automatic start and stop Gravitino server.

    ```bash
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
