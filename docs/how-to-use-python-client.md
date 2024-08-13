---
title: "How to use Apache Gravitino Python client"
slug: /how-to-use-gravitino-python-client
date: 2024-05-09
keyword: Gravitino Python client
license: This software is licensed under the Apache License version 2.
---
# Apache Gravitino™ (incubating) Python client

Apache Gravitino (incubating) is a high-performance, geo-distributed, federated metadata lake.
It manages the metadata directly in different sources, types, and regions, and also provides users
with unified metadata access for data and AI assets.

Gravitino Python client helps data scientists easily manage metadata using Python language.

![gravitino-python-client-introduction](https://github.com/apache/gravitino/blob/main/docs/assets/gravitino-python-client-introduction.png?raw=true)

## Use Guidance

You can use the Gravitino Python client library with Spark, PyTorch, Tensorflow, Ray, and Python environment.

First of all, You must have a Gravitino server set up and run, You can refer to the document
[How to install Gravitino](https://datastrato.ai/docs/latest/how-to-install/) to build Gravitino server from source code and
install it on your local machine.

### Apache Gravitino Python client API

```shell
pip install apache-gravitino
```

1. [Manage metalake using Gravitino Python API](https://datastrato.ai/docs/latest/manage-metalake-using-gravitino/?language=python)
2. [Manage fileset metadata using Gravitino Python API](https://datastrato.ai/docs/latest/manage-fileset-metadata-using-gravitino/?language=python)

### Apache Gravitino Fileset Example

We offer a playground environment to help you quickly understand how to use the Gravitino Python
client to manage non-tabular data on HDFS via Fileset in Gravitino. You can refer to the
document [How to use the playground](https://datastrato.ai/docs/latest/how-to-use-the-playground/)
to launch a Gravitino server, HDFS, and Jupyter Notebook environment in your local Docker environment.

Waiting for the playground Docker environment to start, you can directly open
`http://localhost:8888/lab/tree/gravitino-fileset-example.ipynb` in the browser and run the example.

The [gravitino-fileset-example](https://github.com/apache/gravitino-playground/blob/main/init/jupyter/gravitino-fileset-example.ipynb)
contains the following code snippets:

1. Install HDFS Python client.
2. Create an HDFS client to connect HDFS and to do some test operations.
3. Install the Gravitino Python client.
4. Initialize the Gravitino admin client and create a Gravitino metalake.
5. Initialize the Gravitino client and list metalakes.
6. Create a Gravitino `Catalog` and special `type` is `Catalog.Type.FILESET` and `provider` is
   [hadoop](https://datastrato.ai/docs/latest/hadoop-catalog/)
7. Create a Gravitino `Schema` with the `location` pointed to an HDFS path, and use `hdfs client` to
   check if the schema location is successfully created in HDFS.
8. Create a `Fileset` with `type` is [Fileset.Type.MANAGED](https://datastrato.ai/docs/latest/manage-fileset-metadata-using-gravitino/#fileset-operations),
   use `hdfs client` to check if the fileset location was successfully created in HDFS.
9. Drop this `Fileset.Type.MANAGED` type fileset and check if the fileset location was
   successfully deleted in HDFS.
10. Create a `Fileset` with `type` is [Fileset.Type.EXTERNAL](https://datastrato.ai/docs/latest/manage-fileset-metadata-using-gravitino/#fileset-operations)
    and `location` pointed to exist HDFS path
11. Drop this `Fileset.Type.EXTERNAL` type fileset and check if the fileset location was
    not deleted in HDFS.

## Apache Gravitino Python Client code

You can use any IDE to further develop the Gravitino Python Client. Directly open the client-python module project in the IDE.

### Prerequisites

+ Python 3.8+
+ Refer to [How to build Gravitino](https://datastrato.ai/docs/latest/how-to-build/#prerequisites) to have necessary build
  environment ready for building.

### Build and testing

1. Clone the Gravitino project.

    ```shell
    git clone git@github.com:apache/gravitino.git
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

   Because Python client connects to the Gravitino Server to run integration tests,
   So it runs `./gradlew compileDistribution -x test` command automatically to compile the
   Gravitino project in the `distribution` directory. When you run integration tests via Gradle
   command or IDE, Gravitino integration test framework (`integration_test_env.py`)
   will start and stop the Gravitino server automatically.

    ```shell
    ./gradlew :clients:client-python:test
    ```

5. Distribute the Gravitino Python client module

    ```shell
    ./gradlew :clients:client-python:distribution
    ```

6. Deploy the Gravitino Python client to https://pypi.org/project/apache-gravitino/

    ```shell
    ./gradlew :clients:client-python:deploy
    ```

## Resources

+ Official website https://gravitino.apache.org/
+ Project home on GitHub: https://github.com/apache/gravitino/
+ Playground with Docker: https://github.com/apache/gravitino-playground
+ User documentation: https://datastrato.ai/docs/
+ Videos on Youtube: https://www.youtube.com/@Datastrato
+ Slack Community: [https://the-asf.slack.com#gravitino](https://the-asf.slack.com/archives/C078RESTT19)

## License

Gravitino is under the Apache License Version 2.0, See the [LICENSE](https://github.com/apache/gravitino/blob/main/LICENSE) for the details.

## ASF Incubator disclaimer

Apache Gravitino is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the Apache Incubator. 
Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, 
and decision making process have stabilized in a manner consistent with other successful ASF projects. 
While incubation status is not necessarily a reflection of the completeness or stability of the code, 
it does indicate that the project has yet to be fully endorsed by the ASF.

Apache and Gravitino are either registered trademarks or trademarks of The Apache Software Foundation.

