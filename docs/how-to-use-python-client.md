---
title: "How to use Apache Gravitino Python client"
slug: /how-to-use-gravitino-python-client
date: 2024-05-09
keyword: Gravitino Python client
license: This software is licensed under the Apache License version 2.
---
# Apache Gravitino Python client

Apache Gravitino is a high-performance, geo-distributed, and federated metadata lake.
It manages the metadata directly in different sources, types, and regions.
It also provides users the unified metadata access for data and AI assets.

Gravitino Python client helps data scientists easily manage metadata using Python language.

![gravitino-python-client-introduction](./assets/gravitino-python-client-introduction.png)

## Use Guidance

You can use Gravitino Python client library with Spark, PyTorch, Tensorflow, Ray and Python environment.

First of all, You must have a Gravitino server set up and run.
You can check the [install guide](./how-to-install.md) for building Gravitino server from source code
and installing it in your local environment.

### Apache Gravitino Python client API

```shell
pip install apache-gravitino
```

1. [Manage metalake using Gravitino Python API](./manage-metalake-using-gravitino.md?language=python)
1. [Manage fileset metadata using Gravitino Python API](./manage-fileset-metadata-using-gravitino.md?language=python)

### Apache Gravitino Fileset Example

The Gravitino project provides a playground environment.
Using this environment, you can quickly learn how to use the Gravitino Python client
to manage non-tabular data on HDFS via Fileset in Gravitino.
For more details, check [installing the playground](./playground/install.md).

With the playground environment ready, in your browser, you can go to
`http://localhost:18888/lab/tree/gravitino-fileset-example.ipynb` and run the example.

The `gravitino-fileset-example` contains the following code snippets:

1. Install HDFS Python client.
1. Create a HDFS client to connect HDFS and to do some test operations.
1. Install Gravitino Python client.
1. Initialize Gravitino admin client and create a Gravitino metalake.
1. Initialize Gravitino client and list metalakes.
1. Create a Gravitino `Catalog` and special `type` is `Catalog.Type.FILESET` and `provider` is
   [hadoop](./catalogs/fileset/hadoop/hadoop-catalog.md)
1. Create a Gravitino `Schema` with the `location` pointed to a HDFS path,
   and use `hdfs client` to check if the schema location is successfully created in HDFS.
1. Create a `Fileset` with `type` is [Fileset.Type.MANAGED](./manage-fileset-metadata-using-gravitino.md#fileset-operations),
   use `hdfs client` to check if the fileset location was successfully created in HDFS.
1. Drop this `Fileset.Type.MANAGED` type fileset and check if the fileset location was successfully deleted.
1. Create a `Fileset` with `type` is [Fileset.Type.EXTERNAL](./manage-fileset-metadata-using-gravitino.md#fileset-operations)
   with `location` pointing to an existing HDFS path.
1. Drop this `Fileset.Type.EXTERNAL` type fileset and check if the fileset location was not deleted in HDFS.

## How to development Apache Gravitino Python Client

You can ues any IDE to develop Gravitino Python Client.
Directly open the client-python module project in the IDE.

### Prerequisites

- Python 3.8+
- Refer to [How to build Gravitino](./develop/how-to-build.md#prerequisites) to have necessary build
  environment ready for building.

### Build and testing

1. Clone the Gravitino project.

   ```shell
   git clone git@github.com:apache/gravitino.git
   ```

1. Build the Gravitino Python client module

   ```shell
   ./gradlew :clients:client-python:build
   ```

1. Run unit tests

   ```shell
   ./gradlew :clients:client-python:test -PskipITs
   ```

1. Run integration tests

   Because Python client connects to Gravitino Server to run integration tests,
   So it runs `./gradlew compileDistribution -x test` command automatically
   to compile the Gravitino project in the `distribution` directory.
   When you run integration tests via Gradle command or IDE,
   Gravitino integration test framework (`integration_test_env.py`) will start
   and stop Gravitino server automatically.

   ```shell
   ./gradlew :clients:client-python:test
   ```

1. Distribute the Gravitino Python client module

   ```shell
   ./gradlew :clients:client-python:distribution
   ```

1. Deploy the Gravitino Python client to https://pypi.org/project/apache-gravitino/

   ```shell
   ./gradlew :clients:client-python:deploy
   ```

## Resources

- Official website https://gravitino.apache.org/
- Project home on GitHub: https://github.com/apache/gravitino/
- Playground with Docker: https://github.com/apache/gravitino-playground
- User documentation: https://gravitino.apache.org/docs/
- Slack Community: [https://the-asf.slack.com#gravitino](https://the-asf.slack.com/archives/C078RESTT19)

## License

Gravitino is under the Apache License Version 2.0, see the
[LICENSE](https://github.com/apache/gravitino/blob/main/LICENSE) for the details.

## ASF Incubator disclaimer

Apache Gravitino is an effort undergoing incubation at The Apache Software Foundation (ASF),
sponsored by the Apache Incubator. Incubation is required of all newly accepted projects
until a further review indicates that the infrastructure, communications,
and decision making process have stabilized in a manner consistent with other successful ASF projects. 
While incubation status is not necessarily a reflection of the completeness or stability of the code, 
it does indicate that the project has yet to be fully endorsed by the ASF.

