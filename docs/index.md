---
title: Apache Gravitino overview
slug: /
license: "This software is licensed under the Apache License version 2."
---

## What's Apache Gravitino?

Apache Gravitino is a high-performance, geo-distributed, and federated metadata lake.
It manages the metadata directly in different sources, types, and regions.
It also provides users with unified metadata access for data and AI assets.

[Learn more](./overview.md)&rarr;

## Downloading

You can get Gravitino from the [download page](https://gravitino.apache.org/downloads),
or you can build Gravitino from source code. See [How to build Gravitino](./how-to-build.md).

Gravitino runs on both Linux and macOS platforms, and it requires the installation of
Java 8, Java 11, or Java 17. Gravitino trino-connector runs with Trino, and requires Java 17.
This should include JVMs on x86_64 and ARM64.
It's easy to run locally on one machine, all you need is to have `java` installed on
your system `PATH`, or the `JAVA_HOME` environment variable pointing to a Java installation.

See [How to install Gravitino](./how-to-install.md) to learn how to install the Gravitino server.

Gravitino provides Docker images on [Docker Hub](https://hub.docker.com/u/apache).
Pull the image and run it. For details of the Gravitino Docker image, see
[Docker image details](./docker-image-details.md).

Gravitino also provides a playground to experience the whole Gravitino system with other components.
See the [Gravitino playground repository](https://github.com/apache/gravitino-playground)
and [How to use the playground](./how-to-use-the-playground.md).

## Getting started

To get started with Gravitino, see [Getting started](./getting-started/index.md) for the details.

* [Getting started locally](./getting-started/index.md#local): a quick guide to starting
  and using Gravitino locally.

* [Running on Amazon Web Services](./getting-started/index.md#aws): a
  quick guide to starting and using Gravitino on AWS.

* [Running on Google Cloud Platform](./getting-started/index.md#gcp):
  a quick guide to starting and using Gravitino on GCP.

## How to use Apache Gravitino

Gravitino provides two SDKs to manage metadata from different catalogs in a unified way:
the REST API and the Java SDK.
You can use either to manage metadata. See

* [Manage metalake using Gravitino](./manage-metalake-using-gravitino.md) to learn how to manage
  metalakes.
* [Manage relational metadata using Gravitino](./manage-relational-metadata-using-gravitino.md)
  to learn how to manage relational metadata.
* [Manage fileset metadata using Gravitino](./manage-fileset-metadata-using-gravitino.md) to learn
  how to manage fileset metadata.
* [Manage messaging metadata using Gravitino](./manage-messaging-metadata-using-gravitino.md) to learn how to manage
  messaging metadata.
* [Manage model metadata using Gravitino](./manage-model-metadata-using-gravitino.md) to learn how to manage
  model metadata.

Also, you can find the complete REST API definition in
[Gravitino Open API](./api/rest/gravitino-rest-api),
Java SDK definition in [Gravitino Java doc](pathname:///docs/1.0.0-SNAPSHOT/api/java/index.html),
and Python SDK definition in [Gravitino Python doc](pathname:///docs/1.0.0-SNAPSHOT/api/python/index.html).

Gravitino also provides a web UI to manage the metadata. Visit the web UI in the browser via `http://<ip-address>:8090`.
See [Gravitino web UI](./webui.md) for details.

Gravitino also provides a Command Line Interface (CLI) to manage the metadata. See [Gravitino CLI](./cli.md) for details.

Gravitino currently supports the following catalogs:

**Relational catalogs:**

* [**Doris catalog**](./jdbc-doris-catalog.md)
* [**Hudi catalog**](./lakehouse-hudi-catalog.md)
* [**Hive catalog**](./apache-hive-catalog.md)
* [**Iceberg catalog**](./lakehouse-iceberg-catalog.md)
* [**MySQL catalog**](./jdbc-mysql-catalog.md)
* [**Paimon catalog**](./lakehouse-paimon-catalog.md)
* [**PostgreSQL catalog**](./jdbc-postgresql-catalog.md)
* [**OceanBase catalog**](./jdbc-oceanbase-catalog.md)
* [**StarRocks catalog**](./jdbc-starrocks-catalog.md)

If you want to operate table and partition statistics, you can refer to the [document](./manage-statistics-in-gravitino.md).

**Fileset catalogs:**

* [**Fileset catalog**](./fileset-catalog.md)

**Messaging catalogs:**

* [**Kafka catalog**](./kafka-catalog.md)

**Model catalogs:**

* [**Model catalog**](./model-catalog.md)

## Apache Gravitino playground

To experience Gravitino with other components easily, Gravitino provides a playground to run.
It integrates Apache Hadoop, Apache Hive, Trino, MySQL, PostgreSQL, and Gravitino together as a
complete environment. To experience all the features, see
[Getting started](./getting-started/index.md) and
[How to use the Gravitino playground](./how-to-use-the-playground.md).

* [Install Gravitino playground on AWS or GCP](./getting-started/playground.md):
  a quick guide to starting and using the Gravitino playground on AWS or GCP.
* [Install Gravitino playground locally](./getting-started/playground.md):
  a quick guide to starting and using the Gravitino playground locally.
* [How to use the Gravitino playground](./how-to-use-the-playground.md): provides an example of how
  to use Gravitino and other components together.

## Where to go from here

### Catalogs

Gravitino supports different catalogs to manage the metadata in different sources. Please see:

* [Doris catalog](./jdbc-doris-catalog.md): a complete guide to using Gravitino to manage Doris data.
* [StarRocks catalog](./jdbc-starrocks-catalog.md): a complete guide to using Gravitino to manage StarRocks data.
* [Fileset catalog](./fileset-catalog.md): a complete guide to using Gravitino to manage fileset
  using Hadoop Compatible File System (HCFS).
* [Hive catalog](./apache-hive-catalog.md): a complete guide to using Gravitino to manage Apache Hive data.
* [Hudi catalog](./lakehouse-hudi-catalog.md): a complete guide to using Gravitino to manage Apache Hudi data.
* [Iceberg catalog](./lakehouse-iceberg-catalog.md): a complete guide to using Gravitino to manage Apache Iceberg data.
* [Kafka catalog](./kafka-catalog.md): a complete guide to using Gravitino to manage Kafka topics metadata.
* [Model catalog](./model-catalog.md): a complete guide to using Gravitino to manage model metadata.
* [MySQL catalog](./jdbc-mysql-catalog.md): a complete guide to using Gravitino to manage MySQL data.
* [Paimon catalog](./lakehouse-paimon-catalog.md): a complete guide to using Gravitino to manage Apache Paimon data.
* [PostgreSQL catalog](./jdbc-postgresql-catalog.md): a complete guide to using Gravitino to manage PostgreSQL data.
* [OceanBase catalog](./jdbc-oceanbase-catalog.md): a complete guide to using Gravitino to manage OceanBase data.

### Governance

Gravitino provides governance features to manage metadata in a unified way. See:

* [Manage tags in Gravitino](./manage-tags-in-gravitino.md): a complete guide to using Gravitino
  to manage tags.
* [Manage policies in Gravitino](./manage-policies-in-gravitino.md): a complete guide to using Gravitino
  to manage policies.
* [Manage jobs in Gravitino](./manage-jobs-in-gravitino.md): a complete guide to using Gravitino
  to manage jobs.

### Gravitino Iceberg REST catalog service

* [Iceberg REST catalog service](./iceberg-rest-service.md): a guide to using Gravitino
  as an Apache Iceberg REST catalog service.

### Connectors

#### Trino connector

Gravitino provides a Trino connector to manage Trino metadata in a unified way. To use the Trino connector, see:

* [How to use Gravitino Trino connector](./trino-connector/index.md): a complete guide to using the Gravitino Trino connector.

#### Spark connector

Gravitino provides a Spark connector to manage metadata in a unified way. To use the Spark connector, see:

* [Gravitino Spark connector](./spark-connector/spark-connector.md): a complete guide to using the Gravitino Spark connector.

#### Flink connector

Gravitino provides a Flink connector to manage metadata in a unified way. To use the Flink connector, see:

* [Gravitino Flink connector](./flink-connector/flink-connector.md): a complete guide to using the Gravitino Flink connector.


### Server administration

Gravitino provides several ways to configure and manage the Gravitino server. See:

* [Gravitino metrics](./metrics.md): provides metrics configurations and detailed a metrics list
  of the Gravitino server.

### Security

Gravitino provides security configurations for Gravitino, including HTTPS, authentication and access control configurations.

* [HTTPS](./security/how-to-use-https.md): provides HTTPS configurations.
* [Authentication](./security/how-to-authenticate.md): provides authentication configurations including simple, OAuth, Kerberos.
* [Access Control](./security/access-control.md): provides access control configurations.
* [CORS](./security/how-to-use-cors.md): provides CORS configurations.

### Gravitino MCP server

Gravitino MCP server provides the ability to manage Gravitino metadata for AI tools.

* [Gravitino MCP server](./gravitino-mcp-server.md): a complete guide to using the Gravitino MCP server.

### Programming guides

* [Gravitino Open API](./api/rest/gravitino-rest-api): provides the complete Open API definition of Gravitino.
* [Gravitino Java doc](pathname:///docs/1.0.0-SNAPSHOT/api/java/index.html): provides the Javadoc for the Gravitino API.
* [Gravitino Python doc](pathname:///docs/1.0.0-SNAPSHOT/api/python/index.html): provides the Python doc for the Gravitino API.

### Development guides

* [How to build Gravitino](./how-to-build.md): a complete guide to building Gravitino from
  source.
* [How to test Gravitino](./how-to-test.md): a complete guide to running Gravitino unit and
  integration tests.
* [How to sign and verify Gravitino releases](./how-to-sign-releases.md): a guide to signing and verifying
  a Gravitino release.
* [Publish Docker images](./publish-docker-images.md): a guide to publishing Gravitino Docker images;
  also lists the change logs of Gravitino CI Docker images and release images.
* [How to upgrade Gravitino](./how-to-upgrade.md): a guide to upgrading the schema of Gravitino storage backend from one release version to another.

<img src="https://analytics.apache.org/matomo.php?idsite=62&rec=1&bots=1&action_name=Overview" alt="" />
