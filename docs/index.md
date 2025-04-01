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
or you can build Gravitino from source code. See [How to build Gravitino](./develop/how-to-build.md).

Gravitino runs on both Linux and macOS platforms, and it requires the installation of
Java 8, Java 11, or Java 17. Gravitino trino-connector runs with Trino, and requires Java 17.
This should include JVMs on x86_64 and ARM64.
It's easy to run locally on one machine, all you need is to have `java` installed on
your system `PATH`, or the `JAVA_HOME` environment variable pointing to a Java installation.

See [How to install Gravitino](./install/install.md) to learn how to install the Gravitino server.

Gravitino provides Docker images on [Docker Hub](https://hub.docker.com/u/apache).
Pull the image and run it.
For details of the Gravitino Docker image, see [Docker image details](./develop/docker-image-details.md).

Gravitino also provides a playground environment for your to experiment with the whole Gravitino system.
For the details, check [installing the Apache Gravitino layground](./playground/install.md).

## Getting started

To get started with Gravitino, see [Getting started](./getting-started/index.md) for the details.

* [Getting started locally](./getting-started/index.md#local):
  a quick guide to starting and using Gravitino locally.

* [Running on Amazon Web Services](./getting-started/index.md#aws):
  a quick guide to starting and using Gravitino on AWS.

* [Running on Google Cloud Platform](./getting-started/index.md#gcp):
  a quick guide to starting and using Gravitino on GCP.

## How to use Apache Gravitino

Gravitino provides two SDKs to manage metadata from different catalogs in a unified way:
the REST API and the Java SDK.
You can use either to manage metadata. See

* [Manage metalake using Gravitino](./admin/metalake.md) to learn how to manage metalakes.
* [Manage relational metadata using Gravitino](./metadata/relational.md)
  to learn how to manage relational metadata.
* [Manage fileset metadata using Gravitino](./metadata/fileset.md) to learn
  how to manage fileset metadata.
* [Manage messaging metadata using Gravitino](./metadata/messaging.md) to learn how to manage
  messaging metadata.
* [Manage model metadata using Gravitino](./metadata/model.md) to learn how to manage
  model metadata.

Also, you can find the complete REST API definition in [Gravitino Open API](./api/rest/gravitino-rest-api),
Java SDK definition in [Gravitino Java doc](pathname:///docs/0.9.0-incubating-SNAPSHOT/api/java/index.html),
and Python SDK definition in [Gravitino Python doc](pathname:///docs/0.9.0-incubating-SNAPSHOT/api/python/index.html).

Gravitino also provides a web UI to manage the metadata.
Visit the web UI in the browser via `http://<ip-address>:8090`.
See [Gravitino web UI](./client/webui.md) for details.

Gravitino also provides a Command Line Interface (CLI) to manage the metadata.
See [Gravitino CLI](./client/cli.md) for details.

Gravitino currently supports the following catalogs:

**Relational catalogs:**

* [**Doris catalog**](./catalogs/relational/jdbc/doris.md)
* [**Hudi catalog**](./catalogs/relational/lakehouse/hudi.md)
* [**Hive catalog**](./catalogs/relational/hive/index.md)
* [**Iceberg catalog**](./catalogs/relational/lakehouse/iceberg.md)
* [**MySQL catalog**](./catalogs/relational/jdbc/mysql.md)
* [**Paimon catalog**](./catalogs/relational/lakehouse/paimon.md)
* [**PostgreSQL catalog**](./catalogs/relational/jdbc/postgresql.md)
* [**OceanBase catalog**](./catalogs/relational/jdbc/oceanbase.md)

**Fileset catalogs:**

* [**Hadoop catalog**](./catalogs/fileset/hadoop/index.md)

**Messaging catalogs:**

* [**Kafka catalog**](./catalogs/messaging/kafka/index.md)

**Model catalogs:**

* [**Model catalog**](./catalogs/model/index.md)

## Apache Gravitino playground

Gravitino provides a playground environment for you To experiment Gravitino with other components.
This environment comprises components like *Apache Hadoop*, *Apache Hive*, *Trino*, *MySQL*, *PostgreSQL*,
and the *Apache Gravitino* server.
For the details, please check [installing the Gravitino playground](./playground/install.md)
and [using the playground](./playground/using-the-playground.md).
The playground environment can be installed on AWS, GCP or your local environment.

## Where to go from here

### Catalogs

Gravitino supports different catalogs to manage the metadata in different sources. Please see:

* [Doris catalog](./catalogs/relational/jdbc/doris.md): a complete guide to using Gravitino to manage Doris data.
* [Hadoop catalog](./catalogs/fileset/hadoop/index.md): a complete guide to using Gravitino to manage fileset
  using Hadoop Compatible File System (HCFS).
* [Hive catalog](./catalogs/relational/hive/index.md): a complete guide to using Gravitino to manage Apache Hive data.
* [Hudi catalog](./catalogs/relational/lakehouse/hudi.md): a complete guide to using Gravitino to manage Apache Hudi data.
* [Iceberg catalog](./catalogs/relational/lakehouse/iceberg.md): a complete guide to using Gravitino to manage Apache Iceberg data.
* [Kafka catalog](./catalogs/messaging/kafka/index.md): a complete guide to using Gravitino to manage Kafka topics metadata.
* [Model catalog](./catalogs/model/index.md): a complete guide to using Gravitino to manage model metadata.
* [MySQL catalog](./catalogs/relational/jdbc/mysql.md): a complete guide to using Gravitino to manage MySQL data.
* [OceanBase catalog](./catalogs/relational/jdbc/oceanbase.md): a complete guide to using Gravitino to manage OceanBase data.
* [Paimon catalog](./catalogs/relational/lakehouse/paimon.md): a complete guide to using Gravitino to manage Apache Paimon data.
* [PostgreSQL catalog](./catalogs/relational/jdbc/postgresql.md): a complete guide to using Gravitino to manage PostgreSQL data.

### Governance

Gravitino provides governance features to manage metadata in a unified way. See:

* [Manage tags in Gravitino](./metadata/tags.md):
  A complete guide to using Gravitino to manage tags.

### Gravitino Iceberg REST catalog service

* [Iceberg REST catalog service](./admin/iceberg-server.md):
  A guide to using Gravitino as an Apache Iceberg REST catalog service.

### Connectors

#### Trino connector

Gravitino provides a Trino connector to manage Trino metadata in a unified way. To use the Trino connector, see:

* [How to use Gravitino Trino connector](./connectors/trino/index.md): a complete guide to using the Gravitino Trino connector.

#### Spark connector

Gravitino provides a Spark connector to manage metadata in a unified way. To use the Spark connector, see:

* [Gravitino Spark connector](./connectors/spark/index.md): a complete guide to using the Gravitino Spark connector.

#### Flink connector

Gravitino provides a Flink connector to manage metadata in a unified way. To use the Flink connector, see:

* [Gravitino Flink connector](./connectors/flink/index.md): a complete guide to using the Gravitino Flink connector.

### Server administration

Gravitino provides several ways to configure and manage the Gravitino server. See:

* [Gravitino metrics](./admin/metrics.md): provides metrics configurations and detailed a metrics list
  of the Gravitino server.
* [How to upgrade Gravitino](./admin/upgrade.md): a guide to upgrading the schema of Gravitino storage backend from one release version to another.

### Security

Gravitino provides security configurations for Gravitino, including HTTPS, authentication and access control configurations.

* [HTTPS](./security/how-to-use-https.md): provides HTTPS configurations.
* [Authentication](./security/authentication.md): provides authentication configurations including simple, OAuth, Kerberos.
* [Access Control](./security/access-control.md): provides access control configurations.
* [CORS](./security/cors.md): Guide on CORS configurations.

### Programming guides

* [Gravitino Open API](./api/rest/gravitino-rest-api): provides the complete Open API definition of Gravitino.
* [Gravitino Java doc](pathname:///docs/0.9.0-incubating-SNAPSHOT/api/java/index.html): provides the Javadoc for the Gravitino API.
* [Gravitino Python doc](pathname:///docs/0.9.0-incubating-SNAPSHOT/api/python/index.html): provides the Python doc for the Gravitino API.

### Development guides

* [How to build Gravitino](./develop/how-to-build.md): a complete guide to building Gravitino from source.
* [How to test Gravitino](./develop/testing.md): a complete guide to running Gravitino unit and integration tests.
* [How to sign and verify Gravitino releases](./develop/release-signing.md): a guide to signing and verifying
  a Gravitino release.
* [Publish Docker images](./develop/publish-docker-images.md): a guide to publishing Gravitino Docker images;
  also lists the change logs of Gravitino CI Docker images and release images.

<img src="https://analytics.apache.org/matomo.php?idsite=62&rec=1&bots=1&action_name=Overview" alt="" />
