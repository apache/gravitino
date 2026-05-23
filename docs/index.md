---
title: "Apache Gravitino"
slug: "/"
license: "This software is licensed under the Apache License version 2."
---

## Overview

Apache Gravitino is a high-performance, geo-distributed, federated metadata lake. It manages metadata directly across different sources, types, and regions, and provides unified access for data and AI assets.

[Learn more](./overview.md)&rarr;

## Download

Get Gravitino from the [download page](https://gravitino.apache.org/downloads),
or you can build Gravitino from source code. See [How to build Gravitino](./how-to-build.md).

Gravitino runs on Linux and macOS and requires Java 17 (any JVM on x86_64 or ARM64).
To run locally, ensure `java` is on the system `PATH` or set `JAVA_HOME` to a Java installation.

See [How to install Gravitino](./how-to-install.md) to learn how to install the Gravitino server.

Gravitino provides Docker images on [Docker Hub](https://hub.docker.com/u/apache).
Pull the image and run it. For details of the Gravitino Docker image, see
[Docker image details](./docker-image-details.md).

Gravitino also provides a playground to experience the whole Gravitino system with other components.
See the [Gravitino playground repository](https://github.com/apache/gravitino-playground)
and [How to use the playground](./how-to-use-the-playground.md).

## Getting Started

To get started with Gravitino, see [Getting started](./getting-started/index.md) for the details.

* [Getting started locally](./getting-started/index.md#local-workstation): a quick guide to starting
  and using Gravitino locally.

* [Running on Amazon Web Services](./getting-started/index.md#aws): a
  quick guide to starting and using Gravitino on AWS.

* [Running on Google Cloud Platform](./getting-started/index.md#gcp):
  a quick guide to starting and using Gravitino on GCP.

## Manage Metadata with Gravitino

Gravitino exposes a unified REST API along with Java and Python clients for managing metadata across catalogs. See:

* [Manage metalake using Gravitino](./manage-metalake-using-gravitino.md) for metalake operations.
* [Manage relational metadata using Gravitino](./manage-relational-metadata-using-gravitino.md) for catalog, schema, and table operations.
* [Manage view metadata using Gravitino](./manage-view-metadata-using-gravitino.md) for view operations.
* [Manage fileset metadata using Gravitino](./manage-fileset-metadata-using-gravitino.md) for fileset operations.
* [Manage messaging metadata using Gravitino](./manage-messaging-metadata-using-gravitino.md) for topic operations.
* [Manage model metadata using Gravitino](./manage-model-metadata-using-gravitino.md) for model operations.
* [Manage user-defined functions using Gravitino](./manage-user-defined-function-using-gravitino.md) for UDF operations.

For the complete API references, see the [Gravitino OpenAPI definition](./api/rest/gravitino-rest-api), the [Gravitino Javadoc](pathname:///docs/1.3.0-SNAPSHOT/api/java/index.html), and the [Gravitino Python documentation](pathname:///docs/1.3.0-SNAPSHOT/api/python/index.html).

Gravitino also provides a web UI to manage the metadata. Visit the web UI in the browser via `http://<ip-address>:8090`.
See [Gravitino web UI](./webui.md) for details.

Gravitino also provides a Command Line Interface (CLI) to manage the metadata. See [Gravitino CLI](./cli.md) for details.

Gravitino supports the following catalogs:

**Relational catalogs:**

* [**Doris catalog**](./jdbc-doris-catalog.md)
* [**Hologres catalog**](./jdbc-hologres-catalog.md)
* [**Hudi catalog**](./lakehouse-hudi-catalog.md)
* [**Hive catalog**](./apache-hive-catalog.md)
* [**Iceberg catalog**](./lakehouse-iceberg-catalog.md)
* [**MySQL catalog**](./jdbc-mysql-catalog.md)
* [**Paimon catalog**](./lakehouse-paimon-catalog.md)
* [**PostgreSQL catalog**](./jdbc-postgresql-catalog.md)
* [**OceanBase catalog**](./jdbc-oceanbase-catalog.md)\*
* [**StarRocks catalog**](./jdbc-starrocks-catalog.md)
* [**ClickHouse catalog**](./jdbc-clickhouse-catalog.md)\*
* [**Lakehouse generic catalog**](./lakehouse-generic-catalog.md)

To manage table and partition statistics, see [Manage statistics in Gravitino](./manage-statistics-in-gravitino.md).

**Fileset catalogs:**

* [**Fileset catalog**](./fileset-catalog.md)

**Messaging catalogs:**

* [**Kafka catalog**](./kafka-catalog.md)

**Model catalogs:**

* [**Model catalog**](./model-catalog.md)

To automate table-maintenance workflows, see the [Table maintenance service (optimizer)](./table-maintenance-service/optimizer.md). Start with Gravitino's built-in policies and job templates, and extend through the optimizer interfaces when needed.

Catalogs marked with an asterisk (\*) are not in the standard release tarball or Docker image as of 1.2.0. From 1.2.0 onward, Gravitino hosts contributed catalogs in a `catalogs-contrib` folder; they are not in the standard release but can be built and used separately. See [How to build Gravitino](./how-to-build.md#quick-start) for details.

## Apache Gravitino Playground

The Gravitino playground integrates Apache Hadoop, Apache Hive, Trino, MySQL, PostgreSQL, and Gravitino into a complete environment for trying out the system end-to-end. See [Getting started](./getting-started/index.md) and [How to use the Gravitino playground](./how-to-use-the-playground.md).

* [Install Gravitino playground on AWS or GCP](./getting-started/playground.md):
  a quick guide to starting and using the Gravitino playground on AWS or GCP.
* [Install Gravitino playground locally](./getting-started/playground.md):
  a quick guide to starting and using the Gravitino playground locally.
* [How to use the Gravitino playground](./how-to-use-the-playground.md): provides an example of how
  to use Gravitino and other components together.

## Where to Go from Here

### Catalogs

Gravitino supports several catalogs for managing metadata across different sources. See:

* [Doris catalog](./jdbc-doris-catalog.md): a complete guide to using Gravitino to manage Doris data.
* [Hologres catalog](./jdbc-hologres-catalog.md): a complete guide to using Gravitino to manage Hologres data.
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
* [ClickHouse catalog](./jdbc-clickhouse-catalog.md): a complete guide to using Gravitino to manage ClickHouse data.
* [Lakehouse generic catalog](./lakehouse-generic-catalog.md): a complete guide to using Gravitino to manage lakehouse data sources.

### Governance

Gravitino provides governance features to manage metadata in a unified way. See:

* [Manage tags in Gravitino](./manage-tags-in-gravitino.md): a complete guide to using Gravitino
  to manage tags.
* [Manage policies in Gravitino](./manage-policies-in-gravitino.md): a complete guide to using Gravitino
  to manage policies.
* [Manage jobs in Gravitino](./manage-jobs-in-gravitino.md): a complete guide to using Gravitino
  to manage jobs.

### Gravitino Iceberg REST Catalog Service

* [Iceberg REST catalog service](./iceberg-rest-service.md): a guide to using Gravitino
  as an Apache Iceberg REST catalog service.

### Gravitino Lance REST Catalog Service

* [Lance REST catalog service](./lance-rest-service.md): a guide to using Gravitino
  as a Lance REST catalog service.

### Connectors

#### Trino Connector

* [How to use Gravitino Trino connector](./trino-connector/index.md): a complete guide to using the Gravitino Trino connector.

#### Spark Connector

* [Gravitino Spark connector](./spark-connector/spark-connector.md): a complete guide to using the Gravitino Spark connector.

#### Flink Connector

* [Gravitino Flink connector](./flink-connector/flink-connector.md): a complete guide to using the Gravitino Flink connector.

#### Daft Connector

* [Gravitino Daft connector](./daft-connector/daft-connector.md): an introduction to the Gravitino Daft connector for accessing Gravitino metadata from Daft dataframes.


### Server Administration

* [Gravitino metrics](./metrics.md): metrics configuration and a detailed list of the metrics emitted by the Gravitino server.

### Security

Security configuration covers HTTPS, authentication, and access control.

* [HTTPS](./security/how-to-use-https.md): HTTPS configuration.
* [Authentication](./security/how-to-authenticate.md): authentication configuration, including simple, OAuth, and Kerberos.
* [Access control](./security/access-control.md): access-control configuration.
* [CORS](./security/how-to-use-cors.md): CORS configuration.

### Gravitino MCP Server

The Gravitino MCP server lets AI tools manage Gravitino metadata.

* [Gravitino MCP server](./gravitino-mcp-server.md): a complete guide to using the Gravitino MCP server.

### Programming Guides

* [Gravitino Open API](./api/rest/gravitino-rest-api): provides the complete Open API definition of Gravitino.
* [Gravitino Java doc](pathname:///docs/1.3.0-SNAPSHOT/api/java/index.html): provides the Javadoc for the Gravitino API.
* [Gravitino Python doc](pathname:///docs/1.3.0-SNAPSHOT/api/python/index.html): provides the Python doc for the Gravitino API.

### Development Guides

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
