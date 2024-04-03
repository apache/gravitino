---
title: Gravitino overview
slug: /
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

## What's Gravitino?

Gravitino is a high-performance, geo-distributed, and federated metadata lake. It manages the
metadata directly in different sources, types, and regions. It also provides users with unified
metadata access for data and AI assets.

[Learn more](./overview.md)&rarr;

## Downloading

You can get Gravitino from the [GitHub release page](https://github.com/datastrato/gravitino/releases),
or you can build Gravitino from source code. See [How to build Gravitino](./how-to-build.md).

Gravitino runs on both Linux and macOS platforms, and it requires the installation of Java 8, Java 11, or Java 17. Gravitino trino-connector runs with
Trino, and requires Java 17. This should include JVMs on x86_64 and
ARM64. It's easy to run locally on one machine, all you need is to have `java` installed on
your system `PATH`, or the `JAVA_HOME` environment variable pointing to a Java installation.

See [How to install Gravitino](./how-to-install.md) to learn how to install the Gravitino server.

Gravitino provides Docker images on [Docker Hub](https://hub.docker.com/u/datastrato).
Pull the image and run it. For details of the Gravitino Docker image, see
[Docker image details](./docker-image-details.md).

Gravitino also provides a playground to experience the whole Gravitino system with other components.
See the [Gravitino playground repository](https://github.com/datastrato/gravitino-playground)
and [How to use the playground](./how-to-use-the-playground.md).

## Getting started

To get started with Gravitino, see [Getting started](./getting-started.md) for the details.

* [Getting started locally](./getting-started.md#getting-started-locally): a quick guide to starting
  and using Gravitino locally.
* [Running on Amazon Web Services](./getting-started.md#getting-started-on-amazon-web-services): a
  quick guide to starting and using Gravitino on AWS.
* [Running on Google Cloud Platform](./getting-started.md#getting-started-on-google-cloud-platform):
  a quick guide to starting and using Gravitino on GCP.

## How to use Gravitino

Gravitino provides two SDKs to manage metadata from different catalogs in a unified way: the
REST API and the Java SDK. You can use either to manage metadata. See

* [Manage metalake using Gravitino](./manage-metalake-using-gravitino.md) to learn how to manage
  metalakes.
* [Manage relational metadata using Gravitino](./manage-relational-metadata-using-gravitino.md)
  to learn how to manage relational metadata.
* [Manage fileset metadata using Gravitino](./manage-fileset-metadata-using-gravitino.md) to learn
  how to manage fileset metadata.

Also, you can find the complete REST API definition in
[Gravitino Open API](./api/rest/gravitino-rest-api), and the
Java SDK definition in [Gravitino Javadoc](pathname:///docs/0.4.0/api/java/index.html).

Gravitino provides a web UI to manage the metadata. Visit the web UI in the browser via `http://<ip-address>:8090`. See [Gravitino web UI](./webui.md) for details.

Gravitino currently supports the following catalogs:

**Relational catalogs:**

* [**Iceberg catalog**](./lakehouse-iceberg-catalog.md)
* [**Hive catalog**](./apache-hive-catalog.md)
* [**MySQL catalog**](./jdbc-mysql-catalog.md)
* [**PostgreSQL catalog**](./jdbc-postgresql-catalog.md)

**Fileset catalogs:**

* [**Hadoop catalog**](./hadoop-catalog.md)

Gravitino also provides an Iceberg REST catalog service for the Iceberg table format. See the
[Iceberg REST catalog service](./iceberg-rest-service.md) for details.

## Gravitino playground

To experience Gravitino with other components easily, Gravitino provides a playground to run. It
integrates Apache Hadoop, Apache Hive, Trino, MySQL, PostgreSQL, and Gravitino together as a
complete environment. To experience all the features, see
[Getting started](./getting-started.md) and [How to use the Gravitino playground](./how-to-use-the-playground.md).

* [Install Gravitino playground on AWS or GCP](./getting-started.md#installing-gravitino-playground-on-aws-or-google-cloud-platform):
  a quick guide to starting and using the Gravitino playground on AWS or GCP.
* [Install Gravitino playground locally](./getting-started.md#installing-gravitino-playground-locally):
  a quick guide to starting and using the Gravitino playground locally.
* [How to use the Gravitino playground](./how-to-use-the-playground.md): provides an example of how
  to use Gravitino and other components together.

## Where to go from here

### Catalogs

Gravitino supports different catalogs to manage the metadata in different sources. Please see:

* [Iceberg catalog](./lakehouse-iceberg-catalog.md): a complete guide to using Gravitino to
  manage Apache Iceberg data.
* [Iceberg REST catalog service](./iceberg-rest-service.md): a
  complete guide to using Gravitino as an Apache Iceberg REST catalog service.
* [Hive catalog](./apache-hive-catalog.md): a complete guide to using Gravitino to manage Apache Hive data.
* [MySQL catalog](./jdbc-mysql-catalog.md): a complete guide to using Gravitino to manage MySQL data.
* [PostgreSQL catalog](./jdbc-postgresql-catalog.md): a complete guide to using Gravitino to manage PostgreSQL data.
* [Hadoop catalog](./hadoop-catalog.md): a complete guide to using Gravitino to manage fileset
  using Hadoop Compatible File System (HCFS).

### Trino connector

Gravitino provides a Trino connector to manage Trino metadata in a unified
way. To use the Trino connector, see:

* [How to use Gravitino Trino connector](./trino-connector/index.md): a complete guide to using the Gravitino
  Trino connector.

### Server administration

Gravitino provides several ways to configure and manage the Gravitino server. See:

* [Security](./security.md): provides security configurations for Gravitino, including HTTPS
  and OAuth2 configurations.
* [Gravitino metrics](./metrics.md): provides metrics configurations and detailed a metrics list
  of the Gravitino server.

### Programming guides

* [Gravitino Open API](./api/rest/gravitino-rest-api): provides the complete Open API definition of
  Gravitino.
* [Gravitino Javadoc](pathname:///docs/0.4.0/api/java/index.html): provides the Javadoc for the Gravitino API.

### Development guides

* [How to build Gravitino](./how-to-build.md): a complete guide to building Gravitino from
  source.
* [How to test Gravitino](./how-to-test.md): a complete guide to running Gravitino unit and
  integration tests.
* [How to sign and verify Gravitino releases](./how-to-sign-releases.md): a guide to signing and verifying
  a Gravitino release.
* [Publish Docker images](./publish-docker-images.md): a guide to publishing Gravitino Docker images;
  also lists the change logs of Gravitino CI Docker images and release images.
