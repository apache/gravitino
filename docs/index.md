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
or you can build Gravitino from source code, please see [How to build Gravitino](./how-to-build.md).

Gravitino runs on both Linux and macOS platforms, and it requires the installation of Java 8, Java 11, or Java 17. Gravitino trino-connector runs with
Trino, and requires Java 17. This should include JVMs on x86_64 and
ARM64. It's easy to run locally on one machine, all you need is to have `java` installed on
your system `PATH`, or the `JAVA_HOME` environment variable pointing to a Java installation.

See [How to install Gravitino](./how-to-install.md) to learn how to install Gravitino server.

Gravitino provides Docker images on [Docker Hub](https://hub.docker.com/u/datastrato).
Please pull the image and run it. For the details of the Gravitino Docker image, please see
[Docker image details](./docker-image-details.md).

Gravitino also provides a playground to experience the whole Gravitino system with other components.
Please see the [Gravitino playground repository](https://github.com/datastrato/gravitino-playground)
and [How to use the playground](./how-to-use-the-playground.md).

## Getting started

To get started with Gravitino, please see [Getting started](./getting-started.md) for the details.

* [Getting started locally](./getting-started.md#getting-started-locally): a quick guide to start
  and use Gravitino locally.
* [Running on Amazon Web Services](./getting-started.md#getting-started-on-amazon-web-services): a
  quick guide to start and use Gravitino on AWS.
* [Running on Google Cloud Platform](./getting-started.md#getting-started-on-google-cloud-platform):
  a quick guide to start and use Gravitino on GCP.

## How to use Gravitino

Gravitino provides two SDKs to manage metadata from different catalogs in a unified way, one is
REST API, another is Java SDK. You can use either to manage metadata, please see
[Manage metadata using Gravitino](./manage-metadata-using-gravitino.md) for the details.

Also, you can find the complete REST API definition in
[Gravitino Open API](./api/rest/gravitino-rest-api),
Java SDK definition in [Gravitino Javadoc](pathname:///docs/0.4.0/api/java/index.html).

Gravitino provides a web UI to manage the metadata, you can visit the web UI in the browser via
the URL address `http://<ip-address>:8090`. Please see [Gravitino web UI](./webui.md) for the details.

Gravitino currently supports the following catalogs:

* [**Iceberg catalog**](./lakehouse-iceberg-catalog.md)
* [**Hive catalog**](./apache-hive-catalog.md)
* [**MySQL catalog**](./jdbc-mysql-catalog.md)
* [**PostgreSQL catalog**](./jdbc-postgresql-catalog.md)

Gravitino also provides an Iceberg REST catalog service for Iceberg table format, please see the
[Iceberg REST catalog service](./iceberg-rest-service.md) for the details.

## Gravitino playground

To experience Gravitino with other components easily, Gravitino provides a playground to run. It
integrates Apache Hadoop, Apache Hive, Trino, MySQL, PostgreSQL, and Gravitino together as a
complete environment. To experience the whole features, please also see
[Getting started](./getting-started.md) and [How to use the Gravitino playground](./how-to-use-the-playground.md)
to learn how to use the playground.

* [Install Gravitino playground on AWS or GCP](./getting-started.md#installing-gravitino-playground-on-aws-or-google-cloud-platform):
  a quick guide to start and use Gravitino playground on AWS or GCP.
* [Install Gravitino playground locally](./getting-started.md#installing-gravitino-playground-locally):
  a quick guide to start and use Gravitino playground locally.
* [How to use the Gravitino playground](./how-to-use-the-playground.md): provides an example of how
  to use Gravitino and other components together.

## Where to go from here

### Catalogs

Gravitino supports different catalogs to manage the metadata in different sources. Please see:

* [Iceberg catalog](./lakehouse-iceberg-catalog.md): a complete guide to using Gravitino to
  manage Apache Iceberg data.
* [Iceberg REST catalog service](./iceberg-rest-service.md): a
  complete guide to use Gravitino as an Apache Iceberg REST catalog service.
* [Hive catalog](./apache-hive-catalog.md): a complete guide to using Gravitino to manage Apache Hive data.
* [MySQL catalog](./jdbc-mysql-catalog.md): a complete guide to using Gravitino to manage MySQL data.
* [PostgreSQL catalog](./jdbc-postgresql-catalog.md): a complete guide to using Gravitino to manage PostgreSQL data.

### Trino connector

Gravitino provides a Trino connector to connect to Gravitino to manage the metadata in a unified
way. to use the Trino connector, please see:

* [How to use Gravitino Trino connector](./trino-connector/index.md): a complete guide to use Gravitino
  Trino connector.

### Server administration

Gravitino provides several ways to configure and manage the Gravitino server. Please see:

* [Security](./security.md): provides security configurations for Gravitino, including HTTPS
  and OAuth2 configurations.
* [Gravitino metrics](./metrics.md): provides metrics configurations and detailed metrics list
  of the Gravitino server.

### Programming guides

* [Gravitino Open API](./api/rest/gravitino-rest-api): provides the complete Open API definition of
  Gravitino.
* [Gravitino Javadoc](pathname:///docs/0.4.0/api/java/index.html): provides the Javadoc for the Gravitino API.

### Development guides

* [How to build Gravitino](./how-to-build.md): a complete guide to build Gravitino from
  source.
* [How to test Gravitino](./how-to-test.md): a complete guide to running Gravitino unit tests and
  integration tests.
* [How to sign and verify Gravitino releases](./how-to-sign-releases.md): a guide to sign and verify
  a Gravitino release.
* [Publish Docker images](./publish-docker-images.md): a guide to publish Gravitino Docker images,
  also list the change logs of Gravitino CI Docker images and release images.
