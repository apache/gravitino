---
title: Gravitino overview
slug: /
license: Copyright 2023 Datastrato Pvt Ltd. This software is licensed under the Apache License version 2.
---

## What is Gravitino?

Gravitino is a high-performance, geo-distributed, and federated metadata lake. It manages the
metadata directly in different sources, types, and regions. It also provides users with unified
metadata access for data and AI assets.

[Learn more](./overview)&rarr;

## Downloading

You can get Graviton from the[GitHub release page](https://github.com/datastrato/gravitino/releases),
or you can build Gravitino from source, please see [How to build Gravitino](./how-to-build).

Gravitino runs on both Linux and macOS, and requires Java 8. This should include JVMs on x86_64 and
ARM64. It's easy to run locally on one machine --- all you need is to have `java` installed on
your system `PATH`, or the `JAVA_HOME` environment variable pointing to a Java installation.

See [How to install Gravitino](./how-to-install) to learn how to install Gravitino server.

Gravitino provides Docker image on [Docker Hub](https://hub.docker.com/u/datastrato).
Please pull the image and run it. For the details of Gravitino Docker image, please see
[Dock image details](./docker-image-details).

Gravitino also provides a playground to experience the whole Gravitino system with other components.
Please see the [Gravitino playground repository](https://github.com/datastrato/gravitino-playground)
and [How to use the playground](./how-to-use-the-playground).

## Getting started

To get started with Gravitino, please see [Getting started](./getting-started) for the details.

* [Getting started locally](./getting-started#getting-started-locally): a quick guide to start
  and use Gravitino locally.
* [Running on Amazon Web Services](./getting-started#getting-started-on-amazon-web-services): a
  quick guide to start and use Gravitino on AWS.
* [Running on Google Cloud Platform](./getting-started#getting-started-on-google-cloud-platform):
  a quick guide to start and use Gravitino on GCP.

## Gravitino playground

To experience Gravitino with other components simply, Gravitino provides a playground to run. It
integrates Apache Hadoop, Apache Hive, Trino, Mysql, PostgreSQL, and Gravitino together as a
complete environment. To experience the whole features, please also see
[Getting started](./getting-started) and [How to use the Gravitino playground](./how-to-use-the-playground)
to learn how to use the playground.

* [Install Gravitino playground on AWS or GCP](./getting-started#installing-gravitino-playground-on-aws-or-google-cloud-platform):
  a quick guide to start and use Gravitino playground on AWS or GCP.
* [Install Gravitino playground locally](./getting-started#installing-gravitino-playground-locally):
  a quick guide to start and use Gravitino playground locally.
* [How to use the Gravitino playground](./how-to-use-the-playground): provides an example of how
  to use Gravitino and other components together.

## Where to go from here

### Programming guides

* [Manage metadata using Gravitino](./manage-metadata-using-gravitino): provides the complete
  functionalities of Gravitino metadata management. Including metalake, catalog, schema and
  table management.
* [Gravitino Open API](/apis/java/index.html): provides the complete Open API definition of 
  Gravitino.
* [Gravitino Javadoc](./apis/rest/gravitino-rest-api): provides the Javadoc for Gravitino API.

### Server administration

Gravitino provides several ways to configure and manage the Gravitino server. Please see:

* [How to customize Gravitino server configurations](./gravitino-server-config): provides the
  complete Gravitino server configurations.
* [Security](./security): provides the security configurations for Gravitino, including HTTPS
  and OAuth2 configurations.
* [Gravitino metrics](./metrics): provides the metrics configurations and detailed metrics list
  of Gravitino server.

### Catalog details

Gravitino support different catalogs to manage the metadata in different sources. Please see:

* [How to use Gravitino manage Apache Iceberg metadata](./lakehouse-iceberg-catalog): a complete
  guide to use Gravitino manage Apache Iceberg data.
* [How to set up Gravitino Apache Iceberg REST catalog service](./iceberg-rest-service.md): a
  complete guide to use Gravitino as Apache Iceberg REST catalog service.
* [How to use Gravitino manage Apache Hive metadata](./apache-hive-catalog): a complete guide to
  use Gravitino manage Apache Hive data.
* [How to use Gravitino manage MySQL metadata](./jdbc-mysql-catalog): a complete guide to use
  Gravitino manage MySQL data.
* [How to use Gravitino manage PostgreSQL metadata](./jdbc-postgresql-catalog): a complete guide
  to use Gravitino manage PostgreSQL data.

### Trino connector

Gravitino provides a Trino connector to connect to Gravitino to manage the metadata in a unified
way. to use the Trino connector, please see:

* [How to use Gravitino Trino connector](./trino-connector/index): a complete guide to use Gravitino
  Trino connector.

### Development guides

* [How to build Gravitino](./how-to-build): a complete guide to build Gravitino from
  source.
* [How to test Gravitino](./how-to-test): a complete guide to run Gravitino unit tests and
  integration tests.
* [How to sign and verify a Gravitino releases](./how-to-sign-releases): a guide to sign and verify
  a Gravitino release.
* [Publish Docker images](./publish-docker-images): a guide to publish Gravitino Docker images,
  also list the change logs of Gravitino CI Docker images and release images.
