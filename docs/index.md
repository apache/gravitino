---
title: "Apache Gravitino"
slug: "/"
license: "This software is licensed under the Apache License version 2."
---

Apache Gravitino is a high-performance, geo-distributed, federated metadata catalog. A single Gravitino deployment manages metadata across heterogeneous sources, including Apache Iceberg tables in object storage, Apache Hive tables, relational databases, filesets, Apache Kafka topics, and ML models, with consistent access control and governance across all of them.

## Where to Start

* **Understand Gravitino.** Read the [Overview](./overview.md) for the architecture, the metadata model, and the headline capabilities including federated catalog management and the Iceberg REST catalog service.
* **Try it locally.** The [Gravitino playground](./how-to-use-the-playground.md) is a complete Docker environment with Gravitino, Apache Hive, Trino, Apache Spark, MySQL, PostgreSQL, and Jupyter, ready to run end-to-end demos.
* **Install Gravitino.** [Install the Gravitino server](./how-to-install.md) from a binary release, Docker image, or [Helm chart](./chart.md). The [Iceberg REST catalog server](./iceberg-rest-catalog-chart.md) and the [Lance REST server](./lance-rest-server-chart.md) are also packaged as Helm charts.

## Download

Download Gravitino from the [Apache Gravitino downloads page](https://gravitino.apache.org/downloads), pull container images from [Docker Hub](https://hub.docker.com/u/apache), or build from source by following [How to build Gravitino](./how-to-build.md).

Gravitino runs on Linux and macOS and requires Java 17 on x86_64 or ARM64. Make sure `java` is on the system `PATH` or `JAVA_HOME` points at a Java 17 installation.

<img src="https://analytics.apache.org/matomo.php?idsite=62&rec=1&bots=1&action_name=Overview" alt="" />
