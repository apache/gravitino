---
title: "Getting Started"
slug: "/getting-started/index"
license: "This software is licensed under the Apache License version 2."
---

In about 15 minutes, you can install Gravitino, start the server, and create your first metalake using the REST API. Java 17 is the only prerequisite.

For a fuller demo environment that includes Apache Hive, Trino, Apache Spark, MySQL, and PostgreSQL, use the [Gravitino playground](../how-to-use-the-playground.md). For production deployment patterns including Docker and Kubernetes, see [Install Gravitino](../how-to-install.md) and the [Helm chart](../chart.md).

## Prerequisites

- Linux or macOS host.
- Java 17 (any JVM on x86_64 or ARM64) with `java` on `PATH` or `JAVA_HOME` set. Confirm with `java -version`.
- Port 8090 available on the host for the Gravitino REST API.

## Download and Start Gravitino

Download the latest Gravitino binary distribution from the [Apache Gravitino releases page](https://github.com/apache/gravitino/releases), then extract and enter the directory:

```shell
tar -xzf gravitino-<version>-bin.tar.gz
cd gravitino-<version>-bin
```

Start the server with the default configuration:

```shell
./bin/gravitino.sh start
```

Gravitino runs in the background and listens on port 8090.

## Verify the Server Is Running

Check the version endpoint:

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/version
```

A successful response returns the Gravitino version, build date, and git commit. If you see a connection refused error, check the server logs in `logs/gravitino-server.log`.

Open the Gravitino Web UI at `http://localhost:8090` for a graphical view of the server. The web UI is empty at first since no metalakes exist yet.

## Create Your First Metalake

A metalake is the top-level container for metadata in Gravitino. Each catalog you add later lives inside a metalake. Create one through the REST API:

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{"name":"demo_metalake","comment":"Demo metalake for quickstart"}' \
  http://localhost:8090/api/metalakes
```

List all metalakes to confirm the result:

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes
```

The response includes `demo_metalake` alongside its audit fields. Refresh the Web UI to see the same metalake there.

## Add a Catalog

A metalake holds one or more catalogs, each connecting Gravitino to a specific metadata source. The catalog setup depends on which source you are federating. See the catalog documentation for setup details:

- [Apache Iceberg catalog](../lakehouse-iceberg-catalog.md) for Iceberg tables in object storage.
- [Apache Hive catalog](../apache-hive-catalog.md) for Hive tables managed through an existing Hive Metastore.
- [JDBC catalogs](../jdbc-mysql-catalog.md) for relational databases such as MySQL, PostgreSQL, and others.
- [Fileset catalog](../fileset-catalog.md) for raw file collections in HDFS, S3, GCS, OSS, or ADLS.
- [Apache Kafka catalog](../kafka-catalog.md) for Kafka topic metadata.
- [Model catalog](../model-catalog.md) for ML model metadata.

## Where to Go Next

- Read the [Overview](../overview.md) for architecture and the broader capability set.
- Use the [Gravitino playground](../how-to-use-the-playground.md) for end-to-end demos including federated queries across catalogs.
- Configure [authentication](../security/how-to-authenticate.md) and [access control](../security/access-control.md) before exposing Gravitino beyond a local trial.
- Set up the [Iceberg REST catalog service](../iceberg-rest-service.md) to let any IRC-compatible engine query through Gravitino.
- Manage metadata programmatically with the [Java client](../how-to-use-gravitino-client.md) or [Python client](../how-to-use-python-client.md).

<img src="https://analytics.apache.org/matomo.php?idsite=62&rec=1&bots=1&action_name=GettingStarted" alt="" />

