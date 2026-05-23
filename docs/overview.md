---
title: "Overview"
slug: "/overview"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Apache Gravitino is a high-performance, geo-distributed, federated metadata lake. It manages metadata directly across different sources, types, and regions, and provides unified access for data and AI assets.

![Gravitino Architecture](assets/gravitino-architecture.png)

Gravitino aims to provide:

* A single source of truth (SSOT) for multi-region data, with geo-distributed architecture support.
* Unified management of data and AI assets for both users and engines.
* Centralized security across different sources.
* Built-in data management and data-access management.

## Architecture

![Gravitino Model and Arch](assets/gravitino-model-arch.png)

* **Functionality layer.** Gravitino exposes an API for managing and governing metadata. It covers the standard create, update, and delete operations as well as unified governance features such as access control and discovery.
* **Interface layer.** Gravitino provides a standard REST API as the interface layer. Thrift and JDBC interfaces are planned.
* **Core object model.** Gravitino defines a generic metadata model that represents the metadata in different sources and types and manages them in a unified way.
* **Connection layer.** Gravitino provides a set of connectors that bind to different metadata sources, including Apache Hive, MySQL, PostgreSQL, and others. It also supports connecting to and managing heterogeneous metadata beyond tabular data.

## Features

### Unified Metadata Management and Governance

Gravitino abstracts unified metadata models and APIs across different kinds of metadata sources. For example, it provides relational metadata models for tabular data (Hive, MySQL, PostgreSQL) and a file metadata model for unstructured data (HDFS, S3, and others).

On top of these unified models, Gravitino provides a unified governance layer covering access control, auditing, and discovery.

### Direct Metadata Management

Unlike traditional metadata-management systems that actively or passively collect metadata from underlying systems, Gravitino manages those systems directly through a set of connectors. Changes made in Gravitino are reflected in the underlying systems, and vice versa.

### Geo-Distribution Support

Gravitino supports geo-distributed deployment of Iceberg REST (IRC) catalogs. Different instances of Gravitino can run in different regions or clouds, with a local IRC catalog proxying requests to a remote IRC catalog so users get a global view of metadata across regions or clouds.

### Multi-Engine Support

Gravitino supports several query engines for accessing metadata. With [Trino](https://trino.io/), users can query metadata and data without changing their existing SQL dialects. Support also extends to [Apache Spark](https://spark.apache.org/), [Apache Flink](https://flink.apache.org/), and [Daft](https://docs.daft.ai/), with more engines on the roadmap.

### AI Asset Management

Gravitino aims to unify management across both data and AI assets, including raw files and models.

## Terminology

### The Metadata Object

* **Metalake.** The container or tenant for metadata. Typically, one group has one metalake that holds all of its metadata. Each metalake exposes a three-level namespace (`catalog.schema.table`) for organizing data.
* **Catalog.** A collection of metadata from a specific source. Each catalog has a connector that binds it to its source.
* **Schema.** The second-level namespace, grouping a collection of metadata. A schema can map to a database or schema in a relational source (Apache Hive, MySQL, PostgreSQL, and others), or to a logical namespace in fileset and model catalogs.
* **Table.** The lowest level in the object hierarchy for catalogs that support relational metadata. Tables are created inside schemas within a catalog.
* **Fileset.** A collection of files and directories in a file system. The fileset object holds the logical metadata for those files.
* **Model.** Represents metadata in catalogs that support model management.
* **Topic.** Represents metadata in catalogs that manage topics for a message-queue system, such as Kafka.
