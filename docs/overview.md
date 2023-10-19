---
title: "Overview"
date: 2023-10-19T15:33:00-08:00
license: "Copyright 2023 Datastrato.
This software is licensed under the Apache License version 2."
---

# Overview

## Introduction

Gravitino is a high-performance, geo-distributed, and federated metadata lake. It manages the metadata directly in different sources, types, and regions. It also provides users unified access to the metadata for both data and AI assets.

![Gravitino Architecture](assets/gravitino-architecture.png)

Gravitino aims to provide several key features:

* SSOT (Single Source of Truth) for multi-regional data with geo-distributed architecture support.
* Unified Data + AI assets managements for both users and engines.
* Security in one place, centralize the security for different sources.
* Built-in data management + data access management.

## Architecture

![Gravitino Model and Arch](assets/gravitino-model-arch.png)

* **Functionality Layer**: Gravitino provides a set of APIs for users to manage and govern the 
  metadata, including standard metadata creation, update, and delete operations. In the meantime, it also provides the ability to govern the metadata in a unified way, including access control, discovery, and others.
* **Interface Layer**: Gravitino provides standard REST APIs as the interface layer for users. It will also provide Thrift and JDBC interfaces in the future. 
* **Core Object Model**: Gravitino defines a generic metadata model to represents the metadata in different sources and types, manages them in a unified way.
* **Connection Layer**: In the connection layer, Gravitino provides a set of connectors to connect to different metadata sources, including Hive, MySQL, PostgreSQL, and others. It also provides the ability to connect and manage heterogeneous metadata other than Tabular data.

## Terminology

The model of Gravitino

![Gravitino Model](assets/metadata-model.png)

* **Metalake**: The top-level container for metadata. Typically, one group has one metalake to manage all the metadata in it. Each metalake exposes a three-level namespace(catalog.schema.table) to organize the data.
* **Catalog**: catalog is a collection of metadata from a specific metadata source. Each catalog will have a related connector to connect to the specific metadata source.
* **Schema**: Schema is equivalent to database, Schemas only exist in the specific catalogs that support relational metadata sources, such as Hive, MySQL, PostgreSQL, and others.
* **Table**: The lowest level in the object hierarchy for catalos that support relational metadata sources. Tables can be created in the specific schemas in the catalogs.
* **Model**: Model represents the metadata in the specific catalogs that support model management.