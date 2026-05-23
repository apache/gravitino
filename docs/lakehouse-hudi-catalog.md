---
title: "Hudi Catalog"
slug: "/lakehouse-hudi-catalog"
keywords:
  - lakehouse
  - hudi
  - metadata
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

Apache Gravitino provides the ability to manage Apache Hudi metadata.

### Requirements and Limitations

:::info
Tested and verified with Apache Hudi `0.15.0`.
:::

## Catalog

### Catalog Capabilities

The Hudi catalog:

- Acts as a catalog proxy backed by `HMS`.
- Supports only read operations (list and load) on Hudi schemas and tables.
- Does not support timeline management operations.

### Catalog Properties

| Property name                            | Description                                                                                                                                                                                                                            | Default value | Required | Since Version    |
|------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `catalog-backend`                        | Catalog backend of Gravitino Hudi catalog. Only supports `hms` now.                                                                                                                                                                    | (none)        | Yes      | 0.7.0-incubating |
| `uri`                                    | The URI associated with the backend. Such as `thrift://127.0.0.1:9083` for HMS backend.                                                                                                                                                | (none)        | Yes      | 0.7.0-incubating |
| `client.pool-size`                       | For HMS backend. The maximum number of Hive metastore clients in the pool for Gravitino.                                                                                                                                               | 1             | No       | 0.7.0-incubating |
| `client.pool-cache.eviction-interval-ms` | For HMS backend. The cache pool eviction interval.                                                                                                                                                                                     | 300000        | No       | 0.7.0-incubating |
| `gravitino.bypass.`                      | Property name with this prefix passed down to the underlying backend client for use. Such as `gravitino.bypass.hive.metastore.failure.retries = 3` indicate 3 times of retries upon failure of Thrift metastore calls for HMS backend. | (none)        | No       | 0.7.0-incubating |
| `default.catalog`                        | The default catalog name for the Hive3 metastore backend; this configuration is ignored when using a Hive2 metastore.                                                                                                                  | hive          | No       | 1.1.0            |

#### Catalog Backend Security

Configure backend security with the following properties. For a Kerberos Hive backend, for example, set `authentication.type` to `Kerberos` and provide `authentication.kerberos.principal` and `authentication.kerberos.keytab-uri`.

| Property name                                      | Description                                                                                                                                                    | Default value | Required                                                    | Since Version     |
|----------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|-------------------------------------------------------------|-------------------|
| `authentication.type`                              | Authentication type for the Hudi catalog backend. The HMS backend supports `kerberos` and `simple`.                                                            | `simple`      | No                                                          | 1.0.0 |
| `authentication.impersonation-enable`              | Whether to enable impersonation for the hudi catalog                                                                                                           | `false`       | No                                                          | 1.0.0 |
| `authentication.kerberos.principal`                | The principal of the Kerberos authentication                                                                                                                   | (none)        | required if the value of `authentication.type` is kerberos. | 1.0.0 |
| `authentication.kerberos.keytab-uri`               | The URI of The keytab for the Kerberos authentication.                                                                                                         | (none)        | required if the value of `authentication.type` is kerberos. | 1.0.0 |
| `authentication.kerberos.check-interval-sec`       | The check interval of Kerberos credential for hudi catalog.                                                                                                    | 60            | No                                                          | 1.0.0 |
| `authentication.kerberos.keytab-fetch-timeout-sec` | The fetch timeout of retrieving Kerberos keytab from `authentication.kerberos.keytab-uri`.                                                                     | 60            | No                                                          | 1.0.0 |

Properties with the `gravitino.bypass.` prefix are passed through to the underlying backend client. For example: `gravitino.bypass.hive.metastore.kerberos.principal=XXXX`, `gravitino.bypass.hadoop.security.authentication=kerberos`, `gravitino.bypass.hive.metastore.sasl.enabled=true`, and so on.


### Catalog Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema Capabilities

The Hudi catalog supports only read operations on schemas: `listSchema`, `loadSchema`, and `schemaExists`.

### Schema Properties

The optional `Location` property records the storage path of the Hudi database.

### Schema Operations

The Hudi catalog supports only read operations: `listSchema`, `loadSchema`, and `schemaExists`. Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more details.

## Table

### Table Capabilities

The Hudi catalog supports only read operations on tables: `listTable`, `loadTable`, and `tableExists`.

### Table Partitions

The Hudi catalog can load partitioned tables. Hudi supports only identity partitioning.

### Table Sort Orders

The Hudi catalog does not support table sort orders.

### Table Distributions

The Hudi catalog does not support table distributions.

### Table Indexes

The Hudi catalog does not support table indexes.

### Table Properties

For the HMS backend, the Hudi catalog surfaces all table parameters from the HMS.

### Table Column Types

The following table shows the mapping between Gravitino and [Apache Hudi column types](https://hudi.apache.org/docs/sql_ddl#supported-types):

| Gravitino Type | Apache Hudi Type |
|----------------|------------------|
| `boolean`      | `boolean`        |
| `integer`      | `int`            |
| `long`         | `long`           |
| `date`         | `date`           |
| `timestamp`    | `timestamp`      |
| `float`        | `float`          |
| `double`       | `double`         |
| `string`       | `string`         |
| `decimal`      | `decimal`        |
| `binary`       | `bytes`          |
| `array`        | `array`          |
| `map`          | `map`            |
| `struct`       | `struct`         |

### Table Operations

The Hudi catalog supports only read operations: `listTable`, `loadTable`, and `tableExists`. Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for more details.
