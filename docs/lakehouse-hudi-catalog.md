---
title: "Hudi catalog"
slug: /lakehouse-hudi-catalog
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

### Requirements and limitations

:::info
Tested and verified with Apache Hudi `0.15.0`.
:::

## Catalog

### Catalog capabilities

- Works as a catalog proxy, supporting `HMS` as catalog backend.
- Only support read operations (list and load) for Hudi schemas and tables.
- Doesn't support timeline management operations now.

### Catalog properties

| Property name                            | Description                                                                                                                                                                                                                            | Default value | Required | Since Version    |
|------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `catalog-backend`                        | Catalog backend of Gravitino Hudi catalog. Only supports `hms` now.                                                                                                                                                                    | (none)        | Yes      | 0.7.0-incubating |
| `uri`                                    | The URI associated with the backend. Such as `thrift://127.0.0.1:9083` for HMS backend.                                                                                                                                                | (none)        | Yes      | 0.7.0-incubating |
| `client.pool-size`                       | For HMS backend. The maximum number of Hive metastore clients in the pool for Gravitino.                                                                                                                                               | 1             | No       | 0.7.0-incubating |
| `client.pool-cache.eviction-interval-ms` | For HMS backend. The cache pool eviction interval.                                                                                                                                                                                     | 300000        | No       | 0.7.0-incubating |
| `gravitino.bypass.`                      | Property name with this prefix passed down to the underlying backend client for use. Such as `gravitino.bypass.hive.metastore.failure.retries = 3` indicate 3 times of retries upon failure of Thrift metastore calls for HMS backend. | (none)        | No       | 0.7.0-incubating |

### Catalog operations

Please refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema 

### Schema capabilities

- Only support read operations: listSchema, loadSchema, and schemaExists.

### Schema properties

- The `Location` is an optional property that shows the storage path to the Hudi database

### Schema operations

Only support read operations: listSchema, loadSchema, and schemaExists.
Please refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more details.

## Table 

### Table capabilities

- Only support read operations: listTable, loadTable, and tableExists.

### Table partitions

- Support loading Hudi partitioned tables (Hudi only supports identity partitioning).

### Table sort orders

- Doesn't support table sort orders.

### Table distributions

- Doesn't support table distributions.

### Table indexes

- Doesn't support table indexes.

### Table properties

- For HMS backend, it will bring out all the table parameters from the HMS.

### Table column types

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

### Table operations

Only support read operations: listTable, loadTable, and tableExists.
Please refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for more details.
