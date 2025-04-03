---
title: Lakehouse Hudi catalog
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

Apache Gravitino can be used to manage Apache Hudi metadata.

### Requirements and limitations

The catalog implementation was tested and verified with Apache Hudi *0.15.0*.

## Catalog

### Catalog capabilities

- The catalog works as a proxy, supporting HMS as the backend.
- This catalog only supports read operations (list and load) for Hudi schemas and tables.
- Timeline management operations are not supported at the moment.

### Catalog properties

<table>
<thead>
<tr>
  <th>Property name</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>catalog-backend</tt></td>
  <td>
    Catalog backend of Gravitino Hudi catalog. Only supports `hms` now.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>uri</tt></td>
  <td>
    The URI associated with the backend.
    Such as `thrift://127.0.0.1:9083` for HMS backend.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>client.pool-size</tt></td>
  <td>
    This is for the HMS backend.
    The maximum number of Hive metastore clients in the pool for Gravitino.
  </td>
  <td>1</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>client.pool-cache.eviction-interval-ms</tt></td>
  <td>
    This is for the HMS backend.
    The cache pool eviction interval in milliseconds.
  </td>
  <td>300000</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.bypass.&#42;</tt></td>
  <td>
    Property name with this prefix passed down to the underlying backend client for use.
    Such as `gravitino.bypass.hive.metastore.failure.retries = 3`
    indicate 3 times of retries upon failure of Thrift metastore calls for HMS backend.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
</tbody>
</table>

### Catalog operations

For more details, please refer to
[manage relational metadata](../../../metadata/relational.md#catalog-operations).

## Schema 

### Schema capabilities

- Only support read operations: <tt>listSchema</tt>, <tt>loadSchema</tt>,
  and <tt>schemaExists</tt>.

### Schema properties

- The <tt>location</tt> is an optional property that shows the storage path to the Hudi database.

### Schema operations

Only support read operations: <tt>listSchema</tt>, <tt>loadSchema</tt>,
and <tt>schemaExists</tt>.
For more details, please refer to [managing relational metadata](../../../metadata/relational.md#schema-operations).

## Table 

### Table capabilities

- Only support read operations: <tt>listTable</tt>, <tt>loadTable</tt>, and <tt>tableExists</tt>.

### Table partitions

- Support loading Hudi partitioned tables (Hudi only supports identity partitioning).

### Table sort orders

- Sorted tables are not supported.

### Table distributions

- Distributed tables are not supported.

### Table indexes

- Indexed tables are not supported.

### Table properties

- For HMS backend, it will bring out all the table parameters from the HMS.

### Table column types

The following table shows the mapping between Gravitino
and [Apache Hudi column types](https://hudi.apache.org/docs/sql_ddl#supported-types):

| Gravitino Type | Apache Hudi Type |
|----------------|------------------|
| `array`        | `array`          |
| `binary`       | `bytes`          |
| `boolean`      | `boolean`        |
| `date`         | `date`           |
| `decimal`      | `decimal`        |
| `double`       | `double`         |
| `float`        | `float`          |
| `integer`      | `int`            |
| `long`         | `long`           |
| `map`          | `map`            |
| `string`       | `string`         |
| `struct`       | `struct`         |
| `timestamp`    | `timestamp`      |

### Table operations

Only read operations like <tt>listTable</tt>, <tt>loadTable</tt>,
and <tt>tableExists</tt> are supported.
For more details, please refer to [managing relational metadata](../../../metadata/relational.md#table-operations)

