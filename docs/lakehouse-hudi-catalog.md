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

The Hudi catalog enables Apache Gravitino to surface Apache Hudi table metadata from a Hive Metastore Service. Use it when you want Hudi tables discoverable and governable through Gravitino alongside relational, lakehouse, and fileset catalogs. The catalog is read-only by design: it supports listing, loading, and existence checks for Hudi schemas and tables, but does not create, alter, or drop them. Write operations on Hudi tables continue to go through Hudi's own engines (Flink, Spark).

### Requirements and Limitations

- **Hudi version:** tested and verified with Apache Hudi `0.15.0`.
- **Supported metadata backend:** Hive Metastore Service (HMS) only. The `catalog-backend` property accepts only `hms` at present. JDBC and other backends are not supported.
- **Read-only catalog.** The Hudi catalog supports only read operations: `listSchema`, `loadSchema`, `schemaExists`, `listTable`, `loadTable`, and `tableExists`. Creating, altering, or dropping Hudi schemas or tables through Gravitino is not supported. Use Hudi's own engines (Flink, Spark) for write operations.
- **No timeline management.** Hudi's commit timeline (commit, rollback, savepoint, clean, compact, cluster) is not exposed through the catalog interface. Use Hudi's own engines or the Hudi CLI for timeline operations.
- **Authentication.** `simple` and `Kerberos` are supported on the HMS backend. For Kerberos, set `authentication.type` to `kerberos` and configure the principal and keytab properties, along with the related `gravitino.bypass.` Hadoop-security keys.
- **Identity partitioning only.** Hudi natively supports identity partitioning; other partition transforms are not exposed.
- **No table indexes, no sort orders, no distributions, no column default values.** Table-level properties stored in HMS surface as read-only Gravitino table properties on load.

## Quick Start

Create a minimum-viable Hudi catalog and confirm it can reach the Hive Metastore Service that catalogs your Hudi tables. The example assumes a Gravitino server at `http://localhost:8090`, a metalake named `test`, and a Hive Metastore Service at `thrift://localhost:9083`. Adjust the values for your environment.

### Create the Catalog

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "hudi_catalog",
    "type": "RELATIONAL",
    "comment": "Hudi catalog",
    "provider": "lakehouse-hudi",
    "properties": {
      "catalog-backend": "hms",
      "uri": "thrift://localhost:9083"
    }
  }' \
  http://localhost:8090/api/metalakes/test/catalogs
```

The response is a JSON object describing the created catalog.

### Verify the Catalog

```bash
# List catalogs in the metalake. hudi_catalog should appear.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs" | jq

# Load the catalog directly and inspect its properties.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/hudi_catalog" | jq

# List schemas. The call exercises the HMS Thrift connection and returns the Hive databases visible to the metastore.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/hudi_catalog/schemas" | jq
```

**Success check:** the catalog-list response includes `hudi_catalog`, the load-catalog response shows `"provider":"lakehouse-hudi"` with `catalog-backend` set to `hms` and `uri` set to the Thrift URI, and the schema-list response includes the Hive databases in the metastore (typically at least `default`). If the schema-list call returns a Thrift connection error, verify the HMS is running and reachable from the Gravitino server. For Kerberos-enabled HMS, additional authentication configuration is required; see [Catalog Backend Security](#catalog-backend-security) below.

## Catalog

### Catalog Capabilities

The Hudi catalog:

- Acts as a Gravitino front-end over a Hive Metastore Service (HMS) that catalogs Hudi tables.
- Supports only read operations on Hudi schemas and tables (`listSchema`, `loadSchema`, `schemaExists`, `listTable`, `loadTable`, `tableExists`). Write operations on Hudi tables continue to go through Hudi's own engines (Flink, Spark).
- Surfaces Hudi table parameters stored in HMS as read-only Gravitino table properties on load.
- Does not expose Hudi's commit timeline (commit, rollback, savepoint, clean, compact, cluster). Use Hudi's own engines or the Hudi CLI for timeline operations.

### Catalog Properties

| Property name                            | Description                                                                                                                                                                                                                            | Default | Required | Since    |
|------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `catalog-backend`                        | Catalog backend of Gravitino Hudi catalog. Only supports `hms` now.                                                                                                                                                                    | (none)        | Yes      | 0.7.0-incubating |
| `uri`                                    | The URI associated with the backend. Such as `thrift://127.0.0.1:9083` for HMS backend.                                                                                                                                                | (none)        | Yes      | 0.7.0-incubating |
| `client.pool-size`                       | For HMS backend. The maximum number of Hive metastore clients in the pool for Gravitino.                                                                                                                                               | 1             | No       | 0.7.0-incubating |
| `client.pool-cache.eviction-interval-ms` | For HMS backend. The cache pool eviction interval.                                                                                                                                                                                     | 300000        | No       | 0.7.0-incubating |
| `gravitino.bypass.`                      | Property name with this prefix passed down to the underlying backend client for use. Such as `gravitino.bypass.hive.metastore.failure.retries = 3` indicate 3 times of retries upon failure of Thrift metastore calls for HMS backend. | (none)        | No       | 0.7.0-incubating |
| `default.catalog`                        | The default catalog name for the Hive3 metastore backend; this configuration is ignored when using a Hive2 metastore.                                                                                                                  | hive          | No       | 1.1.0            |

#### Catalog Backend Security

Configure backend security with the following properties. For a Kerberos Hive backend, for example, set `authentication.type` to `Kerberos` and provide `authentication.kerberos.principal` and `authentication.kerberos.keytab-uri`.

| Property name                                      | Description                                                                                                                                                    | Default | Required                                                    | Since     |
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
