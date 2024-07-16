---
title: "Paimon catalog"
slug: /lakehouse-Paimon-catalog
keywords:
  - lakehouse
  - Paimon
  - metadata
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

Apache Gravitino provides the ability to manage Apache Paimon metadata.

### Requirements

:::info
Builds with Apache Paimon `0.8.0`.
:::

## Catalog

### Catalog capabilities

- Works as a catalog proxy, supporting `FilesystemCatalog`.
- Supports DDL operations for Paimon schemas and tables.

- Doesn't support `JdbcCatalog` and `HiveCatalog` backend-catalog now.
- Doesn't support alterTable now.
- Doesn't support alterSchema.

### Catalog properties

| Property name                                      | Description                                                                                                                                                                                                 | Default value          | Required                                                        | Since Version |
|----------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------|-----------------------------------------------------------------|---------------|
| `catalog-backend`                                  | Catalog backend of Gravitino Paimon catalog. Only supports `filesystem` now.                                                                                                                                | (none)                 | Yes                                                             | 0.6.0         |
| `uri`                                              | The URI configuration of the Paimon catalog. `thrift://127.0.0.1:9083` or `jdbc:postgresql://127.0.0.1:5432/db_name` or `jdbc:mysql://127.0.0.1:3306/metastore_db`. It is optional for `FilesystemCatalog`. | (none)                 | required if the value of `catalog-backend` is not `filesystem`. | 0.6.0         |
| `warehouse`                                        | Warehouse directory of catalog. `file:///user/hive/warehouse-paimon/` for local fs or `hdfs://namespace/hdfs/path` for HDFS.                                                                                | (none)                 | Yes                                                             | 0.6.0         |
| `authentication.type`                              | The type of authentication for Paimon catalog backend, currently Gravitino only supports `Kerberos` and `simple`.                                                                                           | `simple`               | No                                                              | 0.6.0         |
| `authentication.kerberos.principal`                | The principal of the Kerberos authentication                                                                                                                                                                | (none)                 | required if the value of `authentication.type` is Kerberos.     | 0.6.0         |
| `authentication.kerberos.keytab-uri`               | The URI of The keytab for the Kerberos authentication.                                                                                                                                                      | (none)                 | required if the value of `authentication.type` is Kerberos.     | 0.6.0         |
| `authentication.kerberos.check-interval-sec`       | The check interval of Kerberos credential for Paimon catalog.                                                                                                                                               | 60                     | No                                                              | 0.6.0         |
| `authentication.kerberos.keytab-fetch-timeout-sec` | The fetch timeout of retrieving Kerberos keytab from `authentication.kerberos.keytab-uri`.                                                                                                                  | 60                     | No                                                              | 0.6.0         |


Any properties not defined by Gravitino with `gravitino.bypass.` prefix will pass to Paimon catalog properties and HDFS configuration. For example, if specify `gravitino.bypass.table.type`, `table.type` will pass to Paimon catalog properties.

### Catalog operations

Please refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema 

### Schema capabilities

- Supporting createSchema, dropSchema, loadSchema and listSchema.
- Supporting cascade drop schema.

- Doesn't support alterSchema.

### Schema properties

- Doesn't support specify location and store any schema properties when createSchema for FilesystemCatalog now.
- Doesn't return any schema properties when loadSchema for FilesystemCatalog now.
- Doesn't support store schema comment for FilesystemCatalog now.

### Schema operations

Please refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more details.

## Table 

### Table capabilities

- Supporting createTable, dropTable, loadTable and listTable.
:::info
dropTable will delete the table location directly, similar with purgeTable.
:::
- Supporting Column default value through table properties, such as 

- Doesn't support alterTable now.

#### Table partitions

- Doesn't support table partition now.

### Table sort orders

- Doesn't support table sort orders.

### Table distributions

- Only supporting `NoneDistribution` now.

### Table indexes

- Doesn't support table indexes.

### Table column types

| Gravitino Type                | Apache Paimon Type             |
|-------------------------------|--------------------------------|
| `Sturct`                      | `Row`                          |
| `Map`                         | `Map`                          |
| `Array`                       | `Array`                        |
| `Boolean`                     | `Boolean`                      |
| `Byte`                        | `TinyInt`                      |
| `Short`                       | `SmallInt`                     |
| `Integer`                     | `Int`                          |
| `Long`                        | `BigInt`                       |
| `Float`                       | `Float`                        |
| `Double`                      | `Double`                       |
| `String`                      | `VarChar(Integer.MAX_VALUE)`   |
| `VarChar`                     | `VarChar(length)`              |
| `Date`                        | `Date`                         |
| `Time`                        | `Time`                         |
| `TimestampType withZone`      | `LocalZonedTimestamp`          |
| `TimestampType withoutZone`   | `Timestamp`                    |
| `Decimal`                     | `Decimal`                      |
| `Binary`                      | `Binary`                       |

:::info
Gravitino Paimon table column types doesn't support Paimon `Char` and `VarBinary` type now.
Gravitino types doesn't support Paimon `MultisetType` type.
:::

### Table properties

You can pass [Paimon table properties](https://paimon.apache.org/docs/0.8/maintenance/configurations/) to Gravitino when creating an Paimon table.

The Gravitino server doesn't allow passing the following reserved fields.

| Configuration item              | Description                                              |
|---------------------------------|----------------------------------------------------------|
| `comment`                       | The table comment.                                       |
| `creator`                       | The table creator.                                       |

### Table operations

Please refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for more details.

## HDFS configuration

You can place `core-site.xml` and `hdfs-site.xml` in the `catalogs/lakehouse-paimon/conf` directory to automatically load as the default HDFS configuration.

:::caution
When writing to HDFS, the Gravitino server can only operate as the specified Kerberos user and doesn't support proxying to other Kerberos users now.
:::
