---
title: "Paimon Catalog"
slug: "/lakehouse-paimon-catalog"
keywords:
  - lakehouse
  - Paimon
  - metadata
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The Paimon catalog enables Apache Gravitino to manage Apache Paimon metadata as a federated proxy over a Paimon catalog backend (`filesystem`, `jdbc`, `hive`, or a Paimon REST catalog, including Aliyun DLF). Use it when you want a single Gravitino-managed access surface that covers Paimon tables alongside relational, lakehouse, and fileset catalogs, with Paimon data accessible through downstream engines such as Flink, Spark, and Trino.

### Requirements and Limitations

- **Paimon version:** built with Apache Paimon `1.2`.
- **Supported catalog backends:** `filesystem`, `jdbc`, `hive`, and `rest`. Select with the `catalog-backend` property. The REST backend supports Paimon's REST catalog protocol, including Aliyun DLF.
- **Supported storage providers:** local filesystem, HDFS, Amazon S3, and Aliyun OSS. The `warehouse` URI scheme selects the storage.
- **JDBC driver required for the `jdbc` backend.** Place the JDBC driver (`com.mysql.cj.jdbc.Driver`, `com.mysql.jdbc.Driver`, or `org.postgresql.Driver`) in `catalogs/lakehouse-paimon/libs` on the Gravitino server.
- **Cloud storage JARs required for S3 or OSS warehouses.** Place the corresponding Paimon filesystem JARs in `catalogs/lakehouse-paimon/lib` on the Gravitino server. See the [Paimon filesystems documentation](https://paimon.apache.org/docs/1.2/maintenance/filesystems/#s3) for details.
- **Authentication.** `simple` and `Kerberos` are supported for the `filesystem` and `jdbc` backends. The `hive` backend does not currently support Kerberos. The `rest` backend uses either a bearer token (`token-provider: bear`, with the `token` property) or Aliyun DLF credentials (`token-provider: dlf`, with the `dlf-access-key-id` and `dlf-access-key-secret` properties).
- **No `dropTable` operation.** Paimon's native `dropTable` removes both metadata and the table location from the filesystem and bypasses the trash. Use `purgeTable` instead.
- **No `alterSchema`.** Schemas can be created, dropped (including cascade), loaded, and listed but cannot be altered.
- **No auto-increment columns and no column-expression defaults.** Column literal defaults are supported through table properties such as `fields.{columnName}.default-value`.
- **No table sort orders.** Table distributions are accepted only as a way to configure Paimon's bucketing (HASH strategy); arbitrary distribution semantics are not supported. The `bucket` and `bucket-key` table properties are reserved and derived from the distribution.
- **Partition fields must not appear in the primary key.** Including a partition field in the primary key results in only one record per partition.

## Quick Start

Create a minimum-viable Paimon catalog and confirm it is reachable. The example uses the `filesystem` backend with a local warehouse path so the walkthrough runs against a default Gravitino installation with no external metastore or cloud storage. For JDBC, Hive, or REST backends (including Aliyun DLF), see the [Catalog Properties](#catalog-properties) section below. The walkthrough assumes a Gravitino server at `http://localhost:8090` and a metalake named `test`.

### Create the Catalog

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "paimon_catalog",
    "type": "RELATIONAL",
    "comment": "Paimon catalog",
    "provider": "lakehouse-paimon",
    "properties": {
      "catalog-backend": "filesystem",
      "warehouse": "file:///tmp/paimon-warehouse"
    }
  }' \
  http://localhost:8090/api/metalakes/test/catalogs
```

The response is a JSON object describing the created catalog.

### Verify the Catalog

```bash
# List catalogs in the metalake. paimon_catalog should appear.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs" | jq

# Load the catalog directly and inspect its properties.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/paimon_catalog" | jq

# List schemas. The response is an empty array on a freshly created filesystem catalog until a schema is added.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/paimon_catalog/schemas" | jq
```

**Success check:** the catalog-list response includes `paimon_catalog`, the load-catalog response shows `"provider":"lakehouse-paimon"` with `catalog-backend` set to `filesystem` and `warehouse` set to the local path, and the schema-list response is a JSON array (an empty array on a fresh catalog is expected). If load-catalog returns an error, confirm that the Gravitino server process has write access to `/tmp/paimon-warehouse`. For non-filesystem backends, ensure the corresponding JDBC driver or cloud storage JAR is present in the Gravitino server's `catalogs/lakehouse-paimon` directory.

## Catalog

### Catalog Capabilities

The Paimon catalog:

- Acts as a catalog proxy backed by `FilesystemCatalog`, `JdbcCatalog`, `HiveCatalog`, or a Paimon REST catalog (including Aliyun DLF).
- Supports DDL operations on Paimon schemas and tables, with the exception of `alterSchema`.
- Supports Paimon views when the underlying backend exposes the view API; see [View Capabilities](#view-capabilities).
- Caches Paimon catalog backends and forwards arbitrary Paimon catalog properties through `gravitino.bypass.` prefixed configuration.
- Manages metadata only; data plane reads and writes continue to go through Paimon's own engines (Flink, Spark, Trino).

### Catalog Properties

| Property name                                      | Description                                                                                                                                                                                                 | Default                                                                  | Required                                                                                                                                                             | Since    |
|----------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|
| `catalog-backend`                                  | Catalog backend of Gravitino Paimon catalog. Supports `filesystem`, `jdbc`, `hive` and `rest`.                                                                                                              | (none)                                                                         | Yes                                                                                                                                                                  | 0.6.0-incubating |
| `uri`                                              | The URI configuration of the Paimon catalog. `thrift://127.0.0.1:9083` or `jdbc:postgresql://127.0.0.1:5432/db_name` or `jdbc:mysql://127.0.0.1:3306/metastore_db`. It is optional for `FilesystemCatalog`. | (none)                                                                         | required if the value of `catalog-backend` is not `filesystem`.                                                                                                      | 0.6.0-incubating |
| `warehouse`                                        | Warehouse directory of catalog. `file:///user/hive/warehouse-paimon/` for local fs, `hdfs://namespace/hdfs/path` for HDFS , `s3://{bucket-name}/path/` for S3 or `oss://{bucket-name}/path` for Aliyun OSS  | (none)                                                                         | Yes                                                                                                                                                                  | 0.6.0-incubating |
| `catalog-backend-name`                             | The catalog name passed to underlying Paimon catalog backend.                                                                                                                                               | The property value of `catalog-backend`, like `jdbc` for JDBC catalog backend. | No                                                                                                                                                                   | 0.8.0-incubating |
| `authentication.type`                              | Authentication type for the Paimon catalog backend. Supported values are `Kerberos` and `simple`.                                                                                                           | `simple`                                                                       | No                                                                                                                                                                   | 0.6.0-incubating |
| `hive.metastore.sasl.enabled`                      | Whether to enable SASL when connecting to a Kerberos Hive metastore. This is a raw Hive configuration.                                                                                                      | `false`                                                                        | Should be `true` for most Kerberos setups (SSL is the rarer alternative) when `authentication.type` is `Kerberos`.                                                   | 0.6.0-incubating |
| `authentication.kerberos.principal`                | The principal of the Kerberos authentication.                                                                                                                                                               | (none)                                                                         | required if the value of `authentication.type` is Kerberos.                                                                                                          | 0.6.0-incubating |
| `authentication.kerberos.keytab-uri`               | The URI of The keytab for the Kerberos authentication.                                                                                                                                                      | (none)                                                                         | required if the value of `authentication.type` is Kerberos.                                                                                                          | 0.6.0-incubating |
| `authentication.kerberos.check-interval-sec`       | The check interval of Kerberos credential for Paimon catalog.                                                                                                                                               | 60                                                                             | No                                                                                                                                                                   | 0.6.0-incubating |
| `authentication.kerberos.keytab-fetch-timeout-sec` | The fetch timeout of retrieving Kerberos keytab from `authentication.kerberos.keytab-uri`.                                                                                                                  | 60                                                                             | No                                                                                                                                                                   | 0.6.0-incubating |
| `oss-endpoint`                                     | The endpoint of the Aliyun OSS.                                                                                                                                                                             | (none)                                                                         | required if the value of `warehouse` is a OSS path                                                                                                                   | 0.7.0-incubating |
| `oss-access-key-id`                                | The access key of the Aliyun OSS.                                                                                                                                                                           | (none)                                                                         | required if the value of `warehouse` is a OSS path                                                                                                                   | 0.7.0-incubating |
| `oss-secret-access-key`                            | The secret key the Aliyun OSS.                                                                                                                                                                              | (none)                                                                         | required if the value of `warehouse` is a OSS path                                                                                                                   | 0.7.0-incubating |
| `s3-endpoint`                                      | The endpoint of the AWS S3.                                                                                                                                                                                 | (none)                                                                         | required if the value of `warehouse` is a S3 path                                                                                                                    | 0.7.0-incubating |
| `s3-access-key-id`                                 | The access key of the AWS S3.                                                                                                                                                                               | (none)                                                                         | required if the value of `warehouse` is a S3 path                                                                                                                    | 0.7.0-incubating |
| `s3-secret-access-key`                             | The secret key of the AWS S3.                                                                                                                                                                               | (none)                                                                         | required if the value of `warehouse` is a S3 path                                                                                                                    | 0.7.0-incubating |
| `token-provider`                                   | The token provider type for Paimon catalog backend.                                                                                                                                                         | Token provider could be `bear` or `dlf`.                                       | required if the value of `catalog-backend` is `rest`.                                                                                                                | 1.2.0            |
| `token`                                            | The bear token for Paimon REST catalog authentication.                                                                                                                                                      | (none)                                                                         | required if the value of `token-provider` is `bear`.                                                                                                                 | 1.2.0            |
| `dlf-access-key-id`                                | The access key ID for Aliyun DLF (Data Lake Formation).                                                                                                                                                     | (none)                                                                         | required if the value of `catalog-backend` is `rest` and accessing Aliyun DLF Paimon REST server.                                                                    | 1.2.0            |
| `dlf-access-key-secret`                            | The access key secret for Aliyun DLF.                                                                                                                                                                       | (none)                                                                         | required if the value of `catalog-backend` is `rest` and accessing Aliyun DLF Paimon REST server.                                                                    | 1.2.0            |
| `dlf-security-token`                               | The security token for Aliyun DLF.                                                                                                                                                                          | (none)                                                                         | No                                                                                                                                                                   | 1.2.0            |
| `dlf-token-path`                                   | The token path for Aliyun DLF.                                                                                                                                                                              | (none)                                                                         | No                                                                                                                                                                   | 1.2.0            |
| `dlf-token-loader`                                 | The token loader for Aliyun DLF.                                                                                                                                                                            | (none)                                                                         | No                                                                                                                                                                   | 1.2.0            |

:::note
- If you want to use the `oss` or `s3` warehouse, you need to place related jars in the `catalogs/lakehouse-paimon/lib` directory, more information can be found in the [Paimon S3](https://paimon.apache.org/docs/1.2/maintenance/filesystems/#s3).
- If you want to use REST backend, Gravitino Paimon catalog supports Aliyun DLF (Data Lake Formation) as the REST catalog service. You need to configure the DLF-related properties eg:
```
{
  "name": "dlf_paimon",
  "type": "RELATIONAL",
  "provider": "lakehouse-paimon",
  "properties": {
    "catalog-backend": "rest",
    "uri": "<catalog server url>",
    "warehouse": "gravitino",
    "token-provider": "dlf",
    "dlf-access-key-id": "<access-key-id>",
    "dlf-access-key-secret": "<access-key-secret>"
  }
}
```
connect to Aliyun DLF, more information can be found in the [Paimon REST Catalog](https://paimon.apache.org/docs/master/concepts/rest/overview/).
- The hive backend does not support the kerberos authentication now.
:::

Any properties not defined by Gravitino with `gravitino.bypass.` prefix will pass to Paimon catalog properties and HDFS configuration. For example, if specify `gravitino.bypass.table.type`, `table.type` will pass to Paimon catalog properties.

#### JDBC Backend

If you are using JDBC backend, you must specify the properties like `jdbc-user`, `jdbc-password` and `jdbc-driver`.

| Property name   | Description                                                                                               | Default   | Required                                              | Since    |
|-----------------|-----------------------------------------------------------------------------------------------------------|-----------------|-------------------------------------------------------|------------------|
| `jdbc-user`     | Jdbc user of Gravitino Paimon catalog for `jdbc` backend.                                                 | (none)          | required if the value of `catalog-backend` is `jdbc`. | 0.7.0-incubating |
| `jdbc-password` | Jdbc password of Gravitino Paimon catalog for `jdbc` backend.                                             | (none)          | required if the value of `catalog-backend` is `jdbc`. | 0.7.0-incubating |
| `jdbc-driver`   | `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver` for MySQL, `org.postgresql.Driver` for PostgreSQL   | (none)          | required if the value of `catalog-backend` is `jdbc`. | 0.7.0-incubating |

:::caution
When using the JDBC backend, download the corresponding JDBC driver and place it in the `catalogs/lakehouse-paimon/libs` directory.
:::

### Catalog Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema Capabilities

The Paimon catalog supports `createSchema`, `dropSchema`, `loadSchema`, `listSchema`, and cascade-dropping schemas. It does not support `alterSchema`.

### Schema Properties

For `FilesystemCatalog`, the Paimon catalog:

- Does not support specifying a schema location or storing arbitrary schema properties at `createSchema`.
- Does not return schema properties at `loadSchema`.
- Does not store the schema comment.

### Schema Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more details.

## Table

### Table Capabilities

The Paimon catalog supports `createTable`, `purgeTable`, `alterTable`, `loadTable`, and `listTable`. Column default values are supported through table properties such as `fields.{columnName}.default-value`; column-expression defaults are not supported.

The Paimon catalog does not support `dropTable` or table sort orders. Table distributions are accepted only as a way to configure Paimon's bucketing (HASH strategy); see [Table Properties](#table-properties) for the related `bucket` and `bucket-key` reserved properties.

:::info
The Paimon catalog deliberately omits `dropTable` because Paimon's `dropTable` removes both the metadata and the table location from the file system and skips the trash. Use `purgeTable` instead.
:::

:::info
Paimon does not support auto-increment columns.
:::

### Table Changes

- RenameTable
- AddColumn
- DeleteColumn
- RenameColumn
- UpdateColumnComment
- UpdateColumnNullability
- UpdateColumnPosition
- UpdateColumnType
- UpdateComment
- SetProperty
- RemoveProperty

### Table Partitions

The Paimon catalog supports only identity partitions, such as `day` and `hour`. See [Paimon DDL Create Table](https://paimon.apache.org/docs/0.8/spark/sql-ddl/#create-table) for more details.

### Table Sort Orders

The Paimon catalog does not support table sort orders.

### Table Distributions

The Paimon catalog does not support table distributions.

### Table Indexes

The Paimon catalog supports only the primary-key index.

:::info
At most one primary-key index can be defined per table, but it may cover multiple fields as a joint primary key.
:::

:::info
A Paimon table's primary key constraint must not include any of the partition fields. Including a partition field in the primary key results in only one record per partition.
:::

### Table Column Types

| Gravitino Type    | Apache Paimon Type           |
|-------------------|------------------------------|
| `Struct`          | `Row`                        |
| `Map`             | `Map`                        |
| `List`            | `Array`                      |
| `Boolean`         | `Boolean`                    |
| `Byte`            | `TinyInt`                    |
| `Short`           | `SmallInt`                   |
| `Integer`         | `Int`                        |
| `Long`            | `BigInt`                     |
| `Float`           | `Float`                      |
| `Double`          | `Double`                     |
| `Decimal`         | `Decimal`                    |
| `String`          | `VarChar(Integer.MAX_VALUE)` |
| `VarChar`         | `VarChar`                    |
| `FixedChar`       | `Char`                       |
| `Date`            | `Date`                       |
| `Time(p)`         | `Time(p)`                    |
| `Timestamp(p)`    | `LocalZonedTimestamp(p)`     |
| `Timestamp_tz(p)` | `Timestamp(p)`               |
| `Fixed`           | `Binary`                     |
| `Binary`          | `VarBinary`                  |

:::info
Gravitino doesn't support Paimon `MultisetType` type.
:::

### Table Properties

Pass [Paimon table properties](https://paimon.apache.org/docs/0.8/maintenance/configurations/) to Gravitino when creating a Paimon table.

:::note
**Reserved**: Fields that cannot be passed to the Gravitino server.

**Immutable**: Fields that cannot be modified once set.
:::

Bucket settings are defined via Gravitino table distribution (HASH strategy). The `bucket` and
`bucket-key` options are reserved and derived from the distribution instead of being set directly.

| Property | Description               | Default | Required  | Reserved | Immutable | Since     |
|--------------------|---------------------------|---------------|-----------|----------|-----------|-------------------|
| `merge-engine`     | The table merge-engine.   | (none)        | No        | No       | Yes       | 0.6.0-incubating  |
| `sequence.field`   | The table sequence.field. | (none)        | No        | No       | Yes       | 0.6.0-incubating  |
| `rowkind.field`    | The table rowkind.field.  | (none)        | No        | No       | Yes       | 0.6.0-incubating  |
| `comment`          | The table comment.        | (none)        | No        | Yes      | No        | 0.6.0-incubating  |
| `owner`            | The table owner.          | (none)        | No        | Yes      | No        | 0.6.0-incubating  |
| `bucket-key`       | The table bucket-key.     | (none)        | No        | Yes      | No        | 0.6.0-incubating  |
| `bucket`           | The table bucket number.  | (none)        | No        | Yes      | No        | 1.2.0  |
| `primary-key`      | The table primary-key.    | (none)        | No        | Yes      | No        | 0.6.0-incubating  |
| `partition`        | The table partition.      | (none)        | No        | Yes      | No        | 0.6.0-incubating  |

### Table Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for more details.

## View

### View Capabilities

- Supports list, create, load, alter, and drop for views stored in the Paimon catalog.
- Each view must include exactly one SQL representation with dialect `query`, which serves as the canonical view definition.
- Additional dialect-specific SQL representations (for example, `spark` or `trino`) can be provided alongside the required `query` representation.
- The `defaultCatalog` and `defaultSchema` fields are stored as Paimon view options and can be used to resolve unqualified identifiers in the SQL text.
- View support depends on the selected Paimon backend and requires backend view API support.

### View Operations

Refer to [Manage view metadata using Gravitino](./manage-view-metadata-using-gravitino.md) for more details.

## HDFS Configuration

Place `core-site.xml` and `hdfs-site.xml` in the `catalogs/lakehouse-paimon/conf` directory to automatically load as the default HDFS configuration.

:::caution
When reading and writing HDFS, the Gravitino server operates only as the configured Kerberos user; proxying to other Kerberos users is not supported.
:::
