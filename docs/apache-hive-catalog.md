---
title: "Hive Catalog"
slug: "/apache-hive-catalog"
keywords:
  - hive
  - hms
  - metadata
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The Hive catalog enables Apache Gravitino to manage Hive metadata through a Hive Metastore Service (HMS), or a compatible implementation such as AWS Glue. Use it when you want a single Gravitino-managed access surface over a Hive metastore, with the option to federate it alongside other relational, lakehouse, and fileset catalogs, and to govern Hive-backed datasets that may sit on HDFS or on cloud object storage (S3, ADLS, GCS).

### Requirements and Limitations

- **Hive Metastore Service required.** The Hive catalog connects to a Hive Metastore Service (HMS), or a compatible implementation such as AWS Glue. Gravitino must have network access to the metastore over the Thrift protocol.
- **Supported HMS versions:** 2.x and 3.x. The Hive catalog detects the metastore version automatically; the `default.catalog` property is honored only by the Hive 3 backend.
- **Supported storage backings:** HDFS, plus S3, Azure Blob Storage (ADLS), and Google Cloud Storage (GCS) when the underlying Hive metastore is configured for cloud storage. See [Hive Catalog with Cloud Storage](#hive-catalog-with-cloud-storage) below.
- **Authentication.** `simple` and `Kerberos` are supported. For Kerberos, set the `kerberos.principal` and `kerberos.keytab-uri` properties together with the related `gravitino.bypass.hadoop.security.authentication`, `gravitino.bypass.hive.metastore.kerberos.principal`, and `gravitino.bypass.hive.metastore.sasl.enabled` Hadoop-security keys.
- **No column default values.**
- **No table indexes.** Hive removed native table indexes after 2.x; use partitioning and bucketing for pruning instead. See [Table Indexes](#table-indexes).
- **Alter Partition is under development.** Other Hive Alter operations (table, column) are supported; see [Alter Operations](#alter-operations) for the full mapping.

## Quick Start

Create a minimum-viable Hive catalog and confirm it is reachable. The example assumes a Gravitino server at `http://localhost:8090`, a metalake named `test`, and a Hive Metastore Service at `thrift://localhost:9083`. Adjust the values for your environment.

### Create the Catalog

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "hive_catalog",
    "type": "RELATIONAL",
    "comment": "Hive catalog",
    "provider": "hive",
    "properties": {
      "metastore.uris": "thrift://localhost:9083"
    }
  }' \
  http://localhost:8090/api/metalakes/test/catalogs
```

The response is a JSON object describing the created catalog.

### Verify the Catalog

```bash
# List catalogs in the metalake. hive_catalog should appear.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs" | jq

# Load the catalog directly and inspect its properties.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/hive_catalog" | jq

# List schemas (Hive databases). The response typically includes at least the `default` database.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/hive_catalog/schemas" | jq
```

**Success check:** the catalog-list response includes `hive_catalog`, the load-catalog response shows `"provider":"hive"` and `metastore.uris` set to the Thrift URI, and the schema-list response includes at least the `default` database. If the schema-list call returns a Thrift connection error, verify the Hive metastore is running and reachable on the configured port, and confirm that the Gravitino server has network access to that port.

## Catalog

### Catalog Capabilities

The Hive catalog:

- Acts as a Gravitino front-end over a Hive Metastore Service or compatible implementation (for example, AWS Glue).
- Supports creating, listing, loading, altering, and dropping Hive databases and tables.
- Supports Hive table partitioning (Identity transform only), bucketing (Hash distribution only), and sort orders, matching what Hive natively supports.
- Supports Hive views stored in the metastore as `VIRTUAL_VIEW`, with automatic dialect detection across Hive, Trino, and Spark.
- Supports user impersonation (`impersonation-enable`) and Kerberos authentication.
- Optionally hides non-Hive tables (Iceberg, Paimon, Hudi) from listings when `list-all-tables=false`.

### Catalog Properties

Besides the [common catalog properties](./gravitino-server-config.md#catalog-properties), the Hive catalog has the following properties:

| Property Name                            | Description                                                                                                                                                                                                                                         | Default Value | Required                     | Since Version |
|------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|------------------------------|---------------|
| `metastore.uris`                         | The Hive metastore service URIs, separate multiple addresses with commas. Such as `thrift://127.0.0.1:9083`                                                                                                                                         | (none)        | Yes                          | 0.2.0         |
| `client.pool-size`                       | The maximum number of Hive metastore clients in the pool for Gravitino.                                                                                                                                                                             | 1             | No                           | 0.2.0         |
| `gravitino.bypass.`                      | Properties with this prefix are passed down to the underlying HMS client. For example, `gravitino.bypass.hive.metastore.failure.retries = 3` configures the HMS client to retry failed Thrift calls three times.                                    | (none)        | No                           | 0.2.0         |
| `client.pool-cache.eviction-interval-ms` | The cache pool eviction interval.                                                                                                                                                                                                                   | 300000        | No                           | 0.4.0         |
| `impersonation-enable`                   | Enable user impersonation for Hive catalog.                                                                                                                                                                                                         | false         | No                           | 0.4.0         |
| `kerberos.principal`                     | The Kerberos principal for the catalog. To use Kerberos, also configure `gravitino.bypass.hadoop.security.authentication`, `gravitino.bypass.hive.metastore.kerberos.principal`, and `gravitino.bypass.hive.metastore.sasl.enabled`.                | (none)        | Required if you use Kerberos | 0.4.0         |
| `kerberos.keytab-uri`                    | The URI of the keytab for the catalog. Supported protocols are `https`, `http`, `ftp`, and `file`.                                                                                                                                                  | (none)        | Required if you use Kerberos | 0.4.0         |
| `kerberos.check-interval-sec`            | Interval, in seconds, at which the catalog re-checks that the principal is still valid.                                                                                                                                                             | 60            | No                           | 0.4.0         |
| `kerberos.keytab-fetch-timeout-sec`      | Timeout, in seconds, for fetching the keytab.                                                                                                                                                                                                       | 60            | No                           | 0.4.0         |
| `list-all-tables`                        | Whether to list all tables in a database, including non-Hive tables such as Iceberg, Paimon, and Hudi. When false, non-Hive tables are filtered out on a best-effort basis; see the note below for known limitations.                            | false         | No                           | 0.5.1         |
| `default.catalog`                        | The default catalog name for the Hive3 metastore backend; this configuration is ignored when using a Hive2 metastore.                                                                                                                               | hive          | No                           | 1.1.0         |

:::note
When `list-all-tables=false`, the Hive catalog removes the following on a best-effort basis:
- Iceberg tables (table property `table_type=ICEBERG`)
- Paimon tables (table property `table_type=PAIMON`)
- Hudi tables (table property `provider=hudi`), together with their `_ro` and `_rt` siblings

**Known limitation.** Filtering is performed server-side via the Hive Metastore, which only
supports exact-key lookups on dot-free property keys. Hudi tables registered directly by Spark
(e.g. via `saveAsTable`) typically only set `spark.sql.sources.provider=hudi` without also
setting `provider=hudi`, so they cannot be filtered out and will appear in the listing.

**Workaround.** Add a dot-free `provider=hudi` property to such tables so the server-side
filter can match them. Either after creation:

```sql
ALTER TABLE <db>.<table> SET TBLPROPERTIES ('provider'='hudi');
```

or at write time via Hudi's Hive sync option:

```scala
df.write.format("hudi")
  .option("hoodie.datasource.hive_sync.table_properties", "provider=hudi")
  .saveAsTable("<db>.<table>")
```

The corresponding `_ro` / `_rt` siblings are removed automatically based on the base table name.
:::

When you use Gravitino with Trino, pass Trino Hive connector configuration through the `trino.bypass.` prefix. For example, set `trino.bypass.hive.config.resources` to forward `hive.config.resources` to the Gravitino Hive catalog at Trino runtime.

When you use Gravitino with Spark, pass Spark Hive connector configuration through the `spark.bypass.` prefix. For example, set `spark.bypass.hive.exec.dynamic.partition.mode` to forward `hive.exec.dynamic.partition.mode` to the Spark Hive connector at Spark runtime.

When you authorize the Hive catalog with Apache Ranger, see the [authorization with Ranger properties](security/authorization-pushdown.md#configure-the-ranger-hadoop-sql-plugin).

### Catalog Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema Capabilities

- A Gravitino schema corresponds to a Hive database in the Hive Metastore Service.
- Supports creating, altering, and dropping Hive databases.
- Supports a schema-level `location` property to override the default warehouse directory; see [Schema Properties](#schema-properties).

### Schema Properties

Schema properties supply or set metadata for the underlying Hive database.
The following table lists predefined schema properties for the Hive database. Additionally, you can define your own key-value pair properties and transmit them to the underlying Hive database.

| Property name | Description                                                              | Default value                                                                           | Required | Since Version |
|---------------|--------------------------------------------------------------------------|-----------------------------------------------------------------------------------------|----------|---------------|
| `location`    | The directory for Hive database storage, such as `/user/hive/warehouse`. | HMS uses the value of `hive.metastore.warehouse.dir` in the `hive-site.xml` by default. | No       | 0.1.0         |

### Schema Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more details.

## Table

### Table Capabilities

- The Hive catalog supports creating, updating, and deleting tables in the HMS.
- The Hive catalog does not support column default values.

### Table Partitioning

The Hive catalog supports [partitioned tables](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-PartitionedTables). To create one, set a partitioning attribute on the table definition.
Although Gravitino supports several partitioning strategies, Apache Hive natively supports only one (partitioned by column). The Hive catalog therefore supports only `Identity` partitioning.

:::caution
The `fieldName` specified in the partitioning attribute must be the name of a column defined in the table.
:::

### Table Sort Orders and Distributions

The Hive catalog supports [bucketed sorted tables](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-BucketedSortedTables). To create one, set the `distribution` and `sortOrders` attributes on the table definition.
Although Gravitino supports several distribution strategies, Apache Hive natively supports only one (clustered by column). The Hive catalog therefore supports only `Hash` distribution.

:::caution
The `fieldName` specified in the `distribution` and `sortOrders` attribute must be the name of a column defined in the table.
:::

### Table Column Types

The Hive catalog supports all data types defined in the [Hive Language Manual](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types).
The following table lists the data types mapped from the Hive catalog to Gravitino.

| Hive Data Type              | Gravitino Data Type | Since Version |
|-----------------------------|---------------------|---------------|
| `boolean`                   | `boolean`           | 0.2.0         |
| `tinyint`                   | `byte`              | 0.2.0         |
| `smallint`                  | `short`             | 0.2.0         |
| `int`/`integer`             | `integer`           | 0.2.0         |
| `bigint`                    | `long`              | 0.2.0         |
| `float`                     | `float`             | 0.2.0         |
| `double`/`double precision` | `double`            | 0.2.0         |
| `decimal`                   | `decimal`           | 0.2.0         |
| `string`                    | `string`            | 0.2.0         |
| `char`                      | `char`              | 0.2.0         |
| `varchar`                   | `varchar`           | 0.2.0         |
| `timestamp`                 | `timestamp`         | 0.2.0         |
| `date`                      | `date`              | 0.2.0         |
| `interval_year_month`       | `interval_year`     | 0.2.0         |
| `interval_day_time`         | `interval_day`      | 0.2.0         |
| `binary`                    | `binary`            | 0.2.0         |
| `array`                     | `list`              | 0.2.0         |
| `map`                       | `map`               | 0.2.0         |
| `struct`                    | `struct`            | 0.2.0         |
| `uniontype`                 | `union`             | 0.2.0         |

:::info
1. Since 0.6.0-incubating, data types other than those listed above are mapped to the Gravitino **[External Type](./manage-relational-metadata-using-gravitino.md#external-type)**, which represents an unresolvable data type from the Hive catalog.
2. Since version 1.0.0, using the `struct` data type with field comments throws an error, because Hive does not honor field comments on struct columns (see [HIVE-26593](https://issues.apache.org/jira/browse/HIVE-26593)).
:::

### Table Properties

Table properties supply or set metadata for the underlying Hive tables.
The following table lists predefined table properties for a Hive table. Additionally, you can define your own key-value pair properties and transmit them to the underlying Hive table.

:::note
**Reserved**: Fields that cannot be passed to the Gravitino server.

**Immutable**: Fields that cannot be modified once set.
:::

| Property Name           | Description                                                                                                                                | Default Value                                                                                                                                       | Required | Reserved | Immutable | Since Version |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|----------|----------|-----------|---------------|
| `location`              | The location for table storage, such as `/user/hive/warehouse/test_table`.                                                                 | HMS uses the database location as the parent directory by default.                                                                                  | No       | No       | Yes       | 0.2.0         |
| `table-type`            | Type of the table. Valid values include `MANAGED_TABLE` and `EXTERNAL_TABLE`.                                                              | `MANAGED_TABLE`                                                                                                                                     | No       | No       | Yes       | 0.2.0         |
| `format`                | The table file format. Valid values include `TEXTFILE`, `SEQUENCEFILE`, `RCFILE`, `ORC`, `PARQUET`, `AVRO`, `JSON`, `CSV`, and `REGEX`.    | `TEXTFILE`                                                                                                                                          | No       | No       | Yes       | 0.2.0         |
| `input-format`          | The input format class for the table, such as `org.apache.hadoop.hive.ql.io.orc.OrcInputFormat`.                                           | The property `format` sets the default value `org.apache.hadoop.mapred.TextInputFormat` and can change it to a different default.                   | No       | No       | Yes       | 0.2.0         |
| `output-format`         | The output format class for the table, such as `org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat`.                                         | The property `format` sets the default value `org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat` and can change it to a different default. | No       | No       | Yes       | 0.2.0         |
| `serde-lib`             | The serde library class for the table, such as `org.apache.hadoop.hive.ql.io.orc.OrcSerde`.                                                | The property `format` sets the default value `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe` and can change it to a different default.         | No       | No       | Yes       | 0.2.0         |
| `serde.parameter.`      | Prefix for serde parameters. For example, `"serde.parameter.orc.create.index" = "true"` tells the ORC serde library to create row indexes. | (none)                                                                                                                                              | No       | No       | Yes       | 0.2.0         |
| `serde-name`            | The name of the serde                                                                                                                      | Table name by default.                                                                                                                              | No       | No       | Yes       | 0.2.0         |
| `comment`               | Used to store a table comment.                                                                                                             | (none)                                                                                                                                              | No       | Yes      | No        | 0.2.0         |
| `numFiles`              | Used to store the number of files in the table.                                                                                            | (none)                                                                                                                                              | No       | Yes      | No        | 0.2.0         |
| `totalSize`             | Used to store the total size of the table.                                                                                                 | (none)                                                                                                                                              | No       | Yes      | No        | 0.2.0         |
| `EXTERNAL`              | Indicates whether the table is external.                                                                                                   | (none)                                                                                                                                              | No       | Yes      | No        | 0.2.0         |
| `transient_lastDdlTime` | Used to store the last DDL time of the table.                                                                                              | (none)                                                                                                                                              | No       | Yes      | No        | 0.2.0         |

### Table Indexes

The Hive catalog does not support table indexes. Hive removed native indexing support after the 2.x line; the equivalent performance levers are partitioning and bucketing. See [Table Partitioning](#table-partitioning) and [Table Sort Orders and Distributions](#table-sort-orders-and-distributions) for the supported attributes.

### Table Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for more details.

#### Alter Operations

Gravitino defines a unified set of [metadata operation interfaces](./manage-relational-metadata-using-gravitino.md#alter-a-table), and almost all [Hive Alter operations](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-AlterTable/Partition/Column) have corresponding table update requests that let you change the structure of an existing table.
The following table lists the mapping relationship between Hive Alter operations and Gravitino table update requests.

##### Alter table

| Hive Alter Operation                          | Gravitino Table Update Request | Since Version |
|-----------------------------------------------|--------------------------------|---------------|
| `Rename Table`                                | `Rename table`                 | 0.2.0         |
| `Alter Table Properties`                      | `Set a table property`         | 0.2.0         |
| `Alter Table Comment`                         | `Update comment`               | 0.2.0         |
| `Alter SerDe Properties`                      | `Set a table property`         | 0.2.0         |
| `Remove SerDe Properties`                     | `Remove a table property`      | 0.2.0         |
| `Alter Table Storage Properties`              | Unsupported                    | -             |
| `Alter Table Skewed or Stored as Directories` | Unsupported                    | -             |
| `Alter Table Constraints`                     | Unsupported                    | -             |

:::note
As Gravitino has a separate interface for updating the comment of a table, the Hive catalog sets `comment` as a reserved property for the table, preventing users from setting the comment property. Apache Hive can modify the comment property of the table.
:::

##### Alter column

| Hive Alter Operation     | Gravitino Table Update Request    | Since Version |
|--------------------------|-----------------------------------|---------------|
| `Change Column Name`     | `Rename a column`                 | 0.2.0         |
| `Change Column Type`     | `Update the type of a column`     | 0.2.0         |
| `Change Column Position` | `Update the position of a column` | 0.2.0         |
| `Change Column Comment`  | `Update the column comment`       | 0.2.0         |

##### Alter partition

:::note
Support for altering partitions is under development.
:::

## View

### View Capabilities

- Supports list, create, load, alter, and drop for views stored in the Hive Metastore Service as `VIRTUAL_VIEW`.
- Each view must contain exactly one SQL representation.
- Supports the `hive`, `trino`, and `spark` dialects.
- When loading an existing HMS view, Gravitino automatically detects whether the view uses the `hive`, `trino`, or `spark` dialect.
- For the `hive` dialect, `defaultCatalog` and `defaultSchema` must be `null`.

### View Operations

Refer to [Manage view metadata using Gravitino](./manage-view-metadata-using-gravitino.md) for more details.

## Hive Catalog with Cloud Storage

To create a Hive catalog backed by S3, Azure Blob Storage (ADLS), or Google Cloud Storage (GCS), see the [Hive catalog with cloud storage](./hive-catalog-with-cloud-storage.md) guide. No cloud-specific configuration is required on the Gravitino side; the Hive catalog works with cloud storage the same way it works with HDFS, with the storage path pointing at the cloud bucket or container. Set the `location` property to the desired cloud path when creating the database or table, and configure the underlying Hive metastore for the storage backend as described in the cloud-storage guide.

