---
title: "AWS Glue catalog"
slug: /aws-glue-catalog
keywords:
  - glue
  - aws
  - metadata
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Apache Gravitino uses [AWS Glue Data Catalog](https://aws.amazon.com/glue/) as a metadata catalog.

### Requirements

* The Glue catalog requires network access to the AWS Glue API.
* Gravitino uses the AWS SDK v2 to communicate with Glue.

:::note
The Glue catalog is case-insensitive for schema and table names. AWS Glue folds database and table names to lowercase on storage.
:::

## Catalog

### Catalog Capabilities

The Glue catalog supports creating, updating, and deleting databases and tables in the AWS Glue Data Catalog.

- Supports all table types stored in Glue (Hive, Iceberg, Delta, Parquet, and others) by default.
- Supports Hive-format table partitioning, bucketing, and sort orders.
- Does not support views. Glue views (tables with `TableType=VIRTUAL_VIEW`) are filtered out.

### Catalog Properties

Besides the [common catalog properties](./gravitino-server-config.md#catalog-properties-configuration), the Glue catalog has the following properties:

| Property Name            | Description                                                                                                                                                                                                 | Default Value            | Required | Immutable | Since Version |
|--------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------|----------|-----------|---------------|
| `aws-region`             | AWS region for the Glue Data Catalog (e.g. `us-east-1`).                                                                                                                                                    | (none)                   | Yes      | Yes       | 1.3.0         |
| `aws-glue-catalog-id`    | The 12-digit AWS account ID that owns the Glue catalog. When omitted, defaults to the caller's AWS account ID.                                                                                              | (none)                   | No       | Yes       | 1.3.0         |
| `aws-access-key-id`      | AWS access key ID for static credential authentication. When omitted, the default credential chain is used.                                                                                                 | (none)                   | No       | No        | 1.3.0         |
| `aws-secret-access-key`  | AWS secret access key paired with `aws-access-key-id`. When omitted, the default credential chain is used.                                                                                                  | (none)                   | No       | No        | 1.3.0         |
| `aws-glue-endpoint`      | Custom Glue endpoint URL for VPC endpoints or LocalStack testing (e.g. `http://localhost:4566`).                                                                                                            | (none)                   | No       | No        | 1.3.0         |
| `warehouse`              | Base storage path used as the warehouse when no explicit `location` is specified at table creation time (e.g. `s3://my-bucket/warehouse`). Table location is derived as `warehouse/database/table`. | (none) | Yes | No | 1.3.0 |
| `default-table-format`   | Default format for tables created via Gravitino's `createTable()` API. Accepted values: `iceberg`, `hive`.                                                                                                  | `hive`                   | No       | No        | 1.3.0         |
| `table-format-filter`    | Comma-separated list of table formats exposed by `listTables()` and `loadTable()`. Accepted values: `all`, `hive`, `iceberg`, `delta`, `parquet`. Use to restrict visible table types.                     | `all`                    | No       | No        | 1.3.0         |

:::note
**Authentication priority**: Static credentials (`aws-access-key-id` + `aws-secret-access-key`) take precedence over the default credential chain (environment variables, instance profile, container credentials).
:::

### Catalog Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for more details.

:::note
Sensitive catalog properties such as `aws-access-key-id` and `aws-secret-access-key` are hidden from the load catalog response since Gravitino 1.3.0. Use the [credential vending API](security/credential-vending.md) to retrieve them at runtime.
:::

## Schema

### Schema Capabilities

The Glue catalog supports creating, updating, and deleting databases in the AWS Glue Data Catalog.

### Schema Properties

The Glue catalog defines no predefined schema properties beyond `comment`. Additional key-value properties pass through to the underlying Glue database.

### Schema Operations

See [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations).

## Table

### Table Capabilities

- The Glue catalog supports creating, updating, and deleting tables in the AWS Glue Data Catalog.
- All entries in the Glue `Table.parameters()` pass through Gravitino intact, so downstream tools can correctly identify the table format.
- Does not support column default value.
- Does not support NOT NULL constraints on columns.
- Does not support table indexes.

### Table Partitioning

The Glue catalog supports [partitioned tables](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-PartitionedTables). Create partitioned tables in the Glue catalog by specifying the partitioning attribute.

The supported partitioning strategies depend on the table format:

- **Hive-format tables**: Only `Identity` partitioning is supported, because the native Glue partition model is Hive-style key=value.
- **Iceberg-format tables**: All Iceberg partition transforms are supported: `identity`, `year`, `month`, `day`, `hour`, `bucket`, and `truncate`.

:::caution
The `fieldName` specified in the partitioning attribute must be the name of a column defined in the table.
:::

### Table Sort Orders and Distributions

The Glue catalog supports [bucketed sorted tables](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-BucketedSortedTables). Create bucketed sorted tables by setting the `distribution` and `sortOrders` attributes.
Although Gravitino supports several distribution strategies, AWS Glue inherently only supports a single distribution strategy (clustered by column). Therefore, the Glue catalog only supports `Hash` distribution.

:::caution
The `fieldName` specified in the `distribution` and `sortOrders` attribute must be the name of a column defined in the table.
:::

### Table Column Types

The Glue catalog supports all data types defined in the [Hive Language Manual](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types).
The following table lists the data types mapped from the Glue catalog to Gravitino.

| Glue Data Type            | Gravitino Data Type | Since Version |
|---------------------------|---------------------|---------------|
| `boolean`                 | `boolean`           | 1.3.0         |
| `tinyint`                 | `byte`              | 1.3.0         |
| `smallint`                | `short`             | 1.3.0         |
| `int` / `integer`         | `integer`           | 1.3.0         |
| `bigint`                  | `long`              | 1.3.0         |
| `float`                   | `float`             | 1.3.0         |
| `double`                  | `double`            | 1.3.0         |
| `decimal`                 | `decimal`           | 1.3.0         |
| `string`                  | `string`            | 1.3.0         |
| `char`                    | `char`              | 1.3.0         |
| `varchar`                 | `varchar`           | 1.3.0         |
| `timestamp`               | `timestamp`         | 1.3.0         |
| `date`                    | `date`              | 1.3.0         |
| `interval_year_month`     | `interval_year`     | 1.3.0         |
| `interval_day_time`       | `interval_day`      | 1.3.0         |
| `binary`                  | `binary`            | 1.3.0         |
| `array`                   | `list`              | 1.3.0         |
| `map`                     | `map`               | 1.3.0         |
| `struct`                  | `struct`            | 1.3.0         |
| `uniontype`               | `union`             | 1.3.0         |

:::info
Data types not listed above map to Gravitino **[External Type](./manage-relational-metadata-using-gravitino.md#external-type)**, which represents an unresolvable data type from the Glue catalog.
:::

### Table Properties

The following table lists predefined properties for Glue tables. Additional key-value properties pass through to the underlying Glue database.

:::note
**Reserved**: Fields that cannot be passed to the Gravitino server.

**Immutable**: Fields that cannot be modified once set.
:::

| Property Name           | Description                                                                                                                                | Default Value                                                                         | Required | Reserved | Immutable | Since Version |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------|----------|----------|-----------|---------------|
| `location`              | The location for table storage, such as `s3://bucket/prefix/test_table`. Derived from `warehouse/database/table` when not specified.       | (derived from warehouse)                                                              | No       | No       | No        | 1.3.0         |
| `format`                | The table file format (`parquet`, `orc`, `textfile`, etc.). When set, `input-format`, `output-format`, and `serde-lib` are derived automatically. Used primarily when creating Hive-format tables via Trino. | (none) | No | No | Yes | 1.3.0 |
| `input-format`          | The input format class for the table, such as `org.apache.hadoop.hive.ql.io.orc.OrcInputFormat`.                                           | `org.apache.hadoop.mapred.TextInputFormat`                                            | No       | No       | Yes       | 1.3.0         |
| `output-format`         | The output format class for the table, such as `org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat`.                                         | `org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat`                          | No       | No       | Yes       | 1.3.0         |
| `serde-lib`             | The serde library class for the table, such as `org.apache.hadoop.hive.ql.io.orc.OrcSerde`.                                                | `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe`                                  | No       | No       | Yes       | 1.3.0         |
| `serde-name`            | The name of the serde.                                                                                                                     | (none)                                                                                | No       | No       | No        | 1.3.0         |
| `serde.parameter.`      | The prefix of the serde parameter, such as `"serde.parameter.orc.create.index" = "true"`, indicating ORC serde lib to create row indexes. | (none)                                                                                | No       | No       | No        | 1.3.0         |
| `table-format`          | Table format stored in Glue `Table.parameters()`. Use `ICEBERG` to create an Iceberg table. Common values: `ICEBERG`, `HIVE`.              | (none)                                                                                | No       | No       | No        | 1.3.0         |
| `metadata_location`     | Iceberg table metadata file location stored in Glue `Table.parameters()`. When set during `createTable()`, registers an existing Iceberg table rather than creating a new one. | (none)                                                               | No       | No       | No        | 1.3.0         |
| `comment`               | Used to store a table comment.                                                                                                             | (none)                                                                                | No       | Yes      | No        | 1.3.0         |

:::note
All entries in the Glue `Table.parameters()` pass through Gravitino's API layer intact. This passthrough ensures `table_type=ICEBERG`, `metadata_location=s3://...`, `spark.sql.sources.provider=delta`, and any other format indicators survive Gravitino's metadata proxy layer.
:::

### Table Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for more details.

#### Alter Operations

Gravitino defines a unified set of [metadata operation interfaces](./manage-relational-metadata-using-gravitino.md#alter-a-table). The following table maps Glue alter operations to Gravitino table update requests.

##### Alter table

| Glue Alter Operation     | Gravitino Table Update Request | Since Version |
|--------------------------|--------------------------------|---------------|
| `Alter Table Properties` | `Set a table property`         | 1.3.0         |
| `Alter Table Comment`    | `Update comment`               | 1.3.0         |
| `Remove Properties`      | `Remove a table property`      | 1.3.0         |

:::caution
Hive-format table rename is not supported. AWS Glue does not provide a native rename API for tables; renaming would require recreating the table.
Iceberg-format table rename is supported.
:::

##### Alter column

| Glue Alter Operation     | Gravitino Table Update Request    | Since Version |
|--------------------------|-----------------------------------|---------------|
| `Change Column Name`     | `Rename a column`                 | 1.3.0         |
| `Change Column Type`     | `Update the type of a column`     | 1.3.0         |
| `Change Column Position` | `Update the position of a column` | 1.3.0         |
| `Change Column Comment`  | `Update the column comment`       | 1.3.0         |

##### Alter partition

The Glue catalog supports partition operations via `SupportsPartitions` for Hive-format identity-partitioned tables:

- `listPartitions()` / `listPartitionNames()`
- `getPartition(partitionName)`
- `addPartition(partition)`
- `dropPartition(partitionName)`

:::caution
Only `IdentityPartition` is supported because the Glue partition model is Hive-style key=value.
:::

## Iceberg Tables

The Glue catalog supports creating and managing Iceberg-format tables through the Apache Iceberg SDK's `GlueCatalog`. When an Iceberg table is created, Gravitino writes the `metadata.json` file to S3 and registers the table in Glue with the correct `metadata_location` parameter, making it usable by Trino (Lakehouse connector), Spark, and other Iceberg-native query engines.

### Create an Iceberg Table

Set `table-format=ICEBERG` in the table properties, or configure `default-table-format=iceberg` on the catalog to make all tables Iceberg by default.

The `warehouse` catalog property must be configured. The table location is derived as `warehouse/database/table` when no explicit `location` is specified.

### Register an Existing Iceberg Table

To register an Iceberg table that already exists in S3, set `metadata_location` to the path of the existing `metadata.json` file during `createTable()`. In this mode, Gravitino registers the table in Glue without creating new metadata.

### Iceberg Column Types

Iceberg tables use the Iceberg type system, which differs from Hive types. The following table lists the Gravitino types supported for Iceberg tables and how they map to Iceberg types:

| Gravitino Data Type      | Iceberg Data Type     | Notes                                               |
|--------------------------|-----------------------|-----------------------------------------------------|
| `boolean`                | `boolean`             |                                                     |
| `byte`                   | `int`                 | Widened to 32-bit integer                           |
| `short`                  | `int`                 | Widened to 32-bit integer                           |
| `integer`                | `int`                 |                                                     |
| `long`                   | `long`                |                                                     |
| `float`                  | `float`               |                                                     |
| `double`                 | `double`              |                                                     |
| `decimal(p, s)`          | `decimal(p, s)`       |                                                     |
| `string`                 | `string`              |                                                     |
| `varchar`                | `string`              | Iceberg has no variable-length char types           |
| `char`                   | `string`              | Iceberg has no variable-length char types           |
| `date`                   | `date`                |                                                     |
| `time(6)`                | `time`                | Only microsecond precision (6) is supported         |
| `timestamp(6)`           | `timestamp`           | Only microsecond precision (6) is supported         |
| `timestamptz(6)`         | `timestamptz`         | Only microsecond precision (6) is supported         |
| `binary`                 | `binary`              |                                                     |
| `fixed(n)`               | `binary`              | Mapped to variable-length binary                    |
| `uuid`                   | `uuid`                |                                                     |
| `list`                   | `list`                |                                                     |
| `map`                    | `map`                 |                                                     |
| `struct`                 | `struct`              |                                                     |

### Iceberg Table Alter Operations

For Iceberg tables, the following alter operations are supported:

| Operation                       | Gravitino Table Update Request    |
|---------------------------------|-----------------------------------|
| Add column                      | `Add a column`                    |
| Delete column                   | `Delete a column`                 |
| Rename column                   | `Rename a column`                 |
| Update column type              | `Update the type of a column`     |
| Update column comment           | `Update the column comment`       |
| Update column nullability       | `Update column nullability`       |
| Set table property              | `Set a table property`            |
| Remove table property           | `Remove a table property`         |

:::caution
Schema changes and property changes are committed in two separate Iceberg transactions. If the schema commit succeeds but the property commit fails, the table is left in a partially altered state.
:::

:::caution
Nested column operations (add, delete, rename, type update) are not supported for Iceberg tables via this catalog.
:::

## Security

### AWS IAM Permissions

The IAM policy attached to the credential used by the Glue catalog must cover both Glue metadata access and S3 data access:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "GlueMetadataAccess",
      "Effect": "Allow",
      "Action": [
        "glue:GetCatalog",
        "glue:GetDatabase", "glue:GetDatabases",
        "glue:CreateDatabase", "glue:UpdateDatabase", "glue:DeleteDatabase",
        "glue:GetTable", "glue:GetTables",
        "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
        "glue:GetPartition", "glue:GetPartitions",
        "glue:CreatePartition", "glue:DeletePartition"
      ],
      "Resource": [
        "arn:aws:glue:<region>:<account-id>:catalog",
        "arn:aws:glue:<region>:<account-id>:database/*",
        "arn:aws:glue:<region>:<account-id>:table/*/*"
      ]
    },
    {
      "Sid": "S3DataAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::<warehouse-bucket>",
        "arn:aws:s3:::<warehouse-bucket>/*"
      ]
    }
  ]
}
```
