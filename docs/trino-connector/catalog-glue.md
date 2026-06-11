---
title: "Apache Gravitino Trino connector - Glue catalog"
slug: /trino-connector/catalog-glue
keyword: gravitino connector trino glue aws
license: "This software is licensed under the Apache License version 2."
---

## Overview

The Glue catalog allows Trino to query data stored in tables registered in the AWS Glue Data Catalog.
It supports both Hive-format and Iceberg-format tables on Amazon S3.

## Requirements

- Network access from the Trino coordinator and workers to the AWS Glue API and Amazon S3.
- An AWS IAM credential (access key pair or instance profile) with the necessary Glue and S3 permissions.
  See [AWS IAM permissions](../aws-glue-catalog.md#aws-iam-permissions) for the required policy.
- Data files stored on Amazon S3.

## Schema Operations

### Create a Schema

```sql
CREATE SCHEMA glue_test.schema_name;
```

## Table Operations

### Create a Hive-Format Table

The default table format is Hive. The following example creates an ORC-format partitioned table:

```sql
CREATE TABLE glue_test.db01.orders
(
  order_id  bigint,
  customer  varchar,
  amount    decimal(10, 2),
  order_dt  date
)
WITH (
  format       = 'ORC',
  location     = 's3://my-bucket/warehouse/db01/orders',
  partitioned_by = ARRAY['order_dt']
);
```

To create a bucketed, sorted table:

```sql
CREATE TABLE glue_test.db01.events
(
  event_id bigint,
  user_id  bigint,
  ts       timestamp
)
WITH (
  format       = 'PARQUET',
  location     = 's3://my-bucket/warehouse/db01/events',
  bucketed_by  = ARRAY['user_id'],
  bucket_count = 8,
  sorted_by    = ARRAY['ts DESC']
);
```

### Create an Iceberg-Format Table

Set `type = 'ICEBERG'` to create an Iceberg table. Iceberg tables support richer partition transforms.

```sql
CREATE TABLE glue_test.db01.logs
(
  log_id   bigint,
  message  varchar,
  event_ts timestamp(6) with time zone
)
WITH (
  type         = 'ICEBERG',
  location     = 's3://my-bucket/warehouse/db01/logs',
  partitioned_by = ARRAY['hour(event_ts)']
);
```

Supported Iceberg partition transform expressions in `partitioned_by`:

| Expression          | Description              |
|---------------------|--------------------------|
| `column`            | Identity partition       |
| `year(column)`      | Partition by year        |
| `month(column)`     | Partition by month       |
| `day(column)`       | Partition by day         |
| `hour(column)`      | Partition by hour        |
| `bucket(column, N)` | Hash into N buckets      |
| `truncate(column, W)` | Truncate to width W    |

:::note
`CREATE OR REPLACE TABLE AS SELECT` is not supported. Use `DROP TABLE` followed by `CREATE TABLE AS SELECT` as an alternative.
:::

### Alter Table

The following alter table operations are supported:

- Add a column
- Drop a column
- Rename a column
- Change a column type
- Set a table property

:::caution
Hive-format table rename is not supported. AWS Glue does not provide a native rename API for tables.
Iceberg-format table rename is supported.
:::

### Select

```sql
SELECT * FROM glue_test.db01.orders WHERE order_dt = DATE '2024-01-01';
```

### Insert

```sql
INSERT INTO glue_test.db01.orders (order_id, customer, amount, order_dt)
VALUES (1, 'alice', 99.99, DATE '2024-01-01');
```

### Update

`UPDATE` is supported only for Iceberg tables (v2 spec or higher).

```sql
UPDATE glue_test.db01.logs SET message = 'updated' WHERE log_id = 1;
```

### Delete

For Hive-format tables, `DELETE` is only supported when the `WHERE` clause matches entire partitions.
For Iceberg tables, row-level deletion is supported.

```sql
-- Hive table: delete an entire partition
DELETE FROM glue_test.db01.orders WHERE order_dt = DATE '2024-01-01';

-- Iceberg table: row-level delete
DELETE FROM glue_test.db01.logs WHERE log_id = 42;
```

### Drop

```sql
DROP TABLE glue_test.db01.orders;

DROP SCHEMA glue_test.db01;
```

## Schema and Table Properties

### Create a Schema with Properties

```sql
CREATE SCHEMA glue_test.db01
WITH (
  location = 's3://my-bucket/warehouse/db01'
);
```

| Property   | Description              | Default Value | Required | Since Version |
|------------|--------------------------|---------------|----------|---------------|
| `location` | S3 location for the schema | (none)      | No       | 1.3.0         |

### Create a Table with Properties

```sql
CREATE TABLE glue_test.db01.table_name
(
  name   varchar,
  salary integer
) WITH (
  type           = 'HIVE',
  format         = 'PARQUET',
  location       = 's3://my-bucket/warehouse/db01/table_name',
  partitioned_by = ARRAY['salary'],
  bucketed_by    = ARRAY['name'],
  bucket_count   = 4,
  sorted_by      = ARRAY['name']
);
```

| Property        | Description                                                                 | Default Value | Required | Since Version |
|-----------------|-----------------------------------------------------------------------------|---------------|----------|---------------|
| `type`          | Table format: `HIVE` or `ICEBERG`                                           | `HIVE`        | No       | 1.3.0         |
| `format`        | File format for Hive-format tables: `PARQUET`, `ORC`, `TEXTFILE`, etc.      | `TEXTFILE`    | No       | 1.3.0         |
| `location`      | S3 storage location for the table                                           | (derived from catalog `warehouse`) | No | 1.3.0 |
| `partitioned_by`| Partition columns or expressions. For Iceberg, use transform syntax such as `year(col)`. | (none) | No | 1.3.0 |
| `bucketed_by`   | Bucket columns (Hive-format tables only)                                    | (none)        | No       | 1.3.0         |
| `bucket_count`  | Number of buckets (required when `bucketed_by` is set)                      | (none)        | No       | 1.3.0         |
| `sorted_by`     | Sort order columns, e.g. `ARRAY['col ASC NULLS LAST', 'col2 DESC']`        | (none)        | No       | 1.3.0         |

## Examples

Follow these steps to use the Glue catalog in Trino through Gravitino.

### Create a Catalog in Gravitino

Use the Trino CLI to create the catalog. Assuming the metalake is `test` and the catalog name is `glue_test`:

```sql
CALL gravitino.system.create_catalog(
  'glue_test',
  'glue',
  MAP(
    ARRAY['aws-region', 'aws-access-key-id', 'aws-secret-access-key', 'warehouse'],
    ARRAY['us-east-1', '<aws-access-key-id>', '<aws-secret-access-key>', 's3://my-bucket/warehouse']
  )
);
```

For more information about the Glue catalog, refer to [AWS Glue catalog](../aws-glue-catalog.md).

### Connect and List Catalogs

Set `gravitino.metalake` to `test` and start the Trino container. Then list catalogs:

```sql
SHOW CATALOGS;
```

The results are similar to:

```text
    Catalog
----------------
 gravitino
 glue_test
 jmx
 system
(4 rows)
```

The `glue_test` catalog corresponds to the catalog created in Gravitino.

## Data Type Mapping

The Glue connector extends the Hive data type mapping with additional support for Iceberg types.

| Gravitino Type               | Trino Type                   | Notes                                        |
|------------------------------|------------------------------|----------------------------------------------|
| `boolean`                    | `BOOLEAN`                    |                                              |
| `byte`                       | `TINYINT`                    |                                              |
| `short`                      | `SMALLINT`                   |                                              |
| `integer`                    | `INTEGER`                    |                                              |
| `long`                       | `BIGINT`                     |                                              |
| `float`                      | `REAL`                       |                                              |
| `double`                     | `DOUBLE`                     |                                              |
| `decimal(p, s)`              | `DECIMAL(p, s)`              |                                              |
| `string`                     | `VARCHAR`                    |                                              |
| `varchar(n)`                 | `VARCHAR(n)`                 |                                              |
| `char(n)`                    | `CHAR(n)`                    |                                              |
| `binary`                     | `VARBINARY`                  |                                              |
| `date`                       | `DATE`                       |                                              |
| `time(6)`                    | `TIME(6)`                    | Iceberg tables only; microsecond precision   |
| `timestamp`                  | `TIMESTAMP(3)`               | Hive-format tables; millisecond precision    |
| `timestamp(6)`               | `TIMESTAMP(6)`               | Iceberg tables; microsecond precision        |
| `timestamptz(6)`             | `TIMESTAMP(6) WITH TIME ZONE`| Iceberg tables only                          |
| `list`                       | `ARRAY`                      |                                              |
| `map`                        | `MAP`                        |                                              |
| `struct`                     | `ROW`                        |                                              |

:::note
`TIME` and `TIMESTAMP WITH TIME ZONE` are available for Iceberg-format tables only. Hive-format tables use millisecond-precision `TIMESTAMP` without time zone.
:::

## Trino Connector Configuration

Gravitino passes catalog properties to the underlying Trino Hive connector. Supply additional Trino connector properties using the `trino.bypass.` prefix:

```sql
CALL gravitino.system.create_catalog(
  'glue_test',
  'glue',
  MAP(
    ARRAY['aws-region', 'aws-access-key-id', 'aws-secret-access-key', 'warehouse',
          'trino.bypass.hive.metastore.glue.max-connections'],
    ARRAY['us-east-1', '<aws-access-key-id>', '<aws-secret-access-key>', 's3://my-bucket/warehouse',
          '50']
  )
);
```

The following Gravitino catalog properties are automatically forwarded to the Trino connector and cannot be overridden via `trino.bypass.*`:

| Gravitino property      | Trino connector property              |
|-------------------------|---------------------------------------|
| `aws-region`            | `hive.metastore.glue.region`          |
| `aws-glue-catalog-id`   | `hive.metastore.glue.catalogid`       |
| `aws-access-key-id`     | `hive.metastore.glue.aws-access-key`, `hive.s3.aws-access-key` |
| `aws-secret-access-key` | `hive.metastore.glue.aws-secret-key`, `hive.s3.aws-secret-key` |
| `aws-glue-endpoint`     | `hive.metastore.glue.endpoint-url`    |

For additional Trino Hive connector configuration options, refer to the
[Trino Hive connector documentation](https://trino.io/docs/current/connector/hive.html).
