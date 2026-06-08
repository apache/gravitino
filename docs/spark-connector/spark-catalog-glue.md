---
title: "Spark connector Glue catalog"
slug: /spark-connector/spark-catalog-glue
keyword: spark connector glue catalog aws
license: "This software is licensed under the Apache License version 2."
---

With the Apache Gravitino Spark connector, accessing data or managing metadata in AWS Glue Data Catalog becomes straightforward, enabling seamless federation queries across Glue catalogs.

## Capabilities

Supports most DDL and DML operations in SparkSQL, except such operations:

- Function operations (Gravitino UDFs are supported, see [Spark connector - User-defined functions](spark-connector-udf.md))
- Partition operations
- View operations
- `LOAD` clause
- `CREATE TABLE LIKE` clause
- `TRUNCATE TABLE` clause

## Table format support

The Glue catalog supports mixed table formats within a single database:

- **Hive-format tables** (PARQUET, ORC, TEXTFILE, etc.): routed to HiveTableCatalog backed by AWS Glue Data Catalog Hive client
- **Iceberg-format tables**: routed to Iceberg's SparkCatalog (GlueCatalog) for I/O

Table routing is based on the `table-format` property in Glue table parameters. Tables with `table-format=ICEBERG` are delegated to the Iceberg backend.

:::note
HiveTableCatalog is configured with `hive.imetastoreclient.factory.class` set to `AWSGlueDataCatalogHiveClientFactory`, which replaces the embedded Derby metastore with a direct AWS Glue API connection. This means no local Derby sync is required and the catalog works correctly across multiple concurrent Spark applications sharing the same Glue catalog.
:::

## Requirements

- Network access to the AWS Glue API and Amazon S3
- AWS IAM credentials with necessary Glue and S3 permissions
  See [AWS IAM permissions](../aws-glue-catalog.md#aws-iam-permissions) for the required policy
- Apache Spark 3.3+, 3.4, or 3.5

## SQL example

```sql
// Suppose glue_catalog is the Glue catalog name managed by Gravitino
USE glue_catalog;

CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;

// Create a Hive-format Parquet table
CREATE TABLE IF NOT EXISTS employees (
    id INT,
    name STRING,
    age INT
)
PARTITIONED BY (department STRING)
STORED AS PARQUET;

DESC TABLE EXTENDED employees;

INSERT OVERWRITE TABLE employees PARTITION(department='Engineering') 
VALUES (1, 'John Doe', 30), (2, 'Jane Smith', 28);

SELECT * FROM employees WHERE department = 'Engineering';

// Create an Iceberg table
CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT,
    customer STRING,
    amount DECIMAL(10, 2),
    order_ts TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(order_ts));

INSERT INTO orders VALUES (1, 'alice', 99.99, TIMESTAMP '2024-01-01 00:00:00');

SELECT * FROM orders WHERE order_ts >= DATE '2024-01-01';
```

## Catalog properties

Gravitino Spark connector transforms the following property names defined in catalog properties to Spark Hive/Iceberg connector configuration.

| Property name in Gravitino catalog properties | Spark Hive connector configuration      | Description                           | Since Version |
|-----------------------------------------------|-------------------------------------------|---------------------------------------|---------------|
| `aws-region`                                  | `aws.region`                              | AWS region for Glue Data Catalog      | 0.7.0         |
| `aws-glue-catalog-id`                         | `aws.glue.catalog.id`                     | 12-digit AWS account ID owning Glue catalog | 0.7.0 |
| `aws-glue-endpoint`                           | `aws.glue.endpoint`                       | Custom Glue endpoint URL             | 0.7.0         |
| `warehouse`                                   | (Iceberg) `warehouse`                     | Base storage path for Iceberg tables  | 0.7.0         |

For Iceberg tables, Gravitino properties are mapped to Iceberg GlueCatalog configuration:

| Gravitino property      | Iceberg GlueCatalog property        | Description                              |
|-------------------------|--------------------------------------|------------------------------------------|
| `aws-region`            | `client.region`                       | AWS region                                |
| `aws-glue-catalog-id`   | `glue.id`                              | Glue catalog ID                           |
| `aws-glue-endpoint`     | `glue.endpoint`                        | Glue endpoint URL                         |
| `aws-access-key-id`     | `client.credentials-provider.*`       | AWS access key (via custom provider)      |
| `aws-secret-access-key` | `client.credentials-provider.*`       | AWS secret key (via custom provider)       |

Gravitino catalog property names with the prefix `spark.bypass.` are passed directly to the Spark Hive connector. For example, using `spark.bypass.hive.exec.dynamic.partition.mode` to pass `hive.exec.dynamic.partition.mode` to the Spark Hive connector.

## Creating a catalog

Use Gravitino REST API or the Gravitino CLI to create a Glue catalog:

```bash
curl -X POST -H "Content-Type: application/json" \
-d '{
  "name": "glue_catalog",
  "type": "RELATIONAL",
  "provider": "glue",
  "properties": {
    "aws-region": "us-east-1",
    "aws-access-key-id": "<your-access-key-id>",
    "aws-secret-access-key": "<your-secret-access-key>",
    "warehouse": "s3://my-bucket/warehouse"
  }
}' http://gravitino-host:8090/api/metalakes/{metalake}/catalogs
```

For more information about the Glue catalog, refer to [AWS Glue catalog](../aws-glue-catalog.md).

## S3 storage

When using AWS S3 with the Glue catalog, configure S3 credentials in the Spark configuration:

```bash
spark-submit \
  --conf spark.sql.catalog.glue_catalog=com.gravitino.spark.connector.GlueCatalog \
  --conf spark.sql.catalog.glue_catalog.warehouse=s3://my-bucket/warehouse \
  --conf spark.sql.catalog.glue_catalog.fs.s3a.access.key=<your-access-key> \
  --conf spark.sql.catalog.glue_catalog.fs.s3a.secret.key=<your-secret-key> \
  ...
```

Alternatively, use `spark.bypass.` prefix in catalog properties to pass S3 configuration:

```json
{
  "properties": {
    "aws-region": "us-east-1",
    "warehouse": "s3://my-bucket/warehouse",
    "spark.bypass.fs.s3a.access.key": "<your-access-key>",
    "spark.bypass.fs.s3a.secret.key": "<your-secret-key>"
  }
}
```

:::note
Ensure you have the AWS Java SDK and Hadoop AWS JARs in your Spark classpath when using S3 storage.
:::
