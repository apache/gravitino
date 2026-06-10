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

- **Hive-format tables** (PARQUET, ORC, TEXTFILE, etc.): routed to HiveTableCatalog using the AWS Glue Data Catalog Hive client
- **Iceberg-format tables**: routed to Iceberg's SparkCatalog (GlueCatalog) for I/O

Table routing is based on the `table-format` property in Glue table parameters. Tables with `table-format=ICEBERG` are delegated to the Iceberg backend.

## Requirements

- Network access to the AWS Glue API and Amazon S3
- AWS IAM credentials with necessary Glue and S3 permissions.
  See [AWS IAM permissions](../aws-glue-catalog.md#aws-iam-permissions) for the required policy
- Apache Spark 3.3, 3.4, or 3.5
- Patched Hive and AWS Glue client JARs (see [Setup](#setup))

## Setup

Spark's bundled Hive 2.3.9 does not include the `HiveMetaStoreClientFactory` interface
(added by [HIVE-12679](https://issues.apache.org/jira/browse/HIVE-12679)) that the AWS Glue client
requires. You must replace the bundled Hive JARs with patched versions bundled together with the
Glue client.

:::note
On AWS managed Spark environments such as Amazon EMR, the Hive libraries are already patched and
the AWS Glue Data Catalog client is pre-installed. You can skip Steps 1 and 2 below and proceed
directly to [Creating a catalog](#creating-a-catalog).
:::

[spark-hive-glue-libs](https://github.com/datastrato/spark-hive-glue-libs) provides pre-built JARs
that include the patched Hive 2.3.10 and the AWS Glue Data Catalog client for Spark.

### Step 1: Download the JARs

```bash
BASE=https://raw.githubusercontent.com/datastrato/spark-hive-glue-libs/main/spark3/glue-3.4.0
mkdir -p /opt/glue-hive-jars
for jar in \
  aws-glue-datacatalog-spark-client-3.4.0.jar \
  hive-exec-2.3.10.jar \
  hive-metastore-2.3.10.jar \
  hive-common-2.3.10.jar \
  hive-serde-2.3.10.jar \
  hive-shims-2.3.10.jar \
  aws-java-sdk-glue-1.12.31.jar \
  aws-java-sdk-core-1.12.31.jar \
  jmespath-java-1.12.31.jar; do
  wget "$BASE/$jar" -P /opt/glue-hive-jars/
done
```

### Step 2: Configure Spark

Add the following configurations when starting Spark:

```bash
spark-submit \
  --conf spark.sql.hive.metastore.version=2.3.10 \
  --conf spark.sql.hive.metastore.jars=path \
  --conf spark.sql.hive.metastore.jars.path=/opt/glue-hive-jars/* \
  --conf spark.sql.hive.metastore.sharedPrefixes=com.amazonaws,org.apache.thrift,org.slf4j,com.google.common \
  ...
```

:::note
The `spark.sql.hive.metastore.jars=path` configuration instructs Spark to load the Hive metastore
client from the specified directory instead of its bundled Hive JARs.
The AWS SDK JARs in the directory are loaded in Spark's `IsolatedClientLoader` to prevent version
conflicts with `hadoop-aws`.
:::

## Creating a catalog

Use the Gravitino REST API or the Gravitino CLI to create a Glue catalog:

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

For more information about the Glue catalog properties, refer to [AWS Glue catalog](../aws-glue-catalog.md).

## SQL example

```sql
-- Suppose glue_catalog is the Glue catalog name managed by Gravitino
USE glue_catalog;

CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;

-- Create a Hive-format Parquet table
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

-- Create an Iceberg table
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

| Property name in Gravitino catalog properties | Spark Hive connector configuration | Description                                 | Since Version |
|-----------------------------------------------|------------------------------------|---------------------------------------------|---------------|
| `aws-region`                                  | `aws.region`                       | AWS region for Glue Data Catalog            | 1.3.0         |
| `aws-glue-catalog-id`                         | `aws.glue.catalog.id`              | 12-digit AWS account ID owning Glue catalog | 1.3.0         |
| `aws-glue-endpoint`                           | `aws.glue.endpoint`                | Custom Glue endpoint URL                    | 1.3.0         |
| `warehouse`                                   | (Iceberg) `warehouse`              | Base storage path for Iceberg tables        | 1.3.0         |

For Iceberg tables, Gravitino properties are mapped to Iceberg GlueCatalog configuration:

| Gravitino property      | Iceberg GlueCatalog property          | Description                               |
|-------------------------|---------------------------------------|-------------------------------------------|
| `aws-region`            | `client.region`                       | AWS region                                |
| `aws-glue-catalog-id`   | `glue.id`                             | Glue catalog ID                           |
| `aws-glue-endpoint`     | `glue.endpoint`                       | Glue endpoint URL                         |
| `aws-access-key-id`     | `client.credentials-provider.*`      | AWS access key (via custom provider)      |
| `aws-secret-access-key` | `client.credentials-provider.*`      | AWS secret key (via custom provider)      |

Gravitino catalog property names with the prefix `spark.bypass.` are passed directly to the Spark Hive connector. For example, using `spark.bypass.hive.exec.dynamic.partition.mode` to pass `hive.exec.dynamic.partition.mode` to the Spark Hive connector.

## S3 storage

When using AWS S3, configure S3 credentials via Spark configuration:

```bash
spark-submit \
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
