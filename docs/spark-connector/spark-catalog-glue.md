---
title: "Spark Connector: Glue Catalog"
slug: "/spark-connector/spark-catalog-glue"
keyword: "spark connector glue catalog aws"
license: "This software is licensed under the Apache License version 2."
---

## Overview

With the Apache Gravitino Spark connector, accessing data or managing metadata in AWS Glue Data Catalog becomes straightforward, enabling seamless federated queries across Glue catalogs.

## Capabilities

Supports most DDL and DML operations in SparkSQL, except these operations:

- Function operations (Gravitino UDFs are supported, see [Spark connector - User-defined functions](spark-connector-udf.md))
- Partition operations
- View operations
- `LOAD` clause
- `CREATE TABLE LIKE` clause
- `TRUNCATE TABLE` clause

## Table Format Support

The Glue catalog supports mixed table formats within a single database:

- **Hive-format tables** (PARQUET, ORC, TEXTFILE, etc.): routed to HiveTableCatalog using the AWS Glue Data Catalog Hive client
- **Iceberg-format tables**: routed to Iceberg's SparkCatalog (GlueCatalog) for I/O

Table routing is based on the `table-format` property in Glue table parameters. Tables with `table-format=ICEBERG` are delegated to the Iceberg backend.

## Requirements

- Network access to the AWS Glue API and Amazon S3
- AWS IAM credentials with necessary Glue and S3 permissions.
  See [AWS IAM permissions](../aws-glue-catalog.md#aws-iam-permissions) for the required policy
- Apache Spark 3.3, 3.4, or 3.5
- Patched Hive and AWS Glue client JARs (see [Setup](#setup); pre-installed on Amazon EMR)
- `iceberg-spark-runtime` and `iceberg-aws-bundle` JARs on the Spark classpath for Iceberg table support (not required on Amazon EMR)

## Setup

Spark's bundled Hive 2.3.9 does not include the `HiveMetaStoreClientFactory` interface
(added by [HIVE-12679](https://issues.apache.org/jira/browse/HIVE-12679)) that the AWS Glue client
requires. Replace the bundled Hive JARs with patched versions bundled together with the
Glue client.

:::note
On AWS managed Spark environments such as Amazon EMR, the Hive libraries are already patched and
the AWS Glue Data Catalog client is pre-installed. Skip Steps 1 and 2 below.
For a complete walkthrough on Amazon EMR, see [Deploy on Amazon EMR](#deploy-on-amazon-emr).
:::

For Iceberg table support on non-EMR environments, also place the following JARs in the Spark classpath:

- `iceberg-spark-runtime` — refer to the [Iceberg catalog](spark-catalog-iceberg.md#preparation) for the version that matches your Spark version
- `iceberg-aws-bundle` — matching the same Iceberg version, provides the AWS SDK v2 dependencies required by `GlueCatalog`

```bash
# Example for Spark 3.5 with Iceberg 1.6.1
curl -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar
curl -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar
```

:::note
On Amazon EMR, AWS SDK v2 is pre-installed on the cluster. `iceberg-aws-bundle` is not needed.
See [Deploy on Amazon EMR](#deploy-on-amazon-emr) for the EMR-specific JAR setup.
:::

[spark-hive-glue-libs](https://github.com/datastrato/spark-hive-glue-libs) provides pre-built JARs
that include the patched Hive 2.3.10 and the AWS Glue Data Catalog client for Spark.

### Step 1: Download the JARs

Download all JARs from the `spark3/glue-3.4.0` directory of
[spark-hive-glue-libs](https://github.com/datastrato/spark-hive-glue-libs).
The directory name refers to the Glue client version (3.4.0), not the Spark version;
these JARs are compatible with Spark 3.3, 3.4, and 3.5.

```bash
mkdir -p /opt/glue-hive-jars
curl -fsSL "https://api.github.com/repos/datastrato/spark-hive-glue-libs/contents/spark3/glue-3.4.0" \
  | jq -r '.[] | select(.name | endswith(".jar")) | .download_url' \
  | while read -r url; do
      curl -fL "$url" -o "/opt/glue-hive-jars/$(basename "$url")"
    done
```

Alternatively, download the JARs directly from the
[spark-hive-glue-libs repository](https://github.com/datastrato/spark-hive-glue-libs/tree/main/spark3/glue-3.4.0).

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

## Deploy on Amazon EMR

Amazon EMR 7.x pre-installs the patched Hive libraries and the AWS Glue Data Catalog client,
so the manual JAR setup described in [Setup](#setup) is not required.

### Prerequisites

- AWS CLI configured with an IAM user or role that has `AmazonEMRFullAccessPolicy_v2` and EC2 permissions
- EC2 instance profile (`EMR_EC2_DefaultRole`) with Glue read and S3 read/write permissions
- An S3 bucket for table storage (e.g. `s3://my-bucket/warehouse`)
- A Gravitino server reachable from the EMR cluster

### Step 1: Create an EMR cluster

```bash
aws emr create-cluster \
  --name "gravitino-glue" \
  --release-label emr-7.2.0 \
  --applications Name=Spark \
  --instance-type m5.xlarge \
  --instance-count 1 \
  --use-default-roles \
  --region <your-region> \
  --tags 'for-use-with-amazon-emr-managed-policies=true' \
  --configurations '[{"Classification":"spark-hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}]' \
  --ec2-attributes KeyName=<your-key-pair>
```

The `spark-hive-site` configuration routes Spark's Hive metastore client to the AWS Glue Data
Catalog. The `for-use-with-amazon-emr-managed-policies=true` tag is required by
`AmazonEMRFullAccessPolicy_v2`.

### Step 2: Add JARs to the Spark classpath

Follow the [Spark connector setup](spark-connector.md#usage) to obtain the
`gravitino-spark-connector-runtime-3.5` JAR. On EMR, place it in `/usr/lib/spark/jars/` instead
of a custom path — that directory is automatically on Spark's system classpath. Using `--jars` or
`--driver-class-path` is not sufficient because the Gravitino plugin classloader must find the JAR
at Spark startup.

For Iceberg table support, also add the Iceberg Spark runtime JAR to `/usr/lib/spark/jars/`:

- [iceberg-spark-runtime-3.5_2.12](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-spark-runtime-3.5_2.12) version **1.10.1**

:::warning
Use `iceberg-spark-runtime` version **1.10.1**, not 1.11.0. EMR 7.2.0 ships with AWS SDK v2
2.23.18, which does not include `RetryMode.ADAPTIVE_V2`. Iceberg 1.11.0 references this field
at runtime and throws `NoSuchFieldError: ADAPTIVE_V2`.
:::

### Step 3: Create a Glue catalog

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{
    "name": "glue_catalog",
    "type": "RELATIONAL",
    "provider": "glue",
    "properties": {
      "aws-region": "<your-region>",
      "warehouse": "s3://<your-bucket>/warehouse"
    }
  }' http://<gravitino-host>:8090/api/metalakes/<metalake>/catalogs
```

When running on EMR with an EC2 instance role, AWS credentials are picked up automatically —
no static `aws-access-key-id` or `aws-secret-access-key` is needed.

### Step 4: Submit a Spark job

:::warning
Use `spark-submit`, not `spark-sql`. The `spark-sql` CLI does not initialize the Gravitino
plugin classloader in the correct order, so registered catalogs will not appear in `SHOW CATALOGS`.
:::

Pass the Gravitino plugin configuration via `spark-submit`:

```bash
spark-submit --master yarn \
  --conf spark.plugins=org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin \
  --conf spark.sql.gravitino.uri=http://<gravitino-host>:8090 \
  --conf spark.sql.gravitino.metalake=<metalake> \
  --conf spark.sql.gravitino.enableIcebergSupport=true \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  your_job.py
```

The following PySpark snippet shows how to query Hive-format and Iceberg tables in the Glue catalog:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql("USE glue_catalog.mydb")

# Query a Hive-format table
spark.sql("SELECT * FROM employees WHERE department = 'Engineering'").show()

# Query an Iceberg table
spark.sql("SELECT * FROM orders WHERE order_ts >= DATE '2024-01-01'").show()

spark.stop()
```

## Create a Catalog

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

## SQL Example

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

-- Create an Iceberg table partitioned by day
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


## Catalog Properties

The Gravitino Spark connector maps the following catalog property names to Spark Hive/Iceberg connector configuration.

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

## S3 Storage

When using AWS S3, configure S3 credentials as global Hadoop properties via Spark configuration:

```bash
spark-submit \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.access.key=<your-access-key> \
  --conf spark.hadoop.fs.s3a.secret.key=<your-secret-key> \
  --conf spark.hadoop.fs.s3a.endpoint.region=<your-region> \
  --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3.access.key=<your-access-key> \
  --conf spark.hadoop.fs.s3.secret.key=<your-secret-key> \
  ...
```

:::note
Both `fs.s3a.*` and `fs.s3.*` must be configured. The AWS Glue Data Catalog Hive client stores table
locations using the `s3://` scheme, while Spark uses `s3a://` for S3 access. Mapping both schemes to
`S3AFileSystem` ensures Hive-format tables are readable.
:::

Alternatively, use `spark.bypass.` prefix in catalog properties to pass S3 configuration:

```json
{
  "properties": {
    "aws-region": "us-east-1",
    "warehouse": "s3a://my-bucket/warehouse",
    "spark.bypass.fs.s3a.access.key": "<your-access-key>",
    "spark.bypass.fs.s3a.secret.key": "<your-secret-key>"
  }
}
```

:::note
Ensure you have the AWS Java SDK and Hadoop AWS JARs in your Spark classpath when using S3 storage.
:::
