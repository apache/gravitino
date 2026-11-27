---
title: "Spark Connector: Hive Catalog"
slug: "/spark-connector/spark-catalog-hive"
keyword: "spark connector hive catalog"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

With the Apache Gravitino Spark connector, accessing data or managing metadata in Hive catalogs becomes straightforward, enabling seamless federation queries across different Hive catalogs.

## Capabilities

Supports most DDL and DML operations in SparkSQL, except such operations:

- Function operations (Gravitino UDFs are supported, see [Spark connector - User-defined functions](spark-connector-udf.md))
- Partition operations
- View DDL operations (`CREATE VIEW`, `DROP VIEW`, `ALTER VIEW`)
- `LOAD` clause
- `CREATE TABLE LIKE` clause
- `TRUNCATE TABLE` clause


:::info
Don't support reading and writing tables with `org.apache.hadoop.hive.serde2.OpenCSVSerde` row format.
:::

## Prerequisites

* Hive metastore 2.x
* HDFS 2.x or 3.x

## SQL Example


```sql

// Suppose hive_a is the Hive catalog name managed by Gravitino
USE hive_a;

CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;

// Create table
CREATE TABLE IF NOT EXISTS employees (
    id INT,
    name STRING,
    age INT
)
PARTITIONED BY (department STRING)
STORED AS PARQUET;
DESC TABLE EXTENDED employees;

INSERT OVERWRITE TABLE employees PARTITION(department='Engineering') VALUES (1, 'John Doe', 30), (2, 'Jane Smith', 28);
INSERT OVERWRITE TABLE employees PARTITION(department='Marketing') VALUES (3, 'Mike Brown', 32);

SELECT * FROM employees WHERE department = 'Engineering';
```


## View

Spark DDL view operations (`CREATE VIEW`, `DROP VIEW`, `ALTER VIEW`) are not supported. However, existing views stored in the Hive Metastore can be read with `SELECT`. When `SELECT` references a view name, the connector loads the view's SQL definition from Gravitino, resolves the query, and executes it.

:::caution
The current implementation materializes all view results on the Spark driver using `LocalScan`. This is suitable only for small or bounded views. Querying a large or unbounded view may exhaust driver memory and cause an OOM error.
:::

```sql
-- Assumes a view was created via Gravitino API, Flink, or Hive directly
SELECT * FROM employee_view;
```

## Dynamic Partition Pruning

Dynamic partition pruning (DPP) is supported for partitioned Hive tables that are read through the Hive SerDe reader.

| Table storage format                      | Supports DPP                                                                               |
|-------------------------------------------|--------------------------------------------------------------------------------------------|
| Text, Avro, CSV, JSON, SequenceFile, etc. | Yes                                                                                        |
| Parquet                                   | Only when `spark.sql.kyuubi.hive.connector.read.convertMetastoreParquet` is set to `false` |
| ORC                                       | Only when `spark.sql.kyuubi.hive.connector.read.convertMetastoreOrc` is set to `false`     |

Both `convertMetastore*` options default to `true`, which reads Parquet and ORC tables with Spark's native readers. The native readers are faster but don't support DPP. Setting an option to `false` enables DPP for that format, but reads the data with the Hive SerDe reader instead.

```sql
SET spark.sql.kyuubi.hive.connector.read.convertMetastoreParquet=false;
```

DPP only prunes partitions, and the query must also meet Spark's own conditions for dynamic partition pruning, see `spark.sql.optimizer.dynamicPartitionPruning.enabled` in [Spark configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration).

## Catalog Properties

Gravitino spark connector will transform below property names which are defined in catalog properties to Spark Hive connector configuration.

| Property name in Gravitino catalog properties | Spark Hive connector configuration | Description                |
|-----------------------------------------------|------------------------------------|----------------------------|
| `metastore.uris`                              | `hive.metastore.uris`              | Hive metastore uri address |

Gravitino catalog property names with the prefix `spark.bypass.` are passed to Spark Hive connector. For example, using `spark.bypass.hive.exec.dynamic.partition.mode` to pass the `hive.exec.dynamic.partition.mode` to the Spark Hive connector.


:::caution
When using the `spark-sql` shell client, you must explicitly set the `spark.bypass.spark.sql.hive.metastore.jars` in the Gravitino Hive catalog properties. Replace the default `builtin` value with the appropriate setting for your setup.
:::


## Storage

### S3

Refer to [Hive catalog with s3](../hive-catalog-with-cloud-storage.md) to set up a Hive catalog with s3 storage. To query the data stored in S3, Spark needs S3 credentials: either add them to the Spark configuration using `spark.sql.catalog.${hive_catalog_name}.fs.s3a.access.key` and `spark.sql.catalog.${hive_catalog_name}.fs.s3a.secret.key`, or store them on the Gravitino catalog (see below). Additionally, download [hadoop aws jar](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws), the AWS SDK jar that matches the Hadoop version of Spark ([aws java sdk v1 jar](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bundle) for Hadoop 3.3, [aws java sdk v2 jar](https://mvnrepository.com/artifact/software.amazon.awssdk/bundle) for Hadoop 3.4 and later) and place them in the classpath of Spark.

## Credentials stored on the Gravitino catalog

The catalog can store object-store credentials itself. The connector passes the credentials the Gravitino catalog vends to the Spark catalog, under the keys the object stores read:

- S3 credentials (`s3-access-key-id` and `s3-secret-access-key`) are written as `fs.s3a.access.key` and `fs.s3a.secret.key`.
- The Azure account key is written as `fs.azure.account.key.<account name>.dfs.core.windows.net`, the key hadoop-azure reads for table locations on the public Azure Data Lake endpoint. For table locations elsewhere, such as `wasb://` blob locations or other Azure clouds, hadoop-azure looks the key up under another name and does not read the one written.
- OSS credentials are written as `fs.oss.accessKeyId` with `fs.oss.accessKeySecret`. Catalog option keys are passed on in lower case, which these two names do not survive, so OSS credentials are written to the configuration of the Spark catalog itself rather than to its options.

A vended key is written only where the key is not configured:

- An S3 or OSS pair is taken as a unit: when either half is configured, neither vended half is written, since one vended half next to a configured one would not authenticate.
- A blank value does not count as configured: for S3 and Azure its entry is removed, for OSS it is ignored.
- For the Azure account key, a key set for all accounts (`fs.azure.account.key`) counts as configured as well.
- For OSS, the vended pair is written only where no OSS key is configured at all; otherwise the keys given as catalog options or `spark.bypass.*` properties are used, each only where the session does not configure it.

A key you set yourself takes precedence over a vended one, in any of these places: through the `spark.sql.catalog.${hive_catalog_name}.fs.s3a.*` options above, through a `spark.bypass.fs.s3a.*` catalog property, session-wide through `spark.hadoop.fs.s3a.*`, or on the session with `spark.conf().set("fs.s3a.access.key", ...)`. The session settings are read when the catalog is initialized, the first time the session uses it: a key set on the session afterwards does not count, and the vended key takes precedence over it in this catalog. A key kept in a Hadoop credential provider is not recognized here, but the filesystem still asks the provider first. An Azure account that authenticates with SAS or OAuth does not use its account key, so the vended key has no effect there.

:::warning
OSS credentials given as catalog options or `spark.bypass.*` properties had no effect before, because their names did not survive the way to the catalog; they take effect now and outrank the vended pair, unless the session configures that key.

Where no key is set, the vended keys are used like a key pair of your own, and the credential provider chain of S3A decides which credentials it picks: with the default chain of Hadoop 3.3 a key pair comes before environment variables and the instance profile, with the default chain of Hadoop 3.4 it comes after them. A catalog that stores S3 keys therefore makes Spark use them on Hadoop 3.3 even where the environment offers credentials of its own, such as an instance profile. To keep other credentials instead, set `fs.s3a.aws.credentials.provider` to the providers to use, for example `org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider`: S3A then consults only those.
:::
