---
title: "Spark connector hive catalog"
slug: /spark-connector/spark-catalog-hive
keyword: spark connector hive catalog
license: "This software is licensed under the Apache License version 2."
---

The Apache Gravitino Spark connector enables seamless federation queries
across different Hive catalogs.
Accessing or managing metadata in Hive catalogs is made straightforward.

## Capabilities

Supports most DDL and DML operations in SparkSQL, except for operations below:

- Function operations 
- Partition operations
- View operations
- Querying UDF
- `LOAD` clause
- `CREATE TABLE LIKE` clause
- `TRUCATE TABLE` clause

:::info
The connector doesn't support reading or writing tables
with `org.apache.hadoop.hive.serde2.OpenCSVSerde` row format.
:::

## Requirement

* Hive metastore 2.x
* HDFS 2.x or 3.x

## SQL example

```sql
-- Suppose hive_a is a Hive catalog managed by Gravitino
USE hive_a;

CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;

-- Create a table
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
INSERT OVERWRITE TABLE employees PARTITION(department='Marketing')
    VALUES (3, 'Mike Brown', 32);

SELECT * FROM employees WHERE department = 'Engineering';
```

## Catalog properties

The Gravitino Spark connector transforms the following properties defined in catalog properties
into Spark Hive connector configurations.

<table>
<thead>
<tr>
  <th>Gravitino catalog property name</th>
  <th>Spark Hive connector configuration</th>
  <th>Description</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>metastore.uris</tt></td>
  <td><tt>hive.metastore.uris</tt></td>
  <td>The URI for the Hive metastore</td>
  <td>`0.5.0`</td>
</tr>
</tbody>
</table>

Gravitino catalog property names prefixed with `spark.bypass.` are passed to the Spark Hive connector.
For example, the property `spark.bypass.hive.exec.dynamic.partition.mode`
is translated to `hive.exec.dynamic.partition.mode` for the Spark Hive connector.

:::caution
When using the `spark-sql` shell, you must set `spark.bypass.spark.sql.hive.metastore.jars`
explicitly in the Gravitino Hive catalog properties.
You need to adapt the default values to your environment.
:::

## Storage

### S3

Please refer to [Hive catalog with S3](../../catalogs/relational/hive/cloud-storage.md)
for a guide on setting up a Hive catalog with S3 storage.
To query data stored in S3, you need to add a S3 secret to the Spark configuration
using `spark.sql.catalog.<hive_catalog_name>.fs.s3a.access.key` and
`spark.sql.catalog.<hive_catalog_name>.fs.s3a.secret.key`.
In addtion to this, you need to download the [Hadoop AWS JAR](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws),
[AWS Java SDK JAR](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bundle)
and place them into the class path for Spark.

