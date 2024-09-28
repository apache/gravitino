---
title: "Spark connector hive catalog"
slug: /spark-connector/spark-catalog-hive
keyword: spark connector hive catalog
license: "This software is licensed under the Apache License version 2."
---

With the Apache Gravitino Spark connector, accessing data or managing metadata in Hive catalogs becomes straightforward, enabling seamless federation queries across different Hive catalogs.

## Capabilities

Supports most DDL and DML operations in SparkSQL, except such operations:

- Function operations 
- Partition operations
- View operations
- Querying UDF
- `LOAD` clause
- `CREATE TABLE LIKE` clause
- `TRUCATE TABLE` clause


:::info
Don't support reading and writing tables with `org.apache.hadoop.hive.serde2.OpenCSVSerde` row format.
:::

## Requirement

* Hive metastore 2.x
* HDFS 2.x or 3.x

## SQL example


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


## Catalog properties

Gravitino spark connector will transform below property names which are defined in catalog properties to Spark Hive connector configuration.

| Property name in Gravitino catalog properties | Spark Hive connector configuration | Description                | Since Version |
|-----------------------------------------------|------------------------------------|----------------------------|---------------|
| `metastore.uris`                              | `hive.metastore.uris`              | Hive metastore uri address | 0.5.0         |

Gravitino catalog property names with the prefix `spark.bypass.` are passed to Spark Hive connector. For example, using `spark.bypass.hive.exec.dynamic.partition.mode` to pass the `hive.exec.dynamic.partition.mode` to the Spark Hive connector.


:::caution
When using the `spark-sql` shell client, you must explicitly set the `spark.bypass.spark.sql.hive.metastore.jars` in the Gravitino Hive catalog properties. Replace the default `builtin` value with the appropriate setting for your setup.
:::