---
title: "Spark connector hive catalog"
slug: /spark-connector/spark-catalog-hive
keyword: spark connector hive catalog
license: "Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

With the Gravitino Spark connector, accessing data or managing metadata in Hive catalogs becomes straightforward, enabling seamless federation queries across different Hive catalogs.

## Capabilities

Supports most DDL and DML operations in SparkSQL, except such operations:

- Function operations 
- Partition operations
- View operations
- Querying UDF
- `LOAD` clause
- `CREATE TABLE LIKE` clause
- `TRUCATE TABLE` clause

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
