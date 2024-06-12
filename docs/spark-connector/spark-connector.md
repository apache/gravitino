---
title: "Gravitino Spark connector"
slug: /spark-connector/spark-connector
keyword: spark connector federation query 
license: "Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

## Overview

The Gravitino Spark connector leverages the Spark DataSourceV2 interface to facilitate the management of diverse catalogs under Gravitino. This capability allows users to perform federation queries, accessing data from various catalogs through a unified interface and consistent access control.

## Capabilities

1. Supports [Hive catalog](spark-catalog-hive.md) and [Iceberg catalog](spark-catalog-iceberg.md).
2. Supports federation query.
3. Supports most DDL and DML SQLs.

## Requirement

* Spark 3.3 or 3.4 or 3.5
* Scala 2.12 or 2.13
* JDK 8 or 11 or 17

:::info
Gravitino Spark connector doesn't support Scala 2.13 for Spark3.3.
:::

## How to use it

1. [Build](../how-to-build.md) or [download](https://mvnrepository.com/artifact/com.datastrato.gravitino/spark-connector-runtime) the Gravitino spark connector jar, and place it to the classpath of Spark.
2. Configure the Spark session to use the Gravitino spark connector.

| Property                                 | Type   | Default Value | Description                                                                                         | Required | Since Version |
|------------------------------------------|--------|---------------|-----------------------------------------------------------------------------------------------------|----------|---------------|
| spark.plugins                            | string | (none)        | Gravitino spark plugin name, `com.datastrato.gravitino.spark.connector.plugin.GravitinoSparkPlugin` | Yes      | 0.5.0         |
| spark.sql.gravitino.metalake             | string | (none)        | The metalake name that spark connector used to request to Gravitino.                                | Yes      | 0.5.0         |
| spark.sql.gravitino.uri                  | string | (none)        | The uri of Gravitino server address.                                                                | Yes      | 0.5.0         |
| spark.sql.gravitino.enableIcebergSupport | string | `false`       | Set to `true` to use Iceberg catalog.                                                               | No       | 0.5.1         |

```shell
./bin/spark-sql -v \
--conf spark.plugins="com.datastrato.gravitino.spark.connector.plugin.GravitinoSparkPlugin" \
--conf spark.sql.gravitino.uri=http://127.0.0.1:8090 \
--conf spark.sql.gravitino.metalake=test \
--conf spark.sql.gravitino.enableIcebergSupport=true \
--conf spark.sql.warehouse.dir=hdfs://127.0.0.1:9000/user/hive/warehouse-hive
```

3. [Download](https://iceberg.apache.org/releases/) corresponding runtime jars and place it to the classpath of Spark if using Iceberg catalog.

4. Execute the Spark SQL query. 

Suppose there are two catalogs in the metalake `test`, `hive` for Hive catalog and `iceberg` for Iceberg catalog. 

```sql
// use hive catalog
USE hive;
CREATE DATABASE db;
USE db;
CREATE TABLE hive_students (id INT, name STRING);
INSERT INTO hive_students VALUES (1, 'Alice'), (2, 'Bob');

// use Iceberg catalog
USE iceberg;
USE db;
CREATE TABLE IF NOT EXISTS iceberg_scores (id INT, score INT) USING iceberg;
INSERT INTO iceberg_scores VALUES (1, 95), (2, 88);

// execute federation query between hive table and iceberg table
SELECT hs.name, is.score FROM hive.db.hive_students hs JOIN iceberg_scores is ON hs.id = is.id;
```

:::info
The command `SHOW CATALOGS` will only display the Spark default catalog, named spark_catalog, due to limitations within the Spark catalog manager. It does not list the catalogs present in the metalake. However, after explicitly using the `USE` command with a specific catalog name, that catalog name then becomes visible in the output of `SHOW CATALOGS`.
:::

## Datatype mapping

Gravitino spark connector support the following datatype mapping between Spark and Gravitino.

| Spark Data Type | Gravitino Data Type | Since Version |
|-----------------|---------------------|---------------|
| `BooleanType`   | `boolean`           | 0.5.0         |
| `ByteType`      | `byte`              | 0.5.0         |
| `ShortType`     | `short`             | 0.5.0         |
| `IntegerType`   | `integer`           | 0.5.0         |
| `LongType`      | `long`              | 0.5.0         |
| `FloatType`     | `float`             | 0.5.0         |
| `DoubleType`    | `double`            | 0.5.0         |
| `DecimalType`   | `decimal`           | 0.5.0         |
| `StringType`    | `string`            | 0.5.0         |
| `CharType`      | `char`              | 0.5.0         |
| `VarcharType`   | `varchar`           | 0.5.0         |
| `TimestampType` | `timestamp`         | 0.5.0         |
| `TimestampType` | `timestamp`         | 0.5.0         |
| `DateType`      | `date`              | 0.5.0         |
| `BinaryType`    | `binary`            | 0.5.0         |
| `ArrayType`     | `array`             | 0.5.0         |
| `MapType`       | `map`               | 0.5.0         |
| `StructType`    | `struct`            | 0.5.0         |
