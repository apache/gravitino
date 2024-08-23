---
title: "Apache Gravitino Flink connector"
slug: /flink-connector/flink-connector
keyword: flink connector federation query 
license: "This software is licensed under the Apache License version 2."
---

## Overview

The Apache Gravitino Flink connector implements the [Catalog Store](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/table/catalogs/#catalog-store) to manage the catalogs under Gravitino. 
This capability allows users to perform federation queries, accessing data from various catalogs through a unified interface and consistent access control.

## Capabilities

1. Supports [Hive catalog](flink-catalog-hive.md)
2. Supports most DDL and DML SQLs.

## Requirement

* Flink 1.18
* Scala 2.12
* JDK 8 or 11 or 17

## How to use it

1. [Build](../how-to-build.md) or [download](https://mvnrepository.com/artifact/org.apache.gravitino/flink-connector) the Gravitino flink connector jar, and place it to the classpath of Flink.
2. Configure the Flink configuration to use the Gravitino flink connector.

| Property                                         | Type   | Default Value     | Description                                                          | Required | Since Version |
|--------------------------------------------------|--------|-------------------|----------------------------------------------------------------------|----------|---------------|
| table.catalog-store.kind                         | string | generic_in_memory | The Catalog Store name, it should set to `gravitino`.                | Yes      | 0.6.0         |
| table.catalog-store.gravitino.gravitino.metalake | string | (none)            | The metalake name that flink connector used to request to Gravitino. | Yes      | 0.6.0         |
| table.catalog-store.gravitino.gravitino.uri      | string | (none)            | The uri of Gravitino server address.                                 | Yes      | 0.6.0         |

Set the flink configuration in flink-conf.yaml.
```yaml
table.catalog-store.kind=gravitino
table.catalog-store.gravitino.gravitino.metalake=test
table.catalog-store.gravitino.gravitino.uri=http://localhost:8080
```
Or you can set the flink configuration in the `TableEnvironment`.
```java
final Configuration configuration = new Configuration();
configuration.setString("table.catalog-store.kind", "gravitino");
configuration.setString("table.catalog-store.gravitino.gravitino.metalake", "test");
configuration.setString("table.catalog-store.gravitino.gravitino.uri", "http://localhost:8080");
EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance().withConfiguration(configuration);
TableEnvironment tableEnv = TableEnvironment.create(builder.inBatchMode().build());
```

3. Execute the Flink SQL query. 

Suppose there is only one hive catalog with the name `hive` in the metalake `test`.

```sql
// use hive catalog
USE hive;
CREATE DATABASE db;
USE db;
CREATE TABLE hive_students (id INT, name STRING);
INSERT INTO hive_students VALUES (1, 'Alice'), (2, 'Bob');
SELECT * FROM hive_students;
```

## Datatype mapping

Gravitino flink connector support the following datatype mapping between Flink and Gravitino.

| Flink Type                       | Gravitino Type                | Since Version |
|----------------------------------|-------------------------------|---------------|
| `array`                          | `array`                       | 0.6.0         |
| `bigint`                         | `long`                        | 0.6.0         |
| `binary`                         | `fixed`                       | 0.6.0         |
| `boolean`                        | `boolean`                     | 0.6.0         |
| `char`                           | `char`                        | 0.6.0         |
| `date`                           | `date`                        | 0.6.0         |
| `decimal`                        | `decimal`                     | 0.6.0         |
| `double`                         | `double`                      | 0.6.0         |
| `float`                          | `float`                       | 0.6.0         |
| `integer`                        | `integer`                     | 0.6.0         |
| `map`                            | `map`                         | 0.6.0         |
| `null`                           | `null`                        | 0.6.0         |
| `row`                            | `struct`                      | 0.6.0         |
| `smallint`                       | `short`                       | 0.6.0         |
| `time`                           | `time`                        | 0.6.0         |
| `timestamp`                      | `timestamp without time zone` | 0.6.0         |
| `timestamp without time zone`    | `timestamp without time zone` | 0.6.0         |
| `timestamp with time zone`       | `timestamp with time zone`    | 0.6.0         |
| `timestamp with local time zone` | `timestamp with time zone`    | 0.6.0         |
| `timestamp_ltz`                  | `timestamp with time zone`    | 0.6.0         |
| `tinyint`                        | `byte`                        | 0.6.0         |
| `varbinary`                      | `binary`                      | 0.6.0         |
| `varchar`                        | `string`                      | 0.6.0         |
