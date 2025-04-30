---
title: "Apache Gravitino Flink connector"
slug: /flink-connector/flink-connector
keyword: flink connector federation query 
license: "This software is licensed under the Apache License version 2."
---

## Overview

The Apache Gravitino Flink connector implements the
[Catalog Store](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/table/catalogs/#catalog-store)
spec for managing the catalogs in Gravitino.
This allows users to perform federation queries, accessing data from various catalogs
through a unified interface and consistent access control.

## Capabilities

1. Supports [Hive catalog](./flink-catalog-hive.md)
1. Supports [Iceberg catalog](./flink-catalog-iceberg.md)
1. Supports [JDBC catalog](./flink-catalog-jdbc.md)
1. Supports [Paimon catalog](./flink-catalog-paimon.md)
1. Supports most DDL and DML SQLs.

## Requirement

* Flink 1.18
* Scala 2.12
* JDK 8 or 11 or 17

## How to use it

1. [Build](../../develop/how-to-build.md) or
   [download](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-flink-connector-runtime-1.18)
   the Gravitino Flink connector runtime JAR files, and place them
   into the classs path for Flink.

1. Configure the Flink configuration to use the Gravitino flink connector.

   <table>
   <thead>
   <tr>
     <th>Property</th>
     <th>Type</th>
     <th>Default Value</th>
     <th>Description</th>
     <th>Required</th>
     <th>Since version</th>
   </tr>
   </thead>
   <tbody>
   <tr>
     <td><tt>table.catalog-store.kind</tt></td>
     <td><tt>string</tt></td>
     <td>`generic_in_memory`</td>
     <td>The catalog store name. Should be set to `gravitino`.</td>
     <td>Yes</td>
     <td>`0.6.0-incubating`</td>
   </tr>
   <tr>
     <td><tt>table.catalog-store.gravitino.gravitino.metalake</tt></td>
     <td><tt>string</tt></td>
     <td>(none)</td>
     <td>The metalake name that the Flink connector use for accessing Gravitino.</td>
     <td>Yes</td>
     <td>`0.6.0-incubating`</td>
   </tr>
   <tr>
     <td><tt>table.catalog-store.gravitino.gravitino.uri</tt></td>
     <td><tt>string</tt></td>
     <td>(none)</td>
     <td>The URI of the Gravitino server.</td>
     <td>Yes</td>
     <td>`0.6.0-incubating`</td>
   </tr>
   </tbody>
   </table>

   Set the Flink configuration in `flink-conf.yaml`, as shown below:
   
   ```yaml
   table.catalog-store.kind: gravitino
   table.catalog-store.gravitino.gravitino.metalake: test
   table.catalog-store.gravitino.gravitino.uri: http://localhost:8090
   ```
   
   Or you can set the Flink configuration in the `TableEnvironment`,
   when invoking the Java API:
   
   ```java
   final Configuration configuration = new Configuration();
   configuration.setString("table.catalog-store.kind", "gravitino");
   configuration.setString("table.catalog-store.gravitino.gravitino.metalake", "test");
   configuration.setString("table.catalog-store.gravitino.gravitino.uri", "http://localhost:8090");
   EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance().withConfiguration(configuration);
   TableEnvironment tableEnv = TableEnvironment.create(builder.inBatchMode().build());
   ```

1. Execute Flink SQL queries.

   Suppose there is only one Hive catalog named `hive` in the metalake `mymetalake`.
   
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

| Flink Type                       | Gravitino Type                | Since Version    |
|----------------------------------|-------------------------------|------------------|
| `array`                          | `list`                        | 0.6.0-incubating |
| `bigint`                         | `long`                        | 0.6.0-incubating |
| `binary`                         | `fixed`                       | 0.6.0-incubating |
| `boolean`                        | `boolean`                     | 0.6.0-incubating |
| `char`                           | `char`                        | 0.6.0-incubating |
| `date`                           | `date`                        | 0.6.0-incubating |
| `decimal`                        | `decimal`                     | 0.6.0-incubating |
| `double`                         | `double`                      | 0.6.0-incubating |
| `float`                          | `float`                       | 0.6.0-incubating |
| `integer`                        | `integer`                     | 0.6.0-incubating |
| `map`                            | `map`                         | 0.6.0-incubating |
| `null`                           | `null`                        | 0.6.0-incubating |
| `row`                            | `struct`                      | 0.6.0-incubating |
| `smallint`                       | `short`                       | 0.6.0-incubating |
| `time`                           | `time`                        | 0.6.0-incubating |
| `timestamp`                      | `timestamp without time zone` | 0.6.0-incubating |
| `timestamp without time zone`    | `timestamp without time zone` | 0.6.0-incubating |
| `timestamp with time zone`       | `timestamp with time zone`    | 0.6.0-incubating |
| `timestamp with local time zone` | `timestamp with time zone`    | 0.6.0-incubating |
| `timestamp_ltz`                  | `timestamp with time zone`    | 0.6.0-incubating |
| `tinyint`                        | `byte`                        | 0.6.0-incubating |
| `varbinary`                      | `binary`                      | 0.6.0-incubating |
| `varchar`                        | `string`                      | 0.6.0-incubating |

