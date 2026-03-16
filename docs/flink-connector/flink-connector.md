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
2. Supports [Iceberg catalog](flink-catalog-iceberg.md)
3. Supports [Paimon catalog](flink-catalog-paimon.md)
4. Supports [Jdbc catalog](flink-catalog-jdbc.md)
5. Supports most DDL and DML SQLs.

## Requirements

* Flink 1.18.x, 1.19.x, or 1.20.x
* Scala 2.12
* JDK 8, 11 or 17

## How to use it

1. Build or download the Gravitino Flink connector runtime jar that matches your Flink minor version, and place it in the Flink classpath.

   | Flink version | Runtime module | Runtime jar |
   |---------------|----------------|-------------|
   | 1.18.x | `:flink-connector:flink-runtime-1.18` | `gravitino-flink-connector-runtime-1.18_2.12-${gravitino-version}.jar` |
   | 1.19.x | `:flink-connector:flink-runtime-1.19` | `gravitino-flink-connector-runtime-1.19_2.12-${gravitino-version}.jar` |
   | 1.20.x | `:flink-connector:flink-runtime-1.20` | `gravitino-flink-connector-runtime-1.20_2.12-${gravitino-version}.jar` |

   If you build from source, use one of the following commands:

   ```shell
   ./gradlew :flink-connector:flink-runtime-1.18:shadowJar
   ./gradlew :flink-connector:flink-runtime-1.19:shadowJar
   ./gradlew :flink-connector:flink-runtime-1.20:shadowJar
   ```

2. Configure the Flink configuration to use the Gravitino flink connector.

| Property                                         | Type   | Default Value     | Description                                                          | Required | Since Version    |
|--------------------------------------------------|--------|-------------------|----------------------------------------------------------------------|----------|------------------|
| table.catalog-store.kind                         | string | generic_in_memory | The Catalog Store name, it should set to `gravitino`.                | Yes      | 0.6.0-incubating |
| table.catalog-store.gravitino.gravitino.metalake | string | (none)            | The metalake name that flink connector used to request to Gravitino. | Yes      | 0.6.0-incubating |
| table.catalog-store.gravitino.gravitino.uri      | string | (none)            | The uri of Gravitino server address.                                 | Yes      | 0.6.0-incubating |
| table.catalog-store.gravitino.gravitino.client.  | string | (none)            | The configuration key prefix for the Gravitino client config.        | No       | 1.0.0            |

To configure the Gravitino client, use properties prefixed with `table.catalog-store.gravitino.gravitino.client.`. These properties will be passed to the Gravitino client after removing the `table.catalog-store.gravitino.` prefix.

**Example:** Setting `table.catalog-store.gravitino.gravitino.client.socketTimeoutMs` is equivalent to setting `gravitino.client.socketTimeoutMs` for the Gravitino client.

**Note:** Invalid configuration properties will result in exceptions. Please see [Gravitino Java client configurations](../how-to-use-gravitino-client.md#gravitino-java-client-configuration) for more support client configuration.

Set the flink configuration in flink-conf.yaml.
```yaml
table.catalog-store.kind: gravitino
table.catalog-store.gravitino.gravitino.metalake: metalake_demo
table.catalog-store.gravitino.gravitino.uri: http://localhost:8090
table.catalog-store.gravitino.gravitino.client.socketTimeoutMs: 60000
table.catalog-store.gravitino.gravitino.client.connectionTimeoutMs: 60000
```
Or you can set the flink configuration in the `TableEnvironment`.
```java
final Configuration configuration = new Configuration();
configuration.setString("table.catalog-store.kind", "gravitino");
configuration.setString("table.catalog-store.gravitino.gravitino.metalake", "metalake_demo");
configuration.setString("table.catalog-store.gravitino.gravitino.uri", "http://localhost:8090");
configuration.setString("table.catalog-store.gravitino.gravitino.client.socketTimeoutMs", "60000");
configuration.setString("table.catalog-store.gravitino.gravitino.client.connectionTimeoutMs", "60000");
EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance().withConfiguration(configuration);
TableEnvironment tableEnv = TableEnvironment.create(builder.inBatchMode().build());
```

3. Add the provider-specific jar files that match the same Flink minor version to Flink's classpath.

To run Flink with the Gravitino connector and access a provider such as Hive, Iceberg, JDBC, or Paimon, you may need to add extra provider jars to Flink's classpath. Always use provider jars that match the same Flink minor version as the selected Gravitino runtime jar. See the provider-specific documents for concrete jar combinations and refer to the [Flink document](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/hive/overview/#dependencies) for more information.

4. Execute the Flink SQL query.

Suppose there is only one hive catalog with the name `catalog_hive` in the metalake `metalake_demo`.

```sql
// use hive catalog
USE CATALOG catalog_hive;
CREATE DATABASE db;
USE db;
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
CREATE TABLE hive_students (id INT, name STRING);
INSERT INTO hive_students VALUES (1, 'Alice'), (2, 'Bob');
SELECT * FROM hive_students;
```

## Catalog naming restrictions

:::caution
When creating catalogs that will be used with the Flink connector, the catalog name **cannot start with a number**. This is a Flink limitation. For example:
- ✅ Valid: `catalog_hive`, `hive_catalog`, `my_catalog_1`
- ❌ Invalid: `1_catalog`, `123catalog`, `2hive`

If you create a catalog with a name starting with a number, it will not be accessible from Flink.
:::

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
