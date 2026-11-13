---
title: "Flink Connector"
slug: "/flink-connector/flink-connector"
keyword: "flink connector federation query"
license: "This software is licensed under the Apache License version 2."
---

## Overview

The Apache Gravitino Flink connector implements the [Catalog Store](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/catalogs/#catalog-store) to manage the catalogs under Gravitino.
This capability allows users to perform federation queries, accessing data from various catalogs through a unified interface and consistent access control.

The connector is published as a version-specific runtime JAR for each supported Flink minor version. Internally, Gravitino uses shared connector logic with version-specific catalog and factory entry points so that each runtime artifact matches the Flink APIs it runs against.

## Capabilities

1. Supports [Hive catalog](flink-catalog-hive.md) (Flink 1.19 and 1.20 only; not yet available on
   Flink 2.x — see [Prerequisites](#prerequisites))
2. Supports [Iceberg catalog](flink-catalog-iceberg.md)
3. Supports [Paimon catalog](flink-catalog-paimon.md)
4. Supports [Jdbc catalog](flink-catalog-jdbc.md)
5. Supports most DDL and DML SQLs.

## Prerequisites

* Scala 2.12 (Flink 1.19 and 1.20 only; Flink 2.x removed the Scala APIs)
* Flink 1.19, 1.20, 2.1, or 2.2
* JDK 8, 11 or 17 for Flink 1.x; JDK 17 for Flink 2.x

On Flink 2.x, the Hive catalog is not supported: `flink-connector-hive` has not published a
release targeting the Flink 2.x line yet (tracked upstream as
[FLINK-38031](https://issues.apache.org/jira/browse/FLINK-38031)). Iceberg, Paimon, and JDBC
catalogs are supported on Flink 2.x.

## Usage

1. [Build](../how-to-build.md) or download the Gravitino Flink connector runtime JAR that matches your Flink minor version, and place it in the classpath of Flink.

| Flink version | Runtime artifact                                                       |
|---------------|------------------------------------------------------------------------|
| 1.19          | `gravitino-flink-connector-runtime-1.19_2.12-${gravitino-version}.jar` |
| 1.20          | `gravitino-flink-connector-runtime-1.20_2.12-${gravitino-version}.jar` |
| 2.1           | `gravitino-flink-connector-runtime-2.1-${gravitino-version}.jar`       |
| 2.2           | `gravitino-flink-connector-runtime-2.2-${gravitino-version}.jar`       |

Do not mix runtime JARs from different Flink minor versions in the same Flink deployment.

2. Configure the Flink configuration to use the Gravitino flink connector.

| Property                                                            | Type    | Default Value     | Description                                                                           | Required |
|---------------------------------------------------------------------|---------|-------------------|---------------------------------------------------------------------------------------|----------|
| table.catalog-store.kind                                            | string  | generic_in_memory | The Catalog Store name, it should set to `gravitino`.                                 | Yes      |
| table.catalog-store.gravitino.gravitino.metalake                    | string  | (none)            | The metalake name that flink connector used to request to Gravitino.                  | Yes      |
| table.catalog-store.gravitino.gravitino.uri                         | string  | (none)            | The uri of Gravitino server address.                                                  | Yes      |
| table.catalog-store.gravitino.gravitino.enableSessionCatalogSupport | boolean | false             | Whether to enable support for Flink's session catalog in the Gravitino catalog store. | No       |
| table.catalog-store.gravitino.gravitino.excludedCatalogs            | list    | (none)            | Semicolon-separated catalog names that are never resolved against Gravitino in this session. Only valid when `enableSessionCatalogSupport` is `true`. See [Use a session catalog with the name of a Gravitino catalog](#use-a-session-catalog-with-the-name-of-a-gravitino-catalog). | No       |
| table.catalog-store.gravitino.gravitino.client.                     | string  | (none)            | The configuration key prefix for the Gravitino client config.                         | No       |

When `table.catalog-store.gravitino.gravitino.enableSessionCatalogSupport` is set to `true`, Gravitino uses `GravitinoSessionCatalogStore`, which combines a `GravitinoCatalogStore` (backed by the Gravitino server) with an in-memory store to support Flink's session catalog. When `false` (the default), only `GravitinoCatalogStore` is used.

When session catalog support is enabled, the following behaviors apply:

- **CREATE CATALOG**: Gravitino-managed catalogs (e.g. `gravitino-hive`, `gravitino-iceberg`) are persisted to the Gravitino server; non-Gravitino-managed catalogs (e.g. `hive`, `jdbc`, or any custom connector type) are stored in the in-memory store only.
- **GET / USE CATALOG**: The in-memory store is checked first. If the catalog is not found there, it is retrieved from the Gravitino server.
- **DROP CATALOG**: The in-memory store is checked first. If the catalog exists there it is removed from memory; otherwise it is removed from the Gravitino server.
- **SHOW / LIST CATALOGS**: Returns the combined set of catalogs from both the in-memory store and the Gravitino server.
- **Session scope**: Catalogs stored only in memory are session-scoped and will not survive when Flink restarts.
- **Name conflict**: `CREATE CATALOG` fails if a catalog with the same name already exists in either store, including a Gravitino catalog created by another client. If a Gravitino catalog with the same name as an existing in-memory catalog is created later, the in-memory entry takes precedence in this session. To create a session catalog with the name of an existing Gravitino catalog, use `excludedCatalogs`.

To configure the Gravitino client, use properties prefixed with `table.catalog-store.gravitino.gravitino.client.`. These properties will be passed to the Gravitino client after removing the `table.catalog-store.gravitino.` prefix.

**Example:** Setting `table.catalog-store.gravitino.gravitino.client.socketTimeoutMs` is equivalent to setting `gravitino.client.socketTimeoutMs` for the Gravitino client.

**Note:** Invalid configuration properties will result in exceptions. Please see [Gravitino Java client configurations](../how-to-use-gravitino-client.md#java-client-configuration) for more support client configuration.

Set the flink configuration in flink-conf.yaml.
```yaml
table.catalog-store.kind: gravitino
table.catalog-store.gravitino.gravitino.metalake: metalake_demo
table.catalog-store.gravitino.gravitino.uri: http://localhost:8090
table.catalog-store.gravitino.gravitino.enableSessionCatalogSupport: true
table.catalog-store.gravitino.gravitino.client.socketTimeoutMs: 60000
table.catalog-store.gravitino.gravitino.client.connectionTimeoutMs: 60000
```
Or you can set the flink configuration in the `TableEnvironment`.
```java
final Configuration configuration = new Configuration();
configuration.setString("table.catalog-store.kind", "gravitino");
configuration.setString("table.catalog-store.gravitino.gravitino.metalake", "metalake_demo");
configuration.setString("table.catalog-store.gravitino.gravitino.uri", "http://localhost:8090");
configuration.setBoolean("table.catalog-store.gravitino.gravitino.enableSessionCatalogSupport", true);
configuration.setString("table.catalog-store.gravitino.gravitino.client.socketTimeoutMs", "60000");
configuration.setString("table.catalog-store.gravitino.gravitino.client.connectionTimeoutMs", "60000");
EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance().withConfiguration(configuration);
TableEnvironment tableEnv = TableEnvironment.create(builder.inBatchMode().build());
```

3. Add necessary jar files to Flink's classpath.

To run Flink with Gravitino connector and then access the data source like Hive, you may need to put additional jars to Flink's classpath. You can refer to the [Flink document](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/hive/overview/#dependencies) for more information.

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

## Use a session catalog with the name of a Gravitino catalog

Existing Flink jobs may create a session catalog, such as a native Paimon catalog, with the same name as a catalog managed by Gravitino. To keep the job's own catalog configuration, add the catalog name to `table.catalog-store.gravitino.gravitino.excludedCatalogs`. Separate multiple names with semicolons.

```yaml
table.catalog-store.gravitino.gravitino.enableSessionCatalogSupport: true
table.catalog-store.gravitino.gravitino.excludedCatalogs: paimon_catalog
```

For each excluded name, the Flink session behaves as follows:

- The Gravitino catalog with that name is not visible: `SHOW CATALOGS`, `USE CATALOG`, and table lookups never resolve it from Gravitino.
- `CREATE CATALOG` with a non-Gravitino-managed type, such as `paimon` or `hive`, creates a session catalog in memory. Creating a second catalog with the same name still fails.
- `ALTER CATALOG` and `DROP CATALOG` only affect the session catalog. `DROP CATALOG` never drops the Gravitino catalog.
- Creating a Gravitino-managed catalog type, such as `gravitino-paimon`, fails, because it would be persisted to Gravitino.

Exclusion only affects the current Flink session. The Gravitino catalog and its metadata are not modified, and other sessions still see it. Names are case-sensitive.

:::caution
A session catalog uses its own connector configuration and authentication. Gravitino does not authorize access through a session catalog, regardless of whether its name is excluded. Excluding a name does not grant any additional access, but make sure that session catalogs are permitted by your platform's access policy.
:::

## Catalog Naming Restrictions

:::caution
When creating catalogs that will be used with the Flink connector, the catalog name **cannot start with a number**. This is a Flink limitation. For example:
- ✅ Valid: `catalog_hive`, `hive_catalog`, `my_catalog_1`
- ❌ Invalid: `1_catalog`, `123catalog`, `2hive`

If you create a catalog with a name starting with a number, it will not be accessible from Flink.
:::

## Datatype Mapping

Gravitino flink connector support the following datatype mapping between Flink and Gravitino.

| Flink Type                       | Gravitino Type                |
|----------------------------------|-------------------------------|
| `array`                          | `list`                        |
| `bigint`                         | `long`                        |
| `binary`                         | `fixed`                       |
| `boolean`                        | `boolean`                     |
| `char`                           | `char`                        |
| `date`                           | `date`                        |
| `decimal`                        | `decimal`                     |
| `double`                         | `double`                      |
| `float`                          | `float`                       |
| `integer`                        | `integer`                     |
| `map`                            | `map`                         |
| `null`                           | `null`                        |
| `row`                            | `struct`                      |
| `smallint`                       | `short`                       |
| `time`                           | `time`                        |
| `timestamp`                      | `timestamp without time zone` |
| `timestamp without time zone`    | `timestamp without time zone` |
| `timestamp with time zone`       | `timestamp with time zone`    |
| `timestamp with local time zone` | `timestamp with time zone`    |
| `timestamp_ltz`                  | `timestamp with time zone`    |
| `tinyint`                        | `byte`                        |
| `varbinary`                      | `binary`                      |
| `varchar`                        | `string`                      |
