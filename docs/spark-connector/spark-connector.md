---
title: "Spark Connector"
slug: "/spark-connector/spark-connector"
keyword: "spark connector federation query"
license: "This software is licensed under the Apache License version 2."
---

## Overview

The Apache Gravitino Spark connector leverages the Spark DataSourceV2 interface to facilitate the management of diverse catalogs under Gravitino. This capability allows users to perform federation queries, accessing data from various catalogs through a unified interface and consistent access control.

## Capabilities

1. Supports [Hive catalog](spark-catalog-hive.md), [Iceberg catalog](spark-catalog-iceberg.md), [Paimon catalog](spark-catalog-paimon.md), [Jdbc catalog](spark-catalog-jdbc.md), and [AWS Glue catalog](spark-catalog-glue.md).
2. Supports federation query.
3. Supports most DDL and DML SQLs.

## Requirement

* Spark 3.3 or 3.4 or 3.5
* Scala 2.12 or 2.13
* JDK 8, 11 or 17

## Usage

1. [Build](../how-to-build.md) or download the package ([gravitino-spark-connector-runtime-3.3](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-spark-connector-runtime-3.3), [gravitino-spark-connector-runtime-3.4](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-spark-connector-runtime-3.4), [gravitino-spark-connector-runtime-3.5](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-spark-connector-runtime-3.5)), and place it to the classpath of Spark.
2. Configure the Spark session to use the Gravitino spark connector.

| Property                                 | Type   | Default Value | Description                                                                                     | Required |
|------------------------------------------|--------|---------------|-------------------------------------------------------------------------------------------------|----------|
| spark.plugins                            | string | (none)        | Gravitino spark plugin name, `org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin` | Yes      |
| spark.sql.gravitino.metalake             | string | (none)        | The metalake name that spark connector used to request to Gravitino.                            | Yes      |
| spark.sql.gravitino.uri                  | string | (none)        | The uri of Gravitino server address.                                                            | Yes      |
| spark.sql.gravitino.enableIcebergSupport | string | `false`       | Set to `true` to use Iceberg catalog.                                                           | No       |
| spark.sql.gravitino.enablePaimonSupport  | string | `false`       | Set to `true` to use Paimon catalog.                                                            | No       |
| spark.sql.gravitino.client.              | string | (none)        | The configuration key prefix for the Gravitino client config.                                   | No       |
| spark.sql.gravitino.authType             | string | `simple`      | The authentication type, one of `simple`, `basic`, `oauth2`, `kerberos`, `token`.               | No       |
| spark.sql.gravitino.token.value          | string | (none)        | The bearer token to present to Gravitino, used when `authType` is `token`.                      | No       |
| spark.sql.gravitino.token.file           | string | (none)        | Path to a file holding the bearer token. Takes precedence over `token.value` and is re-read per request. | No |
| spark.sql.gravitino.token.principalFields | string | `sub`        | Comma separated, ordered JWT claim names used to identify the caller in `token` mode.           | No       |
| spark.sql.gravitino.clientCacheMaxSize   | int    | `100`         | The maximum number of Gravitino clients cached, one per identity.                               | No       |
| spark.sql.gravitino.clientCacheTtlSec    | long   | `3600`        | Evicts a cached Gravitino client this many seconds after it was last used.                      | No       |
| spark.sql.gravitino.catalogCacheTtlSec   | long   | `300`         | Evicts a cached catalog this many seconds after it was loaded.                                  | No       |
| spark.sql.gravitino.iceberg.rest-routing-enabled | boolean | `true` | Whether `hive` and `jdbc` backed Iceberg catalogs must be routed through the Gravitino Iceberg REST server. Set to `false` to retain legacy native backend translation. | No |
| spark.sql.gravitino.iceberg.rest-uri     | string | (none)        | Overrides the auto-discovered Gravitino Iceberg REST server endpoint. See [Iceberg catalog](spark-catalog-iceberg.md#routing-through-the-gravitino-iceberg-rest-server). | No       |
| spark.sql.gravitino.iceberg.reuseOAuth2  | boolean | `true`        | Reuses the Gravitino OAuth2 client configuration for routed Iceberg REST catalogs. Explicit IRC OAuth2 properties override individual reused values. | No |
| spark.sql.gravitino.iceberg.rest.        | string | (none)        | The configuration key prefix for the Iceberg REST client config (e.g. `rest.auth.type`), applied when a catalog is routed through the Gravitino Iceberg REST server. | No       |

To configure the Gravitino client, use properties prefixed with `spark.sql.gravitino.client.`. These properties will be passed to the Gravitino client after removing the `spark.sql.` prefix.

**Example:** Setting `spark.sql.gravitino.client.socketTimeoutMs` is equivalent to setting `gravitino.client.socketTimeoutMs` for the Gravitino client.

**Note:** Invalid configuration properties will result in exceptions. Please see [Gravitino Java client configurations](../how-to-use-gravitino-client.md#java-client-configuration) for more support client configuration.

### Per-user identity in `token` mode

When `spark.sql.gravitino.authType` is `token`, the connector presents the bearer token found in
`spark.sql.gravitino.token.file`, or failing that `spark.sql.gravitino.token.value`, resolving it again on
every request and reading the active Spark session's configuration in preference to the
application's. The identity of the caller is taken from the JWT claim named by
`spark.sql.gravitino.token.principalFields`, which should be set to match the server's
`gravitino.authenticator.oauth.principalFields` so that the connector and the server agree on who
is asking. The claim is read without verifying the signature, because validating the token remains
the server's job; the value is used only to partition the connector's caches. An opaque,
non-JWT token is partitioned by a hash of the token instead.

Gravitino clients and the catalog metadata they load are cached per identity, so two users sharing
one Spark driver never see each other's cached metadata. All other authentication types keep a
single application-wide identity and are unaffected by any of this.

Two limits are worth stating plainly:

* The list of catalogs registered with Spark at driver startup is resolved with the application's
  identity, so a user may see a catalog name that they are not then allowed to open.
* This governs metadata resolution only. Data is read by the executors using the credentials that
  the underlying catalog was built with, not the end user's token.

```shell
./bin/spark-sql -v \
--conf spark.plugins="org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin" \
--conf spark.sql.gravitino.uri=http://127.0.0.1:8090 \
--conf spark.sql.gravitino.metalake=test \
--conf spark.sql.gravitino.enableIcebergSupport=true \
--conf spark.sql.gravitino.client.socketTimeoutMs=60000 \
--conf spark.sql.gravitino.client.connectionTimeoutMs=60000 \
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

## Datatype Mapping

Gravitino spark connector support the following datatype mapping between Spark and Gravitino.

| Spark Data Type                   | Gravitino Data Type           |
|-----------------------------------|-------------------------------|
| `BooleanType`                     | `boolean`                     |
| `ByteType`                        | `byte`                        |
| `ShortType`                       | `short`                       |
| `IntegerType`                     | `integer`                     |
| `LongType`                        | `long`                        |
| `FloatType`                       | `float`                       |
| `DoubleType`                      | `double`                      |
| `DecimalType`                     | `decimal`                     |
| `StringType`                      | `string`                      |
| `CharType`                        | `char`                        |
| `VarcharType`                     | `varchar`                     |
| `TimestampType`                   | `timestamp with time zone`    |
| `TimestampNTZType` *(Spark 3.4+)* | `timestamp without time zone` |
| `DateType`                        | `date`                        |
| `BinaryType`                      | `binary`                      |
| `ArrayType`                       | `array`                       |
| `MapType`                         | `map`                         |
| `StructType`                      | `struct`                      |

:::note
For Gravitino `UUID` type, Spark connector represents it as `StringType` because Spark has no native UUID type.
This behavior is consistent with Spark built-in PostgreSQL JDBC mapping (`uuid` -> `StringType`).
:::
