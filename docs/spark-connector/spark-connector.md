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
4. Supports automatic [lakehouse REST catalog discovery](spark-rest-catalog-discovery.md).

## Requirement

* Spark 3.5 or 4.0
* Scala 2.12 or 2.13 on Spark 3.5; Spark 4 is Scala 2.13 only
* JDK 8, 11 or 17 on Spark 3.5; Spark 4 requires JDK 17

## Usage

1. [Build](../how-to-build.md) or download the package matching your Spark minor version and Scala version ([gravitino-spark-connector-runtime-3.5_2.12](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-spark-connector-runtime-3.5_2.12), [gravitino-spark-connector-runtime-3.5_2.13](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-spark-connector-runtime-3.5_2.13), [gravitino-spark-connector-runtime-4.0_2.13](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-spark-connector-runtime-4.0_2.13)), and place it to the classpath of Spark.
2. Configure the Spark session to use the Gravitino spark connector.

| Property                                         | Type    | Default Value | Description                                                                                                                                                              | Required |
|--------------------------------------------------|---------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| spark.plugins                                    | string  | (none)        | Gravitino spark plugin name, `org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin`                                                                          | Yes      |
| spark.sql.gravitino.metalake                     | string  | (none)        | The metalake name that spark connector used to request to Gravitino.                                                                                                     | Yes      |
| spark.sql.gravitino.uri                          | string  | (none)        | The uri of Gravitino server address.                                                                                                                                     | Yes      |
| spark.sql.gravitino.failFast                     | boolean | `false`       | Set to `true` to fail Spark startup if connector configuration, client creation or catalog discovery fails.                                                              | No       |
| spark.sql.gravitino.enableIcebergSupport         | string  | `false`       | Set to `true` to use Iceberg catalog.                                                                                                                                    | No       |
| spark.sql.gravitino.enablePaimonSupport          | string  | `false`       | Set to `true` to use Paimon catalog.                                                                                                                                     | No       |
| spark.sql.gravitino.client.                      | string  | (none)        | The configuration key prefix for the Gravitino client config.                                                                                                            | No       |
| spark.sql.gravitino.authType                     | string  | `simple`      | The authentication type, one of `simple`, `basic`, `oauth2`, `kerberos`, `token`.                                                                                        | No       |
| spark.sql.gravitino.token.value                  | string  | (none)        | The bearer token to present to Gravitino, used when `authType` is `token`.                                                                                               | No       |
| spark.sql.gravitino.token.file                   | string  | (none)        | Path to a file holding the bearer token. Takes precedence over `token.value` and is re-read per request.                                                                 | No       |
| spark.sql.gravitino.token.principalFields        | string  | `sub`         | Comma separated, ordered JWT claim names used to identify the caller in `token` mode.                                                                                    | No       |
| spark.sql.gravitino.clientCacheMaxSize           | int     | `100`         | The maximum number of Gravitino clients cached, one per identity.                                                                                                        | No       |
| spark.sql.gravitino.clientCacheTtlSec            | long    | `3600`        | Evicts a cached Gravitino client this many seconds after it was last used.                                                                                               | No       |
| spark.sql.gravitino.catalogCacheTtlSec           | long    | `300`         | Evicts a cached catalog this many seconds after it was loaded.                                                                                                           | No       |
| spark.sql.gravitino.iceberg.rest-routing-enabled | boolean | `true`        | Whether `hive` and `jdbc` backed Iceberg catalogs must be routed through the Gravitino Iceberg REST server. Set to `false` to retain legacy native backend translation.  | No       |
| spark.sql.gravitino.iceberg.rest-uri             | string  | (none)        | Overrides the auto-discovered Gravitino Iceberg REST server endpoint. See [Iceberg catalog](spark-catalog-iceberg.md#routing-through-the-gravitino-iceberg-rest-server). | No       |
| spark.sql.gravitino.iceberg.reuseOAuth2          | boolean | `true`        | Reuses the Gravitino OAuth2 client configuration for routed Iceberg REST catalogs. Explicit IRC OAuth2 properties override individual reused values.                     | No       |
| spark.sql.gravitino.iceberg.rest.                | string  | (none)        | The configuration key prefix for the Iceberg REST client config (e.g. `rest.auth.type`), applied when a catalog is routed through the Gravitino Iceberg REST server.     | No       |

The URI and metalake settings are required to register Gravitino catalogs. By default, if configuration
validation, client creation or catalog discovery fails, the connector logs a warning and skips
registering its catalogs and SQL extensions. Spark jobs that do not use Gravitino catalogs can
continue. Set `spark.sql.gravitino.failFast=true` to fail Spark startup instead. The `failFast` setting
itself must be a valid boolean.

To configure the Gravitino client, use properties prefixed with `spark.sql.gravitino.client.`. These properties will be passed to the Gravitino client after removing the `spark.sql.` prefix.

**Example:** Setting `spark.sql.gravitino.client.socketTimeoutMs` is equivalent to setting `gravitino.client.socketTimeoutMs` for the Gravitino client.

**Note:** Invalid client configuration properties are handled according to `spark.sql.gravitino.failFast`
during initialization. Please see [Gravitino Java client configurations](../how-to-use-gravitino-client.md#java-client-configuration)
for supported client configuration.

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
| `TimestampNTZType`                | `timestamp without time zone` |
| `DateType`                        | `date`                        |
| `BinaryType`                      | `binary`                      |
| `ArrayType`                       | `array`                       |
| `MapType`                         | `map`                         |
| `StructType`                      | `struct`                      |

:::note
For Gravitino `UUID` type, Spark connector represents it as `StringType` because Spark has no native UUID type.
This behavior is consistent with Spark built-in PostgreSQL JDBC mapping (`uuid` -> `StringType`).
:::

### PostgreSQL write limitations

Spark JDBC reads PostgreSQL UUID and External columns as strings. Writes to tables containing
these columns are rejected before JDBC execution, including writes of NULL values and
INSERT OVERWRITE. The error identifies the unsupported column. Spark binds these columns
as VARCHAR, which does not preserve the PostgreSQL target type. Use a native PostgreSQL
writer for these tables. Other PostgreSQL column types retain their existing write behavior.

### JDBC query consistency

Spark pushes predicates, aggregates, Top-N, limits, and offsets down to the JDBC source when
the matching `pushDown*` options allow it. Some expressions stay in Spark, because the source
could return different results:

- Expressions on string, char, varchar, binary, UUID, or external-type columns, also inside
  arrays. The source can compare strings with a different collation, and Spark cannot always
  write binary literals in SQL.
- Expressions with a string or binary literal or cast, such as `cast(id AS string) > '9'`.
- Casts to a timestamp type, and casts or date parts such as `hour()` or `year()` of a Spark
  `TIMESTAMP` column. Examples are MySQL `DATETIME`, or PostgreSQL `timestamp` when
  `preferTimestampNTZ` is off. Spark uses the session time zone for these, but the source uses
  its own time zone.
- Date parts other than year, quarter, month, day, hour, and minute, such as `second()` or
  `weekofyear()`. The source can compute them differently.
- Filters with a `TIMESTAMP_NTZ` literal, when the JDBC dialect cannot write it in SQL. The
  PostgreSQL dialect in Spark 3.5 has this limit. Spark 4.0 does not.
- Value-dependent aggregates on timestamp columns whose JDBC reader type differs from the type Spark shows. An
  example is PostgreSQL `timestamptz` on Spark 3.5 with `preferTimestampNTZ=true`.

NULL checks (`IS NULL` and `IS NOT NULL`) and non-distinct `count(column)` on the column itself
can be pushed down when its type does not contain an external type. External types remain in
Spark: for example, PostgreSQL composite NULL checks inspect row fields, while Spark reads a
non-null composite as a string. `count(DISTINCT string_column)` stays in Spark because it
depends on collation.
For PostgreSQL and MySQL, a simple positive string equality or `IN` filter can also be sent
to the source as a prefilter, while Spark rechecks it. This reduces rows transferred to Spark
without trusting the source collation to produce an exact match. These prefilters require
Spark's default binary string comparison. Negated comparisons, casts, compound expressions,
and literals containing a backslash or NUL remain in Spark. Column names differing only by case are excluded from string prefiltering.
Other JDBC sources retain the conservative string policy.

Other expressions are still pushed down, for example `id = 1`, `count(*)`, `LIMIT 10`, or
`ORDER BY id LIMIT 3`. They run with the source semantics. For example, a source `avg()` can
return fewer decimal places than Spark. When a filter stays in Spark, Spark also keeps the
limit and the aggregate of that query in Spark.

#### PostgreSQL timestamps without time zone

By default, a PostgreSQL timestamp without time zone maps to Spark `TIMESTAMP`, as before.
Its value can change with the Spark session time zone. To read the stored wall-clock value,
set `preferTimestampNTZ=true` on the Spark catalog, for example
`spark.sql.catalog.<name>.preferTimestampNTZ=true`. You can also set the Gravitino catalog
property `spark.bypass.preferTimestampNTZ=true`. The Spark catalog option wins when both are
set. This is a catalog-level option: a per-read `spark.read.option("preferTimestampNTZ", ...)`
is ignored so that the reader and the fixed table schema cannot diverge. The option applies to
PostgreSQL catalogs only. Other JDBC catalogs ignore this option and explicitly use
`preferTimestampNTZ=false`, even when `spark.sql.timestampType=TIMESTAMP_NTZ`, to keep their
reader and table schema on `TIMESTAMP`.

With the option on:

- A timestamp without time zone maps to Spark `TIMESTAMP_NTZ`. A timestamp with time zone still
  maps to Spark `TIMESTAMP`.
- Writes store the value from the Spark session time zone instead of the JVM time zone. Jobs
  whose session and JVM time zones differ write different values than before.
- A timestamp column cannot be used as `partitionColumn`. Spark only accepts numeric, date, or
  `TIMESTAMP` partition columns.
- Reading PostgreSQL `infinity` or `-infinity` fails with a `long overflow` error. With the
  option off, Spark reads them as 9999-12-31 and 0001-01-01.
- On Spark 3.5, timestamp array elements still map to `TIMESTAMP`, because the Spark 3.5
  reader does not support `TIMESTAMP_NTZ` array elements.
