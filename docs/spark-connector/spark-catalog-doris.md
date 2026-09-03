---
title: "Spark Connector: Apache Doris Batch Read and Write"
slug: "/spark-connector/spark-catalog-doris"
keyword: "spark connector apache doris batch read write"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The Gravitino Spark connector provides an opt-in Apache Doris batch read/write adapter for tables
managed by the `jdbc-doris` Server Catalog. Gravitino authorization and credential vending remain
the source of truth for reads and writes.

The specialized adapter requires Spark 3.5.3 or newer in the Spark 3.5 line with Scala 2.12.
Compatibility is certified at Spark 3.5.3 and 3.5.9; other Spark 3.5 patch releases are accepted by
the compatibility gate but are not individually certified. Other Spark and Scala lines are outside
the current specialized support declaration.

## Support matrix

| Spark | Scala | Doris | Doris Spark Connector | Specialized Doris adapter |
|---|---|---|---|---|
| 3.5.3 | 2.12 | 3.0.6.2 | `org.apache.doris:spark-doris-connector-spark-3.5:26.0.0` | Certified lower-bound embedded matrix: batch read; opt-in batch append and truncate overwrite |
| 3.5.3 | 2.12 | 4.0.6 | Same artifact | Targeted smoke only; not individually certified |
| 3.5.9 | 2.12 | 4.0.6 | Same artifact | Certified upper-bound embedded matrix: batch read; opt-in batch append and truncate overwrite |
| 3.5.9 | 2.12 | 3.0.6.2 | Same artifact | Certified standalone two-worker external-classpath matrix |
| Other 3.5.3+ patches | 2.12 | 3.0.6.2 or 4.0.6 | Same artifact | Accepted by the compatibility gate; not individually certified |
| 3.5.0–3.5.2 | 2.12 | Any | Same artifact | Outside the specialized build and support contract |
| 3.5.x | 2.13 | Any | No compatible 26.0.0 artifact | Generic JDBC remains available; specialized adapter is not supported |
| Other Spark versions | Corresponding version | Any | Version-specific artifacts may exist, but no Gravitino specialized adapter is provided in this scope | Generic JDBC remains available where supported |

Spark 3.5.0 through 3.5.2 are below the current Gravitino Spark 3.5 build baseline because the
shared catalog implementation uses the write-aware API introduced in Spark 3.5.3. The existing
generic JDBC behavior is not counted as specialized Doris support. The official
Doris Connector and the JDBC driver must be supplied externally on the Spark driver and all
executors. They are not bundled into the Gravitino Spark runtime artifact.

## Preparation

1. Configure a `jdbc-doris` catalog in Gravitino. The server catalog documentation describes the
   JDBC properties and the Doris-specific `doris-fenodes`, `doris-query-port`, write-policy, and
   SQL-lane properties. The Gravitino Server must have MySQL Connector/J and JDBC network access to
   Doris for server-side metadata operations.
2. For the specialized Spark adapter, set both `doris-fenodes` and `doris-query-port` on the catalog.
   Use a comma-separated list such as `fe-1:8030,fe-2:8030`; URI-form endpoints and IPv6 literals
   are rejected.
3. Put the following external dependencies on the Spark driver and executor classpaths:
   - `org.apache.doris:spark-doris-connector-spark-3.5:26.0.0`;
   - the MySQL Connector/J driver required by the Spark JDBC V2 lane; and
   - the Gravitino Spark connector runtime JAR matching Spark 3.5 and Scala 2.12.

## Configuration

Enable the specialized adapter explicitly:

```shell
./bin/spark-sql \
  --jars /path/to/gravitino-spark-connector-runtime-3.5_2.12-${gravitino-version}.jar,/path/to/spark-doris-connector-spark-3.5-26.0.0.jar,/path/to/mysql-connector-j.jar \
  --conf spark.plugins=org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin \
  --conf spark.sql.gravitino.uri=http://127.0.0.1:8090 \
  --conf spark.sql.gravitino.metalake=test \
  --conf spark.sql.gravitino.enableDorisSupport=true
```

Alternatively, let Spark resolve the two external Maven dependencies while keeping the Gravitino
runtime as a local JAR:

```shell
./bin/spark-sql \
  --packages org.apache.doris:spark-doris-connector-spark-3.5:26.0.0,com.mysql:mysql-connector-j:8.0.33 \
  --jars /path/to/gravitino-spark-connector-runtime-3.5_2.12-${gravitino-version}.jar \
  --conf spark.plugins=org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin \
  --conf spark.sql.gravitino.uri=http://127.0.0.1:8090 \
  --conf spark.sql.gravitino.metalake=test \
  --conf spark.sql.gravitino.enableDorisSupport=true
```

The default value of `spark.sql.gravitino.enableDorisSupport` is `false`. With the default value,
`jdbc-doris` continues to use the existing generic JDBC adapter. When the flag is enabled, the
catalog-managed endpoints and vended JDBC credentials cannot be overridden by Spark options.
Unlike the generic JDBC adapter, the specialized Doris adapter does not map catalog
`jdbc-user`/`jdbc-password` values into Spark; it requires a vended `JdbcCredential` for the Spark
driver and executors. This is an intentional stricter credential boundary for the dual Doris
native/JDBC data paths.
Per-read options are rejected instead of being merged into the JDBC lane, and protected
catalog/bypass/per-write options fail closed without exposing their values.

Governed writes use catalog-level settings:

| Catalog property | Default | Behavior |
|---|---|---|
| `doris-write-mode` | `disabled` | Set to `batch` to expose governed batch append. |
| `doris-write-overwrite-mode` | `reject` | Set to `truncate` with batch mode to expose non-atomic full-table truncate overwrite. |

The specialized SQL read lane maps the following catalog properties to Spark JDBC options:

| Catalog property | Spark JDBC option |
|---|---|
| `doris-jdbc-partition-column` | `partitionColumn` |
| `doris-jdbc-lower-bound` | `lowerBound` |
| `doris-jdbc-upper-bound` | `upperBound` |
| `doris-jdbc-num-partitions` | `numPartitions` |
| `doris-jdbc-fetch-size` | `fetchsize` |

The four partition properties must be present together. `numPartitions` and `fetchsize` must be
positive integers. The allowed Doris Connector catalog-initialization options are
`doris.request.retries`, `doris.request.connect.timeout.ms`, `doris.request.read.timeout.ms`,
`doris.request.query.timeout.s`, `doris.request.tablet.size`, `doris.batch.size`,
`doris.exec.mem.limit`, `doris.filter.query.in.max.count`, and
`doris.thrift.max.message.size`. Unknown, protected, and `spark.bypass.*` options fail closed.

## Read behavior

- The connector obtains `SELECT_TABLE` authorization before loading the Doris physical schema or
  constructing a read delegate.
- The logical Gravitino schema is checked against a post-authorization physical snapshot combining
  the official Doris FE schema with catalog-managed JDBC `information_schema.columns`. The JDBC
  metadata supplies exact `COLUMN_TYPE`, ordinal position, and nullability; FE/JDBC disagreement or
  logical/physical drift fails closed.
- Eligible detail projections and predicates use the official Doris tablet reader.
- Aggregates, Top-N, global limits, offsets, and projections requiring Doris-specific normalization
  use Spark JDBC V2 so that Spark query semantics remain visible and testable.
- A normally loaded table exposes `BATCH_READ`. Write capabilities are added only by Spark's
  write-aware load path after `MODIFY_TABLE` authorization.

## Write behavior

- Governed writes are disabled by default and require `doris-write-mode=batch`.
- The write-aware load path obtains `MODIFY_TABLE` authorization before constructing an official
  Doris write delegate or performing Doris write I/O.
- Gravitino authorization does not replace Doris authorization. The vended Doris account requires
  Doris `SELECT_PRIV` to inspect and read the table and `LOAD_PRIV` for governed Stream Load writes.
  The official connector implements full-table overwrite with SQL `TRUNCATE TABLE`, but exact
  native privilege requirements remain Doris-version and deployment dependent; the Gravitino
  adapter delegates that decision to Doris and does not introspect or synthesize grants. The Doris
  3.0.6.2 and 4.0.6 test images both permit this operation with `SELECT_PRIV` and `LOAD_PRIV`
  without `DROP_PRIV`; deployments whose Doris policy requires `DROP_PRIV` must grant it. The
  tested path does not require `ALTER_PRIV`; append is also tested with `SELECT_PRIV` and `LOAD_PRIV`
  only.
- Batch append delegates to the official Doris Stream Load writer. The adapter forces Stream Load
  mode, 2PC, strict mode, zero filter tolerance, schemaless mode off, and automatic redirect off.
  Spark catalog, bypass, and per-write options cannot override these settings, endpoints, or
  credentials.
- The input schema must preserve column count, order, case-sensitive names, nullability direction,
  and lossless Spark types. Read-normalized String/base64 families are not writable. The documented
  precision-specific Doris DATETIME String representation is the only normalized write exception.
- `doris-write-overwrite-mode=truncate` enables full-table truncate-then-load only. This operation
  is not atomic: if the subsequent Stream Load fails, the table may remain empty or partially
  populated. Connector 2PC is a writer transaction contract, not a Spark job-wide atomic commit.
- Spark maps an always-true DataFrameWriterV2 overwrite condition to this same full-table truncate
  path. Non-trivial predicate overwrite, dynamic overwrite, streaming, UPDATE, DELETE, MERGE, CTAS,
  and Spark catalog DDL are not exposed by the specialized adapter.

## Type representation

Standard scalar types retain their normal Spark representation when the logical and physical
schemas are compatible. Doris types that are lossy through JDBC or exceed Spark Catalyst limits
are exposed as documented strings. Binary values and Doris bitmap/HLL values use base64 SQL
projections where applicable. JSON, VARIANT, IP, LARGEINT, complex types, unsigned types, and
wide decimals must not be assumed to retain a native Spark type. Normalized external types are
accepted only when their logical and physical Doris type signatures match; unavailable or
inconsistent signatures fail closed.

Normalized values use their Spark-visible String representation for comparison and ordering.
Consequently, lexical ordering places `"10"` before `"9"`, and comparison with a String literal is
also lexical. Spark may implicitly coerce a String comparison with a numeric literal, which can
overflow or lose precision for values outside the selected numeric type. Use an explicit cast only
when the chosen Spark numeric type can represent the complete value range; native numeric ordering
is not certified for wider values.

The writable normalized exception is Doris `DATETIME(p)` without a time zone. For `p=0`, provide
`YYYY-MM-DD HH:mm:ss`. For `p=1` through `p=6`, append a decimal point and exactly `p` fractional
second digits. The adapter validates both the Gregorian date/time and the precision-specific shape
before a row reaches the official writer.

## Limitations

The specialized adapter currently does not support:

- streaming, predicate overwrite, dynamic overwrite, or job-wide atomic writes;
- Arrow Flight SQL;
- strict native TLS identity verification claims;
- performance guarantees or release-level benchmarks; or
- Spark versions earlier than 3.5.3 or outside Spark 3.5 / Scala 2.12; only 3.5.3 and 3.5.9 are
  individually certified. Doris 1.2.x and other unlisted Doris releases are not certified and do
  not automatically fall back when specialized mode is enabled; keep the feature flag disabled to
  use generic JDBC for those releases.

## Troubleshooting

- **The catalog uses generic JDBC:** verify that `spark.sql.gravitino.enableDorisSupport=true` and
  that the runtime is Spark 3.5.3 or newer in the Spark 3.5 line with Scala 2.12. The certified
  compatibility points are Spark 3.5.3 and 3.5.9.
- **The adapter rejects initialization:** verify the external Doris Connector JAR, JDBC driver,
  `doris-fenodes`, and `doris-query-port`.
- **The read fails with a schema mismatch:** compare the Gravitino logical table schema with the
  current Doris physical table schema and refresh the Gravitino metadata if the table changed.
- **A write remains read-only:** verify `doris-write-mode=batch` on the Server Catalog and use Spark
  3.5.3 or newer with the specialized adapter enabled; validate unlisted patches in your
  environment because they are not individually certified.
- **Truncate overwrite is rejected:** additionally set `doris-write-overwrite-mode=truncate` and
  verify that the Doris account has `SELECT_PRIV` and `LOAD_PRIV`. Grant any additional privilege
  required by the deployment's native Doris policy; the tested Doris 3.0.6.2 and 4.0.6 paths do not
  require `DROP_PRIV`.
- **An unsupported Spark version is reported:** the current specialized stage intentionally covers
  only the Spark 3.5 line from 3.5.3 onward with Scala 2.12. Spark 3.5.3 and 3.5.9 are the certified
  points; do not treat generic JDBC behavior or an untested patch as certified specialized support.
