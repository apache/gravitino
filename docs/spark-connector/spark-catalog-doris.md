---
title: "Spark Connector: Apache Doris Batch Read"
slug: "/spark-connector/spark-catalog-doris"
keyword: "spark connector apache doris batch read"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The Gravitino Spark connector provides an opt-in Apache Doris batch-read adapter for tables
managed by the `jdbc-doris` Server Catalog. Gravitino authorization and credential vending remain
the source of truth for the read.

This page documents the first implementation stage: Spark 3.5.3 and newer patches in the Spark
3.5 line with Scala 2.12. PR #12414 has not yet merged, so other Spark versions are not included
in the current specialized support declaration. After that PR merges, the same branch and PR will
adapt the connector architecture and add the remaining target Spark versions before review.

## Support matrix

| Spark | Scala | Doris Spark Connector | Specialized Doris adapter |
|---|---|---|---|
| 3.5.3+ (3.5.x) | 2.12 | `org.apache.doris:spark-doris-connector-spark-3.5:26.0.0` | Current implementation stage |
| Other Spark versions | Corresponding version | To be verified after PR #12414 | Not supported by this adapter yet |

The existing generic JDBC behavior is not counted as specialized Doris support. The official
Doris Connector and the JDBC driver must be supplied externally on the Spark driver and all
executors. They are not bundled into the Gravitino Spark runtime artifact.

## Preparation

1. Configure a `jdbc-doris` catalog in Gravitino. The server catalog documentation describes the
   JDBC properties and the Doris-specific `doris-fenodes` and `doris-query-port` properties.
2. For specialized Spark reads, set both `doris-fenodes` and `doris-query-port` on the catalog.
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

The default value of `spark.sql.gravitino.enableDorisSupport` is `false`. With the default value,
`jdbc-doris` continues to use the existing generic JDBC adapter. When the flag is enabled, the
catalog-managed endpoints and vended JDBC credentials cannot be overridden by Spark options.

## Read behavior

- The connector obtains `SELECT_TABLE` authorization before loading the Doris physical schema or
  constructing a read delegate.
- The logical Gravitino schema is checked against a Doris physical schema snapshot. Schema or type
  drift fails the read closed.
- The Doris physical schema endpoint does not expose column nullability; the adapter therefore
  does not claim physical nullability validation and uses the logical Gravitino nullability in the
  Spark-visible schema.
- Eligible detail projections and predicates use the official Doris tablet reader.
- Aggregates, Top-N, global limits, offsets, and projections requiring Doris-specific normalization
  use Spark JDBC V2 so that Spark query semantics remain visible and testable.
- The specialized table exposes `BATCH_READ` only.

## Type representation

Standard scalar types retain their normal Spark representation when the logical and physical
schemas are compatible. Doris types that are lossy through JDBC or exceed Spark Catalyst limits
are exposed as documented strings. Binary values and Doris bitmap/HLL values use base64 SQL
projections where applicable. JSON, VARIANT, IP, LARGEINT, complex types, unsigned types, and
wide decimals must not be assumed to retain a native Spark type.

## Limitations

The specialized adapter currently does not support:

- batch writes, overwrite, streaming, or Stream Load;
- Arrow Flight SQL;
- strict native TLS identity verification claims;
- performance guarantees or release-level benchmarks; or
  - Spark versions earlier than 3.5.3 or outside Spark 3.5 / Scala 2.12.

The Spark 3.5 stage is part of one complete Doris Connector PR. It must not be submitted for
review independently; other target Spark versions will be added in the same PR after PR #12414
merges and their official Doris artifacts and compatibility evidence are available.

## Troubleshooting

- **The catalog uses generic JDBC:** verify that `spark.sql.gravitino.enableDorisSupport=true` and
  that the runtime is Spark 3.5.3 or newer in the Spark 3.5 line with Scala 2.12.
- **The adapter rejects initialization:** verify the external Doris Connector JAR, JDBC driver,
  `doris-fenodes`, and `doris-query-port`.
- **The read fails with a schema mismatch:** compare the Gravitino logical table schema with the
  current Doris physical table schema and refresh the Gravitino metadata if the table changed.
- **An unsupported Spark version is reported:** the current specialized stage intentionally covers
  only Spark 3.5.3+ in the Spark 3.5 line with Scala 2.12. Do not treat generic JDBC behavior as
  specialized support.
