---
title: "Troubleshooting"
slug: "/table-maintenance-service/troubleshooting"
keywords:
  - table maintenance
  - troubleshooting
  - spark
license: "This software is licensed under the Apache License version 2."
---

## Overview

Failures fall into three groups, matching where they occur in the workflow. Command and argument errors surface immediately. Evaluation problems produce no output rather than an error, which is what makes them confusing. Execution failures happen inside Spark, so the real message is in the staging log rather than the API response.

Staging logs live under `/tmp/gravitino/jobs/staging/job-runs/{job_id}/`, controlled by `gravitino.job.stagingDir`. Read `error.log` for failures and `output.log` for results.

## Command and Argument Errors

These come back from the CLI immediately and name the problem.

**`Invalid --type`** — command names are kebab-case. Use `update-statistics`, not `update_statistics`.

**`--statistics-payload and --file-path cannot be used together`** — `local-stats-calculator` takes exactly one input source.

**`requires one of --statistics-payload or --file-path`** — the same rule from the other side. With `--calculator-name local-stats-calculator`, one of the two is mandatory.

**`--partition-path must be a JSON array`** — even for a single partition, pass an array:

```text
[{"dt":"2026-01-01"}]
```

**`Specified optimizer config file does not exist`** — check the `--conf-path` value and the file's permissions.

**`No StrategyHandler class configured for strategy type ...`** — the strategy handler mapping is missing from the CLI configuration:

```properties
gravitino.optimizer.strategyHandler.iceberg-data-compaction.className = org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionStrategyHandler
```

The packaged default configuration already contains this, so seeing it usually means a hand-written config file.

## Evaluation Produces Nothing

These are the hard ones, because success and "the policy decided not to act" look identical.

**`No identifiers matched strategy name ...`** — `--strategy-name` takes the policy name, for example `iceberg_compaction_default`. It does not take the policy type `system_iceberg_compaction` or the strategy type `iceberg-data-compaction`, despite being called strategy name.

**A dry run prints no `DRY-RUN` or `SUBMIT` lines** — the trigger conditions were not met. For compaction, check that `custom-data-file-mse` and `custom-delete-file-number` in the table's statistics are large enough to satisfy the policy rules. A table with too few small files is the usual cause, and the fix is more data rather than more configuration.

**`monitor-metrics` returns `evaluation=false` unexpectedly** — check the rule names and the sample window together:

1. Query the current metrics with `list-table-metrics`, adding `--partition-path` for partition scope.
2. Use the exact metric names your environment returns in `gravitino.optimizer.monitor.gravitinoMetricsEvaluator.rules`. Names that look close enough are not.
3. Make sure `--action-time` falls inside a range where both a before and an after sample exist.

## Job Execution Failures

**Status stays `queued` or `started` for a long time** — REST status is polled, not pushed, and `gravitino.job.statusPullIntervalInMs` defaults to five minutes. Lower it to `10000` and restart the server for local work. If the status is genuinely stuck rather than lagging, read `error.log` in the staging directory.

**Spark fails with `hdfs://localhost:9000` or other filesystem errors** — Spark is defaulting to HDFS on a machine that has none:

```properties
spark.hadoop.fs.defaultFS=file:///
```

**`submit-update-stats-job` fails with JDBC metrics errors** — when `--updater-options` includes `gravitino.optimizer.jdbcMetrics.*`, the JDBC driver has to be on the Spark runtime classpath. `ClassNotFoundException` and `No suitable driver` both mean the same thing. With the k8s job executor, add it in `--spark-conf`; with the local job executor, install it into `SPARK_HOME/jars`, as the driver applies `--spark-conf` only after it starts:

```json
{
  "spark.jars": "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"
}
```

`spark.jars` in `spark_conf` replaces a `spark.jars` default of the k8s job executor, so if that
default provides the Iceberg runtime, list it in `spark_conf` as well.

**`The provided credentials did not support` or Iceberg `Not authorized`** — a secured Gravitino
endpoint needs the optimizer Gravitino client auth (`none` / `simple` / `basic` / `oauth`) in CLI
conf and/or short names in `updater_options`. A secured Iceberg REST catalog needs `rest.auth.*` in
`spark-conf` for any built-in job that reads the table. Object-store writes also need
`spark.sql.catalog.<cat>.header.X-Iceberg-Access-Delegation=vended-credentials`. See
[Gravitino client auth](./optimizer-configuration.md#gravitino-client-auth-cli-and-update-stats) and
[Secured Gravitino and Iceberg REST](./optimizer.md#secured-gravitino-and-iceberg-rest-object-storage).

**`NoSuchWarehouse` on Iceberg REST** — `warehouse_location` was set to an `s3://` (or similar)
URI. For REST catalogs use the Gravitino **catalog name** instead.

**Direct rewrite `runJob` returns HTTP 400 listing missing keys** — `where_clause` has no template
default; pass `""` to rewrite the whole table. Shared catalog keys
(`catalog_name`, `table_identifier`, `catalog_type`, `catalog_uri`, `warehouse_location`) are also
required. `strategy` / `sort_order` / `options` have defaults when omitted.

**Rewrite succeeds with `Rewritten data files: 0`** — Iceberg's default `min-input-files` is 5.
One small file per partition is not enough for a non-empty rewrite. Add more small files or pass
rewrite options that lower the threshold, then re-run.

**Built-in Iceberg jobs fail with `Missing Iceberg Spark session extensions`** —
Spark only warns when `IcebergSparkSessionExtensions` is missing, so built-in jobs check the
classpath after `SparkSession` starts and exit with a non-zero status when the Iceberg Spark
runtime is absent. The templates configure Iceberg classes but leave `jars` empty, and
`gravitino-jobs` does not bundle `iceberg-spark-runtime`. A stock Spark install is not enough.
Put a version-matched Iceberg Spark runtime on the job classpath: install it into `SPARK_HOME/jars`
or the Spark image, or, with the k8s job executor, set it in `spark_conf`:

```json
{
  "spark.jars": "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.11.0/iceberg-spark-runtime-3.5_2.12-1.11.0.jar"
}
```

With the local job executor, `spark.jars` in `spark_conf` doesn't add it: the driver applies
`spark_conf` only after spark-submit has built its classpath.

Use the Spark, Scala, and Iceberg versions that match your cluster. See
[Built-in Job Templates](./optimizer-cli-reference.md#built-in-job-templates).

**Multi-level / non-identity partition rewrite** — the rewrite **job** does not reject these
layouts. A **direct** `builtin-iceberg-rewrite-data-files` submit with an empty `where_clause`
rewrites the whole table and succeeds on `PARTITIONED BY (p, days(ts))` (and similar). You may
still see `Rewritten data files: 0` when files are not rewrite-eligible (`min-input-files`).

Strategy-generated partition filters use Iceberg Spark system functions (for example
`days(ts) = DATE "2024-01-01"`, `years(ts) = 54`, `bucket(16, id) = 3`), not Spark builtins such
as `day()`. The `days()` UDF returns `DATE`, so the strategy emits a date literal (not the raw
epoch-day int). Identity `DATE`/`TIMESTAMP`/`TIME` partitions use the same double-quoted typed
literals (`dt = DATE "2024-01-01"`): single quotes are unsafe inside `CALL ... where => '...'`.
With Iceberg Spark runtime 1.4+ these forms are translated by `SparkV2Filters`.
If `Cannot translate Spark expression ...` still appears, confirm the job classpath uses a
matching Iceberg Spark runtime and that the predicate resolved as an Iceberg function (not Spark
`day()`/`year()`). Empty-`where_clause` direct submits remain the whole-table fallback. Confirm
in job run status and `error.log` under `builtin-iceberg-rewrite-data-files`.

## Related

- [Table Maintenance Service](./optimizer.md)
- [Configuration](./optimizer-configuration.md)
- [Walkthrough](./optimizer.md#walkthrough)
- [CLI Reference](./optimizer-cli-reference.md)
