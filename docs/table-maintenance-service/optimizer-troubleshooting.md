---
title: "Optimizer Troubleshooting"
slug: /table-maintenance-service/troubleshooting
keyword: table maintenance, optimizer, troubleshooting, spark, strategy
license: This software is licensed under the Apache License version 2.
---

## `Invalid --type`

Use kebab-case values such as `update-statistics`, not `update_statistics`.

## `--statistics-payload and --file-path cannot be used together`

For `local-stats-calculator`, use exactly one of them.

## `requires one of --statistics-payload or --file-path`

When `--calculator-name local-stats-calculator` is used, one input source is required.

## `--partition-path must be a JSON array`

Use a JSON array format, for example:

```text
[{"dt":"2026-01-01"}]
```

## Job status appears stale (`queued` or `started` for a long time)

Check `gravitino.job.statusPullIntervalInMs` and local staging logs under:

`/tmp/gravitino/jobs/staging/<metalake>/<job-template-name>/<job-id>/error.log`.

For local verification, reduce `gravitino.job.statusPullIntervalInMs` (for example `10000`) and
restart Gravitino so REST status can refresh faster.

## `No identifiers matched strategy name ...`

`--strategy-name` must be the policy name (for example `iceberg_compaction_default`), not the policy type (`system_iceberg_compaction`) and not the strategy type (`iceberg-data-compaction`).

## Dry-run returns no `DRY-RUN` or `SUBMIT` lines

This usually means trigger conditions are not met. For compaction, verify
`custom-data-file-mse` and `custom-delete-file-number` in table statistics/metrics are large
enough to satisfy policy rules.

## `monitor-metrics` returns `evaluation=false` unexpectedly

Check both rule names and metric samples:

1. Query current metrics first with `list-table-metrics` (and `--partition-path` for partition scope).
2. Use the exact metric names returned by your environment in
   `gravitino.optimizer.monitor.gravitinoMetricsEvaluator.rules`.
3. Ensure `--action-time` is inside the range where both before and after samples exist.

## `No StrategyHandler class configured for strategy type ...`

Add strategy handler mapping to optimizer config, for example:

```properties
gravitino.optimizer.strategyHandler.iceberg-data-compaction.className = org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionStrategyHandler
```

If you already use the packaged default optimizer config, this mapping may already exist.

## Spark job fails with `hdfs://localhost:9000` or filesystem errors

Set local filesystem explicitly in Spark config:

```properties
spark.hadoop.fs.defaultFS=file:///
```

## Rewrite fails on multi-level partition (`identity + day(...)`)

In release `1.2.0`, rewrite may fail for partition filters combining identity and day transform
(for example `PARTITIONED BY (p, days(ts))`) with error:

```text
Cannot translate Spark expression ... day(cast(ts as date)) ... to data source filter
```

How to verify:

1. Check job run status by rewrite job id under
   `/api/metalakes/<metalake>/jobs/runs/<job-id>`.
2. Check staging log:
   `/tmp/gravitino/jobs/staging/<metalake>/builtin-iceberg-rewrite-data-files/<job-id>/error.log`.

Workaround:

- Use identity-only partition compaction path for release `1.2.0`.
- Keep this failure case as a reproducible regression test for later fix validation.

Observed compatibility matrix in release `1.2.0` (rewrite path):

- PASS: `p`, `p, c2` (identity-only partition transforms)
- FAIL: `p, years(ts)`, `p, months(ts)`, `p, days(ts)`, `p, hours(ts)`,
  `p, truncate(1, c2)`, `p, bucket(8, id)`

## `submit-update-stats-job` fails with JDBC metrics errors

When `--updater-options` includes `gravitino.optimizer.jdbcMetrics.*`, ensure the JDBC driver is
available to Spark runtime classpath. Typical failures include `ClassNotFoundException` for driver
class or `No suitable driver`.

Example in `--spark-conf`:

```json
{
  "spark.jars": "/path/to/postgresql-42.7.4.jar"
}
```

## `Specified optimizer config file does not exist`

Check your `--conf-path` and file permissions.

## Related docs

- [Table Maintenance Service (Optimizer)](./optimizer.md)
- [Optimizer Configuration](./optimizer-configuration.md)
- [Optimizer Quick Start and Verification](./optimizer-quick-start.md)
- [Optimizer CLI Reference](./optimizer-cli-reference.md)
