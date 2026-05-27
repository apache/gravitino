---
title: "Optimizer Troubleshooting"
slug: "/table-maintenance-service/optimizer-troubleshooting"
keyword: "table maintenance, optimizer, troubleshooting, spark, strategy"
license: "This software is licensed under the Apache License version 2."
---

Common error messages and recovery paths, organized by where in the optimizer workflow you encounter them. Each entry shows the error or symptom, what it usually means, and the fix.

## CLI Usage Errors

These errors come from the optimizer CLI rejecting your invocation before doing any work. The fix is usually in your command-line arguments or config file path.

### Invalid `--type` value

**Error:** `Invalid --type`

The CLI rejects underscored variants of command names. Use kebab-case values such as `update-statistics`, not `update_statistics`.

### Conflicting `--statistics-payload` and `--file-path`

**Error:** `--statistics-payload and --file-path cannot be used together`

The `local-stats-calculator` accepts input from exactly one source: inline JSON via `--statistics-payload` or a JSONL file via `--file-path`. Use one or the other.

### Missing required input source

**Error:** `requires one of --statistics-payload or --file-path`

When `--calculator-name local-stats-calculator` is used, one input source is required. Add either `--statistics-payload` (inline JSON Lines) or `--file-path` (path to a JSONL file).

### Invalid `--partition-path` format

**Error:** `--partition-path must be a JSON array`

Pass a JSON array, not a JSON object. Example:

```text
[{"dt":"2026-01-01"}]
```

### Optimizer config file not found

**Error:** `Specified optimizer config file does not exist`

Check the `--conf-path` value and file permissions. By default the CLI loads `conf/gravitino-optimizer.conf` from the current working directory; use `--conf-path` only to point at a different file.

## Strategy and Policy Errors

These appear during `submit-strategy-jobs` when the optimizer cannot resolve a strategy or finds nothing to do.

### No identifiers match the strategy name

**Error:** `No identifiers matched strategy name ...`

`--strategy-name` must be the policy name (for example `iceberg_compaction_default`), not the policy type (`system_iceberg_compaction`) and not the strategy type (`iceberg-data-compaction`). See [Names You Will See](./optimizer.md#names-you-will-see) for the distinction.

### Missing `StrategyHandler` for strategy type

**Error:** `No StrategyHandler class configured for strategy type ...`

The optimizer cannot find a handler for the strategy type that the policy generates. Add the handler mapping to your optimizer config:

```properties
gravitino.optimizer.strategyHandler.iceberg-data-compaction.className = org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionStrategyHandler
```

If you already use the packaged default optimizer config, this mapping may already exist.

### Dry-run returns no `DRY-RUN` or `SUBMIT` lines

Trigger conditions are not met for any selected partition. For compaction, verify that `custom-data-file-mse` and `custom-delete-file-number` in table statistics and metrics are large enough to satisfy the policy rules. Run `list-table-metrics` on the target to inspect current values and compare against the policy's `minDataFileMse` and `minDeleteFileNumber`.

## Job Execution Errors

These appear after a Spark job has been submitted: status looks wrong, the job fails, or the rewrite cannot complete.

### Job status appears stale (`queued` or `started` for a long time)

REST job status may lag the real Spark process state by up to `gravitino.job.statusPullIntervalInMs` milliseconds (default `300000`, about 5 minutes). For local verification, reduce this value (for example `10000`) and restart Gravitino so REST status refreshes faster.

If the status is genuinely stuck rather than just lagging, check the local staging logs at:

```
/tmp/gravitino/jobs/staging/<metalake>/<job-template-name>/<job-id>/error.log
```

### Spark job fails with `hdfs://localhost:9000` or filesystem errors

The Spark job is trying to use HDFS by default. Set local filesystem explicitly in Spark config:

```properties
spark.hadoop.fs.defaultFS=file:///
```

### Rewrite fails on multi-level partition transforms

In release `1.2.0`, the rewrite path may fail for partition filters combining identity and day transforms (for example `PARTITIONED BY (p, days(ts))`) with an error like:

```text
Cannot translate Spark expression ... day(cast(ts as date)) ... to data source filter
```

To verify the failure:

1. Check job run status by rewrite job id at `/api/metalakes/<metalake>/jobs/runs/<job-id>`.
2. Check the staging log at `/tmp/gravitino/jobs/staging/<metalake>/builtin-iceberg-rewrite-data-files/<job-id>/error.log`.

Workaround:

- Use identity-only partition compaction path for release `1.2.0`.
- Keep this failure case as a reproducible regression test for later fix validation.

Observed compatibility matrix in release `1.2.0` (rewrite path):

- PASS: `p`, `p, c2` (identity-only partition transforms)
- FAIL: `p, years(ts)`, `p, months(ts)`, `p, days(ts)`, `p, hours(ts)`, `p, truncate(1, c2)`, `p, bucket(8, id)`

### `submit-update-stats-job` fails with JDBC metrics errors

When `--updater-options` includes `gravitino.optimizer.jdbcMetrics.*`, the Spark job needs the JDBC driver JAR on its runtime classpath. Typical failures are `ClassNotFoundException` for the driver class or `No suitable driver`.

Add the driver JAR via `--spark-conf`:

```json
{
  "spark.jars": "/path/to/postgresql-42.7.4.jar"
}
```

## Monitoring and Metrics Errors

These appear when reading stored metrics or running rule evaluations.

### `monitor-metrics` returns `evaluation=false` unexpectedly

Two things can cause this: rule names that do not match your stored metrics, or an evaluation window that does not contain both before and after samples.

1. Query current metrics first with `list-table-metrics` (and `--partition-path` for partition scope).
2. Use the exact metric names returned by your environment in `gravitino.optimizer.monitor.gravitinoMetricsEvaluator.rules`. Names produced by `submit-update-stats-job --update-mode metrics` are often `custom-*` prefixed (for example `custom-data-file-mse`).
3. Ensure `--action-time` is inside the range where both before and after samples exist.

## Related

- [Table Maintenance Service (Optimizer)](./optimizer.md)
- [Optimizer Configuration](./optimizer-configuration.md)
- [Optimizer Quick Start and Verification](./optimizer-quick-start.md)
- [Optimizer CLI Reference](./optimizer-cli-reference.md)
