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

Staging logs live under `/tmp/gravitino/jobs/staging/{metalake}/{job_template_name}/{job_id}/`, controlled by `gravitino.job.stagingDir`. Read `error.log` for failures and `output.log` for results.

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

**`submit-update-stats-job` fails with JDBC metrics errors** — when `--updater-options` includes `gravitino.optimizer.jdbcMetrics.*`, the JDBC driver has to be on the Spark runtime classpath. `ClassNotFoundException` and `No suitable driver` both mean the same thing:

```json
{
  "spark.jars": "/path/to/postgresql-42.7.4.jar"
}
```

**Rewrite fails on a multi-level partition** — in release `1.2.0`, rewriting a table partitioned by an identity transform combined with a time transform, such as `PARTITIONED BY (p, days(ts))`, fails with:

```text
Cannot translate Spark expression ... day(cast(ts as date)) ... to data source filter
```

Confirm it by checking the job run at `/api/metalakes/{metalake}/jobs/runs/{job_id}` and reading `error.log` under `builtin-iceberg-rewrite-data-files`. The only workaround is to compact identity-partitioned tables and leave the rest alone.

Observed in `1.2.0`:

| Partitioning                                                                  | Rewrite |
|-------------------------------------------------------------------------------|---------|
| `p`, `p, c2`                                                                    | Works   |
| `p, years(ts)`, `p, months(ts)`, `p, days(ts)`, `p, hours(ts)`                  | Fails   |
| `p, truncate(1, c2)`, `p, bucket(8, id)`                                        | Fails   |

## Related

- [Table Maintenance Service](./optimizer.md)
- [Configuration](./optimizer-configuration.md)
- [Quick Start](./optimizer.md#walkthrough)
- [CLI Reference](./optimizer-configuration.md)
