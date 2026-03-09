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

## `No identifiers matched strategy name ...`

`--strategy-name` must be the policy name (for example `iceberg_compaction_default`), not the policy type (`system_iceberg_compaction`) and not the strategy type (`iceberg-data-compaction`).

## Dry-run returns no `DRY-RUN` or `SUBMIT` lines

This usually means trigger conditions are not met. For compaction, verify `custom-data-file-mse` and `custom-delete-file-number` in table statistics are large enough to satisfy policy rules.

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

## `Specified optimizer config file does not exist`

Check your `--conf-path` and file permissions.

## Related docs

- [Table Maintenance Service (Optimizer)](./optimizer.md)
- [Optimizer Configuration](./optimizer-configuration.md)
- [Optimizer Quick Start and Verification](./optimizer-quick-start.md)
- [Optimizer CLI Reference](./optimizer-cli-reference.md)
