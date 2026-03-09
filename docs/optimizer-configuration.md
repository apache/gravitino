---
title: "Optimizer Configuration"
slug: /table-maintenance-service/configuration
keyword: table maintenance, optimizer, configuration, job template, spark
license: This software is licensed under the Apache License version 2.
---

## Configuration layers

Use these layers together:

| Layer | Scope | Typical keys |
| --- | --- | --- |
| Gravitino server config | Runtime for job manager and executor | `gravitino.job.executor`, `gravitino.job.statusPullIntervalInMs`, `gravitino.jobExecutor.local.sparkHome` |
| Job submission `jobConf` | Per job run | `catalog_name`, `table_identifier`, `spark_*`, template-specific args |
| Optimizer CLI config | CLI commands | `gravitino.optimizer.*` in `conf/gravitino-optimizer.conf` |

## Server-side configuration

Set server-level runtime behavior in `gravitino.conf`.

```properties
gravitino.job.executor=local
gravitino.job.statusPullIntervalInMs=300000
gravitino.jobExecutor.local.sparkHome=/path/to/spark
```

For local demo environments, you can reduce `gravitino.job.statusPullIntervalInMs` to get faster status updates.

## Built-in update stats `jobConf`

Use `builtin-iceberg-update-stats` with at least these keys:

```json
{
  "catalog_name": "rest_catalog",
  "table_identifier": "db.t1",
  "update_mode": "all",
  "updater_options": "{\"gravitino_uri\":\"http://localhost:8090\",\"metalake\":\"test\",\"statistics_updater\":\"gravitino-statistics-updater\",\"metrics_updater\":\"gravitino-metrics-updater\"}",
  "spark_conf": "{\"spark.master\":\"local[2]\",\"spark.hadoop.fs.defaultFS\":\"file:///\"}",
  "spark_master": "local[2]",
  "spark_executor_instances": "1",
  "spark_executor_cores": "1",
  "spark_executor_memory": "1g",
  "spark_driver_memory": "1g",
  "catalog_type": "rest",
  "catalog_uri": "http://localhost:9001/iceberg",
  "warehouse_location": ""
}
```

## Strategy submission configuration

`submit-strategy-jobs` needs optimizer CLI config. This is a minimal working example:

```properties
gravitino.optimizer.gravitinoUri = http://localhost:8090
gravitino.optimizer.gravitinoMetalake = test
gravitino.optimizer.gravitinoDefaultCatalog = rest_catalog
gravitino.optimizer.recommender.statisticsProvider = gravitino-statistics-provider
gravitino.optimizer.recommender.strategyProvider = gravitino-strategy-provider
gravitino.optimizer.recommender.tableMetaProvider = gravitino-table-metadata-provider
gravitino.optimizer.recommender.jobSubmitter = gravitino-job-submitter
gravitino.optimizer.strategyHandler.iceberg-data-compaction.className = org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionStrategyHandler
gravitino.optimizer.jobSubmitterConfig.catalog_name = rest_catalog
gravitino.optimizer.jobSubmitterConfig.spark_master = local[2]
gravitino.optimizer.jobSubmitterConfig.spark_executor_instances = 1
gravitino.optimizer.jobSubmitterConfig.spark_executor_cores = 1
gravitino.optimizer.jobSubmitterConfig.spark_executor_memory = 1g
gravitino.optimizer.jobSubmitterConfig.spark_driver_memory = 1g
gravitino.optimizer.jobSubmitterConfig.catalog_type = rest
gravitino.optimizer.jobSubmitterConfig.catalog_uri = http://localhost:9001/iceberg
gravitino.optimizer.jobSubmitterConfig.warehouse_location =
gravitino.optimizer.jobSubmitterConfig.spark_conf = {"spark.master":"local[2]","spark.hadoop.fs.defaultFS":"file:///"}
```

`--strategy-name` must be the policy name, for example `iceberg_compaction_default`.

## Local filesystem note

If your environment is local and not HDFS-based, set:

```properties
spark.hadoop.fs.defaultFS=file:///
```

Without this, Spark jobs may try `hdfs://localhost:9000` and fail.

## Recommended validation checklist

- Job templates exist: `builtin-iceberg-update-stats`, `builtin-iceberg-rewrite-data-files`.
- Policies are attached to target tables.
- `submit-strategy-jobs` prints `SUBMIT` lines.
- Rewrite logs show `Rewritten data files: <N>` where `N > 0` for non-empty tables.

## Related docs

- [Table Maintenance Service (Optimizer)](./optimizer.md)
- [Optimizer Quick Start and Verification](./optimizer-quick-start.md)
- [Optimizer CLI Reference](./optimizer-cli-reference.md)
- [Optimizer Troubleshooting](./optimizer-troubleshooting.md)
