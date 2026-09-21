---
title: "Configuration"
slug: "/table-maintenance-service/optimizer-configuration"
keywords:
  - table maintenance
  - configuration
license: "This software is licensed under the Apache License version 2."
---

Three layers of configuration apply, and they are set in different places for different lifetimes. Server configuration governs how jobs run at all, CLI configuration governs how the commands reach Gravitino, and `jobConf` governs a single job submission.

| Layer               | Where it lives                  | Lifetime            |
|---------------------|----------------------------------|---------------------|
| Server              | `gravitino.conf`                 | Until server restart |
| CLI                 | `conf/gravitino-optimizer.conf`  | Per command          |
| Job submission      | `jobConf` in the request body    | One job run          |

## Server Configuration

Set these in `gravitino.conf`. They control the job executor rather than maintenance itself, so they apply to every job Gravitino runs.

```properties
gravitino.job.executor=local
gravitino.job.statusPullIntervalInMs=300000
gravitino.jobExecutor.local.sparkHome=/path/to/spark
```

`gravitino.job.statusPullIntervalInMs` defaults to five minutes. Job status is polled rather than pushed, so REST status can lag the real Spark process by a full interval, which makes a working job look hung. Lower it to `10000` for local work and restart the server.

## CLI Configuration

The CLI needs to know where Gravitino is and which components to use. This is a minimal working file for `submit-strategy-jobs`.

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
# Leave empty for a local filesystem; set to your warehouse URI for cloud or HDFS storage.
gravitino.optimizer.jobSubmitterConfig.warehouse_location =
gravitino.optimizer.jobSubmitterConfig.spark_conf = {"spark.master":"local[2]","spark.hadoop.fs.defaultFS":"file:///"}
```

When the Gravitino server has authentication enabled, `builtin-iceberg-update-stats` needs
credentials in `--updater-options` / `updater_options` for the Gravitino client (statistics
updater and `submit-update-stats-job` `runJob` calls):

| `auth_type`      | Fields                                                                                         |
|------------------|------------------------------------------------------------------------------------------------|
| `none` (default) | (none)                                                                                         |
| `simple`         | `username` (optional)                                                                          |
| `basic`          | `username`, `password`                                                                         |
| `oauth`          | client-credentials only: `oauth_server_uri`, `oauth_path`, `oauth_credential`, `oauth_scope` |

Iceberg REST catalog authentication is separate: set `rest.auth.*` in `spark-conf` for any built-in
job that talks to a secured IRC (including update-stats). Expire-snapshots and rewrite-data-files
do not read `updater_options` auth fields.

Passwords and OAuth credentials in `updater_options` (and secrets in `spark_conf`) travel with
the job command line and `jobConf`; avoid logging raw `jobConf` (the submit-update-stats CLI
redacts them in DRY-RUN / SUBMIT output).

Everything under `gravitino.optimizer.jobSubmitterConfig.` becomes the `jobConf` of jobs this CLI submits, so the two layers carry the same keys under different names.

## Job Submission Configuration

A direct job submission carries its own `jobConf`. This is `builtin-iceberg-update-stats` with the keys it needs.

```json
{
  "catalog_name": "rest_catalog",
  "table_identifier": "db.t1",
  "update_mode": "all",
  "updater_options": "{\"gravitino_uri\":\"http://localhost:8090\",\"metalake\":\"test\",\"statistics_updater\":\"gravitino-statistics-updater\",\"metrics_updater\":\"gravitino-metrics-updater\",\"auth_type\":\"basic\",\"username\":\"admin\",\"password\":\"YourSecureGravitinoPassword\"}",
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

`updater_options` and `spark_conf` are JSON strings inside a JSON object, so their quotes are escaped. That nesting is the most common source of malformed submissions.

Built-in Iceberg templates list optional keys as `--flag` + `{{placeholder}}`. Omitting a key from
`jobConf` does not remove that flag from the submitted command; it can leave a dangling flag such as
`--updater-options` with no value. Prefer sending an explicit value for each placeholder you use
(or a documented default) instead of dropping the key. See
[Built-in Job Templates](./optimizer-cli-reference.md#built-in-job-templates).

Built-in Iceberg templates also need an Iceberg Spark runtime on the Spark classpath. They do not
ship that JAR or fill template `jars`, so include it yourself — for example
`"spark.jars":"/path/to/iceberg-spark-runtime-....jar"` inside `spark_conf`. Match the artifact to
your Spark, Scala, and Iceberg versions. Details are under
[Built-in Job Templates](./optimizer-cli-reference.md#built-in-job-templates).

`warehouse_location` may be empty for local filesystem testing. Set it to the warehouse URI for HDFS or cloud object storage.

## Running Against a Local Filesystem

On a machine with no HDFS, Spark still defaults to `hdfs://localhost:9000` and fails. Set the default filesystem explicitly, in `spark_conf` for job submissions and in the CLI `spark_conf` value:

```properties
spark.hadoop.fs.defaultFS=file:///
```

## Checking Your Configuration

Four things are worth confirming before assuming a configuration problem is a code problem.

- `builtin-iceberg-update-stats` and `builtin-iceberg-rewrite-data-files` appear in the job template list.
- The policy is associated with a tag assigned to the target table or one of its ancestors.
- `submit-strategy-jobs` prints `SUBMIT` lines rather than nothing.
- The rewrite log shows `Rewritten data files: N` with `N` greater than zero for a non-empty table.

## Related

- [Table Maintenance Service](./optimizer.md) for the concepts and the walkthrough
- [CLI Reference](./optimizer-cli-reference.md) for every command and the built-in job templates
- [Troubleshooting](./optimizer-troubleshooting.md) when a command or job fails
- [Extension Guide](./optimizer-extension-guide.md) for custom strategies and providers
