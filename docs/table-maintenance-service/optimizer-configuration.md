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

The policy-driven path needs the same keys. Its compaction adapter only derives the table, the
where clause and the rewrite options from the policy, so `catalog_name`, `catalog_type`,
`catalog_uri` and `warehouse_location` must come from `gravitino.optimizer.jobSubmitterConfig.`.
Missing ones are reported when the job is submitted, instead of failing later inside Spark.

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

Only some of these keys are required. The built-in templates give the optional ones a default, so
`jobConf` can leave them out; a submission that misses a required key is rejected with an error
that lists the missing keys. See
[Built-in Job Templates](./optimizer-cli-reference.md#built-in-job-templates) for the required keys
and the defaults of each template.

Built-in Iceberg templates also need an Iceberg Spark runtime on the Spark classpath. They do not
ship that JAR or fill template `jars`, so include it yourself — for example
`"spark.jars":"/path/to/iceberg-spark-runtime-....jar"` inside `spark_conf`. Match the artifact to
your Spark, Scala, and Iceberg versions. Details are under
[Built-in Job Templates](./optimizer-cli-reference.md#built-in-job-templates).

`warehouse_location` may be empty for local filesystem testing. Set it to the warehouse URI for HDFS or cloud object storage.

### Rewrite Manifests Job

Submit `builtin-iceberg-rewrite-manifests` directly through
`POST /api/metalakes/{metalake}/jobs/runs`. Its `jobConf` uses these job-specific keys:

| Key                | Meaning                                               | Default                                       |
| ------------------ | ----------------------------------------------------- | --------------------------------------------- |
| `catalog_name`     | Iceberg catalog registered in Spark                   | Required                                      |
| `table_identifier` | Table identifier, for example `db.t1`                 | Required                                      |
| `spec_id`          | Existing partition spec whose manifests to rewrite    | Current table spec                            |
| `use_caching`      | `true` or `false` to control caching during rewriting | Installed Iceberg default (`false` in 1.11.0) |
| `spark_conf`       | JSON string containing additional Spark settings      | None                                          |

Include the Spark and catalog template settings shown in the
[submission example](./optimizer-cli-reference.md#submitting-the-job), and make the matching
Iceberg Spark runtime available as described above. For this job, omitted, blank, or
unresolved optional argument placeholders are treated as absent. Required catalog/table
arguments cannot be blank or unresolved. `spec_id` must be a non-negative integer identifying
an existing spec; it does not repartition data or migrate manifests between specs.

This job currently supports direct submission. Policy -> Strategy -> Adapter integration
for automatic manifest maintenance will follow separately.

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

## Orphan File Cleanup Job Configuration

Submit `builtin-iceberg-remove-orphan-files` with the same Spark and catalog
settings described above. Its job-specific `jobConf` keys are:

| Key                | Meaning                                                                               | Default                          |
| ------------------ | ------------------------------------------------------------------------------------- | -------------------------------- |
| `catalog_name`     | Iceberg catalog registered in Spark                                                   | Required                         |
| `table_identifier` | Table identifier within the catalog, such as `db.sample`                              | Required                         |
| `older_than`       | Timestamp (explicit offset or Spark session time zone); must be at least 24 hours old | Three days ago (Iceberg default) |
| `location`         | Scan only this directory within the table's storage location                          | Table location                   |
| `dry_run`          | `true` logs candidate paths without deleting; `false` deletes                         | `false`                          |
| `spark_conf`       | JSON string containing custom Spark settings, including the Iceberg runtime if needed | None                             |

For direct template submission, supply `older_than: ""` and `location: ""` to
use the defaults, `dry_run: "false"` (or `"true"` to preview), and
`spark_conf: "{}"` when no overrides are needed.

Keep the three-day default unless your workload needs a longer retention window.
The 24-hour minimum also applies to dry runs; passing the current timestamp is
not supported. For a secured Iceberg REST catalog, supply its authentication
settings explicitly in `spark_conf`, as described above.

See [Remove Orphan Files](./optimizer-cli-reference.md#remove-orphan-files) for a
complete submission example. For policy-driven submission, configure the handler
and policy below. Periodic scheduling remains separate.

### Orphan cleanup policy integration

Register the handler alongside the existing optimizer providers:

```properties
gravitino.optimizer.strategyHandler.iceberg-orphan-file-removal.className = org.apache.gravitino.maintenance.optimizer.recommender.handler.orphan.OrphanFileRemovalStrategyHandler
```

The built-in job adapter is registered automatically. Keep the same Spark and
catalog submission configuration as for direct orphan cleanup, including
`catalog_name` (the catalog alias configured in Spark) and `spark_conf`.
The adapter supplies `table_identifier`, `older_than`, `location`, and `dry_run`.
These policy-derived values override shared submission defaults.

Create a policy through `POST /api/metalakes/{metalake}/policies`:

```json
{
  "name": "orphan_cleanup",
  "policyType": "system_iceberg_orphan_file_removal",
  "enabled": true,
  "content": {
    "olderThanDays": 3,
    "dryRun": true
  }
}
```

Associate the policy with a tag and attach that tag to the target table, schema,
or catalog, following the [policy setup walkthrough](./optimizer.md).
`olderThanDays` defaults to 3 and must be between 1 and 36500 inclusive (approximately 100 years). This bound rejects impractical retention values before a policy is stored. `dryRun` defaults to false.

The policy adapter subtracts `olderThanDays` from the current instant and always
emits `older_than` as an explicit UTC timestamp ending in `Z`. Policy retention
is therefore independent of `spark.sql.session.timeZone`. For direct job
submission, timestamps without an offset use the Spark session time zone.

Optional `location` must not have leading or trailing whitespace and must be the
table's storage root or a descendant; submission requires the table's `location` metadata to validate a custom path. The Spark job
rechecks containment and filesystem symlinks with its own credentials before
listing or deleting files.

The strategy operates on the whole table, including partitioned tables, and does
not need table or partition statistics. Each explicit optimizer invocation makes
an enabled, selected cleanup policy eligible with score 1. This does not install
a periodic scheduler, cooldown, or last-run tracking. Because the trigger is always
true and the score is always 1, every explicit `submit-strategy-jobs` invocation
for an enabled, selected orphan policy submits a Spark cleanup job. Unlike
threshold-based compaction, there is no metric gate to suppress repeated runs.
Operators must control the invocation rate through external scheduling. Start
with `dryRun: true` to inspect candidates; dry-run jobs still consume Spark
resources and also need rate limiting.

Use `submit-strategy-jobs --strategy-name orphan_cleanup` with the target
identifiers. The CLI `--dry-run` previews recommendations without submitting jobs;
the policy's `dryRun: true` submits a Spark job that lists candidates without
deleting them. Start with that policy setting to inspect candidates.
