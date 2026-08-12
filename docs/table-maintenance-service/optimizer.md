---
title: "Table Maintenance Service"
slug: "/table-maintenance-service/optimizer"
keywords:
  - table maintenance
  - compaction
  - statistics
  - quick start
license: "This software is licensed under the Apache License version 2."
---

## Overview

The table maintenance service keeps tables healthy without anyone watching them. You attach a policy to a catalog, schema, or table; the service collects statistics, evaluates them against that policy, and submits a job when the policy says work is needed.

The framework is generic. Metrics collection, policy evaluation, and job submission are not tied to any particular table format, and each is a Java ServiceLoader extension point. What ships built in is deliberately narrower, and in alpha that means Iceberg data file compaction on identity-partitioned tables.

The CLI binary, its configuration file, and its configuration keys carry the older name `optimizer`, so you will see `gravitino-optimizer.sh`, `gravitino-optimizer.conf`, and `gravitino.optimizer.*` throughout. Those are literal strings rather than a second product.

## Alpha Scope

Confirm your environment matches this list before starting an evaluation against the built-ins. Anything outside it needs a custom extension, which is covered in the [Extension Guide](./optimizer-extension-guide.md).

- Compaction is the only built-in strategy. There is no built-in snapshot expiration, orphan file cleanup, or sort and cluster maintenance.
- Compaction applies to Iceberg tables only, and only where every partition uses an identity transform.
- The service is driven through the CLI workflow rather than running on a schedule of its own.


## How It Works

Maintenance runs as four steps. Each is a separate command, so you can stop after any of them, and the dry run on step two shows what would be submitted before anything runs.

| Step     | What you run                                      | What it produces                                        |
|----------|---------------------------------------------------|---------------------------------------------------------|
| Collect  | `update-statistics`, `append-metrics`             | Statistics on the table, metrics in the JDBC repository |
| Evaluate | `submit-strategy-jobs --dry-run`                  | Candidate actions, with nothing submitted               |
| Submit   | `submit-strategy-jobs`, `submit-update-stats-job` | A Spark job, tracked by job status and staging logs     |
| Observe  | `monitor-metrics`, `list-table-metrics`           | Before and after metrics, and rewritten data files      |

## Execution Modes

There are two ways in, and they differ in where the numbers come from rather than in what they do.

The built-in workflow drives everything through the Gravitino server and its job templates, using the policy attached to a table to decide what runs. Use it for server-side operational runs.

The local calculator reads a JSONL file you supply and updates statistics and metrics directly from it. Use it for testing and batch scripts, where you already have the numbers and want to feed them in without the server computing them.

## Naming

Three identifiers look interchangeable and are not.

| Term          | Example                      | Where it appears                                           |
|---------------|------------------------------|------------------------------------------------------------|
| Policy name   | `iceberg_compaction_default` | The policy's own name, and the CLI `--strategy-name`       |
| Policy type   | `system_iceberg_compaction`  | The `policyType` field when creating a policy over REST    |
| Strategy type | `iceberg-data-compaction`    | The `strategy.type` field, and the strategy handler config |

`--strategy-name` takes the **policy name**, despite what it is called. Passing either of the other two reports no matching identifiers rather than naming the mistake.

## Walkthrough

This takes one Iceberg table through the whole workflow: create it, fill it with small files, attach a compaction policy, collect statistics, and let the service decide to compact it. It runs against a local Spark and takes about fifteen minutes.

Each step ends with a check. If a check fails, stop there, since every step depends on the one before it.

### Prerequisites

- A running Gravitino server with a metalake. The examples use `test`.
- Spark available to the job executor, through either `SPARK_HOME` or `gravitino.jobExecutor.local.sparkHome`.
- `gravitino.job.statusPullIntervalInMs` lowered to `10000` and the server restarted. The default is five minutes, which makes every status check in this walkthrough feel broken.

If your Iceberg REST backend runs in memory, do not restart it partway through. Restarting resets both metadata and data files, and you start over.

### Step 1: Confirm the Job Templates Exist

```bash
curl -sS "http://localhost:8090/api/metalakes/test" | jq
curl -sS "http://localhost:8090/api/metalakes/test/jobs/templates?details=true" \
  | jq '.jobTemplates[].name'
```

The template list must include `builtin-iceberg-update-stats` and `builtin-iceberg-rewrite-data-files`. If it does not, the `gravitino-jobs` JAR is missing from `auxlib`. Add it and restart the server before going on.

### Step 2: Create the Demo Catalog, Schema, and Table

```bash
# Catalog. An "already exists" error here is fine.
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "rest_catalog",
    "type": "RELATIONAL",
    "comment": "Iceberg REST catalog",
    "provider": "lakehouse-iceberg",
    "properties": {
      "catalog-backend": "rest",
      "uri": "http://localhost:9001/iceberg"
    }
  }' \
  http://localhost:8090/api/metalakes/test/catalogs

# Schema
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{"name": "db", "comment": "maintenance demo schema", "properties": {}}' \
  http://localhost:8090/api/metalakes/test/catalogs/rest_catalog/schemas

# Table
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "t1",
    "comment": "maintenance demo table",
    "columns": [
      {"name": "id", "type": "integer", "nullable": true},
      {"name": "name", "type": "string", "nullable": true}
    ],
    "properties": {}
  }' \
  http://localhost:8090/api/metalakes/test/catalogs/rest_catalog/schemas/db/tables
```

### Step 3: Create Something Worth Compacting

An empty table gives the policy nothing to react to, so write 100,000 rows capped at 1,000 rows per file. That produces the many small files compaction exists to merge.

```bash
${SPARK_HOME}/bin/spark-sql \
  --conf spark.hadoop.fs.defaultFS=file:/// \
  --conf spark.sql.catalog.rest_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.rest_catalog.type=rest \
  --conf spark.sql.catalog.rest_catalog.uri=http://localhost:9001/iceberg \
  -e "CREATE NAMESPACE IF NOT EXISTS rest_catalog.db; \
      SET spark.sql.files.maxRecordsPerFile=1000; \
      INSERT INTO rest_catalog.db.t1 \
      SELECT id, concat('name_', CAST(id AS STRING)) FROM range(0, 100000);"
```

Without `spark.hadoop.fs.defaultFS=file:///`, Spark reaches for `hdfs://localhost:9000` and fails.

### Step 4: Attach a Compaction Policy

Creating the policy is not enough. It has to be attached to the table, and the attachment is what the service reads.

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "iceberg_compaction_default",
    "comment": "Built-in Iceberg compaction policy",
    "policyType": "system_iceberg_compaction",
    "enabled": true,
    "content": {}
  }' \
  http://localhost:8090/api/metalakes/test/policies

curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{"policiesToAdd": ["iceberg_compaction_default"]}' \
  http://localhost:8090/api/metalakes/test/objects/table/rest_catalog.db.t1/policies
```

Confirm the attachment before moving on:

```bash
curl -sS "http://localhost:8090/api/metalakes/test/objects/table/rest_catalog.db.t1/policies?details=true" | jq
```

### Step 5: Collect Statistics

```bash
update_stats_job_id=$(curl -sS -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "jobTemplateName": "builtin-iceberg-update-stats",
    "jobConf": {
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
  }' \
  http://localhost:8090/api/metalakes/test/jobs/runs | jq -r '.job.jobId')

echo "update-stats job id: ${update_stats_job_id}"
```

Wait for it to finish, then confirm the statistics landed:

```bash
curl -sS "http://localhost:8090/api/metalakes/test/objects/table/rest_catalog.db.t1/statistics" | jq
```

The response must include `custom-data-file-mse` and `custom-delete-file-number`. Those two are what the compaction policy evaluates, so if they are absent the next step has nothing to decide on.

### Step 6: Evaluate and Submit

Write the CLI configuration first. `--strategy-name` takes the **policy name**, not the policy type or the strategy type.

```bash
cat > /tmp/gravitino-optimizer-submit.conf <<'EOF_CONF'
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
EOF_CONF
```

Preview first. A dry run evaluates the policy and prints what it would do without submitting anything.

```bash
./bin/gravitino-optimizer.sh \
  --type submit-strategy-jobs \
  --identifiers rest_catalog.db.t1 \
  --strategy-name iceberg_compaction_default \
  --dry-run \
  --limit 10 \
  --conf-path /tmp/gravitino-optimizer-submit.conf
```

`DRY-RUN` lines mean the policy fired. No output at all means it did not, which usually means the statistics from step 5 are below the policy thresholds rather than that anything is broken.

Then submit for real:

```bash
submit_output=$(./bin/gravitino-optimizer.sh \
  --type submit-strategy-jobs \
  --identifiers rest_catalog.db.t1 \
  --strategy-name iceberg_compaction_default \
  --limit 10 \
  --conf-path /tmp/gravitino-optimizer-submit.conf)
echo "${submit_output}"

strategy_job_id=$(echo "${submit_output}" | sed -n 's/.*jobId=\([^[:space:]]*\).*/\1/p')
[[ -z "${strategy_job_id}" ]] && echo 'ERROR: failed to extract strategy job ID' && exit 1
echo "strategy rewrite job id: ${strategy_job_id}"
```

### Step 7: Verify the Rewrite

```bash
curl -sS "http://localhost:8090/api/metalakes/test/jobs/runs/${update_stats_job_id}" | jq
curl -sS "http://localhost:8090/api/metalakes/test/jobs/runs/${strategy_job_id}" | jq

log_dir="/tmp/gravitino/jobs/staging/test/builtin-iceberg-rewrite-data-files/${strategy_job_id}"
grep -E "Rewritten data files|Added data files|completed successfully" "${log_dir}/output.log"
```

`Rewritten data files: N` with `N` greater than zero means the workflow worked end to end. The staging path comes from `gravitino.job.stagingDir`, which defaults to `/tmp/gravitino/jobs/staging`.

REST job status is polled rather than pushed, so it lags the real Spark process by up to one poll interval. That is why the prerequisites lower it to ten seconds.

## Related

- [Configuration](./optimizer-configuration.md) for the three configuration layers
- [CLI Reference](./optimizer-cli-reference.md) for every command and the built-in job templates
- [Troubleshooting](./optimizer-troubleshooting.md) when something above does not behave
- [Extension Guide](./optimizer-extension-guide.md) for custom strategies and providers
- [Iceberg Compaction Policy](../iceberg-compaction-policy.md) for tuning the built-in strategy
