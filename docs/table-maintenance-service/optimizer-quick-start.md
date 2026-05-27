---
title: "Optimizer Quick Start"
slug: "/table-maintenance-service/optimizer-quick-start"
keyword: "table maintenance, optimizer, quick start, compaction, update stats"
license: "This software is licensed under the Apache License version 2."
---

The walkthrough is a manual end-to-end sequence that produces one compacted instance of a demo Iceberg table. Each step is a command you run by hand. The optimizer does not include a built-in scheduler; to operationalize compaction in production, trigger `submit-strategy-jobs` on a schedule from cron, Airflow, or your existing job scheduler.

## Prerequisites

- **Gravitino server.** A running Gravitino server is required at `http://localhost:8090` (the default). Start one if needed before continuing.
- **Iceberg REST catalog endpoint.** The walkthrough uses `http://localhost:9001/iceberg`. Gravitino's default installation runs an in-memory Iceberg REST service at that endpoint, so no extra setup is needed. Avoid restarting Gravitino during the walkthrough because the in-memory backend resets on restart. To use an external Iceberg REST catalog (Lakekeeper, Apache Polaris, Snowflake Open Catalog), substitute its URI in the catalog creation in Step 2 and in the `catalog_uri` fields in Steps 5 and 6.
- **Metalake.** The examples use a metalake named `test`. Create one if it does not already exist.
- **Spark.** The Spark templates need Spark binaries on the server. Either set `SPARK_HOME` in the environment that started the Gravitino server, or set `gravitino.jobExecutor.local.sparkHome` in `${GRAVITINO_HOME}/conf/gravitino.conf` and restart. See [Optimizer Configuration](./optimizer-configuration.md) for related server config keys.
- **Faster status feedback (optional).** Gravitino pulls job status every `gravitino.job.statusPullIntervalInMs` milliseconds (default `300000`, about 5 minutes). To see status changes faster during the walkthrough, set this key to `10000` in `${GRAVITINO_HOME}/conf/gravitino.conf` and restart.

## Workflow

The walkthrough uses these built-in capabilities:

- Policy type: `system_iceberg_compaction`
- Update-stats job template: `builtin-iceberg-update-stats`
- Compaction job template: `builtin-iceberg-rewrite-data-files`

### Step 1: Preflight Checks

```bash
# Check metalake
curl -sS "http://localhost:8090/api/metalakes/test" | jq

# Check built-in templates
curl -sS "http://localhost:8090/api/metalakes/test/jobs/templates?details=true" | jq '.jobTemplates[].name'
```

**Success check:** both `builtin-iceberg-update-stats` and `builtin-iceberg-rewrite-data-files` appear in the output. If either is missing, ensure the `gravitino-jobs` JAR is in the Gravitino server's `auxlib` directory and restart.

### Step 2: Prepare Demo Metadata Objects

Create a REST Iceberg catalog, schema, and table:

```bash
# Create catalog (ignore "already exists" errors)
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

# Create schema
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "db",
    "comment": "optimizer demo schema",
    "properties": {}
  }' \
  http://localhost:8090/api/metalakes/test/catalogs/rest_catalog/schemas

# Create table
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "t1",
    "comment": "optimizer demo table",
    "columns": [
      {"name": "id", "type": "integer", "nullable": true},
      {"name": "name", "type": "string", "nullable": true}
    ],
    "properties": {}
  }' \
  http://localhost:8090/api/metalakes/test/catalogs/rest_catalog/schemas/db/tables
```

**Success check:** each curl returns a JSON response describing the created object. If a curl returns an "already exists" error, the object was created on a previous run; safe to proceed.

### Step 3: Seed Demo Data

Use Spark SQL to create enough small files so compaction has visible effect:

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

**Success check:** Spark exits without error. The table `rest_catalog.db.t1` now contains 100,000 rows distributed across multiple small data files.

### Step 4: Create and Attach Built-In Compaction Policy

```bash
# Create policy
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "iceberg_compaction_default",
    "comment": "Built-in iceberg compaction policy",
    "policyType": "system_iceberg_compaction",
    "enabled": true,
    "content": {}
  }' \
  http://localhost:8090/api/metalakes/test/policies

# Attach policy to table
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "policiesToAdd": ["iceberg_compaction_default"]
  }' \
  http://localhost:8090/api/metalakes/test/objects/table/rest_catalog.db.t1/policies
```

Verify association:

```bash
curl -sS "http://localhost:8090/api/metalakes/test/objects/table/rest_catalog.db.t1/policies?details=true" | jq
```

**Success check:** the response lists `iceberg_compaction_default` among the policies attached to table `rest_catalog.db.t1`.

### Step 5: Submit Built-In Update-Stats Job

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

The `updater_options` and `spark_conf` values are JSON objects passed as escaped strings, which is what the `jobConf` API expects for nested configuration.

**Success check:** the echo line shows a non-empty job ID. The actual job runs asynchronously on Spark; verify completion in Step 7.

### Step 6: Trigger Rewrite Submission with `submit-strategy-jobs`

```bash
# Required optimizer CLI config for strategy submission.
# Note: --strategy-name is policy name, not strategy.type.
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
# Leave empty for local filesystem; set to your warehouse URI for cloud/HDFS storage.
gravitino.optimizer.jobSubmitterConfig.warehouse_location =
gravitino.optimizer.jobSubmitterConfig.spark_conf = {"spark.master":"local[2]","spark.hadoop.fs.defaultFS":"file:///"}
EOF_CONF

# Optional: preview recommendations without submitting jobs.
./bin/gravitino-optimizer.sh \
  --type submit-strategy-jobs \
  --identifiers rest_catalog.db.t1 \
  --strategy-name iceberg_compaction_default \
  --dry-run \
  --limit 10 \
  --conf-path /tmp/gravitino-optimizer-submit.conf

# Submit rewrite job through strategy evaluation.
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

**Success check:** the dry run prints lines starting with `DRY-RUN` for each partition the policy selects. The real run prints a line containing `SUBMIT` and `jobId=<id>`, and the echo line shows the captured rewrite job ID. The rewrite runs asynchronously on Spark; verify completion in Step 7.

### Step 7: Track Status and Verify Results

Job status visible through the REST API may lag the real Spark process by up to `gravitino.job.statusPullIntervalInMs` milliseconds (default `300000`, about 5 minutes). If you lowered this in the prerequisites, status will refresh faster.

**Check job status:**

```bash
curl -sS "http://localhost:8090/api/metalakes/test/jobs/runs/${update_stats_job_id}" | jq
curl -sS "http://localhost:8090/api/metalakes/test/jobs/runs/${strategy_job_id}" | jq
```

Each response shows the job's current `status`, which progresses through `QUEUED`, `STARTED`, and `SUCCEEDED` (or `FAILED`). Wait until both jobs reach `SUCCEEDED` before checking results. If either reports `FAILED`, inspect the staging log path shown in the response.

**Check metrics after the update-stats job:**

```bash
curl -sS "http://localhost:8090/api/metalakes/test/objects/table/rest_catalog.db.t1/statistics" | jq
```

After a successful update-stats run, the response includes entries for `custom-data-file-mse` and `custom-delete-file-number` with non-zero numeric values. Their presence confirms the update-stats job populated table metrics.

**Verify the rewrite produced output:**

```bash
# The staging directory root is controlled by `gravitino.job.stagingDir` (default: `/tmp/gravitino/jobs/staging`).
log_dir="/tmp/gravitino/jobs/staging/test/builtin-iceberg-rewrite-data-files/${strategy_job_id}"
grep -E "Rewritten data files|Added data files|completed successfully" \
  "${log_dir}/output.log"
```

Look for a line matching `Rewritten data files: N` where `N > 0`. If the grep returns nothing, confirm the staging directory matches the configured `gravitino.job.stagingDir` and that the rewrite job has reached `SUCCEEDED`.

**Success check:** both job status responses show `"status":"SUCCEEDED"`, the statistics response contains `custom-data-file-mse` and `custom-delete-file-number`, and the rewrite log shows `Rewritten data files: N` with `N > 0`.

## Related

- [Iceberg Compaction Policy](../iceberg-compaction-policy.md)
- [Optimizer Configuration](./optimizer-configuration.md)
- [Optimizer CLI Reference](./optimizer-cli-reference.md)
- [Optimizer Troubleshooting](./optimizer-troubleshooting.md)
