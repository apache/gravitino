---
title: "Optimizer Quick Start and Verification"
slug: /table-maintenance-service/quick-start
keyword: table maintenance, optimizer, quick start, compaction, update stats
license: This software is licensed under the Apache License version 2.
---

## Before running quick start

- Prepare a running Gravitino server.
- Ensure target metalake exists (examples use `test`).
- Configure `SPARK_HOME` or `gravitino.jobExecutor.local.sparkHome` for Spark templates.
- If your Iceberg REST backend is in-memory, metadata is reset after restart.

For full config details, see [Optimizer Configuration](./optimizer-configuration.md).

## Success criteria

- Update-stats job finishes and statistics include `custom-data-file-mse` and `custom-delete-file-number`.
- `submit-strategy-jobs` prints `SUBMIT` with a rewrite job ID.
- Rewrite job log shows `Rewritten data files: <N>` where `N > 0` for non-empty tables.

## Quick start A: built-in table maintenance workflow

This workflow uses:

- Built-in policy type: `system_iceberg_compaction`
- Built-in update stats job template: `builtin-iceberg-update-stats`
- Built-in rewrite data files job template: `builtin-iceberg-rewrite-data-files`

### 1. Preflight checks

```bash
# Check metalake
curl -sS "http://localhost:8090/api/metalakes/test" | jq

# Check built-in templates
curl -sS "http://localhost:8090/api/metalakes/test/jobs/templates?details=true" | jq '.jobTemplates[].name'
```

Expected names include:

- `builtin-iceberg-update-stats`
- `builtin-iceberg-rewrite-data-files`

If missing, verify `gravitino-jobs` JAR in `auxlib`, then restart Gravitino.

### 2. Prepare demo metadata objects

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

### 3. Seed demo data (recommended)

Use Spark SQL to create enough small files so compaction has visible effect:

```bash
${SPARK_HOME}/bin/spark-sql \
  --conf spark.hadoop.fs.defaultFS=file:/// \
  --conf spark.sql.catalog.rest_demo=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.rest_demo.type=rest \
  --conf spark.sql.catalog.rest_demo.uri=http://localhost:9001/iceberg \
  -e "CREATE NAMESPACE IF NOT EXISTS rest_demo.db; \
      SET spark.sql.files.maxRecordsPerFile=1000; \
      INSERT INTO rest_demo.db.t1 \
      SELECT id, concat('name_', CAST(id AS STRING)) FROM range(0, 100000);"
```

### 4. Create and attach built-in compaction policy

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

### 5. Submit built-in update stats job

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

### 6. Trigger rewrite submission with `submit-strategy-jobs`

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

### 7. Track status and verify results

```bash
# Check job status by id
curl -sS "http://localhost:8090/api/metalakes/test/jobs/runs/${update_stats_job_id}" | jq
curl -sS "http://localhost:8090/api/metalakes/test/jobs/runs/${strategy_job_id}" | jq

# Verify table statistics after update-stats
curl -sS "http://localhost:8090/api/metalakes/test/objects/table/rest_catalog.db.t1/statistics" | jq

# Staging path is controlled by `gravitino.job.stagingDir` (default: `/tmp/gravitino/jobs/staging`).
# Verify rewrite actually rewrote files (N should be > 0 for non-empty table)
grep -E "Rewritten data files|Added data files|completed successfully" \
  "/tmp/gravitino/jobs/staging/test/builtin-iceberg-rewrite-data-files/${strategy_job_id}/error.log"
```

By default, Gravitino pulls job status every `300000` ms (`gravitino.job.statusPullIntervalInMs`).
REST status may lag real Spark process state by up to about 5 minutes.

## Next read

- [Optimizer Configuration](./optimizer-configuration.md)
- [Optimizer CLI Reference](./optimizer-cli-reference.md)
- [Optimizer Troubleshooting](./optimizer-troubleshooting.md)
