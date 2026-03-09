---
title: "Table Maintenance Service (Optimizer)"
slug: /table-maintenance-service
keyword: table maintenance, optimizer, statistics, metrics, monitor
license: This software is licensed under the Apache License version 2.
---

## What is this service

The Table Maintenance Service (Optimizer) automates table maintenance by connecting:

- Statistics and metrics collection
- Rule evaluation and strategy recommendation
- Job template based execution

The CLI command and configuration keys keep using the `optimizer` name for compatibility.

## Who should read this page

- Platform administrators: deploy and configure server-side job execution and templates.
- Data platform engineers: define policies and run built-in maintenance workflows.
- CLI users: run local calculators and submit/observe jobs from scripts.

## Architecture overview

The optimizer workflow is based on six parts:

1. Metadata objects: catalog/schema/table in a metalake.
2. Statistics and metrics: table/partition signals used for decision making.
3. Policies: strategy intent, for example `system_iceberg_compaction`.
4. Job templates: executable contracts, for example built-in Spark templates.
5. Job executor: local or custom backend that runs submitted jobs.
6. Status and logs: REST job state plus local staging logs.

Typical data flow:

1. Collect statistics/metrics for target tables.
2. Evaluate rules and produce candidate actions.
3. Submit jobs using a concrete template and `jobConf`.
4. Track status and verify result on table metadata.

## Execution modes

| Mode | Main entry | Best for | Output |
| --- | --- | --- | --- |
| Built-in maintenance workflow | Gravitino REST + built-in templates | Server-side operational runs | Submitted Spark jobs and updated metadata |
| Optimizer CLI local calculator | `gravitino-optimizer.sh` | Local file-driven testing and batch scripts | Statistics/metrics updates and optional submissions |

Use built-in maintenance workflow when you want policy-driven server execution.
Use CLI local calculator when you want to feed JSONL input directly.

## Lifecycle

### 1. Collect

Generate or ingest table and partition statistics/metrics.

### 2. Evaluate

Apply policies/rules to decide if maintenance should run.

### 3. Submit

Pick a job template and submit job with concrete `jobConf`.

### 4. Observe

Check REST job status and validate resulting statistics, metrics, or rewritten data files.

## Configuration model

| Layer | Scope | Typical keys |
| --- | --- | --- |
| Gravitino server config | Runtime for job manager and executor | `gravitino.job.executor`, `gravitino.job.statusPullIntervalInMs`, `gravitino.jobExecutor.local.sparkHome` |
| Job submission `jobConf` | Per job run | `catalog_name`, `table_identifier`, `spark_*`, template-specific args |
| Optimizer CLI config | CLI commands | `gravitino.optimizer.*` in `conf/gravitino-optimizer.conf` |

## Before you start

- Prepare a running Gravitino server.
- Ensure target metalake exists (examples use `test`).
- Configure `SPARK_HOME` or `gravitino.jobExecutor.local.sparkHome` for Spark templates.
- For CLI mode, prepare `conf/gravitino-optimizer.conf` from template.
- Use fully qualified identifiers where possible, for example `catalog.schema.table`.

## Quick start

### Quick start A: built-in table maintenance workflow

This workflow uses:

- Built-in policy type: `system_iceberg_compaction`
- Built-in update stats job template: `builtin-iceberg-update-stats`
- Built-in rewrite data files job template: `builtin-iceberg-rewrite-data-files`

#### 1. Preflight checks

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

#### 2. Prepare demo metadata objects

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

#### 3. Create and attach built-in compaction policy

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

#### 4. Submit built-in update stats job

```bash
job_id=$(curl -sS -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "jobTemplateName": "builtin-iceberg-update-stats",
    "jobConf": {
      "catalog_name": "rest_catalog",
      "table_identifier": "db.t1",
      "update_mode": "all",
      "updater_options": "{\"gravitino_uri\":\"http://localhost:8090\",\"metalake\":\"test\",\"statistics_updater\":\"gravitino-statistics-updater\",\"metrics_updater\":\"gravitino-metrics-updater\"}",
      "spark_conf": "{}",
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

echo "update-stats job id: ${job_id}"
```

#### 5. Submit built-in rewrite data files job

```bash
rewrite_job_id=$(curl -sS -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "jobTemplateName": "builtin-iceberg-rewrite-data-files",
    "jobConf": {
      "catalog_name": "rest_catalog",
      "table_identifier": "db.t1",
      "strategy": "binpack",
      "sort_order": "",
      "where_clause": "",
      "options": "",
      "spark_master": "local[2]",
      "spark_executor_instances": "1",
      "spark_executor_cores": "1",
      "spark_executor_memory": "1g",
      "spark_driver_memory": "1g",
      "catalog_type": "rest",
      "catalog_uri": "http://localhost:9001/iceberg",
      "warehouse_location": "",
      "spark_conf": "{\"spark.master\":\"local[2]\"}"
    }
  }' \
  http://localhost:8090/api/metalakes/test/jobs/runs | jq -r '.job.jobId')

echo "rewrite job id: ${rewrite_job_id}"
```

#### 6. Track status and verify results

```bash
# Check job status by id
curl -sS "http://localhost:8090/api/metalakes/test/jobs/runs/${job_id}" | jq
curl -sS "http://localhost:8090/api/metalakes/test/jobs/runs/${rewrite_job_id}" | jq

# Verify table statistics after update-stats
curl -sS "http://localhost:8090/api/metalakes/test/objects/table/rest_catalog.db.t1/statistics" | jq
```

By default, Gravitino pulls job status every `300000` ms (`gravitino.job.statusPullIntervalInMs`).
REST status may lag real Spark process state by up to about 5 minutes.

For quick verification:

- Reduce `gravitino.job.statusPullIntervalInMs` in `gravitino.conf` (for example `5000`) and restart Gravitino.
- Check local Spark logs under `/tmp/gravitino/jobs/staging/<metalake>/<job-template-name>/<job-id>/error.log`.

### Quick start B: optimizer CLI (local calculator)

#### 1. Minimal configuration

Set these in `conf/gravitino-optimizer.conf`:

```properties
gravitino.optimizer.gravitinoUri = http://localhost:8090
gravitino.optimizer.gravitinoMetalake = test
gravitino.optimizer.gravitinoDefaultCatalog = generic
```

#### 2. Prepare a local JSONL file

Create `table-stats.jsonl`:

```json
{"stats-type":"table","identifier":"catalog.db.sales","row_count":100000,"data_size":8388608,"timestamp":1735689600}
{"stats-type":"partition","identifier":"catalog.db.sales","partition-path":{"dt":"2026-01-01"},"row_count":12000,"data_size":1048576,"timestamp":1735689600}
```

#### 3. Update statistics

```bash
./bin/gravitino-optimizer.sh \
  --type update-statistics \
  --calculator-name local-stats-calculator \
  --file-path ./table-stats.jsonl \
  --conf-path ./conf/gravitino-optimizer.conf
```

Expected output:

```text
SUMMARY: statistics totalRecords=... tableRecords=... partitionRecords=... jobRecords=...
```

## Command quick reference

Use `--help` to list all commands, or `--help --type <command>` for command-specific help.

| Command (`--type`) | Required options | Optional options | Purpose |
| --- | --- | --- | --- |
| `submit-strategy-jobs` | `--identifiers`, `--strategy-name` | `--dry-run`, `--limit` | Recommend and optionally submit jobs |
| `update-statistics` | `--calculator-name` | `--identifiers`, `--statistics-payload`, `--file-path` | Calculate and persist statistics |
| `append-metrics` | `--calculator-name` | `--identifiers`, `--statistics-payload`, `--file-path` | Calculate and append metrics |
| `monitor-metrics` | `--identifiers`, `--action-time` | `--range-seconds`, `--partition-path` | Evaluate rules with before/after metrics |
| `list-table-metrics` | `--identifiers` | `--partition-path` | Query stored table/partition metrics |
| `list-job-metrics` | `--identifiers` | None | Query stored job metrics |

## Input format for `local-stats-calculator`

`local-stats-calculator` reads JSON Lines (one JSON object per line).

### Reserved fields

- `stats-type`: `table`, `partition`, or `job`
- `identifier`: object identifier
- `partition-path`: only for partition data, for example `{"dt":"2026-01-01"}`
- `timestamp`: epoch seconds

All other fields are treated as metric/statistic values.

### Supported value forms

Both forms are supported:

```json
{"stats-type":"table","identifier":"catalog.db.t1","row_count":100}
{"stats-type":"table","identifier":"catalog.db.t1","row_count":{"value":100,"timestamp":1735689600}}
```

### Identifier rules

- Table and partition records: `catalog.schema.table`
- If `gravitino.optimizer.gravitinoDefaultCatalog` is set, `schema.table` is also accepted
- Job records: parsed as a regular Gravitino `NameIdentifier`

## CLI workflow examples

### Update statistics in batch

```bash
./bin/gravitino-optimizer.sh \
  --type update-statistics \
  --calculator-name local-stats-calculator \
  --file-path ./table-stats.jsonl \
  --conf-path ./conf/gravitino-optimizer.conf
```

### Append metrics in batch

```bash
./bin/gravitino-optimizer.sh \
  --type append-metrics \
  --calculator-name local-stats-calculator \
  --file-path ./table-stats.jsonl \
  --conf-path ./conf/gravitino-optimizer.conf
```

### Dry-run strategy submission

```bash
./bin/gravitino-optimizer.sh \
  --type submit-strategy-jobs \
  --identifiers catalog.db.sales,catalog.db.orders \
  --strategy-name compaction-high-file-count \
  --dry-run \
  --limit 10 \
  --conf-path ./conf/gravitino-optimizer.conf
```

### Monitor metrics

```bash
./bin/gravitino-optimizer.sh \
  --type monitor-metrics \
  --identifiers catalog.db.sales \
  --action-time 1735689600 \
  --range-seconds 86400 \
  --conf-path ./conf/gravitino-optimizer.conf
```

You can configure evaluator rules in `gravitino-optimizer.conf`:

```properties
gravitino.optimizer.monitor.gravitinoMetricsEvaluator.rules = table:row_count:avg:le,job:duration:latest:le
```

Rule format is `scope:metricName:aggregation:comparison`:

- `scope`: `table` or `job` (`table` rules also apply to partition scope)
- `aggregation`: `max|min|avg|latest`
- `comparison`: `lt|le|gt|ge|eq|ne`

## Output guide

- `SUMMARY: ...`: results for `update-statistics` and `append-metrics`
- `DRY-RUN: ...`: recommendation preview without job submission
- `SUBMIT: ...`: job submitted successfully
- `MetricsResult{...}`: returned by list commands
- `EvaluationResult{...}`: returned by monitor command

## Troubleshooting

### `Invalid --type`

Use kebab-case values such as `update-statistics`, not `update_statistics`.

### `--statistics-payload and --file-path cannot be used together`

For `local-stats-calculator`, use exactly one of them.

### `requires one of --statistics-payload or --file-path`

When `--calculator-name local-stats-calculator` is used, one input source is required.

### `--partition-path must be a JSON array`

Use a JSON array format, for example:

```text
[{"dt":"2026-01-01"}]
```

### Job status appears stale (`queued` or `started` for a long time)

Check `gravitino.job.statusPullIntervalInMs` and local staging logs under:

`/tmp/gravitino/jobs/staging/<metalake>/<job-template-name>/<job-id>/error.log`.

### `Specified optimizer config file does not exist`

Check your `--conf-path` and file permissions.

## Related docs

- [Manage policies in Gravitino](./manage-policies-in-gravitino.md)
- [Iceberg compaction policy](./iceberg-compaction-policy.md)
- [Manage jobs in Gravitino](./manage-jobs-in-gravitino.md)
- [Manage statistics in Gravitino](./manage-statistics-in-gravitino.md)
