---
title: "Built-in Expire Snapshots Job"
slug: /table-maintenance-service/expire-snapshots
keyword: table maintenance, optimizer, expire snapshots, iceberg, metadata cleanup
license: This software is licensed under the Apache License version 2.
---

## Overview

The `builtin-iceberg-expire-snapshots` job template removes old Iceberg snapshots and their
associated metadata files. Without periodic expiration, snapshot JSON files and manifest lists
accumulate indefinitely, slowing table operations and wasting storage.

This job executes Iceberg's `expire_snapshots` stored procedure via Spark SQL.

## Job Template

| Property | Value |
| --- | --- |
| Name | `builtin-iceberg-expire-snapshots` |
| Type | Spark |
| Version | `v1` |
| Main class | `org.apache.gravitino.maintenance.jobs.iceberg.IcebergExpireSnapshotsJob` |

## Parameters

### Required

| Key | Description | Example |
| --- | --- | --- |
| `catalog_name` | Iceberg catalog name registered in Spark | `rest_catalog` |
| `table_identifier` | Fully qualified table name | `db.sample` |

### Optional

| Key | Description | Default |
| --- | --- | --- |
| `older_than` | Expire snapshots older than this timestamp (`yyyy-MM-dd HH:mm:ss`) | 5 days ago (Iceberg default) |
| `retain_last` | Minimum number of most recent snapshots to keep | 1 |
| `stream_results` | Flag: presence enables streaming of intermediate delete results | disabled |
| `spark_conf` | JSON map of custom Spark configurations | none |

## Usage

### Direct job submission via REST

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "jobTemplateName": "builtin-iceberg-expire-snapshots",
    "jobConf": {
      "catalog_name": "rest_catalog",
      "table_identifier": "db.t1",
      "older_than": "2024-01-01 00:00:00",
      "retain_last": "3",
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
  http://localhost:8090/api/metalakes/test/jobs
```

### Expire with only `retain_last`

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "jobTemplateName": "builtin-iceberg-expire-snapshots",
    "jobConf": {
      "catalog_name": "rest_catalog",
      "table_identifier": "db.t1",
      "retain_last": "5",
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
  http://localhost:8090/api/metalakes/test/jobs
```

### Check job status

```bash
curl -sS "http://localhost:8090/api/metalakes/test/jobs/<job-id>" | jq
```

## Generated SQL

The job builds and executes a Spark SQL statement:

```sql
CALL `rest_catalog`.system.expire_snapshots(
  table => 'db.t1',
  older_than => TIMESTAMP '2024-01-01 00:00:00',
  retain_last => 3,
  stream_results => true
)
```

Only non-empty optional parameters are included. The catalog identifier is backtick-quoted
for safety.

## Output

On success, the job logs:

```
Expire Snapshots Results:
  Deleted data files: 12
  Deleted manifest files: 8
  Deleted manifest lists: 3
```

## Verification

After running the job:

```bash
# Verify job completed
curl -sS "http://localhost:8090/api/metalakes/test/jobs/<job-id>" | jq '.job.state'
# Expected: "SUCCEEDED"

# Check staging logs
cat /tmp/gravitino/jobs/staging/test/builtin-iceberg-expire-snapshots/<job-id>/stdout.log
```

## Relationship to Other Jobs

| Job | Purpose |
| --- | --- |
| `builtin-iceberg-rewrite-data-files` | Compacts small data files |
| `builtin-iceberg-update-stats` | Collects file statistics and metrics |
| `builtin-iceberg-expire-snapshots` | Removes old snapshot metadata |

These jobs are complementary. A typical maintenance workflow runs update-stats first, then
compaction, then expire-snapshots to clean up the snapshot history created by compaction.
