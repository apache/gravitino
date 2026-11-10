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

The table maintenance service keeps tables healthy without anyone watching them. Associate a policy with a tag and assign that tag to a table or one of its ancestors. The service collects statistics, evaluates them against the table's derived policy, and submits a job when the policy says work is needed.

The framework is generic. Metrics collection, policy evaluation, and job submission are not tied to any particular table format, and each is a Java ServiceLoader extension point. What ships built in is deliberately narrower, and in alpha that includes Iceberg data file compaction on identity-partitioned tables and table-level orphan cleanup.

The CLI binary, its configuration file, and its configuration keys carry the older name `optimizer`, so you will see `gravitino-optimizer.sh`, `gravitino-optimizer.conf`, and `gravitino.optimizer.*` throughout. Those are literal strings rather than a second product.

## Alpha Scope

Confirm your environment matches this list before starting an evaluation against the built-ins. Anything outside it needs a custom extension, which is covered in the [Extension Guide](./optimizer-extension-guide.md).

- Compaction and orphan file cleanup have built-in strategies. Snapshot expiration is available as a directly submitted built-in job. Scheduling and minimum run intervals remain separate from strategy evaluation. Sort and cluster maintenance also require custom strategies.
- Compaction **strategy** applies to Iceberg tables only, and only where every partition uses an
  identity transform. Direct `builtin-iceberg-rewrite-data-files` on `1.3.x` can cover broader
  layouts; see [Troubleshooting](./optimizer-troubleshooting.md#job-execution-failures).
- The in-process service, `timeTrigger`, and the cron path are documented in [Service](./service.md). The rest of this page is the manual CLI workflow.

## How It Works

This is the shared CLI workflow for the table maintenance service, not a single job type.
Maintenance runs as four steps. Each is a separate command, so you can stop after any of them,
and the dry run on Evaluate shows what would be submitted before anything runs. Direct
`POST .../jobs/runs` submissions skip Evaluate and go straight to a chosen job template.

| Step     | What you run                                      | What it produces                                        |
| -------- | ------------------------------------------------- | ------------------------------------------------------- |
| Collect  | `update-statistics`, `append-metrics`             | Statistics on the table, metrics in the JDBC repository |
| Evaluate | `submit-strategy-jobs --dry-run`                  | Candidate actions, with nothing submitted               |
| Submit   | `submit-strategy-jobs`, `submit-update-stats-job` | A Spark job, tracked by job status and staging logs     |
| Observe  | `monitor-metrics`, `list-table-metrics`           | Before and after metrics, and job staging logs          |

## Execution Modes

There are two ways in, and they differ in where the numbers come from rather than in what they do.

The built-in workflow drives everything through the Gravitino server and its job templates, using the policy derived from a table's effective tags to decide what runs. Use it for server-side operational runs.

The local calculator reads a JSONL file you supply and updates statistics and metrics directly from it. Use it for testing and batch scripts, where you already have the numbers and want to feed them in without the server computing them.

## Policy-driven naming

These three identifiers matter for the **policy-driven** path (`submit-strategy-jobs` and
tag-associated policies). They are not compaction-only — each policy-backed task has its own
names — but direct `runJob` submissions do not use `--strategy-name` and do not need this
distinction. Task sections below state which policy name / type apply when a policy path exists.

| Term          | Example                      | Where it appears                                           |
| ------------- | ---------------------------- | ---------------------------------------------------------- |
| Policy name   | `iceberg_compaction_default` | The policy's own name, and the CLI `--strategy-name`       |
| Policy type   | `system_iceberg_compaction`  | The `policyType` field when creating a policy over REST    |
| Strategy type | `iceberg-data-compaction`    | The `strategy.type` field, and the strategy handler config |

`--strategy-name` takes the **policy name**, despite what it is called. Passing either of the other two reports no matching identifiers rather than naming the mistake.

## Walkthrough

The sections below share prerequisites and one demo table setup, then cover each built-in
maintenance task with the same shape: what the job does, how you submit it, task-specific
notes, and where to verify. Full REST/`jobConf` examples stay in the
[CLI Reference](./optimizer-cli-reference.md); this page does not repeat them per task.

### Shared prerequisites

- A running Gravitino server with a metalake. The examples use `test`.
- Spark available to the job executor. The [local job executor](../local-job-executor.md) needs either `SPARK_HOME` or `gravitino.jobExecutor.local.sparkHome`, and the [Kubernetes job executor](../k8s-job-executor.md) needs a Spark image.
- An Iceberg Spark runtime on that Spark classpath. Built-in Iceberg templates configure
  `IcebergSparkSessionExtensions` and `SparkCatalog`, but `gravitino-jobs` does not ship the
  Iceberg Spark runtime and the templates leave `jars` empty so your Spark and Iceberg versions
  stay under your control. A stock Spark distribution is not enough. Put a matching
  `iceberg-spark-runtime-*` JAR on the job classpath — for example by installing it into
  `SPARK_HOME/jars`. `spark.jars` in `spark_conf` doesn't add it with the local job executor, as
  the driver applies `spark_conf` only after it starts. Pick the artifact that matches
  your Spark, Scala, and Iceberg versions. The jobs module is built and tested against Spark 3.5.x
  and Iceberg 1.11.0 (for example
  `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0`). Without it, the job fails after
  Spark starts with an error naming the missing Iceberg classes. See
  [Troubleshooting](./optimizer-troubleshooting.md#job-execution-failures).
- With the [Kubernetes job executor](../k8s-job-executor.md), the jobs run in Spark pods: put the
  Iceberg Spark runtime in the Spark image, in `gravitino.jobExecutor.k8s.spark.conf.spark.jars`,
  or in `spark.jars` of `spark_conf`; leave `spark_master` empty, as the examples' `local[2]` is
  rejected; and use catalog URIs, warehouse locations, and a `gravitino_uri` in `updater_options`
  that the pods can reach, not `localhost`. See
  [Built-In Jobs](../k8s-job-executor.md#built-in-jobs).
- `gravitino.job.statusPullIntervalInMs` lowered to `10000` and the server restarted. The default is five minutes, which makes every status check in this walkthrough feel broken.

If your Iceberg REST backend runs in memory, do not restart it partway through. Restarting resets both metadata and data files, and you start over.

#### Secured Gravitino and Iceberg REST (object storage)

The local `file:///` samples below omit authentication. Against a secured cluster (Gravitino auth
plus Iceberg REST on object storage), also set:

1. **Gravitino client auth** — configure `none` / `simple` / `basic` / `oauth` for the optimizer
   client in `gravitino-optimizer.conf`, or the matching short names in `updater_options`. See
   [Gravitino client auth](./optimizer-configuration.md#gravitino-client-auth-cli-and-update-stats).
   Without credentials that match the server, `submit-strategy-jobs` and update-stats callbacks
   fail with unauthorized / “credentials did not support”.

2. **REST `warehouse_location`** — for Iceberg REST catalogs this must be the **Gravitino catalog name** (for example `catalog_iceberg_s3`), not an `s3://...` URI. An object-store URI yields `NoSuchWarehouse`. Leave it empty only for local filesystem demos.

3. **Object-store data plane** — Spark must reach table storage through Iceberg REST. On object
   storage that usually means enabling Iceberg vended credentials
   (`spark.sql.catalog.<catalog>.header.X-Iceberg-Access-Delegation=vended-credentials`) and
   configuring IRC `rest.auth.*` for your catalog's auth mode in `spark_conf` (or the matching
   `gravitino.maintenance.ircAuth.*` cluster settings for the in-process service). Do not copy
   local `file:///` samples as-is. See
   [Configuration](./optimizer-configuration.md) (IRC / `spark-conf` auth) and
   [Troubleshooting](./optimizer-troubleshooting.md#job-execution-failures).

### Demo environment

Do this once. Every task below assumes metalake `test`, catalog `rest_catalog`, and table
`rest_catalog.db.t1`.

Confirm the built-in templates are registered (`gravitino-jobs` on `auxlib`, then restart if
needed):

```bash
curl -sS "http://localhost:8090/api/metalakes/test" | jq
curl -sS "http://localhost:8090/api/metalakes/test/jobs/templates?details=true" \
  | jq '.jobTemplates[].name'
```

Expect at least `builtin-iceberg-update-stats`, `builtin-iceberg-rewrite-data-files`,
`builtin-iceberg-rewrite-manifests`, `builtin-iceberg-expire-snapshots`, and
`builtin-iceberg-remove-orphan-files`.

Create the demo catalog, schema, and table (an “already exists” error on the catalog is fine):

```bash
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

curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{"name": "db", "comment": "maintenance demo schema", "properties": {}}' \
  http://localhost:8090/api/metalakes/test/catalogs/rest_catalog/schemas

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

Seed data so maintenance has something to act on. Compaction needs many small files (Iceberg
rewrite defaults to `min-input-files` of 5; one file per partition can finish with
`Rewritten data files: 0`). The insert below writes 100,000 rows capped at 1,000 rows per file:

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
On object storage, apply the [secured settings](#secured-gravitino-and-iceberg-rest-object-storage)
instead of the local `file:///` samples.

Policy-driven compaction also needs statistics. Submit `builtin-iceberg-update-stats` (see
[Update Statistics](./optimizer-cli-reference.md#update-statistics)), wait for it to finish, and
confirm `custom-data-file-mse` and `custom-delete-file-number` on the table. For a secured server,
put Gravitino client auth short names in `updater_options` and set REST `warehouse_location` to
the catalog name as in the shared prerequisites.

Shared CLI submitter settings used by policy-driven runs (and as a pattern for `jobSubmitterConfig`
keys mirrored into direct `jobConf`):

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
# Local demo: leave empty. Iceberg REST: set to the Gravitino catalog name (not s3://...).
gravitino.optimizer.jobSubmitterConfig.warehouse_location =
gravitino.optimizer.jobSubmitterConfig.spark_conf = {"spark.hadoop.fs.defaultFS":"file:///"}
EOF_CONF
```

On a secured cluster, add Gravitino client auth from
[Gravitino client auth](./optimizer-configuration.md#gravitino-client-auth-cli-and-update-stats)
and the object-store `spark_conf` keys from
[Secured Gravitino and Iceberg REST](#secured-gravitino-and-iceberg-rest-object-storage).

REST job status is polled rather than pushed, so it lags Spark by up to one
`gravitino.job.statusPullIntervalInMs` interval. Staging logs live under
`gravitino.job.stagingDir` (default `/tmp/gravitino/jobs/staging`).

### Compaction

**Job:** `builtin-iceberg-rewrite-data-files` merges small data files.

**Submit**

- Policy path: create policy `iceberg_compaction_default` (`policyType`
  `system_iceberg_compaction`), associate it with a tag, assign the tag to the table, then run
  `submit-strategy-jobs` with `--strategy-name` set to the **policy name**. Dry-run first.
- Direct path: `POST .../jobs/runs` with `jobTemplateName` `builtin-iceberg-rewrite-data-files`.

```bash
# Policy + tag (creating the policy alone is not enough)
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
  -d '{
    "name": "iceberg_compaction",
    "comment": "Tables eligible for automatic compaction",
    "properties": {}
  }' \
  http://localhost:8090/api/metalakes/test/tags

curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{"selector": {"type": "ALL_VALUES"}}' \
  http://localhost:8090/api/metalakes/test/tags/iceberg_compaction/policies/iceberg_compaction_default

curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{"tagsToAdd": ["iceberg_compaction"]}' \
  http://localhost:8090/api/metalakes/test/objects/table/rest_catalog.db.t1/tags

curl -sS "http://localhost:8090/api/metalakes/test/objects/table/rest_catalog.db.t1/policies?details=true" | jq

./bin/gravitino-optimizer.sh \
  --type submit-strategy-jobs \
  --identifiers rest_catalog.db.t1 \
  --strategy-name iceberg_compaction_default \
  --dry-run \
  --limit 10 \
  --conf-path /tmp/gravitino-optimizer-submit.conf
```

`DRY-RUN` lines mean the policy fired. No output usually means statistics are still below the
policy thresholds. Drop `--dry-run` to submit.

**Notes**

- Direct `runJob` must include `strategy`, `sort_order`, and `where_clause` in `jobConf` even when
  empty (`""` is fine); omitting them returns HTTP 400. Policy-driven submits fill these through
  the adapter.
- A successful job with `Rewritten data files: 0` usually means fewer than `min-input-files`
  (default 5) eligible files per rewrite group.

**Verify:** job run status and
`.../builtin-iceberg-rewrite-data-files/{jobId}/output.log` for `Rewritten data files`.
See [Rewrite Data Files](./optimizer-cli-reference.md#rewrite-data-files) and
[Iceberg Compaction Policy](../iceberg-compaction-policy.md).

### Manifest rewrite

**Job:** `builtin-iceberg-rewrite-manifests` consolidates manifest metadata for one existing
partition spec. It leaves data files and other specs' manifests unchanged.

**Submit**

- Direct path: `POST .../jobs/runs` with `jobTemplateName` `builtin-iceberg-rewrite-manifests`,
  plus `catalog_name`, `table_identifier`, and the shared Spark/catalog `jobConf` keys.
- Policy path through Policy → Strategy → Adapter is a follow-up; `submit-strategy-jobs` does not
  select this job automatically yet.

**Notes**

- Omit `spec_id` to maintain the current partition spec, or pass an existing spec ID.
- Optional `use_caching` controls Spark caching during the rewrite.

**Verify:** `output.log` rewritten/added manifest counts (both zero can be a successful no-op).
See [Rewrite Manifests](./optimizer-cli-reference.md#rewrite-manifests) and
[configuration](./optimizer-configuration.md#rewrite-manifests-job).

### Snapshot expiry

**Job:** `builtin-iceberg-expire-snapshots` removes old snapshots and related metadata.
Compaction and other rewrites create snapshots; without expiry, metadata accumulates.

**Submit**

- Direct path: `POST .../jobs/runs` with `jobTemplateName` `builtin-iceberg-expire-snapshots`,
  plus `catalog_name`, `table_identifier`, and the shared Spark/catalog `jobConf` keys.
- Policy handler wiring:
  [Snapshot expiration policy integration](./optimizer-configuration.md#snapshot-expiration-policy-integration).

**Notes**

- Prefer starting with `retain_last` alone before combining it with `older_than`.

**Verify:** deleted data/manifest counts in `output.log`.
See [Expire Snapshots](./optimizer-cli-reference.md#expire-snapshots).

### Orphan file cleanup

**Job:** `builtin-iceberg-remove-orphan-files` reclaims unreferenced files.

**Submit**

- Policy path: `system_iceberg_orphan_file_removal` and the handler in
  [Orphan cleanup policy integration](./optimizer-configuration.md#orphan-cleanup-policy-integration).
  Each explicit optimizer invocation evaluates the enabled policy at table level; it does not
  require partition statistics or configure a periodic schedule.
- Direct path: `POST .../jobs/runs` with `jobTemplateName` `builtin-iceberg-remove-orphan-files`.

**Notes**

- Start with `dry_run: "true"` and review candidate paths before deletion.
- Default cutoff is three days ago; explicit cutoffs must be at least 24 hours old (including dry
  runs). A custom scan location must stay within the table's storage location.

**Verify:** candidate or deleted paths in `output.log`.
See [Remove Orphan Files](./optimizer-cli-reference.md#remove-orphan-files) and
[Configuration](./optimizer-configuration.md#orphan-file-cleanup-job-configuration).

## Related

- [Service](./service.md) for the in-process service, `timeTrigger`, and the cron path
- [Configuration](./optimizer-configuration.md) for the three configuration layers
- [CLI Reference](./optimizer-cli-reference.md) for every command and the built-in job templates
- [Troubleshooting](./optimizer-troubleshooting.md) when something above does not behave
- [Extension Guide](./optimizer-extension-guide.md) for custom strategies and providers
- [Iceberg Compaction Policy](../iceberg-compaction-policy.md) for tuning the built-in strategy
