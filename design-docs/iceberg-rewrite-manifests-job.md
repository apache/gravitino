<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Design: Built-in Iceberg Rewrite Manifests Maintenance Job

Tracking issue: [#11196](https://github.com/apache/gravitino/issues/11196). Umbrella: [#8864](https://github.com/apache/gravitino/issues/8864). Implementation: [#12937](https://github.com/apache/gravitino/pull/12937), continuing the original work by @ibrahimErbilen in [#11216](https://github.com/apache/gravitino/pull/11216).

## 1. Background

Iceberg scan planning reads a snapshot's manifest list, uses partition summaries to prune manifests, and opens the remaining manifests to find matching files. Frequent small writes can leave many small manifests and increase planning work. Growth depends on write patterns and Iceberg's automatic manifest merging; it is not a fixed number of manifests per commit.

Data-file compaction and snapshot expiration serve different purposes. Compaction changes data-file layout and can also affect manifests through its commit. Expiration removes obsolete snapshots and unreferenced files. Neither provides an explicit job dedicated to reorganizing the current snapshot's manifests.

Gravitino needs a built-in job that operators can submit through the existing jobs API to invoke Iceberg's `rewrite_manifests` procedure. It consolidates and clusters manifest entries within a selected partition spec without rewriting data files.

## 2. Goals

1. **Job submission**: Register `builtin-iceberg-rewrite-manifests` and submit it through the existing jobs REST API.
2. **Procedure parameters**: Expose Iceberg parameters `table`, `use_caching`, and `spec_id`, with omitted optional values delegated to Iceberg's defaults.
3. **Operator guidance**: Explain how to discover spec IDs and which manifests a run can rewrite.
4. **Validation and results**: Reject malformed arguments, escape SQL inputs, and report rewritten and added manifest counts.

## 3. Non-Goals

1. **Data repartitioning**: Rewriting manifests does not migrate existing files between partition specs.
2. **Row or partition predicates**: This job exposes no `where` filter. Selection is by existing partition spec.
3. **Automatic triggering in the initial PR**: Manifest statistics and a policy are follow-up work; the existing compaction policy does not trigger this job.
4. **Scheduling or framework changes**: Reuse existing job submission and status handling without changing JobManager or introducing a scheduler.

## 4. Existing Architecture Overview

### 4.1 Layer Summary

| Layer                                                | Existing role                                    | Impact of this proposal                           |
| ---------------------------------------------------- | ------------------------------------------------ | ------------------------------------------------- |
| Policy (`api/`)                                      | Defines typed maintenance policy content         | No initial change; separate manifest policy later |
| Strategy handler (`maintenance/optimizer/`)          | Evaluates collected statistics                   | No initial change                                 |
| Job adapter and submitter (`maintenance/optimizer/`) | Converts a strategy result into a job submission | Follow-up wiring for automatic triggering         |
| Spark job (`maintenance/jobs/`)                      | Executes registered built-in job templates       | Add the rewrite-manifests job                     |
| Jobs REST API and core                               | Registers templates, submits runs, tracks status | Reuse existing endpoints and persistence          |

## 5. Proposed Design

### 5.1 Architecture Diagram

```text
Operator -> Jobs REST API -> JobManager -> JobExecutor -> spark-submit
                                                            |
                                                            v
                                               IcebergRewriteManifestsJob
                                                            |
                                                            v
                                               Spark SQL rewrite_manifests
                                                            |
                                                            v
                                               Iceberg metadata commit
                                                            |
                                                            v
                                               Counts and job status
```

The initial implementation enters at the jobs API. A future policy-driven flow will reach the same job through the existing recommender and submitter.

### 5.2 Layer 1 - Policy Definition (`api/`, follow-up)

The follow-up `system_iceberg_rewrite_manifests` policy uses the following configurable thresholds. No policy type is added by #12937.

| Policy setting                      | Default           | Meaning                                                           |
| ----------------------------------- | ----------------- | ----------------------------------------------------------------- |
| `manifest_count_critical`           | `500`             | Trigger at or above this count, regardless of average size        |
| `manifest_count_warning`            | `100`             | Minimum count for the size-based trigger                          |
| `avg_manifest_size_threshold_bytes` | `8388608` (8 MiB) | Trigger below this average size when the warning count is reached |

Evaluate the following expression using statistics for the resolved target spec only:

```text
IF manifest_count >= manifest_count_critical:
    trigger
ELSE IF manifest_count >= manifest_count_warning
        AND avg_manifest_size_bytes < avg_manifest_size_threshold_bytes:
    trigger
ELSE:
    do not trigger
```

Count comparisons are inclusive; the size comparison is strict. For example, 500 manifests trigger regardless of size, 100 manifests averaging less than 8 MiB trigger, and 100 manifests averaging exactly 8 MiB do not. Counts below 100 do not trigger under these defaults.

### 5.3 Layer 2 - Strategy Handler (`maintenance/optimizer/`, follow-up)

#### 5.3.1 Resolve the Target Spec

At the start of a collection/evaluation cycle, resolve `spec-id` from the requested ID or, if omitted, from the table's `default-spec-id`. Validate that the resolved spec exists and carry that ID through collection, evaluation, and job submission. Do not resolve the default again between these steps: partition evolution could otherwise make the job target a different spec from the one evaluated.

Collect only manifests in the current snapshot whose `partition_spec_id` equals that resolved ID:

```sql
-- Example: the resolved spec ID is 1.
SELECT COUNT(*) AS manifest_count, AVG(length) AS avg_manifest_size_bytes
FROM rest_catalog.db.t1.manifests
WHERE partition_spec_id = 1;
```

The SQL ID is rendered from a validated integer. With no matching manifests, record a count of zero and normalize the null average to zero; this cannot trigger under the default count thresholds. A missing map entry means statistics have not been collected for that spec, not zero manifests: collect it before evaluating.

#### 5.3.2 Statistics Storage

Store one row per table and statistic name in `statistic_meta`, not one row per spec. Each statistic value is an object keyed by the decimal spec ID. Use the existing `StatisticValues.objectValue` representation with numeric values, retaining two statistics only:

| Statistic name                     | Value for each spec key            | Purpose                             |
| ---------------------------------- | ---------------------------------- | ----------------------------------- |
| `custom-manifest-number-by-spec`   | Long from `COUNT(*)`               | Manifest count for that spec        |
| `custom-avg-manifest-size-by-spec` | Double from `AVG(length)` in bytes | Average manifest size for that spec |

Example logical contents of the two rows for one table:

```text
statistic_name:  custom-manifest-number-by-spec
statistic_value: {"0": 620, "1": 120}

statistic_name:  custom-avg-manifest-size-by-spec
statistic_value: {"0": 10485760.0, "1": 4194304.0}
```

These illustrate the object values, not a new REST serialization format. A collection run for spec `1` replaces only key `"1"` in each map and preserves key `"0"` and every other spec entry. Read and merge the existing maps before writing them through the table-statistics API. Coordinate concurrent collectors for the same table so one read/merge/write does not overwrite another spec's update. Publish both measurements from the same collection and do not evaluate a partially updated pair; the follow-up implementation must test these update guarantees.

The handler declares `DataRequirement.TABLE_STATISTICS` and looks up the same resolved spec key in both objects. The threshold expression uses those two numeric values, never table-wide totals or values from another spec. Do not add small-manifest counts or spec-count statistics. Any last-success time required for a future cooldown belongs in `statistic_meta` as a `custom-` statistic written after successful completion.

### 5.4 Layer 3 - Job Adapter (`maintenance/optimizer/`, follow-up)

The adapter maps a positive strategy decision to `builtin-iceberg-rewrite-manifests`, supplying the catalog, table, caching option, and the exact resolved `spec-id` used by the collector and trigger expression. Always include that resolved ID in the submitted `jobConf`, even when the original request omitted it. This preserves the target if the table default changes after collection. Reuse existing job submission and tracking. The initial PR requires no adapter because operators submit the template directly.

### 5.5 Layer 4 - Spark Job (`maintenance/jobs/`)

#### 5.5.1 Job Class and Registration

| Property            | Value                                                                      |
| ------------------- | -------------------------------------------------------------------------- |
| Template name       | `builtin-iceberg-rewrite-manifests`                                        |
| Type                | `SparkJobTemplate`                                                         |
| Version             | `v1`                                                                       |
| Main class          | `org.apache.gravitino.maintenance.jobs.iceberg.IcebergRewriteManifestsJob` |
| Registration        | `BuiltInJobTemplateProvider.BUILT_IN_JOBS`                                 |
| Spark configuration | `IcebergSparkConfigUtils.buildTemplateSparkConfigs()`                      |

The job parses and validates its inputs, builds a Spark session, calls the procedure, reports counts, and stops the session. Execution failures produce a non-zero process exit status.

#### 5.5.2 Parameters

Use `POST /api/metalakes/{metalake}/jobs/runs` with the existing `jobTemplateName` and `jobConf` fields. Each job argument key matches its CLI flag without the leading `--`. Translate `use-caching` and `spec-id` to Iceberg SQL parameters `use_caching` and `spec_id` when building the procedure call. Standard Spark configuration placeholders retain their existing names.

| `jobConf` key      | CLI argument    | Type                        | Required | Default                             |
| ------------------ | --------------- | --------------------------- | -------- | ----------------------------------- |
| `catalog`          | `--catalog`     | String                      | Yes      | None                                |
| `table`            | `--table`       | String, such as `db.sample` | Yes      | None                                |
| `use-caching`      | `--use-caching` | Boolean                     | No       | Installed Iceberg version's default |
| `spec-id`          | `--spec-id`     | Non-negative integer        | No       | Table's current spec ID             |
| `spark-conf`       | `--spark-conf`  | JSON object                 | No       | No additional overrides             |

Supply the standard Spark template configuration keys as for the other Iceberg jobs: `spark_master`, `spark_executor_instances`, `spark_executor_cores`, `spark_executor_memory`, `spark_driver_memory`, `catalog_type`, `catalog_uri`, and `warehouse_location`.

`use-caching` controls caching during the rewrite. Omission delegates to the runtime; Iceberg 1.11.0's action defaults to `false`. Set it explicitly when consistent behavior across runtime versions is required.

##### How to find `spec-id`

Omit `spec-id` for routine maintenance of the current partition spec. Do not guess IDs such as `0` or `1`. Read `default-spec-id` and the `partition-specs` array from the table's current Iceberg metadata JSON. The array maps each `spec-id` to its partition fields and transforms; `default-spec-id` identifies the current spec. These are Iceberg table metadata fields, not Gravitino catalog properties.

To discover which specs have manifests in the current snapshot, run:

```sql
SELECT DISTINCT partition_spec_id
FROM rest_catalog.db.t1.manifests;
```

This query lists represented specs, not which one is current. A defined spec with no manifests may be absent; use the metadata JSON to identify the default and interpret the transforms.

For example, suppose metadata shows spec `0` uses `day(event_time)` and the current spec `1` uses `hour(event_time)`. Omitting `spec-id` rewrites eligible manifests for spec `1`. Passing `"spec-id": "0"` consolidates the old day-spec manifests. Neither run converts day-partitioned data files to hour partitioning or rewrites manifests belonging to the other spec.

`spec-id` selects the existing spec whose manifests are eligible for rewriting, and replacement manifests use that same spec. Iceberg validates that the ID exists. This follows the [Iceberg 1.11.0 action implementation](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteManifestsSparkAction.java), where `findMatchingManifests` compares each manifest's `partitionSpecId()` to the selected spec. It is not a partition-evolution operation.

#### 5.5.3 Procedure Call and Alternatives

```sql
-- Rewrite eligible manifests for the current spec.
CALL `iceberg_prod`.system.rewrite_manifests(table => 'db.sample');

-- Rewrite eligible manifests for a known existing spec.
CALL `iceberg_prod`.system.rewrite_manifests(
  table => 'db.sample', use_caching => false, spec_id => 0);
```

| Approach                          | Trade-off                                                                           | Decision                                      |
| --------------------------------- | ----------------------------------------------------------------------------------- | --------------------------------------------- |
| Spark SQL procedure               | Reuses Spark catalog configuration and existing maintenance-job conventions         | Chosen                                        |
| Spark Java action                 | Offers finer control, but requires loading a table and invoking the action directly | Reserve for requirements beyond the procedure |
| Combine with data-file compaction | Couples operations that can run at different frequencies                            | Keep separate jobs                            |

The Java action is a public API, not an Iceberg internal. The SQL procedure is chosen for consistency with the rewrite-data-files and expire-snapshots jobs, not because the Java API requires a separate implementation for every catalog backend. The deployed Spark and Iceberg runtime must support the supplied parameters; the job does not promise compatibility with every Iceberg release.

#### 5.5.4 Output

The procedure returns `rewritten_manifests_count` and `added_manifests_count`. Iceberg 1.11.0 returns integer columns; the job reads them as `Number` and logs their values. Zero counts indicate a successful no-op, such as no eligible manifests needing rewriting. An empty result is guarded defensively, not assumed to be the normal no-op representation.

Counts concern the selected spec. A successful rewrite does not guarantee fewer manifests or improved latency for every workload; compare count, size, and planning behavior before and after.

#### 5.5.5 User Process


1. Confirm the template is registered. `gravitino-jobs` must be in the server's `auxlib`:

   ```bash
   curl -sS "http://localhost:8090/api/metalakes/test/jobs/templates?details=true" \
     | jq '.jobTemplates[].name'
   ```

   The list must contain `builtin-iceberg-rewrite-manifests`.

2. Submit a run. Only `catalog` and `table` are needed beyond the standard Spark keys:

   ```bash
   job_id=$(curl -sS -X POST -H "Accept: application/vnd.gravitino.v1+json" \
     -H "Content-Type: application/json" \
     -d '{
       "jobTemplateName": "builtin-iceberg-rewrite-manifests",
       "jobConf": {
         "catalog": "rest_catalog",
         "table": "db.t1",
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
   ```

3. Poll for completion. Status is polled rather than pushed, so it lags the Spark process by up to `gravitino.job.statusPullIntervalInMs`:

   ```bash
   curl -sS "http://localhost:8090/api/metalakes/test/jobs/runs/${job_id}" | jq '.job.status'
   ```

4. Read the counts from the staging log, rooted at `gravitino.job.stagingDir`:

   ```bash
   grep -E "Rewritten manifests|Added manifests" \
     "/tmp/gravitino/jobs/staging/test/builtin-iceberg-rewrite-manifests/${job_id}/output.log"
   ```

5. Confirm against the table. Compare manifest count and average size before and after the run, grouped by spec. Counts reported by the procedure cover only the selected spec:

   ```sql
   SELECT partition_spec_id, COUNT(*) AS manifest_count, AVG(length) AS avg_manifest_size
   FROM rest_catalog.db.t1.manifests
   GROUP BY partition_spec_id;
   ```

Pair the job with `builtin-iceberg-expire-snapshots`. The rewrite creates a snapshot and leaves the pre-rewrite manifests reachable from the previous one, so storage is only reclaimed once those snapshots expire.


## 6. Safety Considerations

### 6.1 Input Validation

| Input                   | Behavior                                                                    |
| ----------------------- | --------------------------------------------------------------------------- |
| `use-caching`           | Accept `true` or `false`, case-insensitively; reject other supplied values  |
| `spec-id`               | Parse a non-negative 32-bit integer; reject malformed or overflowing values |
| Unknown spec ID         | Iceberg rejects IDs absent from table metadata                              |
| Omitted optional values | Omit them from the SQL call and preserve runtime defaults                   |
| Catalog and table       | Use shared identifier and string escaping when building SQL                 |

Local validation occurs before creating the Spark session. This avoids starting a session for malformed values, but does not imply that `spark-submit` has allocated no resources. Catalog access and spec existence are validated by Iceberg during execution.

### 6.2 Metadata and Concurrent Writers

Iceberg owns the metadata commit and conflict handling. The job does not edit metadata JSON or delete files directly. A failed or uncertain commit must be checked through the table and job status before resubmission. Snapshot expiration remains a separate operation with its own retention requirements.

### 6.3 Backward Compatibility

The template is additive. No existing template name, REST endpoint, client API, or database schema changes. Existing job persistence and status tracking are reused. Installing a runtime that supports the procedure and its supplied parameters remains an operator prerequisite.

## 7. File Changes Summary

### 7.1 New Files

| File under `maintenance/jobs/src/main/java/`                                    | Purpose                        |
| ------------------------------------------------------------------------------- | ------------------------------ |
| `org/apache/gravitino/maintenance/jobs/iceberg/IcebergRewriteManifestsJob.java` | Template and Spark entry point |

### 7.2 Modified Files

| File                                                        | Purpose                                                              |
| ----------------------------------------------------------- | -------------------------------------------------------------------- |
| `BuiltInJobTemplateProvider.java`                           | Register the template                                                |
| `IcebergJobUtils.java`                                      | Reuse shared parsing and escaping; normalize omitted optional values |
| `docs/table-maintenance-service/optimizer-cli-reference.md` | Parameters, spec discovery, and submission examples                  |

### 7.3 Test Files

| Test                                                | Coverage                                                               |
| --------------------------------------------------- | ---------------------------------------------------------------------- |
| `TestIcebergRewriteManifestsJob`                    | Template, parsing, validation, and generated SQL                       |
| `TestIcebergRewriteManifestsJobWithSpark` (planned) | Real procedure execution, preserved table contents, and spec selection |

## 8. Proposed PR Plan

1. **Job implementation (#12937)**: Register the template, implement procedure execution and validation, and document submission and spec discovery. Add a Spark-backed test with manifests under two specs: verify that a run affects only the selected spec, preserves records, and handles a no-op.
2. **Manifest statistics**: Collect `custom-manifest-number-by-spec` and `custom-avg-manifest-size-by-spec`. Test spec-filtered aggregation, empty results, missing entries, merge updates preserving other specs, and concurrent collection updates. Document the two object-valued statistics.
3. **Automatic triggering**: Implement the policy defaults and expression in Section 5.2, the handler, and the adapter. Test counts at 99, 100, 499, and 500; sizes below, at, and above 8 MiB; and a default-spec change between collection and submission. Verify the submitted ID matches the collected and evaluated key.

## 9. Review Decisions and Initial Compatibility

The three open questions are resolved:

1. **Trigger thresholds**: Use the critical-count or warning-count-plus-size expression in Section 5.2, with defaults of 500, 100, and 8 MiB respectively.
2. **Spec consistency**: Collect, store, evaluate, and submit against the same resolved spec. Represent multiple specs in one object-valued row per statistic name, as specified in Sections 5.3 and 5.4.
3. **Initial runtime support**: Commit to and run CI against **Spark 3.5 and Iceberg 1.11.0** for the initial release, aligned with the other built-in Iceberg jobs. Other version combinations are outside the initial compatibility commitment. The Spark-backed tests in the PR plan must run on this combination.

## 10. Comparison with Other Maintenance Flows

| Job                 | Primary target                          | Changes data-file layout? | Partition-spec behavior                               |
| ------------------- | --------------------------------------- | ------------------------- | ----------------------------------------------------- |
| Rewrite data files  | Data files                              | Yes                       | Governed by the data rewrite operation                |
| Expire snapshots    | Snapshot history and unreferenced files | No                        | Governed by snapshot retention                        |
| Remove orphan files | Unreferenced storage objects            | No                        | Not a spec rewrite                                    |
| Rewrite manifests   | Manifest entries for an existing spec   | No                        | Selects one spec; does not migrate data between specs |
