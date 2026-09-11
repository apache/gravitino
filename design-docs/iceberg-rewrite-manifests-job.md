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

# Design of the Iceberg Rewrite Manifests Job in Gravitino

Tracking issue: [#11196](https://github.com/apache/gravitino/issues/11196). Umbrella: [#8864](https://github.com/apache/gravitino/issues/8864). Implementation: [#12937](https://github.com/apache/gravitino/pull/12937), continuing [#11216](https://github.com/apache/gravitino/pull/11216).

---

## Background

### How Iceberg scan planning reads metadata

An Iceberg table stores its file inventory in a three-level tree. Planning a scan walks the tree from the top:

```
table metadata (vN.metadata.json)
        |
        +-- snapshot  -->  manifest list (snap-<id>.avro)
                                |
                                +-- manifest A (.avro)  -->  data file, data file, ...
                                +-- manifest B (.avro)  -->  data file, data file, ...
                                +-- manifest C (.avro)  -->  data file, data file, ...
```

The manifest list holds per-manifest partition summaries, so a filter can skip whole manifests. Every manifest that survives that skip must then be opened and read to find matching data files. Planning cost therefore tracks the **number of manifests the filter cannot exclude**, not the size of the table.

### Why manifest count grows

Each commit writes at least one new manifest for the files it adds. Manifest count grows with **commit frequency**, independent of how much data the table holds:

| Workload | Commits/day | Manifests after 30 days |
|----------|-------------|-------------------------|
| Hourly batch load | 24 | ~720 |
| Streaming, 1-minute checkpoints | 1,440 | ~43,200 |

A streaming table of modest size can accumulate tens of thousands of manifests, each covering a handful of data files. Filters exclude few of them because a small manifest written per commit spans whatever partitions that commit touched, so partition summaries end up wide and non-selective.

Compaction makes this worse rather than better. `rewrite_data_files` commits its results, and that commit writes new manifests of its own.

### What Gravitino ships today

The table maintenance service separates **deciding** that work is needed from **executing** it. Built-in job templates are the execution half; policies, strategies, and the recommender are the decision half.

| Built-in job template | Purpose | Triggered by a built-in policy? |
|-----------------------|---------|---------------------------------|
| `builtin-iceberg-update-stats` | Writes the statistics and metrics policies read | No, submitted directly or by the CLI |
| `builtin-iceberg-rewrite-data-files` | Merges small **data** files | Yes, `system_iceberg_compaction` |
| `builtin-iceberg-expire-snapshots` | Drops old snapshots and their metadata | No, submitted directly |

### The problem

Nothing in that list consolidates the manifests of the **current** snapshot.

- `rewrite_data_files` rewrites data files. The manifests it writes are a side effect of its commit, not a consolidation of what was there before.
- `expire_snapshots` deletes manifests reachable only from expired snapshots. Manifests belonging to the live snapshot are exactly the ones scan planning reads, and expiration never touches them. It is not a substitute.

Iceberg has an action for this, exposed as the `rewrite_manifests` Spark procedure, and Gravitino already delegates it on the **engine** side: [#10500](https://github.com/apache/gravitino/pull/10500) lets a Trino user issue `CALL ... rewrite_manifests`. That closes the manual path and leaves two gaps:

1. It requires a human at a Trino session. There is no server-side job the maintenance service can submit, and therefore no path to policy-driven manifest optimization.
2. It only helps Trino users. Gravitino deployments driving Spark or Flink have no equivalent.

---

## Goals

1. **Built-in template**: A template named `builtin-iceberg-rewrite-manifests` appears in `GET /api/metalakes/{metalake}/jobs/templates` once `gravitino-jobs` is on the server classpath, and is submittable through the existing jobs REST API with no new endpoint.
2. **Full procedure surface**: Every parameter Iceberg's `rewrite_manifests` procedure accepts - `table`, `use_caching`, `spec_id` - is reachable through `jobConf`.
3. **Absent means default**: Omitting an optional parameter leaves Iceberg's own default in force. The generated SQL names only the parameters the caller actually supplied.
4. **Invalid input fails before Spark starts**: A malformed `use_caching` or `spec_id` exits non-zero with a message naming the offending value, rather than being coerced into a valid-looking SQL literal.
5. **Runtime version tolerance**: The job runs against any Iceberg version exposing the procedure, without assuming the integer width of its output columns. Iceberg is a `compileOnly` dependency supplied by the cluster, so the compile-time and runtime versions differ in practice.
6. **Injection safety**: A catalog name or table identifier containing quotes, backticks, or statement separators cannot change the shape of the generated statement.
7. **Consistency with siblings**: The job's class layout, argument convention, template registration, and staging log output match `IcebergExpireSnapshotsJob` closely enough that an operator familiar with one can read the other.

---

## Non-Goals

1. **A built-in policy that triggers this job automatically**: `system_iceberg_compaction` is the only built-in policy type, and it evaluates `custom-data-file-mse` and `custom-delete-file-number` - both data-file metrics. Automatic triggering needs manifest-level metrics to exist first, so it is sequenced as follow-up work below rather than bundled here.
2. **Replacing `expire_snapshots` or `rewrite_data_files`**: The three address different layers - snapshot history, data files, and the manifests indexing them. Running this job does not reduce the need for the other two.
3. **Partition-scoped or predicate-scoped rewrites**: Iceberg's procedure takes no `where` clause; it rewrites all manifests of the current snapshot. Offering a filter would mean bypassing the procedure for the Java action API, which Solution Investigations rejects below.
4. **Engine-side delegation**: Issuing `rewrite_manifests` from a query engine session is already covered by [#10500](https://github.com/apache/gravitino/pull/10500). This job is the server-side counterpart, not a replacement.
5. **Scheduling**: The maintenance service is driven by explicit submission or by the CLI workflow, with no internal scheduler. This job inherits that and does not introduce one.

---

## Solution Investigations

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| A. Engine-side delegation only (status quo, #10500) | Already merged; zero new server code | Needs a human in a Trino session; not policy-drivable; useless to non-Trino deployments | Rejected - leaves the server-side gap that #11196 is about |
| B. Iceberg Java `RewriteManifests` action, called directly | Type-safe; exposes `rewriteIf`, `stagingLocation`, `specId`; no SQL text to escape | Requires catalog-loading plumbing no other job has; binds us to `iceberg-core` internals across the `compileOnly` boundary; diverges from all three sibling jobs | Rejected - see below |
| C. Spark SQL `CALL <catalog>.system.rewrite_manifests` | Identical shape to all three sibling jobs; Spark resolves the catalog from template configs; procedure API is stable across Iceberg versions | Parameters passed as SQL text, so escaping and validation are ours to get right | **Chosen** |
| D. A flag on `builtin-iceberg-rewrite-data-files` | One template instead of two; one Spark submission for both rewrites | Ties two operations with different cost profiles and cadences; a template's `version` field could no longer describe its behavior; callers wanting only manifests would pay for a data rewrite | Rejected - conflates independent maintenance operations |

### Why B was rejected in detail

The Java action is the better API in isolation. Three concrete constraints outweigh that:

- **The `compileOnly` boundary.** `maintenance/jobs` compiles against Iceberg but ships without it; the cluster's `iceberg-spark-runtime` provides it at runtime. The `Procedure` contract behind `CALL` is a stable extension point across Iceberg versions. `RewriteManifests`, `Table`, and the catalog-loading classes are internals we would be pinning ourselves to across a boundary where the version can change without us.
- **Catalog loading.** Under `CALL`, Spark resolves the catalog from `spark.sql.catalog.{{catalog_name}}.*`, which `IcebergSparkConfigUtils.buildTemplateSparkConfigs()` already emits for every Iceberg job template. The Java action needs a loaded `Table`, which means reimplementing that resolution per catalog backend - REST, Hive, JDBC, Glue.
- **Consistency.** All three sibling jobs use `CALL`. A fourth that loads catalogs itself would need every operator and reviewer to learn a second model for no user-visible gain.

If a future requirement needs `rewriteIf` or `stagingLocation`, which the procedure does not expose, the right move is to migrate all Iceberg jobs to the action API together, not to fork this one.

---

## Proposal

### Job template

| Property | Value |
|----------|-------|
| Name | `builtin-iceberg-rewrite-manifests` |
| Type | Spark (`SparkJobTemplate`) |
| Version | `v1` (in `customFields`, under `JobTemplateProvider.PROPERTY_VERSION_KEY`) |
| Main class | `org.apache.gravitino.maintenance.jobs.iceberg.IcebergRewriteManifestsJob` |
| Registered in | `BuiltInJobTemplateProvider.BUILT_IN_JOBS` |
| Spark configs | `IcebergSparkConfigUtils.buildTemplateSparkConfigs()`, shared with every Iceberg job template |

Registration is a one-line addition to an `ImmutableList`. `BuiltInJobTemplateProvider` then validates the name against `BUILTIN_NAME_PATTERN` and the version against `VERSION_VALUE_PATTERN`, and logs and skips any template failing either - so a malformed template degrades to absence rather than to a server that will not start.

### Parameters

No REST API changes. The job is reached through the existing `POST /api/metalakes/{metalake}/jobs/runs` endpoint, whose `jobConf` map is opaque to the server and interpreted per template.

| `jobConf` key | CLI argument | Type | Required | Default | Description |
|---------------|--------------|------|----------|---------|-------------|
| `catalog_name` | `--catalog` | string | Yes | - | Iceberg catalog name as registered in Spark |
| `table_identifier` | `--table` | string | Yes | - | Fully qualified table name, such as `db.sample` |
| `use_caching` | `--use-caching` | boolean | No | Iceberg's default (`true`) | Cache table metadata in Spark for the duration of the rewrite |
| `spec_id` | `--spec-id` | int | No | The table's current spec | Rewrite manifests to this partition spec ID |
| `spark_conf` | `--spark-conf` | JSON object | No | None | Extra Spark configuration applied to the session |

These are exactly the three in-parameters of Iceberg's `RewriteManifestsProcedure` (`table` required, `use_caching` and `spec_id` optional), plus Gravitino's standard `spark_conf`. `spec_id` is what makes the "align manifests with a better partition spec" half of the motivation reachable; without it only consolidation is available.

The standard Spark template keys - `spark_master`, `spark_executor_instances`, `spark_executor_cores`, `spark_executor_memory`, `spark_driver_memory`, `catalog_type`, `catalog_uri`, `warehouse_location` - behave as they do for every other Iceberg job template.

### Generated SQL

The job emits a `CALL` naming only the parameters the caller supplied:

```sql
-- catalog_name and table_identifier only
CALL `iceberg_prod`.system.rewrite_manifests(table => 'db.sample')

-- with both optional parameters
CALL `iceberg_prod`.system.rewrite_manifests(
  table => 'db.sample', use_caching => false, spec_id => 2)
```

Two escaping rules, both from `IcebergJobUtils` and shared with the sibling jobs:

| Value | Function | Rule | Example |
|-------|----------|------|---------|
| Catalog name | `escapeSqlIdentifier` | Double internal backticks, wrap in backticks | ``catalog`; DROP TABLE t; --`` becomes ``` `catalog``; DROP TABLE t; --` ``` |
| Table identifier | `escapeSqlString` | Double single quotes | `db.t' OR '1'='1` becomes `db.t'' OR ''1''=''1` |

`use_caching` and `spec_id` never reach the statement as caller text. They are validated, then re-rendered from the parsed `boolean` and `int`, so the only characters that can appear are those `Boolean.toString` and `Integer.toString` produce.

### Placeholder resolution, and why absent parameters need care

This is the one piece of behavior that is not obvious from the sibling jobs, and it dictated most of the job's input handling.

A template's arguments are literal placeholder text. `JobManager.replacePlaceholder` substitutes `jobConf` values, and when a key is **absent it keeps the placeholder verbatim**:

```java
// core/src/main/java/org/apache/gravitino/job/JobManager.java
} else {
  // If no replacement is found, keep the placeholder as is
  matcher.appendReplacement(result, matcher.group(0));
}
```

So a caller who omits `use_caching` does not produce a missing argument. They produce the literal string `{{use_caching}}` in `argv`, which parses as a value like any other:

| Caller supplies | `argv` reaching the job | Naive handling | Result |
|-----------------|--------------------------|----------------|--------|
| `use_caching: "false"` | `--use-caching false` | `Boolean.parseBoolean` gives `false` | Correct |
| *nothing* | `--use-caching {{use_caching}}` | `Boolean.parseBoolean` gives `false` | **Wrong** - silently disables caching instead of leaving Iceberg's default |
| *nothing* | `--spec-id {{spec_id}}` | `Integer.parseInt` throws | Job fails, though the caller asked for nothing |

The `false` row is the dangerous one: no error, no log line, and a parameter the caller never set now materially changes how the rewrite runs. The two-argument case is worse for `spec_id`, where the same mechanism turns an omitted parameter into a hard failure.

The job therefore passes every optional value through `IcebergJobUtils.nullIfUnresolvedPlaceholder`, which returns `null` for a value that is entirely an unresolved placeholder:

```java
String useCaching = IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get("use-caching"));
String specId = IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get("spec-id"));
```

A `null` is then omitted from the generated SQL, which is what "absent means Iceberg's default" requires. Only a value that is *entirely* a placeholder is dropped - `{{a}}b` is a real value and survives - so a legitimate value that happens to contain braces is unaffected.

The same hazard exists in `IcebergExpireSnapshotsJob` (`older_than`, `retain_last`) and in `IcebergRewriteDataFilesJob`. Fixing those is out of scope here and listed as follow-up work.

### Validation

| Parameter | Accepted | Rejected | Why validate at all |
|-----------|----------|----------|---------------------|
| `use_caching` | `true`, `false`, any case | Anything else, for example `yes` | `Boolean.parseBoolean` maps every non-`true` string to `false`, so a typo would quietly invert the caller's intent |
| `spec_id` | Non-negative integer | Negative, non-numeric | Keeps a malformed value from reaching Spark as an unparseable SQL literal, and reports it against the argument name rather than as a SQL syntax error |

Both run before the `SparkSession` is created, so a bad argument costs no cluster resources. Both exit non-zero with the offending value in the message, then print usage.

### Reading the procedure output

`rewrite_manifests` returns one row, `(rewritten_manifests_count, added_manifests_count)`. Both are `IntegerType` in Iceberg 1.11.0, the version this repo compiles against, but the column width is an Iceberg implementation detail and Iceberg is `compileOnly` - the cluster supplies it, and a `getInt` against a future `LongType` would throw `ClassCastException` at the end of an otherwise successful rewrite.

The job reads the columns as `Number`:

```java
List<Row> results = spark.sql(sql).collectAsList();
if (!results.isEmpty()) {
  Row result = results.get(0);
  System.out.printf(
      "Rewrite Manifests Results:%n  Rewritten manifests: %d%n  Added manifests: %d%n",
      ((Number) result.get(0)).longValue(), ((Number) result.get(1)).longValue());
}
```

The empty-result guard matters because a procedure returning no rows is a legitimate no-op.

### User process

1. Confirm the template is registered. `gravitino-jobs` must be in the server's `auxlib`:

   ```bash
   curl -sS "http://localhost:8090/api/metalakes/test/jobs/templates?details=true" \
     | jq '.jobTemplates[].name'
   ```

   The list must contain `builtin-iceberg-rewrite-manifests`.

2. Submit a run. Only `catalog_name` and `table_identifier` are needed beyond the standard Spark keys:

   ```bash
   job_id=$(curl -sS -X POST -H "Accept: application/vnd.gravitino.v1+json" \
     -H "Content-Type: application/json" \
     -d '{
       "jobTemplateName": "builtin-iceberg-rewrite-manifests",
       "jobConf": {
         "catalog_name": "rest_catalog",
         "table_identifier": "db.t1",
         "spark_master": "local[2]",
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

5. Confirm against the table. `added_manifests_count` well below the previous manifest count is the outcome being sought:

   ```sql
   SELECT count(*) FROM rest_catalog.db.t1.manifests;
   ```

Pair the job with `builtin-iceberg-expire-snapshots`. The rewrite creates a snapshot and leaves the pre-rewrite manifests reachable from the previous one, so storage is only reclaimed once those snapshots expire.

### Implementation process

```
Client
  |  POST /api/metalakes/{metalake}/jobs/runs  { jobTemplateName, jobConf }
  v
JobOperations (server/)
  |
  v
JobManager (core/)
  |  1. look up the registered template by name
  |  2. replacePlaceholder over arguments and configs, using jobConf
  |     (absent key -> placeholder kept verbatim)
  |  3. persist the resolved template on the JobEntity
  v
JobExecutor  --spark-submit-->  IcebergRewriteManifestsJob.main(argv)
                                  |  4. IcebergJobUtils.parseArguments
                                  |  5. nullIfUnresolvedPlaceholder on optionals
                                  |  6. validateUseCaching / validateSpecId   -- fail here, before Spark
                                  |  7. build SparkSession (+ spark_conf)
                                  |  8. buildProcedureCall -> escaped CALL text
                                  v
                                Spark  --resolves spark.sql.catalog.<name>.*-->  Iceberg
                                  |                                               |
                                  |  9. one row: rewritten/added counts  <--------+
                                  v
                                stdout -> staging output.log
```

Steps 4 through 6 all run before step 7, so every input error is reported without acquiring a Spark session.

### Backward compatibility

Additive in every respect:

- **New template name.** No existing name, argument, or `jobConf` key changes meaning. Callers of the other three templates are unaffected.
- **No API change.** No new or modified REST endpoint, DTO, client method, or entity, so no OpenAPI or client update is needed.
- **No schema change.** Job runs persist through the existing `JobEntity`.
- **Servers without `gravitino-jobs`** simply do not list the template, exactly as with the other built-ins today.
- **Versioned template.** `version` is `v1`. A later change to arguments or semantics bumps it, following the convention the sibling templates set.

### Follow-up: metrics and a policy for automatic triggering

Automatic triggering is a Non-Goal above; this sketches the shape so the follow-up issues have a starting point rather than leaving the path undefined.

**Metrics.** `IcebergUpdateStatsAndMetricsJob` aggregates the `.files` metadata table and writes `custom-`-prefixed statistics such as `custom-data-file-mse` and `custom-small-file-number`. The manifest analogue reads the `.manifests` metadata table:

| Proposed statistic | Source | Purpose |
|--------------------|--------|---------|
| `custom-manifest-number` | `COUNT(*)` | The primary signal; planning cost tracks it |
| `custom-avg-manifest-size` | `AVG(length)` | Distinguishes many-small from few-large |
| `custom-small-manifest-number` | `COUNT(*)` below a threshold | Directly comparable to `custom-small-file-number` |
| `custom-manifest-spec-count` | `COUNT(DISTINCT partition_spec_id)` | More than one live spec is what makes `spec_id` useful |

**Policy.** A `system_iceberg_rewrite_manifests` policy type with a `iceberg-manifest-rewrite` strategy type, a `StrategyHandler` registered under `gravitino.optimizer.strategyHandler.*`, and thresholds over the metrics above. It reuses the existing recommender path, so no new framework is required - `CompactionStrategyHandler` is the template to follow.

To stay aligned with table compaction, the handler declares `DataRequirement.TABLE_STATISTICS` in `dataRequirements()` and reads its thresholds from table statistics rather than from a side channel, exactly as `CompactionStrategyHandler` does. Any "when did this last run" signal belongs in `statistic_meta` as a `custom-`-prefixed statistic written back after a successful run - the same path `IcebergUpdateStatsAndMetricsJob` already uses - rather than in new bespoke state.

Sequencing matters: the metrics must land, and be collected for long enough to calibrate thresholds against real tables, before a policy that fires on them is worth shipping.

---

## Task Breakdown

### Phase 1: The job (this design, PR [#12937](https://github.com/apache/gravitino/pull/12937))

- [x] Implement `IcebergRewriteManifestsJob` with `table` and `use_caching`, calling `rewrite_manifests` through Spark SQL
- [x] Register the template in `BuiltInJobTemplateProvider`
- [x] Move shared SQL escaping, argument parsing, and Spark config parsing to `IcebergJobUtils` rather than reaching into `IcebergRewriteDataFilesJob`
- [x] Add `IcebergJobUtils.nullIfUnresolvedPlaceholder` and drop unresolved placeholders for optional arguments
- [x] Validate `use_caching`, rejecting values that are neither `true` nor `false`
- [x] Read procedure output as `Number` so the job tolerates the runtime Iceberg version's column width
- [x] Support `spec_id`, with non-negative-integer validation
- [x] Unit tests: template shape, argument parsing, validation, placeholder filtering, generated SQL, injection escaping
- [x] Document the template and its parameters in `docs/table-maintenance-service/optimizer-cli-reference.md`
- [ ] Add `TestIcebergRewriteManifestsJobWithSpark`, following the two existing `*WithSpark` tests, asserting manifest count falls across a real rewrite

### Phase 2: Manifest metrics (separate issue)

- [ ] Add manifest aggregation over the `.manifests` metadata table to `IcebergUpdateStatsAndMetricsJob`
- [ ] Emit `custom-manifest-number`, `custom-avg-manifest-size`, `custom-small-manifest-number`, `custom-manifest-spec-count`
- [ ] Unit tests for the aggregation SQL and the statistic mapping
- [ ] Document the new statistics alongside the existing ones

### Phase 3: Policy-driven triggering (separate issue, depends on Phase 2)

- [ ] Define the `system_iceberg_rewrite_manifests` policy type and its typed content in `api/`
- [ ] Implement a `StrategyHandler` for strategy type `iceberg-manifest-rewrite`
- [ ] Wire the handler into job submission so the recommender can submit `builtin-iceberg-rewrite-manifests`
- [ ] Unit tests for threshold evaluation and job context construction
- [ ] Integration test for the full collect / evaluate / submit path
- [ ] Document the policy, mirroring `docs/iceberg-compaction-policy.md`

### Phase 4: Placeholder hardening across the existing jobs (separate issue)

- [ ] Apply `nullIfUnresolvedPlaceholder` to `older_than` and `retain_last` in `IcebergExpireSnapshotsJob`
- [ ] Apply it to the optional arguments of `IcebergRewriteDataFilesJob` and `IcebergUpdateStatsAndMetricsJob`
- [ ] Regression tests per job proving an omitted optional parameter leaves the engine default in force
