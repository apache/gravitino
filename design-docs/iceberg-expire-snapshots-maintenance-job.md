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

# Design: Built-in Iceberg Expire Snapshots Maintenance Job

| Field   | Value                                                                |
| ------- | -------------------------------------------------------------------- |
| Status  | Draft                                                                |
| Authors | @laserninja                                                          |
| Created | 2026-05-26                                                           |
| Issue   | [#11194](https://github.com/apache/gravitino/issues/11194)           |
| Module  | `api`, `maintenance/jobs`, `maintenance/optimizer`                   |

---

## 1. Background

Without periodic snapshot expiration, Iceberg table metadata grows
indefinitely — accumulating snapshot JSON files and manifest lists that
slow down table operations and waste storage. The existing built-in
maintenance jobs (`builtin-iceberg-rewrite-data-files` for data compaction
and `builtin-iceberg-update-stats` for metrics) address data file
optimization but do not cover metadata cleanup.

PR [#10500](https://github.com/apache/gravitino/pull/10500) added
Trino-side delegation for `expire_snapshots` as a procedure, but there is
no server-side built-in job that can be triggered automatically via the
Table Maintenance Service (Optimizer) policies.

This design proposes adding full end-to-end support for Iceberg snapshot
expiration: from policy definition through strategy evaluation to Spark
job execution.

---

## 2. Goals

1. Add a new built-in policy type `system_iceberg_snapshot_expiration` for
   declarative snapshot expiration configuration.
2. Add a strategy handler that evaluates when snapshot expiration should
   run based on table statistics (snapshot count, age).
3. Add a job adapter that converts strategy evaluation results into job
   configurations.
4. Add the Spark job that executes Iceberg's `expire_snapshots` procedure.
5. Ensure the full flow works end-to-end: policy → strategy → job
   submission → Spark execution.

---

## 3. Non-Goals

- Orphan file removal (separate Iceberg procedure, separate issue).
- Automatic policy creation — users must explicitly create and attach
  policies.
- Changes to the Optimizer scheduling framework itself.

---

## 4. Existing Architecture Overview

The Gravitino maintenance module follows a layered architecture for
automated table maintenance. The existing Iceberg compaction flow
establishes the pattern:

```
Policy Creation (REST API)
    ↓
GravitinoStrategyProvider  (loads policies as strategies)
    ↓
CompactionStrategyHandler  (evaluates trigger / score expressions)
    ↓
CompactionJobContext → GravitinoCompactionJobAdapter  (converts to job config)
    ↓
GravitinoJobSubmitter  (submits job via REST)
    ↓
IcebergRewriteDataFilesJob  (Spark execution)
```

### 4.1 Layer Summary

| Layer        | Compaction Components                                                              | Purpose                                            |
| ------------ | ---------------------------------------------------------------------------------- | -------------------------------------------------- |
| **Policy**   | `Policy.BuiltInType.ICEBERG_COMPACTION`, `IcebergDataCompactionContent`            | Define configuration, thresholds, expressions      |
| **Strategy** | `CompactionStrategyHandler` extends `BaseExpressionStrategyHandler`                | Evaluate trigger conditions, score partitions       |
| **Adapter**  | `GravitinoCompactionJobAdapter`, `CompactionJobContext`                            | Convert evaluation result to job configuration     |
| **Job**      | `IcebergRewriteDataFilesJob`, registered in `BuiltInJobTemplateProvider`           | Execute Spark procedure                            |

---

## 5. Proposed Design

We add the same four layers for snapshot expiration, following the
compaction pattern.

### 5.1 Architecture Diagram

```
┌──────────────────────────────────────────────────────────────┐
│  REST API: POST /metalakes/{m}/policies                      │
│  type: "system_iceberg_snapshot_expiration"                   │
│  content: IcebergSnapshotExpirationContent                   │
│    { olderThanDays, retainLast, streamResults }              │
└──────────────────────────┬───────────────────────────────────┘
                           ↓
┌──────────────────────────────────────────────────────────────┐
│  GravitinoStrategyProvider                                   │
│  Loads policy → GravitinoStrategy                            │
│    strategyType:    "iceberg-snapshot-expiration"             │
│    jobTemplateName: "builtin-iceberg-expire-snapshots"       │
└──────────────────────────┬───────────────────────────────────┘
                           ↓
┌──────────────────────────────────────────────────────────────┐
│  SnapshotExpirationStrategyHandler                           │
│    extends BaseExpressionStrategyHandler                     │
│    dataRequirements: {TABLE_METADATA, TABLE_STATISTICS}      │
│    Evaluates: snapshot count ≥ threshold                     │
│    Returns: StrategyEvaluation with score + context          │
└──────────────────────────┬───────────────────────────────────┘
                           ↓
┌──────────────────────────────────────────────────────────────┐
│  SnapshotExpirationJobContext → JobAdapter                   │
│    Extracts: older_than, retain_last, stream_results         │
│    Builds: job config map for template substitution          │
└──────────────────────────┬───────────────────────────────────┘
                           ↓
┌──────────────────────────────────────────────────────────────┐
│  GravitinoJobSubmitter                                       │
│    Template: "builtin-iceberg-expire-snapshots"              │
│    Submits via REST: POST /metalakes/{m}/jobs                │
└──────────────────────────┬───────────────────────────────────┘
                           ↓
┌──────────────────────────────────────────────────────────────┐
│  IcebergExpireSnapshotsJob (Spark)                           │
│  CALL catalog.system.expire_snapshots(                       │
│    table => '…', older_than => TIMESTAMP '…',                │
│    retain_last => N, stream_results => bool)                 │
└──────────────────────────────────────────────────────────────┘
```

---

### 5.2 Layer 1 — Policy Definition (`api/`)

#### 5.2.1 New Policy Type

Add `ICEBERG_SNAPSHOT_EXPIRATION` to `Policy.BuiltInType`:

```java
// api/src/main/java/org/apache/gravitino/policy/Policy.java
enum BuiltInType {
  ICEBERG_COMPACTION("system_iceberg_compaction",
      IcebergDataCompactionContent.class),
  ICEBERG_SNAPSHOT_EXPIRATION("system_iceberg_snapshot_expiration",
      IcebergSnapshotExpirationContent.class),          // NEW
  CUSTOM("custom", CustomContent.class);
}
```

#### 5.2.2 New Policy Content Class

Create `IcebergSnapshotExpirationContent` following the
`IcebergDataCompactionContent` pattern:

```java
// NEW: api/src/main/java/org/apache/gravitino/policy/
//      IcebergSnapshotExpirationContent.java
public class IcebergSnapshotExpirationContent implements PolicyContent {

  // Strategy metadata
  public static final String STRATEGY_TYPE_VALUE =
      "iceberg-snapshot-expiration";
  public static final String JOB_TEMPLATE_NAME_VALUE =
      "builtin-iceberg-expire-snapshots";

  // Configurable fields — all independently optional
  @Nullable private final Long olderThanDays;
  @Nullable private final Long retainLast;
  @Nullable private final Boolean streamResults;
  private final long minSnapshotCount;    // trigger threshold, default: 10

  // Trigger / score expressions
  public static final String TRIGGER_EXPR =
      "custom-snapshot-count >= minSnapshotCount";
  public static final String SCORE_EXPR =
      "custom-snapshot-count";

  // Defaults (applied when field is null)
  public static final long DEFAULT_OLDER_THAN_DAYS     = 5;
  public static final long DEFAULT_RETAIN_LAST         = 1;
  public static final boolean DEFAULT_STREAM_RESULTS   = false;
  public static final long DEFAULT_MIN_SNAPSHOT_COUNT  = 10;
}
```

#### 5.2.3 Policy Content Fields

All expiration parameters are **independently optional**. A user may
specify only `olderThanDays`, only `retainLast`, or both — the job adapter
will only pass set parameters to the Spark procedure. This matches the
Iceberg `expire_snapshots` procedure semantics, where each parameter is
independent.

| Field             | Type      | Required | Default | Description                                                  |
| ----------------- | --------- | -------- | ------- | ------------------------------------------------------------ |
| `olderThanDays`   | `Long`    | No       | 5       | Expire snapshots older than this many days                    |
| `retainLast`      | `Long`    | No       | 1       | Minimum number of snapshots to always retain                  |
| `streamResults`   | `Boolean` | No       | false   | Whether to stream deletion results (useful for large tables)  |
| `minSnapshotCount`| `long`    | No       | 10      | Trigger threshold — only run when snapshot count exceeds this |

**Usage scenarios:**

- **Only `olderThanDays`:** Expire snapshots older than N days, keep the
  Iceberg default for `retain_last` (1).
- **Only `retainLast`:** Keep at most N snapshots regardless of age (all
  older ones are expired).
- **Both:** Expire snapshots older than N days, but always keep at least M
  snapshots.
- **Neither:** Use defaults (expire > 5 days, retain at least 1).

#### 5.2.4 Example Policy Creation

```json
POST /metalakes/default/policies
{
  "name": "expire_old_snapshots",
  "type": "system_iceberg_snapshot_expiration",
  "comment": "Expire snapshots older than 7 days, keep at least 3",
  "enabled": true,
  "content": {
    "olderThanDays": 7,
    "retainLast": 3
  }
}
```

A user may also specify only one parameter:

```json
{
  "name": "retain_only_policy",
  "type": "system_iceberg_snapshot_expiration",
  "comment": "Keep at most 5 snapshots",
  "enabled": true,
  "content": {
    "retainLast": 5
  }
}
```

---

### 5.3 Layer 2 — Strategy Handler (`maintenance/optimizer/`)

#### 5.3.1 Strategy Handler

```java
// NEW: maintenance/optimizer/src/main/java/…/handler/snapshot/
//      SnapshotExpirationStrategyHandler.java
public class SnapshotExpirationStrategyHandler
    extends BaseExpressionStrategyHandler {

  public static final String NAME = "iceberg-snapshot-expiration";

  @Override
  public String strategyType() {
    return NAME;
  }

  @Override
  public Set<DataRequirement> dataRequirements() {
    return ImmutableSet.of(
        DataRequirement.TABLE_METADATA,
        DataRequirement.TABLE_STATISTICS);
    // No PARTITION_STATISTICS — snapshot expiration is table-level
  }

  @Override
  protected JobExecutionContext buildJobExecutionContext(
      NameIdentifier nameIdentifier,
      Strategy strategy,
      Table table,
      List<PartitionPath> partitions,
      Map<String, String> jobOptions) {
    return new SnapshotExpirationJobContext(
        nameIdentifier, jobOptions, strategy.jobTemplateName());
  }
}
```

**Key design decision:** Snapshot expiration operates at the **table
level**, not partition level. Unlike compaction, which scores and selects
individual partitions, `expire_snapshots` always processes the entire
table's snapshot history. Therefore:

- `dataRequirements()` excludes `PARTITION_STATISTICS`.
- No partition scoring / selection logic is needed.
- The trigger expression evaluates table-level snapshot count only.

#### 5.3.2 Job Execution Context

```java
// NEW: maintenance/optimizer/src/main/java/…/handler/snapshot/
//      SnapshotExpirationJobContext.java
public class SnapshotExpirationJobContext implements JobExecutionContext {
  private final NameIdentifier nameIdentifier;
  private final Map<String, String> jobOptions;
  private final String jobTemplateName;

  // jobOptions keys (all optional except catalog/table):
  //   older_than_days, retain_last, stream_results,
  //   catalog_name, table_identifier
}
```

#### 5.3.3 Handler Registration

The strategy handler type `"iceberg-snapshot-expiration"` must be
registered so that the `Recommender` can instantiate it when it encounters
a policy with that strategy type. This follows the existing pattern where
handler classes are looked up by strategy type name.

---

### 5.4 Layer 3 — Job Adapter (`maintenance/optimizer/`)

#### 5.4.1 Job Adapter

The adapter only passes parameters that were explicitly set in the policy
content, allowing each to work independently:

```java
// NEW: maintenance/optimizer/src/main/java/…/job/
//      GravitinoSnapshotExpirationJobAdapter.java
public class GravitinoSnapshotExpirationJobAdapter
    implements GravitinoJobAdapter {

  @Override
  public Map<String, String> jobConfig(JobExecutionContext context) {
    SnapshotExpirationJobContext ctx =
        (SnapshotExpirationJobContext) context;
    Map<String, String> config = new HashMap<>();
    config.put("catalog_name",
        ctx.nameIdentifier().namespace()[0]);
    config.put("table_identifier",
        ctx.nameIdentifier().namespace()[1]
            + "." + ctx.nameIdentifier().name());

    Map<String, String> opts = ctx.jobOptions();

    // Only pass older_than if explicitly configured
    if (opts.containsKey("olderThanDays")) {
      long days = Long.parseLong(opts.get("olderThanDays"));
      String ts = Instant.now()
          .minus(Duration.ofDays(days))
          .toString()
          .replace("T", " ")
          .substring(0, 19);       // "yyyy-MM-dd HH:mm:ss"
      config.put("older_than", ts);
    }

    // Only pass retain_last if explicitly configured
    if (opts.containsKey("retainLast")) {
      config.put("retain_last", opts.get("retainLast"));
    }

    // Only pass stream_results if explicitly configured
    if (opts.containsKey("streamResults")) {
      config.put("stream_results", opts.get("streamResults"));
    }

    return config;
  }
}
```

#### 5.4.2 Register Adapter

```java
// UPDATE: GravitinoJobSubmitter.java
private static final Map<String, Class<? extends GravitinoJobAdapter>>
    jobAdapters = ImmutableMap.of(
        "builtin-iceberg-rewrite-data-files",
            GravitinoCompactionJobAdapter.class,
        "builtin-iceberg-expire-snapshots",
            GravitinoSnapshotExpirationJobAdapter.class   // NEW
    );
```

---

### 5.5 Layer 4 — Spark Job (`maintenance/jobs/`) — Already Implemented

`IcebergExpireSnapshotsJob` is implemented in PR
[#11206](https://github.com/apache/gravitino/pull/11206):

- **Template name:** `builtin-iceberg-expire-snapshots`
- **Version:** `v1`
- **Parameters:** `--catalog`, `--table`, `--older-than`, `--retain-last`,
  `--stream-results`, `--spark-conf` (all optional except `--catalog` and
  `--table`)
- **Procedure:**
  `CALL catalog.system.expire_snapshots(table => '…', older_than =>
  TIMESTAMP '…', retain_last => N, stream_results => bool)`
- **Output:** `deleted_data_files_count`,
  `deleted_manifest_files_count`, `deleted_manifest_lists_count`
- **Security:** SQL injection prevention via `escapeSqlString()` /
  `escapeSqlIdentifier()`
- **Tests:** 40 unit tests covering template metadata, argument parsing,
  procedure call building, escaping, and input validation

The job already handles partial parameters gracefully — only parameters
that are non-empty are included in the procedure call.

---

## 6. Statistics Requirements

The strategy handler needs a **snapshot count** statistic to evaluate
whether expiration should be triggered.

| Statistic Name          | Type   | Source                 | Description                      |
| ----------------------- | ------ | ---------------------- | -------------------------------- |
| `custom-snapshot-count` | `Long` | Iceberg table metadata | Number of snapshots in the table |

**Open question:** Does the current statistics provider already collect
snapshot count? If not, we may need to extend
`IcebergUpdateStatsAndMetricsJob` or the statistics provider to include
snapshot metadata statistics.

---

## 7. File Changes Summary

### 7.1 New Files

| File                                                                              | Layer    | Description                        |
| --------------------------------------------------------------------------------- | -------- | ---------------------------------- |
| `api/…/policy/IcebergSnapshotExpirationContent.java`                              | Policy   | Policy content with expiration cfg |
| `maintenance/optimizer/…/handler/snapshot/SnapshotExpirationStrategyHandler.java`  | Strategy | Trigger / score evaluation         |
| `maintenance/optimizer/…/handler/snapshot/SnapshotExpirationJobContext.java`       | Strategy | Job execution context              |
| `maintenance/optimizer/…/job/GravitinoSnapshotExpirationJobAdapter.java`          | Adapter  | Context → job config conversion    |
| `maintenance/jobs/…/iceberg/IcebergExpireSnapshotsJob.java`                       | Job      | Spark job (already in PR #11206)   |

### 7.2 Modified Files

| File                                                    | Change                                                   |
| ------------------------------------------------------- | -------------------------------------------------------- |
| `api/…/policy/Policy.java`                              | Add `ICEBERG_SNAPSHOT_EXPIRATION` to `BuiltInType` enum  |
| `maintenance/optimizer/…/job/GravitinoJobSubmitter.java` | Register expire-snapshots adapter in `jobAdapters` map   |
| `maintenance/jobs/…/BuiltInJobTemplateProvider.java`     | Register `IcebergExpireSnapshotsJob` (in PR #11206)      |
| Handler registry                                        | Register `SnapshotExpirationStrategyHandler`             |

### 7.3 Test Files

| File                                             | Description                         |
| ------------------------------------------------ | ----------------------------------- |
| `TestIcebergSnapshotExpirationContent.java`      | Policy content unit tests           |
| `TestSnapshotExpirationStrategyHandler.java`     | Strategy handler unit tests         |
| `TestGravitinoSnapshotExpirationJobAdapter.java` | Job adapter unit tests              |
| `TestIcebergExpireSnapshotsJob.java`             | Spark job unit tests (in PR #11206) |

---

## 8. Proposed PR Plan

| PR                | Scope                                                                                | Dependencies |
| ----------------- | ------------------------------------------------------------------------------------ | ------------ |
| **PR 1** (this)   | Design document                                                                      | None         |
| **PR 2** (ready)  | Job layer: `IcebergExpireSnapshotsJob` + `BuiltInJobTemplateProvider` + tests        | PR 1         |
| **PR 3**          | Policy layer: `IcebergSnapshotExpirationContent` + `Policy.BuiltInType` + tests      | PR 2         |
| **PR 4**          | Strategy + Adapter: handler, context, adapter, submitter registration + tests         | PR 3         |

PRs 3 and 4 can be combined into a single PR if preferred.

---

## 9. Open Questions

1. **Snapshot count statistics** — Is `custom-snapshot-count` already
   collected by the statistics provider, or does this need to be added?
2. **Trigger expression complexity** — Should we support additional trigger
   conditions beyond snapshot count (e.g., oldest snapshot age, total
   metadata size)?
3. **PR granularity** — Single PR for all remaining layers or split
   per-layer?

---

## 10. Comparison with Compaction Flow

| Aspect            | Compaction                                                     | Snapshot Expiration                     |
| ----------------- | -------------------------------------------------------------- | --------------------------------------- |
| Policy type       | `system_iceberg_compaction`                                    | `system_iceberg_snapshot_expiration`    |
| Strategy type     | `iceberg-data-compaction`                                      | `iceberg-snapshot-expiration`           |
| Job template      | `builtin-iceberg-rewrite-data-files`                           | `builtin-iceberg-expire-snapshots`      |
| Scope             | Per-partition (scored, top-N selected)                          | Whole table                             |
| Data requirements | `TABLE_METADATA` + `TABLE_STATISTICS` + `PARTITION_STATISTICS` | `TABLE_METADATA` + `TABLE_STATISTICS`   |
| Trigger metric    | `custom-data-file-mse`, `custom-delete-file-number`            | `custom-snapshot-count`                 |
| Iceberg procedure | `rewrite_data_files`                                           | `expire_snapshots`                      |
| Key parameters    | strategy, sort-order, where, options (all required)             | older_than, retain_last, stream_results (all independently optional) |
