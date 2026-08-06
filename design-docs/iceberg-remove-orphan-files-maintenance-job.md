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

# Design: Built-in Iceberg Remove Orphan Files Maintenance Job

| Field   | Value                                                        |
| ------- | ------------------------------------------------------------ |
| Status  | Draft                                                        |
| Authors | @laserninja                                                  |
| Created | 2026-06-16                                                   |
| Issue   | [#11195](https://github.com/apache/gravitino/issues/11195)   |
| Module  | `api`, `maintenance/jobs`, `maintenance/optimizer`            |

---

## 1. Background

Orphan files accumulate in Iceberg table storage locations from failed writes,
incomplete transactions, schema evolution, or concurrent operations. These files
are no longer referenced by any table snapshot but remain on disk, wasting
significant storage — especially in high-write-volume environments.

The existing built-in maintenance jobs (`builtin-iceberg-rewrite-data-files` for
data compaction, `builtin-iceberg-update-stats` for metrics, and
`builtin-iceberg-expire-snapshots` for metadata cleanup) address data file
optimization and snapshot lifecycle but do not cover orphan file removal.

PR [#10500](https://github.com/apache/gravitino/pull/10500) added Trino-side
delegation for `remove_orphan_files` as a procedure, but there is no
server-side built-in job that can be triggered automatically via the Table
Maintenance Service (Optimizer) policies.

This design proposes adding full end-to-end support for Iceberg orphan file
removal: from policy definition through strategy evaluation to Spark job
execution.

---

## 2. Goals

1. Add a new built-in policy type `system_iceberg_orphan_file_removal` for
   declarative orphan file cleanup configuration.
2. Add a strategy handler that evaluates when orphan file removal should run
   based on time since last cleanup or table statistics.
3. Add a job adapter that converts strategy evaluation results into job
   configurations.
4. Add the Spark job that executes Iceberg's `remove_orphan_files` procedure.
5. Ensure the full flow works end-to-end: policy → strategy → job
   submission → Spark execution.

---

## 3. Non-Goals

- Snapshot expiration (separate Iceberg procedure, separate issue [#11194](https://github.com/apache/gravitino/issues/11194)).
- Automatic policy creation — users must explicitly create and attach
  policies.
- Changes to the Optimizer scheduling framework itself.
- Custom file-level filtering beyond what Iceberg's procedure supports.

---

## 4. Existing Architecture Overview

The Gravitino maintenance module follows a layered architecture for automated
table maintenance. The existing Iceberg compaction flow establishes the
pattern:

```
Policy Creation (REST API)
    ↓
GravitinoStrategyProvider (loads policies as strategies)
    ↓
CompactionStrategyHandler (evaluates trigger / score expressions)
    ↓
CompactionJobContext → GravitinoCompactionJobAdapter (converts to job config)
    ↓
GravitinoJobSubmitter (submits job via REST)
    ↓
IcebergRewriteDataFilesJob (Spark execution)
```

### 4.1 Layer Summary

| Layer | Compaction Components | Purpose |
| --- | --- | --- |
| **Policy** | `Policy.BuiltInType.ICEBERG_COMPACTION`, `IcebergDataCompactionContent` | Define configuration, thresholds, expressions |
| **Strategy** | `CompactionStrategyHandler` extends `BaseExpressionStrategyHandler` | Evaluate trigger conditions, score partitions |
| **Adapter** | `GravitinoCompactionJobAdapter`, `CompactionJobContext` | Convert evaluation result to job configuration |
| **Job** | `IcebergRewriteDataFilesJob`, registered in `BuiltInJobTemplateProvider` | Execute Spark procedure |

---

## 5. Proposed Design

We add the same four layers for orphan file removal, following the compaction
pattern.

### 5.1 Architecture Diagram

```
┌──────────────────────────────────────────────────────────────┐
│  REST API: POST /metalakes/{m}/policies                      │
│  type: "system_iceberg_orphan_file_removal"                  │
│  content: IcebergOrphanFileRemovalContent                    │
│  { olderThanDays, location, dryRun }                         │
└──────────────────────────┬───────────────────────────────────┘
                           ↓
┌──────────────────────────────────────────────────────────────┐
│  GravitinoStrategyProvider                                   │
│  Loads policy → GravitinoStrategy                            │
│  strategyType: "iceberg-orphan-file-removal"                 │
│  jobTemplateName: "builtin-iceberg-remove-orphan-files"      │
└──────────────────────────┬───────────────────────────────────┘
                           ↓
┌──────────────────────────────────────────────────────────────┐
│  OrphanFileRemovalStrategyHandler                            │
│  extends BaseExpressionStrategyHandler                       │
│  dataRequirements: {TABLE_METADATA}                          │
│  Evaluates: time since last cleanup ≥ threshold              │
│  Returns: StrategyEvaluation with score + context            │
└──────────────────────────┬───────────────────────────────────┘
                           ↓
┌──────────────────────────────────────────────────────────────┐
│  OrphanFileRemovalJobContext → JobAdapter                     │
│  Extracts: older_than, location, dry_run                     │
│  Builds: job config map for template substitution            │
└──────────────────────────┬───────────────────────────────────┘
                           ↓
┌──────────────────────────────────────────────────────────────┐
│  GravitinoJobSubmitter                                       │
│  Template: "builtin-iceberg-remove-orphan-files"             │
│  Submits via REST: POST /metalakes/{m}/jobs                  │
└──────────────────────────┬───────────────────────────────────┘
                           ↓
┌──────────────────────────────────────────────────────────────┐
│  IcebergRemoveOrphanFilesJob (Spark)                         │
│  CALL catalog.system.remove_orphan_files(                    │
│      table => '…', older_than => TIMESTAMP '…',              │
│      location => '…', dry_run => bool)                       │
└──────────────────────────────────────────────────────────────┘
```

---

### 5.2 Layer 1 — Policy Definition (`api/`)

#### 5.2.1 New Policy Type

Add `ICEBERG_ORPHAN_FILE_REMOVAL` to `Policy.BuiltInType`:

```java
// api/src/main/java/org/apache/gravitino/policy/Policy.java
enum BuiltInType {
    ICEBERG_COMPACTION("system_iceberg_compaction",
        IcebergDataCompactionContent.class),
    ICEBERG_ORPHAN_FILE_REMOVAL("system_iceberg_orphan_file_removal",
        IcebergOrphanFileRemovalContent.class),  // NEW
    CUSTOM("custom", CustomContent.class);
}
```

#### 5.2.2 New Policy Content Class

Create `IcebergOrphanFileRemovalContent` following the
`IcebergDataCompactionContent` pattern:

```java
// NEW: api/src/main/java/org/apache/gravitino/policy/
//      IcebergOrphanFileRemovalContent.java
public class IcebergOrphanFileRemovalContent implements PolicyContent {

    // Strategy metadata
    public static final String STRATEGY_TYPE_VALUE =
        "iceberg-orphan-file-removal";
    public static final String JOB_TEMPLATE_NAME_VALUE =
        "builtin-iceberg-remove-orphan-files";

    // Configurable fields
    private final long olderThanDays;     // default: 3
    private final String location;        // default: null (table location)
    private final boolean dryRun;         // default: false
    private final long cleanupIntervalDays; // trigger threshold, default: 7

    // Trigger / score expressions
    public static final String TRIGGER_EXPR =
        "days-since-last-orphan-cleanup >= cleanupIntervalDays";
    public static final String SCORE_EXPR =
        "days-since-last-orphan-cleanup";

    // Defaults
    public static final long DEFAULT_OLDER_THAN_DAYS = 3;
    public static final boolean DEFAULT_DRY_RUN = false;
    public static final long DEFAULT_CLEANUP_INTERVAL_DAYS = 7;
}
```

#### 5.2.3 Policy Content Fields

| Field                 | Type      | Default | Description                                                              |
| --------------------- | --------- | ------- | ------------------------------------------------------------------------ |
| `olderThanDays`       | `long`    | 3       | Only remove orphan files older than this many days                        |
| `location`            | `String`  | null    | Custom location to scan; when specified, **only** this location is scanned instead of the table's default location. If null, the table's registered storage location is used. |
| `dryRun`              | `boolean` | false   | Preview-only mode — list orphan files without deleting                    |
| `cleanupIntervalDays` | `long`    | 7       | Trigger threshold — only run when days since last cleanup exceeds this    |

#### 5.2.4 Example Policy Creation

```json
POST /metalakes/default/policies
{
    "name": "remove_orphans_weekly",
    "type": "system_iceberg_orphan_file_removal",
    "comment": "Remove orphan files older than 3 days, run weekly",
    "enabled": true,
    "content": {
        "olderThanDays": 3,
        "dryRun": false,
        "cleanupIntervalDays": 7
    }
}
```

---

### 5.3 Layer 2 — Strategy Handler (`maintenance/optimizer/`)

#### 5.3.1 Strategy Handler

```java
// NEW: maintenance/optimizer/src/main/java/…/handler/orphan/
//      OrphanFileRemovalStrategyHandler.java
public class OrphanFileRemovalStrategyHandler
        extends BaseExpressionStrategyHandler {

    public static final String NAME = "iceberg-orphan-file-removal";

    @Override
    public String strategyType() {
        return NAME;
    }

    @Override
    public Set<DataRequirement> dataRequirements() {
        return ImmutableSet.of(DataRequirement.TABLE_METADATA);
        // No TABLE_STATISTICS or PARTITION_STATISTICS needed —
        // orphan file removal is table-level, time-driven
    }

    @Override
    protected JobExecutionContext buildJobExecutionContext(
            NameIdentifier nameIdentifier,
            Strategy strategy,
            Table table,
            List<PartitionPath> partitions,
            Map<String, String> jobOptions) {
        return new OrphanFileRemovalJobContext(
                nameIdentifier, jobOptions, strategy.jobTemplateName());
    }
}
```

**Key design decision:** Orphan file removal operates at the **table level**,
not partition level. Unlike compaction, which scores and selects individual
partitions, `remove_orphan_files` scans the entire table's storage location.
Therefore:

- `dataRequirements()` only includes `TABLE_METADATA`.
- No partition scoring / selection logic is needed.

**Trigger modes:** Gravitino supports two trigger mechanisms, and this
design does not limit users to one:

1. **Event trigger** — The strategy handler evaluates table metadata
   (e.g., snapshot count changes, write events) and triggers cleanup when
   conditions are met.
2. **Time trigger** — The Optimizer's scheduling framework can invoke the
   strategy handler periodically (e.g., daily or weekly), and the handler
   decides whether cleanup is needed based on `cleanupIntervalDays` or
   other heuristics.

Both modes use the same strategy handler; the difference is in how
often the handler is invoked.

#### 5.3.2 Job Execution Context

```java
// NEW: maintenance/optimizer/src/main/java/…/handler/orphan/
//      OrphanFileRemovalJobContext.java
public class OrphanFileRemovalJobContext implements JobExecutionContext {
    private final NameIdentifier nameIdentifier;
    private final Map<String, String> jobOptions;
    private final String jobTemplateName;

    // jobOptions keys:
    // older_than, location, dry_run,
    // catalog_name, table_identifier
}
```

#### 5.3.3 Handler Registration

The strategy handler type `"iceberg-orphan-file-removal"` must be registered so
that the `Recommender` can instantiate it when it encounters a policy with that
strategy type. This follows the existing pattern where handler classes are
looked up by strategy type name.

---

### 5.4 Layer 3 — Job Adapter (`maintenance/optimizer/`)

#### 5.4.1 Job Adapter

```java
// NEW: maintenance/optimizer/src/main/java/…/job/
//      GravitinoOrphanFileRemovalJobAdapter.java
public class GravitinoOrphanFileRemovalJobAdapter
        implements GravitinoJobAdapter {

    @Override
    public Map<String, String> jobConfig(JobExecutionContext context) {
        OrphanFileRemovalJobContext ctx =
                (OrphanFileRemovalJobContext) context;
        Map<String, String> config = new HashMap<>();
        config.put("catalog_name",
                ctx.nameIdentifier().namespace()[0]);
        config.put("table_identifier",
                ctx.nameIdentifier().namespace()[1]
                + "." + ctx.nameIdentifier().name());

        // Convert olderThanDays → absolute timestamp
        Map<String, String> opts = ctx.jobOptions();
        if (opts.containsKey("olderThanDays")) {
            long days = Long.parseLong(opts.get("olderThanDays"));
            String ts = Instant.now()
                    .minus(Duration.ofDays(days))
                    .toString()
                    .replace("T", " ")
                    .substring(0, 19);  // "yyyy-MM-dd HH:mm:ss"
            config.put("older_than", ts);
        }

        if (opts.containsKey("location")) {
            config.put("location", opts.get("location"));
        }

        config.put("dry_run",
                opts.getOrDefault("dryRun", "false"));
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
    "builtin-iceberg-remove-orphan-files",
        GravitinoOrphanFileRemovalJobAdapter.class  // NEW
);
```

---

### 5.5 Layer 4 — Spark Job (`maintenance/jobs/`)

#### 5.5.1 Job Class

Create `IcebergRemoveOrphanFilesJob` following the same pattern as
`IcebergRewriteDataFilesJob` and `IcebergExpireSnapshotsJob`:

- **Template name:** `builtin-iceberg-remove-orphan-files`
- **Version:** `v1`
- **Parameters:** `--catalog`, `--table`, `--older-than`, `--location`,
  `--dry-run`, `--spark-conf`

#### 5.5.2 Procedure Call

```sql
CALL catalog.system.remove_orphan_files(
    table => 'db.table_name',
    older_than => TIMESTAMP '2026-06-13 00:00:00',
    location => 's3://bucket/path/',
    dry_run => true
)
```

**Parameters (from Iceberg `remove_orphan_files` procedure):**

| Parameter   | Type        | Required | Description                                                   |
| ----------- | ----------- | -------- | ------------------------------------------------------------- |
| `table`     | `string`    | Yes      | Fully qualified table name                                    |
| `older_than`| `timestamp` | No       | Only remove files older than this timestamp (default: 3 days) |
| `location`  | `string`    | No       | Custom directory to scan for orphans (replaces table location when set) |
| `dry_run`   | `boolean`   | No       | If true, list orphan files without deleting them              |

#### 5.5.3 Output

The procedure returns a result set with one column:

| Column              | Type     | Description                        |
| ------------------- | -------- | ---------------------------------- |
| `orphan_file_location` | `string` | Path of each orphan file found/removed |

The job will log the count of orphan files removed (or found in dry-run mode).

#### 5.5.4 Security

- SQL injection prevention via `escapeSqlString()` / `escapeSqlIdentifier()`
  (same utilities as `IcebergExpireSnapshotsJob`)
- Input validation for `dry_run` (must be `true` or `false`)
- Location path validation to prevent path traversal

#### 5.5.5 Job Registration

```java
// UPDATE: BuiltInJobTemplateProvider.java
private static final List<BuiltInJob> BUILT_IN_JOBS =
    ImmutableList.of(
        new SparkPiJob(),
        new IcebergRewriteDataFilesJob(),
        new IcebergUpdateStatsAndMetricsJob(),
        new IcebergRemoveOrphanFilesJob());  // NEW
```

---

## 6. Safety Considerations

Orphan file removal is inherently more dangerous than snapshot expiration or
compaction because it **permanently deletes files**. Several safety mechanisms
are built into the design:

| Safety Mechanism           | Description                                                       |
| -------------------------- | ----------------------------------------------------------------- |
| **`older_than` default**   | 3-day default ensures recently-written files from in-progress operations are not deleted |
| **`dry_run` mode**         | Allows previewing which files would be deleted before actual removal |
| **Time-based trigger**     | `cleanupIntervalDays` prevents too-frequent execution              |
| **Policy-gated**           | Must be explicitly enabled by an administrator via policy creation  |
| **Iceberg built-in safety**| The procedure itself only identifies files not referenced by any snapshot |

**Important:** The `older_than` threshold should be set conservatively. Files
from in-progress writes or concurrent operations may not yet be referenced by
a committed snapshot. A minimum of 3 days is recommended; values below 1 day
should be used with caution.

---

## 7. File Changes Summary

### 7.1 New Files

| File                                                                             | Layer    | Description                                |
| -------------------------------------------------------------------------------- | -------- | ------------------------------------------ |
| `api/…/policy/IcebergOrphanFileRemovalContent.java`                              | Policy   | Policy content with removal configuration  |
| `maintenance/optimizer/…/handler/orphan/OrphanFileRemovalStrategyHandler.java`    | Strategy | Trigger / score evaluation                 |
| `maintenance/optimizer/…/handler/orphan/OrphanFileRemovalJobContext.java`         | Strategy | Job execution context                      |
| `maintenance/optimizer/…/job/GravitinoOrphanFileRemovalJobAdapter.java`           | Adapter  | Context → job config conversion            |
| `maintenance/jobs/…/iceberg/IcebergRemoveOrphanFilesJob.java`                    | Job      | Spark job                                  |

### 7.2 Modified Files

| File                                                    | Change                                                        |
| ------------------------------------------------------- | ------------------------------------------------------------- |
| `api/…/policy/Policy.java`                              | Add `ICEBERG_ORPHAN_FILE_REMOVAL` to `BuiltInType` enum       |
| `maintenance/optimizer/…/job/GravitinoJobSubmitter.java` | Register remove-orphan-files adapter in `jobAdapters` map      |
| `maintenance/jobs/…/BuiltInJobTemplateProvider.java`     | Register `IcebergRemoveOrphanFilesJob`                         |
| Handler registry                                        | Register `OrphanFileRemovalStrategyHandler`                    |

### 7.3 Test Files

| File                                                    | Description                           |
| ------------------------------------------------------- | ------------------------------------- |
| `TestIcebergOrphanFileRemovalContent.java`               | Policy content unit tests             |
| `TestOrphanFileRemovalStrategyHandler.java`              | Strategy handler unit tests           |
| `TestGravitinoOrphanFileRemovalJobAdapter.java`          | Job adapter unit tests                |
| `TestIcebergRemoveOrphanFilesJob.java`                   | Spark job unit tests                  |

---

## 8. Proposed PR Plan

| PR | Scope | Dependencies |
| --- | --- | --- |
| **PR 1** | Job layer: `IcebergRemoveOrphanFilesJob` + `BuiltInJobTemplateProvider` + tests | None |
| **PR 2** | Policy + Strategy + Adapter: all remaining layers + tests | PR 1 |

Since the total code size across the policy, strategy, and adapter layers is
expected to be well under 1000 lines, PRs 2 and 3 from the original plan are
combined into a single PR.

---

## 9. Open Questions

1. **Cleanup interval tracking** — How should we track when the last orphan
   file removal ran? Options include: (a) a table property, (b) job history
   metadata, (c) a separate statistics entry. The compaction flow uses
   partition statistics; for orphan removal we may need a different mechanism
   since it is table-level and time-driven.
2. **Location parameter** — When `location` is specified, only that
   location is scanned (the table's default location is **not** scanned
   in addition). This follows Iceberg's native `remove_orphan_files`
   behavior. The policy supports `location` for cases where tables use
   external storage paths, but for most use cases it should be left null
   so the table's registered location is used.
3. **Dry-run result persistence** — Should dry-run results be stored
   somewhere (e.g., job output metadata) for review before actual deletion?
4. ~~**PR granularity**~~ — Resolved: single PR for policy + strategy +
   adapter layers since total code is expected to be under 1000 lines.
5. **`older_than` minimum** — Should we enforce a minimum `olderThanDays`
   value (e.g., ≥ 1 day) at the policy level to prevent accidental deletion
   of in-progress files?

---

## 10. Comparison with Other Maintenance Flows

| Aspect              | Compaction                                      | Snapshot Expiration                         | Orphan File Removal                          |
| ------------------- | ----------------------------------------------- | ------------------------------------------- | -------------------------------------------- |
| Policy type         | `system_iceberg_compaction`                      | `system_iceberg_snapshot_expiration`         | `system_iceberg_orphan_file_removal`          |
| Strategy type       | `iceberg-data-compaction`                        | `iceberg-snapshot-expiration`                | `iceberg-orphan-file-removal`                 |
| Job template        | `builtin-iceberg-rewrite-data-files`             | `builtin-iceberg-expire-snapshots`           | `builtin-iceberg-remove-orphan-files`         |
| Scope               | Per-partition (scored, top-N selected)           | Whole table                                  | Whole table (or custom location)              |
| Data requirements   | `TABLE_METADATA` + `TABLE_STATISTICS` + `PARTITION_STATISTICS` | `TABLE_METADATA` + `TABLE_STATISTICS` | `TABLE_METADATA`                       |
| Trigger metric      | `custom-data-file-mse`, `custom-delete-file-number` | `custom-snapshot-count`                  | `days-since-last-orphan-cleanup`              |
| Iceberg procedure   | `rewrite_data_files`                             | `expire_snapshots`                           | `remove_orphan_files`                         |
| Key parameters      | strategy, sort-order, where, options             | older_than, retain_last, stream_results      | older_than, location, dry_run                 |
| Destructiveness     | Rewrites data (recoverable via snapshots)        | Removes metadata (irreversible)              | Removes data files (irreversible)             |
| Safety concern      | Low — data is rewritten, not lost                | Medium — old snapshots are removed           | High — files are permanently deleted          |
