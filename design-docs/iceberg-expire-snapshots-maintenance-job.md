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
| Status  | Implemented (aligned with orphan cleanup)                            |
| Authors | @laserninja                                                          |
| Created | 2026-05-26                                                           |
| Updated | 2026-10-26                                                           |
| Issue   | [#11194](https://github.com/apache/gravitino/issues/11194)           |
| Module  | `api`, `maintenance/jobs`, `maintenance/optimizer`                   |

---

## 1. Background

Without periodic snapshot expiration, Iceberg table metadata grows
indefinitely - accumulating snapshot JSON files and manifest lists that
slow down table operations and waste storage. The existing built-in
maintenance jobs (`builtin-iceberg-rewrite-data-files` for data compaction
and `builtin-iceberg-update-stats` for metrics) address data file
optimization but do not cover metadata cleanup.

PR [#10500](https://github.com/apache/gravitino/pull/10500) added
Trino-side delegation for `expire_snapshots` as a procedure, but there is
no server-side built-in job that can be triggered automatically via the
Table Maintenance Service (Optimizer) policies.

This design adds end-to-end support for Iceberg snapshot expiration: from
policy definition through strategy evaluation to Spark job execution,
following the same pattern as orphan-file removal.

---

## 2. Goals

1. Add a built-in policy type `system_iceberg_snapshot_expiration` for
   declarative retention configuration (`olderThanDays`, `retainLast`,
   `streamResults`).
2. Add a strategy handler that follows orphan cleanup: `trigger-expr` is
   `true`, so each optimizer invocation may submit expire when the policy
   is attached. Scheduling remains external (optimizer CLI / cron / TMS).
3. Add a job adapter that maps policy options to
   `builtin-iceberg-expire-snapshots` jobConf (`older_than`, `retain_last`,
   `stream_results`).
4. Reuse the Spark job that executes Iceberg's `expire_snapshots`
   procedure.
5. Ensure the full flow works end-to-end: policy → strategy → job
   submission → Spark execution.

---

## 3. Non-Goals

- Orphan file removal (separate Iceberg procedure, separate issue).
- Automatic policy creation - users must explicitly create and attach
  policies.
- Changes to the Optimizer scheduling framework itself.
- Triggering expire from a `snapshot-count` metadata gate (see
  [§6](#6-trigger-model)). Industry practice is schedule-driven retention,
  not count thresholds.

---

## 4. Existing Architecture Overview

The Gravitino maintenance module follows a layered architecture for
automated table maintenance. Orphan-file removal is the closest precedent
for whole-table, always-eligible cleanup:

```
Policy Creation (REST API)
    ↓
GravitinoStrategyProvider  (loads policies as strategies)
    ↓
OrphanFileRemovalStrategyHandler  (BaseExpressionStrategyHandler, trigger-expr true)
    ↓
OrphanFileRemovalJobContext → GravitinoOrphanFileRemovalJobAdapter
    ↓
GravitinoJobSubmitter  (submits job via REST)
    ↓
IcebergRemoveOrphanFilesJob  (Spark execution)
```

### 4.1 Layer Summary

| Layer        | Orphan Components                                                                  | Purpose                                            |
| ------------ | ---------------------------------------------------------------------------------- | -------------------------------------------------- |
| **Policy**   | `ICEBERG_ORPHAN_FILE_REMOVAL`, `IcebergOrphanFileRemovalContent`                   | Retention and job options                          |
| **Strategy** | `OrphanFileRemovalStrategyHandler` extends `BaseExpressionStrategyHandler`         | `trigger-expr: true`; table-level eligibility      |
| **Adapter**  | `GravitinoOrphanFileRemovalJobAdapter`, `OrphanFileRemovalJobContext`              | Convert options to jobConf                         |
| **Job**      | `IcebergRemoveOrphanFilesJob`                                                      | Execute Spark procedure                            |

---

## 5. Proposed Design

We add the same four layers for snapshot expiration, mirroring orphan
cleanup rather than compaction's statistics-driven expressions.

### 5.1 Architecture Diagram

```
┌──────────────────────────────────────────────────────────────┐
│  REST API: POST /metalakes/{m}/policies                      │
│  type: "system_iceberg_snapshot_expiration"                   │
│  content: IcebergSnapshotExpirationContent                   │
│    { olderThanDays, retainLast, streamResults }              │
│    rules: trigger-expr=true, job.options.*                   │
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
│    dataRequirements: {TABLE_METADATA}                        │
│    trigger-expr true → shouldTrigger() when optimizer runs   │
│    Returns: StrategyEvaluation with score + context          │
└──────────────────────────┬───────────────────────────────────┘
                           ↓
┌──────────────────────────────────────────────────────────────┐
│  SnapshotExpirationJobContext → JobAdapter                   │
│    olderThanDays → older_than (UTC timestamp)                │
│    retainLast → retain_last                                  │
│    streamResults → stream_results                            │
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

### 5.2 Layer 1 - Policy Definition (`api/`)

#### 5.2.1 Policy Type

`ICEBERG_SNAPSHOT_EXPIRATION` / `system_iceberg_snapshot_expiration` maps to
`IcebergSnapshotExpirationContent`.

#### 5.2.2 Policy Content

```java
public class IcebergSnapshotExpirationContent implements PolicyContent {
  public static final String STRATEGY_TYPE_VALUE = "iceberg-snapshot-expiration";
  public static final String JOB_TEMPLATE_NAME_VALUE = "builtin-iceberg-expire-snapshots";

  public static final long DEFAULT_OLDER_THAN_DAYS = 5;   // Iceberg expire default
  public static final long DEFAULT_RETAIN_LAST = 1;
  public static final boolean DEFAULT_STREAM_RESULTS = false;

  private final long olderThanDays;
  private final long retainLast;
  private final boolean streamResults;

  // rules(): trigger-expr=true, score-expr=1,
  // job.options.olderThanDays / retainLast / streamResults
}
```

Null JSON fields use the defaults above (same pattern as orphan's
`olderThanDays` / `dryRun`).

#### 5.2.3 Expiration Parameters and Defaults

| Field           | Type    | Required | Default | Description |
| --------------- | ------- | -------- | ------- | ----------- |
| `olderThanDays` | `long`  | No       | `5`     | Age cutoff in days; adapter emits UTC `older_than` |
| `retainLast`    | `long`  | No       | `1`     | Minimum recent snapshots to keep |
| `streamResults` | `boolean`| No       | `false` | Forwarded as `stream_results` |

`olderThanDays` must be in `[1, 36500]`. `retainLast` must be `>= 1`.

Unlike an earlier draft that omitted Gravitino defaults so Iceberg table
properties alone would apply, the shipped content always materializes
defaults (aligned with orphan cleanup and with Iceberg's common 5-day / 1
retain defaults). The adapter always submits explicit `older_than`,
`retain_last`, and `stream_results`.

#### 5.2.4 Example Policy Creation

```json
POST /metalakes/default/policies
{
  "name": "expire_old_snapshots",
  "policyType": "system_iceberg_snapshot_expiration",
  "enabled": true,
  "content": {
    "olderThanDays": 7,
    "retainLast": 3
  }
}
```

Empty `content: {}` applies defaults (`5`, `1`, `false`).

---

### 5.3 Layer 2 - Strategy Handler (`maintenance/optimizer/`)

#### 5.3.1 Strategy Handler

```java
public class SnapshotExpirationStrategyHandler extends BaseExpressionStrategyHandler {
  public static final String NAME = IcebergSnapshotExpirationContent.STRATEGY_TYPE_VALUE;

  @Override
  public Set<DataRequirement> dataRequirements() {
    return Set.of(DataRequirement.TABLE_METADATA);
  }

  @Override
  public String strategyType() {
    return NAME;
  }

  @Override
  protected JobExecutionContext buildJobExecutionContext(...) {
    return new SnapshotExpirationJobContext(
        nameIdentifier, jobOptions, strategy.jobTemplateName());
  }
}
```

**Key design decisions:**

1. **Whole table, not partition-level.** `expire_snapshots` always processes
   table snapshot history; `dataRequirements` is only `TABLE_METADATA`
   (same as orphan).
2. **Orphan-style eligibility.** `trigger-expr` is `true`. Each optimizer
   invocation may submit when the policy is attached. Retention is enforced
   by job options (`olderThanDays` / `retainLast`), not by a
   `snapshot-count ≥ N` gate.
3. **Register via config:**
   `gravitino.optimizer.strategyHandler.iceberg-snapshot-expiration.className`.

#### 5.3.2 Job Execution Context

```java
public class SnapshotExpirationJobContext implements JobExecutionContext {
  // nameIdentifier, jobOptions (olderThanDays, retainLast, streamResults),
  // jobTemplateName
}
```

---

### 5.4 Layer 3 - Job Adapter (`maintenance/optimizer/`)

```java
public class GravitinoSnapshotExpirationJobAdapter implements GravitinoJobAdapter {
  // olderThanDays → older_than (UTC timestamp with Z)
  // retainLast → retain_last
  // streamResults → stream_results
  // plus table_identifier (catalog_name from shared submitter config)
}
```

Register in `GravitinoJobSubmitter.jobAdapters` under
`builtin-iceberg-expire-snapshots`.

---

### 5.5 Layer 4 - Spark Job (`maintenance/jobs/`) - Already Implemented

`IcebergExpireSnapshotsJob` / template `builtin-iceberg-expire-snapshots`:

- Parameters: `--catalog`, `--table`, `--older-than`, `--retain-last`,
  `--stream-results`, `--spark-conf`
- Procedure: `CALL catalog.system.expire_snapshots(...)`

---

## 6. Trigger Model

### 6.1 Chosen approach (orphan-aligned)

| Concern | Behavior |
| ------- | -------- |
| When to run | Optimizer / external schedule invokes recommender |
| Policy eligibility | `trigger-expr: true` (always eligible when attached) |
| What to delete | `older_than` + `retain_last` (+ `stream_results`) |

This matches common Iceberg / Amoro practice: **periodic expire with a
retention policy**, not "fire only after snapshot count crosses a
threshold."

### 6.2 Rejected: `snapshot-count ≥ minSnapshotCount`

An earlier draft proposed reading a reserved `snapshot-count` table
property and triggering only when count ≥ `minSnapshotCount` (default 10),
with a custom `StrategyHandler` instead of `BaseExpressionStrategyHandler`.

Rejected because:

- Industry tools (Iceberg maintenance docs, Amoro table-expire) schedule
  expire and apply age / retain-last; count is not the primary trigger.
- A count gate adds catalog reserved-property work without matching how
  operators expect expire to run.
- Orphan cleanup already established the always-eligible + schedule model
  for whole-table cleanup.

A future optional "skip if fewer than N snapshots" optimization can be
added without redesigning the policy or handler base class.

---

## 7. File Changes Summary

### 7.1 New / primary files

| File | Layer | Description |
| ---- | ----- | ----------- |
| `api/…/policy/IcebergSnapshotExpirationContent.java` | Policy | Content + `trigger-expr` / `job.options.*` |
| `maintenance/optimizer/…/handler/snapshot/SnapshotExpirationStrategyHandler.java` | Strategy | Orphan-style handler |
| `maintenance/optimizer/…/handler/snapshot/SnapshotExpirationJobContext.java` | Strategy | Job context |
| `maintenance/optimizer/…/job/GravitinoSnapshotExpirationJobAdapter.java` | Adapter | Options → jobConf |
| `maintenance/jobs/…/iceberg/IcebergExpireSnapshotsJob.java` | Job | Already shipped |

### 7.2 Modified files

| File | Change |
| ---- | ------ |
| `api/…/policy/Policy.java` / DTO / OpenAPI | Built-in type and REST fields |
| `maintenance/optimizer/…/job/GravitinoJobSubmitter.java` | Register expire adapter |
| Optimizer docs | Handler `className` registration |

### 7.3 Tests

| File | Description |
| ---- | ----------- |
| `TestIcebergSnapshotExpirationContent.java` | Policy content |
| `TestGravitinoSnapshotExpirationJobAdapter.java` | Handler + adapter (orphan-style) |
| `TestIcebergExpireSnapshotsJob.java` | Spark job |

---

## 8. Configuration

```properties
gravitino.optimizer.strategyHandler.iceberg-snapshot-expiration.className = \
  org.apache.gravitino.maintenance.optimizer.recommender.handler.snapshot.SnapshotExpirationStrategyHandler
```

The job adapter is registered in code. Shared submitter settings still
supply `catalog_name` and `spark_conf`.

---

## 9. Open Questions

1. **Optional skip-if-few-snapshots** - Worth a later knob, or leave to
   Iceberg no-op when nothing is expired?
2. **Default `streamResults`** - Shipped as `false` (procedure default).
   Operators may prefer `true` on large tables; field remains configurable.

---

## 10. Comparison with Orphan Cleanup

| Aspect            | Orphan cleanup                                      | Snapshot expiration                          |
| ----------------- | --------------------------------------------------- | -------------------------------------------- |
| Policy type       | `system_iceberg_orphan_file_removal`                | `system_iceberg_snapshot_expiration`         |
| Strategy type     | `iceberg-orphan-file-removal`                       | `iceberg-snapshot-expiration`                |
| Job template      | `builtin-iceberg-remove-orphan-files`               | `builtin-iceberg-expire-snapshots`           |
| Scope             | Whole table                                         | Whole table                                  |
| Handler base      | `BaseExpressionStrategyHandler`                     | `BaseExpressionStrategyHandler`              |
| Data requirements | `TABLE_METADATA`                                    | `TABLE_METADATA`                             |
| Trigger           | `trigger-expr: true`                                | `trigger-expr: true`                         |
| Iceberg procedure | `remove_orphan_files`                               | `expire_snapshots`                           |
| Key parameters    | `olderThanDays`, `location`, `dryRun`               | `olderThanDays`, `retainLast`, `streamResults` |
