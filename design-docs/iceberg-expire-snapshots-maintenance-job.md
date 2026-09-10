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
indefinitely - accumulating snapshot JSON files and manifest lists that
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
   run, based on the snapshot count read from Iceberg table metadata.
3. Expose that snapshot count as a reserved Iceberg table property so the
   handler needs no statistics collection.
4. Add a job adapter that converts strategy evaluation results into job
   configurations.
5. Add the Spark job that executes Iceberg's `expire_snapshots` procedure.
6. Ensure the full flow works end-to-end: policy → strategy → job
   submission → Spark execution.

---

## 3. Non-Goals

- Orphan file removal (separate Iceberg procedure, separate issue).
- Automatic policy creation - users must explicitly create and attach
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
│    implements StrategyHandler                                │
│    dataRequirements: {TABLE_METADATA}                        │
│    Reads `snapshot-count` from table metadata properties      │
│    Evaluates: snapshot count ≥ minSnapshotCount               │
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

### 5.2 Layer 1 - Policy Definition (`api/`)

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

  // Configurable fields - all independently optional
  @Nullable private final Long olderThanDays;   // no Gravitino default
  @Nullable private final Long retainLast;      // no Gravitino default
  @Nullable private final Boolean streamResults;
  private final long minSnapshotCount;    // trigger threshold, default: 10

  // Rule keys
  public static final String MIN_SNAPSHOT_COUNT_KEY = "minSnapshotCount";

  // Defaults
  public static final boolean DEFAULT_STREAM_RESULTS   = true;
  public static final long DEFAULT_MIN_SNAPSHOT_COUNT  = 10;
}
```

`olderThanDays` and `retainLast` deliberately have **no Gravitino-side
default**. See [§5.2.3](#523-expiration-parameters-and-defaults).

#### 5.2.3 Expiration Parameters and Defaults

Both expiration conditions are **independently optional**. A user may
specify only `olderThanDays`, only `retainLast`, or both - the job adapter
omits an unset condition from the procedure call rather than filling it in
with a Gravitino default. (`streamResults` is different: it is a transport
knob rather than an expiration condition, so it is always passed - see
[§5.2.4](#524-why-streamresults-defaults-to-true).)

**Why no Gravitino default for `olderThanDays` / `retainLast`:** Iceberg
already defines defaults for both, sourced from table properties:

| Procedure argument | Iceberg fallback (table property)         | Iceberg default |
| ------------------ | ----------------------------------------- | --------------- |
| `older_than`       | `history.expire.max-snapshot-age-ms`      | 5 days          |
| `retain_last`      | `history.expire.min-snapshots-to-keep`    | 1               |

If Gravitino injected its own defaults, a policy that only sets
`retainLast` would silently also apply age-based expiration, and a
per-table `history.expire.*` property set by the table owner would be
overridden by the policy. Omitting unset parameters keeps a single
condition meaningful on its own and keeps table-level Iceberg
configuration authoritative.

| Field             | Type      | Required | Default                                       | Description                                                  |
| ----------------- | --------- | -------- | --------------------------------------------- | ------------------------------------------------------------ |
| `olderThanDays`   | `Long`    | No       | unset - falls back to Iceberg table property  | Expire snapshots older than this many days                    |
| `retainLast`      | `Long`    | No       | unset - falls back to Iceberg table property  | Minimum number of snapshots to always retain                  |
| `streamResults`   | `Boolean` | No       | `true`                                        | Stream deleted-file rows to the driver by RDD partition       |
| `minSnapshotCount`| `long`    | No       | 10                                            | Trigger threshold - only run once the snapshot count reaches this |

**Usage scenarios:**

- **Only `olderThanDays`:** Expire snapshots older than N days; the number
  retained follows `history.expire.min-snapshots-to-keep` (Iceberg
  default 1).
- **Only `retainLast`:** Keep at least N snapshots; the age cutoff follows
  `history.expire.max-snapshot-age-ms` (Iceberg default 5 days).
- **Both:** Expire snapshots older than N days, but always keep at least M
  snapshots.
- **Neither:** Pure Iceberg behavior - expiration is governed entirely by
  the table's `history.expire.*` properties.

#### 5.2.4 Why `streamResults` Defaults to `true`

Iceberg's `stream_results` controls how the list of deleted files reaches
the Spark driver: when `false` (the procedure's own default) the full file
list is collected to the driver in one result set; when `true` the rows are
streamed by RDD partition, which is what the Iceberg documentation
recommends to avoid driver OOM on large deletions.

Setting it to `false` buys very little here: this job consumes only the
summary counts (`deleted_data_files_count` and friends), never the
individual file rows, so materializing the whole list on the driver has no
downstream benefit. The only cost of streaming is slightly more per-
partition overhead on small tables, which is negligible relative to the
snapshot expiration itself.

Because the job runs unattended and on tables of unknown size, we default
`streamResults` to `true` for robustness - deliberately differing from the
procedure default - while keeping the field configurable for parity with
the procedure and for operators who want the collected form.

#### 5.2.5 Example Policy Creation

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
  "comment": "Always keep at least 5 snapshots",
  "enabled": true,
  "content": {
    "retainLast": 5
  }
}
```

---

### 5.3 Layer 2 - Strategy Handler (`maintenance/optimizer/`)

#### 5.3.1 Strategy Handler

```java
// NEW: maintenance/optimizer/src/main/java/…/handler/snapshot/
//      SnapshotExpirationStrategyHandler.java
public class SnapshotExpirationStrategyHandler implements StrategyHandler {

  public static final String NAME = "iceberg-snapshot-expiration";

  private NameIdentifier nameIdentifier;
  private Strategy strategy;
  private Table tableMetadata;

  @Override
  public String strategyType() {
    return NAME;
  }

  @Override
  public Set<DataRequirement> dataRequirements() {
    // Table metadata alone is enough: snapshot count comes from the
    // table's reserved properties. No statistics are needed.
    return EnumSet.of(DataRequirement.TABLE_METADATA);
  }

  @Override
  public void initialize(StrategyHandlerContext context) {
    Preconditions.checkArgument(
        context.tableMetadata().isPresent(), "Table metadata is null");
    this.nameIdentifier = context.nameIdentifier();
    this.strategy = context.strategy();
    this.tableMetadata = context.tableMetadata().get();
  }

  @Override
  public boolean shouldTrigger() {
    OptionalLong snapshotCount = snapshotCount();
    return snapshotCount.isPresent()
        && snapshotCount.getAsLong() >= minSnapshotCount(strategy);
  }

  @Override
  public StrategyEvaluation evaluate() {
    // Score is the snapshot count: tables with more snapshots come first.
    return new StrategyEvaluationImpl(
        snapshotCount().getAsLong(),
        new SnapshotExpirationJobContext(
            nameIdentifier, strategy.jobOptions(), strategy.jobTemplateName()));
  }

  // Reads the `snapshot-count` reserved property from table metadata.
  private OptionalLong snapshotCount() { … }
}
```

**Key design decision 1 - table level, not partition level.** Unlike
compaction, which scores and selects individual partitions,
`expire_snapshots` always processes the entire table's snapshot history.
Therefore no partition scoring or selection logic is needed, and
`dataRequirements()` excludes `PARTITION_STATISTICS`.

**Key design decision 2 - trigger on table metadata, not statistics.**
Snapshot count is already part of Iceberg table metadata and needs no scan,
so the handler reads it from the metadata the Recommender already loads (see
[§6](#6-snapshot-count-from-table-metadata)) instead of depending on a
collected statistic. Two consequences:

- The handler implements `StrategyHandler` directly rather than extending
  `BaseExpressionStrategyHandler`. The base class builds its expression
  context from table/partition statistics plus numeric strategy rules only,
  so metadata-derived values are not addressable from `trigger-expr` /
  `score-expr`. With a single table-level condition, the expression
  machinery buys nothing, so the handler compares snapshot count against
  `minSnapshotCount` in `shouldTrigger()` and returns it as the score.
- Expiration no longer depends on `builtin-iceberg-update-stats` having run
  recently, and it cannot act on a stale snapshot count.

If the `snapshot-count` property is absent (for example, a non-Iceberg table
or an older catalog), `shouldTrigger()` logs and returns `false` rather than
guessing.

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

### 5.4 Layer 3 - Job Adapter (`maintenance/optimizer/`)

#### 5.4.1 Job Adapter

The adapter passes only the expiration conditions that were explicitly set
in the policy content, so each works independently, and always passes
`stream_results`:

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

    // stream_results always has a value: policy setting, or `true`
    config.put(
        "stream_results",
        opts.getOrDefault(
            "streamResults",
            String.valueOf(
                IcebergSnapshotExpirationContent.DEFAULT_STREAM_RESULTS)));

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

### 5.5 Layer 4 - Spark Job (`maintenance/jobs/`) - Already Implemented

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

The job already handles partial parameters gracefully - only parameters
that are non-empty are included in the procedure call.

---

## 6. Snapshot Count from Table Metadata

The trigger input is the table's **snapshot count**. It is taken from table
metadata rather than from a collected statistic.

**Why not a statistic.** The alternative would be a `custom-snapshot-count`
statistic produced by `builtin-iceberg-update-stats` (which today derives
its statistics from the Iceberg `.files` metadata table). That would make
expiration depend on the stats job having run recently, and the trigger
would evaluate a snapshot count that is stale by up to one stats interval -
for a metric that is free to read from metadata.

**What is available today.** The Recommender loads table metadata through
`GravitinoTableMetadataProvider`, which returns a Gravitino `Table` over
REST. For Iceberg tables, `IcebergTable` merges in the reserved properties
built by `IcebergTablePropertiesUtil.buildReservedProperties(TableMetadata)`
- currently `format`, `provider`, `current-snapshot-id`, `location`,
`format-version`, `sort-order`, `identifier-fields`. Snapshot count is not
among them, so the handler cannot read it yet.

**Proposed change.** Expose snapshot count as one more reserved Iceberg
table property. `TableMetadata.snapshots()` is already in memory when the
properties are built, so this costs no I/O and no scan:

| Property         | Type   | Source                        | Description                      |
| ---------------- | ------ | ----------------------------- | -------------------------------- |
| `snapshot-count` | `long` | `TableMetadata.snapshots()`   | Number of snapshots in the table |

This requires:

1. A `SNAPSHOT_COUNT` constant in `IcebergConstants` and a reserved entry in
   `IcebergTablePropertiesMetadata` (reserved, like `current-snapshot-id`, so
   users cannot set it).
2. Populating it in `IcebergTablePropertiesUtil.buildReservedProperties`.

Being reserved and read-only, the property is a safe addition: it shows up
in `loadTable` responses and is rejected on create/alter.

An unset or non-numeric `snapshot-count` is treated as "unknown" - the
handler does not trigger, so a non-Iceberg table attached to the policy is
skipped rather than mis-evaluated.

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
| `catalog-common/…/iceberg/IcebergConstants.java`         | Add `SNAPSHOT_COUNT` constant                            |
| `catalogs/catalog-lakehouse-iceberg/…/IcebergTablePropertiesMetadata.java` | Add reserved `snapshot-count` property entry |
| `catalogs/catalog-lakehouse-iceberg/…/utils/IcebergTablePropertiesUtil.java` | Populate `snapshot-count` from `TableMetadata.snapshots()` |
| `maintenance/optimizer/…/job/GravitinoJobSubmitter.java` | Register expire-snapshots adapter in `jobAdapters` map   |
| `maintenance/jobs/…/BuiltInJobTemplateProvider.java`     | Register `IcebergExpireSnapshotsJob` (in PR #11206)      |
| Handler registry                                        | Register `SnapshotExpirationStrategyHandler`             |

### 7.3 Test Files

| File                                             | Description                         |
| ------------------------------------------------ | ----------------------------------- |
| `TestIcebergSnapshotExpirationContent.java`      | Policy content unit tests           |
| `TestSnapshotExpirationStrategyHandler.java`     | Strategy handler unit tests, including missing / non-numeric `snapshot-count` |
| `TestGravitinoSnapshotExpirationJobAdapter.java` | Job adapter unit tests              |
| `TestIcebergTable.java` (existing)               | Assert `snapshot-count` is exposed and reserved |
| `TestIcebergExpireSnapshotsJob.java`             | Spark job unit tests (in PR #11206) |

---

## 8. Proposed PR Plan

| PR                | Scope                                                                                | Dependencies |
| ----------------- | ------------------------------------------------------------------------------------ | ------------ |
| **PR 1** (this)   | Design document                                                                      | None         |
| **PR 2** (ready)  | Job layer: `IcebergExpireSnapshotsJob` + `BuiltInJobTemplateProvider` + tests        | PR 1         |
| **PR 3**          | Metadata layer: reserved `snapshot-count` Iceberg table property + tests             | PR 1         |
| **PR 4**          | Policy layer: `IcebergSnapshotExpirationContent` + `Policy.BuiltInType` + tests      | PR 2         |
| **PR 5**          | Strategy + Adapter: handler, context, adapter, submitter registration + tests         | PR 3, PR 4   |

PR 3 is independent of PR 2 and can land in parallel. PRs 4 and 5 can be
combined into a single PR if preferred.

---

## 9. Open Questions

1. **Trigger conditions** - Should we support conditions beyond snapshot
   count (e.g., oldest snapshot age, total metadata size)? Oldest snapshot
   age would need a second reserved property
   (`oldest-snapshot-timestamp-ms`); it is equally free to compute from
   `TableMetadata`, so it can be added later without redesign.
2. **PR granularity** - Single PR for all remaining layers or split
   per-layer?

---

## 10. Comparison with Compaction Flow

| Aspect            | Compaction                                                     | Snapshot Expiration                     |
| ----------------- | -------------------------------------------------------------- | --------------------------------------- |
| Policy type       | `system_iceberg_compaction`                                    | `system_iceberg_snapshot_expiration`    |
| Strategy type     | `iceberg-data-compaction`                                      | `iceberg-snapshot-expiration`           |
| Job template      | `builtin-iceberg-rewrite-data-files`                           | `builtin-iceberg-expire-snapshots`      |
| Scope             | Per-partition (scored, top-N selected)                          | Whole table                             |
| Handler base      | `BaseExpressionStrategyHandler` (expression-driven)             | `StrategyHandler` (single metadata check) |
| Data requirements | `TABLE_METADATA` + `TABLE_STATISTICS` + `PARTITION_STATISTICS` | `TABLE_METADATA`                        |
| Trigger input     | `custom-data-file-mse`, `custom-delete-file-number` (statistics) | `snapshot-count` (table metadata property) |
| Iceberg procedure | `rewrite_data_files`                                           | `expire_snapshots`                      |
| Key parameters    | strategy, sort-order, where, options (all required)             | older_than, retain_last (independently optional, fall back to Iceberg table properties), stream_results (defaults to `true`) |
