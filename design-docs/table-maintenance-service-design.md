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

# Design of Table Maintenance Service in Gravitino

## 1. Background

The Table Maintenance Service (TMS) in Gravitino is currently an alpha feature.
The `maintenance/optimizer` package already contains the execution core for statistics collection,
rule evaluation, strategy recommendation, metrics query, and job submission (`Updater`,
`Recommender`, providers, `JobSubmitter`).

Today that core is not hosted as a long-running Gravitino service. Without a server-side component:

1. There is no stable **server-side commit event** after Iceberg commits. Engines would otherwise each need a
   TMS-specific listener, or operators must schedule maintenance externally.
2. Configuration, audit, and service-level metrics are hard to centralize when execution is
   ad hoc and process-local.
3. Each run creates its own runtime and provider instances instead of a shared service lifecycle.
4. When Spark maintenance work is needed, submitted work already returns a `jobId` owned by the
   Gravitino job framework. That job-status boundary should stay.

This design turns TMS into a **main-server REST plugin** on port **8090** (same pattern as IdP
via `gravitino.server.rest.extensionPackages`) so colocated IRC can drive the
evaluate → submit pipeline **in-process** after commits, while reusing the existing optimizer
execution core.

---

## 2. Goals

1. **In-process plugin on the main server**: Load Table Maintenance through
   `gravitino.server.rest.extensionPackages` (Jersey 2 `Feature`, same pattern as IdP) so the IRC
   callback is registered in the main JVM. The commit path does **not** use HTTP. Operator calls that
   replace the optimizer CLI are the ops APIs in **§7**.
2. **IRC in-process commit event**: After successful Iceberg commits via IRC, TMS receives a commit
   event through a **main-server-registered in-process callback / SPI** (IRC and main server share one JVM; see **§5.1.1**). The event handler runs gates, policy trigger evaluation, and job submission in-process (see **§5.4**).
3. **Reuse existing optimizer execution core**: Event handling invokes the same `Updater` /
   `Recommender` / job-submit paths already present in `maintenance/optimizer`, as **in-process
   methods**, not as a second copy of the logic.
4. **Job framework compatibility**: Spark maintenance work continues to use the Gravitino job
   framework. TMS returns or records submitted `jobId` values but does not own job status.
5. **Govern Policy reuse**: Maintenance policies stay on existing `policy_meta` and metalake Policy
   APIs (create / alter / enable / disable / associate). TMS does **not** introduce a parallel policy
   store or `/api/maintenance/table/policies` CRUD.
6. **Multi-node safe event processing**: Use a shared DB claim on `table_maintenance_state` so only
   one TMS replica runs the evaluate → submit pipeline for a given `(table, policy)` at a time (§6).
7. **Commit log**: The **IRC post-commit hook** **INSERTs** one `table_maintenance_event` row per
   successful Iceberg commit (`snapshot_id`, `created_at`). TMS does not write this table, and the
   row is not updated. A policy that has not finished another run is read by joining that row to
   `table_maintenance_state.last_job_id` and `job_run_meta.job_finished_at`, compared with
   `minIntervalMs` (§6.3).

---

## 3. Non-Goals

1. **Standalone maintenance daemon**: No separate process or
   `gravitino-iceberg-rest-server.sh`-style entrypoint.
2. **Dedicated auxiliary HTTP listener**: No `GravitinoAuxiliaryService`, no isolated
   `gravitino.maintenance.classpath`, and no dedicated TMS port (for example **9301**). TMS is not
   a dedicated listener like `iceberg-rest` / `lance-rest`.
3. **Provider SPI rewrite**: Does not replace `StatisticsUpdater`, `StatisticsCalculator`,
   `StatisticsProvider`, `StrategyProvider`, `TableMetadataProvider`, or `JobSubmitter` contracts
   used by the event pipeline.
4. **Engine-side commit report path**: Engines that bypass Gravitino Iceberg REST are out of scope
   for event-driven path.
5. **Commit-path HTTP or Kafka**: No `POST …/events/iceberg-commit`, no health resource, and no Kafka
   produce/consume path. Commit handling is **in-process only** (§5.1.1). APIs that replace the
   optimizer CLI are **§7**, and they are not a commit ingress. Remote IRC / cross-JVM delivery is
   out of scope (follow-up if needed).

---

## 4. Solution Investigations

### 4.1 Option A: Keep process-local execution only

Continue running all optimizer work in ad hoc local processes, with no TMS service endpoint.

**Pros:** No new listener. Minimal implementation work.

**Cons:** No IRC event target; no centralized service for event-driven evaluate → submit.

**Decision:** Rejected.

### 4.2 Option B: In-process plugin on the main server (Chosen)

Register Table Maintenance as a Jersey 2 `Feature` through
`gravitino.server.rest.extensionPackages` (same pattern as IdP) so it runs inside the main server
process. The commit path does **not** use HTTP. After each Iceberg commit, the **colocated** IRC hook
INSERTs the commit row and invokes a main-server-registered **in-process** callback that upserts
state, takes an **atomic per-policy claim**, runs gates, `Recommender` trigger, and job submit.

**Pros:** No extra process or port; no remote event hop on the commit path; reuses Policy + Jobs on
the same server; matches plugin packaging.

**Decision:** **Chosen**.

### 4.3 Option C: Independent long-running Table Maintenance Service process

**Pros:** Full JVM isolation.

**Cons:** Extra deployable; duplicates server lifecycle patterns already covered by the main
webserver plugin.

**Decision:** Rejected. Prefer the in-process plugin on the main server.

### 4.4 Option E: Dedicated aux Jetty listener (:9301)

Implement `GravitinoAuxiliaryService` with `shortName() = "maintenance"`, expose a dedicated Jetty
listener (default **9301**), and keep TMS off the main 8090 JAX-RS app.

**Pros:** Classpath isolation similar to `iceberg-rest` / `lance-rest`.

**Cons:** Extra port and aux enablement; diverges from plugins that already extend
**8090** via `extensionPackages`.

**Decision:** Rejected. Prefer Option B.

---

## 5. Proposal

### 5.1 Architecture

```text
Spark / Flink / Trino
        │  Iceberg REST commit
        v
Gravitino Iceberg REST (IRC, typically :9001)
        │  commit succeeded (same JVM as main server)
        │
        └─ IRC post-commit hook
                │
                ├─ INSERT table_maintenance_event (one row per commit — §6.3)
                └─ in-process callback / SPI  →  TMS handler on main-server classpath
                        │
                        v
                 IcebergCommitEventHandler
                        │
                        ├─ upsert state rows per Active policy
                        ├─ for each policy: atomic DB claim on that policy row (multi-node — §6)
                        ├─ claim lost for a policy → leave that policy for another node
                        v
                 MaintenanceEvaluateSubmitPipeline
                        │
                        +--> per-policy gates (in-flight / min-interval)
                        +--> Recommender.submitForStrategyName(...) → submit when trigger passes
                        │
                        v
                 Gravitino Job framework (rewrite / cleanup / …)
```

#### 5.1.1 In-process commit event

Commit events are delivered **only in-process**. After a successful Iceberg commit, the **IRC
post-commit hook** INSERTs one `table_maintenance_event` row, then invokes a
**main-server-registered callback / SPI** (for example on `GravitinoEnv`). That callback enters
`IcebergCommitEventHandler` → `MaintenanceEvaluateSubmitPipeline` + `table_maintenance_state` claim.
TMS does not write the event row, and the row is not updated.

| Requirement | Detail                                                                                                                                    |
| ----------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| Deployment  | IRC (`iceberg-rest`) and the main Gravitino server share **one JVM**.                                                                     |
| Transport   | In-process callback / SPI only — **no** HTTP `POST …/events/iceberg-commit`, **no** Kafka.                                                |
| Payload     | Normalized `table_identifier` (`catalog.schema.table`) and the committed `snapshot_id`. Policy selection uses Active policies + triggers. |

### 5.2 Internal structure

| Part                                | Responsibility                                                                                                                                           |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `TableMaintenanceRESTFeature`       | Jersey 2 `Feature` registered via `extensionPackages`; registers the in-process callback and the ops resources (§7). No health or commit-event resource. |
| `IcebergCommitEventHandler`         | TMS in-process entry after the IRC hook has inserted the event; upserts state; claims; runs pipeline.                                                    |
| `MaintenanceEvaluateSubmitPipeline` | Per-policy claim → gates → `Recommender` → `JobSubmitter`.                                                                                               |
| `TableMaintenanceStateStore`        | Shared DB access for `table_maintenance_state` upsert / claim / release / rename (§6.1–§6.2, §6.4).                                                      |
| `TableMaintenanceEventStore`        | Shared DB access for `table_maintenance_event` insert / rename / drop (§6.3–§6.4).                                                                       |
| `IcebergTableLifecycleHook`         | In-process IRC rename/drop hook: rewrite or purge TMS rows keyed by `table_identifier` (§6.4).                                                           |
| Existing optimizer classes          | `Updater`, `Recommender`, providers, `JobSubmitter` — unchanged contracts for event path.                                                                |

### 5.3 User process (event-driven)

1. Operator enables the TMS REST plugin (`extensionPackages`) and `iceberg-rest` **in the same JVM**, and turns on in-process commit events (§5.1.1 / §8.2).
2. Operator creates / enables a maintenance policy and associates it to tables (or parents) via
   metalake Policy APIs, for example:

   ```bash
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
     -d '{"policiesToAdd": ["iceberg_compaction_default"]}' \
     http://localhost:8090/api/metalakes/test/objects/table/rest_catalog.db.t1/policies
   ```

3. Engines write through Gravitino Iceberg REST. On commit success, the **IRC hook** INSERTs one
   `table_maintenance_event` row (`table_identifier`, `snapshot_id`) and does not update it (§5.1.1,
   §6.3).
4. The hook then invokes TMS **in-process**. TMS resolves Active policies, upserts one state row per
   `(table_identifier, policy)`, and for each policy claims that row, runs gates → trigger →
   submits when thresholds are met.
5. Operators observe runs in the Gravitino **Jobs** UI / APIs.

### 5.4 Implementation process (event path)

```text
IRC commit succeeded (same JVM)
  └─ IRC post-commit hook (§5.1.1)
        │
        ├─ INSERT table_maintenance_event (§6.3)  ← one new row per commit; never UPDATE
        └─ in-process callback / SPI
              │
              v
        IcebergCommitEventHandler
              │
              ├─ resolve Active policies (StrategyProvider / listPolicies)
              ├─ upsert one state row per policy (§6.2)
              v
        MaintenanceEvaluateSubmitPipeline
              ├─ for each Active policy:
              │     atomic claim: that policy row IDLE → RUNNING (§6.1)
              │       └─ claim failed → skip this policy (another node holds it)
              │     if job_id is set and that job has finished:
              │       last_job_id = job_id; clear job_id
              │     if job_id is still QUEUED/STARTED → release claim; skip
              │     else apply min-interval using last_job_id → job_finished_at
              │     Recommender.submitForStrategyName(...)
              │       └── JobSubmitter → rewrite / … when trigger passes
              │       └── set job_id to this submission; do not change last_job_id
              │     release claim (state → IDLE on that policy row; do not DELETE)
```

The in-process handler runs the gate + submit path on the calling thread (or a bounded executor
owned by the plugin — implementation detail). Maintenance Spark jobs are **submitted asynchronously**
via the job framework; the event path does not block on Spark completion. **The IRC hook INSERTs
one event row per commit; TMS does not update it.** Per-policy claim keeps evaluate → submit single-flight for each
`(table, policy)` across nodes. A non-null `job_id` is the in-flight submission and blocks another
submit. When that job has finished, the pipeline moves it to `last_job_id` and clears `job_id`.
`last_job_id` → `job_run_meta.job_finished_at` is the previous end time used with `minIntervalMs`
(§6.3). Stale `RUNNING` claims are released after
`claimTimeoutMs` (§6.2).

**Gate order:**

1. If `job_id` is set and that job has finished: `last_job_id = job_id`, then clear `job_id`.
2. If `job_id` is still in flight (`QUEUED` / `STARTED`): skip this policy.
3. Otherwise apply min-interval using `last_job_id` → `job_run_meta.job_finished_at` and the resolved
   `minIntervalMs` (table prop → global conf → code default; §8.3).
4. Policy trigger (`Recommender`) for each remaining Active policy.
5. On submit: set `job_id` to this submission. Do not change `last_job_id`.

### 5.5 Combined maintenance policy and ordered builtin job

Gravitino's job framework submits **one** template run at a time. It has no workflow / DAG
orchestration. To run compaction, manifest rewrite, snapshot expiry, and orphan cleanup **in one
ordered Spark job**, TMS adds:

1. A new built-in **policy type** (illustrative name: `system_iceberg_table_maintenance`) whose
   content configures the four operations (enable flags, thresholds, and optional per-operation
   intervals).
2. A matching built-in **job template** (illustrative name: `builtin-iceberg-table-maintenance`)
   that executes the selected operations **inside that one Spark job**.

Operators create one policy instance of that type under the metalake and attach it (directly or via
tag) to catalogs / schemas / tables. That instance has one real `policy_meta.policy_id`. TMS keeps
one `table_maintenance_state` row for `(table_identifier, policy_id)` and submits **one** job when
gates pass. A separate table-level sentinel such as `policy_id = 0` is **not** required for this
model: the per-policy claim already serializes maintenance for that attachment. Do not also attach
the older single-operation maintenance policies on the same table unless an extra table-level mutex
is introduced.

**In-job execution order** (skip any operation that is disabled or whose own interval / trigger did
not pass; keep relative order):

```text
1. compact           (rewrite data files)
2. manifests         (rewrite manifests)
3. expire            (expire snapshots)
4. orphan            (remove orphan files)
```

This order matches a commit-driven / query-first path (typical for TMS and streaming-heavy tables):

- Compact first so readers stop paying for small files.
- Rewrite manifests after compact so the index matches the post-compaction file set (rewriting
  manifests before compact is wasted when compact immediately fragments them again).
- Expire next so old snapshots that still referenced the pre-compaction small files can drop those
  files from metadata.
- Orphan last to delete physical leftovers after expiry (and failed-write debris), behind a safety
  retention window.

An alternate full-cleanup order (`expire → orphan → compact → manifests`) favors overnight
storage reclaim and avoids compacting data that expiry would drop. That order is optional for a
separate batch package; the default combined job for TMS uses the compact-first sequence above.

**Minimum intervals:** yes — add a **job-level** interval for the combined policy, and keep
**per-operation** intervals for steps inside the job (§8.3).

- Job-level `table-maintenance` `minIntervalMs` gates how often TMS may submit the combined job
  (same resolve order as other task types; default aligned with compaction, 1 hour).
- Per-operation intervals decide which of the four steps run in **this** submission (for example
  orphan may still be 7 days even when the combined job is eligible hourly). The job receives the
  selected `ops` subset in `jobConf`.

Custom maintenance policies remain supported as separate `policy_id` rows. The combined type is an
additional built-in option, not a replacement for the Policy API.

---

## 6. Multi-node coordination (shared claim)

On **multiple** Gravitino / TMS nodes, an in-process event may run on **any** replica that hosts
colocated IRC and receives the commit. Without coordination, two nodes could both evaluate and
submit the same policy's job for the same table.

**Approach:** two shared tables in the Gravitino entity DB. Table identity uses a **normalized
string `table_identifier`** (`catalog.schema.table`), **not** `table_meta.table_id`.

Iceberg REST / optimizer tables often have **no** row in `table_meta` (same reason
`table_metrics` stores `table_identifier`, and `iceberg_cleanup_job` keys by
`catalog_id` + `namespace` + `table_name`). TMS must not require Gravitino table metadata to exist.

| Table                     | Role                                                                                        |
| ------------------------- | ------------------------------------------------------------------------------------------- |
| `table_maintenance_event` | **Commit log** — one INSERT per successful Iceberg commit (§6.3)                            |
| `table_maintenance_state` | Multi-node **claim** + in-flight `job_id` and finished `last_job_id` per policy (§6.1–§6.2) |

`table_maintenance_state` primary key is `(metalake_id, table_identifier, policy_id)` — **one row
per attached maintenance policy**. Claim is **per policy row**: each `(table, policy)` is claimed
independently.

Policy attachment remains in `policy_relation_meta` (resolved via `listPolicies()` / object
identifier APIs); the state table stores multi-node claim state, the in-flight `job_id`, and the
last finished `last_job_id` per policy.
The event table stores one row per commit. `metalake_id` and
`policy_id` still come from Gravitino Policy / metalake metadata; only **table** identity avoids
`table_meta`.

### 6.1 Claim flow

```text
Node A / Node B — both receive an event for same table + same policy
        │
        ├─ both IRC hooks INSERT table_maintenance_event   ← one row per commit (§6.3)
        ├─ both resolve Active policies; upsert one row per policy
        ├─ both attempt per-policy claim:
        │     UPDATE … SET state=RUNNING
        │     WHERE metalake_id=? AND table_identifier=? AND policy_id=? AND state=IDLE
        │     ├─ Node A: rows_affected = 1 → runs that policy → release to IDLE
        │     └─ Node B: 0 rows → skip that policy (another node / reclaim)
        v
Different policies on the same table may be claimed by different nodes concurrently
```

Gate checks alone are insufficient (read race). **Claim is the write lock** for that policy row;
gates for a policy run only after its claim succeeds. Each attached policy is unique for a table, so
per-policy claim prevents double-submit of the same job without locking unrelated policies.
**Event insert is not the lock.** It records the commit. The claim on `table_maintenance_state` is
the lock.

### 6.2 State table (shared store)

One relational table holds multi-node claim, the in-flight job, and the last finished job per policy. Style follows
work-queue tables such as `iceberg_cleanup_job` (no soft-delete / version / audit boilerplate).
Table keying follows optimizer **`table_metrics.table_identifier`** (string identity), not
`table_meta.table_id`.

**Table name:** `table_maintenance_state`

| Column             | Type                       | Notes                                                                                     |
| ------------------ | -------------------------- | ----------------------------------------------------------------------------------------- |
| `metalake_id`      | `BIGINT UNSIGNED NOT NULL` | Metalake that owns the maintenance policy                                                 |
| `table_identifier` | `VARCHAR(512) NOT NULL`    | Normalized `catalog.schema.table` (same form as optimizer / event payload)                |
| `policy_id`        | `BIGINT UNSIGNED NOT NULL` | Real `policy_meta.policy_id`                                                              |
| `state`            | `VARCHAR(16) NOT NULL`     | `IDLE` / `RUNNING` only (per policy row)                                                  |
| `updated_at`       | `BIGINT NOT NULL`          | Epoch millis; claim / reclaim                                                             |
| `job_id`           | `BIGINT UNSIGNED NULL`     | In-flight job for **this policy** (`job_run_meta.job_run_id`). Null when none is running. |
| `last_job_id`      | `BIGINT UNSIGNED NULL`     | Last finished job for **this policy**. Its `job_finished_at` is the previous end time.    |

**Primary key:** (`metalake_id`, `table_identifier`, `policy_id`).

`state` has two values: `IDLE` (unclaimed) and `RUNNING` (a node holds evaluate → submit).

**Lifecycle:**

1. On each event: resolve Active policies → `INSERT` each missing `(table_identifier, policy)` row
   as `state=IDLE`. On duplicate key, **do not** change `state` (a live `RUNNING` claim must stay).
2. Claim: conditional `UPDATE … SET state=RUNNING WHERE metalake_id=? AND table_identifier=? AND
   policy_id=? AND state=IDLE` (and reclaim stale `RUNNING` after `claimTimeoutMs` by setting it
   back to `IDLE`). `rows_affected = 1` owns the lock.
3. If `job_id` is set and that job has finished: `last_job_id = job_id`, then set `job_id` to null.
   Do this only after the job ends, not at submit time.
4. Before submit: if `job_id` is still set (job still active) → set `state=IDLE` and skip that policy.
5. On submit: set `job_id` to this submission's `job_run_id`. Do **not** change `last_job_id`.
6. Done: set `state=IDLE` on **that policy row**. **Do not DELETE** — keep `job_id` and `last_job_id`.

Illustrative MySQL DDL:

```sql
CREATE TABLE IF NOT EXISTS `table_maintenance_state` (
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `table_identifier` VARCHAR(512) NOT NULL COMMENT 'normalized catalog.schema.table',
    `policy_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'policy id from policy_meta',
    `state` VARCHAR(16) NOT NULL COMMENT 'IDLE|RUNNING',
    `updated_at` BIGINT(20) NOT NULL COMMENT 'last state upsert time in epoch millis',
    `job_id` BIGINT(20) UNSIGNED NULL COMMENT 'in-flight job_run_id; null when none is running',
    `last_job_id` BIGINT(20) UNSIGNED NULL COMMENT 'last finished job_run_id',
    PRIMARY KEY (`metalake_id`, `table_identifier`, `policy_id`),
    KEY `idx_state_updated` (`state`, `updated_at`),
    KEY `idx_table_identifier` (`table_identifier`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT 'TMS multi-node event claim, in-flight job_id, last finished job';
```

### 6.3 Commit log (`table_maintenance_event`)

The **IRC post-commit hook** INSERTs one row for each successful Iceberg commit. TMS does not write
or update that row. Like
`table_metrics`, rows key by **`table_identifier`** string — **not** `table_meta.table_id` — so IRC
tables without Gravitino table metadata still persist. `snapshot_id` comes from the commit.

```text
IRC post-commit hook
      │
      ▼
INSERT table_maintenance_event   ← one new row; never UPDATE
      │
      ▼
in-process callback → TMS upsert + claim (table_maintenance_state) → pipeline (§5.4)
```

| Column             | Type                       | Notes                                    |
| ------------------ | -------------------------- | ---------------------------------------- |
| `event_id`         | `BIGINT UNSIGNED NOT NULL` | Surrogate PK (auto-increment)            |
| `metalake_id`      | `BIGINT UNSIGNED NOT NULL` | Metalake from config / policy resolution |
| `table_identifier` | `VARCHAR(512) NOT NULL`    | Normalized `catalog.schema.table`        |
| `snapshot_id`      | `BIGINT NOT NULL`          | Snapshot of this commit                  |
| `created_at`       | `BIGINT NOT NULL`          | Epoch millis when the row was inserted   |

**Primary key:** (`event_id`). **Index:** (`metalake_id`, `table_identifier`, `created_at`).

There is **no** unique key on `snapshot_id`. Every commit INSERTs, including a repeated callback for
the same snapshot. Claim, in-flight `job_id`, and `last_job_id` still prevent double-submit (§6.1–§6.2).

**Which policy has not finished another run**

Join the commit row to the policy rows for that table, then to the last **finished** job:

```text
table_maintenance_event e
  JOIN table_maintenance_state s
    ON s.metalake_id = e.metalake_id
   AND s.table_identifier = e.table_identifier
  JOIN job_run_meta j
    ON j.job_run_id = s.last_job_id
```

`j.job_finished_at` is the end time of that policy's previous finished job. Resolve `minIntervalMs`
for the policy's task type (§8.3). When `s.job_id` is null and
`e.created_at - j.job_finished_at > minIntervalMs`, the policy has not completed another run after
the interval elapsed. A non-null `s.job_id` means a job is still in flight, so this commit is not a
missed run.

Illustrative MySQL DDL:

```sql
CREATE TABLE IF NOT EXISTS `table_maintenance_event` (
    `event_id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'commit event id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `table_identifier` VARCHAR(512) NOT NULL COMMENT 'normalized catalog.schema.table',
    `snapshot_id` BIGINT(20) NOT NULL COMMENT 'snapshot id of this commit',
    `created_at` BIGINT(20) NOT NULL COMMENT 'insert time epoch millis',
    PRIMARY KEY (`event_id`),
    KEY `idx_table_created` (`metalake_id`, `table_identifier`, `created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT 'TMS commit log; one INSERT per Iceberg commit';
```

### 6.4 Table rename / drop lifecycle (required with string keys)

Because TMS keys by **`table_identifier`** (not a stable `table_id`), a rename would otherwise orphan
claim / `job_id` / `last_job_id` rows and break cooldown / in-flight gates. Historical **`table_metrics`** rows can
tolerate orphan names; **`table_maintenance_state` cannot**.

**Hook:** after a successful Iceberg table rename (or drop), IRC invokes an in-process
`IcebergTableLifecycleHook` registered by the TMS plugin (same classloader-boundary pattern as the
commit-event callback — §5.1.1). Prefer wiring next to existing IRC rename/drop paths (e.g.
`IcebergTableHookDispatcher.renameTable` / `IcebergRenameTableEvent`).

#### Rename (`old_identifier` → `new_identifier`)

1. **`table_maintenance_state`:** rewrite every row for the metalake:
   `UPDATE … SET table_identifier = new WHERE metalake_id = ? AND table_identifier = old`.
   Preserve `state`, `job_id`, `last_job_id`, `updated_at` (except bump `updated_at` for audit). If a conflicting
   destination key already exists (rare), fail the rewrite loudly or merge per implementation policy
   — do not silently drop `job_id` or `last_job_id`.
2. **`table_maintenance_event`:** rewrite every row for the metalake:
   `UPDATE … SET table_identifier = new WHERE metalake_id = ? AND table_identifier = old`, so later
   joins still find those commits.
3. Policy attachments on Gravitino metadata objects (when present via `metadata_object_id`) are
   outside this table rewrite; object-id bindings survive rename when `table_meta` exists. String-
   based policy attachments, if any, must be updated by the Policy / IRC reconcile path separately.

#### Drop

1. **`table_maintenance_state`:** `DELETE` (or soft-clear) all rows for
   `(metalake_id, table_identifier)`.
2. **`table_maintenance_event`:** `DELETE` rows for `(metalake_id, table_identifier)`.
3. In-flight Spark jobs are **not** cancelled by this hook (job framework owns lifecycle); operators
   cancel via Jobs APIs if needed.

Catalog rename (changes the `catalog.` prefix of many identifiers) is a **follow-up** bulk rewrite;
This design covers table rename/drop within a catalog.

---

## 7. Optimizer CLI replacement APIs

The commit path stays in-process and does **not** call these APIs. They replace the
`gravitino-optimizer` CLI (`--type ...`) for operators and scripts. The same plugin serves them on
the main webserver (**8090**).

- Prefix: `/api/maintenance/table/ops/...`. Not shown in the Compact-policy UI.
- Caller must have **WRITE** on each target table. Missing privilege → **403**.
- `--conf-path` stays server configuration. `update-statistics` and `append-metrics` send JSON Lines
  in the body. The API does not accept a server `--file-path`.
- `dryRun=true` returns the recommendation or job config and does not submit.

| CLI `--type`              | Method | Path                                           | Body or query                                                                                      |
| ------------------------- | ------ | ---------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `submit-strategy-jobs`    | `POST` | `/api/maintenance/table/ops/strategy-jobs`     | `identifiers`, `strategyName`, `dryRun`, `limit`                                                   |
| `submit-update-stats-job` | `POST` | `/api/maintenance/table/ops/update-stats-jobs` | `identifiers`, `dryRun`, `updateMode` (`stats` / `metrics` / `all`), `updaterOptions`, `sparkConf` |
| `update-statistics`       | `POST` | `/api/maintenance/table/ops/statistics`        | `calculatorName`, `identifiers`, `statisticsPayload` (JSON Lines)                                  |
| `append-metrics`          | `POST` | `/api/maintenance/table/ops/metrics`           | `calculatorName`, `identifiers`, `statisticsPayload` (JSON Lines)                                  |
| `monitor-metrics`         | `POST` | `/api/maintenance/table/ops/metrics/monitor`   | `identifiers`, `actionTime`, `rangeSeconds`, `partitionPath`                                       |
| `list-table-metrics`      | `GET`  | `/api/maintenance/table/ops/metrics/tables`    | `identifiers`, `partitionPath`                                                                     |
| `list-job-metrics`        | `GET`  | `/api/maintenance/table/ops/metrics/jobs`      | `identifiers`                                                                                      |

Each route calls the existing optimizer command implementation. The IRC hook and
`MaintenanceEvaluateSubmitPipeline` do not call this group.

---

## 8. Configuration

### 8.1 Enablement keys (`gravitino.conf`)

| Key                                       | Default  | Description                                                                                               |
| ----------------------------------------- | -------- | --------------------------------------------------------------------------------------------------------- |
| `gravitino.server.rest.extensionPackages` | none     | Must include the TMS Feature package (illustrative: `org.apache.gravitino.maintenance.web.rest.feature`). |
| `gravitino.auxService.names`              | none     | Must include `iceberg-rest` when using IRC. TMS itself is **not** started this way.                       |
| `gravitino.maintenance.claimTimeoutMs`    | `300000` | Reclaim a stale `RUNNING` claim after the worker fails.                                                   |

### 8.2 Iceberg REST → TMS in-process event keys

Illustrative keys (exact names may be finalized in implementation).

| Key (illustrative)                                  | Default | Description                                                                                |
| --------------------------------------------------- | ------- | ------------------------------------------------------------------------------------------ |
| `gravitino.iceberg-rest.tableMaintenance.inProcess` | `false` | When `true`, IRC invokes the **main-server-registered event callback / SPI** after commit. |

Because IRC may use an isolated classloader, the callback must be registered by the TMS plugin (for
example on `GravitinoEnv`), not a direct cast to TMS implementation classes. IRC and the main
server must share **one JVM**.

```properties
gravitino.server.rest.extensionPackages = org.apache.gravitino.maintenance.web.rest.feature
gravitino.auxService.names = iceberg-rest
gravitino.iceberg-rest.tableMaintenance.inProcess = true
gravitino.maintenance.claimTimeoutMs = 300000
```

HTTP `tableMaintenance.uri` / Kafka produce-consume keys are **not** in scope (Non-Goal #5).

### 8.3 Task types and minimum interval (global default + table override)

TMS recognizes maintenance **task types**. Single-operation types remain available for dedicated
policies / jobs. The combined policy in §5.5 also introduces a **job-level** type that gates how
often the ordered builtin job may be submitted.

| Task type           | Typical job / policy                                     | Code default `minIntervalMs` |
| ------------------- | -------------------------------------------------------- | ---------------------------- |
| `compaction`        | rewrite data files                                       | `3600000` (1 hour)           |
| `snapshot-expiry`   | expire snapshots                                         | `86400000` (1 day)           |
| `manifest-rewrite`  | rewrite manifests                                        | `86400000` (1 day)           |
| `orphan-cleanup`    | orphan file cleanup                                      | `604800000` (7 days)         |
| `table-maintenance` | combined job (`builtin-iceberg-table-maintenance`, §5.5) | `3600000` (1 hour)           |

For `system_iceberg_table_maintenance`:

1. Resolve **`table-maintenance`** `minIntervalMs` against the policy row's `last_job_id` to decide
   whether TMS may submit the combined job.
2. Resolve each selected operation's task-type interval (and policy content overrides) to build the
   `ops` subset for this run. An operation whose interval has not elapsed is omitted; relative order
   stays `compact → manifests → expire → orphan`.

**Resolution order** (first hit wins), same idea as Amoro table props + AMS defaults:

```text
1. Table property override (if set)
2. Global gravitino.conf key (if set)
3. Code default in the table above
```

**Global keys** (`gravitino.conf`, prefix `gravitino.maintenance.`):

| Key                                    | Description                                           |
| -------------------------------------- | ----------------------------------------------------- |
| `task.compaction.minIntervalMs`        | Default min interval for compaction                   |
| `task.snapshot-expiry.minIntervalMs`   | Default min interval for snapshot expiry              |
| `task.manifest-rewrite.minIntervalMs`  | Default min interval for manifest rewrite             |
| `task.orphan-cleanup.minIntervalMs`    | Default min interval for orphan cleanup               |
| `task.table-maintenance.minIntervalMs` | Default min interval for the combined maintenance job |

**Table-level overrides** (Iceberg / Gravitino table properties):

| Property                                      | Overrides                                            |
| --------------------------------------------- | ---------------------------------------------------- |
| `maintenance.compaction.minIntervalMs`        | Compaction min interval for this table               |
| `maintenance.snapshot-expiry.minIntervalMs`   | Snapshot expiry min interval for this table          |
| `maintenance.manifest-rewrite.minIntervalMs`  | Manifest rewrite min interval for this table         |
| `maintenance.orphan-cleanup.minIntervalMs`    | Orphan cleanup min interval for this table           |
| `maintenance.table-maintenance.minIntervalMs` | Combined maintenance job min interval for this table |

The event path uses per-policy `last_job_id` → `job_run_meta.job_finished_at` and the resolved
`minIntervalMs` for that policy's task type (§5.4 / §5.5 / §6.2). `job_id` is only the in-flight
submission. Policy content still owns **trigger thresholds** (e.g. MSE); interval only caps how
often a successful submit may repeat.

Example table override:

```sql
ALTER TABLE rest_catalog.db.orders SET TBLPROPERTIES (
  'maintenance.table-maintenance.minIntervalMs' = '7200000',
  'maintenance.orphan-cleanup.minIntervalMs' = '604800000'
);
```

---

## 9. Work Plan and Checklist

### 9.1 Suggested Work Plan

This design delivers the in-process plugin, IRC commit hook, `table_maintenance_event` log, shared
`table_maintenance_state` + claim, and inline evaluate → submit pipeline.

| Phase | Work item                           | Notes                                                                                     |
| ----- | ----------------------------------- | ----------------------------------------------------------------------------------------- |
| 1     | Load the in-process plugin          | `TableMaintenanceRESTFeature` registers the callback. No health or commit-event resource. |
| 2     | Internal evaluate → submit pipeline | `MaintenanceEvaluateSubmitPipeline` + Settings gates; unit tests.                         |
| 3     | In-process IRC hook + event log     | IRC hook **INSERTs** the commit row (§6.3); TMS claim (§6).                               |
| 4     | Hardening                           | Service metrics, graceful shutdown, user docs.                                            |
| 5     | Optimizer CLI replacement APIs      | Ops resources in §7. Same commands as `gravitino-optimizer`.                              |
| 6     | Combined maintenance policy + job   | Built-in type + ordered job in §5.5; `table-maintenance` interval in §8.3.                |

#### Phase 1 checklist

- [ ] Add `TableMaintenanceRESTFeature` (Jersey 2 `Feature`) that registers the in-process callback.
      Ops resources are added in phase 5. Do not add a health or commit-event resource.
- [ ] Register via `gravitino.server.rest.extensionPackages` (illustrative package
      `org.apache.gravitino.maintenance.web.rest.feature`).
- [ ] Package plugin jars with the main Gravitino server distribution (on the main server classpath).
- [ ] Document `extensionPackages` enablement.
- [ ] Add a unit test that the feature registers the callback and exposes no commit or health
      resource.

#### Phase 2 checklist

- [ ] Implement `MaintenanceEvaluateSubmitPipeline` calling `Recommender.submitForStrategyName`.
- [ ] Enforce per-policy gates: in-flight / min-interval.
- [ ] Resolve Active attached policies via existing Policy / `StrategyProvider` (no new policy store).
- [ ] Add unit tests for skip / noop / submit / deferred outcomes.

#### Phase 3 checklist

- [ ] Add `IcebergCommitEventHandler` and main-server-registered in-process callback / SPI (§5.1.1 /
      §8.2).
- [ ] Add EntityStore migration for **`table_maintenance_event`** (§6.3) and
      **`table_maintenance_state`** (§6.2).
- [ ] IRC post-commit hook **INSERTs** one `table_maintenance_event` row per commit (§6.3). TMS does
      not write or update that row. Read "policy has not finished another run" by joining
      `created_at` to `last_job_id` → `job_run_meta.job_finished_at` and `minIntervalMs`.
- [ ] Upsert + **per-policy claim** on `table_maintenance_state` (§6.1).
- [ ] Wire IRC post-commit hook to the in-process callback (`tableMaintenance.inProcess`).
- [ ] Wire IRC **rename/drop** in-process hook to rewrite / purge `table_maintenance_state` and
      `table_maintenance_event` rows (§6.4).
- [ ] Integration tests: each commit inserts one event row; claim runs the pipeline once; no
      double-submit; when `job_id` is null and `created_at - last_job_id.job_finished_at > minIntervalMs`
      the policy has not completed another run; a non-null `job_id` is in flight; on finish,
      `job_id` moves to `last_job_id` and `job_id` is cleared; rename updates `table_identifier`;
      drop clears state and event rows.
- [ ] Do **not** ship HTTP `…/events/iceberg-commit` or Kafka ingress.

#### Phase 4 checklist

- [ ] Service metrics: event insert counts, submit counts, claim conflicts, failures.
- [ ] Graceful shutdown tests.
- [ ] Update user-facing TMS / optimizer docs for in-process event mode.

#### Phase 5 checklist

- [ ] Add the seven ops resources in §7, each calling the existing optimizer command implementation.
- [ ] Require WRITE on each target table; missing privilege returns 403.
- [ ] `dryRun=true` returns the recommendation or job config and does not submit.
- [ ] Accept statistics and metrics JSON Lines in the body. Do not accept a server `--file-path`.
- [ ] Tests: each CLI `--type` maps to one route; the commit path does not call these routes.

#### Phase 6 checklist

- [ ] Add built-in policy type `system_iceberg_table_maintenance` and job template
      `builtin-iceberg-table-maintenance` (§5.5).
- [ ] Execute selected ops in order: `compact → manifests → expire → orphan`.
- [ ] Gate submit with `table-maintenance` `minIntervalMs`; select `ops` with per-operation
      intervals (§8.3).
- [ ] One state row and one `job_id` per attached combined policy; no `policy_id = 0` sentinel.
- [ ] Tests: subset ops skip correctly; order preserved; job-level and per-op intervals apply.

### 9.2 Review Checklist

| Area         | Checklist                                                                                                        |
| ------------ | ---------------------------------------------------------------------------------------------------------------- |
| Deployment   | Enabled via `gravitino.server.rest.extensionPackages`; IRC colocated in the same JVM. Ops APIs on **8090** (§7). |
| Classpath    | TMS plugin on main server classpath; **not** an aux isolated listener.                                           |
| Event        | **In-process only** (§5.1.1); IRC hook inserts the commit row; TMS claims and evaluates; no HTTP/Kafka.          |
| Ops API      | Seven routes replace `gravitino-optimizer` (§7). Not used by the commit path. Table WRITE required.              |
| Pipeline     | Inline on event: per-policy claim → gates → `Recommender` → Jobs; no metrics/monitor on event path.              |
| Durability   | IRC hook INSERTs one `table_maintenance_event` row per commit; TMS does not update it (§6.3).                    |
| Rename/drop  | In-process lifecycle hook rewrites / purges string-keyed TMS rows (§6.4).                                        |
| Multi-node   | Shared `table_maintenance_state` + DB **claim** (§6.1–§6.2); no CronJob.                                         |
| Policy       | Reuses metalake Policy APIs + `policy_meta`; no TMS policy CRUD.                                                 |
| Job boundary | Spark work stays in Gravitino job framework; stats land in `statistic_meta` (main DB).                           |
| Security     | Ops APIs require table WRITE. No commit-event or health endpoint.                                                |

---

## 10. References

1. [Gravitino Iceberg REST service](../docs/iceberg-rest-service.md)
2. [Gravitino Lance REST service](../docs/lance-rest-service.md)
3. [Manage policies in Gravitino](../docs/manage-policies-in-gravitino.md)
4. [Iceberg compaction policy](../docs/iceberg-compaction-policy.md)
5. [Table Maintenance optimizer overview](../docs/table-maintenance-service/optimizer.md)
6. [Design of SCIM 2.0 User and Group Provisioning in Gravitino](./gravitino-scim-provisioning.md)
7. [Amoro AIP-3 – Event-Triggered Optimization of Iceberg Tables](https://cwiki.apache.org/confluence/display/AMORO/AIP-3%3A+Event-Triggered+Optimization+of+Iceberg+Tables+in+Amoro)
8. [OpenHouse architecture (Jobs Scheduler / CronJob data services)](https://github.com/linkedin/openhouse/blob/main/ARCHITECTURE.md)
9. [Apache Iceberg REST Catalog OpenAPI](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
