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

The Table Maintenance Service (TMS) is an alpha feature. The `maintenance/optimizer` package already
has the execution core: statistics, rules, strategy, metrics, and job submit (`Updater`,
`Recommender`, providers, `JobSubmitter`).

That core is not yet a long-running Gravitino service. Without a server-side host:

1. No stable **server-side signal** after Iceberg commits (engines would need TMS listeners, or
   operators must schedule outside Gravitino).
2. Config, audit, and metrics stay ad hoc and process-local.
3. Each run builds its own runtime instead of a shared service lifecycle.

This design makes TMS a **main-server REST plugin** on port **8090** (same pattern as IdP via
`gravitino.server.rest.extensionPackages`) with colocated IRC, reusing the optimizer core. Spark
jobs stay on the Gravitino job framework (`jobId` boundary unchanged).

TMS uses a **dual trigger model** (§5.4–§5.6):

- **Commit path** — after each successful Iceberg commit, the IRC post-commit hook asynchronously
  invokes an in-process TMS callback for **compaction only** (§5.4.1).
- **Scheduled path** — each node runs a **`MaintenanceScheduler`** (`selectDueWork` per-row claim).
  When `next_due_at` is due, any node may claim and run that policy. **All four policy types** use
  this path.

---

## 2. Goals

1. **In-process plugin on the main server**: Load TMS via
   `gravitino.server.rest.extensionPackages` (Jersey 2 `Feature`, same as IdP) so the IRC callback is
   in the main JVM. Commit path does **not** use HTTP.
2. **Maintenance profile**: A profile such as `standard` creates and attaches all four policies with
   defaults in one step. Profiles are **not** a fifth policy type (§5.2).
3. **Precedence per type**: For each type, the **nearest** attachment along
   `table → schema → catalog → metalake` wins. Policies are **not** additive (§5.2).
4. **Dual trigger (option B)**: **Compaction** on **commit** (§5.4.1) **and** on the **scheduler** at
   wall-clock schedule (§5.4.2). Manifest rewrite, snapshot expiry, and orphan cleanup use the
   scheduler only (§5.5–§5.6). Policy **schedule** drives `next_due_at` for the **scheduler only**
   (§5.2.4).
5. **Wall-clock schedules in Gravitino**: Schedules live in Gravitino and are read by the scheduler,
   not by the commit hook.
6. **Reuse optimizer core**: Compaction submits one job that runs update-stats, decision, and
   rewrite in-process to the Spark job; other types reuse `Updater` / `Recommender` / submit as
   needed.
7. **Job framework compatibility**: Spark work stays on the job framework. TMS records `jobId` but
   does not own job status.
8. **Govern Policy reuse**: Policies stay on `policy_meta` and metalake Policy APIs. No parallel
   policy store or `/api/maintenance/table/policies` CRUD.
9. **Multi-node safe**: Shared DB **per-policy claims** so only one replica runs claim → submit
   for a `(table, policy)` (§6). Replicas stay **peers**; no maintenance **leader** (§5.3).

---

## 3. Non-Goals

1. **Standalone daemon**: No separate process or `gravitino-iceberg-rest-server.sh`-style entry.
2. **Dedicated aux HTTP listener**: No `GravitinoAuxiliaryService`, no
   `gravitino.maintenance.classpath`, no TMS-only port (e.g. **9301**).
3. **No maintenance leader**: No single node that scans all tables each tick. Timed work uses
   **per-node schedulers** and **per-row claims** (§5.3).
4. **Provider SPI rewrite**: Does not replace `StatisticsUpdater`, `StatisticsCalculator`,
   `StatisticsProvider`, `StrategyProvider`, `TableMetadataProvider`, or `JobSubmitter`.
5. **Engine-side commit report**: Engines that bypass Gravitino Iceberg REST are out of scope for
   commit-path compaction.
6. **Commit-path HTTP or Kafka**: No `POST …/events/iceberg-commit`, no health resource, no Kafka.
   Commit handling is **in-process only** (§5.1.1).
7. **External clock APIs**: No `POST …/maintenance/run-due` (or CronJob) as an alternate timed clock.
   The built-in `MaintenanceScheduler` is the only schedule driver.

---

## 4. Solution Investigations

### 4.1 Deployment options

|                     | A: Process-local only                           | **B: In-process plugin (Chosen)**       | C: Separate TMS process                                  | D: Aux Jetty listener (:9301)                          |
| ------------------- | ----------------------------------------------- | --------------------------------------- | -------------------------------------------------------- | ------------------------------------------------------ |
| Pros                | Simple; no new listener                         | Same JVM plugin; no extra port          | Full JVM isolation                                       | Classpath isolation like IRC                           |
| Cons / why rejected | No IRC target; no central automated maintenance | Slightly couples TMS to the main server | Extra deployable; duplicates main-server plugin patterns | Extra port; diverges from **8090** `extensionPackages` |
| Decision            | Rejected                                        | **Chosen**                              | Rejected                                                 | Rejected                                               |

### 4.2 Maintenance trigger options

**Compaction** is most often tied to **writes/commits**. Manifest rewrite, expire, and orphan usually
are **not** run on every commit.

|                        | [AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-table-optimizers.html) | [Amoro](https://cwiki.apache.org/confluence/display/AMORO/AIP-3%3A+Event-Triggered+Optimization+of+Iceberg+Tables+in+Amoro) | [Databricks](https://docs.databricks.com/aws/en/tables/tune-file-size) | [Floe](https://github.com/nssalian/floe/blob/main/docs/policies.md) |
| ---------------------- | ----------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- | ------------------------------------------------------------------- |
| Write / commit trigger | —                                                                                         | compaction                                                                                                                  | compaction                                                             | compaction                                                          |
| What stays scheduled   | compaction; snapshot expire; orphan clean                                                 | snapshot expire; orphan clean                                                                                               | compaction; manifest rewrite; snapshot expire; orphan clean            | compaction; manifest rewrite; snapshot expire; orphan clean         |

**Why TMS limits the commit path to compaction:**

1. Manifest rewrite, snapshot expire, and orphan clean are too heavy for the commit path (listing /
   scans hurt latency).
2. Inactive tables still need scheduled compaction when commits stop; expire / orphan must not depend
   on successful commits (orphans can appear without one).
3. Expire / orphan need fresh table-wide metadata; industry products keep them on a separate
   schedule, with only compaction on the write path.

**TMS decision:** IRC commit path runs **`system_iceberg_compaction` only** (§5.4.1). Scheduler runs
**all four** types on schedule (§5.4.2, §5.2.4). Manifest / expire / orphan are **scheduler-only**.
Nightly compaction covers tables that stop receiving commits.

### 4.3 Multi-node schedule options

Peer nodes typically use one of three patterns: **1** = policy grain; **2** = row grain;
**3** = external Cron + queue.

|                  | 1. Policy-level compete                                                                                     | 2. Row-level CAS                                                                                                                                                   | 3. External cron enqueue                                                                                                                                        |
| ---------------- | ----------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Typical products | ShedLock; Spring + Redis/DB lock; Quartz JDBC Cluster                                                       | Temporal lease; Hangfire; db-scheduler; SQS visibility timeout (analogy)                                                                                           | OpenHouse CronJob; Floe                                                                                                                                         |
| Pros             | Simple or mature; one winner per policy fire; prevents double runs of the same policy                       | Peers claim different `(table, policy)` rows — no whole-policy lock; same claim shared with the commit path (no double-submit)                                     | Decouples trigger from execution; consumers scale on the **external** queue                                                                                     |
| Cons             | Winner then lists the whole policy scope — hard to parallelize **per table**; lock/trigger is policy-scoped | —                                                                                                                                                                  | Requires **extra components** (external Cron and/or message queue); duplicate-enqueue and consumer **idempotency** still needed                                 |
| Chosen? Reason   | **Rejected.** Coarse policy grain; does not give per-table claim shared with the commit path.               | **Chosen** (§5.3). Matches in-tree cleanup; scheduler and commit path share the same `(table, policy)` claim; scales with due rows, not with a single policy lock. | **Rejected.** TMS must not introduce other runtime components beyond Gravitino and its entity DB. Pattern **2** keeps coordination in-process + existing store. |

---
### 4.4 Table discovery options

Products close the gap from catalog/scope defaults to runnable table work differently:

|      | [AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/catalog-level-optimizers.html) | [Apache Amoro](https://amoro.apache.org/docs/latest/configurations/) | [Floe](https://github.com/nssalian/floe/blob/main/docs/policies.md) |
| ---- | ------------------------------------------------------------------------------------ | -------------------------------------------------------------------- | ------------------------------------------------------------------- |
| How  | Copy catalog default to table on Create/Update                                       | Runtime-merge catalog settings into managed tables                   | Each cron tick lists tables in scope and runs                       |
| Cons | Catalog changes do not re-arm tables that already have table-level optimizers        | Tables not yet in AMS / unseen by the scheduler do not run           | Work waits for the next tick; each tick re-lists the scope          |

**TMS decision — discovery (§5.2.5, §5.3.2):**

1. **Create/Update/Drop hooks:** on IRC `createTable` / `updateTable` / `dropTable`, refresh or purge
   that table's maintenance state (nearest-wins → `table_maintenance_state` / `next_due_at`; drop
   deletes state rows — §6.3).
2. **Periodic discovery:** call IRC/Iceberg catalog APIs on an interval to reconcile scope —
   INSERT missing state rows, UPDATE wrong ones, DELETE stale — for tables that never went through
   IRC APIs.
3. **Discovery stays separate from the schedule loop:** discovery reconciles state rows (insert /
   update / delete); the scheduler (`selectDueWork`) only claims already-due rows and runs work.


## 5. Proposal

### 5.1 Architecture

```text
Spark / Flink / Trino → Iceberg REST commit
        v
Gravitino IRC (:9001, same JVM as main server)
        └─ post-commit hook (§5.1.1, §5.4.1)
                └─ async IcebergCommitEventHandler → compaction only

MaintenanceScheduler (every node — §5.3)
        selectDueWork → claim → submit
        ├─ Compaction due (§5.4.2)  // job: update-stats → decision → compaction
        ├─ Track A (§5.5): manifest | expire
        ├─ Track B (§5.6): orphan (oldest-cleanup-first)
        v
Gravitino Job framework + job_run_meta (§6.4)
```

#### 5.1.1 In-process commit callback

After a successful Iceberg commit, IRC dispatches post-events on the shared `EventBus`. The TMS
`EventListenerPlugin` registered via `gravitino.eventListener.*` (§7.2) — e.g.
`IcebergCommitEventHandler` — handles them asynchronously (§5.4.1). No non-compaction policy
resolve, Recommender, or submit on the IRC thread.

|        | Deployment                             | Transport                                               | Payload                        | Commit scope                                   | IRC thread cost                               |
| ------ | -------------------------------------- | ------------------------------------------------------- | ------------------------------ | ---------------------------------------------- | --------------------------------------------- |
| Detail | IRC and main server share **one JVM**. | In-process `EventBus` only — **no** HTTP, **no** Kafka. | Normalized `table_identifier`. | **`system_iceberg_compaction` only** (§5.4.1). | Async `EventListenerPlugin` (`ASYNC_*` mode). |

---

### 5.2 Policy model

#### 5.2.1 Four built-in policy types

Each activity is a **separate** built-in policy type with its own `content` and `minIntervalMs`:

|                          | Compaction                                   | Manifest rewrite                    | Snapshot expiry                      | Orphan cleanup                        |
| ------------------------ | -------------------------------------------- | ----------------------------------- | ------------------------------------ | ------------------------------------- |
| Illustrative policy type | `system_iceberg_compaction`                  | `system_iceberg_rewrite_manifests`  | `system_iceberg_snapshot_expiration` | `system_iceberg_orphan_file_removal`  |
| Built-in job template    | `builtin-iceberg-compaction`                 | `builtin-iceberg-rewrite-manifests` | `builtin-iceberg-expire-snapshots`   | `builtin-iceberg-remove-orphan-files` |
| Trigger path             | **Commit** (§5.4.1) **+ Scheduler** (§5.4.2) | **Scheduler** (§5.3, §5.5)          | **Scheduler** (§5.3, §5.5)           | **Scheduler** (§5.3, §5.6)            |

#### 5.2.2 Maintenance profile (one-step setup)

A **profile** such as `standard` creates four policies with defaults — not a fifth `policyType`.

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{"profile":"standard","target":"catalog.rest_catalog",
       "overrides":{"compaction":{"enabled":true},"snapshot-expiry":{"olderThanDays":7}}}' \
  http://localhost:8090/api/metalakes/test/maintenance/profiles/apply
```

#### 5.2.3 Precedence (nearest attachment wins)

```text
effective_policy(table, maintenance_type) =
  nearest Active attachment of that type along:
    table → schema → catalog → metalake
```

Only **one** policy per maintenance type is evaluated for a table.

#### 5.2.4 Policy schedule

Each policy stores a **schedule** in `policy_meta.content`; TMS sets wall-clock **`next_due_at`** from it.

**Illustrative `content.schedule` (crontab):**

|                             | `nightly_compaction` | `weekly_snapshot_expiry` | `manifest_rewrite` | `orphan_cleanup`               |
| --------------------------- | -------------------- | ------------------------ | ------------------ | ------------------------------ |
| `content.schedule` (stored) | `0 2 * * *`          | `0 3 * * 0`              | `0 4 * * *`        | `0 4 * * 0` + `enabled: false` |

**Commit vs scheduler:** `schedule` / `next_due_at` are **scheduler only**; commit neither reads nor advances them.

#### 5.2.5 Writing `table_maintenance_state`

`next_due_at` lives only on table-level `table_maintenance_state` rows.

|                             | Table                                                                               | **Above table** (schema / catalog / metalake)                                                   |
| --------------------------- | ----------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| When state rows are written | **Immediately** on policy change; IRC `createTable` / `updateTable`; detach deletes | **Timed discovery** (§5.3.2)                                                                    |
| Behavior                    | O(1) UPSERT/DELETE; set `next_due_at = nextOccurrence(schedule)`                    | Bind association only; discovery lists scope, nearest-wins, INSERT / UPDATE / DELETE state rows |

```text
effective_policy(table, type) → policy_id   // nearest Active along table → schema → catalog → metalake

UPSERT table_maintenance_state
  (metalake_id, table_identifier, policy_id, state='IDLE',
   next_due_at = nextOccurrence(schedule), …)
```

Only **one** state row per `(table, maintenance_type)` effective policy.

**IRC table lifecycle hooks (§5.3.2, §6.3):** `createTable` and `updateTable` UPSERT / refresh state
for effective (ancestor) policies; `dropTable` purges state. Tables that never call IRC
APIs rely on discovery.

---

### 5.3 Scheduled path: `MaintenanceScheduler`

`selectDueWork` vs discovery:

|        | Claim due work                                                      | Discovery                                                                                             |
| ------ | ------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| Config | `gravitino.maintenance.scheduler.pollIntervalSecs` (default **60**) | `gravitino.maintenance.scheduler.discoveryIntervalSecs` (default **3600**)                            |
| Work   | Select existing state rows with `next_due_at <= now`                | Reconcile scope: **INSERT** missing, **UPDATE** wrong (`policy_id` / `next_due_at`), **DELETE** stale |

#### 5.3.1 Worker pool and claim (no pending queue)

Each node runs a **fixed worker pool** (`workerThreads`, default **8**). Workers do **not** batch-load
due rows into an in-memory queue.

```text
worker loop (× workerThreads):
  SELECT up to candidateWindow due candidates   // IDLE or stale RUNNING; next_due_at <= now
  try CAS claim one row (§6.1)                  // only if this worker is free
  if claim wins → heartbeat → submit Spark → release IDLE when done
  if no claimable row → sleep pollIntervalSecs
```

| Concept            | Meaning                                                                                               |
| ------------------ | ----------------------------------------------------------------------------------------------------- |
| `workerThreads`    | Max **concurrent** claims / in-flight submits on this node (default **8**).                           |
| `candidateWindow`  | Max rows per **SELECT** for CAS retries (default **32**). May be **>** `workerThreads`.               |
| `pollIntervalSecs` | Sleep when a worker finds **no** claimable row (default **60**). Not “flush a batch every N seconds”. |

**Claim at most free capacity:** if 8 workers are busy, do **not** claim more rows. Extra due rows stay
`IDLE` (or reclaimable stale `RUNNING`) in `table_maintenance_state` until a worker is free or another
node claims them. Do **not** mark them `RUNNING` and park them in a process queue (that breaks
heartbeat reclaim and loses work on crash).

#### 5.3.2 Scope discovery (above-table attachments)

Discovery expands schema / catalog / metalake attachments into **table-level** state rows (§5.2.5).
It does **not** replace `selectDueWork`.

**Catalog source:** list tables from the **Iceberg/HMS** backend used by IRC — not only Gravitino
`table_meta`.

**One discovery round (illustrative):**

```text
1. Load enabled policies + attachments
2. For each above-table attachment: listTables(scope) via Iceberg/HMS
3. policy_id = effective_policy(table, type)  // nearest-wins
4. INSERT missing state rows with next_due_at = nextOccurrence(schedule)
5. UPDATE wrong rows (e.g. policy_id or next_due_at no longer matches effective policy / schedule)
6. DELETE stale (table, policy) rows; drop tables that left scope
```

**IRC table lifecycle hooks:** `createTable` / `updateTable` UPSERT or refresh ancestor state;
`dropTable` purges state (§6.3). Tables that never call IRC APIs rely on discovery.

---

### 5.4 Compaction: commit path + scheduler schedule (option B)

Compaction is the **only** type with two wake sources: IRC commit (§5.4.1) and scheduler schedule
(§5.4.2).

#### 5.4.1 Commit path (compaction only)

```text
IRC commit succeeded → post-commit hook (§5.1.1)
  └─ async IcebergCommitEventHandler:
        resolve effective compaction policy (§5.2.3); skip if disabled
        skip if state row missing (hooks / discovery own `next_due_at` — §5.2.5, §5.3.2)
        minIntervalMs gate (last_job_id — §7.3)
        claim row (§6.1) + register heartbeats
        submit one job; record job_run_meta (§6.4); release to IDLE when job finishes
        // job body: update-stats → decision → compaction (same Spark job)
        // do not read or write next_due_at — §5.2.4, §5.2.5
```

IRC thread: async hand-off only. No `next_due_at` check. Order: **`minIntervalMs` → claim →
submit** (one job: **update-stats → decision → compaction**). Missing state → skip. Async failure →
next commit or scheduler can still drive. Multi-node: claim (§6.1) prevents double-submit.

#### 5.4.2 Scheduler path (scheduled compaction)

Scheduler: `minIntervalMs` → claim rows with `next_due_at <= now` → submit the **same** one-job
pipeline (update-stats → decision → compaction); after success release to `IDLE` without changing
`next_due_at` (§5.2.5). Inactive tables still run when due; active ones may no-op inside the job
after update-stats. Manifest / expire / orphan: **scheduler only**.

---

### 5.5 Hot pipeline (scheduled — Track A)

Track A: **manifest** and **expire** as **separate** scheduled policies (own state row / claim each).
Compaction uses the compaction track (§5.4.2). Per-type `minIntervalMs` (§7.3).

---

### 5.6 Orphan cleanup track (scheduled — Track B)

Orphan is a **separate track**, not step 3 of Track A.

---

### 5.7 User process

1. Enable TMS plugin + `iceberg-rest` in the same JVM; turn on IRC hooks (§5.1.1 / §7.2).
2. Apply `standard` profile or create/attach four policies; set schedules (§5.2.4).
3. IRC commits trigger compaction callback (§5.4.1); scheduler claims due rows (§5.3).
4. Observe via Jobs APIs.

---

## 6. Multi-node coordination (shared claim)

Shared `table_maintenance_state` in the entity DB; identity is `table_identifier`
(`catalog.schema.table`), not `table_meta.table_id`. Multi-node **claim**, in-flight `job_id`,
finished `last_job_id` per policy (§6.1–§6.2).

Every node polls; **per-row CAS** picks the winner. PK: `(metalake_id, table_identifier, policy_id)`.

### 6.1 Claim flow (`selectDueWork` / commit path)

**Scheduler** (same CAS as `iceberg_cleanup_job.markRunning`):

```text
Both nodes SELECT up to candidateWindow due candidates → each free worker CAS-claims one:
  UPDATE … SET state=RUNNING, heartbeat_at=:now
  WHERE … AND (state=IDLE OR (state=RUNNING AND heartbeat_at < :heartbeatExpiry))
  winner (rows_affected=1) → heartbeat → submit → IDLE when job finishes
  loser / no free worker → leave row due in DB; try next candidate or sleep pollIntervalSecs
Never claim more rows than free workers; unclaimed due rows stay in table_maintenance_state
Heartbeats cover scheduler + commit-path claims (same pattern as iceberg_cleanup_job.heartbeat_at)
```

**Commit path** uses the same CAS and **must** register heartbeats; it does **not** read/advance
`next_due_at`. Gate with `minIntervalMs` **before** claim; **claim is the write lock**. Submitted
compaction job runs **update-stats → decision → compaction** in one Spark job (§5.4.1).

### 6.2 State table (shared store)

**Table name:** `table_maintenance_state`

|       | `metalake_id`              | `table_identifier`                | `policy_id`                | `state`                | `next_due_at`                              | `job_id`               | `last_job_id`                      | `heartbeat_at`                                     |
| ----- | -------------------------- | --------------------------------- | -------------------------- | ---------------------- | ------------------------------------------ | ---------------------- | ---------------------------------- | -------------------------------------------------- |
| Type  | `BIGINT UNSIGNED NOT NULL` | `VARCHAR(512) NOT NULL`           | `BIGINT UNSIGNED NOT NULL` | `VARCHAR(16) NOT NULL` | `BIGINT NOT NULL`                          | `BIGINT UNSIGNED NULL` | `BIGINT UNSIGNED NULL`             | `BIGINT NOT NULL`                                  |
| Notes | Metalake owning the policy | Normalized `catalog.schema.table` | `policy_meta.policy_id`    | `IDLE` / `RUNNING`     | Scheduler eligibility, epoch millis (§5.3) | In-flight `job_run_id` | Last finished; drives min-interval | Last worker heartbeat; stale `RUNNING` reclaimable |

**Primary key:** (`metalake_id`, `table_identifier`, `policy_id`).

Illustrative MySQL DDL:

```sql
CREATE TABLE IF NOT EXISTS `table_maintenance_state` (
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `table_identifier` VARCHAR(512) NOT NULL COMMENT 'normalized catalog.schema.table',
    `policy_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'policy id from policy_meta',
    `state` VARCHAR(16) NOT NULL COMMENT 'IDLE|RUNNING',
    `next_due_at` BIGINT(20) NOT NULL COMMENT 'scheduler eligibility time, epoch millis',
    `job_id` BIGINT(20) UNSIGNED NULL COMMENT 'in-flight job_run_id',
    `last_job_id` BIGINT(20) UNSIGNED NULL COMMENT 'last finished job_run_id',
    `heartbeat_at` BIGINT(20) NOT NULL COMMENT 'last heartbeat from worker, 0 when not running',
    PRIMARY KEY (`metalake_id`, `table_identifier`, `policy_id`),
    KEY `idx_due_state` (`next_due_at`, `state`),
    KEY `idx_table_identifier` (`table_identifier`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT 'TMS per-policy due time, claim, and job state';
```

### 6.3 Table rename / drop lifecycle (required with string keys)

String keys need rewrite/purge via `IcebergTableLifecycleHook` (§7.2).

#### Rename

1. **`table_maintenance_state`:** `UPDATE … SET table_identifier = new WHERE … = old`.

#### Drop

1. **`table_maintenance_state`:** `DELETE` all rows for `(metalake_id, table_identifier)`.

### 6.4 Job run history (`job_run_meta`)

Every submission creates a `job_run_meta` row. On finish: update `last_job_id`, clear `job_id`.
---

## 7. Configuration

### 7.1 Enablement keys (`gravitino.conf`)

| Key                                                     | Default | Description                                              |
| ------------------------------------------------------- | ------- | -------------------------------------------------------- |
| `gravitino.server.rest.extensionPackages`               | none    | TMS Feature package.                                     |
| `gravitino.auxService.names`                            | none    | Include `iceberg-rest` when using IRC.                   |
| `gravitino.maintenance.scheduler.pollIntervalSecs`      | `60`    | Sleep when a worker finds no claimable due row (§5.3.1). |
| `gravitino.maintenance.scheduler.workerThreads`         | `8`     | Concurrent claim/submit workers per node (§5.3.1).       |
| `gravitino.maintenance.scheduler.candidateWindow`       | `32`    | Max SELECT candidates per claim attempt (§5.3.1).        |
| `gravitino.maintenance.scheduler.discoveryIntervalSecs` | `3600`  | Discovery interval for above-table attachments (§5.3.2). |
| `gravitino.maintenance.scheduler.heartbeatTimeoutSecs`  | `300`   | Stale `heartbeat_at` → reclaim `RUNNING` (§6.1).         |

Per-table **schedule cadence** is **`next_due_at`** (from policy crontab). `minIntervalMs` is only the
min-gap gate before claim (§7.3). `pollIntervalSecs` = idle-worker sleep; `workerThreads` = max
concurrent submits; `candidateWindow` = SELECT size (≥ workers); `discoveryIntervalSecs` =
expansion frequency.

```properties
gravitino.server.rest.extensionPackages = org.apache.gravitino.maintenance.web.rest.feature
gravitino.auxService.names = iceberg-rest
gravitino.eventListener.names = tms-commit,tms-lifecycle
gravitino.eventListener.tms-commit.class = org.apache.gravitino.maintenance.IcebergCommitEventHandler
gravitino.eventListener.tms-lifecycle.class = org.apache.gravitino.maintenance.IcebergTableLifecycleHook
```

### 7.2 IRC hooks (`EventListenerPlugin`)

IRC’s built-in `Iceberg*HookDispatcher` layer is hardcoded (ownership / entity import). Custom TMS
logic plugs in through the **EventBus** path: implement `EventListenerPlugin` and register it like
any other listener
([Event listener configuration](../docs/gravitino-server-config.md#event-listener-configuration)).

| Key                                           | Default | Description                                                                                |
| --------------------------------------------- | ------- | ------------------------------------------------------------------------------------------ |
| `gravitino.eventListener.names`               | (empty) | Comma-separated names; include `tms-commit` / `tms-lifecycle` (or one combined name).      |
| `gravitino.eventListener.tms-commit.class`    | (none)  | FQCN of `EventListenerPlugin` for post-commit compaction (`IcebergCommitEventHandler`).    |
| `gravitino.eventListener.tms-lifecycle.class` | (none)  | FQCN of `EventListenerPlugin` for create/update/drop/rename (`IcebergTableLifecycleHook`). |

| Listener        | Handles (post-events)                                                                    | Effect                                                                                   |
| --------------- | ---------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `tms-commit`    | Iceberg table update / commit success events                                             | Async hand-off → compaction only (`mode()` = `ASYNC_ISOLATED` or `ASYNC_SHARED`, §5.4.1) |
| `tms-lifecycle` | `IcebergCreateTableEvent` / `IcebergUpdateTableEvent` / `IcebergDropTableEvent` / rename | UPSERT / refresh / `DELETE` `table_maintenance_state` (§5.2.5, §6.3)                     |

Omit TMS names from `gravitino.eventListener.names` → no TMS hooks; above-table scope still relies
on discovery (§5.3.2).

### 7.3 Task types and minimum interval (per policy type)

`minIntervalMs` is the **minimum gap between consecutive runs** of the same `(table, policy_id)`
(compared via `last_job_id` → `job_run_meta.job_finished_at`). It is **not** the scheduler poll /
`next_due_at` cadence. Null `last_job_id` → gate passes.

| Key                                                    | Default            | Description                                       |
| ------------------------------------------------------ | ------------------ | ------------------------------------------------- |
| `gravitino.maintenance.compaction.minIntervalMs`       | `3600000` (1 hour) | Min gap for `system_iceberg_compaction`.          |
| `gravitino.maintenance.snapshot-expiry.minIntervalMs`  | `3600000` (1 hour) | Min gap for `system_iceberg_snapshot_expiration`. |
| `gravitino.maintenance.manifest-rewrite.minIntervalMs` | `3600000` (1 hour) | Min gap for `system_iceberg_rewrite_manifests`.   |
| `gravitino.maintenance.orphan-cleanup.minIntervalMs`   | `3600000` (1 hour) | Min gap for `system_iceberg_orphan_file_removal`. |

**Resolution order:** table property override → global `gravitino.conf` key → code default.

---

## 8. Work Plan and Checklist

### 8.1 Suggested Work Plan

|           | 1                                                       | 2                                              | 3                                              | 4                                                  | 5                                 | 6–8                 |
| --------- | ------------------------------------------------------- | ---------------------------------------------- | ---------------------------------------------- | -------------------------------------------------- | --------------------------------- | ------------------- |
| Work item | In-process plugin + commit callback                     | State table + claim                            | `MaintenanceScheduler` + discovery             | Compaction on scheduler schedule                   | Track A / B                       | Profile API, harden |
| Notes     | Feature; IRC async `IcebergCommitEventHandler` (§5.4.1) | State materialization; nearest-wins (§5.2, §6) | `selectDueWork` + Iceberg/HMS discovery (§5.3) | Scheduler compaction; commit path unchanged (§5.4) | Hot pipeline + orphan (§5.5–§5.6) | §5.2.2              |

#### Phase 1–4 checklist

- [ ] Phase 1: IRC async `IcebergCommitEventHandler` (§5.4.1, §7.2).
- [ ] Phase 3: scheduler workers; discovery from Iceberg/HMS; immediate table attach; CAS
      `selectDueWork`; heartbeats; `next_due_at`; all four types; Track A; orphan
      oldest-first + interval gate; multi-node claim tests; commit-path
      heartbeat registration; discovery reconcile (§5.2–§5.6, §6.1).
- [ ] Phase 4: commit path does not write `next_due_at`; claim +
      `minIntervalMs` vs double-submit; commit ignores non-compaction (§5.4).

### 8.2 Review Checklist

|           | Deployment                                     | Policy                                                                          | Trigger                                                                           | Discovery                                                       | Multi-node                                                        | Commit callback                             | Durability                       | Orchestration                         | Industry                             |
| --------- | ---------------------------------------------- | ------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- | --------------------------------------------------------------- | ----------------------------------------------------------------- | ------------------------------------------- | -------------------------------- | ------------------------------------- | ------------------------------------ |
| Checklist | `extensionPackages`; IRC same JVM on **8090**. | Four types; nearest-wins; table attach immediate; above-table discovery (§5.2). | Compaction: commit + scheduler; others: scheduler + `next_due_at` (§5.4, §5.2.4). | Iceberg/HMS list; separate from `selectDueWork` (§5.3.2, §4.4). | No leader; per-row CAS like `IcebergCleanupManager` (§5.3, §4.3). | Async `IcebergCommitEventHandler` (§5.4.1). | State for claim/schedule (§6.2). | Track A; orphan separate (§5.5–§5.6). | §4.2 / §4.3 / §4.4 (row CAS chosen). |

---

## 9. References

1. [Gravitino Iceberg REST](../docs/iceberg-rest-service.md); [policies](../docs/manage-policies-in-gravitino.md); [compaction policy](../docs/iceberg-compaction-policy.md)
2. [Expire](./iceberg-expire-snapshots-maintenance-job.md) / [rewrite-manifests](./iceberg-rewrite-manifests-job.md) / [remove-orphan](./iceberg-remove-orphan-files-maintenance-job.md) design docs
3. [Optimizer overview](../docs/table-maintenance-service/optimizer.md)
4. [Amoro AIP-3](https://cwiki.apache.org/confluence/display/AMORO/AIP-3%3A+Event-Triggered+Optimization+of+Iceberg+Tables+in+Amoro); [Amoro configs](https://amoro.apache.org/docs/latest/configurations/)
5. [Floe policies](https://github.com/nssalian/floe/blob/main/docs/policies.md); [AWS Glue optimizers](https://docs.aws.amazon.com/glue/latest/dg/table-optimizers.html)
6. [Databricks auto compaction](https://docs.databricks.com/aws/en/tables/tune-file-size); [OpenHouse](https://github.com/linkedin/openhouse/blob/main/ARCHITECTURE.md)
7. Gravitino `IcebergCleanupManager` / `IcebergCleanupJobStore.takePendingJob`
