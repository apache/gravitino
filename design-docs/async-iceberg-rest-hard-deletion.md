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

# Design: Asynchronous Hard Deletion for the Gravitino Iceberg REST Server

| Field    | Value                                                   |
| -------- | ------------------------------------------------------- |
| Status   | Complete                                                |
| Authors  | @roryqi                                                 |
| Created  | 2026-05-19                                              |
| Module   | `iceberg/iceberg-rest-server`, `iceberg/iceberg-common` |

---

## 1. Background

When a client issues:

```
DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}?purgeRequested=true
```

today's path is fully synchronous: `IcebergTableOperations.dropTable` →
`IcebergTableOperationExecutor.dropTable` → `IcebergCatalogWrapper.purgeTable`
→ `CatalogHandlers.purgeTable`, which walks every snapshot / manifest /
data file and deletes each one through `FileIO` on the Jetty request
thread.

For production tables this fails in three ways:

- Multi-minute purges exceed HTTP timeouts.
- Concurrent purges saturate the Jetty pool.
- Mid-purge failures leak files with no retry or audit trail.

We want the drop to return quickly, finish file deletion reliably in the
background, survive restarts, and run safely across multiple server
replicas — while keeping the synchronous path available as a rollback.

*Not in scope:* `RelationalGarbageCollector`, which deletes tombstoned
**rows** from Gravitino's relational backend. Different IO surface,
different failure model — kept separate.

---

## 2. Goals

1. **Fast response**: `DELETE … ?purgeRequested=true` returns at typical
   request latency (target p99 < 500 ms, < 5 s even for the largest
   tables) regardless of table size.
2. **Operational simplicity**: Ship a single async deletion path; retain
   the synchronous path behind a flag purely for rollback.
3. **Reliable deletion**: The async path deletes every file the
   synchronous purge would have, retries transient failures, and survives
   restarts and replica failover.
4. **Wire compatibility**: No change to the Iceberg REST wire protocol.
5. **Request-thread authorization**: Authorization runs on the request
   thread, never deferred.

---

## 3. Non-Goals

1. **Native soft-delete / undrop semantics**: This design delivers async
   *hard* deletion only — once a drop is accepted the files are destroyed
   with no undrop or recovery path. Full soft-delete / undrop is a separate
   follow-up.
2. **Purge cancellation**: User-initiated cancellation of in-flight purges
   is out of scope; it needs a control API and lifecycle states we can add
   later without breaking this design.
3. **Third-party deletion plugins**: We ship one async implementation. A
   pluggable extension point is explicitly *not* built now — see §4.

---

## 4. Solution Investigations

An earlier draft proposed a pluggable `IcebergPurger` SPI. Review feedback
pushed back: no second implementation is in flight, and the goal is a
simpler design with a smaller bug surface. We chose a single async
implementation with a synchronous rollback flag.

| Approach                                                    | Pros                                                                                     | Cons                                                                                             | Decision                                                                |
|-------------------------------------------------------------|------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------|
| Synchronous only (status quo)                               | Simplest; strongest "deleted means gone" guarantee                                       | Exceeds HTTP timeouts, saturates Jetty, no retry/audit on large tables                           | **Rejected** — the problem we are solving                               |
| Pluggable `IcebergPurger` SPI                               | Extensible without code changes                                                          | Real added surface (SPI, discovery factory, context) with **no** second implementation in flight | **Rejected** — revisit when a second implementation has a real customer |
| Reuse `RelationalGarbageCollector`                          | Proven worker/scheduling pattern                                                         | Different IO surface (object store vs. JDBC) and failure model                                   | **Rejected** — share patterns, not code                                 |
| External job system (Quartz / Temporal)                     | Mature scheduling, retries, observability                                                | Heavy operational burden on every operator                                                       | **Rejected** — disproportionate for one workload                        |
| Enumerate files at enqueue time                             | Worker needs no metadata re-read                                                         | Slow on large tables (defeats the latency goal), bloats job rows                                 | **Rejected** — store `metadata_location`, re-read at run time           |
| Object-store job markers (no DB table)                      | No schema / migration                                                                    | Hand-rolled lease + renewal, no indexed scheduling, fragments per-bucket                         | **Rejected** — higher net complexity (see below)                        |
| **JDBC job table + worker pool, synchronous fallback flag** | Smallest bug surface; restart-safe and cluster-safe via CAS claim; one code path to test | No built-in extension point                                                                      | **Chosen**                                                              |

**Why not an S3-only control plane?** Conditional writes (`If-None-Match`)
let workers claim jobs without a relational backend, but it *raises* net
complexity for Gravitino: a DB gives lease acquisition and an indexed
candidate scan for free, whereas S3 forces hand-rolled leases discovered by
slow, per-request `LIST`. One server fronts many
catalogs across S3 / GCS / ADLS, so a single job table is a uniform control
plane; object markers fragment per-bucket. Gravitino already runs a
relational metastore, so one extra table is low marginal cost — and it
doubles as the name-reuse tombstone (§5.7).

---

## 5. Proposal

### 5.1 Overview

```
  DELETE …?purgeRequested=true
            │
            ▼
  IcebergTableOperationExecutor.dropTable
            │  X-Gravitino-Async-Purge header (absent ⇒ async)
      ┌─────┴───────────────┐
      │ header absent       │ X-Gravitino-Async-Purge: false
      ▼                     ▼
 persist iceberg_purge_job   CatalogUtil.dropTableData
 row, return 204             (synchronous, on request thread)
      │
      ▼
 worker pool (any replica)
   claims job via CAS UPDATE (heartbeat ownership)
   → rebuild snapshot graph from metadata_location
   → stream + delete every reachable file, retry on later poll ticks
   → SUCCEEDED | FAILED
```

The header is optional and has no default value: when it is **absent** —
which is the case for any standard Iceberg client — the drop is async. A
client opts into the synchronous fallback by sending
`X-Gravitino-Async-Purge: false` (the header name uses canonical HTTP casing,
each word capitalized).

### 5.2 User flow

1. A client issues
   `DELETE /v1/{prefix}/namespaces/{ns}/tables/{t}?purgeRequested=true`.
   No client change is needed for async; strict synchronous deletion is
   selected by adding `X-Gravitino-Async-Purge: false`.
2. The server runs authorization on the request thread, loads the table
   metadata location, drops the catalog entry, and persists an
   `iceberg_purge_job` row.
3. The server responds `204 No Content` within typical request latency. The
   table is immediately absent from `LIST` and `HEAD` returns `404`.
4. A worker on any replica claims the job, rebuilds the snapshot graph from
   `metadata_location`, and streams through every reachable file deleting
   it, retrying transient failures on later poll ticks.
5. On success the job is `SUCCEEDED`; on terminal failure it lands in
   `FAILED` for operator inspection (queryable in the job table, §5.9).

### 5.3 Request-path interaction

```java
public void dropTable(IcebergRequestContext ctx, TableIdentifier id,
                      boolean purgeRequested) {
  IcebergCatalogWrapper w = catalogWrapperManager.getCatalogWrapper(ctx.catalogName());
  if (!purgeRequested) { w.dropTable(id); return; }

  if (!asyncPurgeEnabled) {                 // fallback path
    w.purgeTable(id);                       // synchronous, today's behavior
    return;
  }

  TableMetadata metadata = w.loadTableMetadata(id);
  w.dropTable(id);                          // metadata-only drop in the catalog
  purgeJobStore.enqueue(
      IcebergPurgeJob.builder()
          .catalogName(ctx.catalogName())
          .tableIdentifier(id)
          .metadataLocation(metadata.metadataFileLocation())
          .fileIoImpl(w.fileIoImpl())
          .fileIoProperties(w.fileIoProperties())
          .createdBy(ctx.userPrincipal())
          .build());
}
```

Order matters on the async path: load metadata location → drop catalog
entry → enqueue the job. A purge job exists only for a table already gone
from the catalog. `fileIoProperties` is captured at enqueue time so the
worker can reconstruct `FileIO` even if the catalog is later reconfigured.
The enqueued row also serves as the **tombstone** that blocks name reuse
while the files still exist (§5.7).

### 5.4 Schema — `iceberg_purge_job`

```sql
CREATE TABLE IF NOT EXISTS `iceberg_purge_job` (
  `id`                BIGINT(20)    UNSIGNED NOT NULL AUTO_INCREMENT,
  `metalake_name`     VARCHAR(128)  NOT NULL,
  `catalog_name`      VARCHAR(128)  NOT NULL,
  `namespace`         VARCHAR(512)  NOT NULL,
  `table_name`        VARCHAR(256)  NOT NULL,
  `metadata_location` VARCHAR(1024) NOT NULL,
  `file_io_impl`      VARCHAR(256)  NOT NULL,
  `file_io_props`     MEDIUMTEXT    NOT NULL COMMENT 'JSON',
  `state`             VARCHAR(16)   NOT NULL COMMENT 'PENDING|RUNNING|SUCCEEDED|FAILED',
  `attempts`          INT(10)       NOT NULL DEFAULT 0,
  `heartbeat_at`      BIGINT(20)    NULL COMMENT 'last heartbeat from the worker; NULL when unclaimed',
  `created_by`        VARCHAR(128)  NOT NULL COMMENT 'principal that requested the drop (audit)',
  `updated_at`        BIGINT(20)    NOT NULL COMMENT 'last state change; drives poll ordering and terminal-row pruning',
  PRIMARY KEY (`id`),
  KEY `idx_state_updated` (`state`, `updated_at`),
  KEY `idx_object` (`catalog_name`, `namespace`, `table_name`, `state`)
) ENGINE=InnoDB;
```

We store only `metadata_location`, not the file list — enumeration is slow
on large tables, and `TableMetadataParser.read(io, location)` rebuilds the
snapshot graph deterministically when the worker runs.

A single column drives retries: `attempts` counts failures so the worker
gives up at the retry ceiling. A failed job returns to `PENDING` and is
re-claimed on a later poll tick, so the poll cadence (`poll-interval-ms`,
§5.10) is the retry interval — no separate scheduling column is needed. The
ceiling is a config (`max-attempts`, §5.10), not a per-row `max_attempts`
column, since it is the same for every job.

Migration: `upgrade-1.2.0-to-1.3.0-mysql.sql` (and H2 / PostgreSQL).

### 5.5 Worker pool and multi-replica ownership

A `ScheduledThreadPoolExecutor` modeled on `RelationalGarbageCollector`.
Ownership is tracked by a **heartbeat** alone: the worker that claims a job
keeps refreshing `heartbeat_at` while it runs and remembers the ids it owns
in memory (no owner column — a stale row is reclaimable by anyone, which is
exactly the failover behavior we want). A job is abandoned, and reclaimable,
once its heartbeat is older than `heartbeatTimeout`.

**Claiming is gated by free worker capacity.** Each tick the worker first
counts its idle threads and polls for *at most* that many candidates; when
every thread is busy it polls for nothing. A worker therefore never claims a
job it cannot run immediately, so a `RUNNING` row always maps to a thread
actively deleting files — its heartbeat reflects real progress, never a job
idling in an in-memory backlog. This also means the claim count is the idle
thread count, not a separate tunable.

Claiming is an optimistic compare-and-swap `UPDATE` that works on **every**
SQL backend:

```sql
-- read up to :idleThreads candidates (0 when the pool is saturated)
SELECT id FROM iceberg_purge_job
 WHERE state = 'PENDING'
    OR (state = 'RUNNING'
        AND (heartbeat_at IS NULL OR heartbeat_at < :now - :heartbeatTimeout))
 ORDER BY updated_at LIMIT :idleThreads;

-- claim each candidate; the row is ours only if affected_rows = 1
UPDATE iceberg_purge_job
   SET state='RUNNING', heartbeat_at=:now
 WHERE id=:id
   AND ( state='PENDING'
      OR (state='RUNNING'
          AND (heartbeat_at IS NULL OR heartbeat_at < :now - :heartbeatTimeout)) );
```

A loser sees `affected_rows = 0` and moves to the next candidate (so a tick
may end up running fewer jobs than it has idle threads). Where `SELECT …
FOR UPDATE SKIP LOCKED` is available (MySQL 8+, PostgreSQL) the worker uses
it to cut contention up front; the CAS form is the portable fallback (H2,
older MySQL). `SKIP LOCKED` is purely an optimization — the claiming `UPDATE` is
the serialization point, so two replicas can never claim the same job. We do
not use DB-specific advisory locks.

Execution mirrors `CatalogHandlers.purgeTable`, but **streams** the
reachable files instead of materializing them. A large table can reference
millions of data files, so the worker walks the snapshot graph lazily and
deletes in bounded batches:

```java
TableMetadata meta = TableMetadataParser.read(io, job.metadataLocation());
try (CloseableIterable<String> files = reachableFiles(meta)) {  // lazy, not materialized
  Tasks.foreach(files)
       .executeWith(deleteExecutor)  // shared server-wide pool, §5.10
       .retry(perFileRetries)
       .suppressFailureWhenFinished()
       .run(io::deleteFile);
}
```

`deleteExecutor` is a single server-wide pool sized by `delete-threads`
(§5.10), shared by all concurrent jobs rather than allocated per job. This
bounds total file-delete concurrency on the server regardless of how many
jobs run at once; raise it to drain large tables faster when purge
throughput matters.

A separate task sends a heartbeat every `heartbeatTimeout / 3` on the ids
this worker owns. If the host dies the heartbeat stops; once a row ages past
`heartbeatTimeout` another replica reclaims it.

### 5.6 Failure model, restart, and crash recovery

Per-file failures are logged but do not fail the whole job — the
synchronous purge has the same "best effort" stance. A job fails only if the
**metadata phase** fails. `NotFoundException` from `deleteFile` counts as
success. A transient failure goes back to `PENDING` and is retried on a
later poll tick (§5.5), incrementing `attempts` each time; when `attempts`
reaches `max-attempts` the job goes to `FAILED` instead of `PENDING`. A
clearly terminal failure skips the retries and goes straight to `FAILED`.

| Outcome                                              | Action                                                                                |
|------------------------------------------------------|---------------------------------------------------------------------------------------|
| All files deleted (or already gone)                  | `state='SUCCEEDED'`                                                                    |
| Transient failure, `attempts < max-attempts`         | `attempts++`, back to `PENDING`, `heartbeat_at=NULL`; re-claimed on a later poll tick  |
| Transient failure, `attempts` reaches `max-attempts` | `attempts++` → `FAILED` (gave up retrying)                                             |
| Terminal failure (e.g. metadata gone/corrupt)        | → `FAILED` immediately; retrying cannot help                                           |
| Worker killed mid-job                                | `RUNNING` row's heartbeat goes stale; another worker reclaims; deletes are idempotent  |

Restart handling falls out of the durable job table plus the heartbeat
model — no separate mechanism:

1. **Nothing in-flight is lost.** The job is committed before the `DELETE`
   returns; there is no in-memory queue. `PENDING` jobs are picked up on the
   first tick after boot.
2. **Orphaned in-flight jobs are reclaimed.** A crash leaves a `RUNNING` row
   with a stale heartbeat, which the poll already selects; the restarted node
   or any peer reclaims it via the CAS.
3. **Re-running a half-finished job is safe.** The worker rebuilds the file
   set and re-deletes; already-gone files raise `NotFoundException`, counted
   as success. A crash costs only duplicated work, never corruption.

In multi-replica deployments this is seamless. The one rough edge is a
single-replica restart: the crashed node's jobs sit idle until their
heartbeat ages past `heartbeatTimeout` before the rebooted process reclaims
them. We keep `heartbeatTimeout` modest so the resume delay is small; with no
owner column there is nothing extra to sweep on boot.

### 5.7 Name reuse during purge

While a purge job for an identifier is non-terminal (`PENDING` or
`RUNNING`), that identifier is treated as **still occupied** even though its
catalog entry is already gone. The active `iceberg_purge_job` row *is* the
tombstone — no second table is needed. The REST server consults the store on
the request thread (one indexed lookup via `idx_object`):

| Operation                                   | Active job exists                 | No active job (`SUCCEEDED`/`FAILED`/none) |
|---------------------------------------------|-----------------------------------|-------------------------------------------|
| `loadTable` / `HEAD`, `alterTable` / commit | `404 NoSuchTableException`        | `404`                                     |
| `createTable` (same identifier)             | **`409 Conflict`** — being purged | succeeds                                  |
| `registerTable` (same identifier)           | **`409 Conflict`**                | succeeds                                  |

A `SUCCEEDED` job (even before pruning) no longer blocks reuse — the files
are gone. A `FAILED` job also stops blocking, so a failed cleanup never
permanently wedges the namespace; operators see the failed row and reclaim
leaked files out of band. This closes the same-name / same-location recreate
attack: a recreate cannot expose the dropped table's data, because it is
refused until the files are deleted. A **namespace** carries no data, so it
can be recreated freely; tables inside it are guarded individually.

Once a drop is accepted the table is gone for good: this is *hard* delete,
so there is deliberately no undrop or re-register recovery path. A client
that wants a grace period before data is destroyed should use a future
soft-delete feature (a non-goal here, §3), not this path.

**Concurrency.** The create/register conflict check and the worker's claim
both serialize on the row's `state`. A `createTable` / `registerTable` that
observes a non-terminal job (`PENDING` or `RUNNING`) returns `409`; one that
observes a terminal job (`SUCCEEDED` / `FAILED`) or no job proceeds. The
worker's claim CAS moves `PENDING → RUNNING`, so the conflict check and the
claim never disagree about whether the identifier is still occupied.

### 5.8 Object coverage: tables now, views later

The initial scope is **tables only**.

- **Namespace (schema) drops** are *not* a cascade: the Iceberg REST spec
  requires a namespace to be empty before it can be dropped, so the client
  drops each table first. Every such `DELETE …?purgeRequested=true` enqueues
  its own purge job, and the namespace drop itself touches no data files.
  There is no namespace-level purge job.
- **Views** are a planned follow-up. A view carries no data files — cleanup
  only removes its `metadata.json` — so it slots into the same job table
  (adding an `object_type` column) with the same tombstone rule when added.

### 5.9 Events and observability

Existing `IcebergDropTableEvent` continues to fire on the REST thread with
the *requested* purge flag, preserving today's listener contract.

Dedicated purge events (`IcebergPurgeStartedEvent`,
`IcebergPurgeCompletedEvent`, `IcebergPurgeFailedEvent` — work begins, files
deleted, retries exhausted) are **deferred to Phase 2**, not part of 1.3. In
1.3, observability comes entirely from the durable job row plus metrics,
which ship for free with the worker; streaming events are an additive
follow-up that does not change the job model.

The source of truth for "is this table still being purged, done, or failed?"
is the `iceberg_purge_job` row, exposed without touching the wire contract:

- **Iceberg REST clients** observe only that the table is dropped
  (`404` / omitted from `LIST`). The single purge-specific signal is the
  §5.7 `409` on a same-identifier `createTable` / `register`, whose
  `ErrorResponse.message` names the blocking job.
- **Operators** query `iceberg_purge_job` directly (the `idx_object` index
  makes the per-identifier lookup cheap), plus the metrics below; the failure
  reason for a `FAILED` job is in the worker's logs. This is enough to answer
  "still purging / failed?" without new HTTP surface. A read-only management
  endpoint is deferred to Phase 2.
- **Metrics**: gauges `purge.jobs.{pending,running,failed}` and
  `purge.oldest_pending_age_ms`; counters `purge.{completed,failed}`. These
  let operators alert on a growing backlog or any `FAILED` job — the two
  conditions that would otherwise go unnoticed.

`FAILED` is the operationally critical state: in 1.3 it is queryable (DB)
and counted (metric), so a cleanup that exhausts retries is always
discoverable rather than a silent file leak — even before the Phase 2 events
land.

### 5.10 Configuration

Whether a given drop runs asynchronously is **not** a server config — it is a
per-request **client** choice via the `X-Gravitino-Async-Purge` header
(absent ⇒ async; `false` selects synchronous deletion). The server-side keys
only tune the worker pool and retries:

| Key                                                           | Default  | Description                                                                  |
|---------------------------------------------------------------|----------|------------------------------------------------------------------------------|
| `gravitino.iceberg-rest.async-purge.worker-threads`           | `4`      | Worker pool size per server (concurrent jobs).                               |
| `gravitino.iceberg-rest.async-purge.delete-threads`           | `16`     | Server-wide file-delete pool size, shared across all jobs.                   |
| `gravitino.iceberg-rest.async-purge.poll-interval-ms`         | `5000`   | Worker poll interval.                                                        |
| `gravitino.iceberg-rest.async-purge.heartbeat-timeout-ms`     | `300000` | Age after which a job with no heartbeat is reclaimable.                      |
| `gravitino.iceberg-rest.async-purge.max-attempts`             | `5`      | Attempts before `FAILED`.                                                    |
| `gravitino.iceberg-rest.async-purge.terminal-retention-hours` | `168`    | How long terminal (`SUCCEEDED` / `FAILED`) rows are retained before pruning. |

There is no separate claim-batch-size key: a tick claims at most as many
jobs as the worker has idle threads (§5.5), so `worker-threads` already
bounds it.

### 5.11 Security

- `@AuthorizationExpression` on `dropTable` runs on the request thread,
  unchanged.
- The worker uses FileIO credentials snapshotted at enqueue time.
  Credentials that expire before the worker runs (STS tokens, …) are
  refreshed from the catalog on each heartbeat, so long-running purges keep
  valid credentials.
- `iceberg_purge_job` may contain credentials in `file_io_props`; the
  existing Gravitino DB encryption / access controls apply.

### 5.12 Backward compatibility (wire)

The Iceberg REST spec does not require file deletion to be complete before
the response. With this design:

- The catalog entry is removed before `204`, so `HEAD` returns `404` and
  `LIST` no longer includes the table. `CREATE` / `register` at the same
  identifier is rejected with `409 Conflict` until the purge job completes
  (§5.7).
- Object-store files may linger until the worker drains. Documented in
  release notes.

The async-vs-synchronous choice rides on the optional `X-Gravitino-Async-Purge`
header — a Gravitino extension, not part of the Iceberg REST spec. A standard
client never sends it and gets the async default, so the protocol is
unchanged.

---

## 6. Task Breakdown

Ordered so dependencies come first. Each item maps to one GitHub issue/PR.
Async purge ships **as the default in 1.3**; the synchronous path remains
only as a rollback flag.

### Phase 1 (1.3): Async purge core
- [ ] Add the `iceberg_purge_job` schema and migrations (MySQL, H2, PostgreSQL)
- [ ] Implement `IcebergPurgeJobStore` enqueue path (persist job, return)
- [ ] Implement the worker pool: CAS claiming with heartbeat ownership (`FOR UPDATE SKIP LOCKED` where available), heartbeat renewal, streaming file deletion, retry state machine (re-claim failed jobs on later poll ticks, give up at `max-attempts`)
- [ ] Honor the `X-Gravitino-Async-Purge` request header and wire both paths into `IcebergTableOperationExecutor.dropTable`
- [ ] Add observability (§5.9): metrics and the informative `ErrorResponse` message on the §5.7 `409`; operator read path in 1.3 is direct DB query
- [ ] Enforce tombstone semantics (§5.7): on `createTable`/`registerTable`, reject with `409` when an active job exists (`idx_object` lookup on the request thread)

### Phase 1 (1.3): Testing
- [ ] Unit tests: enqueue, state machine, worker claiming/heartbeat/contention (H2)
- [ ] Docker integration tests: latency, restart-resume (`kill -9` mid-purge), two-replica no-duplicate-deletes
- [ ] Scale & concurrency tests (§7)

### Phase 1 (1.3): Documentation
- [ ] Update user-facing docs in `docs/` (async semantics, the `X-Gravitino-Async-Purge` header, config, release notes)

### Phase 2 (post-1.3)
- [ ] Add purge events (§5.9): `IcebergPurgeStartedEvent`, `IcebergPurgeCompletedEvent`, `IcebergPurgeFailedEvent`
- [ ] View support (§5.8): `object_type='VIEW'` cleanup (metadata-only) and the view tombstone rule
- [ ] Read-only `metalakes/{metalake}/catalogs/{catalog}/cleanups` management endpoint (§5.9): REST resource, DTOs, authorization, client support, docs, tests. Filtered list keyed by object identifier; no by-id fetch

---

## 7. Testing

- Unit (`./gradlew :iceberg:iceberg-rest-server:test -PskipITs`):
  - `TestIcebergPurgeJobStore` — enqueue and row contents.
  - `TestIcebergPurgeStateMachine` — PENDING → RUNNING → SUCCEEDED;
    failure → retry (back to PENDING) → FAILED.
  - `TestIcebergPurgeWorker` — claiming, heartbeat renewal, contention (H2).
  - `TestIcebergTableOperationExecutorAsyncPurge` — async default enqueues
    exactly one job; `X-Gravitino-Async-Purge: false` falls back to
    synchronous purge; thrown errors surface as 5xx.
  - `TestIcebergPurgeTombstone` (§5.7) — `createTable`/`register` at an
    identifier with an active job returns `409`, succeeds once
    `SUCCEEDED`/`FAILED`; `loadTable`/`alterTable` return `404`.
- Integration (`gravitino-docker-test`):
  - Drop a table; REST returns < 500 ms; worker eventually clears every file.
  - Restart REST mid-purge (`kill -9`); purge resumes.
  - Two replicas on one DB; no duplicate deletions or orphans.
  - Synchronous fallback: existing `IcebergRESTServiceIT` reruns unchanged
    with `X-Gravitino-Async-Purge: false`.
- Scale & concurrency:
  - High-file-count fixtures: medium (~50K files), large (~500K files).
  - Concurrent-drop tier; drop returns within 5 s regardless of table size.
  - Direct GCS/S3 listing asserts the storage prefix is empty after cleanup.
