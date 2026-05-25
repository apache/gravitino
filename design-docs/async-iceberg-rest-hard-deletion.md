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
| Status   | Draft                                                   |
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
   request latency (target p99 < 500 ms, and < 5 s even for the largest
   tables) regardless of table size.
2. **Operational simplicity**: Ship a single async deletion path with the
   smallest possible bug surface; retain the synchronous path behind a
   feature flag purely for rollback, not as a parallel product surface.
3. **Reliable deletion**: The async path deletes every file the
   synchronous purge would have deleted, retries transient failures, and
   survives restarts and replica failover.
4. **Wire compatibility**: No change to the Iceberg REST wire protocol.
5. **Request-thread authorization**: Authorization runs on the request
   thread, never deferred.
6. **Uniform object coverage**: Tables, views, and namespace (schema)
   drops all flow through the same async cleanup mechanism (PRD §2.2).

---

## 3. Non-Goals

1. **Native soft-delete semantics (R2)**: This design only delivers async
   *hard* deletion (R1). Full soft-delete / undrop semantics are a
   follow-up requirement; §5.7 records the seam they build on so R2 stays
   a small extension rather than a separate V2 mechanism.
2. **Purge cancellation (v1)**: User-initiated cancellation of in-flight
   purges is out of scope for v1; it needs a control API and lifecycle
   states we can add later without breaking the design.
3. **Third-party deletion plugins**: We ship one async implementation. A
   pluggable extension point is explicitly *not* built now — see §4 for
   why, and the conditions under which we would revisit it.

---

## 4. Solution Investigations

The earlier draft proposed a pluggable `IcebergPurger` SPI. Review
feedback (PR #11152) pushed back: the PRD does not ask for an SPI, no
second implementation is in flight, and the competitive frame against
Polaris is "simpler design with smaller bug surface." We re-evaluated and
chose a single async implementation with a synchronous rollback flag.

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| Synchronous only (status quo) | Simplest; strongest "deleted means gone" guarantee | Exceeds HTTP timeouts, saturates Jetty, no retry/audit on large tables | **Rejected** — the problem we are solving |
| Pluggable `IcebergPurger` SPI (factory + classpath loading + context) | Extensible to object-store batch / audit-only without code changes | Real added surface (SPI, discovery factory, context) with **no** second implementation in flight; widens the bug surface against the PRD's "smaller surface" goal | **Rejected** — revisit only when a second implementation has a real customer behind it |
| Reuse `RelationalGarbageCollector` | Proven worker/scheduling pattern already in the codebase | Different IO surface (object store vs. JDBC) and failure model (best-effort per-file vs. transactional rows) | **Rejected** — share patterns, not code |
| External job system only (Quartz / Temporal) | Mature scheduling, retries, observability | Heavy operational burden imposed on every operator | **Rejected** — disproportionate for one deletion workload |
| Enumerate files at enqueue time | Worker needs no metadata re-read | Enumeration is slow on large tables (defeats the latency goal) and bloats job rows | **Rejected** — store `metadata_location`, re-read at run time |
| Object-store job markers (no DB table) — write a purge-intent object to S3, workers `LIST` and claim via conditional write (`If-None-Match`) | No schema / migration; co-located with the data | See below | **Rejected** — higher net complexity for Gravitino |
| **Single async purger (JDBC job table + worker pool) with a synchronous fallback flag** | Smallest bug surface; reliable, restart-safe, cluster-safe via `SKIP LOCKED` (CAS fallback elsewhere); one code path to test | No built-in extension point — a second strategy would need a follow-up refactor | **Chosen** |

#### Why not an S3-only control plane (no DB table)?

Technically feasible — S3 has supported conditional writes (`If-None-Match` /
`If-Match`) since 2024, so workers could compare-and-swap a lease object to
claim a job without a relational backend. We rejected it because it *raises*
net complexity for Gravitino rather than lowering it:

- **Coordination is harder, not easier.** A DB gives `SELECT … FOR UPDATE SKIP
  LOCKED` (or the CAS fallback in §5.4) for free. S3 forces us to hand-roll
  lease acquisition *and* lease renewal on top of conditional writes and
  object metadata.
- **No scheduling primitive.** Backoff / `next_attempt_at` is one indexed
  `WHERE` clause in SQL. With markers there is no index — state and timestamps
  must be encoded into object keys or metadata and discovered by `LIST`, which
  is slow and billed per request as the job count grows.
- **Fragmented across storage backends.** One server serves many catalogs
  across S3 / GCS / ADLS. A single job table is a uniform control plane;
  object markers either fragment per-bucket (no cross-cloud view) or require a
  dedicated control bucket, adding a new dependency and credential surface.
- **Dead-letter and audit.** Operators can query a DB table directly; markers
  need separate tooling.
- **The DB is already there.** Gravitino runs a relational metastore, so its
  connection pool, migration framework, and transaction management are
  existing infrastructure — one extra table is low marginal cost. S3-only
  would *remove* a uniform, strongly-consistent coordination primitive and
  replace it with an eventually-consistent, per-bucket one we maintain
  ourselves. It would only pay off in a deployment with no relational backend,
  which is not Gravitino's case. If the goal is fewer moving parts, the lever is
  to keep the single job table doing double duty — it already serves as the
  name-reuse tombstone (§5.13) — not a move to object-store markers.

---

## 5. Proposal

### 5.1 Overview

```
  DELETE …?purgeRequested=true
            │
            ▼
  IcebergTableOperationExecutor.dropTable
            │  async-purge.enabled ?
      ┌─────┴───────────────┐
      │ true (default 1.3)  │ false (rollback)
      ▼                     ▼
 persist iceberg_purge_job   CatalogUtil.dropTableData
 row, return 204             (synchronous, on request thread)
      │
      ▼
 worker pool (any replica)
   leases job via FOR UPDATE SKIP LOCKED
   → rebuild snapshot graph from metadata_location
   → delete every reachable file, retry w/ backoff
   → SUCCEEDED | DEAD_LETTER
```

There is one async deletion path. The synchronous path is retained only
as a feature-flag rollback (`async-purge.enabled = false`).

### 5.2 Request-path interaction

```java
public void dropTable(IcebergRequestContext ctx, TableIdentifier id,
                      boolean purgeRequested) {
  IcebergCatalogWrapper w = catalogWrapperManager.getCatalogWrapper(ctx.catalogName());
  if (!purgeRequested) { w.dropTable(id); return; }

  if (!asyncPurgeEnabled) {                 // rollback path
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
entry → enqueue the job. A purge job exists only for a table that is
already gone from the catalog. `fileIoProperties` is captured at enqueue
time so the worker can reconstruct `FileIO` even if the catalog is later
reconfigured.

The enqueued job row also serves as the **tombstone** that blocks name
reuse while the files still exist — see §5.13.

### 5.3 Schema — `iceberg_purge_job`

```sql
CREATE TABLE IF NOT EXISTS `iceberg_purge_job` (
  `id`                BIGINT(20)    UNSIGNED NOT NULL AUTO_INCREMENT,
  `metalake_name`     VARCHAR(128)  NOT NULL,
  `catalog_name`      VARCHAR(128)  NOT NULL,
  `namespace`         VARCHAR(512)  NOT NULL,
  `object_name`       VARCHAR(256)  NOT NULL,
  `object_type`       VARCHAR(16)   NOT NULL COMMENT 'TABLE|VIEW',
  `metadata_location` VARCHAR(1024) NOT NULL,
  `file_io_impl`      VARCHAR(256)  NOT NULL,
  `file_io_props`     MEDIUMTEXT    NOT NULL COMMENT 'JSON',
  `state`             VARCHAR(16)   NOT NULL COMMENT 'PENDING|RUNNING|SUCCEEDED|DEAD_LETTER|CANCELLED',
  `attempts`          INT(10)       NOT NULL DEFAULT 0,
  `max_attempts`      INT(10)       NOT NULL,
  `last_error`        TEXT          NULL,
  `lease_owner`       VARCHAR(128)  NULL,
  `lease_expires_at`  BIGINT(20)    NULL,
  `next_attempt_at`   BIGINT(20)    NOT NULL,
  `created_at`        BIGINT(20)    NOT NULL,
  `created_by`        VARCHAR(128)  NOT NULL,
  `updated_at`        BIGINT(20)    NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_state_next_attempt` (`state`, `next_attempt_at`),
  KEY `idx_object` (`catalog_name`, `namespace`, `object_name`, `state`)
) ENGINE=InnoDB;
```

We store only `metadata_location`, not the file list — enumeration is
slow on large tables, and `TableMetadataParser.read(io, location)`
rebuilds the snapshot graph deterministically when the worker runs.

Migration: `upgrade-1.2.0-to-1.3.0-mysql.sql` (and H2 / PostgreSQL).

### 5.4 Worker pool

A `ScheduledThreadPoolExecutor` modeled on `RelationalGarbageCollector`.
Each tick:

```sql
SELECT * FROM iceberg_purge_job
 WHERE state IN ('PENDING','RUNNING')
   AND next_attempt_at <= :now
   AND (lease_expires_at IS NULL OR lease_expires_at < :now)
 ORDER BY next_attempt_at LIMIT :batch
 FOR UPDATE SKIP LOCKED;
```

then updates the row to `RUNNING` with `lease_owner=:me`,
`lease_expires_at=:now+leaseTimeout`. `SKIP LOCKED` is a performance
optimization (it avoids lock waiting between replicas), not the correctness
guarantee — see the leasing model below.

#### Multi-replica leasing model

Three distinct races, handled deliberately:

1. **Two replicas claim the same job.** The claiming `UPDATE` is the
   serialization point, so this cannot happen — see the CAS rule below. Any
   number of replicas can run the worker with no external coordinator.
2. **A held lease expires while its owner is still alive** (long GC, network
   partition). The lease expires, another replica reclaims, and both may
   delete the same files concurrently. We *accept* this race rather than
   eliminate it: deletes are idempotent and `NotFoundException` counts as
   success (§5.5), so the only cost is duplicated work, never incorrect
   results. This is what lets failover stay coordinator-free.
3. **A replica dies mid-job.** Identical to case 2 from the survivors' point
   of view — the lease expires and the job is reclaimed.

To stay portable, claiming is expressed as an optimistic compare-and-swap that
works on **every** SQL backend, not just those with `SKIP LOCKED`:

```sql
-- read candidates (no lock required)
SELECT id FROM iceberg_purge_job
 WHERE state IN ('PENDING','RUNNING')
   AND next_attempt_at <= :now
   AND (lease_expires_at IS NULL OR lease_expires_at < :now)
 ORDER BY next_attempt_at LIMIT :batch;

-- claim each candidate; the row is ours only if affected_rows = 1
UPDATE iceberg_purge_job
   SET state='RUNNING', lease_owner=:me, lease_expires_at=:now+:leaseTimeout
 WHERE id=:id
   AND (lease_expires_at IS NULL OR lease_expires_at < :now);
```

A loser sees `affected_rows = 0` and moves to the next candidate. Where
`SKIP LOCKED` is available (MySQL 8+, PostgreSQL), the worker uses the
`FOR UPDATE SKIP LOCKED` form shown above to skip locked rows up front and
cut contention; where it is not (H2, older MySQL), it falls back to the CAS
form. Both are correct; `SKIP LOCKED` is purely an optimization. We do not use
DB-specific advisory locks, whose semantics differ across engines.

Execution mirrors `CatalogHandlers.purgeTable`:

```java
TableMetadata meta = TableMetadataParser.read(io, job.metadataLocation());
Tasks.foreach(collectAllReachableFiles(meta))
     .executeWith(deleteExecutor)
     .retry(perFileRetries)
     .suppressFailureWhenFinished()
     .run(io::deleteFile);
```

A separate task renews the lease every `leaseTimeout / 3`. If the host
dies, the lease expires and another replica reclaims the job.

### 5.5 Failure model

Per-file failures are logged but do not fail the whole job — the
synchronous purge has the same "best effort" stance. A job fails only if
the **metadata phase** fails.

| Outcome                            | Action                                                              |
| ---------------------------------- | ------------------------------------------------------------------- |
| All files deleted (or already gone) | `state='SUCCEEDED'`                                                |
| Metadata load failed, transient    | `attempts++`, `next_attempt_at = now + backoff(attempts)`           |
| Metadata load failed, terminal     | `attempts++`; if `attempts >= max_attempts` → `state='DEAD_LETTER'` |
| Worker killed mid-job              | Lease expires; another worker picks it up; deletes are idempotent (restart story in §5.15) |
| Recovered via re-register (§5.13)  | Job CAS'd to `state='CANCELLED'`; the worker only leases non-terminal rows, so files are left intact |

`NotFoundException` from `deleteFile` counts as success. Backoff:
`min(maxBackoff, base * 2^attempts)` with jitter.

### 5.6 Views and namespace (schema) drops

The same async mechanism covers all object types (PRD §2.2):

- **Views** carry no data files — the worker cleanup just removes the
  view's `metadata.json`. The job row uses `object_type = 'VIEW'`.
- **Namespace (schema) drops** cascade: the server enqueues an
  independent purge job for each contained table and view. Failures and
  retries are tracked per object, so a re-run never re-deletes an
  already-cleaned object, and one object's failure does not block the
  others.

### 5.7 Recovery and the soft-delete (R2) seam

Because the job stores `metadata_location` and the underlying data /
metadata files are not touched until the worker runs, a table dropped by
mistake can be recovered *before its files are deleted* by re-registering
it at the stored location:

```
POST /v1/{prefix}/namespaces/{ns}/register
{ "name": "<table>", "metadata-location": "<stored metadata_location>" }
```

Once the worker deletes the files the table is unrecoverable — which is
the intended semantics of *hard* delete. Soft delete (R2) layers on top
by deferring file deletion for a retention window (delaying
`next_attempt_at`) on the **same** job row, executor, and scheduler, so
register-table recovery succeeds throughout the window. This keeps R2 a
small extension on R1 rather than a separate V2 mechanism.

### 5.8 Events

Existing `IcebergDropTableEvent` continues to fire on the REST thread
with the *requested* purge flag, preserving today's listener contract.
The async purger additionally emits, via the event listener manager:

- `IcebergPurgeStartedEvent` — work begins on a job.
- `IcebergPurgeCompletedEvent` — files deleted, with elapsed time.
- `IcebergPurgeFailedEvent` — dead-lettered job.

These events are one observability surface; the metrics and operator read
paths (direct DB query in 1.3, management endpoint later) are covered in
§5.14.

### 5.9 Configuration

| Key                                                  | Default   | Description                                                                              |
| ---------------------------------------------------- | --------- | ---------------------------------------------------------------------------------------- |
| `gravitino.iceberg-rest.async-purge.enabled`         | `true`    | When `true`, `purgeRequested=true` deletes files asynchronously. Set `false` to roll back to synchronous deletion on the request thread. |
| `gravitino.iceberg-rest.async-purge.worker-threads`           | `4`       | Worker pool size per server. |
| `gravitino.iceberg-rest.async-purge.delete-threads-per-job`   | `8`       | Parallelism of file deletes within a single job. |
| `gravitino.iceberg-rest.async-purge.poll-interval-ms`         | `5000`    | Worker poll interval. |
| `gravitino.iceberg-rest.async-purge.batch-size`               | `16`      | Jobs leased per tick. |
| `gravitino.iceberg-rest.async-purge.lease-timeout-ms`         | `300000`  | Lease duration before a job is reclaimable. |
| `gravitino.iceberg-rest.async-purge.max-attempts`             | `5`       | Attempts before `DEAD_LETTER`. |
| `gravitino.iceberg-rest.async-purge.backoff-base-ms`          | `30000`   | Exponential backoff base. |
| `gravitino.iceberg-rest.async-purge.backoff-max-ms`           | `3600000` | Exponential backoff ceiling. |
| `gravitino.iceberg-rest.async-purge.completed-retention-hours`| `168`     | How long `SUCCEEDED` rows are retained before pruning. |

### 5.10 Backward compatibility (wire)

The Iceberg REST spec does not require file deletion to be complete
before the response. With this design:

- The catalog entry is removed before `204`, so `HEAD` returns `404` and
  `LIST` no longer includes the table. `CREATE` / `register` at the same
  identifier is **rejected with `409 Conflict` until the purge job
  completes** (§5.13), which closes the PRD §2.2 same-name recreate attack.
- Object-store files may linger until the worker drains. Documented in
  release notes.

Operators needing strict synchronous deletion set
`gravitino.iceberg-rest.async-purge.enabled = false`.

### 5.11 Security

- `@AuthorizationExpression` on `dropTable` runs on the request thread,
  unchanged.
- The worker uses FileIO credentials snapshotted at enqueue time.
  Credentials that expire before the worker runs (STS tokens, …) need
  the refresh strategy under discussion in §8.2.
- `iceberg_purge_job` may contain credentials in `file_io_props`; the
  existing Gravitino DB encryption / access controls apply.

### 5.12 User process

End-to-end flow with async purge enabled (the 1.3 default):

1. Operator runs with `gravitino.iceberg-rest.async-purge.enabled = true`
   (the default). No client-side change is required.
2. A client issues
   `DELETE /v1/{prefix}/namespaces/{ns}/tables/{t}?purgeRequested=true`.
3. The server runs authorization on the request thread, loads the table
   metadata location, drops the catalog entry, and persists an
   `iceberg_purge_job` row.
4. The server responds `204 No Content` within typical request latency
   (target p99 < 500 ms, < 5 s for the largest tables). The table is
   immediately absent from `LIST` and `HEAD` returns `404`.
5. In the background, a worker on any replica leases the job, rebuilds
   the snapshot graph from `metadata_location`, and deletes every
   reachable file, retrying transient failures with exponential backoff.
6. On success the job is marked `SUCCEEDED`; on terminal failure it lands
   in `DEAD_LETTER` for operator inspection. Listeners observe
   `IcebergPurge{Started,Completed,Failed}Event` throughout.
7. *(Recovery)* If the drop was a mistake, the table can be re-registered
   at the stored metadata location any time before the worker deletes the
   files (§5.7). Re-registering also cancels the matching purge job so the
   worker never deletes the recovered table's files (§5.13).

### 5.13 Name reuse during purge (tombstone semantics)

This resolves the §8.1 open question. While a purge job for an identifier
is non-terminal (`PENDING` / `RUNNING`), that identifier is treated as
**still occupied** even though its catalog entry is already gone. The
active `iceberg_purge_job` row *is* the tombstone — no second table is
needed. This is the §8.4 / Q4 "fold the lifecycle onto an existing row"
idea, scoped to the job row rather than a catalog-backing table (which
Gravitino does not have for the Iceberg REST surface).

The REST server consults the purge job store **on the request thread**
(one indexed lookup via `idx_object`) and returns:

| Operation | Active purge job exists for the identifier | No active job (`SUCCEEDED` / `DEAD_LETTER` / none) |
|-----------|---------------------------------------------|-----------------------------------------------------|
| `loadTable` / `HEAD` | `404 NoSuchTableException` (catalog entry already dropped) | `404` |
| `alterTable` / `updateTable` / commit | `404 NoSuchTableException` | `404` |
| `createTable` (same identifier) | **`409 Conflict`** — table is being purged | succeeds (brand-new table) |
| `registerTable` (same identifier) | **`409 Conflict`**, except the recovery path below | succeeds |

"Active" means `state IN ('PENDING','RUNNING')`. A job in `SUCCEEDED`
(even before its row is pruned per `completed-retention-hours`) no longer
blocks reuse — the files are gone, so a recreate is safe. A `DEAD_LETTER`
job also stops blocking, so a failed cleanup never permanently wedges the
namespace; operators see the dead-lettered row and reclaim leaked files
out of band.

This closes the PRD §2.2 attack directly: a same-name / same-location
recreate can no longer expose the dropped table's data, because the
recreate is refused until the data files are deleted. Telling *whether* a
given identifier is mid-purge, done, or dead-lettered is the read-side
concern handled in §5.14.

The same rule applies to **views** (`createView` / `replaceView` → `409`
while a `VIEW` job is active). A **namespace** carries no data, so it can
be recreated freely; any table or view created inside it is still guarded
individually by its own job row.

**Recovery under the tombstone.** Because the identifier is now blocked,
register-as-recovery (§5.7) first lifts the tombstone: re-registering at
the stored `metadata_location` atomically CAS's the matching active job to
a terminal `CANCELLED` state and restores the catalog entry in the same
transaction. Cancelling the job is also what removes the
worker-deletes-the-recovered-table race — the worker only ever leases
non-terminal rows. This is **recovery-scoped** cancellation only; a
general user-facing "cancel my purge" API stays out of scope (§3.2).

**Concurrency.** The create/register conflict check and the worker's
state transitions serialize on the job row, exactly like the §5.4 leasing
CAS. A `createTable` that observes `SUCCEEDED` proceeds; one that observes
`PENDING` / `RUNNING` gets `409`; and recovery's CAS to `CANCELLED`
competes with the worker's CAS to `RUNNING` — at most one wins, so a job
that has already started running cannot be silently cancelled out from
under the worker (recovery then sees `RUNNING` and returns `409`, and the
caller retries once the lease frees or the job terminates).

### 5.14 Observing in-flight and failed purges

§5.13 keeps the *name* reserved; this section answers the read-side
question: "the table is gone from the catalog — is it **still being
purged**, already done, or **dead-lettered**?" The source of truth is the
`iceberg_purge_job` row, exposed to three audiences without touching the
Iceberg REST wire contract (Goal #4).

**Iceberg REST clients (table owner).** By design they observe only that
the table is dropped: `loadTable` / `HEAD` → `404`, `LIST` omits it. The
single purge-specific signal is the §5.13 `409` on a same-identifier
`createTable` / `register`, whose Iceberg `ErrorResponse.message` names the
blocking job (e.g. *"table `db.t` is being purged (job 123, state
RUNNING)"*). A standard client needs nothing more — semantically the table
is deleted — and we add **no** non-standard fields to load/list responses.

**Operators.** In 1.3 the read path is **direct DB query** of
`iceberg_purge_job` (the `idx_object` index makes the per-identifier lookup
cheap), plus the metrics below — both come essentially for free once the
job table and worker exist. This is enough to answer "is this table still
being purged / did it fail?" and to triage dead letters (§4) without
building new HTTP surface under deadline pressure.

A dedicated **management endpoint** (Gravitino admin plane, *not* the
Iceberg REST path) is **deferred to a later release** — see Phase 2 in §6.
When added it would follow the `metalakes/{metalake}/jobs/runs` convention
(hierarchical, metalake/catalog-scoped, filters as query params):

- `GET /metalakes/{metalake}/catalogs/{catalog}/cleanups?state=&schema=&table=&page…`
  — list / filter, keyed by object identifier (catalog + schema + table),
  not an opaque job id (no caller holds one, so there is no by-id fetch).
  The per-object question is answered by `…/cleanups?schema={s}&table={t}`,
  with §5.13 guaranteeing at most one active row.

Each row would report `state`, `attempts` / `max_attempts`, `last_error`,
`lease_owner`, `created_by`, and timestamps. Read-only — there is no cancel
verb (§3.2); the only operator-triggered transition is the
register-as-recovery flow (§5.13).

**Automation / monitoring.**

- **Events** (§5.8): `IcebergPurge{Started,Completed,Failed}Event` for
  streaming consumers.
- **Metrics**: gauges `purge.jobs.{pending,running,dead_letter}` and
  `purge.oldest_pending_age_ms`; counters `purge.{completed,failed}`. These
  let operators alert on a growing backlog ("silently still deleting") or
  any dead-letter ("silently failed to delete") — the two states that would
  otherwise go unnoticed.

`DEAD_LETTER` is the operationally critical state: in 1.3 it is queryable
(DB), counted (metric), and emitted (event) — so a cleanup that exhausts
retries is always discoverable rather than a silent file leak, even before
the management endpoint lands.

### 5.15 Restart and crash recovery

Restart handling is not a separate mechanism — it falls out of the durable
job table (§5.3) plus the lease model (§5.4). Three guarantees hold across
a process restart or crash:

1. **Nothing in-flight is lost.** The job is committed to
   `iceberg_purge_job` *before* the `DELETE` returns (§5.2); there is no
   in-memory queue, so a restart cannot drop pending work. `PENDING` jobs
   are picked up on the first worker tick after boot.
2. **`RUNNING` jobs orphaned by the crash are reclaimed.** A process that
   died mid-purge leaves a row in `state='RUNNING'` with a stale
   `lease_owner` / `lease_expires_at`. The worker poll already selects rows
   whose lease has expired, so once it does the restarted node — or any peer
   — reclaims the row via the CAS. This is the "worker killed mid-job" row
   in §5.5.
3. **Re-running a half-finished job is safe.** The worker rebuilds the file
   set from `metadata_location` and re-deletes; already-gone files raise
   `NotFoundException`, which counts as success (§5.5). A crash anywhere in
   the delete loop costs only duplicated work, never corruption.

In a **multi-replica** deployment this is seamless: surviving replicas
reclaim a dead node's jobs on lease expiry with no coordinator.

The one rough edge is a **single-replica** restart: the crashed node's own
`RUNNING` jobs sit idle until `lease_expires_at` passes — up to one
`leaseTimeout` of dead time — before the rebooted process reclaims them.
**v1 behavior:** rely on lease expiry (correct, bounded, zero extra code);
keep `leaseTimeout` modest so the resume delay is small. **Future
optimization:** a startup lease sweep that resets `RUNNING` rows whose
`lease_owner` matches this node's stable identity back to `PENDING`, so they
resume immediately instead of waiting out the lease.

---

## 6. Task Breakdown

Ordered so dependencies come first. Each item maps to one GitHub issue/PR.
Async purge ships **as the default in 1.3** (PR #11152, confirmed); the
synchronous path remains only as a rollback flag.

### Phase 1 (1.3): Async purge core
- [ ] Add the `iceberg_purge_job` schema and migrations (MySQL, H2, PostgreSQL)
- [ ] Implement `IcebergPurgeJobStore` enqueue path (persist job, return)
- [ ] Implement the worker pool: leasing via `FOR UPDATE SKIP LOCKED`, lease renewal, file deletion, retry/backoff state machine
- [ ] Add the `async-purge.enabled` feature flag and wire both paths into `IcebergTableOperationExecutor.dropTable` (async default, synchronous rollback)
- [ ] Add purge events: `IcebergPurgeStartedEvent`, `IcebergPurgeCompletedEvent`, `IcebergPurgeFailedEvent`
- [ ] Add purge observability (§5.14), low-cost signals only: metrics (`purge.jobs.{pending,running,dead_letter}`, `purge.oldest_pending_age_ms`, `purge.{completed,failed}`) and the informative `ErrorResponse` message on the §5.13 `409`. Operator read path in 1.3 is direct DB query of `iceberg_purge_job`
- [ ] Extend coverage to views (`object_type='VIEW'`, metadata-only cleanup) and namespace-drop cascade (one job per contained object)
- [ ] Enforce tombstone semantics (§5.13): on `createTable`/`createView`/`registerTable`, reject with `409` when an active job exists for the identifier (`idx_object` lookup on the request thread)
- [ ] Add register-table recovery support (§5.7): atomically CAS the matching active job to `CANCELLED` and restore the catalog entry; documentation

### Phase 1 (1.3): Testing
- [ ] Unit tests: enqueue, state machine, worker leasing/renewal/contention (H2)
- [ ] Docker integration tests: latency, restart-resume (`kill -9` mid-purge), two-replica no-duplicate-deletes
- [ ] Scale & concurrency tests (PRD §4 / R3) — see §7

### Phase 1 (1.3): Documentation
- [ ] Update user-facing documentation in `docs/` (async semantics, config, rollback flag, release notes)

### Phase 2 (post-1.3): Management endpoint
- [ ] Add the read-only `metalakes/{metalake}/catalogs/{catalog}/cleanups` management endpoint (§5.14): REST resource, DTOs, authorization, client support, docs, and tests. Filtered list keyed by object identifier; no by-id fetch
- [ ] Startup lease-sweep optimization (§5.15): on boot reset `RUNNING` rows owned by this node's stable identity back to `PENDING` so single-replica restarts resume without waiting out `leaseTimeout`

---

## 7. Testing

- Unit (`./gradlew :iceberg:iceberg-rest-server:test -PskipITs`):
  - `TestIcebergPurgeJobStore` — enqueue and row contents.
  - `TestIcebergPurgeStateMachine` — PENDING → RUNNING → SUCCEEDED;
    failure → retry → DEAD_LETTER.
  - `TestIcebergPurgeWorker` — leasing, renewal, contention (H2 backend).
  - `TestIcebergTableOperationExecutorAsyncPurge` — async default enqueues
    exactly one job; `async-purge.enabled=false` falls back to synchronous
    purge; thrown errors surface as 5xx.
  - `TestIcebergPurgeTombstone` (§5.13) — `createTable`/`register` at an
    identifier with an active job returns `409`; succeeds once the job is
    `SUCCEEDED`/`DEAD_LETTER`; `loadTable`/`alterTable` return `404`;
    re-register CAS's the job to `CANCELLED` and the worker then skips it;
    the recovery-vs-worker CAS race resolves to a single winner.
- Integration (`gravitino-docker-test`):
  - Drop a table; REST returns < 500 ms; worker eventually clears every file.
  - Restart REST mid-purge (kill -9); purge resumes.
  - Two replicas on one DB; no duplicate deletions or orphans.
  - Rollback flag: existing `IcebergRESTServiceIT` reruns unchanged with
    `async-purge.enabled=false`.
- Scale & concurrency (PRD §4 / R3):
  - Locust scenarios under `gravitino-test/cloud/scale-test/`.
  - High-file-count fixtures: medium (~50K files), large (~500K files).
  - Concurrent-drop tier.
  - Cucumber features for drop / soft-delete / cleanup / cleanup-failure.
  - Timing assertion: drop returns within 5 s regardless of table size.
  - Direct GCS/S3 listing asserts the storage prefix is empty after cleanup.

---

## 8. Open questions

1. ~~**Name reuse after drop**~~ — **Resolved (§5.13).** We adopt the
   tombstone approach (Option A): the active `iceberg_purge_job` row keeps
   the identifier occupied, so `CREATE` / `register` at the same identifier
   returns `409 Conflict` until the job reaches `SUCCEEDED`. This closes the
   PRD §2.2 same-name/same-location recreate attack raised with @jerryshao.
   It does change today's drop semantics (recreate is no longer immediate);
   that behavior change is called out in the release notes (§5.10).
2. **Credential refresh** — refresh FileIO credentials from the catalog at
   lease-renewal time, or require static credentials for catalogs that
   opt into async purge? Leaning toward refresh so long-retention soft
   delete (R2) keeps working; cost/complexity trade-off still open.
3. ~~**Multi-server coordination on H2**~~ — **Resolved (§5.4).** Claiming is
   an optimistic compare-and-swap `UPDATE` that works on every SQL backend;
   `SKIP LOCKED` is used as an optimization where available and the CAS form is
   the fallback elsewhere. No advisory locks.
4. ~~**Job row vs. catalog row**~~ — **Resolved (§5.13).** Name reuse (Q1)
   is resolved by treating the active `iceberg_purge_job` row itself as the
   tombstone, so there is no separate catalog tombstone row to fold fields
   onto. The Iceberg REST server fronts arbitrary catalogs and has no
   uniform table-backing row to extend (Gravitino has no `iceberg_tables`
   table), which is precisely why the job row — not a catalog row — carries
   the lifecycle.
