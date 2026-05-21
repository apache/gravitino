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
| **Single async purger (JDBC job table + worker pool) with a synchronous fallback flag** | Smallest bug surface; reliable, restart-safe, cluster-safe via `SKIP LOCKED`; one code path to test | No built-in extension point — a second strategy would need a follow-up refactor | **Chosen** |

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
  `state`             VARCHAR(16)   NOT NULL COMMENT 'PENDING|RUNNING|SUCCEEDED|DEAD_LETTER',
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
  KEY `idx_state_next_attempt` (`state`, `next_attempt_at`)
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
`lease_expires_at=:now+leaseTimeout`. `SKIP LOCKED` is the cluster-safety
primitive: any number of replicas can run the worker without external
coordination. H2 falls back to a conditional update.

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
| Worker killed mid-job              | Lease expires; another worker picks it up; deletes are idempotent   |

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

- The catalog entry is removed before `204`, so `HEAD` returns `404`,
  `LIST` no longer includes the table, and **`CREATE` at the same
  identifier currently succeeds immediately** (today's drop semantics).
  Whether this should instead be blocked until cleanup completes is an
  open design question — see §8.1.
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
   files (§5.7).

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
- [ ] Extend coverage to views (`object_type='VIEW'`, metadata-only cleanup) and namespace-drop cascade (one job per contained object)
- [ ] Add register-table recovery support / documentation (§5.7)

### Phase 1 (1.3): Testing
- [ ] Unit tests: enqueue, state machine, worker leasing/renewal/contention (H2)
- [ ] Docker integration tests: latency, restart-resume (`kill -9` mid-purge), two-replica no-duplicate-deletes
- [ ] Scale & concurrency tests (PRD §4 / R3) — see §7

### Phase 1 (1.3): Documentation
- [ ] Update user-facing documentation in `docs/` (async semantics, config, rollback flag, release notes)

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

1. **Name reuse after drop** — should `CREATE` at the same identifier be
   blocked (e.g. tombstone row → `409`) until cleanup completes? PRD §2.2
   argues an attacker could otherwise read the original table's data via a
   same-name/same-location recreate. Current design keeps today's drop
   semantics (recreate allowed immediately); changing this alters drop
   behavior. **Pending decision with @jerryshao.**
2. **Credential refresh** — refresh FileIO credentials from the catalog at
   lease-renewal time, or require static credentials for catalogs that
   opt into async purge? Leaning toward refresh so long-retention soft
   delete (R2) keeps working; cost/complexity trade-off still open.
3. **Multi-server coordination on H2** — fall back to advisory locks or
   conditional updates where `SKIP LOCKED` is unavailable? Needs a
   decision before implementation.
4. **Job row vs. catalog row** — if name-reuse (Q1) is resolved by keeping
   a tombstone on the catalog's table row, could the lease/retry fields
   live on that row instead of a separate `iceberg_purge_job` table
   (smaller migration)? Note Gravitino has no `iceberg_tables` table; this
   would apply to the JDBC catalog's backing table, so scope/portability
   across catalog types needs confirmation.
