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

| Field   | Value                                                   |
| ------- | ------------------------------------------------------- |
| Status  | Complete                                                |
| Authors | @roryqi                                                 |
| Created | 2026-05-19                                              |
| Module  | `iceberg/iceberg-rest-server`, `iceberg/iceberg-common` |

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

We want an opt-in path where the drop returns quickly, finishes file
deletion reliably in the background, survives restarts, and runs safely
across multiple server replicas — while synchronous deletion remains the
default, selected automatically for any client that does not opt in.

*Not in scope:* `RelationalGarbageCollector`, which deletes tombstoned
**rows** from Gravitino's relational backend. Different IO surface,
different failure model — kept separate.

---

## 2. Goals

1. **Fast response**: `DELETE … ?purgeRequested=true` returns at typical
   request latency (target p99 < 500 ms, < 5 s even for the largest
   tables) regardless of table size.
2. **Operational simplicity**: Ship one async deletion path as an opt-in;
   synchronous deletion stays the default so existing behavior is unchanged
   unless a client explicitly asks for async.
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
implementation gated behind a per-request opt-in, with synchronous deletion
as the default.

| Approach                                                             | Pros                                                                                     | Cons                                                                                             | Decision                                                                |
| -------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------- |
| Synchronous only (status quo)                                        | Simplest; strongest "deleted means gone" guarantee                                       | Exceeds HTTP timeouts, saturates Jetty, no retry/audit on large tables                           | **Rejected** — the problem we are solving                               |
| Pluggable `IcebergPurger` SPI                                        | Extensible without code changes                                                          | Real added surface (SPI, discovery factory, context) with **no** second implementation in flight | **Rejected** — revisit when a second implementation has a real customer |
| Reuse `RelationalGarbageCollector`                                   | Proven worker/scheduling pattern                                                         | Different IO surface (object store vs. JDBC) and failure model                                   | **Rejected** — share patterns, not code                                 |
| External job system (Quartz / Temporal)                              | Mature scheduling, retries, observability                                                | Heavy operational burden on every operator                                                       | **Rejected** — disproportionate for one workload                        |
| Enumerate files at enqueue time                                      | Worker needs no metadata re-read                                                         | Slow on large tables (defeats the latency goal), bloats job rows                                 | **Rejected** — store `metadata_location`, re-read at run time           |
| Object-store job markers (no DB table)                               | No schema / migration                                                                    | Hand-rolled lease + renewal, no indexed scheduling, fragments per-bucket                         | **Rejected** — higher net complexity (see below)                        |
| **JDBC job table + worker pool, async opt-in (synchronous default)** | Smallest bug surface; restart-safe and cluster-safe via CAS claim; one code path to test | No built-in extension point                                                                      | **Chosen**                                                              |

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
            │  X-Gravitino-Async-Purge header (absent ⇒ synchronous)
      ┌─────┴───────────────────────────┐
      │ header absent                   │ X-Gravitino-Async-Purge: true
      ▼                                 ▼
 CatalogUtil.dropTableData         persist iceberg_cleanup_job
 (synchronous, on request thread)  row, return 204
                                        │
                                        ▼
                                  worker pool (any replica)
                                    claims job via CAS UPDATE (heartbeat ownership)
                                    → rebuild snapshot graph from metadata_location
                                    → stream + delete every reachable file, retry on later polls
                                    → SUCCEEDED | FAILED
```

The header is optional and defaults to synchronous: when it is **absent** —
which is the case for any standard Iceberg client — the drop is synchronous,
preserving today's "deleted means gone" behavior. A client opts into async
deletion by sending `X-Gravitino-Async-Purge: true` (the header name uses
canonical HTTP casing, each word capitalized).

### 5.2 User flow

1. A client issues
   `DELETE /v1/{prefix}/namespaces/{ns}/tables/{t}?purgeRequested=true`
   with `X-Gravitino-Async-Purge: true` to opt into async deletion. Absent
   the header — the case for any standard Iceberg client — the drop is
   synchronous (today's behavior). Steps 2–5 describe the async path.
2. The server runs authorization on the request thread, loads the table
   metadata location, drops the catalog entry, and persists an
   `iceberg_cleanup_job` row.
3. The server responds `204 No Content` within typical request latency. The
   table is immediately absent from `LIST` and `HEAD` returns `404`.
4. A worker on any replica claims the job, rebuilds the snapshot graph from
   `metadata_location`, and streams through every reachable file deleting
   it, retrying transient failures on later polls.
5. On success the job is `SUCCEEDED`; on terminal failure it lands in
   `FAILED` for operator inspection (queryable in the job table, §5.9).

### 5.3 Request-path interaction

```java
public void dropTable(IcebergRequestContext ctx, TableIdentifier id,
                      boolean purgeRequested) {
  IcebergCatalogWrapper w = catalogWrapperManager.getCatalogWrapper(ctx.catalogName());
  if (!purgeRequested) { w.dropTable(id); return; }

  if (!asyncPurgeEnabled) {                 // default when header is absent
    w.purgeTable(id);                       // synchronous, today's behavior
    return;
  }

  TableMetadata metadata = w.loadTableMetadata(id);
  w.dropTable(id);                          // metadata-only drop in the catalog
  cleanupManager.addJob(
      new IcebergCleanupJob(
          0L,                               // id assigned by IdGenerator at enqueue
          catalogId,                        // resolved catalog entity id
          id.namespace().toString(),
          id.name(),
          metadata.metadataFileLocation(),
          w.fileIOImpl(),
          w.fileIOProperties(),
          ctx.userPrincipal()));
}
```

Order matters on the async path: load metadata location → drop catalog
entry → enqueue the job. A cleanup job exists only for a table already gone
from the catalog. `fileIOProperties` is captured at enqueue time so the
worker can reconstruct `FileIO` even if the catalog is later reconfigured.
The enqueued row also serves as the **tombstone** that blocks name reuse
while the files still exist (§5.7).

**Repeated drop is idempotent.** Because the catalog entry is removed before
the first `DELETE` returns, a second `DELETE …?purgeRequested=true` for the
same identifier reaches `w.loadTableMetadata(id)` (or `w.dropTable(id)`),
finds nothing, and returns `404 NoSuchTableException` — the standard Iceberg
REST response for dropping a missing table. We do **not** enqueue a second
cleanup job: the first `loadTableMetadata` throws `NoSuchTableException`
before the enqueue line is reached, so a client retrying a drop (or a
duplicate request) never produces a duplicate job or a duplicate purge. This
holds regardless of whether the in-flight job is still `PENDING`/`RUNNING` or
already terminal, so it is consistent with the §5.7 tombstone table.

### 5.4 Schema — `iceberg_cleanup_job`

```sql
CREATE TABLE IF NOT EXISTS `iceberg_cleanup_job` (
  `id`                BIGINT(20)    UNSIGNED NOT NULL COMMENT 'globally unique cleanup job id',
  `catalog_id`        BIGINT(20)    UNSIGNED NOT NULL COMMENT 'globally unique id of the owning catalog, stable across catalog rename',
  `namespace`         VARCHAR(512)  NOT NULL COMMENT 'namespace of the table to be cleaned up',
  `table_name`        VARCHAR(256)  NOT NULL COMMENT 'name of the table to be cleaned up',
  `metadata_location` MEDIUMTEXT   NOT NULL COMMENT 'location of the table metadata file to purge',
  `file_io_impl`      VARCHAR(256)  NOT NULL COMMENT 'FileIO implementation class used to access the table files',
  `file_io_props`     MEDIUMTEXT    NOT NULL COMMENT 'JSON-encoded FileIO properties',
  `state`             VARCHAR(16)   NOT NULL COMMENT 'PENDING|RUNNING|SUCCEEDED|FAILED',
  `attempts`          INT(10)       NOT NULL DEFAULT 0 COMMENT 'number of processing attempts made so far',
  `last_error`        VARCHAR(2048) NULL COMMENT 'truncated reason for the most recent failure, NULL until a job fails',
  `heartbeat_at`      BIGINT(20)    NOT NULL DEFAULT 0 COMMENT 'last heartbeat from the worker, 0 when not running',
  `created_by`        VARCHAR(128)  NOT NULL COMMENT 'principal that requested the drop (audit)',
  `updated_at`        BIGINT(20)    NOT NULL COMMENT 'last state change, drives poll ordering and old finished-job cleanup',
  PRIMARY KEY (`id`),
  KEY `idx_state_updated` (`state`, `updated_at`),
  KEY `idx_object` (`catalog_id`, `namespace`(255), `table_name`(128), `state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'async Iceberg table cleanup jobs';
```

We store only `metadata_location`, not the file list — enumeration is slow
on large tables, and `TableMetadataParser.read(io, location)` rebuilds the
snapshot graph deterministically when the worker runs.

Row `id` is **not** a DB `AUTO_INCREMENT` column: the server allocates it from
Gravitino's `IdGenerator` at enqueue time, and every timestamp (`heartbeat_at`,
`updated_at`) is supplied by the application rather than the database. This keeps
one parameterized set of `INSERT` / `UPDATE` statements portable across H2,
MySQL, and PostgreSQL, served by a single shared SQL provider (§5.5).
The object is keyed by `catalog_id`, not by metalake/catalog *names*. Async
cleanup only runs when the Iceberg REST server is embedded in Gravitino (the job
store reuses the entity store's relational backend), so a real catalog entity —
and its id — always exists, and the request thread reads it for free:
`catalogDispatcher.loadCatalog(...)` returns the `BaseCatalog`, whose
`CatalogEntity` is already in memory, so `catalog.entity().id()` needs no extra
entity-store round trip. `catalog_id` is globally unique (it is
`catalog_meta`'s primary key), so it identifies the catalog without scoping by
metalake, and it is **stable across catalog rename**: a rename mid-cleanup would
leave a name-keyed tombstone stranded while the physical warehouse prefix is
unchanged, but the id-keyed tombstone still matches. The leaf `namespace` and
`table_name` stay as strings — the table is already dropped at enqueue, so there
is no table entity/id to reference. `catalog_id` indexes in full, while
`namespace` and `table_name` are prefix-indexed on MySQL (`namespace(255)`,
`table_name(128)` — table names also cap at 128) to keep `idx_object` within
MySQL's 3072-byte index key-length limit. The job store reuses the entity
store's relational backend —
its connection pool, transaction management, and per-backend dispatch — instead
of opening its own JDBC connections.

A single column drives retries: `attempts` counts failures so the worker
gives up at the retry ceiling. A failed job returns to `PENDING` and is
re-claimed on a later poll, so the poll cadence (`poll-interval-secs`,
§5.10) is the retry interval — no separate scheduling column is needed. The
ceiling is a config (`max-attempts`, §5.10), not a per-row `max_attempts`
column, since it is the same for every job.

`last_error` records the reason for the most recent failure — the metadata
exception that sent a job back to `PENDING`, or the one that drove it to
`FAILED` — truncated to fit the column. It is overwritten on each attempt,
not appended, so it always reflects the latest failure. This puts the
failure reason in the job row itself, so an operator triaging a `FAILED`
cleanup (§5.6, §5.9) reads it from a single indexed query without correlating
worker logs by timestamp. The full stack trace still goes to the logs.

Migration: `upgrade-1.2.0-to-1.3.0-mysql.sql` (and H2 / PostgreSQL).

### 5.5 Worker pool and multi-replica ownership

The worker pool has `worker-threads` threads, modeled on the
`RelationalGarbageCollector` scheduler. **A thread polls for work only when
it is free**: a free thread claims a single job, runs it to completion or
failure, then loops back to poll for the next; a thread busy on a job never
polls. Capacity-gating is therefore by construction — no central scheduler
claims jobs ahead of the threads, the pool never holds a claimed-but-unstarted
job, and a `RUNNING` row always maps to a thread actively deleting files (its
heartbeat reflects real progress, not a job idling in a queue). A poll that
finds nothing claimable waits `poll-interval-secs` and retries. `worker-threads`
thus bounds the concurrent jobs per server; there is no separate claim-batch
size.

Ownership is tracked by a **heartbeat** alone: the worker that claims a job
keeps refreshing `heartbeat_at` while it runs and remembers in memory, per
owned id, the `heartbeat_at` value it last wrote (no owner column — a stale
row is reclaimable by anyone, which is exactly the failover behavior we want).
A job is abandoned, and reclaimable, once its heartbeat is older than
`heartbeatTimeout`.

Claiming is an optimistic compare-and-swap `UPDATE` that works on **every**
SQL backend. A free thread reads a small candidate window and claims the
first row it wins:

```sql
-- a free thread reads a small candidate window
SELECT id FROM iceberg_cleanup_job
 WHERE state = 'PENDING'
    OR (state = 'RUNNING' AND heartbeat_at < :now - :heartbeatTimeout)
 ORDER BY updated_at LIMIT :window;

-- claim one candidate; the row is ours only if affected_rows = 1
UPDATE iceberg_cleanup_job
   SET state='RUNNING', heartbeat_at=:now, updated_at=:now
 WHERE id=:id
   AND ( state='PENDING'
      OR (state='RUNNING' AND heartbeat_at < :now - :heartbeatTimeout) );
```

A loser sees `affected_rows = 0` and tries the next id in its window. The
window is a small constant that only gives a CAS loser alternatives — not a
capacity control, since a free thread still claims exactly one job. The same
portable CAS runs on **every** backend: H2, MySQL, and PostgreSQL share one SQL
provider, so we do **not** use `SELECT … FOR UPDATE SKIP LOCKED` or DB-specific
advisory locks. The claiming `UPDATE` is the serialization point, so two
replicas can never claim the same job; a backend that ever needs a divergent
statement can register its own provider without touching the rest of the design.

**Worker loop (one free thread):**

1. **Poll & claim.** Read the candidate window above and CAS the first
   winnable row to `RUNNING` with a fresh `heartbeat_at`. If nothing is
   claimable, wait `poll-interval-secs` and repeat.
2. **Load metadata.** `TableMetadataParser.read(io, metadata_location)`. The
   root `metadata.json` is the only file whose absence means "the table is
   already gone": a `NotFoundException` here completes the job (a prior attempt
   already deleted it). Any other failure (e.g. corrupt/unreadable metadata)
   releases the job for retry, failing it once `attempts` hits the ceiling
   (§5.6).
3. **Delete leaves before parents.** Walk the snapshot graph and delete one
   dependency level at a time — data files → manifests → manifest lists →
   statistics → ancestor metadata → the root `metadata.json` **last** — only
   advancing once the current level is fully gone. Within a level, files are
   independent: group them into batches of `delete-batch-size` and hand each to
   the shared `deleteExecutor` for a **bulk** delete; `NotFoundException` counts
   as deleted. Leaf-first ordering keeps every crash recoverable: the root
   pointer (and the manifests/manifest lists above any leaf still on disk)
   survives, so a retry re-enumerates and finishes. Deleting a parent first
   would strand its children — and a `metadata.json` removed early would be
   misread as "table already gone", leaking everything beneath it. The worker
   thread itself never issues the storage call directly, and a *separate*
   background task keeps `heartbeat_at` fresh throughout (see "Heartbeat is
   decoupled from deletion" below).
4. **Finish.** Once every reachable file is gone the job is `SUCCEEDED`
   (§5.6). The thread then loops back to step 1.

Execution mirrors `CatalogHandlers.purgeTable`, deleting **in bulk batches**
and never one file at a time. To keep every crash recoverable it deletes one
dependency level at a time — leaves first, the root `metadata.json` last — and
each `deleteAll` blocks until its level is fully gone before the next begins.
Data files are the only unbounded level, so they are streamed and deleted one
manifest at a time rather than collected up front — a million-file table never
materializes its whole path set; only the far smaller manifest, manifest-list
and metadata path lists are held at once:

```java
TableMetadata meta = TableMetadataParser.read(io, job.metadataLocation());
Set<String> manifests = new LinkedHashSet<>();
for (Snapshot s : meta.snapshots()) {
  for (ManifestFile m : s.allManifests(io)) {
    if (!manifests.add(m.path())) continue;                  // dedup shared manifests
    try (CloseableIterable<String> dataFiles =               // lazy, one batch resident at a time
        ManifestFiles.readPaths(m, io, meta.specsById())) {
      deleteAll(io, dataFiles);                              // leaves: streamed, then deleted
    }
  }
}
deleteAll(io, manifests);                                    // then their manifests
deleteAll(io, ReachableFileUtil.manifestListLocations(table));
deleteAll(io, ReachableFileUtil.statisticsFilesLocations(table));
deleteAll(io, ancestorMetadata);                             // older metadata.json
deleteAll(io, List.of(job.metadataLocation()));              // root pointer, deleted last

// deleteAll: bulk-batch the (possibly lazy) iterable, then await it
void deleteAll(FileIO io, Iterable<String> files) {
  Iterators.partition(files.iterator(), deleteBatchSize)     // bounded batches
      .forEachRemaining(batch ->
          futures.add(deleteExecutor.submit(() ->
              // SupportsBulkOperations.deleteFiles when the FileIO supports it
              // (e.g. S3FileIO's batch-delete API), else concurrent per-file
              CatalogUtil.deleteFiles(io, batch, "cleanup", /* bulk */ true))));
  awaitAll(futures);                                         // NotFoundException counts as deleted
}
```

We delete in bulk rather than per file because a single `DeleteObjects`
round trip removes up to ~1000 objects, cutting both request count and
S3 cost by three orders of magnitude on a large table.
`CatalogUtil.deleteFiles(io, batch, msg, true)` routes to
`SupportsBulkOperations.deleteFiles` when the `FileIO` implements it
(S3FileIO, GCSFileIO, ADLSFileIO all do) and falls back to a concurrent
per-file delete otherwise; partial-batch failures are logged and suppressed,
matching the synchronous purge's best-effort stance (§5.6).

`deleteExecutor` is a single server-wide pool sized by `delete-threads`
(§5.10), shared by all concurrent jobs rather than allocated per job. This
bounds total file-delete concurrency on the server regardless of how many
jobs run at once; raise it to drain large tables faster when purge
throughput matters.

**When the delete queue is full.** The `deleteExecutor` is a
`ThreadPoolExecutor` with `delete-threads` threads and a *bounded* work queue,
and a worker that walks a million-file table can enqueue batches faster than
the pool drains them. We do **not** use an unbounded queue (it would let one
huge table OOM the server) and we do **not** drop work on rejection (that
would leak files). Instead the pool uses `CallerRunsPolicy`: once both the
threads and the bounded queue are saturated, `execute(...)` runs the batch
**inline on the calling worker thread**. That worker stops pulling new batches
from its lazy iterator until the inline delete returns, which is exactly the
back-pressure we want — the snapshot walk naturally slows to the rate storage
can absorb, queue depth stays bounded, and no batch is ever discarded. Because
the heartbeat lives on a different thread (next paragraph), a worker blocked in
an inline bulk delete still keeps its lease alive.

**Heartbeat is decoupled from deletion.** Deletion is a long, blocking,
I/O-bound activity: a worker thread can sit inside a bulk `deleteFiles` call —
or inside a `CallerRunsPolicy` inline batch — for seconds at a time, and across
a whole table for many minutes. If that same thread were responsible for
writing `heartbeat_at`, any slow storage round trip would stall the heartbeat,
the row would age past `heartbeatTimeout`, and a peer would wrongly reclaim a
job that is in fact making progress — causing duplicate work and lease
thrashing. So the heartbeat is **never** issued from the deleting thread. A
single background task per process (next paragraph) refreshes `heartbeat_at`
for every owned id on a fixed schedule, independent of where the worker threads
are blocked. This is why `delete-threads` work can be fully synchronous from
the worker's point of view without endangering the lease.

**Updating the heartbeat.** One background task per worker process — not one
per job — runs every `heartbeatTimeout / 3`, so two updates can be missed
(a GC pause, a brief DB hiccup) before the row looks abandoned. For each id
the process owns it issues a guarded `UPDATE`:

```sql
UPDATE iceberg_cleanup_job
   SET heartbeat_at = :now
 WHERE id = :id
   AND state = 'RUNNING'
   AND heartbeat_at = :lastValueIWrote;   -- CAS on the value I last wrote
```

The `heartbeat_at` value the worker last wrote is the lease: the `UPDATE`
is a compare-and-swap on it, so it succeeds (`affected_rows = 1`, and the
worker records the new `:now` as its next `:lastValueIWrote`) **only while
this process is still the owner**. If the process stalled past
`heartbeatTimeout` and a peer reclaimed the job, the peer's claim overwrote
`heartbeat_at`; this process's next CAS then matches no row
(`affected_rows = 0`), and it reads that as "I no longer own this job" —
it stops deleting and drops the id from its in-memory set. That is how
ownership transfers cleanly with no owner column. The claim itself
(`SET heartbeat_at=:now` above) seeds the first `:lastValueIWrote`; finishing
or releasing a job removes the id from the set so the task stops touching it.
A worker owns at most `worker-threads` ids, so this is a handful of cheap
point updates per cycle. If the host dies the task stops entirely and the
rows age out for reclaim.

### 5.6 Failure model, restart, and crash recovery

Per-file failures are logged but do not fail the whole job — the
synchronous purge has the same "best effort" stance. A job fails only if the
**metadata phase** fails. A `NotFoundException` counts as success rather than a
failure: a missing root `metadata.json` means the table is already gone, and a
missing data file, manifest, or manifest list (from `deleteFile` or while
enumerating reachable files) is already deleted. Any other failure goes back to
`PENDING` and is retried when a free thread next polls (§5.5), incrementing
`attempts` each time; when `attempts` reaches `max-attempts` the job goes to
`FAILED` instead of `PENDING`.

| Outcome                                    | Action                                                                                |
| ------------------------------------------ | ------------------------------------------------------------------------------------- |
| All files deleted (or already gone)        | `state='SUCCEEDED'`                                                                   |
| Failure, `attempts < max-attempts`         | `attempts++`, set `last_error`, back to `PENDING`, `heartbeat_at=0`; re-claimed later |
| Failure, `attempts` reaches `max-attempts` | `attempts++`, set `last_error` → `FAILED` (gave up retrying)                          |
| Worker killed mid-job                      | `RUNNING` row's heartbeat goes stale; another worker reclaims; deletes are idempotent |

**After `SUCCEEDED`.** The files are gone, so the tombstone lifts at once:
`createTable` / `register` at the identifier succeed again (§5.7). The
`purge.completed` counter increments and the row is pruned
`retention-hours` later (§5.10).

**After `FAILED`.** `FAILED` is terminal — the poll never re-selects it, so
the worker stops touching the job. The tombstone also lifts (§5.7), so a
failed cleanup never permanently wedges the name; the cost is that any files
the job did not delete are now an orphaned leak. The failure is therefore
made loud rather than silent: it increments `purge.failed`, the row stays
queryable (§5.9) with the reason in its `last_error` column (and the full
stack trace in the worker logs). An operator reclaims
the leaked files out of band — a manual delete or a storage lifecycle rule on
the table's prefix — and then drops the row, or lets it prune after
`retention-hours`. There is no automatic re-drive in 1.3;
re-enqueueing a `FAILED` cleanup is a manual operator action.

Restart handling falls out of the durable job table plus the heartbeat
model — no separate mechanism:

1. **Nothing in-flight is lost.** The job is committed before the `DELETE`
   returns; there is no in-memory queue. `PENDING` jobs are picked up as soon
   as a thread polls after boot.
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
catalog entry is already gone. The active `iceberg_cleanup_job` row *is* the
tombstone — no second table is needed. The REST server consults the store on
the request thread (one indexed lookup via `idx_object`):

| Operation                                   | Active job exists                 | No active job (`SUCCEEDED`/`FAILED`/none) |
| ------------------------------------------- | --------------------------------- | ----------------------------------------- |
| `loadTable` / `HEAD`, `alterTable` / commit | `404 NoSuchTableException`        | `404`                                     |
| `dropTable` (same identifier, repeated)     | `404 NoSuchTableException`        | `404`                                     |
| `createTable` (same identifier)             | **`409 Conflict`** — being purged | succeeds                                  |
| `registerTable` (same identifier)           | **`409 Conflict`**                | succeeds                                  |

A `SUCCEEDED` job (even before pruning) no longer blocks reuse — the files
are gone. A `FAILED` job also stops blocking, so a failed cleanup never
permanently wedges the namespace; operators see the failed row and reclaim
leaked files out of band. This closes the same-name / same-location recreate
attack: a recreate cannot expose the dropped table's data, because it is
refused until the files are deleted. A **namespace** carries no data, so it
can be recreated freely; tables inside it are guarded individually.

**Why the tombstone is necessary — does a recreate reuse the old path?**
Yes. When a `createTable` omits an explicit `location` — the common case —
Iceberg derives the table root from the identifier via
`BaseMetastoreCatalog.defaultWarehouseLocation(identifier)`, i.e.
`<warehouse>/<namespace>/<table>`. A recreate at the same identifier
therefore lands on the **same base prefix** as the table being purged. The
*file names* underneath do not collide — data files get UUID names from
`OutputFileFactory` and each metadata commit writes `<version>-<uuid>.metadata.json`
— and the worker deletes strictly by reachability from the *old*
`metadata_location`, so it can never delete the new table's files. The real
hazard is two live tables transiently sharing one prefix while old files are
still being deleted: confusing for operators, and an opening for a recreate
to read leftover data under the shared prefix. Blocking `createTable` /
`registerTable` with `409` until the cleanup is terminal removes that window
entirely. (A client that *does* pass an explicit, distinct `location` on
recreate would not share the prefix, but we still block on identifier to keep
one simple, predictable rule.)

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
is the `iceberg_cleanup_job` row, exposed without touching the wire contract:

- **Iceberg REST clients** observe only that the table is dropped
  (`404` / omitted from `LIST`). The single purge-specific signal is the
  §5.7 `409` on a same-identifier `createTable` / `register`, whose
  `ErrorResponse.message` names the blocking job.
- **Operators** query `iceberg_cleanup_job` directly (the `idx_object` index
  makes the per-identifier lookup cheap), plus the metrics below; the failure
  reason for a `FAILED` job is in the row's `last_error` column (§5.4), with
  the full stack trace in the worker's logs. This is enough to answer
  "still purging / failed, and why?" without new HTTP surface. A read-only
  management endpoint is deferred to Phase 2.
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
(absent ⇒ synchronous; `true` opts into async deletion). The server-side keys
only tune the worker pool and retries:

| Key                                                           | Default | Description                                                                            |
| ------------------------------------------------------------- | ------- | -------------------------------------------------------------------------------------- |
| `gravitino.iceberg-rest.async-cleanup.worker-threads`         | `2`     | Worker pool size per server (concurrent jobs).                                         |
| `gravitino.iceberg-rest.async-cleanup.delete-threads`         | `4`     | Server-wide file-delete pool size, shared across all jobs.                             |
| `gravitino.iceberg-rest.async-cleanup.delete-batch-size`      | `1000`  | Files per bulk-delete batch handed to `deleteExecutor` (§5.5).                         |
| `gravitino.iceberg-rest.async-cleanup.poll-interval-secs`     | `5`     | Worker poll interval in seconds; also the retry interval.                              |
| `gravitino.iceberg-rest.async-cleanup.heartbeat-timeout-secs` | `300`   | Age in seconds after which a job with no heartbeat is reclaimable.                     |
| `gravitino.iceberg-rest.async-cleanup.max-attempts`           | `5`     | Attempts before `FAILED`.                                                              |
| `gravitino.iceberg-rest.async-cleanup.retention-hours`        | `720`   | How long terminal (`SUCCEEDED` / `FAILED`) rows are retained before pruning (30 days). |

The thread defaults are deliberately modest. Each `delete-threads` thread now
issues a *bulk* delete of up to `delete-batch-size` files per call (§5.5), so
4 threads already sustain high delete throughput — far more than the 16
single-file threads an earlier draft assumed. `worker-threads` is 2 because a
single server rarely needs many tables purging at once and each worker can
saturate the delete pool on its own; both knobs are raised only when a backlog
metric (§5.9) shows the pool is the bottleneck.

There is no separate claim-batch-size key: a free thread claims one job at a
time and only when idle (§5.5), so `worker-threads` already bounds concurrent
jobs.

### 5.11 Security

- `@AuthorizationExpression` on `dropTable` runs on the request thread,
  unchanged.
- The worker uses FileIO credentials snapshotted at enqueue time.
  Credentials that expire before the worker runs (STS tokens, …) are
  refreshed from the catalog on each heartbeat, so long-running purges keep
  valid credentials.
- `iceberg_cleanup_job` may contain credentials in `file_io_props`; the
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
client never sends it and gets the synchronous default, so the protocol is
unchanged.

---

## 6. Task Breakdown

Ordered so dependencies come first. Each item maps to one GitHub issue/PR.
Synchronous purge remains **the default in 1.3**; async purge ships as an
opt-in path selected per request via the `X-Gravitino-Async-Purge` header.

### Phase 1 (1.3): Async purge core
- [ ] Add the `iceberg_cleanup_job` schema and migrations (MySQL, H2, PostgreSQL)
- [ ] Implement `IcebergCleanupJobStore` enqueue path (persist job, return)
- [ ] Implement the worker pool: portable CAS claiming with heartbeat ownership (one SQL provider for H2/MySQL/PostgreSQL, no `SKIP LOCKED`), heartbeat renewal on a thread decoupled from deletion, streaming **bulk** file deletion (`CatalogUtil.deleteFiles(..., true)`) through a bounded `deleteExecutor` with `CallerRunsPolicy` back-pressure, retry state machine that records `last_error` (re-claim failed jobs on later polls, give up at `max-attempts`)
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
  - `TestIcebergCleanupJobStore` — enqueue, row contents, and the state
    machine: PENDING → RUNNING → SUCCEEDED; failure → retry (back to
    PENDING) → FAILED; `last_error` populated on each failure. Runs against
    the H2/MySQL/PostgreSQL backend matrix (`TestJDBCBackend`).
  - `TestIcebergCleanupManager` — claiming, heartbeat renewal on a thread that
    stays fresh while delete batches block, contention (H2), bulk-delete
    batching, and `CallerRunsPolicy` back-pressure when the delete queue fills.
  - `TestIcebergTableOperationExecutorAsyncPurge` — `X-Gravitino-Async-Purge:
    true` enqueues exactly one job; a repeated drop returns `404` and enqueues
    no second job; an absent header runs the synchronous purge (the default);
    thrown errors surface as 5xx.
  - `TestIcebergPurgeTombstone` (§5.7) — `createTable`/`register` at an
    identifier with an active job returns `409`, succeeds once
    `SUCCEEDED`/`FAILED`; `loadTable`/`alterTable`/repeated `dropTable`
    return `404`.
- Integration (`gravitino-docker-test`):
  - Drop a table; REST returns < 500 ms; worker eventually clears every file.
  - Restart REST mid-purge (`kill -9`); purge resumes.
  - Two replicas on one DB; no duplicate deletions or orphans.
  - Synchronous default: existing `IcebergRESTServiceIT` reruns unchanged
    with no `X-Gravitino-Async-Purge` header.
- Scale & concurrency:
  - High-file-count fixtures: medium (~50K files), large (~500K files).
  - Concurrent-drop tier; drop returns within 5 s regardless of table size.
  - Direct GCS/S3 listing asserts the storage prefix is empty after cleanup.
