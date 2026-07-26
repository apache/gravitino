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

# TreeLock HA Design — `LockBackend` SPI + JDBC Backend

Tracking issue: [#10474](https://github.com/apache/gravitino/issues/10474)

---

## 1. Background

### 1.1 What `TreeLock` is today

Gravitino guards every metadata operation with a hierarchical read–write lock implemented in
`core/src/main/java/org/apache/gravitino/lock/`. The package is small (≈ 400 lines of Java
across five files):

| File | Responsibility |
|------|----------------|
| `LockType.java` | `enum { READ, WRITE }` |
| `TreeLockNode.java` | One node in the lock tree. Wraps a `ReentrantReadWriteLock`. Tracks `holdingThreadTimestamp` keyed by `(thread, identifier)`, plus an `AtomicLong` reference count for eviction. |
| `TreeLock.java` | A per-operation handle. `lock(LockType)` walks the path root → leaf, takes `READ` on every ancestor and the requested type on the leaf, and pushes acquired nodes onto a deque. `unlock()` pops in reverse order. |
| `LockManager.java` | Singleton owner of the tree. `createTreeLock(identifier)` walks the path, creating missing nodes via `getOrCreateChild` (a `ConcurrentHashMap.computeIfAbsent`). Runs two daemon schedulers: a stale-node cleaner (BFS evicting nodes with `reference == 0` once total nodes exceed `0.5 × maxTreeNodeInMemory`) and a "deadlock checker" that, despite its name, only logs `WARN` when a node is held longer than 30 seconds. |
| `TreeLockUtils.java` | Static facade — `doWithTreeLock(identifier, lockType, executable)` is the universal entry point and resolves the manager via `GravitinoEnv.getInstance().lockManager()`. |

The pattern is the classic intention-locking convention: `READ` on every ancestor effectively
acts as an `IS`/`IX` lock, and the actual `READ`/`WRITE` lives on the leaf.

```
loading metalake.catalog.db.tbl                renaming metalake.catalog.db.tbl
  /                       READ                   /                       READ
  /metalake               READ                   /metalake               READ
  /metalake/catalog       READ                   /metalake/catalog       READ
  /metalake/catalog/db    READ                   /metalake/catalog/db    WRITE  ← parent
  /metalake/catalog/db/tbl READ
```

### 1.2 Why it exists

`TreeLockUtils.doWithTreeLock(...)` is invoked from 19 source files in `core/`:

```
core/src/main/java/org/apache/gravitino/
├── authorization/
│   ├── AccessControlManager.java
│   ├── OwnerManager.java
│   └── PermissionManager.java
├── catalog/
│   ├── CatalogManager.java
│   ├── FilesetOperationDispatcher.java
│   ├── FunctionOperationDispatcher.java
│   ├── ModelOperationDispatcher.java
│   ├── PartitionOperationDispatcher.java
│   ├── SchemaOperationDispatcher.java
│   ├── TableOperationDispatcher.java
│   ├── TopicOperationDispatcher.java
│   └── ViewOperationDispatcher.java
├── job/
│   ├── BuiltInJobTemplateEventListener.java
│   └── JobManager.java
├── metalake/
│   └── MetalakeManager.java
├── policy/
│   └── PolicyManager.java
├── stats/
│   └── StatisticManager.java
└── tag/
    └── TagManager.java
```

These are the synchronization points for almost every metadata mutation in the system.

### 1.3 Configuration today

`Configs.java` exposes three keys (all `gravitino.lock.*`):

| Key | Default | Purpose |
|-----|---------|---------|
| `gravitino.lock.maxNodes` | `MAX_NODE_IN_MEMORY` | Cap on live `TreeLockNode`s; cleanup triggers above 0.5× this |
| `gravitino.lock.minNodes` | `MIN_NODE_IN_MEMORY` | Floor below which the cleaner is a no-op |
| `gravitino.lock.cleanIntervalInSecs` | `CLEAN_INTERVAL_IN_SECS` | Cleaner cadence |

There is no key today selecting a *type* of lock backend — the in-process implementation is
hard-wired in `GravitinoEnv.java:551` via `this.lockManager = new LockManager(config)`.

---

## 2. Problem Statement

`TreeLock` has zero cross-process awareness. Each Gravitino server holds its own
`LockManager` with its own `treeLockRootNode` in JVM memory. Under HA — the deployment
mode Gravitino is moving toward — the lock tree on server A is invisible to server B.

### 2.1 Concrete failure modes

1. **Concurrent conflicting writes**. Two REST clients each hit a different node and rename
   the same table. Both `WRITE` locks succeed locally; both proceed to the underlying store.
   The result depends on the underlying store's atomicity — see §2.2.
2. **Read-modify-write races on the metadata store**. Several call sites do
   `load → mutate → save` against `EntityStore`. Without cross-process serialization, two
   such sequences interleave and the second writer overwrites the first writer's effect
   without observing it.
3. **Per-JVM "deadlock" detection**. The check in `LockManager.checkDeadLock` only walks
   the local tree, so a thread blocked waiting on a lock held in a *different* JVM is
   invisible. (Strictly speaking even the local check is just a 30-second held-lock warning,
   not a wait-for-graph analysis — see §1.1.)
4. **Stale auth caches across nodes**. Indirectly: many auth-side mutations
   (`AccessControlManager`, `PermissionManager`, `OwnerManager`) rely on TreeLock for
   ordering. Combined with the per-node JCasbin caches (see
   `design-docs/cache-improvement-design.md`), HA exposes both an ordering hole and a
   coherence hole.

### 2.2 Blast radius is not uniform

The damage from a TreeLock miss depends on what the underlying store does:

| Call-site category | Underlying state | What actually breaks under HA |
|--------------------|------------------|-------------------------------|
| Dispatchers writing to **`RelationalEntityStore`** (Gravitino-owned tables: metalake, catalog, schema, role, tag, policy, …) | Gravitino's JDBC store (H2/MySQL/Postgres) | Lost updates, phantom inserts on the metadata DB; no row-level guard exists today |
| Dispatchers proxying to **external catalogs** (Hive Metastore, Iceberg, Kafka, JDBC catalogs) | The external system | The external system arbitrates writes; Gravitino's local view can drift, but corruption of external state is unlikely |
| **Auth managers** writing role/permission/owner state | `RelationalEntityStore` + JCasbin policy cache | Same as the first row, plus stale caches on other nodes |

The implication: a uniform "wrap every TreeLock call in a distributed lock" answer pays the
network cost on every operation — including the proxy-only ones whose blast radius is
already small. We can do better by combining a pluggable lock backend with storage-level
guards.

---

## 3. Goals and Non-Goals

### 3.1 Goals

- Eliminate cross-process race conditions on **Gravitino's own metadata store** (the
  largest blast-radius category).
- Give operators a way to enable cross-node coordination *without* forcing them to deploy a
  new component (ZooKeeper/etcd/Redis), so HA is reachable for everyone running Gravitino
  on a relational backend.
- Preserve the current zero-RPC, zero-allocation in-process lock as the default — single-node
  Gravitino deployments must see no behavior change and no measurable performance change.
- Keep the change reviewable: introduce a clear `LockBackend` SPI now so future backends
  (ZooKeeper, etcd, Raft-on-store, …) can land as bounded follow-up PRs.

### 3.2 Non-goals

- **Solving consistency for external catalogs.** If a Hive table is mutated outside
  Gravitino, no Gravitino-level lock can help. The external system owns its concurrency.
- **Implementing ZooKeeper, etcd, or Redis backends in this PR.** Those are tracked as
  follow-ups; the SPI surface is designed to accommodate them.
- **Replacing the deadlock-warning mechanism with real distributed deadlock detection.**
  The JDBC backend relies on the database's own lock-wait-timeout instead.
- **Re-litigating which call sites need a lock.** A separate audit pass (Phase B in §8)
  will examine whether some `doWithTreeLock` calls can be replaced by storage-level CAS,
  but that work is out of scope here.

---

## 4. Solution Options

### 4.1 Option A — Drop-in distributed lock

Replace `LockManager` with a distributed implementation (ZooKeeper via Curator's
`InterProcessReadWriteLock`, etcd, or Redis Redlock).

| Dimension | Verdict |
|-----------|---------|
| Semantics | Equivalent — read/write hierarchy preserved |
| Performance | **Bad.** Depth-N path = N coordinator round-trips per operation. At a 1 ms LAN RTT and depth 5, that's +5 ms on every metadata read |
| Operational cost | New mandatory component to deploy/monitor/HA |
| Correctness pitfalls | Lease expiry under JVM GC pauses, fencing tokens, split-brain (Redis specifically) |
| Required call-site changes | None |

### 4.2 Option B — Storage-level concurrency

Delete `TreeLock`. Add a `version` column to `RelationalEntity`; rewrite mutations as
`UPDATE … SET …, version = version + 1 WHERE id = ? AND version = ?` and retry on conflict.

| Dimension | Verdict |
|-----------|---------|
| Semantics | Different — optimistic, no parent-level write locks; coarse "lock the whole schema during a rename" must be recreated via composite CAS or an explicit table-level lock |
| Performance | Excellent — zero overhead on the read path; one extra column write |
| Operational cost | None |
| Correctness pitfalls | Doesn't help the proxy paths; multi-row atomic ops need either serializable isolation or careful ordering |
| Required call-site changes | Significant — every mutation in `RelationalEntityStore` and every retry-handling caller |

### 4.3 Option C — Hybrid (recommended)

Two independent, sequenceable mechanisms:

1. **Pluggable `LockBackend` SPI** behind `LockManager`. The default `InProcessLockBackend`
   is byte-for-byte the current behavior. A new `JdbcLockBackend` reuses the relational
   backend Gravitino *already* requires — no new operational dependency. ZooKeeper / etcd
   backends slot in later via the same SPI.
2. **Optimistic CAS in `RelationalEntityStore`** (a follow-up PR — flagged here as the next
   step but not implemented in this change). Catches HA races on the metadata store even
   if no distributed lock backend is configured, and keeps catching them when one is.

This document and its companion PR introduce **(1)** end-to-end and ship a working JDBC
backend. **(2)** is staged into a follow-up because it touches every entity persister and
benefits from a separate review cycle.

---

## 5. Recommended Design

### 5.1 SPI surface

```java
package org.apache.gravitino.lock;

/** A pluggable backend that grants and releases path-keyed RW locks. */
public interface LockBackend extends Closeable {

  /**
   * Acquire a hierarchical lock on the given identifier. Implementations MUST acquire
   * locks root-down to preserve the deadlock-free total ordering already in place.
   *
   * @param identifier the resource to lock
   * @param lockType   READ or WRITE; WRITE is exclusive on the leaf only
   * @return a handle whose {@link LockHandle#close()} releases all locks acquired by
   *         this call, in reverse acquisition order
   */
  LockHandle acquire(NameIdentifier identifier, LockType lockType);

  /** Backend-specific identifier surfaced in logs and metrics. */
  String name();
}

/** Acquired-lock handle. {@code close()} is idempotent. */
public interface LockHandle extends AutoCloseable {
  @Override
  void close();
}
```

`TreeLockUtils` becomes a thin wrapper:

```java
public static <R, E extends Exception> R doWithTreeLock(
    NameIdentifier identifier, LockType lockType, Executable<R, E> executable) throws E {
  LockBackend backend = GravitinoEnv.getInstance().lockManager().backend();
  try (LockHandle handle = backend.acquire(identifier, lockType)) {
    return executable.execute();
  }
}
```

`LockManager` keeps its existing constructor signature and its existing scheduler ownership;
internally it instantiates a `LockBackend` from configuration and exposes `backend()` to
`TreeLockUtils`. The existing `TreeLock` / `TreeLockNode` classes remain intact and become
the implementation of `InProcessLockBackend`.

### 5.2 Configuration additions

| Key | Default | Purpose |
|-----|---------|---------|
| `gravitino.lock.backend.type` | `inprocess` | `inprocess` or `jdbc` |
| `gravitino.lock.backend.jdbc.url` | *(unset → reuses `gravitino.entity.store.relational.jdbcUrl`)* | JDBC URL for the lock table; allowed to be a different DB if operators want to isolate |
| `gravitino.lock.backend.jdbc.user` | *(unset → reuses entity-store user)* | Username |
| `gravitino.lock.backend.jdbc.password` | *(unset → reuses entity-store password)* | Password |
| `gravitino.lock.backend.jdbc.driver` | *(unset → reuses entity-store driver)* | JDBC driver class |
| `gravitino.lock.backend.jdbc.acquireTimeoutMs` | `30000` | Maximum wait per individual acquire; backed by `SET LOCAL lock_timeout` (Postgres) / `SET innodb_lock_wait_timeout` (MySQL) |
| `gravitino.lock.backend.jdbc.poolSize` | `8` | Dedicated connection pool — must not borrow from the entity-store pool because acquired locks hold a connection for the duration of the operation |

The existing three `gravitino.lock.maxNodes` / `minNodes` / `cleanIntervalInSecs` keys
continue to apply only to the in-process backend and are silently ignored (with a log
message at startup) when `backend.type=jdbc`.

### 5.3 Lifecycle & wiring in `GravitinoEnv`

`GravitinoEnv.initialize` already constructs `LockManager` once. The change is contained:

```java
// before
this.lockManager = new LockManager(config);

// after
this.lockManager = new LockManager(config);  // signature unchanged
// LockManager constructor now picks the backend internally
```

Shutdown ordering: `LockManager.close()` (newly added) closes the backend, which releases
any pool/connection resources. This must run before `EntityStore.close()` so that
in-flight locks held against the JDBC backend can drain cleanly.

---

## 6. JDBC Backend — Detailed Design

### 6.1 Schema

A single new table, deployed via the existing Liquibase changelog mechanism the relational
backend already uses for entity tables:

```sql
CREATE TABLE gravitino_lock (
  lock_path     VARCHAR(1024) NOT NULL,
  PRIMARY KEY (lock_path)
);
```

Rationale for this minimal schema:

- **No `lock_holder`/`acquired_at` columns are needed for correctness.** The actual lock is
  the row's `SELECT ... FOR UPDATE` row lock, which the database tracks. Adding
  observability columns (holder, txn id, acquired_at, server id) is straightforward but
  deferred — it would require connection-affinity tracking inside the backend.
- **Pre-populating rows is unnecessary.** Each `acquire` first ensures the row exists via
  `INSERT ... ON CONFLICT DO NOTHING` (Postgres / H2 v2.0+) or `INSERT IGNORE`
  (MySQL), then takes the row lock.
- **`VARCHAR(1024)`** comfortably accommodates the deepest reasonable
  metalake/catalog/schema/table/partition path. Hashing the path was considered and
  rejected — the false-contention risk from collisions outweighs the few bytes saved.

### 6.2 Acquire algorithm

For path `/metalake/catalog/db/tbl` with `WRITE`:

```
BEGIN;                                  -- new dedicated connection from JdbcLockBackend pool
SET LOCAL lock_timeout = '30s';         -- Postgres; equivalent on each dialect

-- Acquire root-down. Every ancestor takes a SHARED lock; the leaf takes the requested type.
INSERT ... ON CONFLICT DO NOTHING into gravitino_lock VALUES ('/');
SELECT 1 FROM gravitino_lock WHERE lock_path='/'                FOR SHARE;

INSERT ... ON CONFLICT DO NOTHING ... VALUES ('/metalake');
SELECT 1 FROM gravitino_lock WHERE lock_path='/metalake'        FOR SHARE;

INSERT ... ON CONFLICT DO NOTHING ... VALUES ('/metalake/catalog');
SELECT 1 FROM gravitino_lock WHERE lock_path='/metalake/catalog' FOR SHARE;

INSERT ... ON CONFLICT DO NOTHING ... VALUES ('/metalake/catalog/db');
SELECT 1 FROM gravitino_lock WHERE lock_path='/metalake/catalog/db' FOR SHARE;

INSERT ... ON CONFLICT DO NOTHING ... VALUES ('/metalake/catalog/db/tbl');
SELECT 1 FROM gravitino_lock WHERE lock_path='/metalake/catalog/db/tbl' FOR UPDATE;
-- ... operation runs while connection (and therefore the locks) are held
COMMIT;                                  -- releases all locks atomically
```

`READ` requests use `FOR SHARE` on the leaf as well; `WRITE` uses `FOR UPDATE` only on the
leaf (consistent with the in-process backend's "READ on ancestors, requested type on leaf"
convention).

### 6.3 Dialect portability

| Database | `INSERT ... IF NOT EXISTS` | Shared row lock | Exclusive row lock | Lock-wait timeout |
|----------|----------------------------|-----------------|--------------------|-------------------|
| **Postgres** | `INSERT ... ON CONFLICT DO NOTHING` | `FOR SHARE` | `FOR UPDATE` | `SET LOCAL lock_timeout = '30s'` |
| **MySQL ≥ 8.0** | `INSERT IGNORE` | `FOR SHARE` | `FOR UPDATE` | `SET innodb_lock_wait_timeout = 30` |
| **H2 ≥ 2.0** | `MERGE INTO ... KEY(...)` | `FOR SHARE`¹ | `FOR UPDATE` | `SET LOCK_TIMEOUT 30000` |

¹ H2's `FOR SHARE` is implemented but not as performant as Postgres/MySQL; the test suite
treats H2 as a developer-only dialect.

A small `JdbcDialect` enum in `JdbcLockBackend` switches the SQL strings. The dialect is
detected from the JDBC URL prefix at backend construction time (the same heuristic
`JDBCBackend` uses today).

### 6.4 Connection management

- **Dedicated pool.** Lock-holding connections must not borrow from the entity-store pool;
  they would starve it. The lock backend opens its own HikariCP pool sized via
  `gravitino.lock.backend.jdbc.poolSize` (default 8).
- **Connection-per-operation.** The acquire phase opens a connection, runs all the
  `INSERT/SELECT` statements, and stores the connection inside the returned `LockHandle`.
  `LockHandle.close()` commits the transaction and returns the connection to the pool.
- **JVM crash safety.** If the Gravitino process dies mid-operation, the database closes
  the abandoned connection, which rolls back the transaction and releases all row locks.
  This is the same property `SELECT ... FOR UPDATE` provides for ordinary application code.
- **No reentrancy guarantee in this version.** A single thread that calls
  `doWithTreeLock` recursively will get a *new* connection on the inner call and will
  block waiting for itself on the outer connection's row locks. Fixing this requires
  thread-local connection threading and is deferred to a follow-up; a defensive runtime
  check inside `JdbcLockBackend.acquire` will throw a clear error if reentry is detected.

### 6.5 Performance characteristics

For a depth-5 path, an acquire issues 5 `INSERT IF NOT EXISTS` + 5 `SELECT FOR SHARE/UPDATE`
= 10 round-trips. Against a co-located Postgres at 0.3 ms/RTT that's ≈ 3 ms of added
latency per acquire, on top of the operation's own DB work. The release is one `COMMIT`
(1 RTT, ≈ 0.3 ms).

Mitigations available *inside the same backend* without changing the SPI:

- **Leaf-only locking** when the caller opts in via a future `LockType.LEAF_ONLY` — for
  operations that don't need the parent-stability guarantee. Out of scope here.
- **Path-prefix caching.** A future revision can cache "this server has held a SHARED lock
  on `/metalake/catalog` recently" and skip re-acquiring it within a short TTL; this
  trades correctness for latency and is intentionally not part of the v1 design.
- **Batched acquire.** Wrap the per-level `INSERT/SELECT` pairs in a JDBC `addBatch()` to
  cut the round-trip count in half.

The in-process backend stays at roughly zero overhead; operators who don't enable HA pay
nothing.

### 6.6 Failure semantics

| Scenario | Behavior |
|----------|----------|
| Lock-table row contended longer than `acquireTimeoutMs` | DB throws `lock_not_available` (Postgres `55P03`) / `ER_LOCK_WAIT_TIMEOUT` (MySQL `1205`). Backend translates to `LockAcquisitionTimeoutException` (new), bubbled to caller. |
| Connection pool exhausted | Hikari throws `SQLTransientConnectionException`. Backend wraps as `LockBackendUnavailableException` (new). |
| JDBC URL unreachable at startup | Backend construction fails and `GravitinoEnv.initialize` aborts — operator sees the error immediately rather than at first request time. |
| Process crash mid-lock | DB releases the abandoned connection's row locks within its own TCP keep-alive / dead-connection-detection window (Postgres default ~ minutes; tunable). |

### 6.7 What this backend does NOT do

- It does not implement reentrant locks across recursive `doWithTreeLock` calls (see §6.4).
- It does not provide cross-node observability ("which node holds this lock?") — the lock
  table carries no holder column in v1.
- It does not solve the auth-cache staleness problem (`design-docs/cache-improvement-design.md`).
- It does not replace the in-process deadlock-warning thread; that thread remains useful
  for the in-process backend.

---

## 7. Correctness Analysis

### 7.1 Mutual exclusion under HA

Two servers attempting `WRITE` on the same path acquire the same lock-table row's
`FOR UPDATE` lock. The database serializes them. The second writer either waits up to
`acquireTimeoutMs` for the first to commit, or fails with a timeout. Reads (`FOR SHARE`)
on different paths proceed in parallel; reads on the same path also proceed in parallel
(modulo the DB's shared-lock implementation).

### 7.2 Hierarchy is preserved

Acquire order is strictly root-down on every path. Release order is reverse (achieved
implicitly by `COMMIT` releasing all row locks at once). Two servers acquiring overlapping
paths therefore acquire shared row locks in the same global order on the common ancestors
and only contend on the leaf — the same property as the in-process backend.

### 7.3 Deadlock freedom

Within a single node: same-path reentry is the only intra-server deadlock risk, and it is
detected and rejected (§6.4). Across nodes: the database's own deadlock detector handles
genuine cycles (e.g., server A holds shared lock on `/m`, server B holds exclusive on
`/m/c` and requests shared on `/m`). Postgres aborts one side with `ERROR: 40P01`; MySQL
returns `ER_LOCK_DEADLOCK`. Both are translated to a retryable `LockDeadlockException`.

### 7.4 What CAN still go wrong

- A lock acquired on server A and a long GC pause on A → the operation takes longer than
  the lock-wait timeout on server B, which gives up. This is a *liveness* failure (B
  retries), not a *safety* failure (A's mutation still happens behind its lock).
- A network partition between Gravitino and the lock DB. The in-progress operation's
  connection dies; the DB releases the lock; the operation rolls back. Other servers
  proceed. Same liveness-only outcome.
- Misconfiguration: pointing two HA Gravitino clusters at the same lock DB but different
  entity stores would cross-couple them. The deployment guide must call this out.

---

## 8. Migration Plan

The introduction is purely additive and the new backend is opt-in.

### Phase A — This PR

1. Add `LockBackend` SPI and `InProcessLockBackend` (current behavior), default-on.
2. Add `JdbcLockBackend` and configuration keys, default-off.
3. Tests: SPI contract + JDBC backend integration test against H2.
4. Document the new keys in `docs/gravitino-server-config.md`.

### Phase B — Follow-up: optimistic CAS in `RelationalEntityStore`

Add a `version` column to `RelationalEntity` and CAS-update mutations. This is the real
correctness fix for the most-common HA case and is independent of which lock backend is
configured. It belongs in its own PR with its own review cycle.

### Phase C — Follow-up: per-call-site audit

Walk the 19 `doWithTreeLock` call sites and classify each as:

- **Required for HA correctness** → keep, runs against the configured backend.
- **In-process invariant only** (e.g., guarding a non-persisted in-memory cache) → keep but
  consider moving to a local-only lock helper that explicitly bypasses the SPI.
- **Redundant given Phase B** → remove.

This cleanup reduces the number of acquires per request and is gated on Phase B landing.

### Phase D — Optional: ZooKeeper / etcd backends

If operators ask for them. The SPI is shaped to accommodate Curator's
`InterProcessReadWriteLock` directly; an etcd backend would use lease-based locks. Not
needed for the foundational HA story.

---

## 9. Compatibility

- **Existing single-node deployments.** No config change required. `gravitino.lock.backend.type`
  defaults to `inprocess`, which is the current behavior. The existing
  `gravitino.lock.{max,min}Nodes` / `cleanIntervalInSecs` keys keep their meaning.
- **Existing tests.** The full `core` test suite passes unchanged because the in-process
  backend wraps the existing `LockManager` logic with no behavioral delta.
- **Wire/HTTP API.** Unchanged. This is internal plumbing.

---

## 10. Open Questions

1. **Should the JDBC backend reuse the entity-store DataSource or always have its own pool?**
   Current design: always its own, to avoid starvation. Operators with very small DBs may
   want the option to share — exposing this would mean adding a third configuration mode
   ("share with entity store"). Defer until requested.
2. **Should `acquireTimeoutMs` cause the calling REST request to fail with `503` or be
   transparently retried?** Current design: bubble up as a typed exception to the caller
   and let the dispatcher decide. The job/scheduling dispatchers may want different
   behaviour from the user-facing dispatchers.
3. **Is there appetite for a `LockType.INTENT_SHARED`/`INTENT_EXCLUSIVE` extension** so the
   ancestor locks can be cheaper than full shared locks? Postgres advisory locks (`pg_try_advisory_xact_lock`)
   would map well, but it's only worth the additional complexity if profiling shows the
   ancestor-lock cost is the bottleneck.
4. **How aggressively should the lock table be vacuumed?** Today's design never deletes
   rows; the row count is bounded by the number of distinct metadata paths the system has
   ever locked, not by traffic. A periodic `DELETE FROM gravitino_lock WHERE lock_path NOT
   IN (... entity table paths ...)` could prune orphans, but is unnecessary for correctness.

---

## 11. Future Work

Beyond Phases B–D in §8:

- **Distributed deadlock observability.** Surface DB-detected deadlocks as Gravitino metrics.
- **Lock table holder columns** (`server_id`, `acquired_at`, `txn_id`) for ops tooling.
- **Read-side lock elision** when the operation is a pure point read of an entity that
  already supports MVCC at the storage layer.
- **Cross-region considerations.** A globally-distributed deployment would want either a
  geo-replicated DB or a Raft-based lock service; out of scope for now.
