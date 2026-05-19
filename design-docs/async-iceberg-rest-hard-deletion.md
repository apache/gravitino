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

# Design: Pluggable Asynchronous Hard Deletion for the Gravitino Iceberg REST Server

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

We want to return quickly, finish deletion reliably in the background,
survive restarts, and let operators plug in alternative strategies
(object-store batch APIs, external job systems, audit-only) without
modifying Gravitino.

*Not in scope:* `RelationalGarbageCollector`, which deletes tombstoned
**rows** from Gravitino's relational backend. Different IO surface,
different failure model — kept separate.

## 2. Goals

1. `DELETE … ?purgeRequested=true` returns at typical request latency
   (target p99 < 500 ms) regardless of table size, when an async purger
   is configured.
2. File-deletion strategy is **pluggable** behind an `IcebergPurger` SPI.
3. The default async implementation deletes every file the synchronous
   purge would have deleted, retries transient failures, and survives
   restarts.
4. No change to the Iceberg REST wire protocol.
5. Authorization runs on the **request thread**, never deferred.

## 3. Non-Goals

- Changing Gravitino-native soft-delete semantics.
- User-initiated cancellation of in-flight purges (v1).
- Async `dropNamespace` / `dropView` (they don't delete data today).

## 4. Overview

```
                           IcebergPurger (SPI)
                                 ▲
        ┌────────────────────────┼────────────────────────────┐
        │                        │                            │
SynchronousIceberg-       JdbcAsyncIceberg-          (third-party plugins:
   Purger                    Purger                   Kafka, S3 Batch,
(legacy parity)          (default async)              audit-only, …)
```

`IcebergTableOperationExecutor.dropTable` no longer knows how purge is
implemented — it calls `purger.purgeTable(request)` and the configured
plugin decides whether the work is synchronous, queued in a DB,
dispatched externally, or skipped.

The default plugin (`JdbcAsyncIcebergPurger`) persists a job row, returns
immediately, and drains the queue from a background worker pool.

## 5. The `IcebergPurger` SPI

### 5.1 Interface

Mirrors `IcebergConfigProvider`
(`iceberg-rest-server/.../service/provider/IcebergConfigProvider.java`)
for consistency.

```java
package org.apache.gravitino.iceberg.service.purge;

public interface IcebergPurger extends Closeable {

  /** Initialize the purger. Properties are stripped of the
   *  {@code gravitino.iceberg-rest.purger.} prefix. */
  void initialize(Map<String, String> properties, IcebergPurgerContext context);

  /** Called on the REST request thread, after authorization and after the
   *  catalog entry has been removed. The implementation either performs
   *  the file deletion inline or accepts responsibility for completing it
   *  later. Throwing surfaces as a 5xx. */
  void purgeTable(IcebergPurgeRequest request);

  String name();
}
```

```java
public final class IcebergPurgeRequest {
  String metalakeName, catalogName, requestUser, requestId, fileIoImpl;
  TableIdentifier tableIdentifier;
  TableMetadata tableMetadata;            // loaded by the server
  Map<String, String> fileIoProperties;
  long requestTimestampMs;
}

public interface IcebergPurgerContext {
  FileIO newFileIo(String impl, Map<String, String> properties);
  Optional<RelationalBackend> relationalBackend();
  EventListenerManager eventListenerManager();
  String serverIdentity();   // host:pid
}
```

The server loads `TableMetadata` once and hands it to the plugin so
every implementation sees a consistent snapshot regardless of when it
acts. `fileIoProperties` is captured at request time so deferred work
can reconstruct `FileIO` even if the catalog is later reconfigured.

### 5.2 Discovery

Modeled on `IcebergConfigProviderFactory`:

```java
private static final Map<String, String> BUILTINS = ImmutableMap.of(
    "synchronous", SynchronousIcebergPurger.class.getCanonicalName(),
    "jdbc-async",  JdbcAsyncIcebergPurger.class.getCanonicalName());

public static IcebergPurger create(Map<String, String> props,
                                   IcebergPurgerContext ctx) {
  String selector = new IcebergConfig(props).get(IcebergConfig.ICEBERG_REST_PURGER);
  String className = BUILTINS.getOrDefault(selector, selector);
  IcebergPurger purger = (IcebergPurger)
      Class.forName(className).getDeclaredConstructor().newInstance();
  purger.initialize(stripPrefix(props, "gravitino.iceberg-rest.purger."), ctx);
  return purger;
}
```

Built in `RESTService.serviceInit` right after the `IcebergConfigProvider`.

### 5.3 Built-in providers

**`SynchronousIcebergPurger`** — wraps `CatalogUtil.dropTableData(io, meta)`.
Today's behavior, repackaged. Phase-1 default; remains the documented
fallback after the default flips.

**`JdbcAsyncIcebergPurger`** — persists an `iceberg_purge_job` row and
returns. A worker pool leases jobs via `SELECT … FOR UPDATE SKIP LOCKED`,
walks the metadata, deletes files, and retries with exponential backoff
up to `max_attempts`. Becomes the default once stable. Detailed in §6.

### 5.4 Third-party plugins

Drop a jar with a class implementing `IcebergPurger` (no-arg constructor)
onto the classpath and set:

```
gravitino.iceberg-rest.purger = com.example.S3BatchIcebergPurger
gravitino.iceberg-rest.purger.s3-batch.account-id = 123456789012
```

Expected uses: emit Kafka events for downstream cleanup, create S3 Batch
Operations jobs from the manifest list, or record intent in an audit
system without deleting.

## 6. Default implementation: `JdbcAsyncIcebergPurger`

### 6.1 Request-path interaction

```java
public void dropTable(IcebergRequestContext ctx, TableIdentifier id,
                      boolean purgeRequested) {
  IcebergCatalogWrapper w = catalogWrapperManager.getCatalogWrapper(ctx.catalogName());
  if (!purgeRequested) { w.dropTable(id); return; }

  TableMetadata metadata = w.loadTableMetadata(id);
  w.dropTable(id);          // metadata-only drop in the catalog
  purger.purgeTable(
      IcebergPurgeRequest.builder()
          .catalogName(ctx.catalogName())
          .tableIdentifier(id)
          .tableMetadata(metadata)
          .fileIoImpl(w.fileIoImpl())
          .fileIoProperties(w.fileIoProperties())
          .requestUser(ctx.userPrincipal())
          .build());
}
```

Order matters: load metadata → drop catalog entry → call the purger. A
purge job exists only for a table that is already gone from the catalog.

### 6.2 Schema — `iceberg_purge_job`

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

### 6.3 Worker pool

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

### 6.4 Failure model

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

## 7. Events

Existing `IcebergDropTableEvent` continues to fire on the REST thread
with the *requested* purge flag, preserving today's listener contract.
Plugins emit, via `IcebergPurgerContext.eventListenerManager()`:

- `IcebergPurgeStartedEvent` — work begins on a request.
- `IcebergPurgeCompletedEvent` — files deleted, with elapsed time.
- `IcebergPurgeFailedEvent` — dead-lettered job.

Third-party purgers are expected to fire the same events.

## 8. Configuration

### 8.1 SPI selection

| Key                              | Default       | Description                                                                                          |
| -------------------------------- | ------------- | ---------------------------------------------------------------------------------------------------- |
| `gravitino.iceberg-rest.purger`  | `synchronous` (initially) → `jdbc-async` (after phase 3) | Short alias or FQCN of an `IcebergPurger` implementation. |

### 8.2 `jdbc-async` tunables

| Key                                                           | Default   |
| ------------------------------------------------------------- | --------- |
| `…purger.jdbc-async.worker-threads`                           | `4`       |
| `…purger.jdbc-async.delete-threads-per-job`                   | `8`       |
| `…purger.jdbc-async.poll-interval-ms`                         | `5000`    |
| `…purger.jdbc-async.batch-size`                               | `16`      |
| `…purger.jdbc-async.lease-timeout-ms`                         | `300000`  |
| `…purger.jdbc-async.max-attempts`                             | `5`       |
| `…purger.jdbc-async.backoff-base-ms`                          | `30000`   |
| `…purger.jdbc-async.backoff-max-ms`                           | `3600000` |
| `…purger.jdbc-async.completed-retention-hours`                | `168`     |

`synchronous` has no tunables.

## 9. Wire compatibility

The Iceberg REST spec does not require file deletion to be complete
before the response. With this design:

- The catalog entry is removed before `204`, so `HEAD` returns `404`,
  `LIST` no longer includes the table, and `CREATE` at the same
  identifier succeeds immediately.
- Object-store files may linger until the worker drains. Documented in
  release notes.

Operators needing strict synchronous deletion set
`gravitino.iceberg-rest.purger = synchronous`.

## 10. Security

- `@AuthorizationExpression` on `dropTable` runs on the request thread,
  unchanged.
- Plugins use FileIO credentials snapshotted at request time. Plugins
  that defer past credential lifetime (STS tokens, …) must refresh or
  require static credentials — a documented plugin responsibility.
- `iceberg_purge_job` may contain credentials in `file_io_props`; the
  existing Gravitino DB encryption / access controls apply.

## 11. Rollout

1. SPI + `SynchronousIcebergPurger` (parity with today). Default
   `synchronous`. No behavior change.
2. Land `JdbcAsyncIcebergPurger` + schema migration behind opt-in.
3. Flip default to `jdbc-async`. `synchronous` stays as fallback.
4. Document the SPI as a public extension point.

## 12. Testing

- Unit (`./gradlew :iceberg:iceberg-rest-server:test -PskipITs`):
  - `TestIcebergPurgerFactory` — alias / FQCN resolution.
  - `TestSynchronousIcebergPurger` — delegates to `CatalogUtil.dropTableData`.
  - `TestJdbcAsyncIcebergPurgerStateMachine` — PENDING → RUNNING →
    SUCCEEDED; failure → retry → DEAD_LETTER.
  - `TestJdbcAsyncIcebergPurgerWorker` — leasing, renewal, contention
    (H2 backend).
  - `TestIcebergTableOperationExecutorPluggable` — `dropTable` invokes
    `purger.purgeTable` exactly once; thrown errors surface as 5xx.
- Integration (`gravitino-docker-test`):
  - Default plugin: drop a table; REST returns < 500 ms; worker
    eventually clears every file.
  - Restart REST mid-purge (kill -9); purge resumes.
  - Two replicas on one DB; no duplicate deletions or orphans.
  - `synchronous`: existing `IcebergRESTServiceIT` reruns unchanged.

## 13. Alternatives

**Hard-code "JDBC async."** Earlier draft. Bakes one operational model
into every deployment. The SPI form is strictly more general for the
same core complexity.

**ServiceLoader instead of FQCN config.** The codebase precedent
(`IcebergConfigProviderFactory`) uses FQCN with aliases. We follow that.

**Reuse `RelationalGarbageCollector`.** Different IO surface (object
store vs JDBC), different failure model. Share patterns, not code.

**External job system (Quartz / Temporal) as the only path.** Heavy
operational burden. Operators who want one write a plugin.

**Enumerate files at enqueue time.** Defeats the latency goal; bloats
rows.

## 14. Open questions

1. **Multi-server coordination on H2** — fall back to advisory locks or
   conditional updates? Need a decision before implementation.
2. **Credential refresh in `jdbc-async`** — refresh from the catalog at
   lease-renewal time, or require static credentials for catalogs that
   opt into async purge?
3. **Admin visibility** — expose an in-flight-jobs read endpoint? If
   yes, on the SPI (`IcebergPurger#describeInFlight()`) or only on the
   default plugin?
4. **SPI stability** — mark `@Evolving` until at least one third-party
   plugin is in production?
5. **Namespace / view purge on the SPI in v1** — adding
   `purgeNamespace` / `purgeView` as default-no-op methods now avoids a
   binary break later.
