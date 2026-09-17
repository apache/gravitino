<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied. See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Design of Multi-Table Atomic Commit in Gravitino Iceberg REST Catalog

Tracking issue: [#10674](https://github.com/apache/gravitino/issues/10674)
Prior implementation attempt: [#10675](https://github.com/apache/gravitino/pull/10675)

---

## 1. Background

### 1.1 Current State

The Iceberg REST Catalog (IRC) specification defines a multi-table commit endpoint:

```
POST /v1/{prefix}/transactions/commit
Body: CommitTransactionRequest { table-changes: [ UpdateTableRequest, ... ] }
Response: 204 No Content
```

Gravitino's IRC server (`iceberg/iceberg-rest-server`) does **not** implement this endpoint. The
`/v1/config` response built by
[`IcebergConfigOperations`](../iceberg/iceberg-rest-server/src/main/java/org/apache/gravitino/iceberg/service/rest/IcebergConfigOperations.java)
advertises 18 table/namespace endpoints plus 8 view endpoints, and `Endpoint.V1_COMMIT_TRANSACTION`
is absent from both lists. Clients therefore see the endpoint as unsupported.

The consequence is that engines cannot perform cross-table atomic DDL/DML against Gravitino. Iceberg's
own client refuses to even try: `RESTSessionCatalog.commitTransaction` opens with
`Endpoint.check(endpoints, Endpoint.V1_COMMIT_TRANSACTION)` and throws before issuing any HTTP call
when the server does not advertise the endpoint. Spark's `MERGE`-style multi-table statements and
Flink's multi-sink jobs fall back to per-table commits, losing the all-or-nothing guarantee the
Iceberg spec promises.

### 1.2 Why the First Attempt Was Not Sufficient

PR #10675 implemented the endpoint by calling `IcebergCatalogWrapper.updateTable()` once per table
change. Review ([#10675 comments](https://github.com/apache/gravitino/pull/10675#issuecomment-4186297707))
identified two blocking problems, and a revised two-phase `validate-then-commit` variant did not
fully resolve either.

**Problem 1 - Commit phase is not atomic.**

Iceberg's per-table commit primitive is `TableOperations.commit(base, updated)`. Each call performs
an independent compare-and-swap on exactly one table's `metadata_location` pointer. Validating all
requirements before committing any table removes the "requirement fails after partial commit" class
of failure, but the commit loop itself remains N independent CAS operations:

```
Phase 1: validate all N tables      -> OK
Phase 2: commit table 1             -> OK   (durably visible to all readers)
         commit table 2             -> FAIL (concurrent writer won the CAS)
         => table 1 committed, table 2 not. Partial state. Client sees an error
            but cannot tell which tables landed.
```

Iceberg's own reference server has the identical limitation and documents it explicitly in
[`RESTCatalogAdapter`](https://github.com/apache/iceberg/blob/main/core/src/test/java/org/apache/iceberg/rest/RESTCatalogAdapter.java#L554-L556).

**Problem 2 - Privilege escalation.**

`IcebergTableOperations.updateTable` is guarded by an annotation-driven expression that includes a
table-level term:

```java
// IcebergTableOperations.java:208-213 - updateTable
"ANY(OWNER, METALAKE, CATALOG) || SCHEMA_OWNER_WITH_USE_CATALOG || "
  + "ANY_USE_CATALOG && ANY_USE_SCHEMA && (TABLE::OWNER || ANY_MODIFY_TABLE)"
```

The `@AuthorizationExpression` interceptor resolves `TABLE` from an `@AuthorizationMetadata`-annotated
**path parameter**. `POST /v1/{prefix}/transactions/commit` carries no table in its path - the table
identifiers live in the request **body**. A naive port of the annotation therefore degrades to a
catalog-level check, letting a principal holding only `USE_CATALOG` modify tables it could not touch
through `updateTable`.

### 1.3 Backend Capability Is Not Uniform

The central constraint, and the reason a single implementation cannot serve all deployments: the
atomicity achievable depends entirely on the configured backend, because the backend owns the
metadata pointer.

Gravitino supports five backends
([`IcebergCatalogBackend`](../catalogs/catalog-common/src/main/java/org/apache/gravitino/catalog/lakehouse/iceberg/IcebergCatalogBackend.java)).
Verified against Iceberg 1.11.0:

| Backend | Pointer store | Multi-table atomic commit | Evidence |
|---------|---------------|---------------------------|----------|
| `JDBC` | `iceberg_tables` row, column `metadata_location` | **Achievable in Gravitino** | `JdbcTableOperations.doCommit` reduces to a conditional `UPDATE ... WHERE metadata_location = :old`. N such statements on one `Connection` inside one DB transaction commit or roll back together. |
| `REST` | Owned by the upstream catalog | **Achievable by delegation** | The transaction can be forwarded upstream as a single `POST /v1/{prefix}/transactions/commit`, so the guarantee equals the upstream's. Note that `RESTCatalog.commitTransaction(List<TableCommit>)` is *not* a faithful forward and is not used here, for the reason given in §5.4. |
| `HIVE` | HMS table parameter | **Not achievable** | `HiveTableOperations.doCommit` issues a per-table `alter_table` (via `MetastoreUtil.alterTable`, HMS Thrift `alter_table_with_environmentContext`), each its own server-side transaction, guarded by `MetastoreLock` or `NoLock`. HMS exposes no multi-table transactional alter. |
| `MEMORY` | In-process map | **Achievable** (JVM lock) | Test-only backend. |
| `CUSTOM` | Arbitrary | **Unknown** | Arbitrary `Catalog` implementation; no capability contract exists. |

On `HIVE`, acquiring `MetastoreLock` on all N tables before committing removes concurrent-writer
races but does not make the alters atomic: a failure after the third of five `alter_table` calls
still leaves three tables advanced. The strongest honest description is "serialized best-effort",
which is *not* the semantics `commitTransaction` promises.

The problem this design must solve is therefore not only "how do we commit N tables atomically" but
**"how does the server behave when the configured backend cannot"**.

---

## 2. Goals

1. **Atomic commit on JDBC backend**: On a `JDBC` backend, a `CommitTransactionRequest` covering N
   tables either advances all N `metadata_location` pointers or advances none. Verifiable by an
   integration test that injects a CAS failure on table N and asserts tables 1..N-1 are unchanged.
2. **Faithful delegation on REST backend**: On a `REST` backend, Gravitino forwards the client's
   `CommitTransactionRequest` body unmodified to the upstream catalog, preserving both the caller's
   identity and the caller's own `UpdateRequirement`s. Verifiable by asserting Gravitino issues
   exactly one upstream `POST /v1/{prefix}/transactions/commit` whose body is byte-for-byte the
   inbound body, not N `POST .../tables/{table}` calls.
3. **Honest capability advertisement**: `Endpoint.V1_COMMIT_TRANSACTION` appears in the `/v1/config`
   response if and only if the resolved backend can commit atomically. Verifiable per backend by
   asserting endpoint presence in the `ConfigResponse`.
4. **No silent degradation**: When the endpoint is invoked on a backend that cannot commit atomically,
   the server returns `501 Not Implemented` with a message naming the backend, and modifies zero
   tables. Verifiable by a `HIVE`-backend test asserting 501 and unchanged metadata locations.
5. **Authorization parity with `updateTable`**: Every table in a `CommitTransactionRequest` is
   authorized against the same expression `updateTable` uses, before any table is modified.
   Verifiable by a test where a principal holds `MODIFY_TABLE` on table A but not table B and the
   whole request is rejected `403` with table A unchanged.
6. **Observability parity**: The operation emits pre/success/failure events and an audit log entry
   carrying every table identifier in the transaction, consistent with the existing
   `IcebergTableEventDispatcher` chain.

---

## 3. Non-Goals

1. **Cross-catalog transactions**: The Iceberg spec scopes `CommitTransactionRequest` to one
   `{prefix}`, i.e. one Gravitino catalog. Committing across two catalogs (or two backends) requires
   distributed 2PC and is out of scope.
2. **Atomicity for the `HIVE` backend**: HMS Thrift offers no multi-table transactional alter. Adding
   it would require an HMS-side change (upstream Hive) or Gravitino taking ownership of Iceberg
   pointers away from HMS. Out of scope; the backend returns 501.
3. **Atomicity for the `CUSTOM` backend**: No capability contract exists for arbitrary `Catalog`
   implementations. A future opt-in SPI is possible but is not part of this design; `CUSTOM` returns
   501 unless the user's `Catalog` implements one of the capability interfaces introduced here.
4. **Metadata-file cleanup on rollback**: A rolled-back transaction leaves the metadata JSON files
   written during phase 1 unreferenced in object storage. This is identical to any failed
   single-table Iceberg commit and is handled by normal table maintenance (`RemoveOrphanFiles`). No
   new cleanup mechanism is introduced.
5. **View transactions**: The spec's `CommitTransactionRequest` covers tables only. Multi-view or
   mixed table/view transactions are out of scope.
6. **Polaris-style staged metadata workspace**: Staging all metadata in a Gravitino-owned entity
   store and flipping pointers there (see §4, Option D) would give uniform semantics across backends
   but requires Gravitino to become the authoritative pointer store. That is a separate, much larger
   design.

---

## 4. Solution Investigations

### Option A: Sequential `updateTable` per table

What PR #10675 originally did: loop over `table-changes`, call
`IcebergCatalogWrapper.updateTable()` for each.

**Pros:** Trivial; no new backend coupling; works on every backend.
**Cons:** Not atomic in any sense. A requirement failure on table 3 leaves tables 1 and 2 committed.
Advertising `V1_COMMIT_TRANSACTION` while providing these semantics actively misleads clients, which
are entitled to assume all-or-nothing and may skip their own compensation logic.
**Decision:** Rejected. Weaker than what clients get by issuing N `updateTable` calls themselves,
because it hides the partial-failure boundary.

### Option B: Two-phase validate-then-commit through `TableOperations`

The revised PR #10675 approach: load every table's `TableOperations`, validate all
`UpdateRequirement`s and build every new `TableMetadata` (phase 1), then loop
`TableOperations.commit(base, updated)` (phase 2).

**Pros:** Eliminates the most common failure mode (a stale requirement) before any write. Backend-agnostic.
**Cons:** Phase 2 is still N independent CAS operations. A concurrent writer that wins the CAS on
table k, or any I/O error, produces exactly the partial state Option A produces. The window is
narrower, not closed. It also cannot be honestly advertised as atomic.
**Decision:** Rejected as the *sole* mechanism. Phase 1 is retained as a component of the chosen
design, because validating before writing is valuable regardless of how phase 2 commits.

### Option C: Backend-capability dispatch (Chosen)

Introduce an explicit capability. Implement a genuinely atomic commit path for each backend that can
support one - batched CAS in a single DB transaction for `JDBC`, request pass-through for `REST`,
in-process lock for `MEMORY` - and refuse the operation on backends that cannot. Gate `/v1/config`
advertisement on the capability.

**Pros:** Every advertised guarantee is real. Clients discover support through the standard
`/v1/config` mechanism they already use. Follows the precedent already in the codebase:
`IcebergConfigOperations.getEndpoints(supportsViewOperations)` already varies the advertised endpoint
set by catalog capability, and `IcebergCatalogWrapper` already exposes `isRESTCatalog()`. The `JDBC`
implementation has an established home: `JdbcCatalogWithMetadataLocationSupport` already lives in
package `org.apache.iceberg.jdbc` precisely to reach `JdbcUtil` and `JdbcClientPool`.
**Cons:** Behaviour differs by deployment - a `HIVE` deployment gets 501 where a `JDBC` deployment
succeeds. Requires per-backend code rather than one shared path.
**Decision:** **Chosen.** The behavioural difference is not introduced by this design; it is inherent
to the backends. Making it explicit and discoverable is strictly better than papering over it with
best-effort semantics.

### Option D: Gravitino-owned staged metadata pointers (Polaris-style)

Gravitino's entity store becomes the authoritative holder of Iceberg metadata pointers. A transaction
stages all metadata changes, then a single batched CAS in Gravitino's relational store flips every
pointer together - uniform semantics across all backends, as
[discussed on the PR](https://github.com/apache/gravitino/pull/10675#issuecomment-4205268980).

**Pros:** Uniform atomicity independent of backend. Opens the door to other cross-table features.
**Cons:** Requires Gravitino to own the pointer, which contradicts the current architecture where
HMS or the upstream REST catalog remains authoritative. Any engine bypassing Gravitino and writing
through HMS directly would diverge from Gravitino's pointer, silently corrupting the table. Also a
schema change plus a migration path for existing catalogs.
**Decision:** Rejected for this design; recorded as a possible long-term direction. It is a change to
Gravitino's ownership model, not an implementation of one REST endpoint, and should not block the
endpoint on backends that can already support it.

---

## 5. Proposal

### 5.1 Capability Interface

There are two genuinely different ways a backend can be atomic, and conflating them into one method
would force an unnatural signature on both. They are modelled as two interfaces in
`org.apache.gravitino.iceberg.common.transaction`, sharing a common capability probe.

```java
/** Common capability probe for backends that can serve a multi-table transaction. */
public interface SupportsMultiTableTransaction {

  /**
   * Whether multi-table transaction support is actually available on this instance. Implementations
   * whose support depends on a runtime property - an upstream catalog's advertised endpoints, or a
   * JDBC driver's transaction support - override this.
   */
  default boolean isMultiTableTransactionAvailable() {
    return true;
  }
}
```

**Strategy 1 - Gravitino commits locally.** Gravitino owns phase 1 (validate, write metadata) and
asks the backend only to swap N pointers together. Used by `JDBC` and `MEMORY`.

```java
/** A backend that can swap the metadata pointers of multiple tables atomically. */
public interface SupportsAtomicMultiTableCommit extends SupportsMultiTableTransaction {

  /**
   * Atomically swaps the metadata pointer of every table in {@code commits}. Either all pointers
   * advance or none do.
   *
   * @param commits per-table base and target metadata, in request order
   * @throws org.apache.iceberg.exceptions.CommitFailedException if any table's base metadata no
   *     longer matches, in which case no table has been modified
   */
  void commitAtomically(List<TableMetadataCommit> commits);
}

/** Base and target metadata for one table within a multi-table commit. */
public final class TableMetadataCommit {
  private final TableIdentifier identifier;
  private final TableMetadata base;
  private final TableMetadata updated;
  // constructor + accessors; immutable
}
```

**Strategy 2 - Gravitino delegates.** Gravitino performs no phase 1 and forwards the client's request
to a backend that implements the transaction itself. Used by `REST`.

```java
/** A backend that can execute a whole Iceberg REST commit-transaction request itself. */
public interface SupportsTransactionForwarding extends SupportsMultiTableTransaction {

  /**
   * Forwards {@code request} unmodified to the backing catalog.
   *
   * @throws org.apache.iceberg.exceptions.CommitFailedException if the backing catalog rejects the
   *     transaction, in which case no table has been modified
   */
  void commitTransaction(CommitTransactionRequest request);
}
```

`IcebergCatalogWrapper` exposes one capability question to callers, without leaking which strategy
applies:

```java
// IcebergCatalogWrapper
public boolean supportsAtomicMultiTableCommit() {
  Catalog catalog = getCatalog();
  return catalog instanceof SupportsMultiTableTransaction
      && ((SupportsMultiTableTransaction) catalog).isMultiTableTransactionAvailable();
}
```

Implementations:

| Backend | Implementing class | Strategy |
|---------|--------------------|----------|
| `JDBC` | `JdbcCatalogWithMetadataLocationSupport` (existing, in `org.apache.iceberg.jdbc`) | 1 - batched CAS in one DB transaction (§5.3). Overrides the probe to reflect `supportsTransactions()`. |
| `REST` | `ForwardingRESTCatalog` (new subclass of `RESTCatalog`) | 2 - raw request forwarded upstream (§5.4). Overrides the probe to reflect the upstream's advertised endpoints. |
| `MEMORY` | New `InMemoryCatalogWithAtomicCommit` | 1 - single `synchronized` block over the backing map. |
| `HIVE` | Not implemented | Neither interface; capability is `false`, endpoint returns 501. |
| `CUSTOM` | Not implemented | `false` unless the user's `Catalog` implements one of these interfaces. |

### 5.2 REST API

#### `POST /v1/{prefix}/transactions/commit`

**Request** - Iceberg `CommitTransactionRequest`, unchanged from the spec:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `table-changes` | array of `UpdateTableRequest` | yes | One entry per table. Each carries `identifier`, `requirements`, `updates`. |
| `table-changes[].identifier` | `TableIdentifier` | yes | Namespace + name. Must resolve within `{prefix}`. |
| `table-changes[].requirements` | array of `UpdateRequirement` | yes | Preconditions. Validated by Gravitino in phase 1 on the `JDBC`/`MEMORY` paths, or by the upstream catalog on the `REST` path. |
| `table-changes[].updates` | array of `MetadataUpdate` | yes | Changes applied to build the target metadata. |

**Response** - `204 No Content`, no body.

**Errors:**

| Status | Condition |
|--------|-----------|
| `400 Bad Request` | Empty `table-changes`; two entries for the same table identifier; identifier outside `{prefix}`. |
| `403 Forbidden` | The principal fails the authorization expression for at least one table (§5.5). |
| `404 Not Found` | Any table in the request does not exist. |
| `409 Conflict` | A requirement no longer holds, or a CAS lost to a concurrent writer. **No table modified.** |
| `501 Not Implemented` | The backend cannot commit atomically - an incapable backend such as `HIVE`, or a `REST` backend whose upstream does not advertise `V1_COMMIT_TRANSACTION`. Message names the backend. **No table modified.** |

**Behavior:** All-or-nothing. On any non-2xx response the caller may assume every table is at the
metadata location it held before the request, with the single documented exception in §5.7.

#### `GET /v1/config` (changed)

**Old behavior:** the endpoint list is `DEFAULT_ENDPOINTS`, plus `DEFAULT_VIEW_ENDPOINTS` when
`catalogWrapper.supportsViewOperations()`.

**New behavior:** `Endpoint.V1_COMMIT_TRANSACTION` is appended when
`catalogWrapper.supportsAtomicMultiTableCommit()`.

```java
private List<Endpoint> getEndpoints(boolean supportsView, boolean supportsAtomicCommit) {
  Stream<Endpoint> endpoints = DEFAULT_ENDPOINTS.stream();
  if (supportsView) {
    endpoints = Stream.concat(endpoints, DEFAULT_VIEW_ENDPOINTS.stream());
  }
  if (supportsAtomicCommit) {
    endpoints = Stream.concat(endpoints, Stream.of(Endpoint.V1_COMMIT_TRANSACTION));
  }
  return endpoints.collect(Collectors.toList());
}
```

**Migration impact:** Additive. Existing clients ignore unknown endpoint entries. `HIVE` deployments
see no change at all. This mirrors how view support is already advertised, so no new client-side
mechanism is required.

### 5.3 JDBC Backend: Batched CAS in One DB Transaction

`JdbcUtil.updateTable(...)` cannot be reused directly. It is `static` and package-private - reachable
from `JdbcCatalogWithMetadataLocationSupport`, which already calls it in `overwriteMetadataLocation`
 - but it internally borrows its own `Connection` from the pool per invocation and runs with
autocommit. N calls are therefore N transactions.

The composable primitive is `JdbcClientPool.run(Action)`, which hands one `Connection` to a lambda
for its whole duration. All CAS statements are issued on that single connection:

```java
// JdbcCatalogWithMetadataLocationSupport implements SupportsAtomicMultiTableCommit
@Override
public void commitAtomically(List<TableMetadataCommit> commits) {
  try {
    jdbcConnections.run(conn -> {
      boolean autoCommit = conn.getAutoCommit();
      conn.setAutoCommit(false);
      try {
        for (TableMetadataCommit commit : commits) {
          int rows = casMetadataLocation(conn, commit);   // UPDATE ... WHERE metadata_location = ?
          if (rows != 1) {
            throw new CommitFailedException(
                "Concurrent modification of table %s in catalog %s",
                commit.identifier(), jdbcCatalogName);
          }
        }
        conn.commit();
        return null;
      } catch (Exception e) {
        conn.rollback();
        throw e;
      } finally {
        conn.setAutoCommit(autoCommit);
      }
    });
  } catch (SQLException e) {
    throw new UncheckedSQLException(e, "Failed to commit multi-table transaction");
  } catch (InterruptedException e) {
    Thread.currentThread().interrupt();
    throw new UncheckedInterruptedException(e, "Interrupted during multi-table commit");
  }
}
```

`casMetadataLocation` issues the schema-appropriate statement. The V0/V1 distinction matters: V1 adds
an `iceberg_type` discriminator column so tables and views can share `iceberg_tables`, and Gravitino
already defaults to V1 (`IcebergCatalogUtil.loadJdbcCatalog` sets
`ICEBERG_JDBC_SCHEMA_VERSION=V1`). `jdbcSchemaVersion` is already resolved and cached in
`JdbcCatalogWithMetadataLocationSupport.loadFields()`.

```sql
-- V1 (iceberg_type discriminator present)
UPDATE iceberg_tables
   SET metadata_location = ?, previous_metadata_location = ?
 WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?
   AND metadata_location = ?
   AND (iceberg_type = 'TABLE' OR iceberg_type IS NULL)

-- V0 (no discriminator column)
UPDATE iceberg_tables
   SET metadata_location = ?, previous_metadata_location = ?
 WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?
   AND metadata_location = ?
```

Both mirror `JdbcUtil.V1_DO_COMMIT_TABLE_SQL` / `V0_DO_COMMIT_SQL` exactly, including the
`previous_metadata_location` write, so a table's commit history stays consistent with what a
single-table commit through `TableOperations` would have produced.

Those constants are `private`, so Gravitino must restate the SQL rather than reference it.
**Risk:** the statement becomes duplicated knowledge and could drift if upstream changes the catalog
schema. Mitigations, in order of preference:

1. Propose upstream that `JdbcCatalog` expose a batch commit primitive, or that `JdbcUtil`'s commit
   SQL constants become package-visible for reuse. If accepted, Gravitino deletes its copy.
2. Until then, a unit test asserts Gravitino's statement is equivalent to the effect of
   `JdbcUtil.updateTable` on a real DB, so an upstream schema change fails the build rather than
   silently corrupting commits.

**DB support.** The pattern requires transactional DDL-free multi-statement transactions, available
in PostgreSQL, MySQL/InnoDB, and SQLite. It is not available on backends without transaction support.
`commitAtomically` probes `conn.getMetaData().supportsTransactions()` once at initialization and the
capability reports `false` when unsupported, so such a deployment cleanly returns 501 rather than
silently degrading.

**Metadata cache.** `IcebergCatalogWrapper.updateTable` currently invalidates and then repopulates
`getMetadataCache()` around each commit. Because `commitAtomically` bypasses `TableOperations`, the
transaction executor must drive the cache explicitly: invalidate all N entries before the swap, then
repopulate all N from the committed metadata after `conn.commit()` returns. Invalidating first means a
crash between swap and repopulate leaves a cold cache, not a stale one.

### 5.4 REST Backend: Pass-Through

For a `REST` backend Gravitino must not decompose the request into N `updateTable` calls. But the
obvious API is not a faithful forward, and the distinction determines the design.

**Why `RESTCatalog.commitTransaction` is not a raw forward.** Its parameter is
`List<TableCommit>`, and the only factory is
`TableCommit.create(TableIdentifier, TableMetadata base, TableMetadata updated)`. That factory
*derives* the wire payload rather than accepting it:

```
TableCommit.create(ident, base, updated)
  -> requirements = UpdateRequirements.forUpdateTable(base, updated.changes())
  -> updates      = updated.changes()
```

So the client's own `UpdateRequirement`s are discarded and replaced with requirements Gravitino
computes. Using it would also force Gravitino to load every table from upstream first (to obtain
`base`) and rebuild `updated`, adding N round trips and a TOCTOU window between that load and the
commit.

#### Option 5.4-a: Rebuild via `RESTCatalog.commitTransaction`

Load each table from upstream, apply the client's updates, hand the resulting
`(identifier, base, updated)` triples to `RESTCatalog.commitTransaction`.

**Pros:** Uses only public Iceberg API; no subclassing.
**Cons:** Client requirements are substituted by Gravitino-derived ones. The net guard is still sound
 - Gravitino validates the client's requirements against `base` in phase 1, and the derived
requirements assert upstream is still exactly at `base` - but the semantics are no longer the
client's. Costs N extra loads. **Rejected.**

#### Option 5.4-b: Forward the raw request body (Chosen)

`RESTCatalog` has a public constructor taking `Function<Map<String, String>, RESTClient>`. A thin
Gravitino subclass captures the `RESTClient` that function produces and POSTs the client's
`CommitTransactionRequest` unmodified to `ResourcePaths.commitTransaction()`:

```java
// org.apache.gravitino.iceberg.common.ops.ForwardingRESTCatalog
public class ForwardingRESTCatalog extends RESTCatalog
    implements SupportsTransactionForwarding {

  private RESTClient restClient;   // captured from the client-builder function
  private Set<Endpoint> upstreamEndpoints;

  @Override
  public boolean isMultiTableTransactionAvailable() {
    return upstreamEndpoints.contains(Endpoint.V1_COMMIT_TRANSACTION);
  }

  @Override
  public void commitTransaction(CommitTransactionRequest request) {
    Endpoint.check(upstreamEndpoints, Endpoint.V1_COMMIT_TRANSACTION);
    restClient.post(
        ResourcePaths.forCatalogProperties(properties).commitTransaction(),
        request,                     // forwarded byte-for-byte
        null,
        this::authHeaders,
        ErrorHandlers.tableCommitHandler());
  }
}
```

**Pros:** One upstream round trip. The client's requirements and updates reach the upstream catalog
exactly as written, so the upstream enforces the client's intended preconditions and Gravitino adds
no semantics of its own. Atomicity is precisely the upstream's.
**Cons:** Requires a Gravitino subclass of `RESTCatalog` and depends on `RESTClient` (public but
lower-level than `Catalog`). Consistent with existing practice - Gravitino already subclasses
`JdbcCatalog` twice and places a class inside `org.apache.iceberg.jdbc`.
**Decision:** **Chosen.** Preserving client-authored requirements is the whole point of a pass-through.

`IcebergCatalogUtil.loadRestCatalog` changes to instantiate `ForwardingRESTCatalog` instead of
`RESTCatalog`. Everything else there - `UserPrincipalForwardingAuthManager`, HTTP timeouts,
`applyDefaultResolvingFileIO` - is unaffected.

Two properties must be preserved:

- **Identity.** `loadRestCatalog` already configures `UserPrincipalForwardingAuthManager`, so the
  forwarded call carries the original caller's identity and upstream authorization stays meaningful.
- **Capability propagation.** If the upstream does not advertise `V1_COMMIT_TRANSACTION`,
  `Endpoint.check` throws. Gravitino must therefore not advertise it either:
  `supportsAtomicMultiTableCommit()` on a `REST` backend consults the upstream's advertised endpoint
  set, captured from the `ConfigResponse` at `initialize()` time. Gravitino's guarantee is exactly the
  upstream's, never stronger.

**Phase 1 is skipped on this path.** Validation and metadata writing belong to the upstream catalog;
Gravitino performing them too would duplicate work and write metadata files the upstream may never
reference. Gravitino's only local responsibility is authorization (§5.5) and invalidating the metadata
cache for every table in the request.

### 5.5 Per-Table Authorization

The annotation interceptor resolves entities from path parameters, so it cannot express "every table
named in the body". Authorization is therefore performed programmatically, using the same primitive
the codebase already uses for body- and response-derived entities
(`IcebergTableOperations.filterListTablesResponse` calls `MetadataAuthzHelper.filterByExpression`).

`MetadataAuthzHelper.checkAccess(NameIdentifier, EntityType, String expression)` is the right
primitive here - it short-circuits to `true` when authorization is disabled, and evaluates the same
OGNL expression the annotation would.

```java
// IcebergTransactionOperations - annotation guards the catalog-level floor only
@AuthorizationExpression(
    expression = "ANY(OWNER, METALAKE, CATALOG) || ANY_USE_CATALOG",
    accessMetadataType = MetadataObject.Type.CATALOG)
public Response commitTransaction(
    @AuthorizationMetadata(type = EntityType.CATALOG) @PathParam("prefix") String prefix,
    CommitTransactionRequest request) { ... }
```

and inside, before any table is touched:

```java
private void authorizeAllTables(String metalake, String catalog, CommitTransactionRequest request) {
  for (UpdateTableRequest change : request.tableChanges()) {
    NameIdentifier ident = toGravitinoIdentifier(metalake, catalog, change.identifier());
    if (!MetadataAuthzHelper.checkAccess(
        ident, EntityType.TABLE, UPDATE_TABLE_AUTHORIZATION_EXPRESSION)) {
      throw new ForbiddenException(
          "Not authorized to modify table %s", change.identifier());
    }
  }
}
```

`UPDATE_TABLE_AUTHORIZATION_EXPRESSION` is extracted as a constant in
`AuthorizationExpressionConstants` from the literal currently inlined at
`IcebergTableOperations.java:208-213`, and both call sites reference the constant. This is what makes
"parity with `updateTable`" enforceable rather than aspirational: the two paths cannot drift because
they share one string.

Ordering is deliberate - authorize **all** tables before validating requirements, so an unauthorized
principal learns nothing about which requirements would have failed.

### 5.6 Implementation Process

New components, following the existing four-layer dispatcher pattern used by tables, views, and
namespaces (`Operations` -> `EventDispatcher` -> `HookDispatcher` -> `OperationExecutor`):

```
                POST /v1/{prefix}/transactions/commit
                              |
        IcebergTransactionOperations           (JAX-RS resource)
 - catalog-level @AuthorizationExpression
 - per-table authorizeAllTables()             <-- 5.5
 - reject duplicate / out-of-prefix idents
                              |
        IcebergTransactionEventDispatcher      (pre / success / failure events)
                              |
        IcebergTransactionHookDispatcher       (hook chain)
                              |
        IcebergTransactionOperationExecutor
 - capability check -> 501 if unsupported     <-- 5.1
 - REST backend? forward raw body & return    <-- 5.4
 - phase 1: validate all, build all metadata  <-- Option B, retained
 - phase 2: commitAtomically(commits)         <-- 5.3
 - metadata cache: invalidate all, then refill
                              |
        IcebergCatalogWrapper.commitTransaction(request)
                              |
   +--------------------------+---------------------------+
   |                          |                           |
JdbcCatalogWith...    ForwardingRESTCatalog       (HIVE / CUSTOM)
 commitAtomically()    commitTransaction()         -> 501
 (local 2-phase)       (raw forward, no phase 1)
```

Phase 1 runs on the `JDBC` and `MEMORY` paths only; the `REST` path skips it (§5.4). Per table, it
mirrors what `CatalogHandlers.updateTable` does internally:

1. Resolve `TableOperations` and `refresh()` to obtain current `base`.
2. Validate every `UpdateRequirement` against `base`. Any failure aborts the whole request with 409.
3. Apply `MetadataUpdate`s to build `updated`.
4. Write the new metadata JSON file (`TableMetadataParser.write`). This is the step that can leave an
   orphan on rollback (§3 non-goal 4).

Phase 2 hands the `(identifier, base, updated)` triples to `commitAtomically`, which performs only
pointer swaps - no I/O to object storage, keeping the transaction window short.

### 5.7 Semantics Documented for Clients

One boundary case must be stated plainly rather than glossed, because clients cannot detect it:

> If the network connection drops after the server's DB transaction commits but before the `204`
> reaches the client, the client sees a failure while all tables have in fact advanced. This is
> ordinary at-least-once ambiguity, identical to a single-table `updateTable` losing its response,
> and is resolved by the client re-reading table metadata. It is not a violation of atomicity: the
> tables are consistent with each other in both outcomes.

`docs/iceberg-rest-service.md` gains a section stating, per backend, whether the endpoint is
available and what guarantee it carries.

### 5.8 User Process

1. Operator configures an IRC catalog with `gravitino.iceberg-rest.catalog-backend = jdbc`.
2. Client (Spark, Flink, or `RESTCatalog` directly) calls `GET /v1/config`; the response now lists
   `POST /v1/{prefix}/transactions/commit` among `endpoints`.
3. Client builds a multi-table transaction and calls
   `RESTCatalog.commitTransaction(tableCommitA, tableCommitB)`.
4. Gravitino authorizes both tables, validates both requirement sets, writes both metadata files,
   then swaps both pointers in one DB transaction.
5. Client receives `204`. Both tables are at their new snapshots.
6. Had table B's requirements been stale, the client would receive `409` and **both** tables would
   remain at their original snapshots.
7. On a `HIVE`-backed catalog, step 2 does not list the endpoint, and `RESTCatalog` fails fast
   client-side without an HTTP call. A client that bypasses the config check and calls the endpoint
   directly receives `501`.

---

## 6. Task Breakdown

### Phase 1: Capability Contract

- [ ] Add `SupportsMultiTableTransaction`, `SupportsAtomicMultiTableCommit`,
      `SupportsTransactionForwarding`, and immutable `TableMetadataCommit` to `iceberg-common`
      (`org.apache.gravitino.iceberg.common.transaction`)
- [ ] Add `IcebergCatalogWrapper.supportsAtomicMultiTableCommit()` and
      `commitTransaction(CommitTransactionRequest)` dispatch skeleton
- [ ] Extract `UPDATE_TABLE_AUTHORIZATION_EXPRESSION` into `AuthorizationExpressionConstants` and
      repoint `IcebergTableOperations.updateTable` at it
- [ ] Unit tests for capability resolution across all five backends

### Phase 2: JDBC Atomic Commit

- [ ] Implement `commitAtomically` in `JdbcCatalogWithMetadataLocationSupport` using
      `JdbcClientPool.run` with `setAutoCommit(false)`
- [ ] Add V0 and V1 CAS statements plus `supportsTransactions()` probe at initialization
- [ ] Unit tests: all-succeed, CAS failure on table N rolls back tables 1..N-1, `supportsTransactions()==false`
      disables the capability
- [ ] Equivalence test asserting Gravitino's CAS statement matches the effect of `JdbcUtil.updateTable`
      (guards against upstream schema drift)
- [ ] File an upstream Iceberg issue proposing a reusable `JdbcCatalog` batch-commit primitive, and
      link it from a code comment

### Phase 3: REST Pass-Through and MEMORY

- [ ] Add `ForwardingRESTCatalog` (subclass of `RESTCatalog`) capturing the `RESTClient` and the
      upstream `ConfigResponse` endpoint set at `initialize()`
- [ ] Implement raw `CommitTransactionRequest` forwarding to `ResourcePaths.commitTransaction()`
- [ ] Switch `IcebergCatalogUtil.loadRestCatalog` to instantiate `ForwardingRESTCatalog`
- [ ] Derive the `REST` capability from the captured upstream endpoint set
- [ ] Implement `InMemoryCatalogWithAtomicCommit` for the `MEMORY` backend
- [ ] Unit tests: exactly one upstream POST is issued; the forwarded body equals the inbound body
      including client-authored requirements; an upstream lacking `V1_COMMIT_TRANSACTION` yields no
      advertisement

### Phase 4: Endpoint, Dispatcher Chain, Authorization

- [ ] Add `IcebergTransactionOperationDispatcher` and `IcebergTransactionOperationExecutor`
      (capability check, phase 1 validate-and-write, phase 2 atomic swap, cache invalidate/refill)
- [ ] Add `IcebergCommitTransactionPreEvent`, `IcebergCommitTransactionEvent`,
      `IcebergCommitTransactionFailureEvent`, and `OperationType.COMMIT_TRANSACTION`
- [ ] Add `IcebergTransactionEventDispatcher` and `IcebergTransactionHookDispatcher`
- [ ] Add `IcebergTransactionOperations` JAX-RS resource with request validation (empty list,
      duplicate identifiers, out-of-prefix identifiers)
- [ ] Implement `authorizeAllTables` using `MetadataAuthzHelper.checkAccess`, invoked before phase 1
- [ ] Wire the dispatcher chain into `RESTService` DI bindings
- [ ] Gate `Endpoint.V1_COMMIT_TRANSACTION` in `IcebergConfigOperations.getEndpoints`
- [ ] Unit tests: 204 success, 409 requirement failure, 404 unknown table, 400 duplicate identifier,
      501 on `HIVE`, 403 when one table is unauthorized
- [ ] Authorization test: principal with `MODIFY_TABLE` on A but not B is rejected, A unchanged

### Phase 5: Integration Tests and Documentation

- [ ] Docker integration test (`@Tag("gravitino-docker-test")`): PostgreSQL-backed JDBC catalog,
      multi-table commit succeeds; concurrent writer forces rollback and all tables verified unchanged
- [ ] Docker integration test: HMS-backed catalog does not advertise the endpoint and returns 501
- [ ] Docker integration test: REST-backed catalog forwards to an upstream IRC server
- [ ] Spark end-to-end test exercising a multi-table write against a JDBC-backed catalog
- [ ] Update `docs/open-api/iceberg-rest-catalog.yaml`, validate with `./gradlew :docs:build`
- [ ] Document the per-backend capability matrix and the response-loss caveat (§5.7) in
      `docs/iceberg-rest-service.md`
