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

# Design: Implementing Concurrency Control for Managed Entities and Removing TreeLock

> Implementation design. The investigation, the comparison of options, and the decision are in [concurrency-control-investigation-and-decision.md](concurrency-control-investigation-and-decision.md). This document only says **how** to build the replacement and in **what order**.
>
> Epic [#10238](https://github.com/apache/gravitino/issues/10238) (supersedes [#10474](https://github.com/apache/gravitino/issues/10474), closed)
>
> **Baseline.** Re-checked against `main` at `03da573642` (2026-09-15). Tasks that already landed are kept in the task list with their PR, so the list is the single place to see what is left.

**Short words used in this doc:**

- **OCC** = Optimistic Concurrency Control = do not take a lock first; when you write, check "is the row still the version I read?" If yes, write. If no, someone else wrote first, so return a conflict.
- **CAS** = Compare-And-Set = the `WHERE id = ? AND current_version = ?` part of that write.
- **External catalog** = the real system that holds the data: Hive, Glue, Iceberg, MySQL, Kafka.
- **Store** = Gravitino's own database.
- **Attachments** = data that only Gravitino has about an object: owner, tags, policies, role grants, statistics.
- **Managed** = `Capability.managedStorage(scope)` is supported, so the store is the only source of truth for that entity's metadata. Note that this is about *metadata*: an `EXTERNAL` fileset is still a managed entity, and a table created through Gravitino in Hive is still external-backed.

---

## Background

The companion document investigated the problem and decided the direction. In short:

- TreeLock only works inside one JVM, so it protects nothing once more than one Gravitino server runs. On one server it still hides a few races, so it cannot simply be deleted; each of those races has to be closed in the database first.
- Correctness moves into the shared database: version checks for same-row updates, plain inserts for creates, and short transactions that lock the parent or endpoint row for cross-row rules.
- No distributed lock and no 2PC.
- For **external-backed** entities the external system stays the source of truth. Users write to it directly, so we promise catch-up, not equality.
- For **managed** entities the store is the only place the data lives, so concurrent and multi-server writes must be strongly consistent.

### What `main` already has (2026-09-15)

The store layer has moved a long way since July. The table is here so the rest of this document can talk about what is *left*.

| Area                                             | Status on `main`                                                                                                                                                                                                                                                                                                            |
|--------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Version-checked update and delete                | every entity service — metalake, catalog, schema, table, fileset, topic, view, function, model + model version, user, group, role, tag, policy, job, job template — goes through `OccWriteSupport.updateWithVersion` / `deleteWithVersion`; the version advances on every write, including overwrite; a 0-row result is classified into `OptimisticLockException` (409, code 1012) or `NoSuchEntityException` (404) by `OccWriteSupport.writeFailure` ([#12166](https://github.com/apache/gravitino/issues/12166) and sub-issues, [#12341](https://github.com/apache/gravitino/issues/12341)) |
| Parent row held in the child's transaction       | `metalake → catalog → schema → table/fileset/topic/view/function/model`; `FOR SHARE` for create and move, `FOR UPDATE` for drop; one lock order; the non-cascade emptiness check runs inside the drop transaction after the version-checked delete and covers every child type                                                |
| Cascade delete in one transaction                | catalog and schema cascades soft-delete children with version checks (`deleteChildrenWithVersions`) and every attachment table in one `doMultipleWithCommit`                                                                                                                                                                 |
| Strict create                                    | catalog, managed schema, managed table, managed function, model, user, group, role insert with `overwrite = false`                                                                                                                                                                                                           |
| Import-time id check                             | view, function and tag refuse an id that already belongs to another parent (`OccWriteSupport.findAndLockForOverwrite`)                                                                                                                                                                                                      |
| Rename × drop                                    | a `false` external drop leaves the registration alone; a rename whose store update matches 0 rows is reported as an error ([#12232](https://github.com/apache/gravitino/issues/12232), #12235)                                                                                                                              |
| Catalog wrapper lifetime                         | operation leases on `CatalogWrapper`; cleanup exactly once after the last lease ([#12403](https://github.com/apache/gravitino/issues/12403), #12404)                                                                                                                                                                        |
| Orphan relation GC                               | `RelationalGarbageCollector` soft-deletes relation rows whose metadata object is gone ([#12154](https://github.com/apache/gravitino/issues/12154), #12160)                                                                                                                                                                  |
| Model version counter                            | raised and read back inside the transaction (`bumpModelVersionAndLatestVersion`)                                                                                                                                                                                                                                             |
| Fileset drop                                     | row first: `store.deleteAndGet` wins the CAS, then removes storage inside the transaction, so a stale loser never touches the filesystem                                                                                                                                                                                     |
| Jobs                                             | version-checked status transitions; a lost CAS defers to the next poll; a two-node test exists ([#12669](https://github.com/apache/gravitino/issues/12669), [#12992](https://github.com/apache/gravitino/issues/12992), `TestJobManagerMultiNode`)                                                                            |
| Multi-node cache                                 | per-node entity cache invalidated by `EntityChangeLogPoller` (default 3 s poll); an optional shared Redis cache is in review ([#12020](https://github.com/apache/gravitino/issues/12020))                                                                                                                                    |

What this leaves is not "missing OCC". It is four narrower things: **operation identity** on external-backed paths, **relation integrity** inside the store, **side effects** outside the store (Ranger, fileset directories, the job executor), and the **read contract**. Each is a part below; the gate letters match the investigation document.

---

## Goals

1. **Close G1 — operation identity.** Every store write on an external-backed path carries the identity it observed *before* the external call, and a mismatch is a conflict, never a write to a different object.
2. **Close G2 — managed rows.** Every managed create is strict, and every lifecycle precondition ("is empty", "in use", "enabled") is decided in the same transaction as the write it guards.
3. **Close G3 — relations.** One live owner per object; no relation can be attached to a deleted endpoint; assignments are ordered against drop and cascade.
4. **Close G4 — side effects.** Plugin calls, fileset storage and job submission have an identity, a durable state and an idempotent recovery, with the impossible parts written down.
5. **Close G5 — reads.** The read contract is written and tested.
6. **Close G6 — validation, then remove the lock.** Each operation family loses its TreeLock only after its invariants pass on two servers sharing one database.
7. **One clear result per conflict**: a concurrent writer gets success, `409`, `AlreadyExists`, or `NoSuchEntity` — never a silent merge, a lost update, or a `500`.

## Non-Goals

1. **An exact copy of the external catalog.** Users write to it directly, so we promise catch-up, not equality.
2. **No distributed lock and no 2PC.**
3. **No change of isolation level.** We stay on `READ_COMMITTED`; where a read needs a consistent snapshot it re-validates the root version instead.
4. **No automatic retry of managed writes inside the server.** A 409 goes back to the caller; the clients learn to recognise it. (The July draft proposed `gravitino.entity.store.occ.maxRetries`; it was never implemented and is dropped.)
5. **No repair of external lost updates.** HMS and Glue whole-object alters stay last-writer-wins until the backend offers a conditional write; Gravitino mirrors the winner and documents the limit.
6. **No identity continuity for id-less objects re-created under the same name.** Gravitino cannot tell "the same table" from "a new table with the old name" on JDBC/PostgreSQL; the policy is explicit re-registration, never reuse of a tombstoned id ([#12153](https://github.com/apache/gravitino/issues/12153)).

---

## Part 1 — Closing G1: operation identity on external-backed paths

The investigation graded every external-catalog mismatch. Levels 0 and 1 are accepted. The level-2 cases all have one shape: **the store write resolves its target too late or by the wrong key**, so a CAS that is correct for the row it reads still hits the wrong row.

| #  | Hazard                                                                                                          | Status  | Fix                                                                                                                                                                                                                                                            |
|----|-----------------------------------------------------------------------------------------------------------------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| F1 | rename against drop deletes a live entity's row and attachments                                                 | done    | [#12232](https://github.com/apache/gravitino/issues/12232)                                                                                                                                                                                                     |
| F2 | drop deletes by name at the service level                                                                       | done    | `deleteTable` and friends CAS on `(id, current_version)` before touching dependents                                                                                                                                                                             |
| F5 | **drop / purge resolve the registration after the external call** (E1)                                          | open    | capture `(id, version)` before the external call; add `EntityStore.delete(ident, type, expectedId, expectedVersion)` (or a `deleteIf` variant) and use it from the table, schema, topic, view and function dispatchers; a mismatch throws `OptimisticLockException` and leaves the row for reconcile |
| F6 | **`operateOnEntity` validates the id after `store.update` committed** (E2)                                      | open    | pass the expected id into the update: `store.update(ident, type, expectedId, updater)` fails inside the transaction, before version, columns or relations move; keep the post-check only as a log                                                                |
| F3 | a copied `StringIdentifier` moves an existing row to another table, permissions included (E3)                   | open    | before an import writes the row, lock the id and refuse it if it belongs to a live entity with another name or parent — the check view/function/tag already do — for table, schema and topic; log, mint a new id, count it in a metric                            |
| F7 | a stale external snapshot lands after a newer one (E4)                                                          | limit   | where the connector exposes a revision (Iceberg metadata location, Paimon schema id) carry it into the store row and refuse an older one; where it does not, document that the next load re-syncs                                                                 |
| F4 | a left-over row is reused by name after an outside drop and recreate                                            | half    | relation GC done ([#12154](https://github.com/apache/gravitino/issues/12154)); reconcile job open ([#12155](https://github.com/apache/gravitino/issues/12155))                                                                                                    |
| F8 | hook-dispatcher privilege removal runs on a `false` drop                                                        | open    | skip the plugin call when the dispatcher returned `false`; carry the entity id into the plugin event so a stale event cannot remove a newer object's privileges                                                                                                    |

**F5 in detail.** Today:

```java
boolean droppedFromCatalog = catalog.dropTable(ident);
if (droppedFromCatalog) store.delete(ident, TABLE);   // resolves the row by name now
```

Target:

```java
Optional<TableEntity> observed = getEntity(ident);           // before the external call
boolean droppedFromCatalog = catalog.dropTable(ident);
if (droppedFromCatalog && observed.isPresent()) {
  store.delete(ident, TABLE, observed.get().id(), observed.get().version());  // 0 rows -> conflict, row kept
}
```

The service side already has the CAS; the new overload only replaces "read the row now" with "the row must still be the one I saw". A row that is not the observed one is left alone and becomes reconcile's job. The external ABA window — an external API that only takes a name — cannot be closed from Gravitino's side and is written down as a limit.

**F4 in detail.** Generalize the existing `OrphanedSchemaCleanup` / `SchemaEntityCleaner` pattern — ask the external system whether the object still exists, then delete the store row — from schemas to table, fileset, topic and model. Reuse `deleteTable`'s cascade so the attachments go with the row. Guard rails, because an external probe is slow and can flap: external-backed catalogs only, never managed ones; delete only after N misses in a row plus a grace period; rate-limit it and run it on the `RelationalGarbageCollector` schedule. The relation-GC pass already exists; membership and owner rows need the same pass ([#13003](https://github.com/apache/gravitino/issues/13003)).

**On backends.** The table upsert behaves differently on MySQL (natural-key conflict keeps the old `table_id`) and PostgreSQL (`ON CONFLICT (table_id)`; a different-id natural-key collision fails). Once F3 and a strict create are in, the create path never upserts and the import path always resolves the id explicitly first, so the two backends converge. Until then the difference is a known behaviour, not a bug to patch in SQL.

---

## Part 2 — Closing G2: managed rows

The rules from the July draft are unchanged and are now the norm in every meta service:

- **R1 — one transaction per rule.** Everything that has to hold together goes into one `doMultipleWithCommit`, *including looking up ids and checking preconditions*.
- **R2 — touch the row you depend on first.** If a transaction needs a row to stay alive, lock it in the first statement (`FOR SHARE` for a child write, `FOR UPDATE` for a parent drop). `0 rows` means the precondition is gone.
- **R3 — same-row updates carry a version check**: `WHERE id = ? AND current_version = ? AND deleted_at = 0`, with the version always going up.
- **R4 — a user create is a plain insert.** `overwrite = true` is only for import and reconcile; the unique key picks the winner.

```sql
-- create a schema under catalog C
BEGIN;
  SELECT catalog_id FROM catalog_meta
   WHERE catalog_id = ? AND deleted_at = 0 FOR SHARE;      -- 0 rows -> NoSuchCatalogException
  INSERT INTO schema_meta (...) VALUES (...);              -- plain insert, unique key picks the winner
COMMIT;

-- drop catalog C (cascade)
BEGIN;
  SELECT catalog_id, current_version FROM catalog_meta
   WHERE catalog_id = ? AND deleted_at = 0 FOR UPDATE;     -- 0 rows -> NoSuchCatalogException
  UPDATE catalog_meta SET deleted_at = ?
   WHERE catalog_id = ? AND current_version = ? AND deleted_at = 0;   -- 0 rows -> 409
  -- non-cascade only: the "is it empty" check goes here, inside the transaction
  ... the cascade over schema/table/fileset/... and owner/tag/policy/role/statistic ...
COMMIT;
```

What is left under G2:

| #  | Gap                                                                                                                                                                                                 | Fix                                                                                                                                                                                                                                                                                              |
|----|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| M1 | **managed fileset create is `exists()` then `put(…, true)`** (`FilesetCatalogOperations`). Two creates can both pass the check; the second replaces the first's metadata under the store lock and reports success | `put(…, false)`; map the duplicate to `FilesetAlreadyExistsException`; move `mkdirs` after the store decision or make it idempotent and clean it on the loser (see Part 3)                                                                                                                          |
| M4 | **manager-level preconditions run outside the deciding transaction**: `CatalogManager.dropCatalog(force = false)` lists user-created schemas, then calls `store.delete(…, cascade = true)`; `MetalakeManager.dropMetalake` is the same shape | pass `cascade = force` down so the non-force path uses the service's in-transaction emptiness check (with built-in schemas excluded there); for the force path, set a durable `DELETING` state on the parent row before enumerating children so no new child is admitted                            |
| M5 | **metalake enable/disable fan-out is not atomic** (`MetalakeManager.updateMetalakeInUseStatusInCatalog`)                                                                                            | either update the metalake row and all its catalog rows in one transaction with the metalake row `FOR UPDATE`, or stop copying the flag and derive the effective state from the metalake row on read; pick the second if the in-use check can afford one extra row read                           |
| M7 | **`overwrite` is a boolean shared by create, import and reconcile**                                                                                                                                 | replace it with an intent enum (`CREATE`, `CREATE_IF_ABSENT`, `IMPORT`, `RECONCILE`) so an import path cannot be reached from a create API and each intent has its own id rule                                                                                                                    |
| M8 | **fileset and policy OCC versions are tied to history versions** ([#12206](https://github.com/apache/gravitino/issues/12206))                                                                       | separate the OCC token from the user-visible version                                                                                                                                                                                                                                             |
| M9 | **rolling upgrade** ([#12205](https://github.com/apache/gravitino/issues/12205))                                                                                                                    | mixed-version alter/drop tests; old servers must not reset a version the new ones advanced                                                                                                                                                                                                       |

### What users see

No REST path or payload changes. `OptimisticLockException` maps to `409` with error code `1012` (done). The Java and Python clients still surface it as a generic error; [#12207](https://github.com/apache/gravitino/issues/12207) adds a typed exception and retry guidance. Two release-note items: writes that used to overwrite each other quietly now report a conflict, and several paths that returned `500` on a concurrent write now return `409` or `404`.

---

## Part 3 — Closing G3: relations

Tag and policy assignment already lock their own row (`FOR UPDATE`) and the metalake (`FOR SHARE`) inside the transaction. The other end — the metadata object — and the principal-side relations are not fenced yet.

| #  | Gap                                                                                                                                            | Fix                                                                                                                                                                                                                                                                              |
|----|------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| M2 | **owner**: `setOwner` soft-deletes then inserts with no endpoint lock; `uk_ow_me_del` includes `owner_id`, so two live owners per object are possible; a deleted principal can still be assigned ([#13002](https://github.com/apache/gravitino/issues/13002)) | migrate the unique key to `(metadata_object_id, metadata_object_type, deleted_at)` with a script that merges duplicates; lock the object row and the principal row (`FOR SHARE`) in a fixed order inside the transaction; define the semantics as "last assignment wins, serialized" |
| M3a | **role membership**: `user_role_rel` / `group_role_rel` inserts do not fence the role ([#13001](https://github.com/apache/gravitino/issues/13001), PR #13006)                                        | lock the role row `FOR SHARE` in the membership transaction                                                                                                                                                                                                                       |
| M3b | **object side of tag / policy / statistic / securable-object relations**: the target id is resolved before the transaction and never re-validated ([#13004](https://github.com/apache/gravitino/issues/13004)) | one type-aware `lockLiveEndpoint(type, id)` helper that selects the target row `FOR SHARE` and checks name and parent, used by every association path; include column ids for column-level relations                                                                              |
| M3c | **statistics** are last-writer-wins with no version and no live-target check                                                                    | add `current_version` CAS to `statistic_meta` updates (last-writer-wins on the *value* may stay, by design, but say so) and the endpoint fence above                                                                                                                              |
| M3d | **relation GC does not cover principal relations** ([#13003](https://github.com/apache/gravitino/issues/13003))                                 | extend `SupportsOrphanedRelationCleanup` to `user_role_rel`, `group_role_rel`, `owner_meta` with a missing user/group/role                                                                                                                                                        |
| M10 | **nested transaction rollback is swallowed** ([#13005](https://github.com/apache/gravitino/issues/13005))                                        | make an inner failure poison the outer `SqlSession` so the outer commit rolls back; audit callers that catch inside a nested scope                                                                                                                                                |

---

## Part 4 — Closing G4: side effects outside the store

| #  | Side effect                                                                                                                                                              | Fix                                                                                                                                                                                                                                                                                                                                              |
|----|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| S1 | **authorization-plugin order** (B1): store first, plugin second, ordered only by the per-JVM principal write lock                                                        | decide one of: (a) carry the store version of the user/group/role into the plugin call and have the plugin apply only if newer; (b) keep a small per-principal in-process guard behind the same `doWithTreeLock` signature and write down that two servers can still reorder. (a) is the target; (b) is the migration step                     |
| S2 | **fileset storage vs. row** (B3): create is directory-first; multi-location delete is not atomic; a rollback after a partial delete leaves a live row over missing data | a `state` column on `fileset_meta` (`CREATING`, `ACTIVE`, `DELETING`): create inserts `CREATING`, does the storage work, then flips to `ACTIVE` with a CAS; drop flips to `DELETING` with a CAS, deletes storage idempotently, then soft-deletes; a background pass finishes `DELETING` rows and removes `CREATING` rows older than a grace period |
| S3 | **jobs** (B4): submit to the executor before `store.put`                                                                                                                 | write a `SUBMITTING` job row first, submit, then CAS to `QUEUED`/`RUNNING` with the executor id; the poller reconciles `SUBMITTING` rows against the executor and treats the executor as the truth                                                                                                                                                |
| S4 | **`dropCatalog(force)` external cascade** (A3)                                                                                                                           | the `DELETING` state from M4 admits no new children while the cascade runs                                                                                                                                                                                                                                                                       |
| S5 | **stale reconcile / import against a live object**                                                                                                                       | reconcile deletes only with the observed `(id, version)` and only after N consecutive misses; it never reuses a tombstoned id                                                                                                                                                                                                                    |

None of these puts a database row lock around an external RPC. The fileset drop callback, which already runs storage deletion inside the store transaction, stays as a deliberate exception with its limits noted (a pooled connection is held during I/O; physical data already deleted cannot be restored).

---

## Part 5 — Closing G5: the read contract

Row OCC makes writes safe; it says nothing about what a reader sees.

- **Per-node cache.** Reads may be served from a per-node cache that `EntityChangeLogPoller` invalidates every `gravitino.entityChangeLog.pollIntervalSecs` (default 3 s). That is eventual, not a bound. Paths that decide authorization or lifecycle (`checkMetalakeInUse`, owner lookup for a permission check, the in-use check before a drop) must read through to the store or through the shared cache when one is configured.
- **Aggregate reads.** `getTableByIdentifier` reads the row, then the columns, in two statements. A drop or overwrite between them can return a row with no columns. Re-read the root row after the child read and compare the version; on a mismatch retry once, then fail.
- **Writes never read the cache for their CAS.** Already true: every service reads its `oldPO` from the store inside `updateX`. Keep it that way and add a test that a stale cached entity cannot make a CAS pass.

---

## Part 6 — Closing G6 and removing the lock

With Parts 1–5 done, going through the 161 `doWithTreeLock` call sites on `main` leaves nothing the database cannot take over except the small guards named in Part 4. Removal is done **per operation family**, each with a mapped invariant and a test that runs without the same-JVM lock:

| Family                                   | Invariant that replaces the lock                                                     | Test that proves it                                                                 |
|------------------------------------------|--------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------|
| `PartitionOperationDispatcher` (6 sites) | none needed — no store write                                                         | existing ITs                                                                        |
| catalog `drop` / `alter` metalake WRITE  | wrapper lease (done) + catalog row `FOR UPDATE`                                      | evict/alter/drop while a connector call is active; cleanup exactly once             |
| table/schema/topic/view/function drop and rename | F5 identity capture + parent row rule                                        | two-server external drop/recreate ABA; rename × drop                                |
| create under a parent                    | parent row `FOR SHARE` (done)                                                        | two-server child create × parent drop, cascade and non-cascade                      |
| import on read                           | `EntityAlreadyExists` reload (done) + F3 id check                                    | two-server import × import; copied `StringIdentifier`                               |
| tag / policy / owner / statistic managers | endpoint fences (Part 3)                                                            | assignment paused after id resolution while the target is dropped on the other server |
| access control managers                  | S1 decision                                                                          | delayed grant/revoke reorder                                                        |
| job manager                              | job OCC (done) + S3                                                                  | `TestJobManagerMultiNode` extended                                                  |

Then, and only then: replace `LockManager` and `TreeLockNode` with a `ConcurrentMap<NameIdentifier, ReadWriteLock>` behind the unchanged `doWithTreeLock` signature for the guards that stay (S1 option b, if chosen), remove the deadlock checker and the node cleanup thread, keep the `gravitino.lock.*` configs but ignore them for one release, and add a test for nested `doWithTreeLock` calls (a fixed-size striped lock is not safe here because a parent and a child on the same stripe deadlock).

**A note on deadlocks.** The July draft claimed that taking TreeLock before a transaction proves adding row locks cannot deadlock. That is too strong: row-lock cycles depend on the complete order across parents, endpoints and cascades, and on other transactions that never took TreeLock. The written lock order (`metalake → catalog → schema → entity → endpoint`), a fixed row order in batch statements, and lock/deadlock timeouts tested on MySQL, PostgreSQL and H2 are the actual protection.

---

## Task Breakdown

**Order.** Stages 1–5 land with TreeLock **unchanged**. Stage 6 is the first stage that changes the lock. Items marked ✅ are on `main` at 2026-09-15 and are kept for the record.

### Stage 0 — Already landed

- [x] OCC contract and 409/1012 response ([#12341](https://github.com/apache/gravitino/issues/12341), #12349)
- [x] Version-CAS for metalake, catalog, schema, table, view, fileset, function, topic, model, user, group, role, tag, policy, job, job template ([#12166](https://github.com/apache/gravitino/issues/12166) sub-issues)
- [x] Shared OCC helpers `OccWriteSupport` ([#12639](https://github.com/apache/gravitino/issues/12639))
- [x] Parent row rule `metalake → catalog → schema → entity`; emptiness check inside the drop transaction covering views and functions
- [x] Rename × drop keeps the live registration ([#12232](https://github.com/apache/gravitino/issues/12232))
- [x] Orphan relation GC ([#12154](https://github.com/apache/gravitino/issues/12154))
- [x] `CatalogWrapper` operation leases ([#12403](https://github.com/apache/gravitino/issues/12403))
- [x] Job status transitions by CAS and a two-node job test ([#12669](https://github.com/apache/gravitino/issues/12669), [#12992](https://github.com/apache/gravitino/issues/12992))
- [x] Tombstoned id reuse rejected ([#12153](https://github.com/apache/gravitino/issues/12153))

### Stage 1 — G1: operation identity (can run in parallel)

- [ ] [#13172](https://github.com/apache/gravitino/issues/13172) F5 — `EntityStore.delete(ident, type, expectedId, expectedVersion)` and its use in table, schema, topic, view, function `drop`/`purge`; the observed identity is read before the external call
- [ ] [#13172](https://github.com/apache/gravitino/issues/13172) F6 — `EntityStore.update` variant that takes the expected id and fails inside the transaction; `operateOnEntity` post-check becomes a log
- [ ] [#13173](https://github.com/apache/gravitino/issues/13173) F3 — import-time id check for table, schema and topic via `findAndLockForOverwrite`; new id + log + metric on collision
- [ ] [#13174](https://github.com/apache/gravitino/issues/13174) F8 — hook dispatchers skip `authorizationPluginRemovePrivileges` on a `false` drop/purge; plugin events carry the entity id
- [ ] F4 — reconcile job for external-backed rows ([#12155](https://github.com/apache/gravitino/issues/12155)): N misses + grace period, observed `(id, version)` delete, external-backed catalogs only
- [ ] F7 — carry the connector revision into the store row where one exists (Iceberg, Paimon); document "next load re-syncs" for the rest
- [ ] Tests: external drop/recreate ABA on two servers; wrong-id alter; copied `StringIdentifier` on two live objects; `false` drop leaves store and plugin alone; JDBC same-name recreate gets no old grants

### Stage 2 — G2: managed rows

- [ ] [#13175](https://github.com/apache/gravitino/issues/13175) M1 — managed fileset create uses a strict insert; duplicate → `FilesetAlreadyExistsException`; loser's directory cleaned or `mkdirs` moved after the store decision
- [ ] [#13176](https://github.com/apache/gravitino/issues/13176) M4 — `dropCatalog(force = false)` and `dropMetalake(force = false)` decide "is empty" inside the delete transaction; built-in schemas excluded there
- [ ] [#13176](https://github.com/apache/gravitino/issues/13176) M5 — metalake in-use: one transaction over the metalake row and its catalog rows, or derive the flag from the metalake row on read
- [ ] [#13175](https://github.com/apache/gravitino/issues/13175) M7 — replace `overwrite: boolean` with an intent enum (`CREATE`, `CREATE_IF_ABSENT`, `IMPORT`, `RECONCILE`)
- [ ] M8 — decouple OCC versions from fileset/policy history versions ([#12206](https://github.com/apache/gravitino/issues/12206))
- [ ] M9 — rolling-upgrade tests for version-CAS ([#12205](https://github.com/apache/gravitino/issues/12205))
- [ ] Client-side `OptimisticLockException` for Java and Python ([#12207](https://github.com/apache/gravitino/issues/12207))
- [ ] Tests: two-server strict create; child create/enable between the API check and the delete; enable/disable race

### Stage 3 — G3: relations

- [ ] M2 — owner unique key `(metadata_object_id, metadata_object_type, deleted_at)` + duplicate-merge script; object and principal rows fenced in the assignment transaction ([#13002](https://github.com/apache/gravitino/issues/13002))
- [ ] M3a — role membership fences the role row ([#13001](https://github.com/apache/gravitino/issues/13001), PR #13006)
- [ ] M3b — `lockLiveEndpoint` helper used by tag, policy, statistic and securable-object association ([#13004](https://github.com/apache/gravitino/issues/13004))
- [ ] [#13177](https://github.com/apache/gravitino/issues/13177) M3c — statistic version CAS and endpoint fence
- [ ] M3d — relation GC for `user_role_rel`, `group_role_rel`, `owner_meta` ([#13003](https://github.com/apache/gravitino/issues/13003))
- [ ] M10 — nested transaction failure poisons the outer commit ([#13005](https://github.com/apache/gravitino/issues/13005))
- [ ] Tests: two initial owner assignments → exactly one live owner; assignment paused after id resolution while the target/principal is dropped on the other server

### Stage 4 — G4: side effects

- [ ] [#13178](https://github.com/apache/gravitino/issues/13178) S1 — decide and implement the authorization-plugin ordering (store version into the plugin call, or a documented in-process guard)
- [ ] [#13179](https://github.com/apache/gravitino/issues/13179) S2 — fileset `state` column and `CREATING/ACTIVE/DELETING` protocol with idempotent cleanup and a background finisher
- [ ] [#13180](https://github.com/apache/gravitino/issues/13180) S3 — job `SUBMITTING` intent row before executor submit; poller reconciles against the executor
- [ ] [#13176](https://github.com/apache/gravitino/issues/13176) S4 — `DELETING` state on catalog/metalake rows before a force cascade
- [ ] Tests: delayed grant/revoke reorder; partial multi-location delete; executor accepts and registration fails

### Stage 5 — G5: reads

- [ ] [#13181](https://github.com/apache/gravitino/issues/13181) Write the read contract (which paths read through, which may be stale) into `docs/` and enforce read-through on in-use, owner and permission checks
- [ ] [#13181](https://github.com/apache/gravitino/issues/13181) Root-version re-validation for row + columns and row + versions reads
- [ ] Tests: read routed to the other server before invalidation; drop between root and child reads

### Stage 6 — G6: validation and removal

- [ ] [#13182](https://github.com/apache/gravitino/issues/13182) Two-server test harness for dispatcher-level races (extend the `TestJobManagerMultiNode` pattern: two `GravitinoEnv`s over one H2/MySQL/PostgreSQL via `BackendTestExtension`), barrier-driven
- [ ] [#13183](https://github.com/apache/gravitino/issues/13183) Check in the list of all `doWithTreeLock` call sites: what each one locks, which rows it touches, what replaces it, which test proves it
- [ ] [#13183](https://github.com/apache/gravitino/issues/13183) Remove the six locks in `PartitionOperationDispatcher`
- [ ] [#13183](https://github.com/apache/gravitino/issues/13183) Remove the metalake WRITE lock from `dropCatalog`/`alterCatalog` (lease + catalog row lock replace it)
- [ ] [#13183](https://github.com/apache/gravitino/issues/13183) Remove the locks per family in the order of the Part 6 table, each behind its passing two-server test
- [ ] [#13183](https://github.com/apache/gravitino/issues/13183) Replace `LockManager`/`TreeLockNode` with a per-identifier map for the remaining guards; remove the deadlock checker and cleanup thread; keep `gravitino.lock.*` for one release
- [ ] [#13183](https://github.com/apache/gravitino/issues/13183) Lock-order and deadlock-timeout tests on MySQL, PostgreSQL and H2; contention benchmark against the July baseline

### Test rules for every stage

- Drive races with explicit barriers, not with a loop that hopes to hit the timing.
- Check both the API result and the final database state, including soft-deleted rows, generated ids, version rows and every attachment table.
- Run store-protocol races on each supported relational backend with distinct connections; run dispatcher, hook and recovery races through two server processes sharing one database and one external backend. Two unrelated H2 files prove nothing.
- For external-catalog races, also check the final external state and that the copy catches up; include a bypass writer (direct HMS/JDBC) and crash injection for the guarantees that mention them.
