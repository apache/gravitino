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
> Epic [#10238](https://github.com/apache/gravitino/issues/10238) · Tracking [#10474](https://github.com/apache/gravitino/issues/10474)

**Short words used in this doc:**

- **OCC** = Optimistic Concurrency Control = do not take a lock first; when you write, check "is the row still the version I read?" If yes, write. If no, someone else wrote first, so retry or return an error.
- **CAS** = Compare-And-Set = the `WHERE id = ? AND current_version = ?` part of that write.
- **External catalog** = the real system that holds the data: Hive, Glue, Iceberg, MySQL, Kafka.
- **Store** = Gravitino's own database.
- **Attachments** = data that only Gravitino has about an object: owner, tags, policies, role grants, statistics.

---

## Background

The companion document [concurrency-control-investigation-and-decision.md](concurrency-control-investigation-and-decision.md) investigated the problem and decided the direction. In short:

- TreeLock only works inside one JVM, so it protects nothing once more than one Gravitino server runs.
- Correctness moves into the shared database: version checks for same-row updates, plain inserts for creates, and short transactions that lock the parent row for parent/child rules.
- No distributed lock and no 2PC.
- For **external-backed** entities (Hive, Glue, JDBC, Iceberg, Paimon, Kafka topics) the external system stays the source of truth. Users write to it directly, so we promise catch-up, not equality. The investigation graded every mismatch and found only four that are bad enough to block anything.
- For **managed** entities (metalake, catalog, schema of a managed catalog, fileset, model, view, function, user/group/role, tag, policy, job) the store is the only place the data lives, so concurrent and multi-server writes must be correct.
- TreeLock is removed once five gates are closed: **G1** identity and delete bugs, **G2** parent rule and version checks down to the entity level, **G3** `CatalogWrapper` reference counting, **G4** authorization-plugin call order, **G5** two-server tests.

This document designs that work. Every section below says which gate it closes.

---

## Goals

1. **Close G1**: no write path can delete or move the attachments of a live entity.
2. **Close G2**: every managed write is one transaction with a checked condition, from metalake down to table, fileset, model, topic, view and function.
3. **One clear result per conflict**: a concurrent writer gets success, `409`, `AlreadyExists`, or `NoSuchEntity` — never a silent merge, a lost update, or a `500`.
4. **Close G3 and G4**, then remove TreeLock, in that order.

## Non-Goals

1. **An exact copy of the external catalog.** Users write to it directly, so we promise catch-up, not equality.
2. **No distributed lock and no 2PC.**
3. **Fresh reads on every server.** Cache invalidation is the `EntityChangeLogPoller` work.
4. **No change of isolation level.** We stay on `READ_COMMITTED`.

---

## Part 1 — Closing G1: the four bad external-catalog bugs

The investigation graded every external-catalog mismatch. Levels 0 and 1 are accepted: the external system stays the source of truth, the copy is fixed on the next read, and left-over rows are work for a background job. Four cases are level 2 — a live object's attachments are deleted, or a row ends up pointing at a different object — and those are designed here. Two of them come from concurrency, two from users changing the external system directly.

| #   | Bug                                                                                      | Fix                                                                                                                                                                                                         |
|-----|------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| F1  | rename against drop deletes a live entity's row and attachments                          | when the external drop returned `false`, leave the store alone; a rename whose store update matched 0 rows must report that instead of success ([#12232](https://github.com/apache/gravitino/issues/12232)) |
| F2  | drop deletes by name, so it can hit the next table with that name                        | delete with a version check on `(id, current_version)` of the entity the call actually worked on                                                                                                            |
| F3  | a copied `StringIdentifier` moves an existing row to another table, permissions included | before an import writes the row, check that the id is free: if it already belongs to a live entity with another name or parent, log it, use a new id, and count it in a metric                              |
| F4  | a left-over row is reused by name after an outside drop and recreate                     | reconcile job ([#12155](https://github.com/apache/gravitino/issues/12155)) plus relation GC ([#12154](https://github.com/apache/gravitino/issues/12154)) — see below                                        |

**F4 in detail, because it has two halves.**

*Reconcile.* Generalize the existing `OrphanedSchemaCleanup` / `SchemaEntityCleaner` pattern — ask the external system whether the object still exists, then delete the store row — from schemas to table, fileset, topic and model. Reuse `deleteTable`'s cascade so the attachments go with the row. Guard rails, because an external probe is slow and can flap: external-backed catalogs only, never managed ones; delete only after N misses in a row plus a grace period; rate-limit it and run it on the `RelationalGarbageCollector` schedule.

*Relation GC.* Add a pass to `RelationalGarbageCollector` that soft-deletes relation rows whose `metadata_object_id` points at no live entity of that type — one statement per relation table (`owner_meta`, `tag_relation_meta`, `policy_relation_meta`, `statistic_meta`, `role_meta_securable_object`), for example:

```sql
UPDATE owner_meta SET deleted_at = ?
 WHERE deleted_at = 0 AND metadata_object_type = 'TABLE'
   AND NOT EXISTS (SELECT 1 FROM table_meta
                    WHERE table_id = metadata_object_id AND deleted_at = 0);
```

The normal `deleteTable` path already cascades these relations in one transaction (`TableMetaService:270-302`); this pass only catches rows left behind by an out-of-band loss. It does not need to tell "same table" from "recreated with the same name" — it only removes relations whose id no longer exists.

Keeping the id itself stable across a delete is not possible for id-less catalogs: the external object carries no id, so a re-import mints a new one. Reusing a tombstoned id was considered and rejected ([#12153](https://github.com/apache/gravitino/issues/12153), closed), because it would bring a deleted table's owner, tags and policies back onto a different object that happens to share the name.

---

## Part 2 — Closing G2: managed entities, worked example on the catalog

The catalog is a good example because it uses everything at once: a strict create, a rename, a state flag, a cascade over a dozen tables, attachments, an in-memory cache, and a live classloader.

### What one catalog write touches

```
CatalogManager (in memory)                 CatalogMetaService (store)
  catalogCache: NameIdentifier → Wrapper     catalog_meta row  (catalog_id PK, unique metalake_id+catalog_name+deleted_at)
    └── CatalogWrapper                       drop cascades over these, all keyed by catalog_id:
          └── IsolatedClassLoader              schema_meta, table_meta, table_column…, fileset_meta, fileset_version,
                └── connector + pools          topic_meta, function_meta, function_version, model_meta, model_version,
                                               model_version_alias, view_meta,
                                               owner_meta, role_meta_securable_object,
                                               tag_relation_meta, policy_relation_meta, statistic_meta
                                             entity_change_log row (one DROP for the whole subtree)
```

Three things follow. The cascade is wide, and it includes the attachments of every child. Children point at the parent by `catalog_id`, so a rename needs no cascade at all. And dropping a catalog also tears down Java objects, which no database rule can help with.

### One problem at a time

**D1 — create is fine; its parent check is not.**
`insertCatalog(entity, overwrite = false)` is a plain insert (`CatalogMetaService:180-203`), so the unique key `(metalake_id, catalog_name, deleted_at)` picks exactly one winner out of two concurrent creates. Good. But the metalake is checked for being alive *before* the insert and outside its transaction (`CatalogManager:615`, `checkMetalake`), so `createCatalog` running against `dropMetalake` can leave a live catalog under a deleted metalake. The order of the two steps inside `createCatalog` is right — `store.put` first, then `catalogCache.get(... createCatalogWrapper ...)` (`CatalogManager:617-621`) — so the loser never builds a connector.

**D2 — alter has a check, but the wrong kind and the wrong result.**
`updateCatalog` reads the row, applies the change, and updates with a **whole-row** condition — `catalog_id`, `catalog_name`, `metalake_id`, `type`, `provider`, `catalog_comment`, `properties`, `audit_info`, `current_version`, `deleted_at` (`CatalogMetaBaseSQLProvider:193-220`) — while `POConverters.updateCatalogPOWithVersion:234-252` **keeps the version the same** (`nextVersion = lastVersion`).

It does notice a concurrent write, but:

- the loser gets `IOException("Failed to update the entity: …")` → HTTP **500**, not 409, and there is no retry;
- "the row changed" and "the row was deleted" look the same — a concurrent drop also updates 0 rows, and should give a 404;
- the version never goes up, so nothing else can build on it, and any later code that expects a plain version check would in fact have no protection;
- the condition compares `properties` and `audit_info` as JSON text, and depends on the `NULL` branches written for nullable columns.

**D3 — the in-use flag is a read-then-write of a property.**
`enableCatalog` and `disableCatalog` load the catalog, look at the flag, and write the whole entity back with `PROPERTY_IN_USE` flipped (`CatalogManager:736-820`). Enable and disable at the same time hit the same problem as D2: one of them gets a 500. Worse, the metalake-level version of this copies the flag into every catalog row outside any transaction, and the code says so: *"we can't make sure we can change all catalog properties in a transaction"* (`MetalakeManager:400-405`). A crash or a race there leaves catalogs that disagree with their metalake.

**D4 — drop has no version check, and its "is it empty" check is outside the transaction.**
`deleteCatalog` looks up the id *before* the transaction starts (`CatalogMetaService:273`), then soft-deletes by `catalog_id` with no version in the condition. The non-cascade path lists schemas and throws `NonEmptyEntityException` **before** `doMultipleWithCommit` (`:362-370`), so:

```text
Tdrop:   list schemas → empty ───────────────────── soft-delete the catalog (commit)
Tcreate:                     insert a schema under the live catalog (commit)
result:  deleted catalog, live schema, and everything created under it later
```

The method also ends with `return true` no matter what happened (`:415`), so the caller cannot tell whether anything was deleted.

**D5 — the cascade is one transaction, but nothing stops a late child.**
The cascade is correctly wrapped in a single `doMultipleWithCommit` (`:276-360`). That is still not enough: a `createSchema` that commits after `softDeleteSchemaMetasByCatalogId` leaves a live schema — and then live tables, filesets and their grants — under a deleted catalog. Since everything is keyed by `catalog_id`, creating a catalog with the same name again gives a **new** id, so those rows can never be seen and never be cleaned.

**D6 — the wrapper can be closed while it is in use.**
`dropCatalog` and `alterCatalog` remove the entry from `catalogCache`, and the removal listener calls `CatalogWrapper.close()`, which closes the `IsolatedClassLoader` and releases its pool (`CatalogManager:387-395`, `:287-308`). Today the metalake **write** lock keeps every other operation in that JVM out. Without the lock, a thread already inside `doWithCatalog` can run against a closed classloader and fail with `NoClassDefFoundError` instead of a clean `NoSuchCatalogException`. **This is the one rule the database cannot express.**

### The change

Four rules first, then how they apply to the catalog.

- **R1 — one transaction per rule.** Everything that has to hold together goes into one `doMultipleWithCommit`, *including looking up ids and checking preconditions*.
- **R2 — touch the row you depend on first.** If a transaction needs a row to stay alive, lock it in the first statement. The database row lock then keeps the racing transactions apart for free, and `0 rows` means the precondition is gone.
- **R3 — same-row updates carry a version check**: `WHERE id = ? AND current_version = ? AND deleted_at = 0`, with the version always going up, and no whole-row or JSON comparison in the condition.
- **R4 — a user create is a plain insert.** `overwrite = true` is only for import and reconcile; the unique key picks the winner.

Applied to the catalog:

| Problem | Change                                                                                                                                                                                                                                                                                                                                                 |
|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| D1      | move the metalake check into the create transaction, as a **shared** lock on the metalake row (`SELECT … FROM metalake_meta WHERE metalake_id = ? AND deleted_at = 0 FOR SHARE`, `FOR UPDATE` on H2). Shared plus shared lets catalog creates run in parallel; shared plus exclusive still blocks a metalake drop                                      |
| D2      | raise `current_version` on every successful write; cut the condition down to `catalog_id + current_version + deleted_at`; on 0 rows, read the row again by id ignoring `deleted_at` and throw either `OptimisticLockException` (409) or `NoSuchCatalogException` (404)                                                                                 |
| D3      | give the in-use flag its own version check on the catalog row; for the metalake case, either update the metalake row and its catalog rows in one transaction, or stop copying the flag and read it from the metalake instead                                                                                                                           |
| D4      | look up the id inside the transaction; make the delete `… SET deleted_at = ? WHERE catalog_id = ? AND current_version = ? AND deleted_at = 0`; move the "is it empty" check inside, after the exclusive lock; return whether a row was really deleted                                                                                                  |
| D5      | the drop transaction takes an **exclusive** lock on the catalog row first, and every child create takes a **shared** lock on the same row. Either the child commits first and the cascade sees it, or the drop commits first and the child fails with `NoSuchCatalogException`. No `FOR UPDATE` is ever held while an external catalog call is running |
| D6      | count the users of `CatalogWrapper`: `close()` runs when the last one leaves, and a lookup after the cache entry is removed builds a new wrapper. This replaces the lock instead of needing one                                                                                                                                                        |

The two halves of the parent rule look like this:

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
  -- non-cascade only: the "is it empty" check goes here, inside the transaction
  UPDATE catalog_meta SET deleted_at = ?
   WHERE catalog_id = ? AND current_version = ? AND deleted_at = 0;   -- 0 rows -> 409
  ... the existing cascade over schema/table/fileset/... and owner/tag/policy/role/statistic ...
COMMIT;
```

One written-down lock order — `metalake → catalog → schema → entity` — plus a fixed row order inside batch updates (`ORDER BY` id, or a `SELECT … FOR UPDATE` first) keeps this free of deadlocks.

### The same four shapes everywhere else

The catalog is one case of four shapes that repeat. Every other managed entity has at least one of them:

| Shape                       | Rule   | Where else                                                                                                                                                                                                                                                                                                                                                               |
|-----------------------------|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| live parent + cascade       | R1, R2 | metalake → catalog, catalog → schema, and **schema → table/fileset/model/topic/view/function**, the level that has no protection at all today                                                                                                                                                                                                                            |
| same-row update             | R3     | fileset and policy raise the version only when a versioned field changed, so a change and a change back is not noticed; topic keeps the version fixed, like catalog; every entity still uses whole-row conditions                                                                                                                                                        |
| plain insert on create      | R4     | more than twenty `insert*OnDuplicateKeyUpdate` call sites; managed schema, fileset and model creates; `addUser`, `addGroup` and `createRole`, where the only thing picking a winner today is a wide namespace TreeLock over a blind upsert                                                                                                                               |
| counter / relation endpoint | R2     | `ModelVersionMetaService.insertModelVersion` reads `model_latest_version` and raises it in a later statement, so two `linkModelVersion` calls end up in one version; the owner unique key in `OwnerMetaService.setOwner` includes `owner_id`, so an object can have two live owners; tag, policy and statistic association looks up endpoint ids outside the transaction |

The fixes follow directly: raise the counter in the **first** statement and read it back inside the transaction; drop `owner_id` from the owner unique key and lock the object row first (with a migration that merges existing duplicates); make every association lock its endpoint inside the transaction.

### What users see

No REST path or payload changes. `OptimisticLockException` goes in `api/.../exceptions` and maps to `409`; `OperationDispatcher.operateOnEntity` (`OperationDispatcher.java:204-226`) must stop hiding it. Retries wrap the **store step only**, never the external call:

| Key                                         | Default | Meaning                                                |
|---------------------------------------------|---------|--------------------------------------------------------|
| `gravitino.entity.store.occ.maxRetries`     | 3       | how many times a managed write is retried before a 409 |
| `gravitino.entity.store.occ.retryBackoffMs` | 10      | first wait between retries, doubling each time         |

Two changes worth a release note: writes that used to overwrite each other quietly now report a conflict, and several paths that returned `500` on a concurrent write now return `409` or `404`.

---

## Part 3 — Closing G3 and G4, then removing the lock

With the four fixes from Part 1 and the rules from Part 2 in place, going through the 157 `doWithTreeLock` call sites outside tests leaves exactly two things the database cannot take over:

| Item                                                                                                                                                                                                                                                                                | What replaces it                                                                                                                                                                                                                                                                                                        |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Catalog wrapper lifetime** (D6): `dropCatalog` and `alterCatalog` can close a classloader that other threads are still using                                                                                                                                                      | count the users of `CatalogWrapper` and close it only when the last one leaves                                                                                                                                                                                                                                          |
| **Order of authorization-plugin calls**: grant and revoke write the store first and call the plugin second, both inside a user/group/role write lock (`AccessControlManager:222-252`, `PermissionManager:81-143`). Ranger is never read back, so a wrong order stays wrong for ever | send the store version with the plugin call and let the newer version win; or keep a small per-principal in-process lock until that exists. Note that the table-level privilege calls in the hook dispatchers already run **outside** the lock (`TableHookDispatcher:115-131`), so they have no protection today either |

Everything else the lock covers is either replaced by the rules above, or was never protected by it: a same-entity `alter` takes a **read** lock, `setOwner` takes a read lock on the principal, `PartitionOperationDispatcher` does not write to the store at all, and import-on-read writes to the store under a lock the other servers cannot see.

**Conclusion: TreeLock goes.** It stops being a correctness tool once F1–F4 and the entity-level parent rule are merged and the wrapper is reference-counted. A small `ConcurrentMap<NameIdentifier, ReadWriteLock>` stays behind the unchanged `doWithTreeLock` signature, to cut down contention and to hold the authorization guard until the version-based plugin call exists.

---

## Task Breakdown

**Order.** Stages 1–3 land with TreeLock **unchanged**, which is safe: TreeLock is always taken before a store transaction opens and released after it closes, so adding row locks and version checks underneath cannot create a new deadlock. Stage 4 is the first stage that changes the lock.

| Stage | Content                                                                      | TreeLock    | Done when                                                        |
|-------|------------------------------------------------------------------------------|-------------|------------------------------------------------------------------|
| 1     | the four level-2 external-catalog bugs (F1–F4)                               | unchanged   | no path can delete or move the attachments of a live entity      |
| 2     | managed entities: parent rule and version checks down to the entity          | unchanged   | two-server tests pass for `metalake → catalog → schema → entity` |
| 3     | counters, relation endpoints, state machines, version checks everywhere else | unchanged   | every managed write is one transaction with a checked condition  |
| 4     | wrapper reference counting, plugin call order, TreeLock removal              | **removed** | one-server and two-server suites pass with the tree gone         |

### Stage 1 — Close G1: the level-2 bugs (can run in parallel)

- [ ] F1 — [#12232](https://github.com/apache/gravitino/issues/12232): do not delete from the store when the external drop returned `false`; make a rename that updated 0 rows visible instead of reporting success
- [ ] F2 — make every drop delete with a CAS on `(id, current_version)` of the entity the call worked on, instead of by name
- [ ] F3 — refuse an import whose `StringIdentifier` already belongs to a live entity with another name or parent: log it, use a new id, and add a metric
- [ ] F4 — reconcile job ([#12155](https://github.com/apache/gravitino/issues/12155)): check the external system, and after N misses and a grace period remove the row together with its attachments; plus relation GC ([#12154](https://github.com/apache/gravitino/issues/12154))
- [ ] Tests: rename × drop keeps the row and its grants; the drop of one table cannot delete the next table with that name; a copied id in `TBLPROPERTIES` does not move an existing row; a recreated table with the same name on a JDBC catalog does not get the old grants

### Stage 2 — Close G2, part one: hierarchy and version checks

**2a — the catalog (the example above), then metalake and schema**

- [ ] D2/D4 — version always goes up; conditions use only id, version and `deleted_at`; a 0-row result becomes `OptimisticLockException` (409) or `NoSuchEntityException` (404); drop returns whether it deleted a row
- [ ] D1/D5 — shared lock on the parent row for create, exclusive for drop, both inside the transaction; move the "is it empty" check inside; look up ids inside the transaction
- [ ] D3 — the in-use flag gets its own version check; the metalake case goes into one transaction, or the flag is read from the metalake instead of copied
- [ ] A fixed row order inside cascade batch updates; one written-down lock order `metalake → catalog → schema → entity`; lock and deadlock timeouts set and tested on MySQL, PostgreSQL and H2

**2b — the entity level (`schema → entity`)** — needs 2a

- [ ] Use the same rule for table, fileset, model, topic, view and function create and rename
- [ ] Include views and functions in every "is it empty" check
- [ ] Two-server tests: child create × cascade drop; child create × non-cascade drop; rename × drop of the target parent; import × parent drop

### Stage 3 — Close G2, part two: counters, endpoints, state machines

- [ ] `insertModelVersion`: raise the counter in the first statement and read it back inside the transaction; key the `deleteModel` cascade on `model_id`
- [ ] Owner unique-key migration (remove `owner_id`) plus a script that merges duplicate live owners; lock the object row first
- [ ] Tag, policy, statistic and role association: look up endpoint ids and lock the endpoint inside the transaction
- [ ] Version always going up, and version-only conditions, for the remaining entities (fileset, policy, topic, user, group, role, tag, view, function, job, statistic)
- [ ] Convert managed schema (`ManagedSchemaOperations:95-119`) and managed fileset (`FilesetCatalogOperations:572`) to a plain insert and map the duplicate to `AlreadyExistsException`; the other managed creates are already strict. Replace the `overwrite` flag with clear intents (`CREATE`, `CREATE_IF_ABSENT`, `IMPORT`, `RECONCILE`) so an import path cannot be reached from a create API
- [ ] Limited retries plus the two config keys; stop `operateOnEntity` from hiding these errors
- [ ] Write down the allowed job state changes and enforce them with a version check
- [ ] Metrics: conflicts, retries, retries used up, create conflicts, parent-lock timeouts and deadlocks, import id collisions, rows removed by reconcile, write P99

### Stage 4 — Close G3–G5 and remove TreeLock

- [ ] Count the users of `CatalogWrapper` so `close()` runs only after the last one leaves; test that a concurrent `dropCatalog` gives `NoSuchCatalogException`, never `NoClassDefFoundError`
- [ ] Decide how the authorization plugin is called: with the store version, or with a small per-principal lock kept in process and written down
- [ ] Check in the list of all 157 `doWithTreeLock` call sites: what each one locks, which rows it touches, what replaces it, which test proves it
- [ ] Remove the locks in `PartitionOperationDispatcher` first — they guard no store write
- [ ] Run the two-server matrix on MySQL, PostgreSQL and H2, review the results, then replace `LockManager` and `TreeLockNode` with a `ConcurrentMap<NameIdentifier, ReadWriteLock>` behind the unchanged `doWithTreeLock` signature
- [ ] Add tests for nested locks — a fixed-size striped lock is not safe here, because `doWithTreeLock` is called inside `doWithTreeLock`, and a parent and a child that land on the same stripe deadlock (read lock, then write lock)
- [ ] Remove the deadlock checker thread and the node cleanup thread; keep the `gravitino.lock.*` configs but ignore them for one release
- [ ] Run the single-server test suite and a contention benchmark; correctness must not change when the helper is turned off in a test build

### Test rules for every stage

- [ ] Drive races with explicit barriers, not with a loop that hopes to hit the timing
- [ ] Check both the API result and the final database state, including soft-deleted rows, generated ids, version rows and every attachment table
- [ ] Run against two Gravitino servers sharing one database; several threads on one server is not proof
- [ ] For external-catalog races, also check the final external state and that the copy catches up
