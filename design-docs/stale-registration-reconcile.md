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

# Design: Reconcile Stale Registrations of Non-Managed Entities

---

## 1. Background

For non-managed catalogs (JDBC, Hive, Iceberg, Kafka, ...) the source system is
the source of truth. Gravitino keeps only a registration row per
schema/table/view/topic/fileset to attach owners, tags, policies, audit
information and properties.

Registrations are kept in sync only on Gravitino's own write path. When an
object is created, renamed or dropped directly in the source, nothing
reconciles the registration:

- A dropped schema stays live in `schema_meta`. Consumers that walk the
  catalog (dashboard metrics, lineage, search sync) resolve the name from the
  store and then fail on `loadSchema` with 404. See
  [#13279](https://github.com/apache/gravitino/issues/13279) for the
  drop-side symptom.
- `TableOperationDispatcher` and `SchemaOperationDispatcher` deliberately
  preserve a registration when the source drop reports `false`, because a
  `false` is ambiguous between "renamed" and "dropped out of band". A true
  out-of-band drop therefore always leaves a stale row.

A few code paths already delete registrations directly through
`EntityStore.delete` when they notice the source object is gone (e.g.
`IcebergTableHookDispatcher.deleteTableEntity`,
`SchemaEntityCleaner.deleteOrphanedSchemaEntities`). These ad-hoc deletes
bypass the dispatcher chain, so they skip secret cleanup, authorization-plugin
privilege removal, `Drop*Event` emission and orphan cleanup, and when run from
another process they race with concurrent creates because tree locks are per
JVM.

This design proposes one server-side reconciliation mechanism that removes
stale registrations through the dispatcher chain, so a reconcile-triggered
removal behaves exactly like an explicit drop.

---

## 2. Goals

1. Define what "stale" means per entity type and how to confirm absence in the
   source safely.
2. Add a server-side reconcile task for schemas in non-managed catalogs that
   removes stale registrations through `SchemaDispatcher`, with events,
   secret cleanup, authorization-plugin privilege removal and orphan cleanup.
3. Provide both a periodic trigger and an on-demand REST API, with a policy
   switch between report-only and auto-remove.
4. Expose stale registrations via REST so administrators can inspect them
   before removal.

---

## 3. Non-Goals

- Tables, views, topics and filesets: the mechanism is designed to extend to
  them, but the first implementation covers schemas only.
- UI surfacing: the REST response carries everything a UI needs, but no web
  page is added in this phase.
- Changing `list*` behavior for non-managed catalogs (tracked separately in
  the epic).
- The reverse direction (object exists in source but is not registered):
  already handled by lazy import on `loadSchema`/`loadTable`.
- Managed catalogs (fileset, model, generic-lakehouse): Gravitino owns the
  storage there, no source drift is possible.

---

## 4. Existing Architecture Overview

### 4.1 Dispatcher chain

```
REST → EventDispatcher → NormalizeDispatcher → HookDispatcher → OperationDispatcher
```

Dropping a schema through `SchemaDispatcher` currently does all of the
following:

- `SchemaOperationDispatcher.dropSchema`: deletes from the source catalog,
  deletes the store registration, cleans write-through secrets via
  `SecretManager.deleteSecretsFromProperties`, all under a WRITE tree lock on
  the catalog node (`TreeLockUtils.doWithTreeLock`). (On cascade, filesets
  are dropped via `FilesetDispatcher` first, before the catalog lock is
  acquired, so each fileset cleans its own write-through secrets and no
  nested tree locks are taken.)
- `SchemaHookDispatcher`: post-drop, calls
  `authorizationPluginRemovePrivileges` so Ranger plugins drop privileges.
- `SchemaEventDispatcher`: emits `DropSchemaEvent` for audit, search index
  and webhooks.

### 4.2 What #13279 already added

`SchemaOperationDispatcher.dropSchema(ident, cascade=true)` removes the
registration even when the source schema is already absent, reports
`dropped: true` if either side was removed, and cleans stored secrets.
Non-cascading drops keep the old conservative behavior.

This gives reconcile an existing, fully-wired removal entry point: removing a
stale schema is a cascading drop where the source side is already gone.

### 4.3 Existence probes

Connectors expose explicit per-object existence probes
(`SupportsSchemas.schemaExists`, `TableCatalog.tableExists`, ...). These are
single-object probes, not listings, so they are not affected by listing
permission filters.

### 4.4 Background task pattern

There is no central scheduler. Server subsystems each own a
`ScheduledExecutorService` with `start()`/`close()`, wired in
`GravitinoEnv.initGravitinoServerComponents()`, with interval config keys in
`Configs`. `RelationalGarbageCollector` is the closest reference.

---

## 5. Proposed Design

### 5.1 Definition of stale

A registration is stale for a given entity when all of the following hold:

1. The owning catalog does not manage storage for that entity scope
   (`catalog.capabilities().managedStorage(Capability.Scope.SCHEMA).supported()`
   is false; same pattern as `CatalogManager.isManagedStorageCatalog`).
2. An explicit existence probe against the source reports the object absent
   (`NoSuch*Exception` from the probe, never a missing entry in a listing).
3. The probe did not fail for infrastructural reasons (connection failure,
   timeout, authentication): those mark the catalog "unreachable" and skip
   all removals for that catalog in this round.

### 5.2 StaleSchemaReconciler

New class `StaleSchemaReconciler` in `core`, following the
`RelationalGarbageCollector` pattern:

```
every reconcileIntervalSecs (default: disabled):
  for each non-managed catalog in each metalake:
    try to initialize/probe the catalog:
      on connection failure -> log, mark unreachable, skip catalog
    for each schema registration in the catalog (from the entity store):
      if schemaExists(name) reports absent:
        record as stale
        if policy == AUTO_REMOVE:
          dispatcher.dropSchema(ident, cascade = true)
```

For catalogs with hierarchical namespaces (e.g. Iceberg), the store is
enumerated per namespace level, and each registered name is expanded to its
ancestor chain via `HierarchicalSchemaUtil.allScopes` so registrations at
every level are probed. When a parent and its children are all stale, the
removal follows `SchemaEntityCleaner`'s approach: locate the outermost stale
schema and drop it with cascade = true, carrying the descendants with it.

The periodic task will run removals under a dedicated system identity (e.g.
`reconciler`), so `DropSchemaEvent` audit records are distinguishable from
user-initiated drops (today the event user comes from
`PrincipalUtils.getCurrentUserName()`, which has no login context on a
background thread). See Open Question #3.

Removal goes through `SchemaDispatcher` (the full chain in 4.1), inside the
server JVM, so tree locking, events, secret cleanup, authorization-plugin
privilege removal and orphan cleanup behave exactly as an explicit cascading
drop.

Per-catalog error isolation: a failing catalog never blocks or aborts
reconcile of other catalogs.

### 5.3 Configuration

| Key | Default | Meaning |
| --- | ------- | ------- |
| `gravitino.reconcile.enabled` | `false` | Master switch for the periodic task |
| `gravitino.reconcile.intervalSecs` | `3600` | Period between reconcile rounds |
| `gravitino.reconcile.policy` | `report-only` | `report-only` or `auto-remove` |

The on-demand API is always available regardless of `enabled`; the policy
applies to both triggers.

### 5.4 REST API

Two endpoints on `CatalogOperations` (`server/.../web/rest`), following the
`testExistingConnection` pattern:

```
POST /api/metalakes/{metalake}/catalogs/{catalog}/reconcile
```

Runs reconcile for one catalog synchronously. Request body carries an optional
`dryRun` (default: follow server policy). Returns the stale report.

```
GET /api/metalakes/{metalake}/catalogs/{catalog}/stale-schemas
```

Returns the stale report for one catalog. The endpoint runs a live
report-only scan on each call — there is no server-side report cache, so the
result is correct on multi-node deployments. Caching may be added later as an
optimization.

Response DTO (`common/.../dto/responses/StaleEntitiesResponse`):

```json
{
  "catalog": "catalog1",
  "catalogUnreachable": false,
  "staleSchemas": [
    { "name": "db1", "detectedAt": 1727000000000, "removed": false }
  ]
}
```

Authorization: `@AuthorizationExpression(expression = "ANY(OWNER, METALAKE, CATALOG)",
accessMetadataType = MetadataObject.Type.CATALOG)` on both endpoints.

---

## 6. Safety Considerations

1. **Never delete on doubt.** Absence must come from an explicit probe
   returning `NoSuch*Exception`. Connection errors, timeouts and auth failures
   skip the whole catalog for that round. Listings are never used as absence
   evidence.
2. **Renames are not removals.** An object renamed in the source keeps its
   `gravitino.identifier` property in the renamed object, and the
   rename-recovery half of this mechanism already exists:
   `SchemaOperationDispatcher.importSchema` reuses the stored `StringIdentifier`
   uid when lazy-importing a renamed schema ("this could be happened when
   Schema is renamed by external systems not controlled by Gravitino. In this
   case, we need to overwrite the stored entity to keep consistency."). So the
   renamed object comes back under its new name with the original uid. What is
   missing is the old name's registration: the probe for the old name reports
   absent, and deleting that registration loses the Gravitino-only metadata
   attached to it. The first implementation accepts this trade-off; matching
   the stale registration to the renamed object via `gravitino.identifier` is
   an extension of the existing recovery path, left to a follow-up (see Open
   Questions).
3. **Tree locking.** All removals run inside the server JVM through the
   dispatcher, which acquires the WRITE tree lock on the catalog node, so
   reconcile removals cannot race with concurrent creates. Note this lock is
   catalog-scoped: each removal acquires the catalog WRITE lock individually,
   so a reconcile round removing many schemas repeatedly blocks other writes
   to that catalog — which is one motivation for the rate limiting in Open
   Question #5.
4. **Default posture is safe.** Reconcile is disabled by default; when
   enabled, the default policy is report-only. Auto-removal is an explicit
   operator choice.
5. **Observability.** Each round logs: catalogs scanned, catalogs skipped as
   unreachable, stale schemas found, removals attempted/succeeded. A metric
   for stale-count per catalog can be added once the shape settles.

---

## 7. Open Questions

1. **Rename handling.** Should reconcile try to match a stale registration to
   a renamed source object via the `gravitino.identifier` property (probe
   reports absent for the old name, but the renamed object carries the same
   identifier — see Safety #2 for the existing lazy-import recovery), and
   re-point the registration instead of deleting it? This preserves
   tags/owners across renames but requires a listing scan per catalog.
2. **Report caching.** The GET endpoint runs a live report-only scan per
   call. If probing cost becomes a problem on large catalogs, should a
   short-lived cached report be added, and where (memory is wrong on
   multi-node)?
3. **System identity.** The periodic task needs a principal for the event
   chain (today a background thread has no login user). Introduce a dedicated
   system identity (e.g. `reconciler`), and should it be visible/excluded in
   authorization audit?
4. **Per-catalog overrides.** Is a global policy enough, or do operators need
   per-catalog policy (e.g. auto-remove for a dev catalog, report-only for
   production)?
5. **Rate limiting.** Should a reconcile round cap the number of removals per
   run to bound blast radius of a misbehaving probe?
