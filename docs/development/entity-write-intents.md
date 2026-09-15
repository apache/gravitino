---
title: "Entity Write Intents"
slug: "/development/entity-write-intents"
keyword: "entity store, concurrency, create, import, reconcile"
license: "This software is licensed under the Apache License version 2."
---

# Entity write intents

Use `EntityStore.put(entity, EntityWriteIntent)` to express why a registration is being written.
A prior `exists` check is advisory: the database insert decides which concurrent create succeeds.

| Intent | Existing name or ID | Result |
| --- | --- | --- |
| `CREATE` | Any conflict is rejected. | Insert the new entity. |
| `CREATE_IF_ABSENT` | An existing entity at the requested name is retained. An ID conflict at another name is rejected. | Return the inserted or existing entity. |
| `IMPORT` | Only the same name and stable ID may be reused; existing contents are retained. | Return the inserted or existing registration. |
| `RECONCILE` | The observed name, stable ID, and storage version must still match. | Replace the observed registration and advance its version. |

User creates use `CREATE`. Kafka's implicit default schema uses `CREATE_IF_ABSENT`.
Import-on-read uses `IMPORT` and consumes the returned entity so it cannot publish the losing
request's proposed metadata. A copied external Gravitino ID cannot move an existing registration
or revive a tombstoned row. External renames and identity conflicts require explicit recovery;
import is not permission to rebind an ID.

## Conditional reconciliation

The relational store supports write snapshots for schema, table, topic, and view registrations.
Obtain the snapshot **before** reading the external state that will be synchronized:

```java
EntityWriteSnapshot<TableEntity> observed =
    store.getWriteSnapshot(ident, Entity.EntityType.TABLE, TableEntity.class);
Table external = catalog.loadTable(ident);
TableEntity replacement = buildReplacement(observed.entity(), external);
store.put(replacement, EntityWriteIntent.RECONCILE, observed);
```

The snapshot bypasses the entity cache and pairs the current entity with its database version.
Reconciliation checks the stable ID and version while holding the metadata row lock, then uses the
existing transactional update of metadata, history, columns, and cache change log. A stale snapshot
cannot overwrite a newer update, even if that update restores the original values. Do not retry a
conflict with the same external snapshot and a freshly fetched database token: read both again.
Reconciliation cannot rename an entity. Unsupported stores fail explicitly instead of falling back
to an unconditional overwrite.

This guards Gravitino metadata. It does not make an external catalog read transactional with
Gravitino or prevent direct changes in that catalog after the read. TreeLock is not a distributed
lock, and this API does not provide a transaction across the two systems.

## Fileset creation

Fileset creation uses a strict insert with a post-insert action. Only the insert winner runs
`mkdirs`; the action runs before the database transaction commits. A duplicate becomes
`FilesetAlreadyExistsException` and cannot modify the winner's metadata or create its own directory.
If the action fails, the metadata insert is rolled back.

Filesystem operations are not transactional. A failure after creating some locations, or a database
commit failure after directory creation, can leave directories behind. Creation accepts existing
directories on retry. It does not recursively remove such paths because they may already contain
user data. The database transaction holds its locks while the filesystem action runs.

## Compatibility

The boolean `put(entity, overwrite)` overload is deprecated and retained for the transition release.
It preserves historical behavior for existing callers, including unconditional overwrite when
`true` is passed; it does not acquire the guarantees of `RECONCILE`. Production dispatchers and
catalog operations use explicit intents. The single-argument `put(entity)` remains a strict create.
