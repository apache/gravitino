---
title: "Multi-Node Invalidation for Entity Store Cache"
status: "Draft"
date: "2026-06-18"
---

## Problem

The jcasbin cache already does cross-node invalidation via `entity_change_log` and `EntityChangeLogPoller`. The entity store cache only invalidates locally, so multi-node deployments must set `gravitino.cache.enabled=false`, which hurts some catalogs (Iceberg especially). The goal is to make `enabled=true` usable in multi-node.

What already works and need not change:

- **Writes are strongly consistent.** The write path reads from the DB (not cache) and writes back with an optimistic version lock, so a stale cache cannot cause lost updates.
- **`list(namespace)`** bypasses the cache.
- **Structural entity keys** `(ident, type)` for metalake/catalog/schema/table/topic/model/fileset/view already emit change-log rows on ALTER/DROP; CREATE needs no signal (no negative caching, `list` bypasses cache).

Two gaps this design must close:

1. **Relation keys** `(relType, ident, identType)` emit no change-log at all.
2. **Auth and metadata entities** (`role` / `user` / `group` / `tag` / `policy`) emit no change-log at all today, for either their own ALTER/DROP or their relations (their MetaServices contain zero `insertEntityChange` calls). These entities are cached (directly via `get`, and as relation values via `listEntitiesByRelation`), so their mutations must also gain emit points. This is in scope for both options below.

## The Relation Difficulty

A relation is cached **bidirectionally**: the same `ROLE_USER_REL` can be two keys:

```
(ROLE_USER_REL, role1, ROLE) → [userA, userB]
(ROLE_USER_REL, userA, USER) → [role1, role2]
```

Dropping role1 must also invalidate the reverse keys under each user or group, which cannot be derived from "role1 dropped" alone. The local BFS plus reverse-index handles this on the writing node but **breaks across nodes** (the remote may not have cached role1's direction, so it cannot find the users). The two options below differ in how they solve this.

## Option A: Coarse Invalidation by Metalake

This is the main proposal. On any change-log row for metalake M, the remote wipes all of M's cache keys and lets reads repopulate lazily. Correct because **relations never cross a metalake**, so wiping M covers both directions of any relation a write in M touches.

**How:**

1. Add `invalidateByMetalake(M)` to the cache.
2. Emit one coarse row carrying `metalake_name` at every mutation that lacks one today: the relation mutations (`insertRelation` / `batchInsertRelations` / `updateEntityRelations` / `deleteRelation`) **and** the auth and metadata entity mutations (role/user/group/tag/policy update and delete). Structural-entity ALTER/DROP rows already carry `metalake_name`.
3. Poller and listener: for each polled row, invalidate its metalake (batch by metalake per cycle).

**Notes:**

- The change-log schema is unchanged (`metalake_name` already exists).
- Key format: the identifier uses `.`, the type uses `:`. The metalake's own key is `M:METALAKE`; children are `M.…`. So `invalidateByMetalake` needs two steps: a prefix scan of `"M."` (the `.` boundary avoids hitting sibling `production`) **plus** explicitly removing `"M:METALAKE"`.
- Downside: a single write wipes M's whole working set, so reads reload from the DB. This is bounded to one metalake and softened by the existing per-key `withCacheLock` single-flight and lazy reload. The weak spot is a **write-hot metalake** under continuous DDL, plus a **cluster-synchronized reload** when all nodes poll on the same cadence (see Stampede Mitigation below).

### Stampede Mitigation

Ordered by value; combine the first two:

1. **Generation-based lazy invalidation instead of bulk eviction.** Keep a per-metalake generation counter; stamp each cache entry with the generation at write time. `invalidateByMetalake(M)` just bumps M's counter, with no eviction. On `get`, an entry whose stamp is behind the current generation is treated as a miss and reloaded individually. This turns "evict M's whole working set at once" into "reload each key on its next access", which is naturally spread over time and collapses through the existing per-key single-flight. Only keys actually read get reloaded; cold entries cost nothing until evicted by size or TTL. Cost: one extra `long` per entry plus the generation lookup on read. This is the most effective option because it removes the synchronized burst entirely.
2. **Jitter the poll phase per node.** Randomize each node's initial poll offset (and optionally add small per-cycle jitter) so nodes do not wipe and reload the same metalake at the same instant. Cheap, independent of option 1, and reduces the cluster-synchronized spike.
3. **Refresh-ahead on the reload** (Caffeine `refreshAfterWrite`): serve the existing value while one async load refreshes it. This reduces latency spikes but slightly widens the staleness window, so it is a weaker fit for invalidate-on-change semantics; list it only as a fallback.

Recommendation: option 1 (generation counter) plus option 2 (poll jitter). Together they keep coarse invalidation's small code footprint while removing the synchronized-stampede downside, leaving only the bounded per-key reload cost.

## Option B: Precise Invalidation per Endpoint

This is the alternative. It invalidates only the keys actually affected. Reviewers may prefer it for write-hot metalakes since it has no stampede.

### Key Insight

The relation mutations **already enumerate every endpoint to invalidate**, right there in their local code:

```java
// insertRelation: both endpoints are arguments
cache.invalidate(srcIdentifier, srcType, relType);
cache.invalidate(dstIdentifier, dstType, relType);

// updateEntityRelations: src + all affected dst, all in arguments
cache.invalidate(srcEntityIdent, srcEntityType, relType);
for (toAdd)    cache.invalidate(toAdd, srcEntityType, relType);
for (toRemove) cache.invalidate(toRemove, srcEntityType, relType);
```

So the remote **does not need to discover endpoints** (the hard part above). Encode the same `invalidate(ident, type, relType)` calls into the change-log; the remote decodes and replays them. No reverse-index dependence, nothing breaks.

### Encoding

Add two columns to `entity_change_log`: `change_kind` (`ENTITY`/`RELATION`) and `relation_type` (nullable, only for `RELATION` rows). Each affected endpoint becomes one row, reusing `entity_full_name`/`entity_type`:

```
granting role1 to userA, userB (one updateEntityRelations) produces 3 rows:
 (RELATION, ROLE_USER_REL, ROLE, role1)
 (RELATION, ROLE_USER_REL, USER, userA)
 (RELATION, ROLE_USER_REL, USER, userB)
```

Remote: each row maps to `cache.invalidate(fullName, entity_type, relation_type)`. Bidirectional coverage is automatic (the two directions are separate rows). Cost: a batch op fans out to N rows.

### Mutation Channels to Cover

**Channel 1 (explicit relation mutations)** (`insertRelation` / `batchInsertRelations` / `updateEntityRelations` / `deleteRelation`): next to each local `cache.invalidate(x, t, relType)`, symmetrically emit a row `(RELATION, relType, t, x)`.

**Channel 2 (entity DROP cascading into relations)** (for example, dropping role1 cascades `role_user_rel`): the endpoints live in the cascaded relation rows, not in the delete arguments, so **`SELECT` the opposite endpoints before the cascade soft-delete** and emit one row each. Verified cascade map:

| Deleted Entity | Cascaded Relations | Opposite Endpoints to Emit |
|---|---|---|
| role | `ROLE_USER_REL` / `ROLE_GROUP_REL` / `METADATA_OBJECT_ROLE_REL` | associated users / groups / securable objects |
| user | `ROLE_USER_REL` / `OWNER_REL` | associated roles; owned objects |
| group | `ROLE_GROUP_REL` / `OWNER_REL` | associated roles; owned objects |
| catalog/schema/table/topic/model/fileset | `METADATA_OBJECT_ROLE_REL` / `OWNER_REL` / `POLICY_METADATA_OBJECT_REL` / `TAG_METADATA_OBJECT_REL` | associated roles / owner / policies / tags |
| metalake | all of the above | same (cascaded at the metalake level) |

Channel 2 is the main implementation cost, but the logic is deterministic: whichever relation table is cascade-soft-deleted, query its opposite endpoints and emit them.

**Channel 3 (auth and metadata entity own ALTER/DROP)** (role/user/group/tag/policy): these emit nothing today, so add `(ENTITY, ALTER|DROP, type, ident)` rows for their own attribute changes, the same way structural entities already do.

**Notes:**

- No stampede; immune to write-hot metalakes.
- Downside: much more code (Channel 2 across every MetaService cascade) plus a schema change plus a row fan-out per batch op.

## Comparison

| | A: Coarse by Metalake | B: Precise per Endpoint |
|---|---|---|
| Scope wiped | whole metalake | only affected keys |
| Schema | unchanged | two new columns |
| Relation mutation | 1 coarse row | N precise rows |
| Entity DROP cascade | free (metalake wiped) | needs endpoint SELECT |
| Code size | small | large |
| Stampede | one metalake; mitigated | none |
| Weak spot | write-hot metalake | code surface |

**Recommendation:** ship A first (smallest surface, reuses schema, fastest path to multi-node). Keep B for write-hot metalakes if they prove a problem; same consistency model, and B can replace A per metalake later.

## Applies to Both

1. **Change-log writes share the backend mutation's transaction** (the existing `insertEntityChange` already does), or concurrency can drop signals or endpoints.
2. **Self-node replay is idempotent.** The writing node invalidated locally, then its own poller replays once more; re-invalidating is a no-op. (Same as jcasbin.)

## Tests

- A: a row for M invalidates all `M.` keys plus `M:METALAKE`, and leaves sibling `prod`/`production` untouched.
- B: each cascade enumerates the right endpoints; remote replay drops exactly the right keys.
- Multi-node e2e: node A runs grant/revoke/drop role, setOwner, and attach tag, then node B sees fresh `listEntitiesByRelation` (both directions) within one poll interval.
- Regression: entity-key behavior and write-path and `list` strong consistency are unchanged.
