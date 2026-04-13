# Gravitino Cache Improvement Design

---

## 1. Background

### 1.1 System Overview

Gravitino is a unified metadata management control plane. Compute engines (Spark, Flink, Trino)
call it during query planning to resolve catalog, schema, and table metadata, and to verify
user permissions. The access pattern is distinctly **read-heavy, write-light**: DDL operations
are infrequent, and metadata is resolved once per job.

Gravitino is evolving from single-node to multi-node active-active HA deployment. Each node
currently maintains its own independent in-process Caffeine cache with no cross-node
synchronisation. Under HA, any write on one node leaves other nodes' caches stale until TTL
expiry.

---

### 1.2 Current Cache Architecture Overview

Gravitino maintains three distinct caching layers for the authorization path:

```
┌──────────────────────────────────────────────────────┐
│  Layer 3: Per-request cache (AuthorizationRequestContext) │
│  Scope: one HTTP request; prevents duplicate auth calls   │
├──────────────────────────────────────────────────────┤
│  Layer 2: Auth policy caches (JcasbinAuthorizer)          │
│  loadedRoles  Cache<Long, Boolean>   hook update/TTL      │
│  ownerRel     Cache<Long, Optional<Long>> hook update/TTL │
├──────────────────────────────────────────────────────┤
│  Layer 1: Entity store cache (RelationalEntityStore)      │
│  CaffeineEntityCache — or NoOpsCache when disabled        │
│  Caches entity reads and relation queries for all modules │
│  Controlled by Configs.CACHE_ENABLED                      │
└──────────────────────────────────────────────────────┘
```

**JCasbin is the core of the auth cache system.** It maintains an in-memory policy table:

```
(roleId, objectType, metadataId, privilege) → ALLOW | DENY
```

The Layer 2 caches exist solely to manage JCasbin's policy loading lifecycle:

| Cache                                   | Role                                                                                                           |
|-----------------------------------------|----------------------------------------------------------------------------------------------------------------|
| `loadedRoles: Cache<Long, Boolean>`     | Tracks which roles are already loaded into JCasbin — prevents repeated [C2]+[C3] queries on every auth request |
| `ownerRel: Cache<Long, Optional<Long>>` | Caches owner lookups — **prevents [D1] on every auth request** (2–4 `isOwner()` calls per request, see §1.3.2) |

Without `loadedRoles`, every auth request would re-execute N DB queries to reload all of a
user's role policies into JCasbin. These two caches are the reason the auth path is fast on
the warm path. Layer 1 (entity cache) additionally accelerates the name→ID resolution calls
([A], [B], [C1]) that feed into JCasbin's enforce call.

---

#### 1.2.1 Problems with the Current Entity Cache

**The entity cache (Layer 1) has accumulated significant complexity and is not well-suited to
serve as a general-purpose or auth-dedicated caching layer.**

##### Mixed responsibilities make it hard to maintain

`CaffeineEntityCache` uses a single `Cache<EntityCacheRelationKey, List<Entity>>` to store
three semantically different kinds of data:

| Stored data             | Key form                                         | Example relation types                                    |
|-------------------------|--------------------------------------------------|-----------------------------------------------------------|
| Direct entity           | `(nameIdentifier, entityType, null)`             | any entity: catalog, schema, table, user, role, ...       |
| Relation result set     | `(nameIdentifier, entityType, relType)`          | `ROLE_USER_REL`, `TAG_METADATA_OBJECT_REL`, ...           |
| Reverse index entries   | `ReverseIndexCache` (separate radix tree)        | entity → list of cache keys that reference it             |

On top of this, a `cacheIndex` (radix tree) keeps a prefix-indexed view of all keys to
support cascading invalidation. The resulting invalidation logic (`invalidateEntities`) is a
BFS traversal that walks both the forward index and the reverse index, making it difficult to
reason about correctness and hard to extend safely.

The five relation types currently tracked (`METADATA_OBJECT_ROLE_REL`, `ROLE_USER_REL`,
`ROLE_GROUP_REL`, `POLICY_METADATA_OBJECT_REL`, `TAG_METADATA_OBJECT_REL`) are all
auth-related, which reflects the original design intent: **the entity cache was built
primarily to serve the auth path.** Over time it accumulated relation types and reverse-index
logic without a clear ownership model, making it harder to maintain and evolve.

##### Limited benefit for non-auth interfaces

For general metadata API calls (list catalogs, list schemas, list tables), the entity cache
provides minimal benefit:

| Operation                          | Goes through cache? | Notes                                             |
|------------------------------------|---------------------|---------------------------------------------------|
| `list(namespace, type)`            | **No**              | Bypasses cache entirely; always hits DB           |
| `get(ident, type)` (single entity) | Yes                 | Cache helps on repeated reads of the same entity  |
| `update(ident, type)`              | Invalidate only     | Invalidates entry, write always goes to DB        |
| `listEntitiesByRelation(...)`      | Yes                 | Only for the five auth-centric relation types     |

In practice, the most common metadata browsing operations (`LIST` endpoints) are not cached
at the entity store level. The cache's real workload is the auth path, where the same user
entity, role assignments, and resource IDs are resolved on every single authorization check.

**Conclusion:** The entity cache is a de-facto auth cache dressed up as a general-purpose
cache. Its complexity is unjustified for the non-auth use case, and its TTL-based consistency
model is insufficient for the auth use case (see §1.8). A purpose-built auth cache layer —
separate from the entity store — is the cleaner path forward.

---

### 1.3 JCasbin Authorization — Deep Dive

#### 1.3.1 Call Graph for a Single `authorize()` Check

```
JcasbinAuthorizer.authorize(principal, metalake, metadataObject, privilege)
│
├─ [A] getUserEntity(username, metalake)
│       entityStore.get(USER by NameIdentifier)
│       → Needed to obtain integer userId for JCasbin enforce()
│
├─ [B] MetadataIdConverter.getID(metadataObject, metalake)        ← TARGET RESOURCE
│       entityStore.get(entity by NameIdentifier)
│       → Needed to get integer metadataId for JCasbin enforce()
│       → Called on every auth request
│
├─ [C] loadRolePrivilege(metalake, username, userId, requestContext)
│   │   (guarded by requestContext.hasLoadRole — runs once per HTTP request)
│   │
│   ├─ [C1] entityStore.listEntitiesByRelation(ROLE_USER_REL, userIdentifier)
│   │         → Get all roles assigned to this user
│   │
│   └─ For each role NOT already in loadedRoles cache:
│       ├─ [C2] entityStore.get(RoleEntity by name)   ← async, thread pool
│       └─ loadPolicyByRoleEntity(roleEntity)
│           └─ For each securableObject in role.securableObjects():
│               ├─ [C3] MetadataIdConverter.getID(securableObject, metalake)
│               └─ enforcer.addPolicy(roleId, objType, metadataId, privilege, effect)
│
│   loadedRoles.put(roleId, true)   ← mark role as loaded
│
├─ [D] isOwner() / loadOwnerPolicy(...)   ← called on EVERY auth request (not only OWNER
│   │   privilege checks). Nearly all auth expressions contain ANY(OWNER, METALAKE, CATALOG),
│   │   which expands to METALAKE::OWNER || CATALOG::OWNER || … and calls isOwner() directly
│   │   via OGNL, independently of the authorize() path. Typical call count: 2–4 per request.
│   ├─ Check ownerRel cache → if HIT, return (most non-owner users get Optional.empty())
│   └─ [D1] entityStore.listEntitiesByRelation(OWNER_REL, ...)
│             ownerRel.put(metadataId, Optional.of(ownerId))
│
└─ [E] enforcer.enforce(userId, objectType, metadataId, privilege)   ← in-memory, O(1)
```

#### 1.3.2 What Each Cache Protects

`loadedRoles: Cache<Long, Boolean>` — answers "is this role's policy already in JCasbin?"
Without it, every request re-executes [C2]+[C3] for all roles the user has (N+1 queries).
With it, [C2]+[C3] only run on first load per role. **This is the most critical cache.**

`ownerRel: Cache<Long, Optional<Long>>` — caches ownership lookups for OWNER-privilege
checks. **Contrary to initial analysis, `ownerRel` is consulted on virtually every auth
request**, not only when `privilege == OWNER`. The reason is that nearly every authorization
expression in `AuthorizationExpressionConstants` includes `ANY(OWNER, METALAKE, CATALOG)`
or similar clauses (e.g. `LOAD_TABLE_AUTHORIZATION_EXPRESSION`,
`FILTER_TABLE_AUTHORIZATION_EXPRESSION`, `LOAD_CATALOG_AUTHORIZATION_EXPRESSION`). The
`ANY(OWNER, …)` macro expands to `METALAKE::OWNER || CATALOG::OWNER || …`, and each
`X::OWNER` term calls `isOwner()` directly — a code path that is **independent of
`authorize()`**. As a result, every auth request triggers 2–4 `isOwner()` calls (one per
ancestor level), each consulting `ownerRel`. For most non-owner users, `ownerRel` caches
`Optional.empty()`, which lets the ownership sub-check fail quickly without a DB query.
Without `ownerRel`, every auth request would add 2–4 extra DB queries against `owner_meta`.

**What these caches do NOT protect** (hit DB on every auth request without entity cache):

| Call                                         | Description                               | Protected by      |
|----------------------------------------------|-------------------------------------------|-------------------|
| [A] `getUserEntity()`                        | Fetch User entity → get integer userId    | Entity cache only |
| [B] `MetadataIdConverter.getID()` target     | Resolve target resource name → integer ID | Entity cache only |
| [C1] `listEntitiesByRelation(ROLE_USER_REL)` | Get user's role list                      | Entity cache only |

---

### 1.4 Impact of Disabling Entity Cache

Layer 2 sits **on top of** Layer 1. When Layer 1 is disabled (NoOpsCache), calls [A], [B],
[C1] hit DB on every auth request.

| Call                                             | With entity cache             | Without entity cache            |
|--------------------------------------------------|-------------------------------|---------------------------------|
| [A] `getUserEntity()`                            | Cache hit after first request | **DB query every auth request** |
| [B] `MetadataIdConverter.getID()` target         | Cache hit after first request | **DB query every auth request** |
| [C1] `listEntitiesByRelation(ROLE_USER_REL)`     | Cache hit after first request | **DB query every auth request** |
| [C2] `entityStore.get(RoleEntity)`               | Protected by `loadedRoles`    | DB only on cold role load       |
| [C3] `MetadataIdConverter.getID()` per privilege | Protected by `loadedRoles`    | DB only on cold role load       |
| [D1] `listEntitiesByRelation(OWNER_REL)`         | Protected by `ownerRel`       | **DB query 2–4x per request**   |

---


## 2. Goals

### 2.1 The Two Problems to Solve

**Problem 1 — Performance:** With entity cache disabled, [A] and [C1] hit DB on every auth
request. The new auth cache layer must protect these without relying on entity store cache.
([B] also hits DB, but this is correct and acceptable — see §1.5.)

**Problem 2 — Consistency:** `loadedRoles` is TTL-bounded (1 hour staleness) and updated by hook with in a instance. Permission
changes must take effect at the next auth request, not after TTL expiry.

Both problems are solved by the same mechanism: a version-validated cache for the user's role
list (userId comes for free from the same query).

### 2.2 Requirements

| Goal                            | Requirement                                                                                                   |
|---------------------------------|---------------------------------------------------------------------------------------------------------------|
| HA auth consistency             | Privilege revocations visible on all nodes at the next auth request                                           |
| Auth self-sufficiency           | [A] and [C1] protected without relying on entity store cache                                                  |
| Auth performance                | Hot path: ≤ 3 lightweight DB queries                                                                          |
| No new mandatory infrastructure | Solution requires only the existing DB                                                                        |
| Incremental delivery            | Phase 1 independently shippable                                                                               |

---

## 3. Industry Reference

### 3.1 Apache Polaris — Per-Entity Version Tracking

#### Schema

All entity types (catalogs, namespaces, tables, roles, principals) share a single `ENTITIES`
table (single-table inheritance). The two version columns are the key fields for caching:

```sql
ENTITIES (
  id                     BIGINT,   -- Unique entity ID
  catalog_id             BIGINT,   -- Owning catalog (0 for top-level entities)
  parent_id              BIGINT,   -- Parent entity ID, forms the hierarchy tree
  type_code              INT,      -- Entity type enum (see hierarchy below)
  name                   VARCHAR,
  entity_version         INT,      -- Bumped on rename / property update / drop  ← key
  sub_type_code          INT,      -- Subtype (ICEBERG_TABLE, ICEBERG_VIEW, etc.)
  properties             JSON,     -- User-visible properties (location, format, etc.)
  internal_properties    JSON,     -- Internal properties (credentials, storage config, etc.)
  grant_records_version  INT,      -- Bumped on every GRANT or REVOKE               ← key
)

GRANT_RECORDS (
  securable_catalog_id  BIGINT,
  securable_id          BIGINT,   -- The resource being secured (table/namespace/catalog)
  grantee_catalog_id    BIGINT,
  grantee_id            BIGINT,   -- The principal or role receiving the grant
  privilege_code        INT       -- One of 102 defined privileges
)
```

`GRANT_RECORDS` has no version column of its own. The version fingerprint is stored in
`ENTITIES.grant_records_version` — detecting staleness requires no scan of `GRANT_RECORDS`.

#### Entity Type Hierarchy

```
ROOT
  ├── PRINCIPAL          (user account,      isGrantee)
  ├── PRINCIPAL_ROLE     (user-level role,   isGrantee)
  └── CATALOG
        ├── CATALOG_ROLE (catalog-level role, isGrantee)
        ├── NAMESPACE
        │     └── TABLE_LIKE / POLICY / FILE
        └── TASK
```

Only `PRINCIPAL`, `PRINCIPAL_ROLE`, and `CATALOG_ROLE` are **grantees** (can receive grants).
All others are **securables** (privileges are set on them).

#### How `grantRecordsVersion` Is Maintained

Every `grantPrivilege` / `revokePrivilege` call performs three writes in **one DB transaction**:

1. Insert or delete the `GRANT_RECORDS` row.
2. Increment `grant_records_version` on the **grantee** entity row.
3. Increment `grant_records_version` on the **securable** entity row.

Both sides are bumped atomically — no separate changelog table is needed.

#### Version-Validated Cache

The cache unit is `ResolvedPolarisEntity` = entity metadata + grant records in both directions.
On every request, `bulkValidate()` issues one batch query for all path entities:

```sql
SELECT * FROM ENTITIES WHERE (catalog_id, id) IN ((?, ?), ...)
```

| Path                    | Condition              | Action                                 |
|-------------------------|------------------------|----------------------------------------|
| Cache hit               | Both versions current  | Serve from cache — **0 extra queries** |
| Stale, targeted refresh | Either version behind  | Reload only the changed dimension      |
| Cache miss              | Not in cache           | Full load                              |

The DB is the single source of truth; no broadcast is needed for correctness.

**Key difference from Gravitino:** Polaris bundles entity + grants in one cached object, so one
batch query covers both dimensions. Gravitino separates user→role from role→privilege, requiring
2 version-check queries on a warm hit (see §4.1 Step 1 and Step 3). Both achieve strong
consistency.

### 3.2 Other References

**Nessie** — HTTP fan-out invalidation: async POST to peer nodes on write, convergence < 200 ms.

**Keycloak** — JGroups embedded cluster messaging: in-JVM broadcast, no separate service.
Recommended future direction if Gravitino needs stronger delivery guarantees.

**DB version polling** — monotonic counters incremented in write transaction; a background
thread polls for version changes and proactively invalidates caches. Considered but not
adopted; per-request validation (§4.1) achieves strong consistency without background threads.

---

## 4. Design

### 4.1 Per-Request Version Check (Polaris Style)

Every auth request executes two lightweight version-check queries before serving from cache.
If any version has advanced, only the stale portion is reloaded. Staleness window: **zero**.

#### 4.1.1 Schema Changes

Three new version columns, all `DEFAULT 1` — fully backward compatible. Existing rows get
version 1; first auth check after migration populates caches normally.

```sql
ALTER TABLE `role_meta`
    ADD COLUMN `securable_objects_version` INT UNSIGNED NOT NULL DEFAULT 1
    COMMENT 'Incremented atomically with any privilege grant/revoke for this role';

ALTER TABLE `user_meta`
    ADD COLUMN `role_grants_version` INT UNSIGNED NOT NULL DEFAULT 1
    COMMENT 'Incremented atomically with any role assignment/revocation for this user';

ALTER TABLE `group_meta`
    ADD COLUMN `role_grants_version` INT UNSIGNED NOT NULL DEFAULT 1
    COMMENT 'Incremented atomically with any role assignment/revocation for this group';
```

Write paths that must bump the version **in the same DB transaction**:

| Operation                          | Column                                                  | Location           |
|------------------------------------|---------------------------------------------------------|--------------------|
| Grant / revoke privilege on role R | `role_meta.securable_objects_version WHERE role_id = R` | `RoleMetaService`  |
| Assign / revoke role for user U    | `user_meta.role_grants_version WHERE user_id = U`       | `UserMetaService`  |
| Assign / revoke role for group G   | `group_meta.role_grants_version WHERE group_id = G`     | `GroupMetaService` |

Version comparison uses `!=` (not `<`) to safely handle theoretical INT wrap-around.

**Ownership transfers** require no schema change and no cache. The `ownerRel` cache is
**removed** (see §7.2). Step 2.5 queries `owner_meta` directly with a single batch query on
every auth request — strongly consistent, no versioning complexity needed.

#### 4.1.2 Cache Data Structures (Changes in JcasbinAuthorizer)

```java
// ─── BEFORE ──────────────────────────────────────────────────────────
private Cache<Long, Boolean>           loadedRoles;  // roleId → loaded?
private Cache<Long, Optional<Long>>    ownerRel;

// ─── AFTER ───────────────────────────────────────────────────────────

// NEW: replaces entity cache dependency for [A] (userId) and [C1] (role list).
// Step 1 query returns both user_id and role_grants_version in one shot.
// metalakeName→metalakeId resolved inline via JOIN — no dedicated cache needed.
private GravitinoCache<String, CachedUserRoles> userRoleCache;
// key = metalakeName + ":" + userName

record CachedUserRoles(
    long       userId,            // integer userId for JCasbin enforce()
    int        roleGrantsVersion, // user_meta.role_grants_version at load time
    List<Long> roleIds            // role ID list at load time
) {}

// NEW: mirrors userRoleCache for groups (group can also hold role assignments).
private GravitinoCache<String, CachedGroupRoles> groupRoleCache;
// key = metalakeName + ":" + groupName

record CachedGroupRoles(
    long       groupId,
    int        roleGrantsVersion, // group_meta.role_grants_version at load time
    List<Long> roleIds
) {}

// TYPE CHANGE: was Cache<Long, Boolean>, now stores securable_objects_version.
// Enables version-based staleness detection rather than TTL expiry.
private GravitinoCache<Long, Integer>  loadedRoles;
// roleId → securable_objects_version at the time JCasbin policies were loaded

// REMOVED: ownerRel cache eliminated (see §7.2).
// isOwner() is called 2–4 times per request, but version-validated caching offers no
// query savings: the version check query already returns the owner_id, so there is nothing
// expensive to avoid. Step 2.5 queries owner_meta directly via one batch query per request.
// private Cache<Long, Optional<Long>> ownerRel;
```

**Why no cache for [B] (target resource name→ID):**
Adding a `metadataIdCache` would require invalidation on every entity rename, drop, or
recreate across all entity types. Since JCasbin uses integer IDs (not names), the DB lookup
for [B] is always correct (~1 ms indexed). Simpler and more correct to hit DB every request.

**Why `ownerRel` is removed:**
`isOwner()` is called 2–4 times per auth request (once per ancestor in the OGNL expression
chain). A version-validated cache would still query `owner_meta` on every request to check
versions — and that same query already returns the `owner_id`. There is no expensive
downstream work to avoid (unlike roles, where skipping securable-object loading saves
significant work). Caching adds complexity with zero query savings. Step 2.5 issues one
batch query per request directly against `owner_meta`, achieving strong consistency with
no extra infrastructure. See §7.2.

#### 4.1.3 Auth Check Flow

```
authorize(metalakeName, username, resource, operation)
│
├─ STEP 1 — User version check (1 query, metalake resolved via JOIN):
│
│   SELECT um.user_id, um.role_grants_version
│   FROM user_meta um
│   JOIN metalake_meta mm ON um.metalake_id = mm.metalake_id AND mm.deleted_at = 0
│   WHERE mm.metalake_name = ? AND um.user_name = ? AND um.deleted_at = 0
│   ↑ returns only 2 integer columns — no JSON, no audit fields
│
│   userRoleCache HIT and role_grants_version matches:
│     → use cached userId and roleIds               [A] and [C1] avoided
│
│   MISS or version mismatch:
│     → SELECT role_id FROM user_role_rel WHERE user_id = ? AND deleted_at = 0
│     → re-associate userId ↔ roleIds in JCasbin enforcers
│     → userRoleCache.put(key, new CachedUserRoles(userId, version, roleIds))
│
├─ STEP 2 — Resolve target resource ID (always DB, no cache):
│
│   metadataId = MetadataIdConverter.getID(resource, metalake)  ← 1 indexed DB query
│   Always correct: rename does not change ID; drop+recreate returns the new ID.
│
│   TODO: A strong-consistency name→id cache could eliminate this DB query on the warm
│   path. Version-based validation does not apply here (checking the version requires
│   the same query that returns the ID). A viable approach would require an
│   entity_mutation_log for cross-node invalidation plus write-path eviction on the
│   local node. Not implemented in this phase.
│
├─ STEP 2.5 — Direct ownership batch query (called for EVERY auth request, no cache):
│   (Triggered by ANY(OWNER, …) in the OGNL expression — 2–4 isOwner() calls per request)
│
│   Collect all ancestor metadataIds from the OGNL expression (metalakeId, catalogId, …).
│
│   SELECT metadata_object_id, owner_id FROM owner_meta
│   WHERE metadata_object_id IN (?, ?, …) AND deleted_at = 0
│   ↑ 1 indexed batch query covers all ancestors simultaneously
│
│   For each ancestor: compare owner_id with userId → ALLOW if match, continue if not.
│   No cache read or write — always strongly consistent.
│
├─ STEP 3 — Role batch version check (1 query):
│
│   SELECT role_id, securable_objects_version
│   FROM role_meta WHERE role_id IN (?, ?, ...) AND deleted_at = 0
│   ↑ one query validates all of the user's roles simultaneously
│
│   For each role where loadedRoles.get(roleId) == dbVersion:
│     → policy current; skip                       [C2][C3] avoided
│
│   For stale/cold roles:
│     → allowEnforcer.deleteRole(roleId); denyEnforcer.deleteRole(roleId)
│     → batchListSecurableObjectsByRoleIds(staleRoleIds)  (1 query for all stale roles)
│     → loadPoliciesForRoles(staleObjects)
│     → loadedRoles.put(roleId, dbVersion)
│
└─ STEP 4 — enforce() (in-memory, O(1))
   allowEnforcer.enforce(userId, objectType, metadataId, privilege)
   denyEnforcer.enforce(userId, objectType, metadataId, privilege)
```

#### 4.1.4 Properties

| Dimension           | Value                                                                          |
|---------------------|--------------------------------------------------------------------------------|
| Staleness window    | **0** — every request validates against DB                                     |
| Hot path DB queries | **4** (Step 1 + Step 2 + Step 2.5 owner batch + Step 3 role batch)             |
| Owner check         | Always 1 batch query; no cache, always strongly consistent                     |
| Cold/stale path     | **5–6** queries                                                                |
| Background threads  | **None**                                                                       |
| Failure mode        | DB unavailable → auth blocked (same as today)                                  |
| HA correctness      | **Fixed** — DB queried directly on every request for ownership; role versions  |
|                     | validated via `role_meta.securable_objects_version` in Step 3                  |

#### 4.1.5 Correctness Under Rename and Drop

| Scenario                                      | Analysis                                                                                                                                                                                                                                |
|-----------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **User / Group rename**                       | `userRoleCache` is keyed on `metalakeName:userName`. A rename produces a cache miss → Step 1 queries DB and returns the correct result. The old key has no traffic and expires via TTL. **Safe.**                                       |
| **User / Group drop**                         | Step 1 returns zero rows → auth denied. The old cache entry expires harmlessly. **Safe.**                                                                                                                                               |
| **User / Group drop + same-name recreate**    | The new entity gets a new auto-increment `user_id` and `role_grants_version = 1`. The cached entry holds the old `user_id` and an older version → **version mismatch on the next Step 1 forces a cache refresh.** ✅                     |
| **SecurableObject rename**                    | JCasbin stores integer `metadataId`. Rename does not change the ID. Step 2 resolves the new name to the same ID via DB. `enforce()` matches the existing policy. **No action needed.** ✅                                                |
| **SecurableObject drop**                      | Step 2 returns "not found" → auth denied. Orphan JCasbin policies remain in memory but can never be matched (no ID resolves to the dropped object). **Safe.**                                                                           |
| **SecurableObject drop + same-name recreate** | The new object gets a new `metadataId`. No JCasbin policy covers it → DENY until a new privilege grant bumps `securable_objects_version` in the same transaction and Step 3 detects the version change to reload policies. **Correct.** |

#### 4.1.6 Concurrent Mutation During Auth (TOCTOU)

The version check in Step 1 and the policy reload in Step 3 are not atomic with the
`enforce()` call in Step 4. A concurrent write on another thread or node can advance a
version counter between these steps. This section analyses the bounded impact.

**Scenario A — Role revoked between Step 1 read and Step 3 policy check**

```
Thread A  Step 1: reads role_grants_version = 5 → matches cache → roleIds = [R1, R2]
Thread B  commits: revokes R2 from user → role_grants_version bumped to 6
Thread A  Step 3: checks R1, R2 versions → both current (policies loaded) → skips reload
Thread A  Step 4: enforce() sees R2 policy → may ALLOW using revoked role
```

Thread A's check reflects the DB state at the moment of Step 1. The revoke is visible
on Thread A's **next** request (Step 1 reads version 6 → mismatch → reloads role list →
R2 absent → JCasbin user-role mapping updated → DENY). The inconsistency window is
bounded to **the duration of one auth request** (typically single-digit milliseconds).

Strictly eliminating this window would require serializable isolation across the entire
auth flow, which is impractical at auth-request frequency. The window is acceptable.

**Scenario B — Privilege revoked during policy reload (Step 3)**

```
Thread A  Step 3: detects R1 stale (version N → N+1) → calls deleteRole(R1), reloads
Thread B  commits: another revoke on R1 → version bumped to N+2
Thread A  reads R1 securable objects from DB → may get version-N+1 snapshot
Thread A  stores loadedRoles(R1) = N+1
Next req  Step 3: db version = N+2, cache = N+1 → mismatch → reload again ✅
```

Thread A might serve one request using N+1 policies (missing the N+2 revoke). The
**next** request detects the mismatch and reloads. Bounded to one request window.

**Scenario C — Concurrent reload of the same role by two threads**

Both threads call `deleteRole(R1)` then reload. `SyncedEnforcer` serialises JCasbin
mutations; the second reload overwrites the first with the same (or newer) DB data.
Final state is correct. No correctness issue.

**Summary**

| Race                                       | Worst-case window                   | Acceptability                        |
|--------------------------------------------|-------------------------------------|--------------------------------------|
| Revoke committed after Step 1 version read | Duration of 1 auth request          | Acceptable — next request is correct |
| Revoke committed during Step 3 reload      | Duration of 1 auth request          | Acceptable — next request is correct |
| Concurrent reload of same role             | None (serialised by SyncedEnforcer) | No issue                             |

The TOCTOU window is an inherent property of non-serializable reads in distributed systems.
It applies only during concurrent admin mutations (which are rare in practice), not on
the steady-state auth path.

---

## 5. Phased Implementation Plan

### Phase 1 — Foundation (common to both approaches, no schema changes)

| Step  | Change                                                                               | Module                                               |
|-------|--------------------------------------------------------------------------------------|------------------------------------------------------|
| 1.1   | Fix auth N+1: `batchListSecurableObjectsByRoleIds()` + rewrite `loadRolePrivilege()` | `RoleMetaService`, `JcasbinAuthorizer`               |
| 1.2   | Introduce `GravitinoCache<K,V>` interface; wrap existing Caffeine caches             | `GravitinoCache.java`, `CaffeineGravitinoCache.java` |
| 1.3   | Disable entity store cache: `CACHE_ENABLED` default → `false`                        | `Configs.java`                                       |
| 1.4   | Fix DBCP2: `minEvictableIdleTimeMillis` 1 s → 30 s; `minIdle` 0 → 5                  | `SqlSessionFactoryHelper.java`                       |

**Outcome:** Auth cold path `3+T` queries. Calls [A][C1] hit DB every request (acceptable
as a stepping stone — Phase 2 closes this). Consistency still TTL-bounded.

---

### Phase 2 — Version-Validated Auth Cache Implementation

| Step  | Change                                                                                                                          | Module                                 |
|-------|---------------------------------------------------------------------------------------------------------------------------------|----------------------------------------|
| 2.1   | `ADD COLUMN securable_objects_version` on `role_meta`                                                                           | `schema-x.y.z-*.sql`                   |
| 2.2   | `ADD COLUMN role_grants_version` on `user_meta`                                                                                 | `schema-x.y.z-*.sql`                   |
| 2.3   | `ADD COLUMN role_grants_version` on `group_meta`                                                                                | `schema-x.y.z-*.sql`                   |
| 2.4   | Bump `securable_objects_version` in privilege grant/revoke transaction                                                          | `RoleMetaService`                      |
| 2.5   | Bump `role_grants_version` in role assign/revoke transaction (user + group)                                                     | `UserMetaService`, `GroupMetaService`  |
| 2.6   | Add `userRoleCache: GravitinoCache<String, CachedUserRoles>`                                                                    | `JcasbinAuthorizer`                    |
| 2.7   | Add `groupRoleCache: GravitinoCache<String, CachedGroupRoles>`                                                                  | `JcasbinAuthorizer`                    |
| 2.8   | Change `loadedRoles` type: `Boolean` → `Integer` (stores version)                                                               | `JcasbinAuthorizer`                    |
| 2.9   | Rewrite `loadRolePrivilege()` + `authorize()` with 4-step flow (§4.1.3)                                                         | `JcasbinAuthorizer`                    |
| 2.10  | Add mapper methods (see §6.1)                                                                                                   | mapper + SQL                           |
| 2.11  | Remove `ownerRel` cache; add `selectOwnersByMetadataObjectIds` batch mapper; Step 2.5 queries `owner_meta` directly per request | `JcasbinAuthorizer`, `OwnerMetaMapper` |

**Outcome:** Zero staleness. Hot path: 3 lightweight DB queries (2 version checks + 1 ID lookup).

---


## 6. Implementation Details

### 6.1 Mapper Additions

```java
// RoleMetaMapper.java
void               bumpSecurableObjectsVersion(@Param("roleId") long roleId);
Map<Long, Integer> batchGetSecurableObjectsVersions(@Param("roleIds") List<Long> roleIds);

// UserMetaMapper.java
void                bumpRoleGrantsVersion(@Param("userId") long userId);
Map<String, Object> getUserVersionInfo(
    @Param("metalakeName") String metalakeName, @Param("userName") String userName);

// GroupMetaMapper.java (same pattern as UserMetaMapper)
void                bumpRoleGrantsVersionForGroup(@Param("groupId") long groupId);
Map<String, Object> getGroupVersionInfo(
    @Param("metalakeName") String metalakeName, @Param("groupName") String groupName);

// OwnerMetaMapper.java (for Step 2.5 direct batch query, no version needed)
List<Map<String, Object>> selectOwnersByMetadataObjectIds(
    @Param("metadataObjectIds") List<Long> metadataObjectIds);
```

```xml
<!-- Step 1 query: resolves metalake name inline, returns userId + version -->
<select id="getUserVersionInfo" resultType="map">
  SELECT um.user_id, um.role_grants_version
  FROM user_meta um
  JOIN metalake_meta mm ON um.metalake_id = mm.metalake_id AND mm.deleted_at = 0
  WHERE mm.metalake_name = #{metalakeName} AND um.user_name = #{userName}
  AND um.deleted_at = 0
</select>

<!-- Step 2.5 query: batch ownership lookup, always correct, no version column needed -->
<select id="selectOwnersByMetadataObjectIds" resultType="map">
  SELECT metadata_object_id, owner_id, owner_type FROM owner_meta
  WHERE metadata_object_id IN
  <foreach item="id" collection="metadataObjectIds" open="(" separator="," close=")">#{id}</foreach>
  AND deleted_at = 0
</select>

<!-- Step 3 / poll query: batch version check for roles -->
<select id="batchGetSecurableObjectsVersions" resultType="map">
  SELECT role_id, securable_objects_version FROM role_meta
  WHERE role_id IN
  <foreach item="id" collection="roleIds" open="(" separator="," close=")">#{id}</foreach>
  AND deleted_at = 0
</select>

<update id="bumpSecurableObjectsVersion">
  UPDATE role_meta SET securable_objects_version = securable_objects_version + 1
  WHERE role_id = #{roleId}
</update>

<!-- Step 2.5: direct ownership query, no cache -->
<select id="selectOwnerByMetadataObjectId" resultType="map">
  SELECT owner_id, owner_type FROM owner_meta
  WHERE metadata_object_id = #{metadataObjectId} AND deleted_at = 0
</select>
```

### 6.2 Write Path Changes

**`RoleMetaService` — privilege change (grant or revoke):**
```java
SessionUtils.doMultipleWithCommit(
    () -> securableObjectMapper.softDeleteSecurableObjects(roleId, ...),  // existing
    () -> securableObjectMapper.insertSecurableObjects(newObjects),        // existing
    () -> roleMetaMapper.bumpSecurableObjectsVersion(roleId)              // NEW, same tx
);
```

**`UserMetaService` — role assignment change:**
```java
SessionUtils.doMultipleWithCommit(
    () -> userRoleRelMapper.softDeleteUserRoleRel(userId, roleIds),  // existing
    () -> userRoleRelMapper.insertUserRoleRels(newRelations),        // existing
    () -> userMetaMapper.bumpRoleGrantsVersion(userId)              // NEW, same tx
);
```

**`OwnerMetaService` — ownership transfer:** no write-path changes needed.
`owner_meta` continues to soft-delete the old row and insert a new one unchanged.
Step 2.5 always queries `owner_meta` directly, so there is no cache to invalidate and no
version column to maintain.

The version bump is in the **same transaction** as the data change. If the transaction rolls
back, the version is not incremented — no spurious cache invalidations.

### 6.3 GravitinoCache Interface

```java
public interface GravitinoCache<K, V> extends Closeable {
    Optional<V> getIfPresent(K key);
    void put(K key, V value);
    void invalidate(K key);
    void invalidateAll();
    long size();
}
```

`CaffeineGravitinoCache<K,V>` — wraps Caffeine with configurable TTL and max size.
`NoOpsGravitinoCache<K,V>` — no-op implementation for tests.

---

## 7. Decision Points

### 7.1 Can Phase 1 and Phase 2 Be Merged?

Yes. If the team has capacity, Phase 1 and Phase 2 can ship together. The separation exists
only if there is pressure to disable entity cache before the version infrastructure is ready.

### 7.2 `ownerRel` — Correct Analysis and Design Decision

#### Corrected Access-Frequency Analysis

The original framing — "only consulted when `privilege == OWNER`" — was **incorrect**.
Nearly all authorization expressions in `AuthorizationExpressionConstants` include
`ANY(OWNER, METALAKE, CATALOG)` or `ANY(OWNER, METALAKE, CATALOG, SCHEMA, ...)`, which
expand to a chain of `METALAKE::OWNER || CATALOG::OWNER || ...` checks. Each term calls
`isOwner()` directly via OGNL evaluation, independently of the `authorize()` code path.

Consequence: every auth request triggers **2–4 `isOwner()` calls**, one per ancestor level
(e.g. METALAKE + CATALOG for a catalog-level check; METALAKE + CATALOG + SCHEMA + TABLE for
a table-level check). Removing `ownerRel` without replacement would therefore add **2–4
extra DB queries per auth request**, not +1 as originally stated in §4.1.4. This materially
changes the cost analysis.

#### Why Version-Validated Caching Offers No Benefit for `ownerRel`

The key insight is that `loadedRoles` and `ownerRel` serve fundamentally different purposes:

| Cache | What the version check returns | What it saves |
|-------|-------------------------------|---------------|
| `loadedRoles` | `(role_id, securable_objects_version)` — 2 integers | Skips reloading all securable objects + JCasbin `addPolicy` calls — **expensive** |
| `ownerRel` (hypothetical) | `(metadata_object_id, owner_id, owner_version)` | Nothing — the version check **already returns `owner_id`**, which is the only thing the cache would have stored |

For `ownerRel`, the version check query IS the data query. There is no downstream expensive
operation to avoid. A version-validated cache would add complexity (schema change, cache
reads/writes, invalidation logic) while saving exactly zero DB queries.

#### Decision: Remove `ownerRel`, Query `owner_meta` Directly

Step 2.5 issues one batch query per request against `owner_meta` with no caching:

```sql
SELECT metadata_object_id, owner_id FROM owner_meta
WHERE metadata_object_id IN (metalakeId, catalogId, ...)  -- all ancestors at once
AND deleted_at = 0
```

This is:
- **1 query** regardless of how many ancestor levels are in the expression (vs. 2–4 separate queries with the old per-call pattern)
- **Always strongly consistent** — no staleness, no version columns, no invalidation logic
- **No schema change** — `owner_meta` is untouched

**Cross-node consistency:** Guaranteed by always reading from DB. Ownership transfers are
immediately visible on all nodes with no propagation needed.

**Hot-path query count for Phase 2:**

| Path      | Count                                                              |
|-----------|--------------------------------------------------------------------|
| Hot path  | **4** (Step 1 + Step 2 + Step 2.5 owner batch + Step 3 role batch) |
| All paths | Step 2.5 is always 1 query, never more                             |

### 7.3 Group Role Assignments

`group_meta` is **not optional** — groups can hold role assignments and must have the same
version-validation coverage as users. `ALTER TABLE group_meta ADD COLUMN role_grants_version`
is in §4.1.1, and `groupRoleCache` ships in Phase 2 alongside `userRoleCache`.

### 7.4 Alternative Considered: JcasbinAuthorizer as Distributed Cache

During design review, the question was raised: since auth ultimately loads policies into the
`SyncedEnforcer` on each node, could we treat each `JcasbinAuthorizer` instance as a
distributed cache and maintain cross-node consistency by propagating policy changes?

**What this means in practice:** JCasbin provides a `Watcher` extension interface for exactly
this purpose. When one node calls `addPolicy()` / `removePolicy()`, the watcher broadcasts
the change; peer nodes receive the notification and call `loadPolicy()` to refresh their local
enforcer. Mature implementations exist (`casbin-redis-watcher`, `casbin-kafka-watcher`, etc.).

**Why it was not adopted:**

| Dimension                        | JCasbin Watcher                                        | Per-Request Version Check (chosen)           |
|----------------------------------|--------------------------------------------------------|----------------------------------------------|
| Consistency                      | **Eventual** — broadcast can fail or be lost           | **Strong** — DB validated on every request   |
| Infrastructure                   | Requires Redis / Kafka / etc.                          | Existing DB only                             |
| Cold start / node restart        | Must reload full policy from DB regardless             | Handled naturally; load on first access      |
| Write-path cost                  | Each write triggers full `loadPolicy()` on all peers   | No cross-node cost; each node updates lazily |
| Broadcast failure window         | Unbounded until TTL expiry                             | Not applicable — no broadcast                |
| `UpdatableWatcher` (incremental) | Reduces reload cost but adds implementation complexity | N/A                                          |

**Core problem with the push model:** JCasbin's default `loadPolicy()` is a full reload — every
privilege change causes every peer node to re-fetch all policies from the DB. At scale
(many roles × many securable objects) this is prohibitively expensive. `UpdatableWatcher`
supports incremental updates but its implementation complexity converges toward reinventing
per-request version check while still requiring an external message broker.

**Key insight:** The current design already treats `SyncedEnforcer` as a local cache. The
`loadedRoles`, `userRoleCache`, and `groupRoleCache` caches manage its policy-loading
lifecycle; version numbers decide when to invalidate. The difference from the Watcher approach
is **push vs. pull** — and pull against the existing DB achieves strong consistency without
any additional infrastructure.

**Potential future hybrid:** If the 3 per-request DB queries become a bottleneck, a Watcher
could be added as an **optimistic hint** layer (early notification → skip the version-check
queries on likely-clean requests). The per-request version check must be retained as the
correctness guarantee. This is out of scope for the current phases.

### 7.5 Possible Future Direction: Auth Decision Cache

Not on the current roadmap. Once Phase 2 is stable, caching the final auth decision
`(userId, objectType, metadataId, privilege) → ALLOW|DENY` would reduce the hot path to
zero DB queries. The prerequisite is Phase 2's version-check infrastructure.

---

## 8. Summary

### 8.1 Query Count Comparison

With entity cache **enabled** and all in-process caches warm, the current system serves auth
from pure in-process Caffeine with **0 DB queries**. Phase 2 exchanges this for a small,
bounded query count plus strong HA consistency.

| Scenario                 | Entity cache ON (current)    | Phase 1                   | Phase 2                 |
|--------------------------|------------------------------|---------------------------|-------------------------|
| Hot path — all current   | **0** (fully in-memory)      | 3+ heavy full-row queries | **3 lightweight**       |
| OWNER privilege hot path | 0 (ownerRel warm)            | 1 heavy JOIN              | **4**                   |
| After mutation (stale)   | 0 (**stale — TTL hides it**) | 3+ heavy                  | **4 on next request**   |
| Cold start               | ~3+T                         | ~3+T heavy                | **4–5 lightweight**     |
| HA staleness             | Up to 1 hour                 | Up to 1 hour              | **0**                   |

### 8.2 Change Surface

| Dimension                  | Phase 1      | Phase 2                                |
|----------------------------|--------------|----------------------------------------|
| Entity cache               | **Disabled** | Disabled                               |
| Schema — existing tables   | None         | **+3 columns** (role/user/group_meta)  |
| Schema — new tables        | None         | None                                   |
| New caches                 | None         | **2** (userRoleCache + groupRoleCache) |
| Cache type changes         | None         | **1** (loadedRoles Boolean→Integer)    |
| Removed caches             | None         | **1** (ownerRel)                       |
| Background threads         | None         | **None**                               |
| Invalidation granularity   | —            | Targeted (per request)                 |
| Cache stampede on mutation | —            | None                                   |
| External dependencies      | None         | None                                   |
