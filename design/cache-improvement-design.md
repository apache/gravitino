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
│  loadedRoles  Cache<Long, Boolean>        TTL 1 hour      │
│  ownerRel     Cache<Long, Optional<Long>> TTL 1 hour      │
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

| Cache | Role |
|-------|------|
| `loadedRoles: Cache<Long, Boolean>` | Tracks which roles are already loaded into JCasbin — prevents repeated [C2]+[C3] queries on every auth request |
| `ownerRel: Cache<Long, Optional<Long>>` | Caches owner lookups for OWNER-privilege checks — prevents [D1] on every ownership check |

Without `loadedRoles`, every auth request would re-execute N DB queries to reload all of a
user's role policies into JCasbin. These two caches are the reason the auth path is fast on
the warm path. Layer 1 (entity cache) additionally accelerates the name→ID resolution calls
([A], [B], [C1]) that feed into JCasbin's enforce call.

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
├─ [D] loadOwnerPolicy(...)        ← only called when privilege == OWNER
│   ├─ Check ownerRel cache → if HIT, return
│   └─ [D1] entityStore.listEntitiesByRelation(OWNER_REL, ...)
│             ownerRel.put(metadataId, Optional.of(ownerId))
│
└─ [E] enforcer.enforce(userId, objectType, metadataId, privilege)   ← in-memory, O(1)
```

#### 1.3.2 What Each Cache Protects

`loadedRoles: Cache<Long, Boolean>` — answers "is this role's policy already in JCasbin?"
Without it, every request re-executes [C2]+[C3] for all roles the user has (N+1 queries).
With it, [C2]+[C3] only run on first load per role. **This is the most critical cache.**

`ownerRel: Cache<Long, Optional<Long>>` — caches [D1] results. Only consulted when
`privilege == OWNER`; regular privilege checks (SELECT, CREATE, ALTER, ...) never touch it.

**What these caches do NOT protect** (hit DB on every auth request without entity cache):

| Call | Description | Protected by |
|------|-------------|-------------|
| [A] `getUserEntity()` | Fetch User entity → get integer userId | Entity cache only |
| [B] `MetadataIdConverter.getID()` target | Resolve target resource name → integer ID | Entity cache only |
| [C1] `listEntitiesByRelation(ROLE_USER_REL)` | Get user's role list | Entity cache only |

---

### 1.4 Impact of Disabling Entity Cache

Layer 2 sits **on top of** Layer 1. When Layer 1 is disabled (NoOpsCache), calls [A], [B],
[C1] hit DB on every auth request.

| Call | With entity cache | Without entity cache |
|------|------------------|----------------------|
| [A] `getUserEntity()` | Cache hit after first request | **DB query every auth request** |
| [B] `MetadataIdConverter.getID()` target | Cache hit after first request | **DB query every auth request** |
| [C1] `listEntitiesByRelation(ROLE_USER_REL)` | Cache hit after first request | **DB query every auth request** |
| [C2] `entityStore.get(RoleEntity)` | Protected by `loadedRoles` | DB only on cold role load |
| [C3] `MetadataIdConverter.getID()` per privilege | Protected by `loadedRoles` | DB only on cold role load |
| [D1] `listEntitiesByRelation(OWNER_REL)` | Protected by `ownerRel` | DB only on first owner check |

---

### 1.5 What Needs Caching and What Does Not

**[B] — target resource name→ID — does NOT need a new cache.**

JCasbin stores entity **integer IDs** in policies (see §1.6). Entity rename does not change
the ID — DB lookup always returns the correct integer ID. Adding a `metadataIdCache`
(name→ID) would require invalidation on every entity rename or drop across all entity types
(catalog, schema, table, etc.) — a massive, fragile write-path change with no benefit.
A single indexed DB lookup (~1 ms) is simpler and always correct.

**[C1] — user's role list — DOES need a cache, with version validation.**

Expensive JOIN query, hard consistency requirement: role revocation must take effect
immediately, not after TTL expiry.

**[A] — userId — is handled for free by the [C1] cache.**

The user version check query (§4.1.3 Step 1) already returns `user_id`. After the first
auth request, userId is cached as part of `CachedUserRoles`. No separate userId cache needed.

**metalakeName → metalakeId — handled by a JOIN in the Step 1 query.**

`metalake_meta` is tiny (few rows per deployment); an inline JOIN resolves it without a
separate lookup or cache.

---

### 1.6 Key Finding: JCasbin Stores Entity IDs, Not Names

```java
// JcasbinAuthorizer.loadPolicyByRoleEntity()
allowEnforcer.addPolicy(
    String.valueOf(roleEntity.id()),
    securableObject.type().name(),
    String.valueOf(MetadataIdConverter.getID(securableObject, metalake)),  // integer ID
    privilege.name().toUpperCase(),
    condition.name().toLowerCase());
```

JCasbin policy tuples use **integer entity IDs** throughout. Consequences:
- Entity renames do not affect loaded policies (ID is stable under rename).
- Auth cache staleness is caused **only** by: privilege grant/revoke, role assignment /
  revocation, ownership change. DDL (rename, drop) requires no auth cache update.
- Name→ID resolution at auth time ([B]) always goes to DB — correct and required.

---

### 1.7 Correctness Under Rename and Drop

| Scenario | Analysis |
|----------|----------|
| **User / Group rename** | `userRoleCache` key = `metalakeName:userName`. New name → cache miss → Step 1 queries DB and returns correct result. Old key has no traffic and expires via TTL. **Safe.** |
| **User / Group drop** | Step 1 returns zero rows → auth denied. Old cache entry expires harmlessly. **Safe.** |
| **User / Group drop + same-name recreate** | New entity gets a new auto-increment `user_id`, `role_grants_version = 1`. Cached entry has old `user_id` and a different version → **version mismatch forces cache refresh.** ✅ |
| **SecurableObject rename** | JCasbin stores integer `metadataId`. Rename does not change ID. Step 2 resolves new name to same ID via DB. `enforce()` matches existing policy. **No action needed.** ✅ |
| **SecurableObject drop** | Step 2 returns "not found" → auth denied. Orphan JCasbin policies remain in memory but can never be matched. **Safe.** |
| **SecurableObject drop + same-name recreate** | New object gets new `metadataId`. No JCasbin policy covers it → DENY until a new grant bumps `securable_objects_version` and triggers a policy reload. **Correct.** |

---

### 1.8 Auth N+1 Problem

`loadRolePrivilege()` executes [C2] per role not in `loadedRoles`. Before the batch fix:

```
[C1]: 1 query (list roles for user)
[C2]: 1 query per role (get RoleEntity)            ← N queries
[C3]: 2 queries per role (securable objects)       ← 2N queries
```

After `batchListSecurableObjectsByRoleIds()`: `3 + T` total on cold cache.

---

### 1.9 HA Consistency Gap

```
Node A: REVOKE privilege P from role R  →  DB updated; Node A loadedRoles evicted ✅
Node B: authorize(user U, resource X)   →  role R still in loadedRoles ← stale ❌
                                            U retains revoked access for up to 1 hour
```

---

## 2. Goals

### 2.1 The Two Problems to Solve

**Problem 1 — Performance:** With entity cache disabled, [A] and [C1] hit DB on every auth
request. The new auth cache layer must protect these without relying on entity store cache.
([B] also hits DB, but this is correct and acceptable — see §1.5.)

**Problem 2 — Consistency:** `loadedRoles` is TTL-bounded (1 hour staleness). Permission
changes must take effect at the next auth request, not after TTL expiry.

Both problems are solved by the same mechanism: a version-validated cache for the user's role
list (userId comes for free from the same query).

### 2.2 Requirements

| Goal | Requirement |
|------|-------------|
| HA auth consistency | Privilege revocations visible on all nodes at the next auth request (or within poll interval for Approach B) |
| Auth self-sufficiency | [A] and [C1] protected without relying on entity store cache |
| Auth performance | Hot path: ≤ 3 lightweight DB queries (Approach A) or ≤ 1 (Approach B) |
| No new mandatory infrastructure | Solution requires only the existing DB |
| Incremental delivery | Phase 1 independently shippable |

### 2.3 Staleness Tolerance

| Data Type | Effect When Stale | Approach A | Approach B |
|-----------|------------------|------------|------------|
| Role privileges | Revoked access still granted | **0** | ≤ poll interval |
| User role assignments | Revoked roles still active | **0** | ≤ poll interval |
| Ownership | Transfer not reflected | **0** (direct query) | **0** (direct query) |
| Table / schema existence | Object visibility inconsistency | ≤ 2 s | ≤ 2 s |

---

## 3. Industry Reference

### 3.1 Apache Polaris — Per-Entity Version Tracking

Polaris achieves strong consistency by embedding two version counters on every entity
(`entityVersion` and `grantRecordsVersion`) and validating them on each cache access:

| Path | Condition | DB queries |
|------|-----------|-----------|
| Cache hit | Both versions current | **0** |
| Stale, targeted refresh | Either version behind | **1** — returns only the changed part |
| Cache miss | Not in cache | **1** — full load |

`loadEntitiesChangeTracking(ids)` issues one lightweight query returning only integer version
columns for a batch of IDs — the same pattern used in Approach A's Step 3 below.

**Key difference from Gravitino:** Polaris bundles entity + grants in one cached object, so
one batch query validates both dimensions. Gravitino separates user→role from role→privilege,
requiring 2 version-check queries on a warm hit. Both achieve strong consistency.

### 3.2 Other References

**Nessie** — HTTP fan-out invalidation: async POST to peer nodes on write, convergence < 200 ms.

**Keycloak** — JGroups embedded cluster messaging: in-JVM broadcast, no separate service.
Recommended future direction if Gravitino needs stronger delivery guarantees.

**DB version polling** — monotonic counters incremented in write transaction; a background
thread polls for version changes and proactively invalidates caches. Directly applicable as
Approach B below.

---

## 4. Design Approaches

Both approaches share the same schema changes and cache data structures. They differ only
in **when** version validation is performed: inline on every auth request (Approach A) or
proactively by a background thread (Approach B).

---

### 4.1 Approach A — Per-Request Version Check (Polaris Style)

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

| Operation | Column | Location |
|-----------|--------|----------|
| Grant / revoke privilege on role R | `role_meta.securable_objects_version WHERE role_id = R` | `RoleMetaService` |
| Assign / revoke role for user U | `user_meta.role_grants_version WHERE user_id = U` | `UserMetaService` |
| Assign / revoke role for group G | `group_meta.role_grants_version WHERE group_id = G` | `GroupMetaService` |

Version comparison uses `!=` (not `<`) to safely handle theoretical INT wrap-around.

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
// OWNER privilege checks query owner_meta directly (Step 2.5 below).
// private Cache<Long, Optional<Long>> ownerRel;
```

**Why no cache for [B] (target resource name→ID):**
Adding a `metadataIdCache` would require invalidation on every entity rename, drop, or
recreate across all entity types. Since JCasbin uses integer IDs (not names), the DB lookup
for [B] is always correct (~1 ms indexed). Simpler and more correct to hit DB every request.

**Why `ownerRel` is removed:**
`ownerRel` has the same HA staleness problem as `loadedRoles` but cannot be easily
version-validated (`owner_meta` uses soft-delete; new rows always start at version 1).
`ownerRel` is only consulted for `privilege == OWNER`. Since Step 2 already resolves
`metadataId`, one direct indexed query on `owner_meta` (Step 2.5) gives strong consistency
for OWNER checks at the cost of 1 extra query, only on OWNER checks. See §7.2.

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
├─ [Only when privilege == OWNER] STEP 2.5 — Query ownership directly (no cache):
│
│   SELECT owner_id, owner_type FROM owner_meta
│   WHERE metadata_object_id = ? AND deleted_at = 0
│   (metadataId already known from Step 2; indexed on metadata_object_id)
│   → Compare owner_id with userId; return ALLOW/DENY immediately.
│   Non-OWNER privilege checks skip Step 2.5 entirely.
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

| Dimension | Value |
|-----------|-------|
| Staleness window | **0** — every request validates against DB |
| Hot path DB queries | **3** (Step 1 + Step 2 + Step 3; Steps 1 and 3 return integer cols only) |
| OWNER privilege hot path | **4** (+ Step 2.5 indexed owner_meta query) |
| Cold/stale path | **4–5** queries |
| Background threads | **None** |
| Failure mode | DB unavailable → auth blocked (same as today) |
| HA correctness | **Fixed** — every node checks DB version on every request |

---

### 4.2 Approach B — Background Poll + Proactive Invalidation

A background thread on each node periodically detects auth mutations and proactively
invalidates stale cache entries before any auth request arrives. The hot path never issues
version-check queries. Staleness window: **≤ poll interval** (configurable).

The critical difference from Approach A: Approach A needs fine-grained per-entity versions
to know **exactly what changed** (this specific user? this specific role?) so it can reload
only the stale part inline. Approach B only needs to know **whether anything changed**, then
proactively invalidates before the next request. This means the schema can be much simpler.

#### 4.2.1 Schema Changes — Two Options

**Option B-Global: one new table, no changes to existing tables**

```sql
CREATE TABLE `auth_global_version` (
    `metalake_id`  BIGINT UNSIGNED NOT NULL COMMENT 'metalake this version tracks',
    `version`      BIGINT UNSIGNED NOT NULL DEFAULT 1
                   COMMENT 'monotonically increasing; bumped by any auth mutation in this metalake',
    PRIMARY KEY (`metalake_id`)
);
```

Write path — **every** auth mutation (grant/revoke privilege, assign/revoke role, transfer
ownership) adds one UPDATE in the same transaction:

```sql
-- in the same transaction as the actual grant/revoke/assign
UPDATE auth_global_version SET version = version + 1 WHERE metalake_id = #{metalakeId}
```

Background poll:
```sql
SELECT version FROM auth_global_version WHERE metalake_id = ?
```
→ If version advanced → invalidate **all** auth cache entries for this metalake (coarse).

**Trade-off:** Any single auth mutation invalidates all users' and all roles' cache entries
for the metalake. The next batch of auth requests all experience cache misses simultaneously
(cache stampede). For most deployments where auth mutations are rare, this is acceptable.
For high-churn environments, Option B-Fine below is better.

---

**Option B-Fine: same per-table versions as Approach A (fine-grained)**

Identical schema changes as §4.1.1 (three `ALTER TABLE` statements). The background poll
batch-checks `role_grants_version` and `securable_objects_version` and invalidates only the
specific users and roles that changed — no cache stampede.

```sql
-- Poll query 1: batch check all cached users
SELECT user_id, role_grants_version FROM user_meta
WHERE user_id IN (...all cached user IDs...) AND deleted_at = 0

-- Poll query 2: batch check all cached roles
SELECT role_id, securable_objects_version FROM role_meta
WHERE role_id IN (...all cached role IDs...) AND deleted_at = 0
```

**Trade-off:** Requires 3 schema changes (same as Approach A). Targeted invalidation —
only stale users and roles are evicted, all other cache entries remain valid.

#### 4.2.2 Cache Data Structures

**Identical to Approach A** — `userRoleCache`, `groupRoleCache`, `loadedRoles` (Boolean→
Integer), `ownerRel` removed. The cache structures are the same; only the invalidation
mechanism differs.

#### 4.2.3 Background Poll Thread

```java
// Runs every poll_interval seconds (configurable, default 5 s)
class AuthCachePollThread implements Runnable {

    @Override
    public void run() {
        if (useGlobalVersion) {
            pollGlobalVersion();    // Option B-Global
        } else {
            pollFineGrainedVersions();  // Option B-Fine
        }
    }

    // ── Option B-Global ──────────────────────────────────────────────────
    private void pollGlobalVersion() {
        long cachedVersion = lastSeenGlobalVersion.get(metalakeId);
        // SELECT version FROM auth_global_version WHERE metalake_id = ?
        long dbVersion = authGlobalVersionMapper.getVersion(metalakeId);
        if (dbVersion != cachedVersion) {
            invalidateAllForMetalake(metalakeName);  // coarse: evict everything
            lastSeenGlobalVersion.put(metalakeId, dbVersion);
        }
    }

    private void invalidateAllForMetalake(String metalakeName) {
        // 1. Evict all userRoleCache entries whose key starts with metalakeName + ":"
        userRoleCache.invalidateIf(key -> key.startsWith(metalakeName + ":"));
        groupRoleCache.invalidateIf(key -> key.startsWith(metalakeName + ":"));

        // 2. Evict all loadedRoles entries for roles in this metalake, clear JCasbin
        Set<Long> metalakeRoleIds = metalakeToRoleIds.getOrDefault(metalakeId, Set.of());
        for (long roleId : metalakeRoleIds) {
            loadedRoles.invalidate(roleId);
            allowEnforcer.deleteRole(String.valueOf(roleId));
            denyEnforcer.deleteRole(String.valueOf(roleId));
        }
        metalakeToRoleIds.remove(metalakeId);  // clear the auxiliary index
    }

    // ── Option B-Fine ────────────────────────────────────────────────────
    private void pollFineGrainedVersions() {
        // Batch check all cached users
        Map<Long, String>  userIdToKey  = collectCachedUserIds();
        Map<Long, Integer> userVersions = userMetaMapper.batchGetRoleGrantsVersions(userIdToKey.keySet());
        for (var entry : cachedUserEntries()) {
            int dbVer = userVersions.getOrDefault(entry.getValue().userId(), -1);
            if (dbVer == -1 || dbVer != entry.getValue().roleGrantsVersion()) {
                userRoleCache.invalidate(entry.getKey());
            }
        }

        // Batch check all cached roles
        Set<Long>          cachedRoles  = loadedRoles.asMap().keySet();
        Map<Long, Integer> roleVersions = roleMetaMapper.batchGetSecurableObjectsVersions(cachedRoles);
        for (long roleId : cachedRoles) {
            int dbVer = roleVersions.getOrDefault(roleId, -1);
            if (dbVer == -1 || !Objects.equals(dbVer, loadedRoles.getIfPresent(roleId).orElse(null))) {
                loadedRoles.invalidate(roleId);
                allowEnforcer.deleteRole(String.valueOf(roleId));
                denyEnforcer.deleteRole(String.valueOf(roleId));
            }
        }
    }
}
```

`metalakeToRoleIds: Map<Long, Set<Long>>` is an auxiliary in-memory index maintained in
`JcasbinAuthorizer`: when a role is loaded into `loadedRoles`, add it to the metalake's set;
when evicted, remove it. Required only for Option B-Global to identify which roles to clear.

Thread lifecycle: started in `JcasbinAuthorizer.init()`, shut down in `close()`.
Uses a single-thread `ScheduledExecutorService` with fixed delay (not fixed rate, to avoid
overlapping polls if one takes longer than the interval).

#### 4.2.4 Auth Check Flow

Same for both B-Global and B-Fine — no version queries in the request path:

```
authorize(metalakeName, username, resource, operation)
│
├─ STEP 1 — Check userRoleCache (no version query — poll thread keeps it current):
│
│   userRoleCache HIT → use cached userId and roleIds    [A] and [C1] from cache
│
│   MISS (first request, or evicted by poll thread):
│     → SELECT um.user_id, um.role_grants_version
│         FROM user_meta um JOIN metalake_meta mm ON ... WHERE ...  ← same as Approach A Step 1
│     → SELECT role_id FROM user_role_rel WHERE user_id = ? AND deleted_at = 0
│     → userRoleCache.put(key, new CachedUserRoles(userId, version, roleIds))
│
├─ STEP 2 — Resolve target resource ID (always DB, no cache):
│   metadataId = MetadataIdConverter.getID(resource, metalake)  ← 1 indexed DB query
│
├─ [Only when privilege == OWNER] STEP 2.5 — Query ownership directly (no cache):
│   SELECT owner_id, owner_type FROM owner_meta WHERE metadata_object_id = ? AND deleted_at = 0
│
├─ STEP 3 — Check loadedRoles (no version query — poll thread keeps it current):
│   loadedRoles HIT → [C2][C3] avoided
│   MISS → reload securable objects for this role from DB
│
└─ STEP 4 — enforce() (in-memory, O(1))
```

**Note on entity storage cache dependency:** After poll-based invalidation, the reload path
(Step 1 on cache miss, securable objects reload on loadedRoles miss) goes directly to DB
through MyBatis mappers — **no entity storage cache involved**. Phase 2 auth caches rebuild
themselves from DB in the same way as cold start. The entity cache (Layer 1) is fully
disabled in Phase 2 and is not part of any reload path.

#### 4.2.5 Properties

| Dimension | B-Global (new version table) | B-Fine (per-table versions) |
|-----------|------------------------------|------------------------------|
| Staleness window | **≤ poll interval** | **≤ poll interval** |
| Schema changes | **1 new table**, 0 existing table changes | 3 columns on existing tables |
| Hot path DB queries | **1** (Step 2 only) | **1** (Step 2 only) |
| Invalidation granularity | Coarse — entire metalake | Fine — only changed user/role |
| Cache stampede risk | **Yes** — all users cold miss after any mutation | **No** — only stale entries evicted |
| Write path contention | One row per metalake (may be hot under concurrent mutations) | One row per role/user |
| Background threads | 1 per node | 1 per node |
| Failure mode | Poll failure → stale reads until next poll | Poll failure → stale reads until next poll |

---

### 4.3 Comparison and Recommendation

| Dimension | Approach A | Approach B-Global | Approach B-Fine |
|-----------|-----------|-------------------|-----------------|
| Staleness window | **0** | ≤ poll interval | ≤ poll interval |
| Hot path DB queries | **3** | **1** | **1** |
| OWNER hot path queries | **4** | **2** | **2** |
| Schema changes | 3 columns on existing tables | **1 new table** | 3 columns on existing tables |
| Invalidation on mutation | Targeted (per-user/role) | Coarse (whole metalake) | Targeted (per-user/role) |
| Cache stampede risk | None | **Yes** (all users cold after any mutation) | None |
| Write path | +1 UPDATE in existing tx | +1 UPDATE in existing tx | +1 UPDATE in existing tx |
| Background threads | **None** | 1 per node | 1 per node |
| Failure mode | DB down → auth blocked | Poll failure → stale reads | Poll failure → stale reads |
| Best for | Zero-staleness requirement | Simple schema, low-mutation rate | High-QPS + targeted invalidation |

**Recommendation:**

1. **Default choice: Approach A** — zero staleness, no background thread, simpler failure
   semantics. The +2 lightweight queries per request are the only cost.

2. **If QPS is high and a few seconds' staleness is acceptable:** Approach B-Global is the
   simplest implementation — only 1 new table, no changes to existing tables. Acceptable
   when auth mutations are rare (which is typical: role/privilege changes happen on admin
   actions, not on every query).

3. **If B-Global's cache stampede is a concern:** Approach B-Fine gives targeted invalidation
   at the cost of the same schema changes as Approach A.

4. **Combined:** Approach B for the common case, Approach A version-check as a fallback on
   cache miss or before the first poll has run after startup.

**Note on entity storage cache:** After invalidation in either Approach B variant, the reload
path uses direct DB queries through MyBatis mappers — identical to cold start. Entity cache
(Layer 1) is fully disabled in Phase 2 and is not part of any reload path. There is no
entity storage cache dependency in Phase 2.

---

## 5. Phased Implementation Plan

### Phase 1 — Foundation (common to both approaches, no schema changes)

| Step | Change | Module |
|------|--------|--------|
| 1.1 | Fix auth N+1: `batchListSecurableObjectsByRoleIds()` + rewrite `loadRolePrivilege()` | `RoleMetaService`, `JcasbinAuthorizer` |
| 1.2 | Introduce `GravitinoCache<K,V>` interface; wrap existing Caffeine caches | `GravitinoCache.java`, `CaffeineGravitinoCache.java` |
| 1.3 | Disable entity store cache: `CACHE_ENABLED` default → `false` | `Configs.java` |
| 1.4 | Fix DBCP2: `minEvictableIdleTimeMillis` 1 s → 30 s; `minIdle` 0 → 5 | `SqlSessionFactoryHelper.java` |

**Outcome:** Auth cold path `3+T` queries. Calls [A][C1] hit DB every request (acceptable
as a stepping stone — Phase 2 closes this). Consistency still TTL-bounded.

---

### Phase 2A — Approach A Implementation

| Step | Change | Module |
|------|--------|--------|
| 2A.1 | `ADD COLUMN securable_objects_version` on `role_meta` | `schema-x.y.z-*.sql` |
| 2A.2 | `ADD COLUMN role_grants_version` on `user_meta` | `schema-x.y.z-*.sql` |
| 2A.3 | `ADD COLUMN role_grants_version` on `group_meta` | `schema-x.y.z-*.sql` |
| 2A.4 | Bump `securable_objects_version` in privilege grant/revoke transaction | `RoleMetaService` |
| 2A.5 | Bump `role_grants_version` in role assign/revoke transaction (user + group) | `UserMetaService`, `GroupMetaService` |
| 2A.6 | Add `userRoleCache: GravitinoCache<String, CachedUserRoles>` | `JcasbinAuthorizer` |
| 2A.7 | Add `groupRoleCache: GravitinoCache<String, CachedGroupRoles>` | `JcasbinAuthorizer` |
| 2A.8 | Change `loadedRoles` type: `Boolean` → `Integer` (stores version) | `JcasbinAuthorizer` |
| 2A.9 | Rewrite `loadRolePrivilege()` + `authorize()` with 4-step flow (§4.1.3) | `JcasbinAuthorizer` |
| 2A.10 | Add mapper methods (see §6.1) | mapper + SQL |
| 2A.11 | Remove `ownerRel`; add `selectOwnerByMetadataObjectId` for OWNER checks | `JcasbinAuthorizer`, `OwnerMetaMapper` |

**Outcome:** Zero staleness. Hot path: 3 lightweight DB queries (2 version checks + 1 ID lookup).

---

### Phase 2B — Approach B Implementation (alternative to 2A)

Cache data structure changes are **identical to 2A** (steps 2A.6–2A.9, 2A.11). The difference
is in schema and the poll mechanism. Two sub-options:

**Phase 2B-Global (simpler schema — recommended starting point for Approach B):**

| Step | Change | Module |
|------|--------|--------|
| 2B-G.1 | Create `auth_global_version` table (1 row per metalake) | `schema-x.y.z-*.sql` |
| 2B-G.2 | Bump `auth_global_version.version` in every auth mutation transaction | `RoleMetaService`, `UserMetaService`, `GroupMetaService` |
| 2B-G.3 | Add `AuthGlobalVersionMapper.getVersion(metalakeId)` | mapper + SQL |
| 2B-G.4 | Add `AuthCachePollThread` (B-Global variant); maintain `metalakeToRoleIds` auxiliary index | `JcasbinAuthorizer` |
| 2B-G.5 | Add config `gravitino.authorization.poll.interval.secs` (default: 5) | `Configs.java` |
| 2B-G.6 | Simplify `authorize()`: remove version queries from Steps 1 and 3 | `JcasbinAuthorizer` |

**Outcome:** 1 new table, 0 changes to existing tables. Staleness ≤ poll interval. Hot path: 1 DB query.
Cache stampede possible after mutations (all users for metalake invalidated simultaneously).

---

**Phase 2B-Fine (same schema as Approach A — targeted invalidation):**

| Step | Change | Module |
|------|--------|--------|
| 2B-F.1–3 | `ADD COLUMN` on `role_meta`, `user_meta`, `group_meta` — same as 2A.1–2A.3 | `schema-x.y.z-*.sql` |
| 2B-F.4–5 | Bump versions in mutation transactions — same as 2A.4–2A.5 | `RoleMetaService`, `UserMetaService`, `GroupMetaService` |
| 2B-F.6 | Add `AuthCachePollThread` (B-Fine variant) with batch version poll queries | `JcasbinAuthorizer` |
| 2B-F.7 | Add config `gravitino.authorization.poll.interval.secs` (default: 5) | `Configs.java` |
| 2B-F.8 | Simplify `authorize()`: remove version queries from Steps 1 and 3 | `JcasbinAuthorizer` |

**Outcome:** 3 schema changes (same as Approach A). Staleness ≤ poll interval. Hot path: 1 DB query.
Targeted invalidation — no cache stampede.

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

// OwnerMetaMapper.java (for Step 2.5)
Map<String, Object> selectOwnerByMetadataObjectId(
    @Param("metadataObjectId") long metadataObjectId);
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

### 7.2 `ownerRel` — Why Removed Instead of Version-Validated

`owner_meta` uses soft-delete for ownership transfers (delete old row + insert new row).
The new row's `current_version` always starts at 1, making `current_version` useless for
detecting ownership staleness. Adding `owner_version` to every owned entity type (catalog,
schema, table, fileset, topic, view, ...) is invasive.

**Resolution:** Remove `ownerRel` cache. Since `ownerRel` is only consulted for
`privilege == OWNER` (regular privilege checks never touch it), and `metadataId` is already
resolved in Step 2, one additional indexed query on `owner_meta` (Step 2.5) provides strong
consistency for OWNER checks at the cost of +1 query only on OWNER evaluations.

### 7.3 Group Role Assignments

`group_meta` is **not optional** — groups can hold role assignments and must have the same
version-validation coverage as users. `ALTER TABLE group_meta ADD COLUMN role_grants_version`
is in §4.1.1, and `groupRoleCache` ships in Phase 2 alongside `userRoleCache`.

### 7.4 Possible Future Direction: Auth Decision Cache

Not on the current roadmap. Once Phase 2 is stable, caching the final auth decision
`(userId, objectType, metadataId, privilege) → ALLOW|DENY` would reduce the hot path to
zero DB queries. The prerequisite is Phase 2's version-check infrastructure.

---

## 8. Summary

### 8.1 Query Count Comparison

With entity cache **enabled** and all in-process caches warm, the current system serves auth
from pure in-process Caffeine with **0 DB queries**. Phase 2 exchanges this for a small,
bounded query count plus strong HA consistency.

| Scenario | Entity cache ON (current) | Phase 1 | Phase 2A | Phase 2B (either variant) |
|----------|--------------------------|---------|----------|---------------------------|
| Hot path — all current | **0** (fully in-memory) | 3+ heavy full-row queries | **3 lightweight** | **1** |
| OWNER privilege hot path | 0 (ownerRel warm) | 1 heavy JOIN | **4** | **2** |
| After mutation (stale) | 0 (**stale — TTL hides it**) | 3+ heavy | **4 on next request** | Evicted by poll; next req: 3–5 |
| Cold start | ~3+T | ~3+T heavy | **4–5 lightweight** | **3–5 lightweight** |
| HA staleness | Up to 1 hour | Up to 1 hour | **0** | **≤ poll interval** |

### 8.2 Change Surface

| Dimension | Phase 1 | Phase 2A | Phase 2B-Global | Phase 2B-Fine |
|-----------|---------|----------|-----------------|---------------|
| Entity cache | **Disabled** | Disabled | Disabled | Disabled |
| Schema — existing tables | None | **+3 columns** (role/user/group_meta) | **None** | +3 columns (same as 2A) |
| Schema — new tables | None | None | **1 new table** (auth_global_version) | None |
| New caches | None | **2** (userRoleCache + groupRoleCache) | **2** (same) | **2** (same) |
| Cache type changes | None | **1** (loadedRoles Boolean→Integer) | **1** (same) | **1** (same) |
| Removed caches | None | **1** (ownerRel) | **1** (ownerRel) | **1** (ownerRel) |
| Background threads | None | **None** | 1 per node | 1 per node |
| Invalidation granularity | — | Targeted (per request) | Coarse (whole metalake) | Targeted (per-user/role) |
| Cache stampede on mutation | — | None | **Possible** | None |
| External dependencies | None | None | None | None |
