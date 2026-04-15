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
2 version-check queries on a warm hit (see §4.7 Step 1 and Step 3). Both achieve strong
consistency.

### 3.2 Other References

**Nessie** — HTTP fan-out invalidation: async POST to peer nodes on write, convergence < 200 ms.

**Keycloak** — JGroups embedded cluster messaging: in-JVM broadcast, no separate service.
Recommended future direction if Gravitino needs stronger delivery guarantees.

**DB version polling** — monotonic counters incremented in write transaction; a background
thread polls for version changes and proactively invalidates caches. Considered but not
adopted; per-request validation (§4.7) achieves strong consistency without background threads.

---

## 4. Design

### 4.1 Design Overview

Three caches drive auth performance: the user/group → role mapping, entity name → integer ID,
and ownership lookups. Each has different access frequency, mutation rate, and security impact
— and consequently a different consistency model.

**Consistency tier 1 — strong (version-validated):** User-role assignments and role-privilege
definitions are security-critical. A revoked permission must not be served from cache even one
second after revocation. Each auth request issues two lightweight version-check queries against
`user_meta`, `group_meta`, and `role_meta`. If any `updated_at` timestamp has advanced since
the cached value, only the stale portion is reloaded. Staleness window: **zero**.

**Consistency tier 2 — eventual (write-path hook + change poller):** Entity name→ID mappings
and ownership records change far less frequently (DDL, ownership transfers) and a brief window
of inconsistency has lower security impact. The local node sees changes immediately via hooks
that fire after transaction commit. HA peer nodes converge within the change poll interval
(default 1 s) via two lightweight poll queries. No external infrastructure (Kafka, Redis) is
required — the existing DB is the single source of truth for both tiers.

---

### 4.1.1 Current-vs-Target Gap (Code-Aligned)

The design below intentionally closes concrete gaps in the current implementation:

| Area                                 | Current behavior (main branch)                                            | Target behavior (this design)                                                               |
|--------------------------------------|---------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| Role loading                         | `JcasbinAuthorizer.loadRolePrivilege()` loads only `ROLE_USER_REL`        | Load both user direct roles and group-derived roles (`group_role_rel`)                      |
| Owner check path                     | `isOwner()` calls `MetadataIdConverter.getID()` twice for the same object | Resolve metadata ID once per call path and reuse                                            |
| Role cache coherence                 | `loadedRoles: Cache<Long, Boolean>` is TTL-driven                         | `loadedRoles: roleId -> updated_at` with per-request version validation                     |
| Cross-node entity/owner invalidation | In-process hook/TTL only, no durable HA invalidation stream               | DB-backed pollers (`owner_meta.updated_at`, `entity_change_log`) with targeted invalidation |
| Request-scope dedup                  | `AuthorizationRequestContext` has allow/deny result cache + `hasLoadRole` | Add request-scope owner/id dedup maps with strict request-thread scope                      |

This section is used as implementation acceptance criteria and should stay synchronized with code
changes in `server-common/.../JcasbinAuthorizer.java`,
`server-common/.../MetadataIdConverter.java`, and `core/.../AuthorizationRequestContext.java`.

---

### 4.2 Strong Consistency: User, Group, and Role Caches

#### Why Strong Consistency Is Required

Privilege revocations are the primary security enforcement operation. If a user's role is
revoked or a role's privilege is removed, the change must take effect on the **next** auth
request on any node, not after TTL expiry. TTL-only caching is fundamentally unable to
provide this guarantee.

The chosen approach is Polaris-style per-request version validation: each row in `user_meta`,
`group_meta`, and `role_meta` carries an `updated_at` timestamp set in the same DB transaction
as the security write. On every auth request, the authorizer fetches these timestamps and
compares them against cached values. A mismatch triggers a targeted reload of only the changed
entry — not a full policy flush.

Groups are **not optional**: a user can belong to a group that itself holds role assignments.
`group_meta.updated_at` receives the same treatment as `user_meta.updated_at`, so group-role
changes are immediately reflected everywhere.

Using a timestamp instead of a monotonic counter has a theoretical same-millisecond collision
risk (two writes within 1 ms yield the same value → cache misses the second change), but this
is negligible for administrative operations (GRANT/REVOKE) in practice.

#### Schema Changes

```sql
-- Role privilege tracking (strong consistency — Step 3 version check)
ALTER TABLE `role_meta`
    ADD COLUMN `updated_at` BIGINT NOT NULL DEFAULT 0
    COMMENT 'Set to currentTimeMillis() on any privilege grant/revoke for this role.
             JcasbinAuthorizer compares db.updated_at vs cached updated_at per request
             to decide whether to reload JCasbin policies for this role.';

-- User role assignment tracking (strong consistency — Step 1a version check)
ALTER TABLE `user_meta`
    ADD COLUMN `updated_at` BIGINT NOT NULL DEFAULT 0
    COMMENT 'Set to currentTimeMillis() on any role assign/revoke for this user.
             JcasbinAuthorizer compares db.updated_at vs cached updated_at per request
             to decide whether to reload the user-role mapping.';

-- Group role assignment tracking (strong consistency — Step 1b version check)
ALTER TABLE `group_meta`
    ADD COLUMN `updated_at` BIGINT NOT NULL DEFAULT 0
    COMMENT 'Set to currentTimeMillis() on any role assign/revoke for this group.
             JcasbinAuthorizer compares db.updated_at vs cached updated_at per request
             to decide whether to reload the group-role mapping.';
```

#### Index and Backfill Notes

To keep Step 1 and Step 3 checks predictable under load, add/verify covering indexes for
high-frequency predicates:

```sql
-- Suggested read-path indexes for version checks
CREATE INDEX idx_user_meta_name_del_upd
    ON user_meta (metalake_id, user_name, deleted_at, updated_at);
CREATE INDEX idx_group_meta_del_upd
    ON group_meta (group_id, deleted_at, updated_at);
CREATE INDEX idx_role_meta_del_upd
    ON role_meta (role_id, deleted_at, updated_at);
CREATE INDEX idx_owner_meta_obj_del_upd
    ON owner_meta (metadata_object_id, deleted_at, updated_at);
```

Backfill strategy for the newly added `updated_at` columns:

1. DDL adds columns with default `0`.
2. One-time backfill sets `updated_at = create_time` (or `last_modified_time` if available)
   for existing active rows.
3. New write-path hooks become the long-term source of truth.

Using explicit backfill avoids a long-lived "all zero" window that would force unnecessary cold
reloads at rollout time.

---

### 4.3 Eventual Consistency: Ownership Cache (`ownerRelCache`)

#### Why `ownerRelCache` Is Critical for Performance

Nearly all authorization expressions include `ANY(OWNER, METALAKE, CATALOG)` or
`ANY(OWNER, METALAKE, CATALOG, SCHEMA, ...)`. These expand via OGNL to a chain of
`METALAKE::OWNER || CATALOG::OWNER || ...` calls. Each term calls `isOwner()` **directly**,
independent of the `authorize()` path. Every auth request triggers **2–4 `isOwner()` calls**
(one per ancestor level). Without a cache, this adds 2–4 extra `owner_meta` DB queries per
request. For most non-owner users, the result is `Optional.empty()`, so the cache primarily
stores empty-ownership negatives that let the check fail quickly.

#### Why Version-Validated Caching Is Unnecessary for Ownership

| Cache                                            | What a version check returns     | What it saves                                                                     |
|--------------------------------------------------|----------------------------------|-----------------------------------------------------------------------------------|
| `loadedRoles`                                    | `(role_id, updated_at)`          | Skips reloading all securable objects + JCasbin `addPolicy` calls — **expensive** |
| `ownerRelCache` (hypothetical version-validated) | `(metadata_object_id, owner_id)` | Nothing — the version check query **already returns `owner_id`**                  |

A version-validated `ownerRelCache` would add schema columns, write-path version bumps, and
per-request version queries — while saving exactly zero DB queries beyond what the version
check itself costs. Complexity without benefit.

#### Invalidation Strategy: TTL Safety-Net + Write-Path Hook + Owner Change Poller

`ownerRelCache` uses a three-layer strategy:

1. **Local node — immediate**: `handleMetadataOwnerChange()` hook fires after the ownership
   transfer transaction commits and calls `ownerRelCache.invalidate(metadataId)`.
2. **HA peer nodes — targeted, near real-time (≤ 1 s)**: the owner change poller queries
   `owner_meta WHERE updated_at > maxOwnerUpdatedAt`. For each returned row it calls
   `ownerRelCache.invalidate(metadataObjectId)` — only the changed entries are evicted;
   unrelated cached ownerships remain hot.
3. **TTL — safety net only**: a long TTL (e.g. 1 hour) catches any missed invalidation
   (e.g. poller downtime). Correctness relies on hook + poller, not TTL.

`owner_meta` is a 1:1 table (one row per entity with an owner). The poller can read
`updated_at` directly from the source table and immediately get the `metadata_object_id` to
invalidate — no intermediate log table is needed. This avoids write amplification and keeps
the design simple.

#### Why Eventual Consistency Is Safe for Ownership

Privilege revocation (GRANT/REVOKE) is handled by the **strong-consistency** Steps 1 + 3.
Ownership transfer is an administrative reorganisation, not an emergency access revocation —
a ≤ 1 s grace period on HA peer nodes is operationally acceptable and consistent with how
similar systems (AWS IAM, Apache Polaris) treat structural metadata changes.

#### Schema Change

```sql
-- Ownership mutation tracking (eventual consistency — owner change poller)
ALTER TABLE `owner_meta`
    ADD COLUMN `updated_at` BIGINT NOT NULL DEFAULT 0
    COMMENT 'Set to currentTimeMillis() on any ownership transfer.
             The owner change poller reads updated_at > maxSeen to find changed rows
             and invalidates only the specific metadataObjectIds in ownerRelCache.';
```

---

### 4.4 Eventual Consistency: Name→ID Cache (`metadataIdCache`)

#### The Problem: Repeated `getID()` Calls in OGNL Expression Evaluation

`MetadataIdConverter.getID()` calls `entityStore.get()` for every unique `(MetadataObject,
privilege)` combination in the OGNL expression. The `allowAuthorizerCache` deduplicates
complete `(principal, metalake, obj, privilege)` results, but different privileges on the same
object (e.g. `METALAKE::USE_CATALOG`, `METALAKE::USE_SCHEMA`, `METALAKE::DENY_USE_CATALOG`)
each trigger a separate `getID(METALAKE)` call. A full `LOAD_TABLE_AUTHORIZATION_EXPRESSION`
evaluation can trigger **8–12 `getID()` calls**, of which most are for the same 3–4 objects.

#### Hierarchical Cache Key with Prefix-Based Cascade Invalidation

The cache key uses a hierarchical `::` separator that enables prefix-based cascade eviction:

| Entity type | Key example                     | Is non-leaf?      |
|-------------|---------------------------------|-------------------|
| METALAKE    | `lake1::`                       | ✓ (trailing `::`) |
| CATALOG     | `lake1::cat1::`                 | ✓                 |
| SCHEMA      | `lake1::cat1::s1::`             | ✓                 |
| TABLE       | `lake1::cat1::s1::t1::TABLE`    | leaf              |
| FILESET     | `lake1::cat1::s1::fs1::FILESET` | leaf              |
| TOPIC       | `lake1::cat1::s1::tp1::TOPIC`   | leaf              |
| MODEL       | `lake1::cat1::s1::m1::MODEL`    | leaf              |
| VIEW        | `lake1::cat1::s1::v1::VIEW`     | leaf              |

`invalidateByPrefix("lake1::cat1::")` evicts the catalog entry AND all schemas, tables,
filesets, and other entities beneath it in a single O(n) pass over the cache (bounded, DDL is
rare).

#### Why `entity_change_log` Instead of Adding `updated_at` to Entity Tables

The natural alternative is adding `updated_at` to each entity table (metalake_meta,
catalog_meta, schema_meta, table_meta, …) and polling them directly. This has three
fundamental problems:

| Problem                                        | Explanation                                                                                                                                                                                                   |
|------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Old name unavailable after rename**          | Entity tables store the **current** name only. After `table1 → table2`, the row holds `table2`. The poller can detect *a* change happened but cannot reconstruct the old cache key to invalidate it.          |
| **JOIN cost to reconstruct full path**         | Nested tables (schema, table, fileset, …) store only their simple name. Rebuilding the full `catalog.schema.table` path requires multi-level JOINs per entity type — eight separate queries or a complex UNION every poll cycle. |
| **Cascade requires scanning all child tables** | Dropping `cat1` means also polling schema_meta, table_meta, … for all rows under `cat1`. With `entity_change_log`, **one row** for the catalog + `invalidateByPrefix("lake::cat1::")` evicts the entire subtree. |

`entity_change_log` solves all three: `(metalake_name, entity_type, full_name)` gives the
poller exactly what it needs to call `buildCacheKey + invalidateByPrefix`, and `operate_type`
documents the nature of the change for observability.

**Rename correctness:** Rename does not change `metadataId`. The write path logs
`operate_type=ALTER, full_name=oldName`. The poller calls `invalidateByPrefix(buildCacheKey
(lake, CATALOG, "cat1"))` → evicts `lake::cat1::` and all children. New name keys are cold
misses → DB → same numeric ids. Policy in JCasbin is unaffected (keyed on numeric ids, not
names).

**Per-request dedup:** A `Map<String, Long>` in `AuthorizationRequestContext` provides an
additional within-request dedup layer. When the Caffeine cache is cold, the first `getID()`
call for a given object populates the request-level map; subsequent calls within the same
request avoid repeated Caffeine lookups. On the warm path (Caffeine hits), the request-level
map is a minor CPU optimisation.

**Write amplification:** one row per affected entity per operation — never per child. Entity
DDL is rare in production. Rows are pruned after a configurable retention window (default 1
hour).

#### Invalidation Strategy: Persistent Caffeine Cache + Write-Path Hook + Entity Change Log Poller

`metadataIdCache` uses a three-layer strategy:

1. **Local node — immediate**: `handleEntityStructuralChange()` hook fires after transaction
   commit and calls `metadataIdCache.invalidateByPrefix(buildCacheKey(...))`. Non-leaf entities
   cascade to all children; leaf entities match exactly one entry.
2. **HA peer nodes — cascade, near real-time (≤ 1 s)**: the entity change poller reads
   `entity_change_log WHERE created_at > maxEntityCreatedAt`, rebuilds the cache key from
   `(metalake_name, entity_type, full_name)`, and calls `invalidateByPrefix` — **one log row
   per DROP/ALTER operation regardless of how many children exist**.
3. **TTL — safety net only**: a long TTL (e.g. 1 hour) as a last resort.

#### Schema Change

```sql
-- Entity name→id mutation tracking (eventual consistency — entity change poller)
CREATE TABLE `entity_change_log` (
  `id`            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `metalake_name` VARCHAR(128)    NOT NULL,
  `entity_type`   VARCHAR(32)     NOT NULL
      COMMENT 'METALAKE | CATALOG | SCHEMA | TABLE | FILESET | TOPIC | MODEL | VIEW',
  `full_name`     VARCHAR(512)    NOT NULL
      COMMENT 'Dot-separated full name of the affected entity. For RENAME, stores the
               OLD name (the stale key to invalidate). For DROP/ALTER, the entity name.',
  `operate_type`  VARCHAR(16)     NOT NULL
      COMMENT 'DROP | CREATE | ALTER (ALTER covers rename and other structural changes)',
  `created_at`    BIGINT          NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `idx_created_at` (`created_at`)
) COMMENT 'Append-only log of entity structural changes.
           One row per affected entity per operation. The entity change poller reads
           this table to drive targeted invalidation of metadataIdCache on HA peer nodes.
           Rows older than the retention window (default 1 h) are pruned periodically.';
```

---

### 4.5 Write Path Invariants

All schema tracking writes must execute **in the same DB transaction** as the data change.
If the transaction rolls back, none of the tracking writes are committed — no spurious cache
invalidations on HA peers.

| Operation                              | Schema write                                                                                                                                                   | Location                               |
|----------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------|
| Grant / revoke privilege on role R     | `role_meta.updated_at = now() WHERE role_id = R`                                                                                                               | `RoleMetaService`                      |
| Assign / revoke role for user U        | `user_meta.updated_at = now() WHERE user_id = U`                                                                                                               | `UserMetaService`                      |
| Assign / revoke role for group G       | `group_meta.updated_at = now() WHERE group_id = G`                                                                                                             | `GroupMetaService`                     |
| Ownership transfer for entity E        | `owner_meta.updated_at = now() WHERE metadata_object_id = E`                                                                                                   | `OwnerMetaService`                     |
| Rename entity (old name → new name)    | INSERT into `entity_change_log`: `operate_type=ALTER`, **old** `full_name`, `created_at=now()`                                                                 | All entity MetaService classes         |
| Drop entity with name N                | INSERT into `entity_change_log`: `operate_type=DROP`, `full_name=N`, `created_at=now()`                                                                        | All entity MetaService classes         |
| Create entity with name N              | INSERT into `entity_change_log`: `operate_type=CREATE`, `full_name=N`, `created_at=now()` (optional — new names are cache misses; insert is a safety net only) | All entity MetaService classes         |

---

### 4.6 Cache Data Structures (Changes in JcasbinAuthorizer)

```java
// ─── BEFORE ──────────────────────────────────────────────────────────
private Cache<Long, Boolean>           loadedRoles;  // roleId → loaded?
private Cache<Long, Optional<Long>>    ownerRel;

// ─── AFTER ───────────────────────────────────────────────────────────

// NEW: replaces entity cache dependency for [A] (userId) and [C1] (role list).
// Step 1 query returns both user_id and updated_at in one shot.
// metalakeName→metalakeId resolved inline via JOIN — no dedicated cache needed.
private GravitinoCache<String, CachedUserRoles> userRoleCache;
// key = metalakeName + ":" + userName

record CachedUserRoles(
    long       userId,     // integer userId for JCasbin enforce()
    long       updatedAt,  // user_meta.updated_at at load time — staleness sentinel
    List<Long> roleIds     // role ID list at load time
) {}

// NEW: mirrors userRoleCache for groups (group can also hold role assignments).
private GravitinoCache<String, CachedGroupRoles> groupRoleCache;
// key = metalakeName + ":" + groupName

record CachedGroupRoles(
    long       groupId,
    long       updatedAt,  // group_meta.updated_at at load time — staleness sentinel
    List<Long> roleIds
) {}

// TYPE CHANGE: was Cache<Long, Boolean>, now stores role_meta.updated_at.
// Enables staleness detection rather than TTL expiry.
private GravitinoCache<Long, Long> loadedRoles;
// roleId → role_meta.updated_at at the time JCasbin policies were loaded

// NEW: caches name → integer id for every MetadataObject referenced in OGNL expressions.
// Without this, every authorize()/isOwner() call triggers entityStore.get() for each unique
// object in the expression chain (METALAKE, CATALOG, SCHEMA, TABLE etc.), multiplied by the
// number of distinct privilege checks on that object.
// Consistency: immediate on local node via handleEntityStructuralChange() hook on drop/rename;
//              HA peer nodes: entity change poller reads entity_change_log WHERE created_at > maxSeen
//              and calls invalidateByPrefix(cacheKey) for each row within the poll interval (default 1 s).
//              Non-leaf entities (CATALOG, SCHEMA) use prefix invalidation to cascade-evict all
//              children in one call. See §4.4.
// TTL: long safety-net only (e.g. 1 hour). Correctness comes from hook + poller, not TTL.
private GravitinoCache<String, Long> metadataIdCache;
// Key format — hierarchical path, type suffix on leaf entities only:
//   metalakeName::cat::schema::        ← SCHEMA (non-leaf, trailing :: for prefix cascade)
//   metalakeName::cat::schema::t::TABLE ← TABLE (leaf, ::TYPE suffix for disambiguation)
// On DROP/RENAME of a non-leaf: invalidateByPrefix("lake::cat::") evicts the entity + all children.
// On DROP/RENAME of a leaf:     invalidateByPrefix("lake::cat::schema::t::TABLE") evicts exactly one entry.

// RESTORED with mutation-poller-driven invalidation (see §4.3 for full rationale).
// Consistency: immediate on local node via handleMetadataOwnerChange() hook;
//              HA peer nodes: invalidated within mutation poll interval (default 1 s).
// TTL serves only as a safety-net last resort (default: long, e.g. 1 hour).
private GravitinoCache<Long, Optional<Long>> ownerRelCache;
// key = metadataId (Long); value = Optional<Long> ownerId (empty = no owner set)

// ── Targeted pollers — drive HA cross-node invalidation of ownerRelCache / metadataIdCache ──

// Max updated_at seen across all owner_meta rows so far.
// Poller finds rows WHERE updated_at > maxOwnerUpdatedAt and invalidates specific entries.
private final AtomicLong maxOwnerUpdatedAt = new AtomicLong(0L);

// Max created_at seen across all entity_change_log rows so far.
// Poller finds rows WHERE created_at > maxEntityCreatedAt and invalidates specific entries.
private final AtomicLong maxEntityCreatedAt = new AtomicLong(0L);

// Single-thread scheduled executor shared by both poll tasks.
// One tiny owner_meta query + one tiny entity_change_log query per interval.
// Distinct from the executor thread pool (removed in Phase 2); never touches auth logic.
private ScheduledExecutorService changePoller;
// Poll interval: configurable, default 1 s (Configs.GRAVITINO_CHANGE_POLL_INTERVAL_MS).
```

---

### 4.7 Auth Check Flow

```
authorize(metalakeName, username, resource, operation)
│
├─ STEP 1 — User + Group version check (2 queries, metalake resolved via JOIN):
│
│   [1a] User query:
│   SELECT um.user_id, um.updated_at
│   FROM user_meta um
│   JOIN metalake_meta mm ON um.metalake_id = mm.metalake_id AND mm.deleted_at = 0
│   WHERE mm.metalake_name = ? AND um.user_name = ? AND um.deleted_at = 0
│
│   userRoleCache HIT and updated_at matches:
│     → use cached userId and roleIds               [A] and [C1] avoided
│   MISS or updated_at mismatch:
│     → SELECT role_id FROM user_role_rel WHERE user_id = ? AND deleted_at = 0
│     → re-associate userId ↔ roleIds in JCasbin allow/deny enforcers
│     → userRoleCache.put(key, CachedUserRoles(userId, updatedAt, roleIds))
│
│   [1b] Group query (user may belong to groups that also hold roles):
│   SELECT gm.group_id, gm.updated_at
│   FROM group_meta gm
│   JOIN group_user_rel gu ON gm.group_id = gu.group_id AND gu.deleted_at = 0
│   WHERE gu.user_id = ? AND gm.deleted_at = 0
│
│   For each group:
│     groupRoleCache HIT and updated_at matches:
│       → use cached groupId and roleIds            [group C1] avoided
│     MISS or updated_at mismatch:
│       → SELECT role_id FROM group_role_rel WHERE group_id = ? AND deleted_at = 0
│       → addRoleForUser(userId, roleId) in JCasbin enforcers
│       → groupRoleCache.put(groupKey, CachedGroupRoles(groupId, updatedAt, roleIds))
│
│   Note: current code only loads user-direct roles (ROLE_USER_REL). Loading group roles
│   via [1b] is a NEW capability introduced in Phase 2 alongside groupRoleCache.
│
├─ STEP 2 — Resolve name → integer id (via metadataIdCache, eventual consistency):
│
│   key = buildCacheKey(metalakeName, entityType, fullName)
│         → non-leaf (CATALOG, SCHEMA): "metalake::cat::schema::"  (trailing :: for prefix cascade)
│         → leaf    (TABLE, FILESET…):  "metalake::cat::schema::t::TABLE"
│   metadataIdCache.getIfPresent(key)
│     HIT  → use cached id, 0 DB                        [B] avoided on warm path
│     MISS → entityStore.get() → 1 indexed DB query
│            → metadataIdCache.put(key, id)
│
│   Called for every MetadataObject referenced in authorize()/isOwner() within the OGNL
│   expression (METALAKE, CATALOG, SCHEMA, TABLE, …). Without this cache, each unique
│   (obj, privilege) combination in the expression chain triggers a separate DB lookup
│   for the same entity id. See §4.4 for key structure and invalidation details.
│
├─ STEP 2.5 — Ownership check per isOwner() call (two-tier cache):
│   (Triggered by ANY(OWNER, …) in the OGNL expression — 2–4 calls per request.
│    Note: OGNL evaluates lazily with short-circuit; ancestor IDs are not pre-collected.)
│
│   Each isOwner(principal, metalake, metadataObject) call:
│     1. metadataId ← from metadataIdCache (Step 2, already resolved)
│     2. requestContext.ownerCache HIT for metadataId → return immediately (per-request dedup)
│     3. MISS → ownerRelCache.getIfPresent(metadataId)
│              HIT  → put into requestContext.ownerCache; compare ownerId; return, 0 DB
│              MISS → SELECT owner_id FROM owner_meta
│                     WHERE metadata_object_id = ? AND deleted_at = 0  ← 1 indexed query
│                     ownerRelCache.put(metadataId, ownerId)
│                     requestContext.ownerCache.put(metadataId, ownerId)
│                     compare ownerId with userId → return result
│
│   requestContext.ownerCache: per-HTTP-request Map<Long, Optional<Long>>.
│     Within one request the same metadataId is evaluated at most once.
│   ownerRelCache: persistent Caffeine cache (long TTL safety-net). Local node: hook
│     invalidates the specific metadataId immediately. HA peer nodes: owner poller queries
│     owner_meta WHERE updated_at > maxSeen, invalidates only the changed entries (≤ 1 s).
│     See §4.3 for rationale.
│
│   Also fixes existing bug: isOwner() currently calls MetadataIdConverter.getID() twice
│   for the same object (JcasbinAuthorizer lines 224, 228). Phase 2 consolidates to 1 call.
│
├─ STEP 3 — Role batch version check (1 query):
│
│   SELECT role_id, updated_at
│   FROM role_meta WHERE role_id IN (?, ?, ...) AND deleted_at = 0
│   ↑ one query validates all of the user's roles simultaneously
│
│   For each role where loadedRoles.get(roleId) == db.updated_at:
│     → policy current; skip                       [C2][C3] avoided
│
│   For stale/cold roles (db.updated_at != cached || not in cache):
│     → allowEnforcer.deleteRole(roleId); denyEnforcer.deleteRole(roleId)
│     → batchListSecurableObjectsByRoleIds(staleRoleIds)  (1 query for all stale roles)
│     → loadPoliciesForRoles(staleObjects)
│     → loadedRoles.put(roleId, db.updated_at)
│
└─ STEP 4 — enforce() (in-memory, O(1))
   allowEnforcer.enforce(userId, objectType, metadataId, privilege)
   denyEnforcer.enforce(userId, objectType, metadataId, privilege)
```

---

### 4.8 Properties

| Dimension                              | Value                                                                                                                             |
|----------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| Staleness — privilege / role changes   | **0** — version-validated on every request (Steps 1 + 3)                                                                          |
| Staleness — name→id, ownership (local) | **Immediate** — write-path hook fires after transaction commit                                                                    |
| Staleness — name→id, ownership (HA)    | **≤ poll interval** (default **1 s**) — mutation poller detects version advance, invalidates cache                                |
| Hot path DB queries                    | **3** (Step 1a user + Step 1b groups + Step 3 role versions; Steps 2 and 2.5 served from cache)                                   |
| name→id warm                           | **0** — metadataIdCache hit                                                                                                       |
| Owner check warm                       | **0** — ownerRelCache hit + requestContext.ownerCache dedup within request                                                        |
| Cold / stale path                      | **3 + k** (k = unique MetadataObjects not yet in metadataIdCache or ownerRelCache)                                                |
| Background threads                     | **1** lightweight mutation poller (single scheduled thread, 1 s interval, one tiny DB query — distinct from the removed executor) |
| Failure mode                           | DB unavailable → auth blocked (same as today); poller retries silently                                                            |
| HA correctness — privilege / role      | **Fixed** — Step 1 version check detects any GRANT/REVOKE on all nodes immediately                                                |
| HA correctness — name→id, ownership   | **Near real-time** — mutation poller bounds staleness to ≤ poll interval (~1 s)                                                   |

---

### 4.9 Correctness Under Mutation

The caches in Phase 2 fall into two consistency tiers. The analysis below covers both:

- **Strong consistency** (Steps 1 + 3): `userRoleCache`, `groupRoleCache`, `loadedRoles` — version-validated on every request.
- **Eventual consistency** (hook + change poller): `metadataIdCache`, `ownerRelCache` — immediate on local node via hook; HA peer nodes converge within the change poll interval (default 1 s) via **targeted** per-entry invalidation.

##### Strong-Consistency Scenarios (user/group/role)

| Scenario                                   | Analysis                                                                                                                                                                                                     |
|--------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **User / Group rename**                    | `userRoleCache` keyed on `metalakeName:userName`. Rename → cache miss → Step 1 queries DB → correct result. Old key expires via TTL. **Safe.**                                                               |
| **User / Group drop**                      | Step 1 returns zero rows → auth denied immediately. Old cache entry expires harmlessly. **Safe.**                                                                                                            |
| **User / Group drop + same-name recreate** | New entity gets new `user_id` and `updated_at = createdTime` (distinct from old entity's value). Cached entry holds old `user_id` and old `updated_at` → **mismatch on next Step 1 forces cache refresh.** ✅ |

##### Eventual-Consistency Scenarios — `metadataIdCache` (name → id)

| Scenario                                      | Local node                                                                                                                                                               | HA peer node (entity change poller, ≤ 1 s)                                                                                                                                                                                                                                                      |
|-----------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **SecurableObject rename** (table1 → table2)  | `handleEntityStructuralChange(table1)` → `metadataIdCache.invalidate("..::TABLE::table1")` immediately. `table2` is a cold miss → DB → same id=100. **Immediate, safe.** | Write path inserted a row `(TABLE, table1, updatedTime)` into `entity_change_log`. Poller reads it → `metadataIdCache.invalidate("..::TABLE::table1")`. `table2` is then a cold miss → DB → id=100. **Targeted, no security impact.**                                                           |
| **SecurableObject drop**                      | `handleEntityStructuralChange` → `metadataIdCache.invalidate(key)` immediately → next request: cache miss → DB "not found" → auth denied. **Immediate.**                 | Write path inserted `(TABLE, table1, updatedTime)`. Poller fires within ≤ 1 s → targeted `invalidate(key)` → cache miss → DB "not found" → DENY. **⚠️ ≤ 1 s window** before poller fires; JCasbin may still hold policy for the old id. Acceptable under the agreed eventual consistency model. |
| **SecurableObject drop + same-name recreate** | Hook invalidates old name on drop. New entity gets new id=200. Next request: cold miss → DB → id=200. No policy for id=200 → DENY. **Correct.**                          | Poller fires within ≤ 1 s on drop row → targeted invalidate. Cold miss → DB → id=200 → no policy → DENY. **Same ⚠️ ~1 s window as drop.**                                                                                                                                                       |
| **SecurableObject rename + privilege check**  | Rename does not change id. Old name invalidated immediately by hook. New name cold miss → DB → same id. **Safe.**                                                        | Old name row logged → poller invalidates it within ≤ 1 s. New name cold miss → DB → same id. No policy change; Step 3 still correct. **Safe.**                                                                                                                                                  |

> **Note on the drop ⚠️ window**: The risk is bounded to ≤ the change poll interval (default 1 s). Only the specific dropped entity's cache key is invalidated (not `invalidateAll()`), so unaffected entries remain hot. The dropped entity's JCasbin policy persists until the role's `updated_at` advances (next GRANT/REVOKE on that role → Step 3 detects mismatch → reload → policy cleared).

##### Eventual-Consistency Scenarios — `ownerRelCache` (metadataId → ownerId)

| Scenario                                    | Local node                                                                                                                                                                                         | HA peer node (owner change poller, ≤ 1 s)                                                                                                                                                                                                                                                                                                                                                            |
|---------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Ownership transferred** (owner1 → owner2) | `handleMetadataOwnerChange` → `ownerRelCache.invalidate(metadataId)` immediately. Next `isOwner(owner1)` → cache miss → DB → ownerId=owner2 → false. `isOwner(owner2)` → DB → true. **Immediate.** | `owner_meta.updated_at` set in same transaction. Poller reads `WHERE updated_at > maxSeen` → targeted `ownerRelCache.invalidate(metadataObjectId)` within ≤ 1 s. `isOwner(owner1)` → cache miss → false. `isOwner(owner2)` → DB → true. **⚠️ ≤ 1 s window** where old owner retains access / new owner blocked.                                                                                      |
| **Owner user dropped**                      | Hook fires → ownerRelCache invalidated → DB → empty owner.                                                                                                                                         | Stale ownerId may point to deleted user for ≤ 1 s. Deleted user can no longer authenticate at the API layer → no real risk.                                                                                                                                                                                                                                                                          |
| **Owned object dropped**                    | `handleEntityStructuralChange` → `metadataIdCache.invalidate(key)` → next request: `getID()` → DB entity not found → auth denied before `ownerRelCache` is consulted. **Immediate.**               | **⚠️ ≤ 1 s window** before entity change poller fires: `metadataIdCache` still has stale `id=100` → `isOwner()` resolves the old id → `ownerRelCache` may return stale ownerId → `enforce(id=100)` may ALLOW via orphan JCasbin policy. After entity change poller fires: `metadataIdCache.invalidate(key)` → `getID()` → entity not found → DENY. **Same ≤ 1 s window as the plain drop scenario.** |

> **Note on ownership ⚠️ window**: Ownership transfer is an administrative operation (not an emergency access revocation). A ≤ 1 s window on HA peer nodes is operationally acceptable in all deployments. Invalidation is **targeted** — only the transferred entity's cache entry is evicted, leaving unrelated entries hot.

---

### 4.10 Concurrent Mutation During Auth (TOCTOU)

The version check in Step 1 and the policy reload in Step 3 are not atomic with the
`enforce()` call in Step 4. A concurrent write on another thread or node can advance a
version counter between these steps. This section analyses the bounded impact.

**Scenario A — Role revoked between Step 1 read and Step 3 policy check**

```
Thread A  Step 1: reads updated_at = T5 → matches cache → roleIds = [R1, R2]
Thread B  commits: revokes R2 from user → user_meta.updated_at set to T6
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

### 4.11 Minimal Acceptance Test Matrix

Before merge, validate the following scenarios (single-node + HA two-node setup):

| Case                      | Setup                                   | Expected result                                                     |
|---------------------------|-----------------------------------------|---------------------------------------------------------------------|
| Revoke user role          | User has role R, then R revoked         | Next auth request denied on all nodes (no TTL wait)                 |
| Revoke role privilege     | Role R loses privilege P                | Next auth request for P denied on all nodes                         |
| Group role grant/revoke   | User inherits role only via group       | Auth reflects group mutation on next request                        |
| Owner transfer            | owner1 -> owner2                        | Local node immediate switch; HA peer converges within poll interval |
| Entity rename             | table old -> new                        | Old name denied after invalidation; new name resolves same ID       |
| Entity drop               | object dropped                          | Auth denied after invalidation (documented bounded HA window)       |
| Drop + recreate same name | drop old, create new                    | New ID used; stale policy does not authorize new entity             |
| Poller transient failure  | pause DB access for poller then recover | No privilege escalation; convergence resumes after recovery         |

Tests should include both functional assertions and query-count assertions on warm path
(target: Step 1 + Step 3 lightweight checks, caches hot for Step 2/2.5).

---

## 5. Summary

### 5.1 Query Count Comparison

With entity cache **enabled** and all in-process caches warm, the current system serves auth
from pure in-process Caffeine with **0 DB queries**. Phase 2 trades the old TTL-only model
for a mixed design: strong consistency for privilege/role checks, near-real-time eventual
consistency (≤ 1 s, targeted per-entry invalidation) for name→id and ownership lookups.

| Scenario                              | Entity cache ON (current)    | Phase 1                   | Phase 2 (warm)                                                 |
|---------------------------------------|------------------------------|---------------------------|----------------------------------------------------------------|
| Hot path — privilege check            | **0** (fully in-memory)      | 3+ heavy full-row queries | **3 lightweight**                                              |
| Hot path — owner check                | 0 (ownerRel warm)            | 1 heavy JOIN per call     | **0** (ownerRelCache hit)                                      |
| Hot path — name→id                    | 0 (entity cache warm)        | 1 full-row per call       | **0** (metadataIdCache hit)                                    |
| After privilege/role mutation (stale) | 0 (**stale — TTL hides it**) | 3+ heavy                  | **3 on next request** ✅                                        |
| After ownership/rename/drop (stale)   | 0 (**stale — TTL hides it**) | next request or TTL       | local: **immediate** (hook); HA: **≤ 1 s** (targeted poller)   |
| Cold start                            | ~3+T                         | ~3+T heavy                | **3+k lightweight** (k = cold cache misses)                    |
| HA staleness — privilege/role         | Up to 1 hour                 | Up to 1 hour              | **0** (version-validated)                                      |
| HA staleness — name→id, ownership     | Up to 1 hour                 | Up to 1 hour              | **≤ 1 s** (targeted per-entry invalidation via change pollers) |

### 5.2 Change Surface

| Dimension                        | Phase 1      | Phase 2                                                                                                                                                        |
|----------------------------------|--------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Entity cache                     | **Disabled** | Disabled                                                                                                                                                       |
| Schema — existing tables         | None         | **+4 `updated_at` columns**: role_meta, user_meta, group_meta (strong consistency sentinels), owner_meta (eventual consistency, owner change poller)           |
| Schema — new tables              | None         | **1** (`entity_change_log` — append-only log of entity name changes; pruned hourly)                                                                            |
| New caches (persistent)          | None         | **4** (`userRoleCache`, `groupRoleCache`, `metadataIdCache`, `ownerRelCache`)                                                                                  |
| Cache type changes               | None         | **1** (`loadedRoles` Boolean→Long — stores `updated_at` timestamp)                                                                                             |
| Removed caches                   | None         | None (old TTL-only `ownerRel` replaced by `ownerRelCache` with hook + poller)                                                                                  |
| New per-request context fields   | None         | **2** (`metadataIdCache`, `ownerCache` in `AuthorizationRequestContext`)                                                                                       |
| New authorizer hook              | None         | **1** (`handleEntityStructuralChange` — entity drop/rename invalidation)                                                                                       |
| Write-path additions             | None         | `updated_at` set (role/user/group/owner) + `entity_change_log` INSERT on rename/drop                                                                           |
| Background threads               | None         | **1** change poller (single scheduled thread, 1 s interval; runs `pollOwnerChanges` + `pollEntityChanges`)                                                     |
| Invalidation granularity         | —            | Privilege/role: per-request `updated_at` check; name→id: targeted per-name via `entity_change_log`; ownership: targeted per-entity via `owner_meta.updated_at` |
| Cache stampede on mutation       | —            | None — targeted invalidation; only the changed entries are evicted                                                                                             |
| External dependencies            | None         | None                                                                                                                                                           |

---

## 6. Alternative Approaches Considered

### 6.1 Can Phase 1 and Phase 2 Be Merged?

Yes. If the team has capacity, Phase 1 and Phase 2 can ship together. The separation exists
only if there is pressure to disable entity cache before the version infrastructure is ready.

### 6.2 Alternative: JcasbinAuthorizer as Distributed Cache

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

### 6.3 Possible Future Direction: Auth Decision Cache

Not on the current roadmap. Once Phase 2 is stable, caching the final auth decision
`(userId, objectType, metadataId, privilege) → ALLOW|DENY` would reduce the hot path to
zero DB queries. The prerequisite is Phase 2's version-check infrastructure.

---

## 7. Implementation Details

### 7.1 Mapper Additions

```java
// ── Result types (Java records) ────────────────────────────────────────────

/** Step 1a: user identity + role-list staleness sentinel. */
record UserAuthInfo(long userId, long updatedAt) {}

/** Step 1b: one row per group the user belongs to. */
record GroupAuthInfo(long groupId, long updatedAt) {}

/** Step 2.5: owner identity for a single metadata object. */
record OwnerInfo(long ownerId, String ownerType) {}

/** Step 3: role version sentinel returned by batch query. */
record RoleUpdatedAt(long roleId, long updatedAt) {}

/** Owner change poller result — one row per changed owner_meta entry. */
record ChangedOwnerInfo(long metadataObjectId, long updatedAt) {}

/** Entity change poller result — one row per entity_change_log entry. */
record EntityChangeRecord(
    String metalakeName,
    String entityType,
    String fullName,
    String operateType,   // "DROP" | "CREATE" | "ALTER"
    long   createdAt
) {}

// ── Mapper interfaces ───────────────────────────────────────────────────────

// RoleMetaMapper.java
void                touchUpdatedAt(@Param("roleId") long roleId, @Param("now") long now);
List<RoleUpdatedAt> batchGetUpdatedAt(@Param("roleIds") List<Long> roleIds);

// UserMetaMapper.java
void         touchUpdatedAt(@Param("userId") long userId, @Param("now") long now);
UserAuthInfo getUserInfo(
    @Param("metalakeName") String metalakeName, @Param("userName") String userName);

// GroupMetaMapper.java
void               touchUpdatedAt(@Param("groupId") long groupId, @Param("now") long now);
List<GroupAuthInfo> getGroupInfoByUserId(@Param("userId") long userId);

// OwnerMetaMapper.java
// Step 2.5 — single ownership lookup, deduped by requestContext
OwnerInfo          selectOwnerByMetadataObjectId(@Param("metadataObjectId") long metadataObjectId);
// Owner change poller — returns entries changed since the last poll
List<ChangedOwnerInfo> selectChangedOwners(@Param("updatedAtAfter") long updatedAtAfter);

// EntityChangeLogMapper.java
List<EntityChangeRecord> selectChanges(
    @Param("createdAtAfter") long createdAtAfter,
    @Param("maxRows")        int  maxRows);
void insertChange(
    @Param("metalakeName") String metalakeName,
    @Param("entityType")   String entityType,
    @Param("fullName")     String fullName,
    @Param("operateType")  String operateType,
    @Param("createdAt")    long   createdAt);
void pruneOldEntries(@Param("before") long before);
```

```xml
<!-- Step 1a: user staleness check, resolves metalake inline -->
<select id="getUserInfo" resultType="map">
  SELECT um.user_id, um.updated_at
  FROM user_meta um
  JOIN metalake_meta mm ON um.metalake_id = mm.metalake_id AND mm.deleted_at = 0
  WHERE mm.metalake_name = #{metalakeName} AND um.user_name = #{userName}
  AND um.deleted_at = 0
</select>

<!-- Step 1b: group staleness check, returns all groups the user belongs to -->
<select id="getGroupInfoByUserId" resultType="map">
  SELECT gm.group_id, gm.updated_at
  FROM group_meta gm
  JOIN group_user_rel gu ON gm.group_id = gu.group_id AND gu.deleted_at = 0
  WHERE gu.user_id = #{userId} AND gm.deleted_at = 0
</select>

<!-- Step 2.5: single ownership query, called per isOwner(); deduped by requestContext cache -->
<select id="selectOwnerByMetadataObjectId" resultType="map">
  SELECT owner_id, owner_type FROM owner_meta
  WHERE metadata_object_id = #{metadataObjectId} AND deleted_at = 0
</select>

<!-- Step 3: batch staleness check for roles -->
<select id="batchGetUpdatedAt" resultType="map">
  SELECT role_id, updated_at FROM role_meta
  WHERE role_id IN
  <foreach item="id" collection="roleIds" open="(" separator="," close=")">#{id}</foreach>
  AND deleted_at = 0
</select>

<update id="touchUpdatedAt">
  UPDATE role_meta SET updated_at = #{now}
  WHERE role_id = #{roleId}
</update>

<!-- Owner change poller: returns metadata_object_ids changed after a given time -->
<select id="selectChangedOwners" resultType="map">
  SELECT metadata_object_id, updated_at
  FROM owner_meta
  WHERE updated_at > #{updatedAtAfter}
  ORDER BY updated_at
</select>

<!-- Entity change log: returns affected names (for metadataIdCache targeted invalidation) -->
<select id="selectChanges" resultType="map">
  SELECT metalake_name, entity_type, full_name, operate_type, created_at
  FROM entity_change_log
  WHERE created_at > #{createdAtAfter}
  ORDER BY created_at
  LIMIT #{maxRows}
</select>

<insert id="insertChange">
  INSERT INTO entity_change_log
    (metalake_name, entity_type, full_name, operate_type, created_at)
  VALUES
    (#{metalakeName}, #{entityType}, #{fullName}, #{operateType}, #{createdAt})
</insert>

<!-- Prune old entries to bound table size; safe to run even on active nodes -->
<delete id="pruneOldEntries">
  DELETE FROM entity_change_log WHERE created_at &lt; #{before} LIMIT 1000
</delete>
```

### 7.2 Write Path Changes

**`RoleMetaService` — privilege change (grant or revoke):**
```java
long now = System.currentTimeMillis();
SessionUtils.doMultipleWithCommit(
    () -> securableObjectMapper.softDeleteSecurableObjects(roleId, ...),  // existing
    () -> securableObjectMapper.insertSecurableObjects(newObjects),        // existing
    () -> roleMetaMapper.touchUpdatedAt(roleId, now)                      // NEW, same tx
);
```

**`UserMetaService` — role assignment change:**
```java
long now = System.currentTimeMillis();
SessionUtils.doMultipleWithCommit(
    () -> userRoleRelMapper.softDeleteUserRoleRel(userId, roleIds),  // existing
    () -> userRoleRelMapper.insertUserRoleRels(newRelations),        // existing
    () -> userMetaMapper.touchUpdatedAt(userId, now)                 // NEW, same tx
);
```

**`OwnerMetaService` — ownership transfer:**

The transaction sets `owner_meta.updated_at = currentTimeMillis()` on the affected row
(same transaction as the owner change). The existing `handleMetadataOwnerChange()` hook fires
after commit for immediate local-node invalidation:

```java
long now = System.currentTimeMillis();
SessionUtils.doMultipleWithCommit(
    () -> ownerMetaMapper.softDeleteOwner(metadataObjectId),                        // existing
    () -> ownerMetaMapper.insertOwnerWithUpdatedAt(metadataObjectId, newOwnerId, now) // NEW, same tx
    // The new row carries updated_at = now; the owner poller picks it up on HA peers.
);
// After commit — immediate local invalidation
authorizer.handleMetadataOwnerChange(metalake, oldOwnerId, nameIdentifier, type);

// JcasbinAuthorizer — wire it to ownerRelCache
@Override
public void handleMetadataOwnerChange(String metalake, Long oldOwnerId,
                                       NameIdentifier nameIdentifier, Entity.EntityType type) {
    Long metadataId = MetadataIdConverter.getID(  // metadataIdCache hit, 0 DB
        NameIdentifierUtil.toMetadataObject(nameIdentifier, type), metalake);
    ownerRelCache.invalidate(metadataId);   // immediate on local node
    // HA peer nodes: owner poller detects updated_at advance → targeted invalidate() ≤ 1 s
}
```

**Entity rename/drop paths — new `handleEntityStructuralChange()` hook:**

The entity MetaService classes (`CatalogMetaService`, `SchemaMetaService`, `TableMetaService`,
and analogous for FILESET, TOPIC, VIEW, MODEL) must:
1. INSERT a row into `entity_change_log` with the **old name** (rename) or name (drop)
   **in the same transaction** as the data change.
2. Call `handleEntityStructuralChange()` **after commit** for immediate local-node invalidation.

```java
// In entity rename write path (example: CatalogMetaService)
String oldFullName = catalog.name();   // captured BEFORE the rename executes
long   now = System.currentTimeMillis();
SessionUtils.doMultipleWithCommit(
    () -> catalogMapper.updateCatalog(catalogId, newName, ...),                  // existing
    () -> entityChangeLogMapper.insertChange(                                     // NEW, same tx
              metalakeName, "CATALOG", oldFullName, "ALTER", now)
    // Stores the OLD name + operate_type=ALTER. On DELETE cascade child records
    // in SCHEMA/TABLE etc. do NOT need separate rows — prefix invalidation covers them.
);
// After commit — immediate local prefix invalidation of old key + all children
authorizer.handleEntityStructuralChange(metalake, oldNameIdentifier, EntityType.CATALOG);

// In entity drop write path (example: CatalogMetaService)
long now2 = System.currentTimeMillis();
SessionUtils.doMultipleWithCommit(
    () -> catalogMapper.softDeleteCatalog(catalogId),                            // existing
    () -> entityChangeLogMapper.insertChange(                                     // NEW, same tx
              metalakeName, "CATALOG", catalogFullName, "DROP", now2)
    // One row for the catalog; prefix invalidation on HA peers cascades to all children.
);
authorizer.handleEntityStructuralChange(metalake, nameIdentifier, EntityType.CATALOG);

// GravitinoAuthorizer (new method)
void handleEntityStructuralChange(String metalake, NameIdentifier nameIdent, Entity.EntityType type);

// JcasbinAuthorizer implementation
@Override
public void handleEntityStructuralChange(String metalake, NameIdentifier nameIdent,
                                          Entity.EntityType type) {
    // Build hierarchical cache key; non-leaf types get trailing "::" for prefix cascade.
    String cacheKey = buildCacheKey(metalake, type, nameIdent.toString());
    metadataIdCache.invalidateByPrefix(cacheKey);  // immediate on local node;
    // works for both leaf (exact match) and non-leaf (cascades all children under prefix)
    // HA peer nodes: entity change poller reads entity_change_log WHERE created_at > maxSeen
    // → buildCacheKey + invalidateByPrefix for each changed entry ≤ 1 s
}

/** Build a hierarchical metadataIdCache key from its components.
 *  Non-leaf entities (METALAKE, CATALOG, SCHEMA) end with "::" so that
 *  invalidateByPrefix cascades to all child entities in one call.
 *  Leaf entities (TABLE, FILESET, TOPIC, VIEW, MODEL) append "::TYPE" for disambiguation.
 *
 *  Examples:
 *    (lake1, CATALOG, "cat1")           → "lake1::cat1::"
 *    (lake1, SCHEMA,  "cat1.schema1")   → "lake1::cat1::schema1::"
 *    (lake1, TABLE,   "cat1.schema1.t1")→ "lake1::cat1::schema1::t1::TABLE"
 */
static String buildCacheKey(String metalake, Entity.EntityType type, String fullName) {
    StringBuilder sb = new StringBuilder(metalake);
    for (String part : fullName.split("\\.")) {
        sb.append("::").append(part);
    }
    if (isNonLeaf(type)) {
        sb.append("::");        // trailing :: enables prefix cascade for all children
    } else {
        sb.append("::").append(type.name());  // ::TABLE, ::FILESET, etc.
    }
    return sb.toString();
}

private static boolean isNonLeaf(Entity.EntityType type) {
    return type == Entity.EntityType.METALAKE
        || type == Entity.EntityType.CATALOG
        || type == Entity.EntityType.SCHEMA;
}
```

**Change Poller Implementation (JcasbinAuthorizer):**

```java
private void startChangePoller(long intervalMs) {
    changePoller = Executors.newSingleThreadScheduledExecutor(
        r -> new Thread(r, "gravitino-change-poller"));
    changePoller.scheduleAtFixedRate(
        this::pollChanges, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
}

/** Two targeted poll queries per interval — owner changes and entity name changes. */
private void pollChanges() {
    pollOwnerChanges();
    pollEntityChanges();
}

private void pollOwnerChanges() {
    try {
        long since = maxOwnerUpdatedAt.get();
        List<ChangedOwnerInfo> rows = ownerMetaMapper.selectChangedOwners(since);
        long maxAt = since;
        for (ChangedOwnerInfo row : rows) {
            ownerRelCache.invalidate(row.metadataObjectId());  // targeted: only this entry
            if (row.updatedAt() > maxAt) maxAt = row.updatedAt();
        }
        maxOwnerUpdatedAt.set(maxAt);
    } catch (Exception e) {
        LOG.warn("Owner change poller failed, will retry", e);
    }
}

private void pollEntityChanges() {
    try {
        long since = maxEntityCreatedAt.get();
        List<EntityChangeRecord> rows = entityChangeLogMapper.selectChanges(since, 1000);
        long maxAt = since;
        for (EntityChangeRecord row : rows) {
            Entity.EntityType type = Entity.EntityType.valueOf(row.entityType());
            String cacheKey = buildCacheKey(row.metalakeName(), type, row.fullName());
            // invalidateByPrefix handles both leaf (exact match) and non-leaf (cascade):
            //   DROP/ALTER on CATALOG "cat1" → prefix "lake::cat1::" evicts catalog + all children
            //   DROP on TABLE "cat1.s1.t1"   → prefix "lake::cat1::s1::t1::TABLE" evicts exactly one entry
            metadataIdCache.invalidateByPrefix(cacheKey);
            if (row.createdAt() > maxAt) maxAt = row.createdAt();
        }
        maxEntityCreatedAt.set(maxAt);
    } catch (Exception e) {
        LOG.warn("Entity change poller failed, will retry", e);
    }
}

@Override
public void close() {
    if (changePoller != null) {
        changePoller.shutdownNow();
    }
    // ... other cleanup
}
```

**entity_change_log pruning:** A low-priority task (e.g. once per hour) calls
`entityChangeLogMapper.pruneOldEntries(System.currentTimeMillis() - RETENTION_MS)` to keep
the table small. The retention window must be longer than the poll interval by a safe margin
(default: 1 hour retention, 1 s poll interval). Pruning is idempotent and can run on any node.

All schema writes (`updated_at` set, `entity_change_log` INSERT) happen in
**the same DB transaction** as the data change. If the transaction rolls back, none of the
tracking writes are committed — no spurious cache invalidations on HA peers.

### 7.3 GravitinoCache Interface

```java
public interface GravitinoCache<K, V> extends Closeable {
    Optional<V> getIfPresent(K key);
    void put(K key, V value);
    void invalidate(K key);
    void invalidateAll();
    /** Evict all entries whose key starts with the given prefix string.
     *  Only applicable when K = String. Used by metadataIdCache for cascade
     *  invalidation: dropping a catalog evicts the catalog entry plus all
     *  schema/table/fileset/... entries beneath it in one call. */
    void invalidateByPrefix(String prefix);
    long size();
}
```

`CaffeineGravitinoCache<K,V>` — wraps Caffeine with configurable TTL and max size.
`invalidateByPrefix` iterates `cache.asMap().keySet()` and evicts matching entries
(`O(n)` over cache size, which is bounded and DDL is rare):
```java
@Override
public void invalidateByPrefix(String prefix) {
    cache.asMap().keySet().stream()
         .filter(k -> ((String) k).startsWith(prefix))
         .forEach(cache::invalidate);
}
```
`NoOpsGravitinoCache<K,V>` — no-op implementation for tests.

---

## 8. Phased Implementation Plan

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

| Step  | Change                                                                                                                                                                                                                                                                                                                                                         | Module                                                                                          |
|-------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| 2.1   | `ADD COLUMN updated_at` on `role_meta`                                                                                                                                                                                                                                                                                                                         | `schema-x.y.z-*.sql`                                                                            |
| 2.2   | `ADD COLUMN updated_at` on `user_meta`                                                                                                                                                                                                                                                                                                                         | `schema-x.y.z-*.sql`                                                                            |
| 2.3   | `ADD COLUMN updated_at` on `group_meta`                                                                                                                                                                                                                                                                                                                        | `schema-x.y.z-*.sql`                                                                            |
| 2.4   | Set `role_meta.updated_at = now()` in privilege grant/revoke transaction                                                                                                                                                                                                                                                                                       | `RoleMetaService`                                                                               |
| 2.5   | Set `user_meta.updated_at = now()` and `group_meta.updated_at = now()` in role assign/revoke transaction                                                                                                                                                                                                                                                       | `UserMetaService`, `GroupMetaService`                                                           |
| 2.6   | Add `userRoleCache: GravitinoCache<String, CachedUserRoles>`                                                                                                                                                                                                                                                                                                   | `JcasbinAuthorizer`                                                                             |
| 2.7   | Add `groupRoleCache: GravitinoCache<String, CachedGroupRoles>`; implement group role loading in `loadRolePrivilege()` (currently missing entirely)                                                                                                                                                                                                             | `JcasbinAuthorizer`                                                                             |
| 2.8   | Change `loadedRoles` type: `Boolean` → `Integer` (stores version)                                                                                                                                                                                                                                                                                              | `JcasbinAuthorizer`                                                                             |
| 2.9   | Rewrite `loadRolePrivilege()` + `authorize()` with 4-step flow (§4.7); remove `executor` thread pool (replaced by batch query)                                                                                                                                                                                                                                 | `JcasbinAuthorizer`                                                                             |
| 2.10  | Add mapper methods (see §7.1)                                                                                                                                                                                                                                                                                                                                  | mapper + SQL                                                                                    |
| 2.11  | Replace `ownerRel` (TTL-only) with `ownerRelCache` (TTL safety-net + `handleMetadataOwnerChange()` hook); add `ownerCache: Map<Long, Optional<Long>>` to `AuthorizationRequestContext` for per-request dedup; fix double `getID()` call in `isOwner()`                                                                                                         | `JcasbinAuthorizer`, `AuthorizationRequestContext`                                              |
| 2.12  | Add `metadataIdCache: GravitinoCache<String, Long>` (TTL safety-net + `handleEntityStructuralChange()` hook); add `metadataIdCache: Map<String, Long>` to `AuthorizationRequestContext` for per-request dedup                                                                                                                                                  | `JcasbinAuthorizer`, `AuthorizationRequestContext`, `GravitinoAuthorizer`                       |
| 2.13  | Wire `handleEntityStructuralChange()` into entity drop/rename write paths; INSERT into `entity_change_log` (old name on rename, name on drop) in the same transaction                                                                                                                                                                                          | `CatalogMetaService`, `SchemaMetaService`, `TableMetaService`, (FILESET / TOPIC / VIEW / MODEL) |
| 2.14  | Extend `AuthorizationRequestContext.loadRole()` guard to cover both user and group role loading in one pass                                                                                                                                                                                                                                                    | `AuthorizationRequestContext`                                                                   |
| 2.15  | Add `updated_at` column to `owner_meta`; add `OwnerMetaMapper.selectChangedOwners(updatedAtAfter)`                                                                                                                                                                                                                                                             | `schema-x.y.z-*.sql`, mapper + SQL                                                              |
| 2.16  | Create `entity_change_log` table; add `EntityChangeLogMapper` (`selectChanges(createdAtAfter)`, `insertChange(metalakeName, entityType, fullName, operateType, createdAt)`, `pruneOldEntries(before)`); add `buildCacheKey()` + `isNonLeaf()` helpers; add `invalidateByPrefix()` to `GravitinoCache`                                                          | `schema-x.y.z-*.sql`, mapper + SQL, `JcasbinAuthorizer`, `GravitinoCache`                       |
| 2.17  | Wire `entity_change_log` INSERT into entity rename/drop write paths (old name + `operate_type=ALTER/DROP`); one row per operation regardless of subtree size                                                                                                                                                                                                   | All entity MetaService classes                                                                  |
| 2.18  | Implement change poller in `JcasbinAuthorizer`: `maxOwnerUpdatedAt`, `maxEntityCreatedAt`, `startChangePoller()` / `pollOwnerChanges()` / `pollEntityChanges()` / `close()`                                                                                                                                                                                    | `JcasbinAuthorizer`                                                                             |
| 2.19  | **Performance validation**: run auth hot-path benchmark before and after Phase 2; confirm ≤ 3 DB queries per `authorize()` call under cache-hit conditions; measure p50/p99 latency with concurrent auth load (≥ 100 threads); verify `metadataIdCache` and `ownerRelCache` hit rates ≥ 95% after warm-up; confirm poller CPU overhead < 1% under steady state | `JcasbinAuthorizer` benchmark / JMH or load test                                                |

**Outcome:** Privilege/role changes: 0 staleness (version-validated). Name→id and ownership: eventual consistency, ≤ poll interval (~1 s) on HA nodes via targeted invalidation, immediate on local node via hooks. Hot path: **3 DB queries** (Steps 1a + 1b + 3; Steps 2 and 2.5 served from Caffeine cache).
