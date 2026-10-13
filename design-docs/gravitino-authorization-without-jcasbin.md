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

# Design of a JCasbin-free Policy Evaluator for the Built-in Authorizer in Gravitino

| Item                     | Value                                                                                                                                       |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------- |
| Design issue             | [#13789](https://github.com/apache/gravitino/issues/13789)                                                                                  |
| Implementation PR        | [#12286](https://github.com/apache/gravitino/pull/12286)                                                                                    |
| Per-role policy index    | [#12173](https://github.com/apache/gravitino/issues/12173)                                                                                  |
| Enforcer removal         | [#12282](https://github.com/apache/gravitino/issues/12282)                                                                                  |
| Shared role bindings bug | [#13748](https://github.com/apache/gravitino/issues/13748)                                                                                  |
| Scope                    | `JcasbinAuthorizer` in `server-common`, `AuthorizationRequestContext` in `core`, the JCasbin dependency in `server-common` and Iceberg REST |

---

## Background

### What the authorizer decides

Gravitino's built-in authorizer answers questions of the form:

> Can the current caller use privilege `P` on metadata object `O` (type + id) in metalake `M`?

Its inputs are:

| Input                           | Source                                                                                                                                                                                                                           |
| ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| The caller                      | A user stored in Gravitino, plus the groups carried by the request. With OAuth/JWT, groups come from the token's groups claim and live only in the request's `UserPrincipal`; Gravitino does not store user-to-group membership. |
| Roles held by the caller        | Roles granted to the user directly, plus roles granted to each of the request's groups (`user_role_rel`, `group_role_rel`).                                                                                                      |
| What each role allows or denies | The role's securable objects; each privilege has condition `ALLOW` or `DENY`.                                                                                                                                                    |
| Active roles                    | With role assumption, a request can narrow its ALLOW privileges to a subset of its roles. DENY always applies over all held roles.                                                                                               |
| Ownership                       | `OWNER` is answered from the owner relation (`owner_meta`), not from role privileges.                                                                                                                                            |

The rules for a role-privilege check, that is, `authorize` or `deny` for a privilege other than `OWNER`, are:

1. A `DENY` from **any** held role on `(O, P)` wins.
2. Otherwise, an `ALLOW` from any **active** role on `(O, P)` grants access.
3. Otherwise the check is denied. Callers walk parent scopes (metalake, catalog, schema chain, object) and stop at the first scope that grants.

These rules cover only the role-privilege branch of an authorization expression. Expressions combine it with paths that do not read role policies:

| Path                  | Expression token                                     | Answered from                                                                                           |
| --------------------- | ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Ownership             | `OWNER`, `ANY(OWNER, ...)`, `TABLE::OWNER`           | The owner relation                                                                                      |
| Service administrator | `SERVICE_ADMIN`                                      | The configured service administrators                                                                   |
| Metalake membership   | `METALAKE_USER`                                      | The user's existence in the metalake                                                                    |
| Self access           | `USER::SELF`, `GROUP::SELF`, `ROLE::SELF` (`isSelf`) | The principal's user name and groups; for a role, the caller's role membership, not the role's policies |

For example, `LOAD_TABLE_AUTHORIZATION_EXPRESSION` starts with `ANY(OWNER, METALAKE, CATALOG) || ...`, so a catalog owner can load a table even when one of their roles denies `SELECT_TABLE`. `testHasMetadataPrivilegePermissionAllowsOwnerWithDenyManageGrants` keeps this behavior. This design does not change these paths.

List endpoints evaluate the expression for every returned object, so each object goes through the role-privilege rules unless another path already grants it. `hasDenyPolicy` is a list shortcut: if the caller holds no DENY for the relevant privileges at any scope, a parent-level ALLOW is enough and per-object checks can be skipped.

### How JCasbin is used today

The authorizer embeds two JCasbin `SyncedEnforcer` instances, one for ALLOW and one for DENY, with this model (`jcasbin_model.conf`):

```ini
[request_definition]
r = sub, metadataType, metadataId, act

[policy_definition]
p = sub, metadataType, metadataId, act, eft

[role_definition]
g = _, _

[policy_effect]
e = some(where (p.eft == allow)) && !some(where (p.eft == deny))

[matchers]
m = g(r.sub, p.sub) && r.metadataId == p.metadataId && r.metadataType == p.metadataType && ((p.act == "OWNER" && r.act == "OWNER") || r.act == p.act)
```

Gravitino state is translated into JCasbin rows:

| Gravitino state                                              | JCasbin row                            | Written when                                       |
| ------------------------------------------------------------ | -------------------------------------- | -------------------------------------------------- |
| Role `R` grants `SELECT_TABLE` on table 42                   | `p, R, TABLE, 42, SELECT_TABLE, allow` | Role policies are loaded or reloaded               |
| User `U` holds role `R`, directly or through a request group | `g, U, R`                              | Each request binds its roles and prunes stale ones |

A check calls `enforcer.enforce(userId, type, id, privilege)`. Around the enforcers, the authorizer already keeps version-validated caches:

- `userRoleCache` / `groupRoleCache`: user or group to role ids, validated by `user_meta.updated_at` / `group_meta.updated_at`.
- `loadedRoles`: role id to its loaded version (`role_meta.updated_at`). Its removal listener deletes the role's `p` rows from both enforcers.
- `metadataIdCache` / `ownerRelCache`: name to id and id to owner, invalidated by local hooks and the entity change-log poller.
- A global `ReentrantReadWriteLock` guards every enforcer read and write.

In practice JCasbin is used as an in-memory table of `(role, type, id, privilege, effect)` rows plus a user-to-role edge list. None of its richer features are used: no role hierarchy, no domains, no ABAC expressions, and the adapter is a no-op.

### Problem 1: every probe scans every policy on the node

JCasbin 1.99.0 has no index by subject or object. `enforce` evaluates the Aviator matcher against **every** `p` row loaded in the enforcer, including rows of roles the caller does not hold. 1.99.0 is the latest release on Maven Central, and the loop in `CoreEnforcer.enforce` is unchanged up to `v1.100.0-rc2`. The enforcer accumulates rows for up to `gravitino.authorization.jcasbin.roleCacheSize` roles (default 10,000) across all metalakes.

Standalone benchmark from [#12173](https://github.com/apache/gravitino/issues/12173) (real model, one user with one role, one target object):

| Roles loaded | `p` rows | `enforce` avg | Hash lookup |
| ------------ | -------- | ------------- | ----------- |
| 100          | 6,000    | ~1.0 ms       | 0.06 µs     |
| 1,000        | 60,000   | ~12.8 ms      | 0.015 µs    |
| 5,000        | 300,000  | ~51 ms        | 0.015 µs    |
| 10,000       | 600,000  | ~106 ms       | 0.015 µs    |

The cost is multiplied because one request makes many probes:

- each scope in the parent walk, including every level of a hierarchical schema name;
- the allow enforcer and the deny enforcer;
- with active-role narrowing, one probe per active role (`enforceNarrowed`);
- in list endpoints, all of the above per listed object.

The latency of one user's list call therefore depends on how many policies *other* users' roles have loaded on the same node.

### Problem 2: a shared, mutable user-to-role graph

Role membership is computed per request (user roles plus roles of the request's groups), but then **published** to shared `g(userId, roleId)` rows that every request of the same user reads.

**Correctness ([#13748](https://github.com/apache/gravitino/issues/13748)).** The same user can send concurrent requests with different group claims, for example an old token in a long-running Spark job and a new token after an IdP group change. Each request binds its roles and prunes the ones it does not hold, so one request can overwrite the other's view:

| Step | Event                                                                               | Shared `g(alice, ...)`         |
| ---- | ----------------------------------------------------------------------------------- | ------------------------------ |
| 1    | Request A (`groups=[analysts, contractors]`) binds `read_sales` and `no_pii` (DENY) | read_sales, no_pii             |
| 2    | Request B (`groups=[analysts]`) prunes `no_pii`, which it does not hold             | read_sales                     |
| 3    | A checks `SELECT_TABLE` on `sales.orders`                                           | **allowed**: the DENY was lost |

The reverse interleaving injects an ALLOW into a request that should hold no role.

**Duplicated state.** Role membership already lives in version-validated caches. The `g` rows are a second copy that must be bound, pruned, and kept consistent with the first. Binding and pruning take the global write lock on the request path, and policy reload cannot use `deleteRole`, because that would also drop other requests' fresh `g` rows.

### Problem 3: policy reload mutates shared state in place

Reloading a role means removing its `p` rows and then adding the new ones on a shared enforcer. Without a lock, a concurrent `enforce` could observe the empty intermediate state and deny a valid request. All enforcer access therefore goes through one global read-write lock, and the cache removal listener also mutates the enforcer. Any eviction, including ordinary TTL or size eviction, removes rows that in-flight requests still depend on, and those requests must detect the change and reload.

### Problem 4: the DENY existence check works on string rows

`hasDenyPolicy` iterates `getRolesForUser` and `getFilteredNamedPolicy("p", 0, roleId)` and compares positional `List<String>` fields. Every policy is stored as a list of strings, with ids converted to `String`. There is no per-role summary of denied privileges.

### Problem 5: dependency weight without matching benefit

JCasbin brings the enforcer, Aviator, model files, a no-op adapter, engine-specific tests, and license entries in `LICENSE.bin` and `LICENSE.iceberg`. The work it actually does is an equality match that a hash map does in O(1).

---

## Goals

1. **Policy-count-independent probes**: The cost of one authorization probe does not depend on the number of policies loaded on the node. It is at most O(roles held by the caller) hash lookups, and O(1) after a request builds its merged index (see [Adaptive merge](#adaptive-merge-for-long-lists)).
2. **Request-isolated role membership**: A request's effective roles are computed from its own principal and stored only in its own context. No request can add or remove roles seen by another request.
3. **Immutable published policies**: Once a role's policies are published, they are never edited in place. Reload replaces a reference.
4. **Unchanged decisions**: Global DENY precedence, active-role narrowing for ALLOW only, OWNER via the owner relation, role version validation, and fail-closed handling of unreadable or unresolvable roles behave as today. This is verified by a differential comparison against the current JCasbin model.
5. **Defined freshness**: A request that starts after a role change has committed uses the new role version, on any node. Within a request, the probes of one authorization expression all use one role version. [Freshness and consistency](#freshness-and-consistency) lists the guarantee for each scope.
6. **No JCasbin dependency**: `server-common` and the Iceberg REST server no longer depend on JCasbin or Aviator at runtime. JCasbin remains a test-only dependency for the differential test.
7. **Bounded lock and memory use**: Evaluation never runs under the global policy lock, and a request never builds a merged index of more than 100,000 policies.

---

## Non-Goals

1. **Privilege model changes**: No new privileges, conditions, or decision rules. This design changes how decisions are computed, not what they are.
2. **Public API or configuration changes**: No REST, client, or configuration-key change. The class name `JcasbinAuthorizer` is kept so that existing `gravitino.authorization.impl` values keep working, and the `gravitino.authorization.jcasbin.*` keys keep their names even though JCasbin is gone. Renaming either is a separate decision because it needs a deprecation path.
3. **Cross-node invalidation changes**: The existing channels (local hooks and the entity change-log poller) are reused as they are. Other nodes detect role changes through the per-request version check, not through a broadcast; adding one is out of scope.
4. **External policy engines**: OPA, Cedar, or Ranger are not introduced for the built-in authorizer; see [Solution Investigations](#solution-investigations).
5. **Persisting compiled policies**: Compiled indexes stay in memory and are rebuilt from role entities on demand, as JCasbin rows are today. No schema change is needed.

---

## Solution Investigations

| Approach                                                                                                                                        | Pros                                                                                | Cons                                                                                                                                                                                                                                                                                                                                                                                                                         | Decision                                                         |
| ----------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| A. Keep JCasbin, replace `g(r.sub, p.sub)` with a request-aware `hasRole` function ([#13747](https://github.com/apache/gravitino/issues/13747)) | Small diff; fixes [#13748](https://github.com/apache/gravitino/issues/13748)        | Keeps the O(N) scan, in-place reload, the global lock around every probe, and the dependency                                                                                                                                                                                                                                                                                                                                 | Rejected as the long-term design; acceptable as a short-term fix |
| B. Keep JCasbin, one enforcer per role or per user                                                                                              | Removes the global scan                                                             | Per role: still evaluates Aviator per row and pays model overhead per enforcer. Per user: duplicates policies across users and must be invalidated on every role change                                                                                                                                                                                                                                                      | Rejected                                                         |
| C. Precompute a per-user effective policy map                                                                                                   | O(1) lookups                                                                        | Group membership is per request, so the key is (user, group set, active roles). Any role change invalidates every combination that includes it, and the number of combinations is unbounded                                                                                                                                                                                                                                  | Rejected                                                         |
| D. External policy engine (OPA, Cedar, Ranger)                                                                                                  | Rich policy languages and central management                                        | Adds a network hop or a new runtime, and still needs Gravitino's id resolution, versioning and owner logic. Ranger already exists as a separate authorization plugin                                                                                                                                                                                                                                                         | Rejected for the built-in authorizer                             |
| E. Upgrade JCasbin, or use its `SyncedCachedEnforcer`                                                                                           | No code change beyond configuration                                                 | Newer versions keep the same linear scan. The cached enforcer caches results per request key, so list requests with distinct objects still scan on first use. Its invalidation only removes the cache entry whose key equals the changed policy row, which never matches a `(userId, ...)` request key, and grouping changes and `removeFilteredPolicy` do not invalidate at all, so a revoked ALLOW can keep being returned | Rejected                                                         |
| F. Immutable per-role hash indexes pinned by a request-local view                                                                               | O(k) or O(1) probes; isolation by construction; no in-place mutation; no dependency | New concurrency code for publication and invalidation that must be proven correct                                                                                                                                                                                                                                                                                                                                            | **Chosen**                                                       |

Option A and option F both make role membership request-local, so they agree on [#13748](https://github.com/apache/gravitino/issues/13748). Option F additionally removes the costs listed in Problems 1, 3, 4 and 5. Option E is listed because upgrading is the cheapest change, but it leaves Problem 1 in place and adds a stale-result risk.

---

## Proposal

### Overview

The key idea is: **resolve, don't scan.** A request already knows its role ids before it evaluates anything. Each role is compiled once into an immutable hash index, and the request pins references to the indexes of the roles it holds.

```
Shared, per role, immutable              Per request, request-local
---------------------------------        -------------------------------------
loadedRoles                              AuthorizationRequestContext
  roleId -> CachedRolePolicies             boundRoleIds = {7, 9}
    version (current_version)              rolePolicyView = RequestRolePolicies
    index: PolicyKey -> Effect   <--pin--    allowRoles, denyRoles
    complete flag                            deniedPrivileges
    deniedPrivileges summary                 generation
```

- The shared cache holds **per-role** data only. It contains no user or group membership.
- A request computes its effective role ids (direct roles plus roles of its own groups), pins the matching `CachedRolePolicies`, and evaluates against them.
- The pinned role indexes never change. The view itself is request-local state: a validated generation, a probe budget, and an optional merged index.
- Publishing a new role version or invalidating a role advances a **generation**. Before each new uncached decision, a request whose pinned roles changed refreshes its view.

### API changes

There is no public API, REST, or client change. `AuthorizationRequestContext` in `core` gains an authorizer-defined view, so that it does not depend on `server-common`, and an epoch that lets an expression detect a view replacement:

```java
public class AuthorizationRequestContext {
  /** An authorizer-defined view of the caller's role policies. */
  public interface RolePolicyView {
    /** Returns the policy generation most recently validated for this view. */
    long generation();
  }

  /** Returns this request's view, or null before role loading. */
  @Nullable public RolePolicyView getRolePolicyView();

  /** Publishes a view; replacing an earlier one drops cached decisions and advances the epoch. */
  public void setRolePolicyView(RolePolicyView view);

  /** Returns the view-replacement epoch; odd while a replacement is in progress. */
  public long getRolePolicyEpoch();
}
```

The storage objects used only by the authorizer also carry the role version. `RoleUpdatedAt` and the role rows of `AuthPrefetchRow` gain a `version` field, filled from `role_meta.current_version` by `RoleMetaMapper.batchGetRoleUpdatedAt` and `UserMetaMapper.batchGetAuthSubjectsForUser`. No column is added.

### Data model

All new types are package-private in `org.apache.gravitino.server.authorization.jcasbin`. Nothing is persisted.

#### `PolicyKey` and `Effect`

```java
final class PolicyKey {          // (metadataType, metadataId, privilege), hash precomputed
  String metadataType;
  long metadataId;
  String privilege;
}

enum Effect { ALLOW, DENY }      // DENY wins over ALLOW
```

`PolicyKey` replaces a `p` row without its subject; the subject is implied by which role's index holds the key.

#### `ResolvedRolePolicies`: one role compiled

Built from a `RoleEntity` outside any lock:

| Field                           | Meaning                                                                                                                                  |
| ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `index: Map<PolicyKey, Effect>` | Each securable object resolved to its metadata id; each privilege becomes one key. Within one role, DENY beats ALLOW on the same key.    |
| `unresolvedObjects`             | Objects whose id could not be resolved, usually dropped objects still referenced by the role. Logged with throttling.                    |
| `unresolvedDenies`              | Privilege names of DENYs on unresolved objects. They keep the `hasDenyPolicy` shortcut disabled even though the exact object is unknown. |

If an object's name cannot be normalized because the catalog capability lookup failed, its DENY privileges are installed conservatively on the **catalog** id, so a parent ALLOW cannot bypass a DENY that is temporarily unresolvable. If even the catalog id cannot be resolved, the role is marked unreadable and none of its policies are published.

#### `CachedRolePolicies`: shared, immutable per-role entry

```java
final class CachedRolePolicies {
  long version;                         // role_meta.current_version read before loading
  Map<PolicyKey, Effect> index;         // unmodifiable copy
  boolean complete;                     // false if any securable object was unresolved
  Set<String> deniedPrivileges;         // DENY privileges in index + unresolvedDenies
}
```

Stored in `loadedRoles: GravitinoCache<Long, CachedRolePolicies>`. An incomplete entry is reused only while `partialRoleLoadBackoff` (10 s) holds for the role, so a role that references a dropped object is retried periodically rather than on every request.

#### `RequestRolePolicies`: per-request view

Implements `RolePolicyView` and is stored in the request context. The first four fields are fixed when the view is built and only reference immutable role entries. The last three are request-local state that changes while the request runs.

```java
final class RequestRolePolicies {
  // Fixed when the view is built.
  List<CachedRolePolicies> allowRoles;          // held AND active roles
  List<CachedRolePolicies> denyRoles;           // held roles with any DENY, active or not
  Set<String> deniedPrivileges;                 // union over all held roles
  boolean readable;                             // false if any role could not be read
  // Request-local state.
  AtomicLong generation;                        // last validated policy generation
  AtomicLong remainingProbes;                   // budget before building a merged index
  volatile Map<PolicyKey, Effect> mergedIndex;  // built lazily for long requests
}
```

A role is in `allowRoles` only if the request's active roles include it. For named activation, the role name and the policies must come from the same role version, so a racing newer loader cannot activate a renamed role.

#### Publication state in the authorizer

| Field                                        | Purpose                                                                                                                                                                                                                                                      |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `rolePolicyLock` (read-write lock)           | Serializes publication and invalidation, and guards the generation check that validates a view. It is held only for in-memory map updates and that check; evaluation and merging run after it is released, and no database or metadata lookup runs under it. |
| `rolePolicyGenerationCounter`                | Monotonic counter, advanced on each publication or explicit invalidation.                                                                                                                                                                                    |
| `roleClearGenerations: roleId -> generation` | Generation of a role's latest publication or invalidation. Bounded by the role cache size; on overflow the older half is pruned and `prunedRoleClearGeneration` is raised, so a request older than that watermark conservatively reloads once.               |
| `loadingRoles: roleId -> RoleLoadState`      | In-flight loaders keep publication state even if the cache evicts the role, so a slow loader with an older version cannot overwrite a newer publication or resurrect an invalidated role.                                                                    |

TTL and size eviction of `loadedRoles` **do not** advance generations: requests pin immutable objects, so eviction cannot change what an in-flight request sees.

#### Role version contract

A cached index is reused only while its version is at least the role's version in the database, so the version must strictly increase with every change that can alter a decision. `role_meta.updated_at` does not satisfy this: it is a millisecond database timestamp, two updates can store the same value, and on PostgreSQL `CURRENT_TIMESTAMP` is the transaction start time, which does not follow commit order. The design therefore uses `role_meta.current_version`:

| Requirement                                  | How it is met                                                                                                                                                                                                                                                                                                                                                                                                                       |
| -------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Strictly increasing per role                 | `RoleMetaService.updateRole` sets `current_version = old + 1` with `WHERE current_version = old`, so concurrent updates cannot reuse a value.                                                                                                                                                                                                                                                                                       |
| Covers every decision-changing update        | Granting or revoking privileges goes through `updateRole`, and overwriting a role through `insertRole(role, true)` also sets `current_version = current_version + 1`. Both change the version in the same transaction as the securable objects.                                                                                                                                                                                     |
| Deletions                                    | Dropping a role removes its `role_meta` row; the loader sees no version and invalidates the role. Dropping a metadata object deletes the securable-object rows that point at it without changing the role version. Those policies keep an id that no longer exists and is never reused, so they cannot affect a decision.                                                                                                           |
| Loaded content is not older than its version | The loader reads the version first, then reads the role from the database through `RoleMetaService`, not through the entity store cache. Cross-node entity cache invalidation is asynchronous, so a cached entity could be older than the version just read and would otherwise be cached under it. The content may be newer than the version; the next request then sees a newer version and reloads, which is the safe direction. |

### User process

Nothing changes for users or administrators:

1. An administrator creates roles, grants privileges, and grants roles to users and groups through the existing REST API.
2. A user sends a request; groups, if any, come from the authentication token as before.
3. The server allows or denies the request with the same rules as today. A role or grant change takes effect for every request that starts after it commits, on any node. Requests already running follow the rules in [Freshness and consistency](#freshness-and-consistency).

### Implementation process

#### Component flow

```
REST request
   │
   ▼
Authorization expression ──► JcasbinAuthorizer.authorize / deny / hasDenyPolicy
                                 │
                                 ├─ loadRolePrivilege (once per request)
                                 │     ├─ userRoleCache / groupRoleCache   (version-validated)
                                 │     └─ loadRequestPolicies
                                 │           ├─ role_meta.current_version batch read
                                 │           ├─ RoleMetaService reads for stale roles (no entity cache)
                                 │           ├─ resolveRolePolicies (metadataIdCache, no lock)
                                 │           └─ publish to loadedRoles (write lock)
                                 │
                                 ├─ resolve object id (metadataIdCache)
                                 └─ evaluateWithLoadedRolePolicies
                                       └─ RequestRolePolicies.evaluate / hasDeny
```

#### Loading a request view

```
loadRolePrivilege(request):
  g0 = generationCounter.get()                       // read before any role is loaded
  roleIds = userRoles (version-validated) ∪ groupRoles(request's own groups, version-validated)
  context.boundRoleIds = roleIds
  loadRequestPolicies(roleIds, g0):
    [write lock] register a RoleLoadState per role; take cached entries
    batch-read role_meta.current_version for the roles (no lock)
    for each role:
      cached entry is current and complete (or within backoff) -> use it
      otherwise -> read the RoleEntity from the database and compile it (no lock)
                   [write lock] publish unless invalidated after g0 or a newer version won;
                                record a new clear generation for the role
    build RequestRolePolicies
    [read lock] if every chosen entry is still current, mark the view validated
                at the current generation
    [write lock] release the RoleLoadStates
```

Missing roles (no `role_meta` row, or `NoSuchEntityException`) are invalidated and skipped. Roles that cannot be read are recorded as unreadable; the view becomes unreadable and every new role-privilege decision fails closed. Ownership, service-administrator, metalake-membership, and self checks do not read the view, so they are unaffected.

#### Evaluating a single check

```java
boolean evaluate(PolicyKey key, boolean allow) {    // allow = true for the allow authorizer
  if (!readable) return !allow;                      // fail closed for both authorizers
  if (mergedIndex != null) return mergedIndex.get(key) == (allow ? ALLOW : DENY);
  for (role : denyRoles)  if (role.index.get(key) == DENY)  return !allow;
  if (allow) for (role : allowRoles) if (role.index.get(key) == ALLOW) return true;
  return false;
}
```

| Rule            | JCasbin                                           | This design                                                 |
| --------------- | ------------------------------------------------- | ----------------------------------------------------------- |
| Subject match   | `g(userId, p.sub)` over shared rows               | Role is in this request's pinned list                       |
| Object match    | Linear scan, `==` on type, id, act                | Hash lookup on `PolicyKey`                                  |
| DENY precedence | `!some(where p.eft == deny)` in the deny enforcer | Scan `denyRoles` first                                      |
| Active roles    | One `enforce` per active role                     | `allowRoles` holds active roles only; `denyRoles` holds all |
| OWNER           | Owner relation, already outside the enforcer      | Unchanged                                                   |

`hasDenyPolicy` becomes a set intersection, `privileges ∩ deniedPrivileges ≠ ∅`. An unreadable view returns `true`, which disables the shortcut and falls back to per-object checks.

#### Adaptive merge for long lists

Probing each held role is cheap when a user holds a few roles, but listing 10,000 tables for a user who holds 1,000 roles would cost 10 million lookups. The view starts with a probe budget equal to the total number of policies it pins, and each probe consumes the number of role lookups it performed. When the budget is exhausted, the view builds one merged `Map<PolicyKey, Effect>` (active ALLOWs first, then all DENYs overwrite), and later probes are a single lookup.

The merge is built only after the probes have already cost as much as building it, so small requests never pay for it. The microprobe in [#12286](https://github.com/apache/gravitino/issues/12286) with 100,000 policies measured allocation per small request dropping from about 5.3 MB to about 1 KB.

Resource bounds:

| Resource              | Bound                                                                                                                                                                                                                          |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Lock occupancy        | The merge reads only pinned, immutable indexes and runs after the policy lock is released, so it never delays publication or invalidation.                                                                                     |
| Merged index size     | A view that pins more than `RequestRolePolicies.MAX_MERGED_POLICIES` (100,000) policies never merges and keeps probing its roles, at O(k) per probe. At roughly 100 bytes per entry, one merged index stays below about 10 MB. |
| Concurrent requests   | Each long request may hold one merged index, so the peak is the number of concurrent long list requests times that bound. A merged index lives only as long as its request.                                                    |
| Retained old versions | A request keeps the role entries it pinned even after the cache evicts or replaces them, until the request ends. Older versions are therefore bounded by request lifetime, not by the role cache size.                         |
| Observability         | Building a merged index logs its size, role count, and duration at DEBUG level on `RequestRolePolicies`.                                                                                                                       |

#### Staleness check before each decision

```
evaluateWithLoadedRolePolicies(check):
  repeat up to MAX_ROLE_POLICY_RELOADS (3) times:
    [read lock]
      if view.generation == currentGeneration -> current = view       // O(1) fast path
      else if no bound role has clearGeneration > view.generation -> advance view; current = view
    [unlock]
    if current != null -> return check(current)                        // outside the lock
    reload the request's roles (synchronized on the request)
  fail closed
```

An uncached probe never uses a role version that was replaced or invalidated on this node before the probe validated its view. Decisions already returned are not revoked, which is the same guarantee as today. [Freshness and consistency](#freshness-and-consistency) describes how cached decisions and complete expressions behave.

#### Freshness and consistency

A request computes its role membership once, caches decisions, and evaluates expressions that combine several probes. Each scope has its own guarantee:

| Scope                    | Guarantee                                                                                                                                                                                                                                                                                                                                                                   |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Role membership          | Computed once per request from the request's groups and the version-validated user and group bindings. A binding granted or revoked during the request takes effect from the next request.                                                                                                                                                                                  |
| Uncached probe           | Uses the newest role version published or invalidated on this node before the probe; see the staleness check above.                                                                                                                                                                                                                                                         |
| Cached decision          | Reused within the request until its view is replaced. Replacing the view drops every cached decision.                                                                                                                                                                                                                                                                       |
| Authorization expression | All probes and cached decisions of one evaluation come from one view. If the view is replaced during evaluation, the expression is evaluated again, up to three times, and then fails closed.                                                                                                                                                                               |
| List parent shortcut     | The parent-scope grant and `hasDenyPolicy` come from one view. If the view is replaced in between, the shortcut is skipped and every object is checked.                                                                                                                                                                                                                     |
| Listed objects           | Each object's expression is its own unit. Objects of one list may be decided under different role versions if a role changes during the list.                                                                                                                                                                                                                               |
| Other nodes              | There is no broadcast of role policy changes. A request that starts after a role change has committed reads `role_meta.current_version` while loading its roles, so it uses the new version on every node. In-flight requests on other nodes keep their view until they end. Name-to-id and owner caches are invalidated by the change-log poller within its poll interval. |

The expression guarantee uses a sequence counter in the request context:

```
setRolePolicyView(view):                      // replacing an earlier view
  epoch++                                     // odd: replacement in progress
  rolePolicyView = view
  allow and deny decision caches = new maps   // replaced, never cleared in place
  epoch++                                     // even again

evaluate(expression):
  repeat up to MAX_EVALUATION_ATTEMPTS (3) times:
    before = epoch
    result = evaluate every probe of the expression
    if before is even and epoch == before -> return result
  return false                                // fail closed
```

Any probe that observes the new view or the new caches does so after the first increment, so the evaluation that contains it sees a changed epoch. The caches are replaced rather than cleared, so a probe still running against the old view can only write into a cache that is no longer used. Every caller of the evaluator treats `false` as "deny" or "hide", so failing closed never grants access.

Known limitation: user and group role bindings are still validated by `user_meta.updated_at` and `group_meta.updated_at`, which can repeat like `role_meta.updated_at`. Both tables already advance `current_version` with a compare-and-set on every binding change, so the same fix applies; it is listed as a follow-up.

#### Example

| Object              | Value                                                                             |
| ------------------- | --------------------------------------------------------------------------------- |
| User                | `alice` (id 100), no direct roles                                                 |
| Group `analysts`    | Granted role `read_sales` (id 7): ALLOW `SELECT_TABLE` on schema `sales` (id 20)  |
| Group `contractors` | Granted role `no_pii` (id 9): DENY `SELECT_TABLE` on table `sales.orders` (id 42) |

Compiled entries in `loadedRoles`:

```
7 -> { (SCHEMA, 20, SELECT_TABLE) -> ALLOW },   deniedPrivileges = {}
9 -> { (TABLE, 42, SELECT_TABLE) -> DENY },     deniedPrivileges = {SELECT_TABLE}
```

Request A, token `groups=[analysts, contractors]`, reads `sales.orders`:

1. Role ids are {7, 9}. View: `allowRoles=[7, 9]`, `denyRoles=[9]`, `deniedPrivileges={SELECT_TABLE}`.
2. At the table scope, `(TABLE, 42, SELECT_TABLE)` is DENY in role 9, so the request is rejected.

Request B, an older token with `groups=[analysts]`, runs at the same time:

1. Role ids are {7}. View: `allowRoles=[7]`, `denyRoles=[]`.
2. At the schema scope, `(SCHEMA, 20, SELECT_TABLE)` is ALLOW in role 7, so the request is granted.

Neither request writes anything the other reads, so the [#13748](https://github.com/apache/gravitino/issues/13748) interleaving cannot happen. If an administrator then revokes the ALLOW from `read_sales`, the hook invalidates role 7 and records a new clear generation. Request B's next check sees that role 7 changed after its view was pinned, reloads, and is denied.

#### Complexity

Notation: `N` = policies loaded on the node, `k` = roles held by the caller, `a` = active roles, `L` = objects in a list, `d` = scopes checked per object.

| Operation                | JCasbin                                  | This design                                |
| ------------------------ | ---------------------------------------- | ------------------------------------------ |
| One probe                | O(N) matcher evaluations                 | O(k) hash lookups; O(1) after merge        |
| Narrowed probe           | O(a · N)                                 | O(k)                                       |
| List of `L` objects      | O(L · d · N)                             | O(min(L · d · k, held policies + L · d))   |
| `hasDenyPolicy`          | O(policies of held roles) on string rows | O(\|privileges\|) set lookups              |
| Bind or prune membership | Write lock per request with role changes | None                                       |
| Reload a role            | Remove and add rows under the write lock | Replace one reference under the write lock |

### Backward compatibility

- No public API, REST, configuration-key, or default-authorizer change. The class stays `JcasbinAuthorizer`, and the existing `gravitino.authorization.jcasbin.*` keys (`roleCacheSize`, `ownerCacheSize`, `metadataIdCacheSize`, `cacheExpirationSecs`, `changePollIntervalSecs`) keep their names and meaning; `roleCacheSize` now bounds the number of cached per-role indexes.
- No storage or schema change; policies are still derived from role entities on demand. The authorization queries additionally read the existing `role_meta.current_version` column.
- Removed: `GravitinoAdapter`, `jcasbin_model.conf`, `TestJcasbinModel`, `jcasbin_policy.txt`, and the JCasbin dependency from `server-common` and Iceberg REST. `LICENSE.bin` and `LICENSE.iceberg` drop the JCasbin and Aviator entries. Commons Codec, previously transitive through JCasbin, is declared explicitly.
- Log targets in `conf/log4j2.properties.template` and the Helm chart change from the JCasbin package to the authorizer package. Operators who configured a custom logger for `org.casbin` should switch to the authorizer package.
- Relationship with [#13747](https://github.com/apache/gravitino/issues/13747): both make role membership request-local and carry the same three interleaving tests (`testInterleavedGroupRequestsMustNot*`). If [#13747](https://github.com/apache/gravitino/issues/13747) merges first, the implementation also deletes its `jcasbin_request_model.conf` and `RequestRoleFunction`.

### Verification and Phase 1 safety gate

These tests are committed in [#12286](https://github.com/apache/gravitino/issues/12286) and must pass before the evaluator is replaced:

- **Decision equivalence**: `TestPolicyIndexDifferential` compares 135,000 decisions (ALLOW, DENY, and the list deny check) with the JCasbin model, a test-only dependency, over 300 seeded scenarios. They include inactive and unheld roles and cover both per-role probes and merged indexes.
- **Publication and invalidation on real threads**: `testRoleUpdatesOnWorkerThreadsReachLaterRequests` updates and invalidates a role on a worker thread while another worker evicts the role cache. Every request that starts after an update must observe it, over 20 alternating ALLOW and DENY rounds.
- **Complete-expression interleavings**: `testExpressionDoesNotMixRolePolicyVersions` inserts a role update between the ALLOW and DENY probes of `ANY_USE_CATALOG`. `testListShortcutDoesNotMixRolePolicyVersions` inserts one between the list parent grant and its deny check.
- **Version contract**: `testNewRoleVersionWithSameTimestampReloadsPolicies` and `testDefaultRoleReaderBypassesEntityStore`, plus `current_version` assertions in `TestRoleMetaService` and `TestUserMetaService` on H2, MySQL, and PostgreSQL.
- **Resource bounds**: `testPolicyEvaluationRunsOutsidePolicyLock` and `testMergeLimitKeepsProbingRoles`.
- **Regressions**: `TestJcasbinAuthorizer` and `TestRequestRolePolicies` cover:
  - direct and group roles, and group revocation at the IdP;
  - the [#13748](https://github.com/apache/gravitino/issues/13748) interleavings (`testInterleavedGroupRequestsMustNot*`), including list filtering;
  - role changes between and within requests, and a DENY evicted or changed mid-request;
  - global DENY precedence and active-role narrowing;
  - unreadable roles, incomplete loads, and the retry backoff;
  - large roles without a scan, and the adaptive merge.
- **Mutation checks**: removing the expression retry, the shortcut epoch check, DENY precedence, or the version comparison, or evaluating under the lock, makes the corresponding test fail.
- **Performance**: `TestListAuthorizationPerformance` (opt-in) covers large-role, many-role, and long-list scenarios. It guards complexity, not absolute latency.

Deferred to Phase 2, and not required before replacement: storage-backed end-to-end authorization tests and multi-node scenarios. The remaining known limitation is the timestamp-based validation of user and group bindings described above.

### Risks

| Risk                                     | Mitigation                                                                                                                 |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| Decisions drift from the JCasbin model   | Differential comparison; a regression test for each rule in [Evaluating a single check](#evaluating-a-single-check)        |
| Stale ALLOW after a role change          | Generations are checked before every new uncached decision; delayed loaders cannot publish over newer or invalidated state |
| One expression mixes two role versions   | View replacement advances a seqlock epoch and drops cached decisions; the expression is evaluated again or fails closed    |
| A remote update keeps the same timestamp | Role versions use `role_meta.current_version`, and roles are read from the database rather than the entity cache           |
| Lock held while merging a large index    | Evaluation and merging run after the policy lock is released                                                               |
| Memory of merged indexes for long lists  | Built only after probes have paid its cost, capped at 100,000 policies, and lives only for the request                     |
| Growth of `roleClearGenerations`         | Bounded by the role cache size; pruning raises a watermark that forces a conservative reload                               |

---

## Task Breakdown

### Phase 1: Replace the enforcer

- [ ] Add this design document ([#13789](https://github.com/apache/gravitino/issues/13789)).
- [ ] Replace JCasbin evaluation with per-role `PolicyKey -> Effect` indexes and request-local `RequestRolePolicies` views, with generation-based publication and invalidation, expression-level consistency, `current_version` role versions, and the resource bounds; remove the enforcer, the model, the adapter, and the runtime JCasbin dependency; add the Phase 1 safety gate tests ([#12173](https://github.com/apache/gravitino/issues/12173), [#12282](https://github.com/apache/gravitino/issues/12282), PR [#12286](https://github.com/apache/gravitino/issues/12286)).

### Phase 2: Follow-ups

- [ ] Add storage-backed authorization integration tests that cover direct roles, group roles, DENY precedence, and active roles end to end.
- [ ] Add multi-node tests showing that role and binding changes made on one node reach new requests on another node through the version checks.
- [ ] Validate user and group role bindings by `user_meta.current_version` and `group_meta.current_version` instead of `updated_at`.
- [ ] Decide whether to rename `JcasbinAuthorizer`, its package, and the `gravitino.authorization.jcasbin.*` keys, keeping the old names as deprecated aliases.
