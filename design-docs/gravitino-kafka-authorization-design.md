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

# Design of Native Kafka Authorization Push-Down in Gravitino

| Field  | Value |
| ------ | ----- |
| Status | Draft |
| Author | @brucearctor |
| Issue  | [#14968](https://github.com/apache/gravitino/issues/14968) |
| Scope  | `authorizations/authorization-kafka`, `authorizations/authorization-chain`, `core` |

> **Note:** This document specifies the target design and normative authorizer behavior for [#14968](https://github.com/apache/gravitino/issues/14968). Runtime changes are delivered separately via the implementation tasks in [Task Breakdown](#task-breakdown).

---

## Background

Apache Gravitino manages metadata for messaging catalogs (`catalog-kafka`) and defines Kafka topic privileges (`CREATE_TOPIC`, `PRODUCE_TOPIC`, `CONSUME_TOPIC`) alongside hierarchical metadata ownership (`METALAKE`, `CATALOG`, `SCHEMA`, `TOPIC`). However, unlike metadata requests routed through the Gravitino server, data-plane Kafka producers and consumers connect directly to Kafka brokers over the Kafka wire protocol.

Currently, Gravitino has **no authorization push-down provider for Kafka catalogs**:

1. **No Kafka Push-Down Provider**: `RangerAuthorization` (`authorization-ranger`) only supports `HadoopSQL` (Hive, Iceberg, Paimon) and `HDFS` (Fileset) service types.
2. **Chaining Does Not Support `kafka`**: `ChainedAuthorization` (`authorization-chain`) only accepts `catalogProvider = "hive"` and rejects `"kafka"`.
3. **Modern KRaft Brokers Use Native `StandardAuthorizer`**: Apache Kafka 3.x/4.x KRaft deployments (`org.apache.kafka.metadata.authorizer.StandardAuthorizer`) and Kafka-compatible streaming engines enforce native Kafka `AclBinding` entries managed via `AdminClient` without requiring Apache Ranger.
4. **Core Lifecycle Gaps for Direct ACL Materialization**:
   - `UserGroupManager.removeUser` and `UserGroupManager.removeGroup` delete entities from `EntityStore` without notifying catalog authorization plugins, leaving principal ACLs orphaned on external brokers.
   - `CatalogHookDispatcher.dropCatalog` invokes `AuthorizationUtils.removeCatalogPrivileges(...)` *before* `CatalogManager.dropCatalog` validates drop preconditions (`metalake-in-use`, `in-use` when `force=false`, and schema emptiness when `force=false`), risking premature deletion of live external privileges on rejected drops.
   - `CatalogManager.alterCatalog` commits updated properties to `EntityStore` before rebuilding `AuthorizationPlugin` (while `BaseCatalog.initAuthorizationPluginInstance` wraps `IllegalArgumentException` in `RuntimeException`), so invalid `authorization.*` properties on alter can leave a catalog unloadable unless validated pre-commit.

```
Gravitino REST / Client APIs
  ├── Grant/Revoke Roles (CREATE_TOPIC, PRODUCE_TOPIC, CONSUME_TOPIC)
  └── Set Owner (Metalake / Catalog / Schema / Topic)
         │
         ▼
   EntityStore + GravitinoAuthorizer   (Control-plane enforcement only)
         ✗  No push-down to Kafka
         ▼
   Kafka Broker StandardAuthorizer     (Enforces native AclBindings on data plane; out of sync)
         ▲
         │
Kafka Producers / Consumers
```

Because grants and ownership changes in Gravitino are not mirrored to Kafka ACLs, administrators must either duplicate ACL management manually outside Gravitino (causing policy drift and stale access) or deploy Apache Ranger solely for Kafka.

---

## Goals

1. **Native Kafka ACL Push-Down**: Automatically synchronize Gravitino role grants, revocations, role updates/deletions, metalake user/group removals, ownership transfers, and topic/catalog lifecycle events on `kafka` catalogs into native Kafka `AclBinding`s via `AdminClient`.
2. **Full `ALLOW` and `DENY` Semantics Without `CREATE` Bypass**: Enforce both `ALLOW` and `DENY` conditions for `PRODUCE_TOPIC`, `CONSUME_TOPIC`, and `CREATE_TOPIC` across `METALAKE`, `CATALOG`, `SCHEMA`, and `TOPIC` scopes while ensuring `ALLOW CREATE_TOPIC` and parent ownership never bypass topic-level `DENY` rules.
3. **Reference-Preserving Reconciliation**: Retain shared Kafka ACLs (`TOPIC:DESCRIBE`, `GROUP:READ`, `CLUSTER:IDEMPOTENT_WRITE`, and overlapping topic operations) whenever a principal still holds another active Gravitino role or ownership grant requiring them, with create-before-delete updates to avoid transient authorization dips.
4. **Non-Destructive Coexistence with Unmanaged External ACLs**: Preserve broker ACLs owned by unmanaged principals (broker superusers, Schema Registry, external service accounts) or unmanaged resource/operation/host tuples (`CLUSTER_ACTION`, `TRANSACTIONAL_ID`, custom hosts); managed `(principal, host, resource, operation)` bindings for Gravitino-registered principals are owned exclusively by Gravitino because Kafka `AclBinding` entries carry no provenance metadata.
5. **Explicit Opt-In Group ACL Semantics**: Default `authorization.kafka.group-acl.enabled` to `false` with operator warnings because stock `StandardAuthorizer` does not expand group membership, while supporting `Group:<name>` push-down when enabled for group-aware broker `Authorizer` or `KafkaPrincipalBuilder` deployments.
6. **Chaining and Client Config Hardening**: Enable `"kafka"` in `authorization-chain` with leak-safe `IsolatedClassLoader` management, and reject `config.providers` properties in `AdminClient` configs to prevent reading Gravitino server environment variables or files.

---

## Non-Goals

1. **Broker-Side User-to-Group Membership Expansion**: Gravitino does not store user-to-group membership relations (which belong to external identity providers), and stock `StandardAuthorizer` evaluates only the authenticated principal and `User:*`. Materializing group grants onto per-member `User:<name>` ACLs is out of scope.
2. **Authentication, Credential Provisioning, and Custom Identity Mapping**: `onUserAdded` and `onGroupAdded` will not provision SASL/SCRAM credentials, mTLS certificates, or OAuth principals on Kafka brokers; broker authentication remains managed by the cluster's IdP/SASL layer. V1 formats principals directly as `<principal-type>:<gravitino-name>`; custom name translation via `AuthorizationUserGroupMappingProvider` (e.g., mapping usernames to X.509 Distinguished Names) can be layered onto principal formatting in a follow-up.
3. **Transactional Producer (`TRANSACTIONAL_ID`) ACLs**: Gravitino's privilege model has no `transactional.id` securable object. Idempotent producers (`enable.idempotence=true`) are covered via optional `CLUSTER:IDEMPOTENT_WRITE`, whereas transactional producers using `transactional.id` remain managed outside Gravitino.
4. **Per-Topic Consumer Group Entities**: Gravitino `CONSUME_TOPIC` does not parameterize consumer group IDs per topic; consumer group `READ` ACLs use catalog-level pattern configuration (`LITERAL` or `PREFIXED`) rather than introducing a new metadata object type.
5. **Periodic Background Broker Reconciliation or Brownfield ACL Import**: Consistent with Gravitino's `AuthorizationPlugin` SPI, ACL push-down is event-driven on Gravitino write callbacks rather than polling Kafka brokers on a background timer or importing pre-existing broker ACLs into Gravitino roles.
6. **Multi-Schema Topic Prefix Isolation**: Because `catalog-kafka` exposes only the `default` schema covering all topics in the cluster, `METALAKE`, `CATALOG`, and `SCHEMA` grants all map to `TOPIC:*` (`LITERAL`).
7. **Dotted Topic Names in RBAC Securable Paths**: Gravitino's core `MetadataObjects.parse` / `SecurableObjects.parse` splits `fullName` on `.` into `[catalog, schema, topic]`; supporting literal `.` inside topic names in Gravitino's core identifier parser is outside this plugin's scope.

---

## Solution Investigations

### 1. Enforcement Architecture

| Option | Description | Pros | Cons | Decision |
| ------ | ----------- | ---- | ---- | -------- |
| **Option A: Custom Broker `Authorizer` Plugin** | Deploy a Gravitino-specific JAR implementing Kafka `Authorizer` inside every broker to query Gravitino at runtime. | No ACL push-down lag; could evaluate Gravitino groups directly on the broker. | Requires deploying custom plugin JARs on all Kafka brokers, introduces broker runtime dependency/availability coupling on Gravitino, and fails for managed Kafka services without custom plugin hooks. | Rejected |
| **Option B: Apache Ranger Kafka Plugin** | Extend `authorization-ranger` to manage a Ranger Kafka service definition and require Ranger Kafka plugin on brokers. | Reuses `authorization-ranger` patterns and Ranger policy/role separation. | Forces operators to deploy and operate Apache Ranger even when running native KRaft `StandardAuthorizer` or managed Kafka engines. | Rejected |
| **Option C: Native `AdminClient` ACL Push-Down** | Implement `AuthorizationPlugin` in `authorization-kafka` pushing standard Kafka `AclBinding`s via Kafka `AdminClient`. | Zero broker-side JARs; works out-of-the-box with Kafka `StandardAuthorizer` and compatible engines; supports Ranger-free deployments and chaining. | Broker `StandardAuthorizer` lacks group expansion (addressed via opt-in `group-acl.enabled`) and shared principals require reference-preserving reconciliation. | **Chosen** |

### 2. ACL Revocation and Reconciliation Strategy

| Option | Description | Pros | Cons | Decision |
| ------ | ----------- | ---- | ---- | -------- |
| **Option A: Direct Diff Deletion** | Compute ACLs for the revoked privilege/role and immediately call `deleteAcls`. | Simplest implementation; zero `EntityStore` lookups during revocation. | Deletes shared ACLs (`TOPIC:DESCRIBE`, `GROUP:READ`, `CLUSTER:IDEMPOTENT_WRITE`, or overlapping `READ`/`WRITE`) still needed by other roles or ownership held by the same principal. | Rejected |
| **Option B: Full Principal Wipe-and-Recreate** | Delete all ACLs matching `User:<name>` and recreate ACLs from active Gravitino state. | Guarantees no stale plugin ACLs remain for that principal. | Causes transient `TopicAuthorizationException` windows for live clients and destroys external ACLs (`CLUSTER_ACTION`, `TRANSACTIONAL_ID`, ACLs from other metalakes) on the same principal. | Rejected |
| **Option C: Touched-Object Delta + Active-Grant Set Difference** | Compute candidate deletions from touched objects, serialize per-plugin under `aclSyncLock`, apply create-before-delete, and delete only `candidateRemovals \ activeBindings` unioned over active roles and ownership. | Zero authorization dip, preserves overlapping role/owner ACLs, and preserves ACLs outside the plugin's managed `(principal, host, resource, operation)` boundary (unmanaged principals, custom hosts, `TRANSACTIONAL_ID`, `CLUSTER_ACTION`). | Requires querying active roles and ownership from `EntityStore` on revocation/update/removal. | **Chosen** |

### 3. `CREATE_TOPIC` Mapping (`CLUSTER:CREATE` vs `TOPIC:*:CREATE`)

Apache Kafka authorizes `CreateTopics` by checking `CLUSTER:kafka-cluster` (`CREATE`) before evaluating `TOPIC` ACLs and **short-circuiting to `ALLOWED`** if a cluster-level `ALLOW` rule matches.

| Option | Description | Pros | Cons | Decision |
| ------ | ----------- | ---- | ---- | -------- |
| **Option A: `CLUSTER:kafka-cluster` (`CREATE`)** | Emit `CLUSTER:kafka-cluster` `CREATE` on `ALLOW CREATE_TOPIC` and catalog `Owner` grants. | Matches older Kafka tutorials that use cluster-level `CREATE`. | Short-circuits Kafka authorization before `TOPIC` ACL checks, allowing `ALLOW CREATE_TOPIC` or catalog ownership to bypass topic-level or catalog-level `DENY CREATE_TOPIC` rules. | Rejected |
| **Option B: `TOPIC:*` (`PatternType.LITERAL`, `CREATE`)** | Emit `TOPIC:*` (`CREATE`, plus `DESCRIBE` on `ALLOW`) and omit `CLUSTER:CREATE` from both `CREATE_TOPIC` and `Owner`. | Evaluated at the `TOPIC` resource layer where Kafka `StandardAuthorizer` consistently enforces `DENY` before `ALLOW` without bypassing `DENY` grants. | Relies on `TOPIC:*` `CREATE` support in Kafka 3.x+/KRaft (matches Gravitino's target `StandardAuthorizer` deployment). | **Chosen** |

### 4. Metalake Identity Removal Hook (`onUserRemoved` vs `onMetalakeUserRemoved`)

Removing a user or group from a metalake in `UserGroupManager` does not currently invoke any authorization plugin hook, leaving Kafka ACLs orphaned after identity deletion.

| Option | Description | Pros | Cons | Decision |
| ------ | ----------- | ---- | ---- | -------- |
| **Option A: Reuse `onUserRemoved` / `onGroupRemoved` in `UserGroupManager`** | Invoke existing `onUserRemoved` / `onGroupRemoved` hooks when deleting a user or group from a metalake. | No additions to the `AuthorizationPlugin` interface. | `RangerAuthorizationPlugin.onUserRemoved` (`rangerClient.deleteUser`) and `JdbcAuthorizationPlugin.onUserRemoved` (`DROP USER`) delete global external accounts that must survive removal from a single metalake. | Rejected |
| **Option B: Synthesize `onRevokedRolesFromUser` + `onOwnerSet(..., null)`** | Enumerate the principal's roles and owned objects across the metalake and emit role-revoke and null-owner callbacks before deletion. | Avoids adding new `AuthorizationPlugin` methods. | `JdbcAuthorizationPlugin.onOwnerSet` dereferences `newOwner.type()` without a null check (throwing `NullPointerException` when `newOwner == null`), and scanning every owned entity across all catalogs in a metalake lacks a reverse `OWNER_REL` index in `SupportsRelationOperations`. | Rejected |
| **Option C: Add Default `onMetalakeUserRemoved` / `onMetalakeGroupRemoved` Hooks** | Add `default` no-op methods returning `Boolean.TRUE` on `AuthorizationPlugin`, invoke them across active (`in-use=true` and `metalake-in-use=true`) catalogs before `EntityStore` deletion, and delegate in `KafkaAuthorizationPlugin`. | 100% source/binary backward compatible; zero unintended side effects on `RangerAuthorizationPlugin` or `JdbcAuthorizationPlugin`; enables clean pre-delete Kafka ACL lookup and revocation across single and bulk removals. | Adds two default methods to `AuthorizationPlugin` and explicit forwarding in `ChainedAuthorizationPlugin`. | **Chosen** |

---

## Proposal

### SPI and Core Lifecycle Changes (Old vs. New Behavior)

#### 1. `AuthorizationPlugin` (`core`)

**Before:**

```java
public interface AuthorizationPlugin
    extends UserGroupAuthorizationPlugin,
        RoleAuthorizationPlugin,
        MetadataAuthorizationPlugin,
        Closeable {}
```

**After:**

```java
public interface AuthorizationPlugin
    extends UserGroupAuthorizationPlugin,
        RoleAuthorizationPlugin,
        MetadataAuthorizationPlugin,
        Closeable {

  default Boolean onMetalakeUserRemoved(User user) throws AuthorizationPluginException {
    return Boolean.TRUE;
  }

  default Boolean onMetalakeGroupRemoved(Group group) throws AuthorizationPluginException {
    return Boolean.TRUE;
  }
}
```

Existing `AuthorizationPlugin` implementations remain binary- and source-compatible with no behavior change unless overridden.

#### 2. `UserGroupManager` & `AuthorizationUtils` (`core`)

- **Old behavior**: `UserGroupManager.removeUser(metalake, user)` and `UserGroupManager.removeGroup(metalake, group)` called `store.delete(...)` directly without notifying any catalog authorization plugin. Neither single removal nor bulk `AccessControlManager.removeUsers` / `removeGroups` triggered plugin cleanup.
- **New behavior**: `UserGroupManager.removeUser` and `removeGroup` load the `UserEntity` / `GroupEntity` first; if found, they invoke `AuthorizationUtils.callAuthorizationPluginForMetalake(metalake, ...)` (`onMetalakeUserRemoved` / `onMetalakeGroupRemoved`) *before* `store.delete(...)`. `callAuthorizationPluginForMetalake` inspects both `in-use` and `metalake-in-use` properties in `EntityStore` and skips disabled catalogs so an offline disabled cluster cannot block metalake identity deletion.

#### 3. Catalog Drop Precondition Ordering & Pre-Commit Authz Config Validation (`core`)

- **`dropCatalog` Old behavior**: `CatalogHookDispatcher.dropCatalog` called `AuthorizationUtils.removeCatalogPrivileges(ident, locations)` unconditionally before `CatalogManager.dropCatalog(ident, force)` checked `metalake-in-use`, catalog `in-use` (`!force`), or user-created schema emptiness (`!force`), wiping live external privileges right before throwing `MetalakeNotInUseException`, `CatalogInUseException`, or `NonEmptyCatalogException`.
- **`dropCatalog` New behavior**: External catalog privilege removal executes only *after* `CatalogManager.dropCatalog` verifies all drop preconditions (`checkMetalakeInUse`, `!catalogInUse || force`, and `!force` schema-emptiness checks) under the catalog `WRITE` lock while the catalog wrapper is still open prior to `store.delete(...)`. Rejected drops never touch broker or Ranger privileges.
- **`createCatalog` / `alterCatalog` Pre-Commit Validation**: `BaseCatalog.initAuthorizationPluginInstance` preserves `IllegalArgumentException` (`400 Bad Request`) rather than wrapping it in `RuntimeException`, and authorization plugin configuration (`KafkaAuthorizationProperties.validate()`, including `config.providers` blocking) is validated *before* `CatalogManager.alterCatalogUnderLock` commits `store.update(...)` so invalid `authorization.*` edits cannot persist and leave a catalog unloadable in `EntityStore`.

#### 4. `ChainedAuthorization` & `ChainedAuthorizationPlugin` (`authorization-chain`)

**Before (`ChainedAuthorization.newPlugin`):**

```java
switch (catalogProvider) {
  case "hive":
    return new ChainedAuthorizationPlugin(metalake, catalogProvider, config);
  default:
    throw new IllegalArgumentException("Unknown catalog provider: " + catalogProvider);
}
```

**After (`ChainedAuthorization.newPlugin`):**

```java
switch (catalogProvider) {
  case "hive":
  case "kafka":
    return new ChainedAuthorizationPlugin(metalake, catalogProvider, config);
  default:
    throw new IllegalArgumentException("Unknown catalog provider: " + catalogProvider);
}
```

In addition, `ChainedAuthorizationPlugin` previously did not close `IsolatedClassLoader` instances on `close()` or partial initialization failure and did not forward metalake removal hooks. Under this design, it will track created `IsolatedClassLoader`s, close already-initialized plugins and loaders on failure or `close()`, and forward `onMetalakeUserRemoved` / `onMetalakeGroupRemoved`.

---

### Privilege, Scope, and Ownership Mapping

#### Why Per-Principal Flattening Instead of Managed Roles (Contrast with Ranger)

Unlike `RangerAuthorizationPlugin` (`authorization-ranger`), which provisions managed Ranger Role objects (`gravitino_<metalake>_<role>`) and delegates role-to-principal expansion to Ranger, Apache Kafka's native `StandardAuthorizer` has no Role entity and evaluates only flat `(Principal, ResourcePattern, Operation, PermissionType)` `AclBinding` tuples. Consequently, `KafkaAuthorizationPlugin` treats role, user, and group creation as broker no-ops, materializes ACLs directly per bound `User:<name>` (or opt-in `Group:<name>`) principal when roles or ownership are assigned, and reconciles revocations against remaining active grants and ownership in `EntityStore`.

#### Scope Resolution

| Gravitino `MetadataObject.Type` | Kafka `ResourcePattern`                 | Notes |
| ------------------------------- | --------------------------------------- | ----- |
| `METALAKE`, `CATALOG`, `SCHEMA` | `TOPIC:*` (`PatternType.LITERAL`)       | Catalog-wide wildcard (single `default` schema covers all cluster topics) |
| `TOPIC`                         | `TOPIC:<topic_name>` (`PatternType.LITERAL`) | Topic-specific literal ACL |
| Any other type (`TABLE`, `FILESET`, etc.) | Ignored (empty ACL set)       | Non-Kafka securable objects are no-ops |

#### Privilege and Ownership Mapping

| Gravitino Privilege / Role                 | Scope (`Metalake`/`Catalog`/`Schema` vs `Topic`) | Kafka `AclBinding`s (`ALLOW`) | Kafka `AclBinding`s (`DENY`) |
| ------------------------------------------ | ------------------------------------------------ | ----------------------------- | ---------------------------- |
| `PRODUCE_TOPIC`                            | `TOPIC:*` or `TOPIC:<name>`                      | `TOPIC` (`WRITE`, `DESCRIBE`), plus `CLUSTER:kafka-cluster` (`IDEMPOTENT_WRITE`) if enabled | `TOPIC` (`WRITE`) |
| `CONSUME_TOPIC`                            | `TOPIC:*` or `TOPIC:<name>`                      | `TOPIC` (`READ`, `DESCRIBE`), plus configured `GROUP` (`READ`) | `TOPIC` (`READ`) |
| `CREATE_TOPIC`                             | `TOPIC:*` (cluster-wide topic creation)          | `TOPIC:*` (`CREATE`, `DESCRIBE`) | `TOPIC:*` (`CREATE`) |
| `Owner` (`USER`, or `GROUP` when enabled)  | `TOPIC:*` or `TOPIC:<name>`                      | `TOPIC` (`READ`, `WRITE`, `CREATE`, `DELETE`, `ALTER`, `DESCRIBE`, `ALTER_CONFIGS`, `DESCRIBE_CONFIGS`), `GROUP` (`READ`), `CLUSTER:kafka-cluster` (`IDEMPOTENT_WRITE` if enabled) | *(none)* |
| Control-plane-only privileges (`USE_CATALOG`, `USE_SCHEMA`, `MANAGE_GRANTS`, `CREATE_ROLE`, `APPLY_TAG`, etc.) | Any (`METALAKE`, `CATALOG`, `SCHEMA`, `TOPIC`) | *(none — enforced by `GravitinoAuthorizer` only)* | *(none — enforced by `GravitinoAuthorizer` only)* |

#### Design Rules for Safe Enforcement

1. **Asymmetric `DENY` Emission**: `DENY PRODUCE_TOPIC` emits only `TOPIC:WRITE DENY` (not `DESCRIBE` or `CLUSTER:IDEMPOTENT_WRITE`), and `DENY CONSUME_TOPIC` emits only `TOPIC:READ DENY` (not `GROUP:READ`), so denying produce or consume on one topic never breaks consuming other topics or producing to other topics.
2. **Wildcard Identity (`*`) Rejection**: `KafkaAuthorizationProperties.validate()` rejects `*` for `user-principal-type` and `group-principal-type`, and `formatUserPrincipal` / `formatGroupPrincipal` reject `*` usernames/group names so a single Gravitino identity named `*` can never become Kafka's cluster-wide `User:*` wildcard. Cleanup/update callbacks likewise ignore any pre-existing `*` identity safely.
3. **Kafka `DENY`-First Precedence**: Because Kafka `StandardAuthorizer` evaluates matching `DENY` entries before `ALLOW` entries, a catalog-, schema-, or metalake-scoped `DENY` (`TOPIC:*`) overrides topic-level `ALLOW` grants and topic ownership on the same principal.

#### Effective Precedence Matrix

| Gravitino Grants on Same Principal | Materialized Kafka `AclBinding`s | Gravitino REST Control Plane | Kafka Broker Data Plane (`StandardAuthorizer`) |
| ---------------------------------- | -------------------------------- | ---------------------------- | ---------------------------------------------- |
| Role A `ALLOW PRODUCE_TOPIC` (`topic1`) + Role B `ALLOW CONSUME_TOPIC` (`topic1`) | `TOPIC:topic1` (`WRITE`, `READ`, `DESCRIBE`), `GROUP` (`READ`), `CLUSTER` (`IDEMPOTENT_WRITE`); revoking Role A removes `WRITE` and `IDEMPOTENT_WRITE` while keeping `READ`, `DESCRIBE`, and `GROUP:READ` | Allowed (`alterTopic`, `loadTopic`) | Allowed (`Produce` + `Consume` on `topic1`; `Consume` preserved after revoking Role A) |
| Role A `ALLOW PRODUCE_TOPIC` (`topic1`) + Role B `DENY PRODUCE_TOPIC` (`topic1`) | `TOPIC:topic1` `WRITE ALLOW` + `TOPIC:topic1` `WRITE DENY` (plus `DESCRIBE` / `IDEMPOTENT_WRITE` `ALLOW` from Role A) | Denied (`!ANY_DENY_PRODUCE_TOPIC`) | **Denied** (`Produce` on `topic1`; Kafka evaluates `DENY` before `ALLOW`) |
| Catalog/Schema `ALLOW PRODUCE_TOPIC` (`TOPIC:*`) + Topic `DENY PRODUCE_TOPIC` (`topic1`) | `TOPIC:*` (`WRITE`, `DESCRIBE` `ALLOW`), `CLUSTER` (`IDEMPOTENT_WRITE` `ALLOW`), `TOPIC:topic1` (`WRITE` `DENY` only) | Allowed on `topic2`, Denied on `topic1` | **Allowed** `Produce` on `topic2`, **Denied** `Produce` on `topic1` (asymmetric `DENY` preserves `DESCRIBE` and `IDEMPOTENT_WRITE`) |
| Catalog/Schema `DENY PRODUCE_TOPIC` (`TOPIC:*`) + Topic `ALLOW PRODUCE_TOPIC` (`topic1`) | `TOPIC:*` (`WRITE` `DENY`) + `TOPIC:topic1` (`WRITE`, `DESCRIBE` `ALLOW`) | Denied on all topics in catalog | **Denied** `Produce` on `topic1` (`TOPIC:*` literal wildcard `DENY` overrides topic-specific `ALLOW`) |
| `ALLOW CREATE_TOPIC` (or `CATALOG` `Owner`) + `DENY CREATE_TOPIC` (`METALAKE` / `CATALOG` / `SCHEMA`) | `TOPIC:*` (`CREATE`, `DESCRIBE` `ALLOW`) + `TOPIC:*` (`CREATE` `DENY`); no `CLUSTER:CREATE` emitted | Denied for non-owner (`!ANY_DENY_CREATE_TOPIC`); allowed for `CATALOG`/`METALAKE` `OWNER` | **Denied** `CreateTopics` on broker (`TOPIC:*` `CREATE DENY` is never short-circuited by plugin ACLs; assumes no out-of-band `CLUSTER:CREATE` or `CLUSTER:ALL` `ALLOW` ACL is granted externally to the principal or `User:*`) |
| `Owner` (`CATALOG` or `TOPIC`) + Explicit RBAC `DENY` (`PRODUCE_TOPIC` or `CONSUME_TOPIC`) | `Owner` `ALLOW` set + `TOPIC` `WRITE`/`READ` `DENY` | **Allowed** (`TopicOperations` checks `OWNER \|\| ...` before RBAC `DENY`) | **Denied** on wire for the denied operation (`Owner` materializes as `ALLOW` ACLs, so Kafka `DENY` takes precedence until revoked — matching `RangerAuthorizationPlugin`, which places `Owner` in `policyItems` and `DENY` in `denyPolicyItems` without `denyExceptions`) |

---

### Callback Behavior Specification

Every callback on `AuthorizationPlugin` has explicit push-down or intentional no-op semantics in `KafkaAuthorizationPlugin`:

| Callback | Behavior in `KafkaAuthorizationPlugin` |
| -------- | -------------------------------------- |
| `onRoleCreated(Role)` / `onRoleAcquired(Role)` | Intentional no-op (`Boolean.TRUE`); Kafka has no standalone role entity and materializes ACLs per bound principal. |
| `onRoleUpdated(Role, RoleChange...)` | Computes touched-object delta (`computeRoleChangeBindings`), intersects added bindings with current persisted `EntityStore` role bindings under `aclSyncLock` before calling `createAcls`, and deletes `candidateRemovals \ activeBindings` reconciled against persisted post-update roles and ownership for each bound user (and bound group if `group-acl.enabled=true`; logs warning if `false` and group-bound Kafka grants change). |
| `onRoleDeleted(Role)` | Records `role.id()` in bounded `deletedRoleIds` and removes role bindings not retained by other active roles or ownership for each bound user (and group if enabled). |
| `onGrantedRolesToUser(List<Role>, User)` | Formats `User:<name>` (rejecting `*`), resolves the granted role names against current persisted `EntityStore` roles bound to the user (excluding `deletedRoleIds`) under `aclSyncLock`, and creates mapped `AclBinding`s via `createAcls`. |
| `onRevokedRolesFromUser(List<Role>, User)` | Deletes `candidateRemovals \ activeBindings` excluding revoked role names (preserving peer `Group:<name>` bindings when `user-principal-type == group-principal-type`). |
| `onGrantedRolesToGroup(List<Role>, Group)` | When `group-acl.enabled=false` (default), no broker write (logs warning if roles carry Kafka bindings); when `true`, formats `Group:<name>` (rejecting `*`), resolves persisted bound roles from `EntityStore` (excluding `deletedRoleIds`) under `aclSyncLock`, and creates mapped `AclBinding`s via `createAcls`. |
| `onRevokedRolesFromGroup(List<Role>, Group)` | No-op when `group-acl.enabled=false`; when `true`, deletes `candidateRemovals \ activeBindings` (preserving peer `User:<name>` bindings when prefixes match). |
| `onUserAdded(User)` / `onUserAcquired(User)` / `onGroupAdded(Group)` / `onGroupAcquired(Group)` | Intentional no-op (`Boolean.TRUE`); broker authentication identities and credentials are managed by external SASL/OAuth/mTLS IdPs. |
| `onUserRemoved(User)` / `onMetalakeUserRemoved(User)` | Computes exact Gravitino-managed active bindings for `User:<name>` (subtracting peer `Group:<name>` bindings if prefixes match) and deletes only those exact `AclBinding`s via `deleteExactAcls`. |
| `onGroupRemoved(Group)` / `onMetalakeGroupRemoved(Group)` | No-op when `group-acl.enabled=false`; when `true`, deletes exact Gravitino-managed bindings for `Group:<name>` (subtracting peer `User:<name>` bindings if prefixes match). |
| `onOwnerSet(MetadataObject, Owner, Owner)` | Ignores objects outside this catalog/metalake; creates owner ACLs for `newOwner` first (warning and skipping broker write if `GROUP` with `group-acl.enabled=false`), then removes `preOwner` owner ACLs not retained by other active roles or owned objects. |
| `onMetadataUpdated(RemoveMetadataObject)` | For `TOPIC`: filters `TOPIC:<name>` literal ACLs to plugin-managed bindings (`principalExists` + configured `host` + managed operations), reconciles/deletes orphaned auxiliary `GROUP:READ` and `CLUSTER:IDEMPOTENT_WRITE` ACLs *before* deleting the `TOPIC` ACLs. For `CATALOG`/`METALAKE`: deletes all Gravitino-managed bindings for metalake principals on this catalog. Other types: no-op. |
| `onMetadataUpdated(RenameMetadataObject)` | For `TOPIC`: copies plugin-managed `TOPIC:<old>` ACLs to `TOPIC:<new>` (`createAcls` first), then deletes `TOPIC:<old>` ACLs. For `METALAKE`/`CATALOG`: updates cached `metalake` or `catalogName` in memory. |
| `close()` | Closes `AdminClient` with `Duration.ofMillis(requestTimeoutMs)`. |

#### End-to-End Catalog and Topic Lifecycle Semantics

- **`createCatalog` and `FutureGrantManager` Replay**: `CatalogHookDispatcher.createCatalog` assigns the creator as `CATALOG` `Owner` (`onOwnerSet` pushes `TOPIC:*` owner ACLs) and invokes `FutureGrantManager.grantNewlyCreatedCatalog`, which replays any existing `METALAKE` `Owner` and `METALAKE`-scoped `CREATE_TOPIC`, `PRODUCE_TOPIC`, and `CONSUME_TOPIC` role grants onto the new Kafka catalog. If post-hook ACL synchronization fails, `CatalogHookDispatcher` rolls back the catalog creation.
- **`createTopic` vs. Lazy Import on `loadTopic` / `listTopics`**: Creating a topic via Gravitino (`POST .../topics`) triggers `TopicHookDispatcher.createTopic` → `OwnerManager.setOwner`, pushing `TOPIC:<name>` owner ACLs for the creator. By contrast, pre-existing topics created directly on Kafka and lazily imported into `EntityStore` during `loadTopic` have no initial Gravitino `Owner`; existing broker ACLs on those topics remain untouched until `setOwner` or role grants are applied in Gravitino.
- **`alterTopic` vs. `RenameMetadataObject`**: Gravitino's `TopicChange` API does not currently support renaming topics (`TopicOperationDispatcher.alterTopic`), so comment/property alterations do not rewrite Kafka ACLs. `KafkaAuthorizationPlugin` still implements `RenameMetadataObject` (`createAcls` on `TOPIC:<new>` before `deleteAcls` on `TOPIC:<old>`) for `AuthorizationPlugin` SPI completeness.
- **`disableCatalog`, `enableCatalog`, and `alterCatalog`**: Disabling a catalog (`in-use=false`) closes the catalog plugin and `AdminClient` **without** revoking live broker ACLs (unlike `dropCatalog`). Altering catalog properties (`authorization.kafka.*` or `bootstrap.servers`) rebuilds the plugin instance for subsequent callbacks without retroactively migrating or deleting previously materialized broker ACLs; ACL-shaping settings (`user-principal-type`, `group-principal-type`, `host`, `consumer-group-pattern*`) and target clusters must therefore remain fixed while grants are materialized, or be changed only after revoking active grants (or dropping and recreating the catalog).

---

### Configuration Properties and Security

| Property Name                                      | Description | Default | Required |
| -------------------------------------------------- | ----------- | ------- | -------- |
| `authorization-provider`                           | Set to `kafka` (or `chain` with provider `kafka`). Immutable after catalog creation (`BaseCatalogPropertiesMetadata`). | `(none)` | Yes |
| `authorization.kafka.bootstrap.servers`            | Override Kafka bootstrap servers for `AdminClient` (defaults to catalog `bootstrap.servers`). | Catalog `bootstrap.servers` | Yes (if catalog property absent, e.g., `chain`) |
| `authorization.kafka.admin.*`                      | Forwarded to `AdminClient` (with prefix stripped), overriding `gravitino.bypass.*`. | `(none)` | No |
| `authorization.kafka.user-principal-type`          | Kafka principal type prefix for users (`<type>:<name>`). `*` rejected. | `User` | No |
| `authorization.kafka.group-principal-type`         | Kafka principal type prefix for groups (`<type>:<name>`). `*` rejected. | `Group` | No |
| `authorization.kafka.group-acl.enabled`            | Push `Group:<name>` ACLs to Kafka (enable only for group-aware broker `Authorizer` / `KafkaPrincipalBuilder`). | `false` | No |
| `authorization.kafka.consumer-group-pattern`       | Consumer group pattern granted `READ` on `ALLOW CONSUME_TOPIC` and ownership. | `*` | No |
| `authorization.kafka.consumer-group-pattern-type`  | Consumer group `PatternType` (`LITERAL` or `PREFIXED`). | `LITERAL` | No |
| `authorization.kafka.host`                         | Host written into managed `AccessControlEntry` objects. | `*` | No |
| `authorization.kafka.idempotent-write-acl.enabled` | Include `CLUSTER:kafka-cluster` `IDEMPOTENT_WRITE` on `ALLOW PRODUCE_TOPIC` and ownership. | `true` | No |
| `authorization.kafka.request-timeout-ms`           | Timeout in ms for `AdminClient` requests, futures, and `close()`. | `30000` | No |

#### Precedence, Config Provider Hardening, and Chaining

- **AdminClient Property Precedence**: Base `gravitino.bypass.*` (prefix stripped) → `bootstrap.servers` → `request.timeout.ms` & `default.api.timeout.ms` (explicit `authorization.kafka.request-timeout-ms` overrides bypass; default `30000` applies only if absent in bypass) → `authorization.kafka.admin.*` overrides → default `client.id` (`gravitino-kafka-authz-<catalogId>`).
- **`config.providers` Blocked & Pre-Commit Validation**: `KafkaAuthorizationProperties` rejects `gravitino.bypass.config.providers*` and `authorization.kafka.admin.config.providers*` so Kafka client config interpolation (`${file:...}`, `${env:...}`) cannot read Gravitino server secrets or environment variables. All `authorization.kafka.*` properties are validated before persisting `createCatalog` or `alterCatalog` updates, failing fast with `IllegalArgumentException` (`400 Bad Request`).
- **Chained Usage**: `ChainedAuthorizationProperties` extracts only `authorization.chain.<name>.*` (rewritten to `authorization.*`), so chained configurations must specify `authorization.chain.<name>.kafka.bootstrap.servers` (and any `kafka.admin.*` security settings) explicitly.

---

### User Process

1. **Create a Kafka catalog** (`POST /api/metalakes/{metalake}/catalogs`) with `type = "messaging"`, `provider = "kafka"`, `authorization-provider = "kafka"`, and any SASL/SSL settings under `gravitino.bypass.*` or `authorization.kafka.admin.*`:
   ```properties
   bootstrap.servers=broker1:9093
   authorization-provider=kafka
   authorization.kafka.consumer-group-pattern=analytics-
   authorization.kafka.consumer-group-pattern-type=PREFIXED
   ```
2. **Create users and roles**, via `POST /api/metalakes/{metalake}/users` and `POST /api/metalakes/{metalake}/roles`, granting topic privileges (`CREATE_TOPIC`, `PRODUCE_TOPIC`, `CONSUME_TOPIC`) at catalog, schema (`default`), or topic scope (`PUT /api/metalakes/{metalake}/permissions/roles/{role}/{type}/{fullName}/grant`).
3. **Bind roles to users or assign topic ownership**: Grant roles with `PUT /api/metalakes/{metalake}/permissions/users/{user}/grant` (or create topics via `POST /api/metalakes/{metalake}/catalogs/{catalog}/schemas/default/topics` and assign owners via `PUT /api/metalakes/{metalake}/owners/{metadataObjectType}/{fullName}`). Gravitino immediately pushes the mapped `AclBinding`s for `User:alice` to the Kafka cluster via `AdminClient`.
4. **Clients connect directly to Kafka brokers**: Kafka `StandardAuthorizer` authorizes `alice`'s `Producer` and `Consumer` wire-protocol requests to `bootstrap.servers` against the pushed ACLs with no broker plugin required.
5. **Revoke, update, or delete**: Revoking a role (`PUT /api/metalakes/{metalake}/permissions/users/{user}/revoke`), overriding role privileges (`PUT /api/metalakes/{metalake}/permissions/roles/{role}/`), transferring ownership, removing a user (`DELETE /api/metalakes/{metalake}/users/{user}`), or dropping a topic (`DELETE /api/metalakes/{metalake}/catalogs/{catalog}/schemas/default/topics/{topic}`) reconciles and removes only the ACLs no longer backed by any active Gravitino grant or ownership.

---

### Implementation Process, Concurrency, and Failure Semantics

#### Architecture & Control Flow

```
AccessControlManager / OwnerManager / TopicHookDispatcher / CatalogHookDispatcher
   │
   ▼  (Role / User / Group / Owner / MetadataObject callbacks)
KafkaAuthorizationPlugin  [synchronized on aclSyncLock]
   ├── KafkaPrivilegeMapper                   (Privilege / Owner ──► Set<AclBinding>)
   ├── EntityStoreAuthorizationStateProvider  (TCCL = GravitinoEnv classloader for MyBatis)
   │     ├── listRolesForUser / listRolesForGroup / listUserNamesByRole / listGroupNamesByRole
   │     └── listOwnedObjectsForPrincipal     (500-topic chunked batchListEntitiesByRelation)
   └── AdminClient (kafka-clients, compression codecs excluded)
         │  createAcls / describeAcls / deleteAcls (bounded get(requestTimeoutMs))
         ▼
Kafka Broker (StandardAuthorizer)
```

#### Key Reconciliation Invariants

1. **Per-Plugin Serialization (`aclSyncLock`) & Persisted-State Grant Lookup**: All grant, revoke, role update/delete, user/group removal, ownership change, and metadata rename/remove callbacks synchronize on `aclSyncLock`. Because `PermissionManager` releases its role `READ` lock before binding principals and invoking plugin callbacks, both grant additions (`resolvePersistedGrantedRoles`) and role-update additions (`computeRoleBindingsForPrincipal` retention) read current persisted `EntityStore` roles and principal bindings under `aclSyncLock` (excluding bounded `deletedRoleIds`), preventing stale pre-lock snapshots from recreating concurrently revoked or deleted ACLs.
2. **Create-Before-Delete Updates**: On `onRoleUpdated`, `onOwnerSet`, and topic renames (`MetadataObjectChange.RenameMetadataObject`), new ACLs are created via `adminClient.createAcls(...)` *before* `removeBindingsPreservingActiveGrants` or `deleteExactAcls` removes superseded ACLs, preventing temporary access loss.
3. **`overridePrivilegesInRole` Reconciliation**: `PermissionManager` commits the full post-update role in `EntityStore` before emitting per-object `onRoleUpdated` callbacks (which share the pre-update `Role` snapshot). `computeRoleChangeBindings` derives candidate removals only from touched securable objects in the `RoleChange[]`, and `removeBindingsPreservingActiveGrants` reconciles against the live post-update roles queried from `EntityStoreAuthorizationStateProvider`.
4. **Principal Type Collision Protection (`sameUserAndGroupPrincipalType()`)**: When `user-principal-type` equals `group-principal-type` and group ACLs are enabled, revocations and identity deletions union active bindings across both `USER` and `GROUP` identities of the same name using separate `excludedRoleNames` / `peerExcludedRoleNames`.
5. **Scoped Topic Drop / Rename & Idempotent Retry Order**: On topic removal or rename, `filterPluginManagedTopicBindings` retains only `TOPIC` `LITERAL` ACLs matching the configured `host`, managed operations/permission types (`KafkaPrivilegeMapper.isManagedTopicOperation`), and principals confirmed via `stateProvider.principalExists(...)` (since Kafka `AclBinding` entries carry no provenance metadata, matching tuples for Gravitino-registered principals are treated as exclusively plugin-managed). During topic drop, auxiliary `GROUP:READ` and `CLUSTER:IDEMPOTENT_WRITE` ACLs are reconciled and deleted *before* deleting the topic's own `TOPIC` ACLs so retrying after a transient broker failure remains idempotent.
6. **MyBatis ClassLoader Bridge & Lean Packaging**: `EntityStoreAuthorizationStateProvider` wraps `EntityStore` calls with the thread context classloader set to `GravitinoEnv.class.getClassLoader()` so calls from `IsolatedClassLoader` resolve MyBatis mappers cleanly. `authorization-kafka/build.gradle.kts` will exclude unused compression codecs (`at.yawk.lz4:lz4-java`, `org.lz4:lz4-java`, `zstd-jni`, `snappy-java`) since only control-plane ACL RPCs are used.

#### Failure and Retry Consistency Semantics

Gravitino invokes `AuthorizationPlugin` synchronously outside database transactions:

| Operation / Callback | Execution Order (`EntityStore` vs Broker) | Consistency Guarantee on Broker Failure |
| -------------------- | ----------------------------------------- | --------------------------------------- |
| `deleteRole`, `removeUser` / `removeUsers`, `removeGroup` / `removeGroups`, `dropCatalog` | **Pre-commit** (plugin runs *before* `EntityStore` delete) | Fail-safe: `EntityStore` remains unchanged on broker timeout; retrying the operation completes cleanup cleanly. |
| `grantRolesToUser` / `Group`, `revokeRolesFromUser` / `Group` | **Post-commit, retry-idempotent** (`EntityStore` commits first, plugin still invoked on repeat call) | Eventual convergence: caller retrying the grant/revoke re-drives the exact Kafka ACL sync. |
| `onRoleUpdated` (`grant`/`revoke`/`override`), `onOwnerSet`, `dropTopic` | **Post-commit, edge-triggered** (`EntityStore` commits first; no-op repeat short-circuits or topic is gone) | Requires re-toggling the privilege/owner (or re-creating/dropping) because `AuthorizationPlugin` has no background periodic sync loop. |

---

### Backward Compatibility and Known Operational Trade-Offs

- **Backward Compatibility**: Fully opt-in per catalog (`authorization-provider=kafka` or `chain`). Existing Kafka catalogs without authorization push-down, Hive/Iceberg/Paimon Ranger catalogs, and custom `AuthorizationPlugin` implementations are unaffected.
- **Disabled-Catalog (`in-use=false`) User Removal**: Skipping disabled catalogs when removing a metalake user avoids blocking metalake administration when a Kafka cluster is offline, but if that catalog is later re-enabled without recreating the user, that deleted user's pre-existing broker ACLs must be cleaned manually on the cluster because `principalExists` is now `false`.
- **1:1 Catalog-to-Cluster Topology**: Consistent with [docs/kafka-catalog.md](../docs/kafka-catalog.md), one Gravitino Kafka catalog maps to one physical Kafka cluster. Dropping one catalog removes catalog- and metalake-scoped (`TOPIC:*`, shared `GROUP`/`CLUSTER`) ACLs attributable to that metalake's grants on the configured cluster.
- **Topic Ownership Query Scaling**: Because `SupportsRelationOperations` indexes `OWNER_REL` from entity to owner rather than owner to entities, `EntityStoreAuthorizationStateProvider` queries topic owners in chunks of `500` via `batchListEntitiesByRelation`.

---

## Task Breakdown

- [ ] Add `onMetalakeUserRemoved` and `onMetalakeGroupRemoved` default hooks on `AuthorizationPlugin` and wire `UserGroupManager` and `AuthorizationUtils.callAuthorizationPluginForMetalake` (`core`).
- [ ] Ensure `dropCatalog` removes external catalog privileges only after `CatalogManager` drop preconditions (`metalake-in-use`, `in-use`, and schema emptiness) pass, and validate authorization plugin properties pre-commit on `createCatalog` / `alterCatalog` (`core`).
- [ ] Enable `"kafka"` in `ChainedAuthorization` and harden `IsolatedClassLoader` lifecycle and metalake hook forwarding in `ChainedAuthorizationPlugin` (`authorizations/authorization-chain`).
- [ ] Register the `authorizations/authorization-kafka` Gradle module in `settings.gradle.kts` and `build.gradle.kts` (excluding unused Kafka compression codecs) and register `KafkaAuthorization` via `ServiceLoader`.
- [ ] Implement `KafkaAuthorizationProperties` (`bootstrap.servers` fallback, `authorization.kafka.*` validation, wildcard `*` rejection, `config.providers` blocking, `AdminClient` precedence).
- [ ] Implement `KafkaPrivilegeMapper` (`PRODUCE_TOPIC`, `CONSUME_TOPIC`, `CREATE_TOPIC`, `Owner`, `ALLOW`/`DENY`, scope resolution, asymmetric `DENY` rules).
- [ ] Implement `KafkaAuthorizationStateProvider` and `EntityStoreAuthorizationStateProvider` (context classloader bridge and 500-topic batched `OWNER_REL` lookups).
- [ ] Implement `KafkaAuthorizationPlugin` (`aclSyncLock`, `deletedRoleIds`, touched-object delta reconciliation, scoped topic drop/rename, catalog drop cleanup).
- [ ] Add unit tests for `core` hooks (`TestAccessControlManager`, `TestCatalogHookDispatcher`) and `authorization-kafka` (`TestKafkaAuthorization`, `TestKafkaAuthorizationProperties`, `TestKafkaAuthorizationPlugin`).
- [ ] Add Docker integration tests (`KafkaAuthorizationIT`) verifying end-to-end ACL enforcement against a KRaft `StandardAuthorizer` Kafka broker.
- [ ] Update `.github/workflows/backend-integration-test-action.yml` to exclude `:authorizations:authorization-kafka:test` (covered by `access-control-integration-test-action.yml`).
- [ ] Add user documentation for Kafka authorization properties, privilege mappings, group ACL opt-in, and `authorization-chain` configuration in `docs/kafka-catalog.md`.
