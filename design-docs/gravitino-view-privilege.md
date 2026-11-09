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

# Design of View Privilege Control in Gravitino

## Background

Apache Gravitino's [View Management design](https://docs.google.com/document/d/1qKZMcY5ifgZF-BjGF2FwYBNWyTwqrCDLaGW_D2jD_LY) provides unified view management across heterogeneous catalogs (Iceberg, Paimon, HMS, JDBC, and Gravitino-managed catalogs). The parent design assigns view privilege control to "Phase IV. Governance & Security" and lists four required privileges: `CREATE_VIEW`, `SELECT_VIEW`, `ALTER_VIEW`, `DROP_VIEW`.

The existing Gravitino access control framework covers catalogs, schemas, tables, filesets, topics, models, tags, policies, jobs, and functions. Views are a partially-defined object type in this model — the privilege types exist in the API but are not enforced end-to-end, leaving view access control in an inconsistent state:

- **Privilege type set is incomplete.** Only `CREATE_VIEW` and `SELECT_VIEW` exist in `Privilege.Name` (see `api/src/main/java/org/apache/gravitino/authorization/Privilege.java:146-148`). `ALTER_VIEW`-equivalent and `DROP_VIEW`-equivalent are missing, so view alter and drop operations have no privilege to check against.
- **REST endpoints do not enforce privileges.** `ViewOperations.java` (the generic view REST endpoints) carries zero `@AuthorizationExpression` annotations on its five endpoints (`listViews`, `createView`, `loadView`, `alterView`, `dropView` at lines 67, 93, 132, 158, 191 respectively). Compare with `TableOperations.java` where every endpoint is wired (lines 83, 119, 167, 204, 241).
- **No visibility filtering on `listViews`.** `ViewOperations.listViews` (lines 71–94) returns the raw `dispatcher.listViews(viewNS)` result with no filter applied. Compare with `TableOperations.listTables` (lines 86–112) which calls `MetadataAuthzHelper.filterByExpression(..., FILTER_TABLE_AUTHORIZATION_EXPRESSION, ...)` to drop entries the caller has no privilege on. The `FILTER_VIEW_AUTHORIZATION_EXPRESSION` constant already exists in `AuthorizationExpressionConstants.java:119` but is consumed only by the Iceberg-REST path (`IcebergViewOperations.java:350`); the generic path never references it. Result: any user who can reach the list endpoint sees every view in the schema, regardless of per-view privileges.
- **No ownership tracking for views.** `core/src/main/java/org/apache/gravitino/hook/` contains 12 `*HookDispatcher` classes (Catalog, Schema, Table, Fileset, Function, Model, …) but no `ViewHookDispatcher`. As a result, view owners are never set on creation, and the `VIEW::OWNER` clause already present in `LOAD_VIEW_AUTHORIZATION_EXPRESSION` (`AuthorizationExpressionConstants.java:97`) never resolves to true for the view creator.
- **Engine-side ACL translation is silent.** The Ranger plugin (`authorizations/authorization-ranger/`) contains zero references to `MetadataObject.Type.VIEW` or any view privilege — view grants are not translated to Ranger ACLs.
- **Iceberg-REST is the only path that enforces view auth today.** `IcebergViewOperations.java` (lines 90, 126, 164, 201, 243, 282) is fully wired with `@AuthorizationExpression` and `ICEBERG_LOAD_VIEW_AUTHORIZATION_EXPRESSION`. This means the same conceptual operation has two enforcement paths with different coverage depending on the catalog type, which is confusing for users and operators.

The result is a privilege model where granting `SELECT_VIEW` to a user has visible effect on Iceberg-REST views but no effect on views served via the generic REST path, and where alter/drop operations on any view are unprotected.

**Current state — two parallel paths, only one enforces view auth:**

```
                       ┌────────────────────────────────────────┐
                       │       Authorization framework          │
                       │  (roles, grants, owner, expressions)   │
                       └──────────────┬─────────────────────────┘
                                      │ checked at REST layer
                                      │
       ┌──────────────────────────────┴──────────────────────────────┐
       │                                                             │
       ▼                                                             ▼
  IcebergViewOperations                                       ViewOperations
  (Iceberg REST spec path)                                    (Gravitino REST path)
       │ @AuthorizationExpression                                  │ (no @AuthorizationExpression)
       │ on every endpoint  ✅                                      │ on any endpoint     ❌
       ▼                                                            ▼
  Iceberg REST handlers                                       ViewNormalizeDispatcher
                                                                    │
                                                                    │ (no ViewHookDispatcher
                                                                    │  → view owner never set ❌
                                                                    │  → VIEW::OWNER clause
                                                                    │    never resolves true)
                                                                    ▼
                                                              ViewOperationDispatcher
                                                                    │
                                                                    ▼
                                                          Catalog connector (Iceberg/
                                                          Paimon/HMS/JDBC/managed)

  Ranger plugin: zero references to MetadataObject.Type.VIEW ❌
  (view grants in Gravitino are not translated to Ranger ACLs)
```

---

## Goals

1. **Complete the View Privilege Type Set**: Complete the privilege management and protect against the `DROP` operation, similar to function privilege management.

2. **Enforce View Privileges on Generic REST Endpoints**: Enforce privilege authentication on REST endpoints.

3. **Ownership Tracking for Views**: Track ownership in a manner similar to the function privilege management process, inheriting the existing ownership model.

4. **View Security Mode**: Introduce Definer/Invoker security support for view privilege management, allowing owners flexibility in switching modes for new/existing views.

6. **Backward Compatibility**: It should be backward compatible with existing privilege management and the underlying data engine if views have previously been defined within the sub-system.

---

## Non-Goals

1. **Data-Level Access Control**: View privileges in Gravitino govern metadata access only. Whether the user can read the underlying tables referenced by the view SQL is enforced by the compute engine and the underlying catalog's permission system. The parent design doc commits to this scope split explicitly.

2. **Per-User Identity Propagation in Spark and Flink Connectors**: Today's Gravitino Spark and Flink connectors authenticate to Gravitino using a singleton service identity established at catalog initialization. Consequently, we only have session context-level identity and, unlike Trino, lack per-user caller identity.

3. **Unifying Generic View Auth Paths**: The Data Lake/Warehouse and Database have different approaches to managing views, privileges, and security. Unifying them into a single model is out of scope.

4. **Materialized Views and Temporary Views**: Out of scope per the parent design (only logical views are in scope).

5. **Alter View Not Supported for Rename**: Some data engines, such as MySQL and Iceberg, have different mechanisms for supporting rename functionality, including renaming within or across namespaces. Therefore, we are keeping this feature out of scope in the current version.

5. **Identity Mapping between Gravitino and Databases**: This implementation will not manage the identity alignment between Gravitino's user principal and the database's user principal.

---

## Proposal

### Privilege Types

Three privilege types are defined for views, matching the existing `*_TABLE` and `*_FUNCTION` shape. `CREATE_VIEW` and `SELECT_VIEW` already exist; `ALTER_VIEW` is new. **Drop view operations are not gated by a dedicated privilege — they require ownership of the view itself, or ownership of any parent (schema, catalog, or metalake), consistent with `dropTable` / `dropFileset` / `dropFunction` in Gravitino.**

| Privilege      | Securable Object Levels                 | Description                                                                              | Status   |
|----------------|-----------------------------------------|------------------------------------------------------------------------------------------|----------|
| `CREATE_VIEW`  | Metalake, Catalog, Schema               | Permission to create new views in a schema.                                              | Existing |
| `SELECT_VIEW`  | Metalake, Catalog, Schema, View         | Permission to read view metadata and resolve the view's SQL during query planning.       | Existing |
| `ALTER_VIEW`  | Metalake, Catalog, Schema, View         | Permission to alter a view's metadata (comment, properties, representations).    | **New**  |

**Naming rationale:**

- `CREATE_VIEW` — Consistent with `CREATE_TABLE`, `CREATE_FILESET`. Used for delegated objects whose primary store is the underlying catalog. No change.
- `SELECT_VIEW` — Consistent with `SELECT_TABLE`. Reading view metadata is the analog of selecting from a table at the metadata layer; the actual SELECT against underlying data remains the engine's responsibility. No change.
- `ALTER_VIEW` — Matches the parent View Management design doc and standard SQL `ALTER VIEW` syntax. Covers all alter operations supported by the parent design ( updateComment, setProperty, removeProperty, addRepresentation, updateRepresentation, removeRepresentation). Diverges from Gravitino's existing `MODIFY_TABLE` / `MODIFY_FUNCTION` convention but aligns with the parent doc and with the `alterView` REST/API/Java method names.

**Privilege inheritance:** Privileges granted at metalake, catalog, or schema level cascade to all views within that scope. Same as `*_TABLE` and `*_FUNCTION`.

**Deny privileges:** Each privilege has a corresponding deny form (`DENY_CREATE_VIEW`, `DENY_SELECT_VIEW`, `DENY_ALTER_VIEW`). Existing deny entries for `CREATE_VIEW` and `SELECT_VIEW` already exist at `Privileges.java:319-321`; `DENY_ALTER_VIEW` is added.

**Bitmask allocation:** `CREATE_VIEW` and `SELECT_VIEW` keep their existing bits (`1L << 28`, `1L << 29`). `ALTER_VIEW` is allocated the next free bit after `MODIFY_FUNCTION (1L << 32)` — `1L << 33`.

---

### Securable Object Hierarchy

Views are schema-scoped, following Gravitino's standard four-level hierarchy:

```
metalake
  └── catalog
        └── schema
              └── view
```

`MetadataObject.Type.VIEW` already exists, and `VIEW_SUPPORTED_TYPES` in `Privileges.java:60-65` already covers METALAKE, CATALOG, SCHEMA, VIEW. `VIEW` is also already accepted by `MANAGE_GRANTS_SUPPORTED_TYPES` (line 86). No structural changes are required.

**Privilege and ownership applicability by level:**

| Securable Object | CREATE_VIEW | SELECT_VIEW | ALTER_VIEW | Drop (ownership-gated) |
|------------------|-------------|-------------|------------|------------------------|
| Metalake         | ✅           | ✅           | ✅          | ✅ (metalake owner)     |
| Catalog          | ✅           | ✅           | ✅          | ✅ (catalog owner)      |
| Schema           | ✅           | ✅           | ✅          | ✅ (schema owner)       |
| View             | —           | ✅           | ✅          | ✅ (view owner)         |

> `CREATE_VIEW` is not applicable at the view level because creation happens at the schema level (a view must be created within a schema).
>
> Drop has no dedicated privilege — it is authorized by ownership at any level. Owning a parent (metalake / catalog / schema) implicitly authorizes dropping any view in that scope. The full expression is shown in Authorization Enforcement below.

---

### Visibility Control

View visibility and access follow the same patterns as tables:

1. **`listViews`**
   - Requires `USE_CATALOG` + `USE_SCHEMA` at the endpoint level to access the schema (consistent with `listTables`).
   - Applies `FILTER_VIEW_AUTHORIZATION_EXPRESSION` (`AuthorizationExpressionConstants.java:119`) to the result set — only views the user has `SELECT_VIEW`, `ALTER_VIEW`, `CREATE_VIEW` (at schema, catalog, or metalake level), or ownership on are returned.

2. **`loadView`**
   - Requires `USE_CATALOG` + `USE_SCHEMA`, plus `SELECT_VIEW`, `ALTER_VIEW`, `CREATE_VIEW`, or view ownership (consistent with `loadTable`).
   - If the user lacks privileges, the authorization framework denies access.

3. **`createView`**
   - Requires `USE_CATALOG` + `USE_SCHEMA` + (`CREATE_VIEW` at schema, catalog, or metalake, or schema ownership).

4. **`alterView`**
   - Requires `USE_CATALOG` + `USE_SCHEMA` + (`ALTER_VIEW` or view ownership).

5. **`dropView`**
   - Gated by ownership at the view level **or any parent level** (schema, catalog, metalake), mirroring `dropTable` exactly. See Authorization Enforcement below for the full expression.

---

### Authorization Enforcement

All view operations go through the Gravitino REST server, where `@AuthorizationExpression` annotations on `ViewOperations.java` enforce privilege checks when authorization is enabled. This follows the same pattern as `TableOperations.java` (lines 83–254) — the server-side authorization layer is the single enforcement point, and the engine connectors (Trino, Spark, Flink) act as pure REST clients.

Existing expressions in `AuthorizationExpressionConstants.java` are reused:

- `LOAD_VIEW_AUTHORIZATION_EXPRESSION` (line 93) — gates `loadView`.
- `FILTER_VIEW_AUTHORIZATION_EXPRESSION` (line 119) — used by `listViews` to filter results.

Two new expressions are added, mirroring the `*_TABLE` analogs:

- `CREATE_VIEW_AUTHORIZATION_EXPRESSION` — gates `createView`.
- `ALTER_VIEW_AUTHORIZATION_EXPRESSION` — gates `alterView`, and used as the `secondaryExpression` on `loadView` (mirroring the pattern at `TableOperations.java:169`).

Drop reuses the established owner-hierarchy expression form (no new constant needed). The expression is identical in shape to `dropTable`'s at `TableOperations.java:241`:

```
ANY(OWNER, METALAKE, CATALOG) ||
SCHEMA_OWNER_WITH_USE_CATALOG ||
ANY_USE_CATALOG && ANY_USE_SCHEMA && VIEW::OWNER
```

This authorizes drop for the view owner, the schema owner (with `USE_CATALOG`), the catalog owner, or the metalake owner. No `DROP_VIEW` privilege is introduced — Gravitino's hierarchical ownership model already covers admin and steward personas through ownership escalation, and adding `DROP_VIEW` would make views the only droppable object type with a per-operation drop privilege (`dropTable`, `dropFileset`, and `dropFunction` are all owner-gated today).

A new `ViewHookDispatcher` is added under `core/src/main/java/org/apache/gravitino/hook/`, modeled on `TableHookDispatcher`. It sets the creator as the view owner on `createView` (mirroring `TableHookDispatcher:83-93`), which unblocks the `VIEW::OWNER` clause already present in `LOAD_VIEW_AUTHORIZATION_EXPRESSION` and used by the new `ALTER_VIEW_AUTHORIZATION_EXPRESSION`.

The Iceberg-REST view authorization path (`IcebergViewOperations.java`) is unchanged — it continues to use `ICEBERG_LOAD_VIEW_AUTHORIZATION_EXPRESSION` independently to preserve Iceberg REST spec compliance.

---

### View Security Mode

The parent View Management design introduces `securityConfig.securityMode` (`DEFINER` | `INVOKER`) on the `View` interface and persists it in `view_version_info.security_mode`. In this section, we will discuss the application of the security mode on Data Engines (Iceberg, Paimon, MySQL, etc.) and Connectors (Trino/Spark/Flink).

**Default**: `DEFINER` (matches the parent doc's `view_version_info.security_mode DEFAULT 'DEFINER'`).

**Mutability**: `securityMode` is **mutable post-creation**. A new `ViewChange.updateSecurityMode(newMode)` is added alongside the existing alter operations (`rename`, `updateComment`, `setProperty`, etc.). Gating follows the standard `alterView` rule — the caller needs `ALTER_VIEW` on the view, or ownership at the view, schema, catalog, or metalake level. Each mutation creates a new view version per the parent doc's existing versioning behavior. In pass-through mode (see below), the change is propagated to the data engine via its native `ALTER VIEW … SQL SECURITY …` DDL; in Gravitino-managed mode it is a metadata-only update.

This diverges from the parent doc's V1 schema comment marking `security_mode` as `immutable in V1`. The column itself is retained; the mutability constraint is lifted by this design. Rationale: an immutable security mode would force users to drop and recreate views (losing version history and dependent grants) just to change DEFINER↔INVOKER, which is operationally worse than allowing the alter.

---

#### Storage Strategy Alignment: Pass-through vs Gravitino-managed

The enforcement mode is determined by the storage path the query takes through the parent doc's storage strategy. The mapping below covers the parent doc's three tiers (complete delegation, delegation + extension, fully Gravitino-managed) and resolves the non-dialect / non-supporting-data-engine cases the parent doc identifies:

| Parent doc storage strategy                       | Data engine                          | Dialect path                                   | Enforcement mode      |
|---------------------------------------------------|--------------------------------------|------------------------------------------------|-----------------------|
| Complete delegation                               | Iceberg, Paimon                      | Any dialect                                    | **Gravitino-managed** |
| Delegation + extension                            | HMS, MySQL, Postgres, Doris, …       | Native dialect (stored in data engine)         | **Pass-through**      |
| Delegation + extension                            | HMS, MySQL, Postgres, Doris, …       | Non-native dialect (Gravitino DB only)         | **Gravitino-managed** |
| Fully Gravitino-managed                           | Hudi, Delta, Lance, Generic          | Any dialect                                    | **Gravitino-managed** |

**Pass-through mode** applies only when the query traverses the data engine's native view machinery — specifically, the native-dialect path against HMS or a JDBC data engine that itself supports a security mode (MySQL `SQL SECURITY`, Postgres `security_invoker`, etc.):

- Gravitino translates `createView` / `alterView` to the data engine's native DDL with the appropriate security clause (e.g., `CREATE DEFINER='<user-id>'@'%' VIEW … SQL SECURITY DEFINER` for MySQL; `ALTER VIEW … SQL SECURITY INVOKER` for an alter that toggles the mode).
- The data engine enforces `securityMode` natively at query execution time.
- Gravitino stores `securityMode` as metadata for visibility/audit but is **not** in the SELECT execution path.
- In this implementation, Gravitino will **validate the user identity at the metadata level** and directly use the user ID for data engine level query execution.

**Gravitino-managed mode** applies in every other case, which covers the two distinct scenarios the parent doc surfaces:

1. **Data lakes without a native view-security concept** — Iceberg, Paimon, Hudi, Delta, Lance, … The data engine has no notion of view-bound security; if Gravitino doesn't enforce, nobody does. This is the parent doc's "complete delegation" and "fully Gravitino-managed" tiers.
2. **Non-native dialect against a view-security-supporting data engine** — the view is stored in MySQL/Postgres/HMS in its native dialect, but the connector is asking for a different dialect (e.g., the Trino-dialect representation of a MySQL-stored view). Per the parent doc, non-native dialects live only in Gravitino DB; the data engine never sees them. The data engine therefore cannot enforce on this path even though it natively supports security mode for its own dialect. This resolves the parent doc's "non-dialect" question explicitly.

In Gravitino-managed mode:

- Gravitino owns the enforcement decision at query time using the single-branch model defined below.
- For storage access, Gravitino vends scoped credentials via existing credential-vending infrastructure (Iceberg REST `config.s3.*` keys, fileset credential APIs).
- For the Iceberg REST flow, integration with the merged Iceberg `referenced-by` parameter ([PR #13810](https://github.com/apache/iceberg/pull/13810)) is required to discriminate view-mediated reads from direct reads. The Spark + REST reference implementation ([PR #13979](https://github.com/apache/iceberg/pull/13979)) is in progress upstream; Gravitino's Iceberg REST endpoint will adopt it as a follow-up.

The enforcement is a single branch on `securityMode`:

```
principal = (securityMode == DEFINER) ? view.owner() : session.user();
require(principal has SELECT_TABLE on each underlying table);
vendCredentials(principal, underlyingTable);
```

- **DEFINER (default)**: the authorizing principal is the **view owner**, read from `owner_meta` (managed via the `ViewHookDispatcher` introduced in Authorization Enforcement above). Independent of the calling connection.
- **INVOKER**: the authorizing principal is the **session identity** of the caller — whatever the connector authenticated to Gravitino's REST API as. The connector-specific semantics are detailed in the next section.

---

#### Connector Identity Model

Gravitino's three connectors differ in whether they propagate the real end-user identity or only a session-level service identity when calling Gravitino's REST API. This design's approach embraces both models:

- **Trino — per-user identity**. Trino's connector uses per-session user forwarding when `gravitino.client.session.forwardUser=true` is set (`trino-connector/.../GravitinoConnector.java:88-89` and `GravitinoAuthProvider.buildForSession()`). Built on Trino's `ConnectorSession.getUser()` SPI primitive, which is the only Trino-native input — the surrounding cache and per-user `GravitinoAdminClient` construction is Gravitino-implemented. With this enabled, Gravitino sees Alice's query as Alice.

- **Spark and Flink — session identity (service identity today)**. Today's Spark and Flink connectors authenticate to Gravitino with a singleton `GravitinoAdminClient` configured at catalog initialization (`flink-connector/.../GravitinoCatalogManager.java:307`; equivalent in Spark). Every call from Spark or Flink to Gravitino's REST API authenticates as that service account, regardless of which end-user originated the query. Replicating Trino's `forwardUser` is non-trivial in these engines because their catalog SPIs (`TableCatalog.loadTable(Identifier)`, `Catalog.getTable(ObjectPath)`) do not carry a user parameter. In this case, we will currently use the session context as the "user" identity.

These identity models interact with the two enforcement modes and the underlying data engine type:

| Connector + identity model            | Data lake (Iceberg, Paimon, …)<br>**Gravitino-managed**  | View-security data engine (MySQL, Postgres, HMS, …)<br>**Pass-through** (native dialect) | View-security data engine, **non-native dialect**<br>**Gravitino-managed**  |
|---------------------------------------|----------------------------------------------------------|------------------------------------------------------------------------------------------|------------------------------------------------------------------------------|
| **Trino** with `forwardUser=true`     | DEFINER ✅ owner / INVOKER ✅ real user                  | DEFINER ✅ / INVOKER ✅ (data engine enforces)                                            | DEFINER ✅ owner / INVOKER ✅ real user                                       |
| **Trino** without `forwardUser`       | DEFINER ✅ owner / INVOKER ⚠️ service                     | DEFINER ✅ / INVOKER ✅ (data engine enforces)                                            | DEFINER ✅ owner / INVOKER ⚠️ service                                          |
| **Spark** (singleton service id)      | DEFINER ✅ owner / INVOKER ⚠️ service                     | DEFINER ✅ / INVOKER ✅ (data engine enforces)                                            | DEFINER ✅ owner / INVOKER ⚠️ service                                          |
| **Flink** (singleton service id)      | DEFINER ✅ owner / INVOKER ⚠️ service                     | DEFINER ✅ / INVOKER ✅ (data engine enforces)                                            | DEFINER ✅ owner / INVOKER ⚠️ service                                          |

Two consequences of this design:

1. **DEFINER (the default) is fully enforced in every cell from V1.** Whether the data engine has a native view-security concept (MySQL/Postgres/HMS) or not (data lakes), and whether the connector forwards real-user identity (Trino with `forwardUser`) or only a session/service identity (Spark/Flink), Gravitino's lookup of `view.owner()` from `owner_meta` and subsequent authorization is independent of the calling connection. DEFINER is the default precisely because it is comprehensively enforceable today.

2. **INVOKER's accuracy depends on the cell**:
   - **Pass-through cells** (native-dialect path to MySQL/Postgres/HMS): the data engine enforces with whatever identity the connector's JDBC/Thrift connection uses. Identity propagation between connector and data engine is a connector / deployment concern outside this design — typically per-user JDBC credentials, proxy authentication, or Kerberos delegation.
   - **Gravitino-managed cells** (data lakes, or non-native dialect on a JDBC data engine): Gravitino enforces using session identity. Accurate per-user on Trino with `forwardUser`; degraded to service-identity enforcement on Spark/Flink today. The degradation is principled — INVOKER means "whoever Gravitino sees as the caller" — and matches deployment reality, since operators today grant `spark_svc` / `flink_svc` the privileges they want Spark/Flink jobs to have. When per-session forwarding lands in those connectors, INVOKER becomes per-user automatically with no design changes here.

---

### GRANT / REVOKE Syntax

View privileges are managed through Gravitino's existing GRANT/REVOKE REST API. No new endpoints are needed.

**REST API:**

```
PUT /api/metalakes/{metalake}/roles/{role}/grant
```

```json
{
  "securableObjects": [
    {
      "fullName": "catalog.schema.customer_summary",
      "type": "VIEW",
      "privileges": [
        {"name": "SELECT_VIEW", "condition": "ALLOW"},
        {"name": "ALTER_VIEW", "condition": "ALLOW"}
      ]
    }
  ]
}
```

> The Gravitino CLI (`gcli`) does not currently include view privileges in its validation list. CLI support is tracked separately, consistent with the function-privilege design doc's position.

---

### Engine Connector Integration

All view operations (list, create, load, alter, drop) go through the Gravitino server's REST API, where the `@AuthorizationExpression` annotations on `ViewOperations.java` automatically enforce privilege checks when authorization is enabled on the Gravitino server.

This is the same pattern used for tables and other objects — the server-side authorization layer is the single enforcement point, and the engine connectors (Trino, Spark, Flink) act as pure REST clients.

Extending the Ranger plugin to translate Gravitino view privileges to underlying-engine ACLs (e.g., Hive Ranger) is a known follow-up. The Ranger plugin currently contains no references to `MetadataObject.Type.VIEW`.
