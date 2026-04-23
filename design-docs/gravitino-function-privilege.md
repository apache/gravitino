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
# Design of Function Privilege Control in Gravitino

## Background

Apache Gravitino provides a unified function registry that allows users to define custom functions (UDFs) once and share them across multiple compute engines (Trino, Spark, Flink). Gravitino currently supports full function metadata management — register, list, get, alter, drop — but **does not yet have privilege control for functions**.

The existing Gravitino access control framework covers catalogs, schemas, tables, views, topics, filesets, models, tags, policies, and jobs. Each object type has well-defined privilege types (e.g., `CREATE_TABLE`, `SELECT_TABLE`, `MODIFY_TABLE`) with privilege inheritance through the metalake → catalog → schema hierarchy. Functions are the notable gap in this model:

- **No function privilege types** are defined in `Privilege.Name` — any authenticated user can register, execute, alter, or drop any function.
- **No function visibility control** — `listFunctions` returns all functions regardless of the user's permissions.
- **No ownership tracking** — `FunctionHookDispatcher` is missing, so function ownership is not set on creation.

---

## Goals

1. **Integrate with Existing Access Control Framework**: Define function privilege types that follow established Gravitino naming conventions and privilege inheritance patterns.

2. **Function Visibility Control**: Users should only see functions they have privileges on. `listFunctions` and `getFunction` should filter results based on user permissions, consistent with how tables and filesets handle visibility.

3. **Ownership Tracking**: Functions should have owners, set automatically on registration and manageable through Gravitino's existing ownership mechanism.

4. **Backward Compatibility**: Existing function management APIs remain unchanged. Privilege enforcement is additive — when authorization is disabled, behavior is identical to current functionality.

---

## Non-Goals

1. **DEFINER/INVOKER Security Mode**: Gravitino's function metadata model does not currently include a `securityType` field. Adding security mode support to functions is a separate metadata model concern and is outside the scope of this privilege design.

2. **New Function Management Capabilities**: This design adds privilege control on top of the existing function management API. No new CRUD operations or metadata model changes are introduced.

3. **Per-Definition Privilege Control**: Privileges are defined at the function level, not at the individual function definition (overload) level. A user who can execute a function can execute any of its definitions.

4. **Built-in Function Privilege Control**: Built-in functions provided by compute engines are not managed by Gravitino and are outside the scope of this design.

---

## Proposal

### Privilege Types

Three privilege types are defined for functions, following established Gravitino privilege naming conventions:

| Privilege           | Securable Object Levels             | Description                                                   |
|---------------------|-------------------------------------|---------------------------------------------------------------|
| `REGISTER_FUNCTION` | Metalake, Catalog, Schema           | Permission to register new functions                          |
| `EXECUTE_FUNCTION`  | Metalake, Catalog, Schema, Function | Permission to execute/invoke a function and view its metadata |
| `MODIFY_FUNCTION`   | Metalake, Catalog, Schema, Function | Permission to alter a function's metadata                     |

**Naming rationale:**

- `REGISTER_FUNCTION` — Consistent with `REGISTER_MODEL` and the `registerFunction` API method. Gravitino uses "register" for managed metadata objects (functions, models) to distinguish from "create" used for delegated objects (tables, views, schemas).
- `EXECUTE_FUNCTION` — The universally adopted privilege name across all surveyed systems (Databricks UC, Trino, MySQL, PostgreSQL, OceanBase, Hologres). Analogous to `SELECT_TABLE` / `SELECT_VIEW` in semantics (the fundamental "use" privilege), but `EXECUTE` is the standard term for functions.
- `MODIFY_FUNCTION` — Consistent with `MODIFY_TABLE`. Covers alter operations on function metadata (comment, definitions, properties). Drop operations require function ownership, following the same pattern as table and fileset drop (see `TableOperations.java` and `FilesetOperations.java` where DROP uses owner-only expressions).

**Privilege inheritance:** Privileges granted at metalake, catalog, or schema level cascade to all functions within that scope, consistent with existing behavior for tables and filesets.

**Deny privileges:** Each privilege has a corresponding deny form (`DENY_REGISTER_FUNCTION`, `DENY_EXECUTE_FUNCTION`, `DENY_MODIFY_FUNCTION`) following the existing deny privilege pattern.

---

### Securable Object Hierarchy

Functions follow the existing three-level namespace hierarchy:

```
metalake
  └── catalog
        └── schema
              └── function
```

This is consistent with tables, filesets, and other schema-scoped objects. Functions can be registered under any catalog type that supports schemas (relational, Hive, Iceberg, etc.). A new `MetadataObject.Type.FUNCTION` type and `SecurableObjects.ofFunction()` factory method are added.

**Privilege applicability by level:**

| Securable Object | REGISTER_FUNCTION | EXECUTE_FUNCTION | MODIFY_FUNCTION |
|------------------|-------------------|------------------|-----------------|
| Metalake         | ✅                 | ✅                | ✅               |
| Catalog          | ✅                 | ✅                | ✅               |
| Schema           | ✅                 | ✅                | ✅               |
| Function         | —                 | ✅                | ✅               |

> `REGISTER_FUNCTION` is not applicable at the function level because creation happens at the schema level (a function must be created within a schema).

---

### Visibility Control

Function visibility follows the same pattern as tables and filesets — users can only see functions they have at least one operational privilege on:

1. **`listFunctions`**
   - Requires `USE_CATALOG` + `USE_SCHEMA` at the endpoint level to access the schema (consistent with `listTables`).
   - Applies a filter expression to the result set — only functions the user has at least one privilege on (`EXECUTE_FUNCTION`, `MODIFY_FUNCTION`, or ownership) are returned.
   - `REGISTER_FUNCTION` alone does not grant visibility (consistent with `CREATE_TABLE` not granting table visibility).

2. **`getFunction`**
   - Requires `USE_CATALOG` + `USE_SCHEMA` at the endpoint level, plus `EXECUTE_FUNCTION`, `MODIFY_FUNCTION`, or ownership (consistent with `loadTable`).
   - If the user lacks privileges, the authorization framework denies access.

---

### Authorization Pushdown — Not Applicable

Unlike tables, which are delegated to underlying data sources (Hive, MySQL, Iceberg, etc.) that have their own privilege systems, **function management is fully managed by Gravitino** — all function metadata is stored in Gravitino's own database via `ManagedFunctionOperations`. There is no delegation to external catalogs.

Therefore, **authorization pushdown is not needed for functions**. Gravitino's own authorization layer is the single enforcement point for all function privilege checks. This is simpler than the table model and eliminates the complexity of privilege mapping to heterogeneous data source privilege systems.

---

### GRANT / REVOKE Syntax

Function privileges are managed through Gravitino's existing GRANT/REVOKE REST API. No new API endpoints are needed.

**REST API:**

```
PUT /api/metalakes/{metalake}/roles/{role}/grant
```

```json
{
  "securableObjects": [
    {
      "fullName": "catalog.schema.my_function",
      "type": "FUNCTION",
      "privileges": [
        {"name": "EXECUTE_FUNCTION", "condition": "ALLOW"}
      ]
    }
  ]
}
```

> Note: Trino's `GRANT` SQL syntax only supports table and schema privileges (see [Trino GRANT docs](https://trino.io/docs/current/sql/grant.html)); there is no `GRANT ... ON FUNCTION` syntax. Spark and Flink also do not support SQL `GRANT`/`REVOKE`. The Gravitino CLI (`gcli`) does not yet support function privilege types either (the CLI's privilege validation list currently covers catalog, schema, table, fileset, and topic privileges only). Users should manage function privileges through the REST API.

---

### Engine Connector Integration

All function operations (register, get, list, alter, drop) go through the Gravitino server's REST API, where the `@AuthorizationExpression` annotations on `FunctionOperations.java` automatically enforce privilege checks when authorization is enabled on the Gravitino server.

This is the same pattern used for tables and other objects — the server-side authorization layer is the single enforcement point, and the engine connectors (Trino, Spark, Flink) act as pure REST clients.
