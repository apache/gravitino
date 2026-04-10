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

2. **Function Visibility Control**: Users should only see functions they have privileges on. `listFunctions` and `getFunction` should filter results based on user permissions, following the "can't see what you can't execute" pattern found across all surveyed systems.

3. **Ownership Tracking**: Functions should have owners, set automatically on registration and manageable through Gravitino's existing ownership mechanism.

4. **Backward Compatibility**: Existing function management APIs remain unchanged. Privilege enforcement is additive — when authorization is disabled, behavior is identical to current functionality.

---

## Non-Goals

1. **DEFINER/INVOKER Security Mode**: Gravitino's function metadata model does not currently include a `securityType` field (unlike views). Adding security mode support to functions is a separate metadata model concern and is outside the scope of this privilege design.

2. **New Function Management Capabilities**: This design adds privilege control on top of the existing function management API. No new CRUD operations or metadata model changes are introduced.

3. **Per-Definition Privilege Control**: Privileges are defined at the function level, not at the individual function definition (overload) level. A user who can execute a function can execute any of its definitions.

4. **Built-in Function Privilege Control**: Built-in functions provided by compute engines are not managed by Gravitino and are outside the scope of this design.

---

## Proposal

### Privilege Types

Three privilege types are defined for functions, following established Gravitino privilege naming conventions:

| Privilege | Securable Object Levels | Description |
|-----------|------------------------|-------------|
| `REGISTER_FUNCTION` | Metalake, Catalog, Schema | Permission to register new functions |
| `EXECUTE_FUNCTION` | Metalake, Catalog, Schema, Function | Permission to execute/invoke a function and view its metadata |
| `MODIFY_FUNCTION` | Metalake, Catalog, Schema, Function | Permission to alter a function's metadata |

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
|-----------------|:---:|:---:|:---:|
| Metalake | ✅ | ✅ | ✅ |
| Catalog | ✅ | ✅ | ✅ |
| Schema | ✅ | ✅ | ✅ |
| Function | — | ✅ | ✅ |

> `REGISTER_FUNCTION` is not applicable at the function level because creation happens at the schema level (a function must be created within a schema).

---

### Authorization Expressions

Authorization expressions define the privilege requirements for each function operation. These follow the established pattern from tables and filesets (see `TableOperations.java` and `FilesetOperations.java` for reference implementations).

#### Register Function

```
ANY(OWNER, METALAKE, CATALOG) ||
SCHEMA_OWNER_WITH_USE_CATALOG ||
ANY_USE_CATALOG && ANY_USE_SCHEMA && ANY_REGISTER_FUNCTION
```

- Metalake/catalog/schema owners can always register functions.
- Non-owners need `USE_CATALOG` + `USE_SCHEMA` + `REGISTER_FUNCTION`.
- The `accessMetadataType` is `SCHEMA` (the parent container).

#### Get Function

```
ANY(OWNER, METALAKE, CATALOG) ||
SCHEMA_OWNER_WITH_USE_CATALOG ||
ANY_USE_CATALOG && ANY_USE_SCHEMA && (FUNCTION::OWNER || ANY_EXECUTE_FUNCTION || ANY_MODIFY_FUNCTION)
```

- Metalake/catalog/schema owners can always view function metadata.
- Non-owners need `USE_CATALOG` + `USE_SCHEMA` + (`EXECUTE_FUNCTION` or `MODIFY_FUNCTION` or function ownership).
- The `accessMetadataType` is `FUNCTION`.

#### List Functions

Listing uses **filter-based authorization** rather than deny-based, consistent with how table listing works:

```
ANY(OWNER, METALAKE, CATALOG, SCHEMA, FUNCTION) ||
ANY_EXECUTE_FUNCTION ||
ANY_MODIFY_FUNCTION
```

Functions are filtered from the list if the user has no matching privilege. This implements the "can't see what you can't execute" pattern. Note that `REGISTER_FUNCTION` alone does not grant visibility (consistent with `CREATE_TABLE` not granting table visibility).

#### Alter Function

```
ANY(OWNER, METALAKE, CATALOG) ||
SCHEMA_OWNER_WITH_USE_CATALOG ||
ANY_USE_CATALOG && ANY_USE_SCHEMA && (FUNCTION::OWNER || ANY_MODIFY_FUNCTION)
```

- Metalake/catalog/schema owners and function owners can alter functions.
- Non-owners need `USE_CATALOG` + `USE_SCHEMA` + `MODIFY_FUNCTION`.

#### Drop Function

```
ANY(OWNER, METALAKE, CATALOG) ||
SCHEMA_OWNER_WITH_USE_CATALOG ||
ANY_USE_CATALOG && ANY_USE_SCHEMA && FUNCTION::OWNER
```

- Only function owners (and metalake/catalog/schema owners) can drop functions.
- This follows the same pattern as `DROP_TABLE` and `DROP_FILESET`, where only the object owner can perform the drop — `MODIFY_FUNCTION` alone is not sufficient for drop.

---

### Visibility Control

Function visibility follows the "can't see what you can't execute" pattern observed across all surveyed systems:

1. **`listFunctions`** — Returns only functions the user has at least one privilege on (`EXECUTE_FUNCTION`, `MODIFY_FUNCTION`, or ownership). `REGISTER_FUNCTION` alone does not grant visibility. This is implemented via a filter expression applied to the result set, consistent with table and fileset listing.

2. **`getFunction`** — Requires `EXECUTE_FUNCTION`, `MODIFY_FUNCTION`, or ownership. If the user lacks privileges, the authorization framework denies access.

3. **Function definition protection** — Function definitions (implementations, source code) are part of the function metadata returned by `getFunction`. Since `getFunction` requires privilege, function definitions are protected by default.

---

### Authorization Pushdown — Not Applicable

Unlike tables, which are delegated to underlying data sources (Hive, MySQL, Iceberg, etc.) that have their own privilege systems, **function management is fully managed by Gravitino** — all function metadata is stored in Gravitino's own database via `ManagedFunctionOperations`. There is no delegation to external catalogs.

Therefore, **authorization pushdown is not needed for functions**. Gravitino's own authorization layer is the single enforcement point for all function privilege checks. This is simpler than the table model and eliminates the complexity of privilege mapping to heterogeneous data source privilege systems.

---

### Implementation Changes

#### 1. API Layer (`api/`)

**`Privilege.java`** — Add function privilege enum values:

```java
enum Name {
    // ... existing privileges ...

    // Function privileges
    REGISTER_FUNCTION(0L, 1L << 30),
    EXECUTE_FUNCTION(0L, 1L << 31),
    MODIFY_FUNCTION(0L, 1L << 32),
    // Note: bit 32+ requires highBits usage since lowBits is a long (64 bits)
}
```

> **Bit allocation note:** The existing privileges use bits 0–29 in `lowBits`. Function privileges continue from bit 30. Since Java's `long` provides 64 bits, there is ample room. If bits exceed 63, the `highBits` field (currently 0 for all privileges) is used.

**`MetadataObject.java`** — Add FUNCTION type:

```java
enum Type {
    // ... existing types ...
    FUNCTION;    // Mapped to user-defined function in a schema
}
```

**`SecurableObjects.java`** — Add factory method:

```java
public static SecurableObject ofFunction(
    SecurableObject schema, String function, List<Privilege> privileges) {
  return new SecurableObjectImpl(
      schema, function, MetadataObject.Type.FUNCTION, privileges);
}
```

**`Privileges.java`** — Add privilege factory methods and supported types:

```java
private static final Set<MetadataObject.Type> FUNCTION_REGISTER_SUPPORTED_TYPES =
    Sets.immutableEnumSet(
        MetadataObject.Type.METALAKE,
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.SCHEMA);

private static final Set<MetadataObject.Type> FUNCTION_EXECUTE_MODIFY_SUPPORTED_TYPES =
    Sets.immutableEnumSet(
        MetadataObject.Type.METALAKE,
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.SCHEMA,
        MetadataObject.Type.FUNCTION);

public static Privilege allow(Privilege.Name name) {
    // ... add cases ...
    case REGISTER_FUNCTION:
        return RegisterFunction.allow();
    case EXECUTE_FUNCTION:
        return ExecuteFunction.allow();
    case MODIFY_FUNCTION:
        return ModifyFunction.allow();
}
```

#### 2. Server Layer (`server/`, `server-common/`)

**`FunctionOperations.java`** — Add authorization annotations to REST endpoints:

```java
@GET
@Produces("application/vnd.gravitino.v1+json")
@AuthorizationExpression(
    expression = AuthorizationExpressionConstants.FILTER_FUNCTION_AUTHORIZATION_EXPRESSION,
    accessMetadataType = MetadataObject.Type.FUNCTION)
public Response listFunctions(...) { ... }

@GET
@Path("{function}")
@Produces("application/vnd.gravitino.v1+json")
@AuthorizationExpression(
    expression = AuthorizationExpressionConstants.LOAD_FUNCTION_AUTHORIZATION_EXPRESSION,
    secondaryExpression = AuthorizationExpressionConstants.MODIFY_FUNCTION_AUTHORIZATION_EXPRESSION,
    accessMetadataType = MetadataObject.Type.FUNCTION)
public Response getFunction(...) { ... }

@POST
@Produces("application/vnd.gravitino.v1+json")
@AuthorizationExpression(
    expression = AuthorizationExpressionConstants.REGISTER_FUNCTION_AUTHORIZATION_EXPRESSION,
    accessMetadataType = MetadataObject.Type.SCHEMA)
public Response registerFunction(...) { ... }

@PUT
@Path("{function}")
@Produces("application/vnd.gravitino.v1+json")
@AuthorizationExpression(
    expression = AuthorizationExpressionConstants.MODIFY_FUNCTION_AUTHORIZATION_EXPRESSION,
    accessMetadataType = MetadataObject.Type.FUNCTION)
public Response alterFunction(...) { ... }

@DELETE
@Path("{function}")
@Produces("application/vnd.gravitino.v1+json")
@AuthorizationExpression(
    expression = AuthorizationExpressionConstants.DROP_FUNCTION_AUTHORIZATION_EXPRESSION,
    accessMetadataType = MetadataObject.Type.FUNCTION)
public Response dropFunction(...) { ... }
```

**`AuthorizationExpressionConstants.java`** — Add function authorization expressions:

```java
// Register function
public static final String REGISTER_FUNCTION_AUTHORIZATION_EXPRESSION =
    "ANY(OWNER, METALAKE, CATALOG) || "
    + "SCHEMA_OWNER_WITH_USE_CATALOG || "
    + "ANY_USE_CATALOG && ANY_USE_SCHEMA && ANY_REGISTER_FUNCTION";

// Get function
public static final String LOAD_FUNCTION_AUTHORIZATION_EXPRESSION =
    "ANY(OWNER, METALAKE, CATALOG) || "
    + "SCHEMA_OWNER_WITH_USE_CATALOG || "
    + "ANY_USE_CATALOG && ANY_USE_SCHEMA && (FUNCTION::OWNER || ANY_EXECUTE_FUNCTION || ANY_MODIFY_FUNCTION)";

// Alter function
public static final String MODIFY_FUNCTION_AUTHORIZATION_EXPRESSION =
    "ANY(OWNER, METALAKE, CATALOG) || "
    + "SCHEMA_OWNER_WITH_USE_CATALOG || "
    + "ANY_USE_CATALOG && ANY_USE_SCHEMA && (FUNCTION::OWNER || ANY_MODIFY_FUNCTION)";

// Drop function (owner only, consistent with DROP_TABLE and DROP_FILESET)
public static final String DROP_FUNCTION_AUTHORIZATION_EXPRESSION =
    "ANY(OWNER, METALAKE, CATALOG) || "
    + "SCHEMA_OWNER_WITH_USE_CATALOG || "
    + "ANY_USE_CATALOG && ANY_USE_SCHEMA && FUNCTION::OWNER";

// Filter for list functions
public static final String FILTER_FUNCTION_AUTHORIZATION_EXPRESSION =
    "ANY(OWNER, METALAKE, CATALOG, SCHEMA, FUNCTION) || "
    + "ANY_EXECUTE_FUNCTION || "
    + "ANY_MODIFY_FUNCTION";
```

**`PrivilegeExpressionConverter.java`** — Add privilege expression mappings:

```java
"ANY_REGISTER_FUNCTION" → "((ANY(REGISTER_FUNCTION, METALAKE, CATALOG, SCHEMA)) 
                          && !(ANY(DENY_REGISTER_FUNCTION, METALAKE, CATALOG, SCHEMA)))"

"ANY_EXECUTE_FUNCTION" → "((ANY(EXECUTE_FUNCTION, METALAKE, CATALOG, SCHEMA, FUNCTION)) 
                           && !(ANY(DENY_EXECUTE_FUNCTION, METALAKE, CATALOG, SCHEMA, FUNCTION)))"

"ANY_MODIFY_FUNCTION" → "((ANY(MODIFY_FUNCTION, METALAKE, CATALOG, SCHEMA, FUNCTION)) 
                          && !(ANY(DENY_MODIFY_FUNCTION, METALAKE, CATALOG, SCHEMA, FUNCTION)))"
```

#### 3. Core Layer (`core/`)

**`FunctionHookDispatcher.java`** — New class to handle ownership and hooks:

```java
public class FunctionHookDispatcher implements FunctionDispatcher {
  private final FunctionDispatcher dispatcher;
  private final AuthorizationUtils authorizationUtils;

  @Override
  public Function registerFunction(NameIdentifier ident, ...) {
    Function function = dispatcher.registerFunction(ident, ...);
    // Set current user as function owner
    authorizationUtils.setOwner(ident, MetadataObject.Type.FUNCTION);
    return function;
  }

  // Other methods delegate directly to dispatcher
}
```

**`GravitinoEnv.java`** — Wire `FunctionHookDispatcher` into the operation chain:

```
FunctionEventDispatcher
  → FunctionNormalizeDispatcher
  → FunctionHookDispatcher (NEW)
  → FunctionOperationDispatcher
```

This resolves the existing TODO: `// TODO: Add FunctionHookDispatcher when needed`.

**`AuthorizationUtils.java`** — Add FUNCTION to the set of types that support ownership and privilege operations.

#### 4. Documentation

**`docs/security/access-control.md`** — Add function privilege table:

```markdown
### Function privileges

| Name              | Description                       | Supported securable object types       |
|-------------------|-----------------------------------|---------------------------------------|
| REGISTER_FUNCTION   | Register a function in a schema   | Metalake, Catalog, Schema             |
| EXECUTE_FUNCTION  | Execute a function                | Metalake, Catalog, Schema, Function   |
| MODIFY_FUNCTION   | Alter a function's metadata       | Metalake, Catalog, Schema, Function   |
```

**`docs/manage-user-defined-function-using-gravitino.md`** — Add section on function privileges.

---

### Relationship with Security Mode

Security mode (DEFINER/INVOKER) is primarily meaningful for **SQL UDFs whose body references database objects** (e.g., a function that queries a table internally). For purely computational functions (e.g., a JSON parser, a string formatter), security mode has no practical effect because no database objects are accessed during execution.

If `securityType` is added to the function metadata model in the future, the relationship with privileges would be:

| Concern | Controlled By | Enforced By |
|---------|--------------|-------------|
| Who can **register** a function? | `REGISTER_FUNCTION` privilege | Gravitino |
| Who can **see** a function? | `EXECUTE_FUNCTION` privilege (visibility filter) | Gravitino |
| Who can **invoke** a function? | `EXECUTE_FUNCTION` privilege | Gravitino (metadata check) + Engine (runtime) |
| Who can **alter** a function? | `MODIFY_FUNCTION` privilege | Gravitino |
| Who can **drop** a function? | Function ownership (or metalake/catalog/schema ownership) | Gravitino |
| Whose permissions are used to access **database objects referenced in the function body**? | `securityType` (DEFINER/INVOKER) | Compute engine only |

**Example scenario (hypothetical, requires securityType support):**

```
User A registers a SQL UDF `get_user_count` that internally queries `secret_table`:
  RETURN SELECT count(*) FROM secret_table WHERE dept = input_dept;

With securityType=DEFINER:
  User B (granted EXECUTE_FUNCTION) calls get_user_count('engineering').
  1. Gravitino checks: Does User B have EXECUTE_FUNCTION? → Yes, proceed.
  2. Engine executes the function body using User A's permissions (DEFINER mode).
     - User A has SELECT on secret_table → query succeeds.
     - User B does NOT need access to secret_table.

With securityType=INVOKER:
  User B calls get_user_count('engineering').
  1. Gravitino checks: Does User B have EXECUTE_FUNCTION? → Yes, proceed.
  2. Engine executes the function body using User B's own permissions (INVOKER mode).
     - User B does NOT have SELECT on secret_table → query fails.
```

> **Note:** Adding `securityType` to the function metadata model is a separate concern from this privilege design and is not in scope.

---

### GRANT / REVOKE Syntax

Function privileges are managed through Gravitino's existing GRANT/REVOKE REST API and CLI. No new API endpoints are needed.

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

**CLI:**

```bash
# Grant EXECUTE_FUNCTION on a specific function
gcli role grant --metalake demo_metalake --role data_analyst \
  --privilege "EXECUTE_FUNCTION" --securable-object "catalog.schema.parse_json" --type FUNCTION

# Grant EXECUTE_FUNCTION on all functions in a schema (via schema-level grant)
gcli role grant --metalake demo_metalake --role data_analyst \
  --privilege "EXECUTE_FUNCTION" --securable-object "catalog.schema" --type SCHEMA

# Grant REGISTER_FUNCTION on a schema
gcli role grant --metalake demo_metalake --role data_engineer \
  --privilege "REGISTER_FUNCTION" --securable-object "catalog.schema" --type SCHEMA

# Revoke MODIFY_FUNCTION
gcli role revoke --metalake demo_metalake --role data_analyst \
  --privilege "MODIFY_FUNCTION" --securable-object "catalog.schema.parse_json" --type FUNCTION
```

**Trino SQL (via Gravitino connector):**

```sql
-- Grant via Gravitino's Trino connector
GRANT EXECUTE ON FUNCTION catalog.schema.my_func TO ROLE data_analyst;
REVOKE EXECUTE ON FUNCTION catalog.schema.my_func FROM ROLE data_analyst;
```

> Note: Trino SQL GRANT/REVOKE support for functions requires implementation in the Gravitino Trino connector's `ConnectorAccessControl`, which translates Trino's `EXECUTE` function privilege to Gravitino's `EXECUTE_FUNCTION`.

**Spark and Flink:**

Open-source Spark and Flink do **not** support SQL `GRANT`/`REVOKE` syntax for any object type (including functions). Users must manage function privileges through Gravitino's REST API or CLI. The Spark/Flink connectors themselves do not need GRANT/REVOKE implementation.

---

### Engine Connector Integration

#### Trino Connector

Trino has its own access control SPI (`ConnectorAccessControl`) that the Gravitino Trino connector must implement. Trino calls these methods during query planning (e.g., `checkCanExecuteFunction()` when a function is referenced in a query, `filterFunctions()` when listing functions). Without implementation, Trino would bypass Gravitino's privilege checks at the engine level.

The Gravitino Trino connector translates Trino's function access control calls to Gravitino privilege checks:

```java
public class GravitinoConnectorAccessControl implements ConnectorAccessControl {

    @Override
    public void checkCanExecuteFunction(
            ConnectorSecurityContext context,
            SchemaRoutineName function) {
        gravitinoClient.checkPrivilege(
            currentUser(context),
            securableObject(function),
            Privilege.Name.EXECUTE_FUNCTION);
    }

    @Override
    public void checkCanCreateFunction(
            ConnectorSecurityContext context,
            SchemaRoutineName function) {
        gravitinoClient.checkPrivilege(
            currentUser(context),
            securableObject(function.getSchemaName()),
            Privilege.Name.REGISTER_FUNCTION);
    }

    @Override
    public void checkCanDropFunction(
            ConnectorSecurityContext context,
            SchemaRoutineName function) {
        // Drop requires function ownership; the primary enforcement is
        // server-side via DROP_FUNCTION_AUTHORIZATION_EXPRESSION.
        // The Trino connector can verify ownership as a pre-check.
        gravitinoClient.checkOwnership(
            currentUser(context),
            securableObject(function));
    }

    @Override
    public Set<SchemaRoutineName> filterFunctions(
            ConnectorSecurityContext context,
            Set<SchemaRoutineName> functionNames) {
        return functionNames.stream()
            .filter(fn -> gravitinoClient.hasPrivilege(
                currentUser(context),
                securableObject(fn),
                Privilege.Name.EXECUTE_FUNCTION))
            .collect(Collectors.toSet());
    }
}
```

#### Spark and Flink Connectors

Spark and Flink connectors **do not need separate privilege enforcement logic**. All function operations (register, get, list, alter, drop) go through the Gravitino server's REST API, where the `@AuthorizationExpression` annotations on `FunctionOperations.java` automatically enforce privilege checks when authorization is enabled on the Gravitino server.

This is the same pattern used for tables and other objects — the server-side authorization layer is the single enforcement point, and the engine connectors act as pure REST clients.

---

## Development Plan

| Phase | Task | Priority |
|-------|------|----------|
| **I. API & Core** | | |
| | Add `FUNCTION` to `MetadataObject.Type` | P0 |
| | Add `REGISTER_FUNCTION`, `EXECUTE_FUNCTION`, `MODIFY_FUNCTION` to `Privilege.Name` | P0 |
| | Add `SecurableObjects.ofFunction()` factory method | P0 |
| | Add privilege factory methods in `Privileges.java` | P0 |
| | Add privilege validation rules (supported types per privilege) | P0 |
| **II. Server Authorization** | | |
| | Add authorization expression constants for function operations | P0 |
| | Add privilege expression converter mappings | P0 |
| | Add `@AuthorizationExpression` annotations to `FunctionOperations.java` | P0 |
| | Implement `FunctionHookDispatcher` for ownership tracking | P0 |
| | Wire `FunctionHookDispatcher` into operation chain in `GravitinoEnv.java` | P0 |
| **III. Engine Connectors** | | |
| | Trino connector `ConnectorAccessControl` function privilege checks | P1 |
| **IV. Documentation** | | |
| | Update `access-control.md` with function privilege table | P0 |
| | Update `manage-user-defined-function-using-gravitino.md` with privilege section | P1 |
| **V. Testing** | | |
| | Unit tests for privilege definitions and validation | P0 |
| | Integration tests for function authorization expressions | P0 |
| | Engine connector privilege enforcement tests | P1 |

---

## References

1. [Investigation of Function Privilege Control](./investigation_of_function_privilege.md) — Comprehensive survey of function privilege implementations across 17 systems
2. [Gravitino Access Control](https://gravitino.apache.org/docs/security/access-control) — Existing privilege model documentation
3. [Design of View Management in Gravitino](../view-design/Design_of_View_Management_in_Gravitino.md) — Reference for privilege design patterns
4. [Gravitino Function Management](https://gravitino.apache.org/docs/manage-user-defined-function-using-gravitino) — Current function management API
5. [Databricks UC Privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges) — Industry reference for function privileges
6. [Trino Access Control SPI](https://github.com/trinodb/trino/blob/master/core/trino-spi/src/main/java/io/trino/spi/security/SystemAccessControl.java) — Reference for function visibility filtering
