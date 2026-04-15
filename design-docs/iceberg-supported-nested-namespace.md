# [Iceberg REST] Supported Nested Namespace Design

## Background

This document describes one practical solution to support Iceberg nested namespaces in Gravitino.
The scope is not only UI privilege granting, but also namespace mapping, identifier handling,
authorization scope, and compatibility behavior across Iceberg REST and Gravitino.

References:

- https://github.com/apache/gravitino/blob/main/docs/security/access-control.md
- https://github.com/apache/gravitino/blob/main/docs/iceberg-rest-service.md
- https://github.com/apache/gravitino/blob/main/docs/manage-relational-metadata-using-gravitino.md
- https://github.com/apache/gravitino/discussions/7296

## Goal

- Support nested namespace operations from Iceberg REST to Gravitino through schema mapping.
- Support privilege granting for different nested namespace scopes (including UI workflow).
- Keep metadata model stable and avoid heavy refactor.

## Non-Goal

- Create nested namespace by Gravitino REST API directly.
- Delete nested namespace by Gravitino REST API directly.
- Modify nested namespace by Gravitino REST API directly (for example, rename or alter properties).
- Introduce a new metadata object (for example `NestedNamespace`) in this phase.

## Solution Options

### Option A: Add a new metadata object `NestedNamespace`

Use a new metadata object `NestedNamespace` to represent nested namespace explicitly.
`NestedNamespace` has a one-to-one mapping with Iceberg `Namespace` to avoid ambiguity
with existing Gravitino `Namespace` concepts.

Catalog -> NestedNamespace a -> NestedNamespace a.b -> Table a.b.c
                              -> NestedNamespace a.c -> NestedNamespace a.c.d -> Table a.c.d.e

Pros:

- Clearer concept modeling.

Cons:

- Large refactor across metadata model, API, authorization, and UI.

### Option B (Recommended): Reuse `Schema` entity and enhance schema expression capability

Keep physical metadata unchanged (still persisted as `Schema`) and introduce
`HierarchicalSchema` as a logical expression layer in Iceberg REST adaptation,
identifier rendering, and authorization scope matching.

Pros:

- Low-impact evolution path without introducing a new metadata entity.
- Decouples nested namespace semantics from `.` and reduces parser ambiguity.
- Allows future separator change by configuration instead of storage migration.
- Reuses existing metadata and authorization model to reduce implementation risk.

Cons:

- Requires explicit conversion rules between logical path and physical schema name.
- Authorization matching and identifier serialization become more complex.
- `list schema` remains flat in physical storage model.

#### Option B.1: Use `/` as logical separator

Examples:

- `A/B/C` as logical `HierarchicalSchema` path.
- Mapped physical schema name can still be stored as `A.B.C` (or encoded form).

Pros:

- Most intuitive as a path-like hierarchy.

Cons:

- High conflict risk with URL path routing and REST template matching.
- Requires stricter encode/decode handling in endpoint parameters.

#### Option B.2 (Recommended): Use `:` as logical separator

Examples:

- `A:B:C` as logical `HierarchicalSchema` path.
- Physical schema name remains mapped through conversion layer.

Pros:

- Better readability than escaping `.` in many clients and UI forms.
- Lower routing conflict risk than `/`.
- Easier to keep backward compatibility with existing non-nested schema handling.

Cons:

- Needs clear validation rule to avoid ambiguity with existing schema names containing `:`.

#### Option B.3: Use `::` as logical separator

Examples:

- `A::B::C` as logical `HierarchicalSchema` path.

Pros:

- Strong visual boundary between path segments.
- Reduces accidental split in user-entered names compared with single-character separator.

Cons:

- Slightly heavier user input and display.
- Requires stricter parser and escaping rules for edge cases.

## Design

### Identifier Rules

- Introduce logical identifier concept: `HierarchicalSchema`.
- `HierarchicalSchema` uses configurable separator in logic layer (`/`, `:`, or `::`).
- In this phase, `:` is the recommended separator to balance readability and compatibility.
- Schema name can still contain `.` in physical storage to keep model stable.
- When `:` is used as separator, segment values containing `:` must be percent-encoded.
- `%` must also be encoded as `%25` to avoid decode ambiguity.
- Encoding/decoding order:
  - Serialize: encode each segment first, then join with `:`.
  - Parse: split by `:`, then decode each segment.
- Decode is applied exactly once to avoid double-decoding issues.
- Add quote around schema names when needed to avoid parse ambiguity.
- Keep flat storage model and convert `HierarchicalSchema` path to physical schema name by mapping rules.
- `NameIdentifier` should support quoted `schema` names.
- In this phase, quoted identifier support is limited to `schema` only; other identifier parts keep existing parsing rules.

Examples:

- Nested namespace `A:B` maps to logical `HierarchicalSchema` path `A:B`.
- Nested namespace `A:B:C` maps to logical `HierarchicalSchema` path `A:B:C`.
- Logical `HierarchicalSchema` path is then converted to physical schema name through mapping rules.
- Namespace levels `["team:core", "sales"]` are serialized as `team%3Acore:sales`.
- Parsing `team%3Acore:sales` returns `["team:core", "sales"]`.
- For parsing ambiguity, use quoted schema name in identifier rendering, for example:
  - `metalake.catalog.'A.B'.table1`
  - `metalake.catalog.'A.B.C'.table2`
- In UI display, show logical path (for example `A:B:C`) for readability, but keep quoted form for parser-safe serialization.

According to URL encoding rules (RFC 3986), single quotes have no reserved purpose 
and must be percent-encoded if they appear in a URL component. The encoded form for a single quote is %27.

Example:
Invalid: https://example.com/path/to'O'reilly
Valid (encoded): https://example.com/path/to%27O%27reilly


### Iceberg REST Side Behavior

- **Create nested namespace**:
  - Creating `A.B.C` will create (or ensure existence of) three schemas in Gravitino: `A`, `A.B`, and `A.B.C`.
  - Set the created namespace owner as current user.
- **Drop nested namespace**: drop corresponding schema in Gravitino.
- **Rename nested namespace**: not needed because Iceberg REST does not support namespace rename.

### Gravitino Side Behavior

- `list schema` stays flat instead of nested.
- Gravitino does not provide a dedicated `list sub-schema` API.
- `list schema` returns all schemas in a flat result set, including nested schemas.
- Example: if schemas are `A`, `B`, `A.B`, `A.B.C`, the `list schema` result includes all four.
- Gravitino side does not support creating nested namespace directly.
- Gravitino side does not support deleting nested namespace directly.
- Gravitino side does not support modifying nested namespace directly (for example, renaming or altering properties).
- Existing schema/table APIs remain compatible with non-nested cases.

## Privileges and Authorization

- No new privilege type is introduced.
- Authorization follows nested namespace scope by logical `HierarchicalSchema` path and mapped schema name.
- Namespace privileges follow inheritance: privilege on parent namespace applies to child namespace.
- `create_schema` privilege is sufficient to create namespace.
- We cannot bind `create_schema` privilege under a namespace node.
- UI privilege granting is one usage scenario of this overall nested namespace solution.

Examples:

- Privilege on `A.B` applies to that specific scope.
- Privilege on `A` also applies to `A:B` (or other configured child path) based on the namespace inheritance rule.

## Code Snippets (Design-Level)

The following snippets are design-level examples to clarify how `HierarchicalSchema`
(`:` preferred) should be converted and consumed in key code paths.

### Snippet 1: Convert Iceberg namespace to logical path and physical schema

```java
// Example utility methods in IcebergRESTUtils (or a dedicated NamespacePathMapper)
public static String toHierarchicalSchemaPath(Namespace namespace, String separator) {
  // namespace.levels() = ["A", "B", "C"] -> "A:B:C"
  return String.join(separator, namespace.levels());
}

public static String toPhysicalSchemaName(String hierarchicalPath) {
  // Phase-1 compatible mapping: keep storage as flattened schema
  // "A:B:C" -> "A.B.C"
  return hierarchicalPath.replace(":", ".");
}
```

### Snippet 2: Namespace extraction in authorization interceptor

```java
// Example in IcebergMetadataAuthorizationMethodInterceptor
Namespace rawNamespace = RESTUtil.decodeNamespace(value);
String hierarchicalPath = HierarchicalSchemaUtil.toPath(rawNamespace, ":");
String schema = HierarchicalSchemaUtil.toPhysicalSchemaName(hierarchicalPath);

nameIdentifierMap.put(
    Entity.EntityType.SCHEMA,
    NameIdentifierUtil.ofSchema(metalakeName, catalog, schema));
```

### Snippet 3: Parent-scope authorization check

```java
// Example path inheritance for A:B:C
List<String> authzScopes = HierarchicalSchemaUtil.parentScopes("A:B:C", ":");
// Result: ["A", "A:B", "A:B:C"]
// Authorization passes if user has required privilege on any allowed parent scope by policy.
```

### Snippet 4: Create nested namespace in executor flow

```java
// For create namespace A:B:C, ensure parent schemas exist in physical model
for (String scope : HierarchicalSchemaUtil.parentScopes("A:B:C", ":")) {
  String schemaName = HierarchicalSchemaUtil.toPhysicalSchemaName(scope); // A, A.B, A.B.C
  // create schema if not exists
}
```

## Affected Classes

### Iceberg REST and namespace dispatch

- `iceberg/iceberg-rest-server/src/main/java/org/apache/gravitino/iceberg/service/IcebergRESTUtils.java`
- `iceberg/iceberg-rest-server/src/main/java/org/apache/gravitino/iceberg/service/dispatcher/IcebergNamespaceOperationDispatcher.java`
- `iceberg/iceberg-rest-server/src/main/java/org/apache/gravitino/iceberg/service/dispatcher/IcebergNamespaceOperationExecutor.java`
- `iceberg/iceberg-rest-server/src/main/java/org/apache/gravitino/iceberg/service/dispatcher/IcebergNamespaceEventDispatcher.java`

### Authorization interception

- `iceberg/iceberg-rest-server/src/main/java/org/apache/gravitino/server/web/filter/BaseMetadataAuthorizationMethodInterceptor.java`
- `iceberg/iceberg-rest-server/src/main/java/org/apache/gravitino/server/web/filter/IcebergMetadataAuthorizationMethodInterceptor.java`
- `iceberg/iceberg-rest-server/src/main/java/org/apache/gravitino/server/web/filter/LoadTableAuthzHandler.java`
- `iceberg/iceberg-rest-server/src/main/java/org/apache/gravitino/server/web/filter/RenameTableAuthzHandler.java`
- `iceberg/iceberg-rest-server/src/main/java/org/apache/gravitino/server/web/filter/RenameViewAuthzHandler.java`

### Identifier and metadata object mapping

- `api/src/main/java/org/apache/gravitino/NameIdentifier.java`
- `core/src/main/java/org/apache/gravitino/utils/NameIdentifierUtil.java`

### Tests

- `iceberg/iceberg-rest-server/src/test/java/org/apache/gravitino/server/web/filter/TestIcebergMetadataAuthorizationMethodInterceptor.java`
- `api/src/test/java/org/apache/gravitino/TestNameIdentifier.java`
- `core/src/test/java/org/apache/gravitino/utils/TestNameIdentifierUtil.java`

## Expected Changes

### 1) Namespace path mapping

- Add a dedicated conversion utility for `HierarchicalSchema` path:
  - Iceberg namespace levels -> logical path (preferred `:`).
  - Logical path -> physical schema name (phase-1 uses `.` flattened mapping).
- Add validation rules to reject ambiguous names (for example, raw schema names containing `:` when `:` is separator).

### 2) Authorization behavior

- In interceptor and handlers, stop treating the last namespace level as the only schema segment.
- Build schema identity from the full namespace path through conversion rules.
- Evaluate parent-scope inheritance using hierarchical logical scopes (`A`, `A:B`, `A:B:C`) before or during expression evaluation.

### 3) Namespace operation behavior

- `createNamespace` should ensure parent schemas exist for each hierarchical level.
- `dropNamespace` should target mapped physical schema and preserve existing non-nested behavior.
- `listNamespaces` should be compatible with the logical hierarchy while keeping current flat storage model.

### 4) Identifier compatibility

- Keep `NameIdentifier` external compatibility for existing dotted identifiers.
- Add schema-level rendering/parsing guidance for logical separator and quoted schema output where ambiguity exists.
- Keep change scope limited to schema handling in this phase to reduce regression risk for table/view/function paths.

### 5) Configuration and rollout

- Add a server-side config item for logical namespace separator with default `:`.
- Keep separator choices limited to `/`, `:`, `::` in this phase.
- Document fallback/rollback behavior by preserving physical schema mapping and existing APIs.

## Compatibility

- No metadata model migration required.
- Existing non-nested namespace behavior remains unchanged.
- Limiting quoted identifier parsing to `schema` reduces regression risk for catalog/table/view/function identifier parsing.

## Test Plan

- Unit tests for schema name parse/quote handling when name contains `.`.
- Unit/integration tests for Iceberg REST create/drop nested namespace mapping.
- Authorization tests for nested scope behavior (`A`, `A.B`, `A.B.C`).
- Regression tests for non-nested namespace authorization behavior.

