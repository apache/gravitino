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
- Reuses existing metadata and authorization model to reduce implementation risk.

Cons:

- Requires explicit conversion rules between logical path and physical schema name.
- Authorization matching and identifier serialization become more complex.
#### Option B Separator (Fixed): Use `:` as logical separator

Examples:

- `A:B:C` as logical `HierarchicalSchema` path.
- Physical schema name remains mapped through conversion layer.

Pros:

- Better readability than escaping `.` in many clients and UI forms.
- Lower routing conflict risk than `/`.
- Easier to keep backward compatibility with existing non-nested schema handling.

Cons:

- Needs clear validation rule to avoid ambiguity with existing schema names containing `:`.

## Design

### Identifier Rules

- Introduce logical identifier concept: `HierarchicalSchema`.
- `HierarchicalSchema` uses fixed separator `:` in logic layer.
- `:` is mandatory in this design, not configurable.
- Schema name can still contain `.` in physical storage to keep model stable.
- When `:` is used as separator, segment values containing `:` must be percent-encoded.
- `%` must also be encoded as `%25` to avoid decode ambiguity.
- Encoding/decoding order:
  - Serialize: encode each segment first, then join with `:`.
  - Parse: split by `:`, then decode each segment.
- Decode is applied exactly once to avoid double-decoding issues.
- Keep flat storage model and convert `HierarchicalSchema` path to physical schema name by mapping rules.
- Identifier rendering rule:
  - Use encoded `HierarchicalSchema` path directly in schema position.
  - Do not rely on single-quote wrapping for schema disambiguation in this phase.

Examples:

- Nested namespace `A:B` maps to logical `HierarchicalSchema` path `A:B`.
- Nested namespace `A:B:C` maps to logical `HierarchicalSchema` path `A:B:C`.
- Logical `HierarchicalSchema` path is then converted to physical schema name through mapping rules.
- Namespace levels `["team:core", "sales"]` are serialized as `team%3Acore:sales`.
- Parsing `team%3Acore:sales` returns `["team:core", "sales"]`.
- Identifier rendering example:
  - `metalake.catalog.A:B.table1`
  - `metalake.catalog.team%3Acore:sales.table2`
- In UI display, show logical path (for example `A:B:C`) for readability.
- HTTP transport rule:
  - Namespace values in URL/query/body must follow RFC 3986 percent-encoding when needed.
  - Example: namespace path `team%3Acore:sales` should be URL-encoded as
    `team%253Acore%3Asales` when put into a URL query component.


### Iceberg REST Side Behavior

- **Create nested namespace**:
  - Creating `A:B:C` will create (or ensure existence of) three schemas in Gravitino: `A`, `A.B`, and `A.B.C`.
  - Set the created namespace owner as current user.
- **Update nested namespace**:
  - Support updating namespace properties through mapped schema operations.
  - Property update is applied to the mapped target namespace scope.
- **Drop nested namespace**: drop corresponding schema in Gravitino.
- **Rename nested namespace**: not needed because Iceberg REST does not support namespace rename.

### Gravitino Side Behavior

- `list schema` should express nested hierarchy semantics for users.
- `list schema` REST API (GET `/metalakes/{metalake}/catalogs/{catalog}/schemas`) should support an
  optional query parameter `parentHierarchicalSchema`.
  - When `parentHierarchicalSchema` is not provided, return only top-level schemas (first layer).
  - When `parentHierarchicalSchema` is provided, return only the direct child schemas under the
    given parent (next layer), instead of the full subtree.
  - `parentHierarchicalSchema` value follows the same logical `HierarchicalSchema` encoding
    rules as described in `Identifier Rules` (segment percent-encoding, and then RFC 3986
    percent-encoding for transport in a query component).
- Gravitino does not provide a dedicated `list sub-schema` API; hierarchy is expressed via
  `list schema`/`list namespaces` results.
- Example: for schemas `A`, `B`, `A:B`, `A:B:C`, hierarchy view is `A -> A:B -> A:B:C` and `B`;
  root listing returns `A` and `B`, and querying parent `A` returns `A:B`.
- To make nested semantics explicit, `list namespaces` should express parent-child relationships
  (hierarchical view) even when underlying storage is flat.
- Example hierarchical view from flat schemas: `A` -> `A:B` -> `A:B:C`, and `B` as another root.
- This list-level hierarchical expression is the primary semantic model for users, reducing
  ambiguity caused by one request creating multiple physical schema objects.
- Gravitino server REST supports namespace create/update/drop operations for nested namespace
  workflows, aligned with Iceberg REST behavior.
- Existing schema/table APIs remain compatible with non-nested cases.

## Privileges and Authorization

- Authorization follows nested namespace scope by logical `HierarchicalSchema` path and mapped schema name.
- Namespace privileges follow inheritance: privilege on parent namespace applies to child namespace.
- UI privilege granting is one usage scenario of this overall nested namespace solution.

### Option P1 (Recommended): Extend `create_schema` semantics

- Keep current privilege model and do not add a new privilege type.
- Clarify `create_schema` as container-scoped capability: permission on parent namespace allows
  creating direct child namespace under that scope.
- Example: `create_schema` on `A` allows creating `A:B`, and `create_schema` on `A:B` allows
  creating `A:B:C`.

Pros:

- Lowest implementation and migration cost.
- Reuses existing authorization model and UI privilege workflow.
- Keeps backward compatibility for current grants.

Cons:

- Semantics are less explicit because `create_schema` now covers both normal schema creation and
  nested namespace creation.

### Option P2: Introduce a dedicated nested-namespace privilege

- Add a new privilege (for example `create_nested_namespace`) for creating child namespaces.
- Keep `create_schema` semantics unchanged for existing schema creation behavior.
- Evaluate both privileges independently in authorization expression where needed.

Pros:

- Clearer and more explicit permission model.
- Better long-term extensibility for fine-grained namespace governance.

Cons:

- Requires privilege model/API/UI updates and migration planning.
- Increases operational complexity for users and administrators.

### Selection Guidance

- Phase-1 recommends Option P1 for faster delivery and lower risk.
- Option P2 can be considered in a later phase if stronger permission separation is required.

Examples:

- Privilege on `A:B` applies to that specific scope.
- Privilege on `A` also applies to `A:B` (or other configured child path) based on the namespace inheritance rule.

## Code Snippets (Design-Level)

The following snippets are design-level examples to clarify how `HierarchicalSchema`
(`:` preferred) should be converted and consumed in key code paths.

### Snippet 1: Convert Iceberg namespace to logical path and physical schema

```java
// Example utility methods in IcebergRESTUtils (or a dedicated HierarchicalSchemaUtil)
private static String encodeSegment(String raw) {
  // Encode % first, then separator-related characters.
  return raw.replace("%", "%25").replace(":", "%3A");
}

private static String decodeSegment(String encoded) {
  // Decode exactly once; reject malformed escape sequences in real implementation.
  return encoded.replace("%3A", ":").replace("%25", "%");
}

public static String serializeHierarchicalPath(String[] levels) {
  // ["team:core", "sales"] -> "team%3Acore:sales"
  return Arrays.stream(levels).map(HierarchicalSchemaUtil::encodeSegment).collect(Collectors.joining(":"));
}

public static String[] parseHierarchicalPath(String path) {
  // "team%3Acore:sales" -> ["team:core", "sales"]
  return Arrays.stream(path.split(":", -1)).map(HierarchicalSchemaUtil::decodeSegment).toArray(String[]::new);
}

public static String toPhysicalSchemaName(String hierarchicalPath) {
  // Phase-1 mapping keeps flat schema storage: "A:B:C" -> "A.B.C"
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
List<String> authzScopes = HierarchicalSchemaUtil.parentScopes("A:B:C");
// Result: ["A", "A:B", "A:B:C"]
// Authorization passes if user has required privilege on any allowed parent scope by policy.
```

### Snippet 4: Create nested namespace in executor flow

```java
// For create namespace A:B:C, ensure parent schemas exist in physical model
for (String scope : HierarchicalSchemaUtil.parentScopes("A:B:C")) {
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
- `updateNamespace` should support property updates for mapped namespace scope.
- `dropNamespace` should target mapped physical schema and preserve existing non-nested behavior.
- `listSchemas` should accept an optional query parameter `parentHierarchicalSchema`.
  - When absent, return only top-level schemas (first layer).
  - When present, return only direct children under the given parent (next layer).
- `listNamespaces` should return hierarchy-aware semantics (or equivalent parent-child expression)
  while keeping current flat storage model.

### 4) Identifier compatibility

- Keep `NameIdentifier` external compatibility for existing dotted identifiers.
- Add schema-level rendering/parsing guidance for logical separator and quoted schema output where ambiguity exists.
- Keep change scope limited to schema handling in this phase to reduce regression risk for table/view/function paths.

## Compatibility

- No metadata model migration required.
- Existing non-nested namespace behavior remains unchanged.
- Limiting quoted identifier parsing to `schema` reduces regression risk for catalog/table/view/function identifier parsing.

## Test Plan

- Unit tests for schema name parse/quote handling when name contains `.`.
- Unit/integration tests for Iceberg REST create/update/drop nested namespace mapping.
- Authorization tests for nested scope behavior (`A`, `A:B`, `A:B:C`).
- Regression tests for non-nested namespace authorization behavior.

