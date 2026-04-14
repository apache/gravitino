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
- Introduce a new metadata object (for example `SubNamespace`) in this phase.

## Solution Options

### Option A: Add a new metadata object `SubNamespace`

Use a new metadata object `SubNamespace` to replace schema

Catalog -> SubNamespace a -> SubNamespace a.b -> Table a.b.c
                          -> SubNamespace a.c -> SubNamespace a.c.d -> Table a.c.d.e

Pros:

- Clearer concept modeling.

Cons:

- Large refactor across metadata model, API, authorization, and UI.

### Option B (Chosen): Use `Schema` with dot-separated name

Pros:

- Simple implementation.
- Reuses existing metadata and authorization model.
- In most cases, `schema` is enough for namespace management.
- Iceberg nested namespace is a special case, so avoiding a large refactor is more cost-effective.

Cons:

- Cannot naturally express nested tree structure in Gravitino storage.
- `list schema` remains flat (for example: `A`, `B`, `A.B`).

## Design

### Identifier Rules

- Schema name can contain `.`.
- Add quote around schema names when needed to avoid parse ambiguity.
- Keep flat storage model and use dot-joined schema name as namespace mapping.
- `NameIdentifier` should support quoted `schema` names.
- In this phase, quoted identifier support is limited to `schema` only; other identifier parts keep existing parsing rules.

Examples:

- Nested namespace `A.B` maps to schema name `A.B`.
- Nested namespace `A.B.C` maps to schema name `A.B.C`.
- For parsing ambiguity, use quoted schema name in identifier rendering, for example:
  - `metalake.catalog.'A.B'.table1`
  - `metalake.catalog.'A.B.C'.table2`
- In UI display, show schema as `A.B` (human readable), but keep quoted form for parser-safe serialization.

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
- Gravitino side does not support creating nested namespace directly.
- Gravitino side does not support deleting nested namespace directly.
- Gravitino side does not support modifying nested namespace directly (for example, renaming or altering properties).
- Existing schema/table APIs remain compatible with non-nested cases.

## Privileges and Authorization

- No new privilege type is introduced.
- Authorization follows nested namespace scope by mapped schema name.
- Namespace privileges follow inheritance: privilege on parent namespace applies to child namespace.
- `create_schema` privilege is sufficient to create namespace.
- We cannot bind `create_schema` privilege under a namespace node.
- UI privilege granting is one usage scenario of this overall nested namespace solution.

Examples:

- Privilege on `A.B` applies to that specific scope.
- Privilege on `A` also applies to `A.B` based on the namespace inheritance rule.

## Compatibility

- No metadata model migration required.
- Existing non-nested namespace behavior remains unchanged.
- Limiting quoted identifier parsing to `schema` reduces regression risk for catalog/table/view/function identifier parsing.

## Test Plan

- Unit tests for schema name parse/quote handling when name contains `.`.
- Unit/integration tests for Iceberg REST create/drop nested namespace mapping.
- Authorization tests for nested scope behavior (`A`, `A.B`, `A.B.C`).
- Regression tests for non-nested namespace authorization behavior.

