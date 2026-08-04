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

# Design of Metric View Support in Gravitino

## Background

Business metrics such as revenue, order count, and active users are shared semantic assets consumed by analytics, BI, and AI applications. When their definitions are kept only in individual semantic-layer tools or project files, discovery, ownership, version history, access control, and consistent reuse become fragmented. Gravitino therefore needs a governed metadata model that manages metric definitions alongside the data entities they reference.

Semantic-layer definitions are commonly authored and exchanged as YAML. That is convenient for authoring and interoperability, but a raw document does not provide Gravitino consumers with a typed API for datasets, relationships, fields, metrics, and AI context. This design introduces an OSI/Ossie-compatible structured representation while retaining the existing View lifecycle and governance model.

## Goals

- **Unified lifecycle.** Represent metric definitions as schema-scoped metadata and manage them through the existing View lifecycle.
- **Structured access.** Expose datasets, relationships, fields, metrics, AI context, and extensions through typed APIs.
- **Governance.** Apply View-level identity, authorization, ownership, audit, tags, policies, and version history to metric definitions.
- **Compatibility.** Preserve existing logical View behavior and provide explicit capability handling for connectors that do not support Metric Views.
- **Validation.** Define deterministic write-time checks and clear boundaries for catalog-dependent validation.

## Non-Goals

- **Non-OSI native models.** Compatibility with dbt, Cube, Databricks, Snowflake, or other non-OSI semantic definitions is outside this design.
- **Document authoring and conversion.** YAML parsing, formatting, conversion, and exact textual round trips are not server API contracts. External tools may provide best-effort stable serialization.
- **Compilation and execution.** Semantic query planning, SQL generation, engine execution, and engine-specific compatibility are separate work.
- **Materialization.** Metric caches, refresh policies, and materialized results are not defined here.
- **Continuous dependency maintenance.** Catalog-wide lineage, automatic revalidation after catalog changes, and transitive cycle analysis are not included.
- **Member-level authorization.** Datasets, fields, and metrics are governed as members of the enclosing Metric View rather than as independently authorized entities.

## Proposed Design

### Object Model and Constraints

A Metric View is a specialized use of the existing View object under a metalake, catalog, and schema. It does not introduce a new top-level metadata object.

```text
metalake.catalog.schema
  View (logical)
    SQLRepresentation
  View (metric)
    MetricRepresentation
```

- **Containment and governance.** The enclosing Metric View is the governed object. Datasets, relationships, fields, metrics, AI context, and extensions are members of its representation.
- **Semantic identity.** A logical View defines fixed SQL computation and fixed output columns. A Metric View defines query-time semantic choices, so the two are distinct kinds of definitions.
- **Namespace.** Logical and Metric Views share the same schema-level View namespace and name rules; same-name objects cannot coexist (see Storage and Connector Behavior for conflict resolution).
- **Representation.** A Metric View contains exactly one `MetricRepresentation`. It cannot contain a SQL representation, and alter requests that change a View between logical and metric semantics are rejected.
- **Lifecycle and columns.** Metric Views reuse View create, list, load, alter, drop, and version operations. Their `columns` collection is always empty because the output schema is selected at query time.

### Representation Model

The upstream OSI document places its specification version beside an array of semantic models. The abbreviated form is:

```yaml
version: 0.2.0.dev0
semantic_model:
  - name: sales_semantic_model
    datasets:
      - name: orders
        source: sales.mart.orders
```

Gravitino maps the root `version` to `MetricRepresentation.osiVersion` and one `semantic_model` item to `semanticModel`. A three-part OSI dataset source maps to a `NameIdentifier`. View identity and lifecycle remain in the surrounding View object.

```text
MetricRepresentation
  type: "metric"
  osiVersion: string
  semanticModel: MetricModel
```

The representation has three fields:

- `type`: The fixed value "metric" classifies the View as a Metric View.
- `osiVersion`: A required string identifying the OSI profile used to interpret and validate the model. The initial supported value is `0.2.0.dev0`.
- `semanticModel`: The stable, structured Gravitino model exposed through public APIs.
- A Metric View contains exactly one `MetricRepresentation`.
- Its `columns` array is empty.
- It cannot contain a SQL representation.
- Both `osiVersion` and `semanticModel` are required.

#### MetricModel Schema

The canonical model follows the pinned OSI `0.2.0.dev0` profile. Fields marked with `?` are optional; all other fields are required. Names below use OSI wire-format spelling, while language bindings use idiomatic accessor names.

```text
MetricModel
  name: string
  description?: string
  ai_context?: AIContext
  datasets: Dataset[1..*]
  relationships?: Relationship[]
  metrics?: Metric[]
  custom_extensions?: CustomExtension[]
```

- `MetricModel` contains at least one `Dataset`.
- Names in each collection follow the uniqueness and reference rules defined with the nested types below.

Dataset and field definitions:

```text
Dataset
  name: string
  source: NameIdentifier
  primary_key?: string[]
  unique_keys?: string[][]
  description?: string
  ai_context?: AIContext
  fields?: Field[]
  custom_extensions?: CustomExtension[]

Field
  name: string
  expression: Expression
  dimension?: Dimension
  label?: string
  description?: string
  ai_context?: AIContext
  custom_extensions?: CustomExtension[]
```

- `Dataset` names are unique within `MetricModel`.
- `Field` names are unique within each `Dataset`.
- Internal field references resolve within the model.
- Each `source` is a `NameIdentifier` with namespace `[catalog, schema]`; `source.name` identifies a `Table` or `View`, and the enclosing object supplies the metalake.
- For `Table` and logical `View` sources, Gravitino validates columns explicitly declared in `primary_key`, `unique_keys`, `from_columns`, and `to_columns` against the source schema. It does not infer source-column references from field or metric expressions.
- Metric View sources validate direct existence only.
- Inline query sources are not supported; register the query as a logical View and reference that View through a `NameIdentifier`.
- Catalog unavailability is treated as a retriable validation failure.

Relationship and metric definitions:

```text
Relationship
  name: string
  from: string
  to: string
  from_columns: string[1..*]
  to_columns: string[1..*]
  ai_context?: AIContext
  custom_extensions?: CustomExtension[]

Metric
  name: string
  expression: Expression
  description?: string
  ai_context?: AIContext
  custom_extensions?: CustomExtension[]
```

- `Relationship` and `Metric` names are unique within `MetricModel`.
- Each relationship endpoint references an existing `Dataset`.
- `from_columns` and `to_columns` are non-empty and have equal length.
- Each metric expression satisfies the `Expression` rules below.

Supporting types:

```text
Expression
  dialects: DialectExpression[1..*]

DialectExpression
  dialect: Dialect
  expression: string

Dimension
  is_time?: boolean

AIContext = string | { instructions?: string, synonyms?: string[],
                       examples?: string[], ... }

CustomExtension
  vendor_name: string
  data: string

Dialect = "ANSI_SQL" | "SNOWFLAKE" | "MDX" | "TABLEAU"
          | "DATABRICKS" | "MAQL" | "BIGQUERY"
```

- Each `Expression` contains at least one `DialectExpression`.
- Every dialect entry uses a supported `Dialect`.
- Every dialect entry has a non-empty `expression`.
- `Dimension`, `AIContext`, and `CustomExtension` values satisfy the structures above.

The required `MetricModel.name` is independent of the enclosing View name. This preserves semantic-model identity across imports and View renames.

Every supported `custom_extensions` array is retained losslessly. Unknown standardized fields are rejected until the declared OSI profile supports them.

Create and alter reject an unsupported `osiVersion` before validating or persisting `semanticModel`. The REST API returns HTTP 400 with the requested and supported versions, and creates no View or View version.

Once an `osiVersion` has been accepted for persistence by a released Gravitino version, later releases retain read support for it. Write support may be removed only through a major-version compatibility change with a documented migration path. Unknown stored versions fail explicitly and are never silently coerced.

**Implementation note:**

Gravitino selects a versioned OSI validation profile by `osiVersion`. Each profile pins the upstream JSON Schema and adds only version-specific projection or semantic rules beyond the shared validator. Compatible OSI versions may reuse the same validator implementation, while incompatible changes require a new profile. Every new OSI version must still be explicitly registered in a Gravitino release; unregistered versions are rejected. Adapters required to read previously accepted versions are retained for backward compatibility.

- All representation and model checks run on `create` and `alter` before a View or View version is persisted.
- Validation checks direct references only.
- Transitive dependency and cycle correctness are not checked; a cyclic definition may be persisted and later rejected by a downstream consumer.
- Catalog changes do not trigger automatic revalidation.
- Catalog-wide revalidation is excluded because it would require a dependency index and potentially global impact analysis.

### Usage

Metric Views reuse the existing View lifecycle. Their columns are always empty, and create, list, load, alter, and drop operations use the existing View APIs.

#### Supported Alter Operations

Metric Views support the existing `ViewChange` operations:

- `rename`: Renames the enclosing View only; `MetricModel.name` is unchanged. The target name must be available in the shared View namespace.
- `setProperty`: Adds or replaces a View-level property.
- `removeProperty`: Removes a View-level property.
- `replaceView`: Atomically replaces the View body. The `columns` must remain empty, exactly one `MetricRepresentation` must remain, and changing between metric and logical semantics is rejected.

Member-level patch operations are not supported; changes to datasets, relationships, fields, or metrics require replacing the complete `MetricModel`.

Metric Views do not use `defaultCatalog` or `defaultSchema` because dataset sources use `NameIdentifier`; both values must be `null` in create and `replaceView` requests.

#### Java API

The Java API uses immutable builders for the structured definition and the existing ViewCatalog lifecycle methods:

```java
NameIdentifier ident = NameIdentifier.of("mart", "sales_metrics");
Dataset orders =
    Dataset.builder()
        .withName("orders")
        .withSource(NameIdentifier.of("sales", "mart", "orders"))
        .build();

MetricModel model =
    MetricModel.builder()
        .withName("sales_semantic_model")
        .withDatasets(List.of(orders))
        .build();

MetricRepresentation representation =
    MetricRepresentation.builder()
        .withOsiVersion("0.2.0.dev0")
        .withSemanticModel(model)
        .build();

View created =
    catalog.createMetricView(
        ident, "Sales metric definitions", List.of(representation),
        null, null, Map.of());
```

```java
View loaded = catalog.loadView(ident);
NameIdentifier[] views = catalog.listViews(Namespace.of("mart"));

MetricModel updatedModel =
    MetricModel.builder()
        .withName("sales_semantic_model")
        .withDescription("Updated sales model")
        .withDatasets(List.of(orders))
        .build();
MetricRepresentation updatedRepresentation =
    MetricRepresentation.builder()
        .withOsiVersion("0.2.0.dev0")
        .withSemanticModel(updatedModel)
        .build();

View updated =
    catalog.alterView(
        ident,
        ViewChange.replaceView(
            new Column[0],
            new Representation[] {updatedRepresentation},
            null, null, "Updated sales metric definitions"));

boolean dropped = catalog.dropView(ident);
```

#### REST API

REST uses the existing View resources. Create supplies an empty columns array and one Metric representation:

```http
POST /metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/views
{
  "name": "sales_metrics",
  "comment": "Sales metric definitions",
  "columns": [],
  "representations": [
    {
      "type": "metric",
      "osiVersion": "0.2.0.dev0",
      "semanticModel": {
        "name": "sales_semantic_model",
        "datasets": [
          { "name": "orders", "source": { "namespace": ["sales", "mart"], "name": "orders" } }
        ]
      }
    }
  ]
}
```

List, load, alter, and drop use the same resource:

```http
GET /metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/views
GET /metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/views/sales_metrics

PUT /metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/views/sales_metrics
{
  "updates": [
    {
      "@type": "replaceView",
      "columns": [],
      "representations": [
        {
          "type": "metric",
          "osiVersion": "0.2.0.dev0",
          "semanticModel": {
            "name": "sales_semantic_model",
            "description": "Updated sales model",
            "datasets": [
              { "name": "orders", "source": { "namespace": ["sales", "mart"], "name": "orders" } }
            ]
          }
        }
      ],
      "comment": "Updated sales metric definitions"
    }
  ]
}

DELETE /metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/views/sales_metrics
```

### Storage and Connector Behavior

- **Source of truth.** Metric Views are stored only in the Gravitino EntityStore. Logical Views remain stored by their underlying catalogs.
- **Listing.** The server merges authorized catalog-backed logical Views with authorized Gravitino-managed Metric Views into the existing View listing.
- **Connector capability.** A connector that does not support Metric Views filters them from `listViews` and returns an explicit unsupported-Metric-View error for a direct `loadView`. Generic REST and Java View APIs continue to expose them.
- **Namespace conflicts.** Create checks both storage sources. If an external client later creates a same-name logical View directly in the catalog, list and load report a conflict and select neither object. The external operation must rename or remove its object; ownership, versions, tags, and policies remain attached to the Gravitino Metric View and never transfer.
- **Persistence and history.** Each stored View version contains `type`, `osiVersion`, and the structured `semanticModel`. Every successful alter creates an immutable View version. Existing logical View records require no migration.

### Authorization and Governance

- Reuse the existing View privileges for create, select, alter, drop, and owner management, together with View-level audit, tag, and policy behavior.
- Apply governance metadata to the enclosing Metric View and its immutable versions; semantic-model members do not have independent privileges.
- Catalog reference validation proves that a source exists, but it does not grant data access to the caller.
- A compiler or execution engine must authorize referenced data separately using the effective query caller.

## Development Plan

- **Add API types.** Implement `MetricRepresentation` and structured model DTOs in Java, REST, and Python without coupling `MetricModel.name` to the enclosing View name, with compatibility fixtures.
- **Implement lifecycle and persistence.** Add create, load, alter, drop, immutable versioning, and EntityStore persistence while preserving the shared View namespace.
- **Implement catalog exposure.** Merge View listings, add connector capability handling, and enforce deterministic namespace-conflict behavior.
- **Implement OSI version profiles and validation.** Pin OSI `0.2.0.dev0` as the initial profile, register supported versions, reject unregistered versions, retain read support for previously accepted versions, and add document-local consistency and direct `NameIdentifier` source and column checks.
- **Integrate governance.** Reuse View authorization, ownership, audit, tag, and policy paths and verify that conflicts cannot redirect governance operations.
- **Complete verification and documentation.** Add end-to-end tests for lifecycle, versioning, validation, merged listing, connector behavior, governance, and compatibility; replace the unreleased document-representation prototype before publication.
