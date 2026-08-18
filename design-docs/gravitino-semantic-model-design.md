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

# Design of Semantic Model Support in Gravitino

## Background

Business definitions such as revenue, active users, dimensions, and dataset relationships are
shared semantic assets consumed by analytics, BI, and AI applications. When these definitions live
only in individual tools or project files, their identity, discovery, ownership, access control,
auditability, and reuse become fragmented. Gravitino needs a governed and engine-neutral metadata
object for managing these definitions alongside the data entities they reference.

Unlike a logical View, which defines a fixed query and output schema, a semantic model defines
datasets, relationships, dimensions, and metrics that consumers combine at query time. Gravitino
therefore models it as a dedicated metadata object rather than a relational View.

Apache Ossie, formerly Open Semantic Interchange (OSI), defines a vendor-neutral structured
`SemanticModel`.

Gravitino's design direction for semantic metadata has evolved from preserving opaque YAML
documents, to exposing a strongly typed model, and now to managing it as an independent first-class
metadata object. This progression reflects the growing importance of semantics in Gravitino's
metadata and governance model. This design therefore adopts an Ossie-compatible analytical model as
a new schema-scoped Gravitino entity.

## Goals

1. **Lifecycle.** Manage Ossie-compatible Semantic Models as first-class, schema-scoped metadata
   with stable identity and a dedicated lifecycle.
2. **Governance.** Apply authorization, ownership, audit, events, tags, and policies to Semantic
   Models.
3. **Validation.** Define deterministic write-time checks and clear boundaries for catalog-dependent
   validation.
4. **Interoperability.** Define a stable Ossie-compatible contract that can evolve with Apache Ossie.
5. **User experience.** Support discovery and lifecycle management of Semantic Models as a distinct
   schema-scoped object category in the Gravitino UI.

## Non-Goals

1. **Query compilation and execution.** Semantic query planning, SQL generation, engine execution,
   and engine-specific query syntax are separate work.
2. **Non-Ossie native models.** Native dbt, Cube, Databricks, Snowflake, or other vendor-specific
   semantic definitions are not modeled by this design.
3. **Document authoring and conversion.** YAML or JSON import, export, formatting, conversion, and
   exact textual round trips are not server API contracts.
4. **Ontology management.** Apache Ossie Ontology definitions and mappings are separate metadata
   concepts and require their own design.
5. **Materialization.** Metric caches, refresh policies, pre-aggregations, and materialized results
   are not defined here.
6. **Member-level authorization.** Datasets, fields, relationships, and metrics are governed as
   members of their enclosing Semantic Model rather than as independent securable objects.

## Proposal

### Object Model and Namespace

`SemanticModel` is a new metadata entity under a schema, alongside Tables, Views, Functions, and
other schema-scoped objects.

```text
metalake
  catalog
    schema
      Table
      View
      SemanticModel
```

The implementation introduces `MetadataObject.Type.SEMANTIC_MODEL` and
`Entity.EntityType.SEMANTIC_MODEL`. Public Java types reside in a dedicated semantic package rather
than the relational View package.

- **Identity.** The Semantic Model entity name maps to the Ossie `SemanticModel.name` field when
  serialized. No separate nested model name or identity is stored.
- **Scope.** A Semantic Model belongs to one schema. Its fully qualified identity is
  `metalake.catalog.schema.semanticModel`.
- **Namespace.** Semantic Models have an independent typed namespace. A Semantic Model may have the
  same name as a Table, View, Function, or another entity type in the same schema, but two Semantic
  Models with the same name cannot coexist.
- **Source of truth.** Semantic Models are always managed by Gravitino and are never persisted in an
  underlying catalog.
- **One model per entity.** One Ossie `semantic_model` item maps to one Gravitino Semantic Model. An
  external document containing multiple items maps to multiple entities.

The term `SemanticModel` means an **analytical semantic model** compatible with the Ossie Core
specification. It is not an umbrella for every semantic artifact. The
[Ossie Ontology specification](https://github.com/apache/ossie/blob/88e0011148283302c9a04cd0287e00e0b9d87354/ontology/ontology.json)
defines Ontology as a separate document and maps logical semantic models to ontology concepts.
Gravitino should likewise introduce a separate `Ontology` entity and an explicit binding or mapping
contract if that capability is added later. Such a binding should reference the stable identities of
the Ontology and Semantic Model rather than embed a second copy of either definition.

`SemanticModel` is a separate metadata type and lifecycle from the existing Gravitino `Model`, which
represents an ML model artifact.

### Semantic Model Contract

The API exposes a structured, immutable model. The initial contract follows the
[Apache Ossie Core schema pinned at commit `88e0011`](https://github.com/apache/ossie/blob/88e0011148283302c9a04cd0287e00e0b9d87354/core-spec/osi-schema.json),
whose declared specification version is `0.2.0.dev0`. Fields marked with `?` are optional; all other
fields are required. Language-specific APIs follow their normal naming conventions, while this
document uses Ossie field names for comparison with the upstream schema.

The Gravitino entity combines common metadata with the Ossie-compatible definition:

```text
SemanticModel
  name: string
  comment?: string
  ai_context?: AIContext
  datasets: Dataset[1..*]
  relationships?: Relationship[]
  metrics?: Metric[]
  custom_extensions?: CustomExtension[]
  properties: map<string, string>
  audit_info: AuditInfo
```

- `name` is the Gravitino entity name and maps to Ossie `SemanticModel.name`.
- `comment` follows the common Gravitino entity convention and maps to Ossie
  `SemanticModel.description`.
- `properties` stores Gravitino-specific metadata and is not part of an exported Ossie model.
- `custom_extensions` is part of the semantic definition and is preserved for Ossie interchange.
- Collection order is preserved so consumers can produce stable serialized output.

#### Datasets and Fields

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
  datatype?: DataType
  ai_context?: AIContext
  custom_extensions?: CustomExtension[]
```

- Dataset names are unique within a Semantic Model. Field names are unique within each Dataset.
- `source` is a three-part `NameIdentifier`. For Ossie interchange, Gravitino separates its catalog,
  schema, and entity segments with `.`. A segment containing `.` or a backtick is enclosed in
  backticks, and embedded backticks are escaped by doubling them. For example, the segments
  `sales.eu`, `mart`, and `orders` are encoded as `` `sales.eu`.mart.orders ``. Import uses the same
  quote-aware grammar. The request already identifies the metalake, so cross-catalog references
  within that metalake are allowed while cross-metalake references are not.
- A source must resolve to either a Table or a logical View. The semantic definition does not need
  to declare which of those two types it references; validation succeeds when either entity exists.
- Apache Ossie does not currently define cross-model dataset references. Therefore, a Semantic Model
  is not a valid Dataset source; cross-model composition is deferred until an explicit compatible
  reference contract is defined.
- Inline query sources are not supported. A query-backed source must first be created as a logical
  View and then referenced by `NameIdentifier`.
- For Table and logical View sources, every column named by `primary_key`, `unique_keys`,
  `from_columns`, or `to_columns` must exist in the source metadata. Validation rejects the
  definition when column metadata is unavailable or a named column is missing; Gravitino does not
  infer these references from free-form field or metric expressions.

#### Relationships and Metrics

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
  datatype?: DataType
  ai_context?: AIContext
  custom_extensions?: CustomExtension[]
```

- Relationship and Metric names are unique within a Semantic Model.
- Each relationship endpoint names a Dataset in the same Semantic Model.
- `from_columns` and `to_columns` are non-empty, have equal lengths, and name columns exposed by
  their respective Dataset sources.
- Metrics may reference fields and datasets in the same Semantic Model. Cross-model metric
  references are not defined by this contract.

#### Supporting Types

```text
Expression
  dialects: DialectExpression[1..*]

DialectExpression
  dialect: string
  expression: string

Dimension
  is_time?: boolean

AIContext = string | AIContextObject

AIContextObject
  instructions?: string
  synonyms?: string[]
  examples?: string[]
  additional properties: allowed and retained losslessly

CustomExtension
  vendor_name: string
  data: string

DataType = "String" | "Integer" | "Decimal" | "Float" | "Boolean"
           | "Date" | "Time" | "DateTime" | "DateTimeTz" | "Opaque"
```

- `DataType` is an Ossie-derived logical type vocabulary and is deliberately independent of
  `org.apache.gravitino.rel.types.Type`. Java enum constants use upper snake case, such as
  `DataType.DECIMAL`, and serialize to the exact Ossie wire values, such as `"Decimal"`. Gravitino
  does not infer or convert these values from source column types.
- Every Expression has at least one DialectExpression.
- Dialect identifiers are non-empty strings. `ANSI_SQL`, `SNOWFLAKE`, `MDX`, `TABLEAU`,
  `DATABRICKS`, `MAQL`, and `BIGQUERY` are well-known values exposed by the Java API through
  `org.apache.gravitino.semantic.Dialects`; other identifiers are accepted and preserved without
  normalization, translation, or fallback. Dialect identifiers are compared exactly and
  case-sensitively, so `trino` and `TRINO` are distinct.
- Each dialect identifier appears at most once in an Expression and each expression string is
  non-empty.
- Unknown model fields are rejected where the pinned Ossie schema sets `additionalProperties` to
  `false`.
- Every supported `custom_extensions` array and every additional AI-context property is retained
  losslessly.
- The Java API represents `AIContext` as an immutable wrapper containing exactly one `String` or
  `AIContextObject`, created through overloaded `AIContext.of` factories. `AIContextObject` exposes
  unknown JSON-compatible fields through `Map<String, Object> additionalProperties`; the selected
  variant and all standard and additional fields participate in equality and JSON round-tripping.

#### Validation

Create and alter validate the complete candidate object before persistence. Validation is atomic:
if any check fails, no change is persisted.

1. **Contract validation.** Check required fields, collection cardinality, enum values, string
   constraints, and the pinned Ossie structure.
2. **Model-local validation.** Check name uniqueness, relationship endpoints, referenced fields,
   key shapes, and dialect uniqueness without consulting a catalog.
3. **Catalog validation.** Resolve every Dataset source and validate explicitly named source
   columns. Validation uses the caller's authorization context so it does not disclose metadata the
   caller cannot access.

Invalid definitions return `400`. Missing sources or source columns are invalid definitions. A
connection failure while accessing a source catalog returns `502` with `CONNECTION_FAILED_CODE`.
Authorization failures return `403`.

Catalog changes do not automatically revalidate stored Semantic Models. A later source rename,
drop, or schema change can therefore leave a previously valid definition with an unavailable
reference. Automatic revalidation would require a durable dependency index and cross-catalog impact
analysis; it is outside this design. Loads continue to return the stored definition, while consumers
must handle unavailable sources when they compile or use it.

Gravitino does not validate the execution semantics of expressions, transitive logical View
dependencies, fanout correctness, or engine compatibility. Those checks require a compiler or query
engine and remain outside the metadata write path.

#### Ossie Compatibility

The public API is a Gravitino contract derived from Ossie, not a stored YAML or JSON document. For
compatibility validation, the server projects a candidate object into an in-memory Ossie document:

```yaml
version: 0.2.0.dev0
semantic_model:
  - name: sales_model
    description: Governed sales definitions
    datasets:
      - name: orders
        source: sales.mart.orders
```

The projection maps the entity name to `SemanticModel.name`, `comment` to `description`, and encodes
each source `NameIdentifier` using the reversible source grammar above. The request path supplies the
metalake and is not serialized into Ossie.

The request path does not invoke the upstream Python validator or create an intermediate YAML
string. The implementation validates the in-memory projection against a Gravitino profile derived
from the bundled copy of the pinned JSON Schema, then runs Gravitino-specific model-local and
catalog checks. The profile preserves the pinned structural constraints while treating dialect
identifiers as open non-empty strings. Upstream validation tools are used only in compatibility
fixtures that contain Ossie-defined dialects.

The entity carries no per-model Ossie specification version. Each Gravitino release pins one exact
upstream schema commit and defines the supported read and write contract. Updating that schema
requires a Gravitino code change, compatibility fixtures, and storage read tests. If a future
incompatible Ossie contract must coexist with the current one, explicit per-entity versioning
requires a separate design; it should not be inferred from `custom_extensions`.

### API and Lifecycle

`Catalog.asSemanticModelCatalog()` is supported only for `Catalog.Type.RELATIONAL`; other catalog
types throw `UnsupportedOperationException`. Because definitions are stored by Gravitino, this
support does not depend on whether the underlying relational connector implements a semantic-model
capability.

```text
SemanticModelCatalog
  listSemanticModels(namespace): NameIdentifier[]
  loadSemanticModel(identifier): SemanticModel
  createSemanticModel(identifier, comment, definition, properties): SemanticModel
  alterSemanticModel(identifier, changes...): SemanticModel
  dropSemanticModel(identifier): boolean
```

Expected exceptions include `NoSuchSemanticModelException`,
`SemanticModelAlreadyExistsException`, and `InvalidSemanticModelException`.
`listSemanticModels` returns identifiers rather than complete definitions so listing a schema does
not transfer every model body. Callers use `loadSemanticModel` to retrieve selected models.

#### Alter Semantics

Supported `SemanticModelChange` operations are:

- `rename`: Changes the entity name and therefore the Ossie name produced by future serialization.
- `updateComment`: Replaces the model comment.
- `setProperty` and `removeProperty`: Update Gravitino-specific properties.
- `replaceDefinition`: Atomically replaces AI context, datasets, relationships, metrics, and custom
  extensions.

Each alter request applies all changes atomically to the current Semantic Model. Rename retains the
stable entity ID. Owner, tag, and policy changes use their existing governance stores and remain
outside `SemanticModelChange`.

Fine-grained member patch operations are not included. Datasets, relationships, fields, and metrics
have interdependent validation rules, so replacing the complete definition provides a clear atomic
contract. Additional typed changes can be added later without changing stored identity.

#### Events

Semantic Model operations use Gravitino's existing listener framework. List, load, create, alter,
and drop emit corresponding pre, success, and failure events. Rename is represented as an alter
event. Event payloads follow existing Gravitino conventions, and this design introduces no new
delivery or ordering guarantees. Tag and policy operations continue to use the existing Tag and
Policy event dispatchers. Association events identify the target metadata object as
`SEMANTIC_MODEL` and do not emit Semantic Model alter events.

#### Java API

```java
NameIdentifier ident = NameIdentifier.of("semantic", "sales_model");

Expression orderAmountExpression =
    Expression.builder()
        .withDialects(
            new DialectExpression[] {
              DialectExpression.builder()
                  .withDialect(Dialects.ANSI_SQL)
                  .withExpression("order_amount")
                  .build()
            })
        .build();

Field orderAmount =
    Field.builder()
        .withName("order_amount")
        .withExpression(orderAmountExpression)
        .withDatatype(DataType.DECIMAL)
        .build();

Dataset orders =
    Dataset.builder()
        .withName("orders")
        .withSource(NameIdentifier.of("sales", "mart", "orders"))
        .withFields(new Field[] {orderAmount})
        .build();

Expression totalRevenueExpression =
    Expression.builder()
        .withDialects(
            new DialectExpression[] {
              DialectExpression.builder()
                  .withDialect(Dialects.ANSI_SQL)
                  .withExpression("SUM(orders.order_amount)")
                  .build()
            })
        .build();

Metric totalRevenue =
    Metric.builder()
        .withName("total_revenue")
        .withExpression(totalRevenueExpression)
        .withDescription("Total revenue across all orders")
        .withDatatype(DataType.DECIMAL)
        .build();

AIContext aiContext =
    AIContext.of(
        AIContextObject.builder()
            .withInstructions("Use certified metrics only")
            .withSynonyms(new String[] {"sales"})
            .withAdditionalProperties(Map.of("audience", "finance"))
            .build());

SemanticModelDefinition definition =
    SemanticModelDefinition.builder()
        .withAIContext(aiContext)
        .withDatasets(new Dataset[] {orders})
        .withMetrics(new Metric[] {totalRevenue})
        .build();

SemanticModel created =
    catalog
        .asSemanticModelCatalog()
        .createSemanticModel(
            ident, "Governed sales definitions", definition, Map.of());

SemanticModel loaded = catalog.asSemanticModelCatalog().loadSemanticModel(ident);
NameIdentifier[] models =
    catalog.asSemanticModelCatalog().listSemanticModels(Namespace.of("semantic"));

SemanticModel updated =
    catalog
        .asSemanticModelCatalog()
        .alterSemanticModel(
            ident,
            SemanticModelChange.updateComment("Updated sales definitions"),
            SemanticModelChange.replaceDefinition(definition));

boolean dropped = catalog.asSemanticModelCatalog().dropSemanticModel(ident);
```

`SemanticModelDefinition` is an immutable request value that groups the definition fields. It has no
name or independent lifecycle and is not another metadata object. The returned `SemanticModel`
exposes its definition fields directly.

#### REST API

Semantic Models use a dedicated resource and do not reuse `/views`:

```http
POST /api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/semantic-models
{
  "name": "sales_model",
  "comment": "Governed sales definitions",
  "definition": {
    "datasets": [
      {
        "name": "orders",
        "source": {
          "namespace": ["sales", "mart"],
          "name": "orders"
        }
      }
    ],
    "relationships": [],
    "metrics": []
  },
  "properties": {}
}
```

List, load, alter, and drop operations use the following resources:

```http
GET    /api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/semantic-models
GET    /api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/semantic-models/{name}
PUT    /api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/semantic-models/{name}
DELETE /api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/semantic-models/{name}
```

An alter request applies all changes atomically:

```http
PUT /api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/semantic-models/sales_model
{
  "updates": [
    {
      "@type": "updateComment",
      "newComment": "Updated sales definitions"
    },
    {
      "@type": "replaceDefinition",
      "definition": {
        "datasets": [
          {
            "name": "orders",
            "source": {
              "namespace": ["sales", "mart"],
              "name": "orders"
            }
          }
        ],
        "relationships": [],
        "metrics": []
      }
    }
  ]
}
```

#### Python API

The Python client mirrors the Java lifecycle and structured types:

```python
ident = NameIdentifier.of("semantic", "sales_model")
orders = Dataset("orders", NameIdentifier.of("sales", "mart", "orders"))
definition = SemanticModelDefinition(datasets=[orders])

created = catalog.as_semantic_model_catalog().create_semantic_model(
    ident,
    "Governed sales definitions",
    definition,
    {},
)
loaded = catalog.as_semantic_model_catalog().load_semantic_model(ident)
models = catalog.as_semantic_model_catalog().list_semantic_models(
    Namespace.of("semantic")
)
updated = catalog.as_semantic_model_catalog().alter_semantic_model(
    ident,
    SemanticModelChange.update_comment("Updated sales definitions"),
)
dropped = catalog.as_semantic_model_catalog().drop_semantic_model(ident)
```

### Storage and Parent Lifecycle

Semantic Models are persisted through the Gravitino EntityStore as dedicated entities with stable
entity IDs. Their typed metadata is stored independently of Table and View entity types. Storage
follows the identity-and-version pattern used by View and Function: one table stores the stable
identity and current version pointer, while another stores complete version snapshots.

#### Relational Schema

The following MySQL-style DDL defines the logical schema. H2 and PostgreSQL use equivalent types and
indexes following the existing EntityStore conventions.

```sql
CREATE TABLE IF NOT EXISTS `semantic_model_meta` (
    `semantic_model_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'semantic model id',
    `semantic_model_name` VARCHAR(128) NOT NULL COMMENT 'semantic model name',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'semantic model identity audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'last allocated version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'semantic model deleted at',
    PRIMARY KEY (`semantic_model_id`),
    UNIQUE KEY `uk_sid_smn_del` (`schema_id`, `semantic_model_name`, `deleted_at`),
    KEY `idx_smm_mid` (`metalake_id`),
    KEY `idx_smm_cid` (`catalog_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT 'semantic model metadata';

CREATE TABLE IF NOT EXISTS `semantic_model_version_info` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `semantic_model_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'semantic model id',
    `version` INT UNSIGNED NOT NULL COMMENT 'semantic model version',
    `semantic_model_name` VARCHAR(128) NOT NULL COMMENT 'semantic model name snapshot',
    `semantic_model_comment` TEXT DEFAULT NULL COMMENT 'semantic model comment snapshot',
    `semantic_model_definition` MEDIUMTEXT NOT NULL COMMENT 'structured definition snapshot (JSON)',
    `properties` MEDIUMTEXT DEFAULT NULL COMMENT 'semantic model properties snapshot (JSON)',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'semantic model version audit info',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'version deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_smid_ver_del` (`semantic_model_id`, `version`, `deleted_at`),
    KEY `idx_smvi_mid` (`metalake_id`),
    KEY `idx_smvi_cid` (`catalog_id`),
    KEY `idx_smvi_sid` (`schema_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT 'semantic model version information';
```

`semantic_model_definition` contains AI context, datasets, relationships, metrics, and custom
extensions as structured JSON managed by typed converters. Keeping the definition as one snapshot
preserves collection order and allows create and alter to validate and persist the complete model
atomically. Child members are not stored in separate tables because they do not have independent
identity, lifecycle, or authorization.

#### Version and Lifecycle Behavior

- Create writes the identity row and version 1 snapshot in one transaction.
- Alter follows Gravitino's existing version-based OCC mechanism. It atomically writes a new
  snapshot, increments `last_version`, and advances `current_version` with a server-internal CAS. A
  transaction-level CAS conflict rolls back and returns `409` with
  `OPTIMISTIC_LOCK_CONFLICT_CODE`. Because alter requests do not carry a caller-observed version,
  cross-request read-modify-write is last-write-wins. Rename also updates
  `semantic_model_meta.semantic_model_name`.
- Load joins `semantic_model_meta.current_version` to the matching snapshot and returns only the
  current Semantic Model.
- Owner, tag, and policy changes use their existing governance stores and do not create Semantic
  Model versions.
- Older snapshots follow the configured EntityStore version-retention and garbage-collection rules.
  The current API does not expose version identifiers, historical loads, or rollback, and storage of
  version rows does not guarantee permanent history retention.
- Drop follows Gravitino's existing version-based OCC mechanism and soft-deletes the identity and
  version rows under the existing retention rules. A transaction-level OCC conflict returns `409`
  with `OPTIMISTIC_LOCK_CONFLICT_CODE`, while an already-missing Semantic Model preserves the
  idempotent `false` result.
- Schema and catalog non-empty checks include Semantic Models. Cascade or force deletion of a parent
  removes the contained Semantic Models through the normal parent lifecycle.
- Deleting a referenced source in another schema or catalog does not delete the Semantic Model.
  Without a global dependency index, the stored reference may become unavailable as described under
  Validation.

The storage schema is a private implementation detail. Public clients receive typed fields and must
not depend on the JSON encoding or version bookkeeping used by the relational metadata store.

### Authorization and Governance

Semantic Models are new securable metadata objects. The design introduces:

| Privilege               | Purpose                                           |
|-------------------------|---------------------------------------------------|
| `CREATE_SEMANTIC_MODEL` | Create a Semantic Model under a schema            |
| `SELECT_SEMANTIC_MODEL` | Discover and load the model definition            |
| `MODIFY_SEMANTIC_MODEL` | Rename or alter the model definition and metadata |

- **Create.** A metalake or catalog owner may create directly; a schema owner also requires
  `USE_CATALOG`; otherwise, the caller requires `USE_CATALOG`, `USE_SCHEMA`, and
  `CREATE_SEMANTIC_MODEL`.
- **List and load.** Return only models on which the caller has `SELECT_SEMANTIC_MODEL`,
  `MODIFY_SEMANTIC_MODEL`, or ownership.
- **Alter.** Requires `MODIFY_SEMANTIC_MODEL` or ownership.
- **Drop.** A metalake or catalog owner may drop directly; a schema owner also requires
  `USE_CATALOG`; otherwise, the caller must own the Semantic Model and have both `USE_CATALOG` and
  `USE_SCHEMA`.

Owner, audit, tag, and policy APIs add `SEMANTIC_MODEL` to their supported metadata object types.
These controls apply to the complete model.

Catalog validation runs as the author and therefore requires visibility of referenced source
metadata. This write-time check does not grant later users permission to query those sources. A
compiler or execution engine must authorize referenced data using the effective query caller.

### Connector and Consumer Behavior

- Existing `TableCatalog` and `ViewCatalog` APIs are unchanged. Semantic Models are not merged into
  Table or View listings.
- Underlying connectors do not persist, list, load, or filter Semantic Models. The Gravitino server
  handles the dedicated lifecycle uniformly for every relational catalog.
- Engine connectors that do not understand semantic models observe no new relational objects.
- Semantic-aware tools, UIs, AI agents, and future compilers use `SemanticModelCatalog` or its REST
  resource explicitly.
- The UI presents Semantic Models as a separate schema object category, not under Tables or Views.

#### Native Metric Views in Connectors

Apache Spark introduced Metric Views in Spark 4.2, and connected catalogs may already contain
Spark-native Metric Views. Mapping these objects to Gravitino Semantic Models requires separate
decisions about discovery, model conversion, source of truth, namespace conflicts, and CRUD
behavior.

This design does not import or expose connector-native Metric Views. Spark Metric View integration
will be addressed in a separate design as the upstream capability evolves.

## Development Plan

Each task includes focused tests for the behavior it introduces.

| Phase                                   | Task                                                                                                         | Priority |
|-----------------------------------------|--------------------------------------------------------------------------------------------------------------|----------|
| **I. Core Infrastructure**              |                                                                                                              |          |
|                                         | Define Semantic Model identity, structured Java APIs, exceptions, and change types                           | P0       |
|                                         | Add EntityStore persistence, relational schemas and upgrade scripts, version lifecycle, and parent lifecycle | P0       |
|                                         | Implement Ossie-compatible and catalog-aware validation                                                      | P0       |
|                                         | Add server-side services, REST resources, and OpenAPI definitions                                            | P0       |
| **II. Governance and Security**         |                                                                                                              |          |
|                                         | Add privileges, ownership, and authorization filtering                                                       | P1       |
|                                         | Add audit, tag, and policy integration                                                                       | P1       |
|                                         | Add lifecycle event integration                                                                              | P1       |
| **III. Clients, Documentation, and UX** |                                                                                                              |          |
|                                         | Add Java client support                                                                                      | P0       |
|                                         | Add Python client support                                                                                    | P0       |
|                                         | Add read-only MCP server tools for listing and loading schema-scoped Semantic Models                         | P1       |
|                                         | Add Ossie conformance fixtures and interoperability documentation                                            | P1       |
|                                         | Add MCP server tools for creating, altering, and dropping Semantic Models                                    | P2       |
|                                         | Add UI support for discovering, viewing, and managing Semantic Models as a separate schema object category   | P2       |
