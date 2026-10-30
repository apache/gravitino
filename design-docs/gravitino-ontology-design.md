# Design: Ontology Export and Glossary Terms

## Background

Gravitino stores [Semantic Models](./gravitino-semantic-model-design.md) that conform to the Apache Ossie
core specification, and [Domains](./domain-design.md) that group assets by business purpose through tags.
Neither answers two questions a business user or an agent asks next: *what concepts exist in this
business and how do they relate*, and *what does this business word mean*.

The Semantic Model design defers the first question explicitly:

> **Ontology management.** Apache Ossie Ontology definitions and mappings are separate metadata
> concepts and require their own design.

The second question is covered by the [Business Glossary design](https://github.com/apache/gravitino/pull/14239)
(issue #14238, in review), which defines the `GlossaryTerm` object, its hierarchy, bindings and
privileges. **This design adopts `GlossaryTerm` as the term model and does not introduce a competing
one.** Once the glossary design merges, that link becomes a relative path to `glossary-design.md`.

Apache Ossie specifies two documents beyond the core schema: `ontology/ontology.json`, which declares a
concept graph, and `ontology/mapping.json`, which binds a Semantic Model's datasets and fields onto those
concepts. The question this design answers is not *how Gravitino stores them* but *what Gravitino does
with them*.

The reference for the overall shape is Databricks' Unity Catalog semantics. Databricks defines exactly
four components — metric views, domains, pages and certification — and states what they are for:

> Together, these tools make up the human-modeled layer of the Genie Ontology: the business context that
> you define and govern explicitly. Genie One combines this modeled context with context it infers
> automatically from your assets and usage, forming a unified context layer.

**The Genie Ontology is not a stored object.** No user creates, versions or deletes one; it is computed at
query time from modeled context plus inferred context. Gravitino's counterparts to the four modeled
components are Semantic Models, Domains and Glossary Terms, and it has nothing that corresponds to the
computed layer itself.

This design adds an **export**, not an entity. Gravitino renders its vocabulary as an Ossie ontology
document on request. Nothing is stored, and no metadata object, status, version, privilege or lifecycle is
introduced.

## Goals

1. **Export conformance**: an exported document is valid against the Ossie ontology schema, as a Semantic
   Model is exported against the core schema.
2. **One vocabulary**: the concepts come from the glossary's `GlossaryTerm` and from nothing else. There is
   no second concept object to keep in sync.
3. **No new governance surface**: exporting adds no storage table, no status, no version snapshot, no
   privilege family and no lifecycle transitions.
4. **Standard interchange**: a consumer that speaks Ossie reads the vocabulary without speaking
   Gravitino's API, so the vocabulary can feed an agent, a planner or another catalog.

## Non-Goals

1. **Storing ontologies.** Recorded as a rejected alternative below.
2. **Authoring concepts that are not terms.** Whether a concept may exist without a term is Open
   Question 1; the export's derivation rules wait on it.
3. **Importing an ontology document.** With nothing to store it in, the import direction has no meaning
   yet.
4. **Inference.** Deriving concepts from assets, usage or query logs. Engine and agent feature, not
   catalog metadata.
5. **Query planning and execution.** Unchanged from the Semantic Model design.
6. **Term CRUD, hierarchy, bindings, domains and resolve.** Specified and implemented by the Business
   Glossary design; this document does not restate them.
7. **A rich-text term body.** The glossary term carries a `definition`. If a long-form body is needed, it
   is a field on `GlossaryTerm` in the glossary design.

## Usage Scenario

1. Stewards curate terms in domain Finance — `Revenue`, `Active Customer` — with definitions, synonyms
   and a `BROADER` hierarchy between them.
2. A steward maps the `marketing` Semantic Model onto that vocabulary: dataset `orders` supplies
   `Revenue`, metric `active_customers` supplies `Active Customer`.
3. An agent, or an external tool that speaks Ossie, calls
   `GET /api/metalakes/{metalake}/semantic-models/{model}/ossie-ontology` and receives a document that
   validates against `ontology/ontology.json`.
4. The document carries the terms as concepts, the `BROADER` hierarchy as `extends`, and the model's
   datasets and metrics as the mapping document's targets.
5. Nothing is written back. Asking again later reflects whatever the glossary and the model say at that
   moment.

## Design

### What is rendered

Two documents, both derived on request and neither stored:

| Document | Address | Content |
| :--- | :--- | :--- |
| Ontology | `GET .../semantic-models/{model}/ossie-ontology` | `version`, `name`, `description`, `ai_context`, `requires`, `prefixes`, `ontology[]` |
| Mapping | `GET .../semantic-models/{model}/ossie-ontology/mapping` | `name`, `description`, `semantic_model`, `concept_mappings[]` |

The ontology document is addressed per Semantic Model because the mapping half is model-specific while the
concept half is not: a term belongs to a business, not to a catalog, and the same vocabulary is routinely
mapped onto several models. Addressing per model keeps one vocabulary from being copied per model.

### Ossie conformance

The schema is closed at every level, so validation is part of the render path rather than a documentation
concern:

- the root accepts only `version`, `name`, `description`, `ai_context`, `requires`, `prefixes` and
  `ontology`; `ontology_mappings` is rejected, because Ossie deprecates the embedded form and directs
  writers to the standalone mapping document;
- each concept requires `concept` and `type`, where `type` is a closed enum of `EntityType` and
  `ValueType`;
- each relationship requires `name` and `verbalizes`, with at least one non-blank phrasing. A relationship
  without a verbalization is rejected, not defaulted, because the phrasing is what an agent quotes when it
  explains the edge;
- `additionalProperties` is `false`, so every property of a component, relationship or role must be one the
  schema defines.

Two consequences follow for a derived document:

- **The export drops what Ossie has no home for.** A term's definition, synonyms, bindings, status and
  owner have no schema field. They are omitted, not smuggled into a `custom_extensions` key, because a
  non-conformant document is worse than a lossy one.
- **Every concept must be resolvable inside the document.** A concept named by `extends` or by a
  relationship role has to be declared in the same document, so the render fails rather than emitting a
  dangling name.

The specification version is pinned to `0.2.0.dev0`, matching `OssieVersion.DEFAULT_VERSION`, and is
recorded in the document's `version`. Because the document is rendered rather than stored, a version bump
is a code change and never a migration.

### What the export cannot carry

This is the cost of not storing an ontology, and it should be read as a limitation rather than a detail:

| Glossary term has | Ossie concept has | Result |
| :--- | :--- | :--- |
| `name` | `concept` | carried |
| `definition` | nothing | **dropped** |
| `synonyms` | nothing | **dropped** |
| `BROADER` | `extends` | carried |
| `RELATED` | `relationships` (typed) | **partly** — a relation with no type has no `verbalizes` to quote |
| `TermBinding` | mapping document | carried |
| `status`, owner, `sourceRef` | nothing | **dropped** |

A consumer that needs the definition or the synonyms reads the term through the glossary API; the ontology
document is the machine-readable skeleton, not the whole vocabulary.

### Access control

No privilege is added. The export is a read of two existing objects, so it requires what already exists:

- the caller needs the privilege that reads the Semantic Model, because the address is the model;
- a term appears in the document only if the caller can see it under the glossary's own rules, so a draft
  term is absent for a caller who cannot see drafts;
- a document grants no access to data, exactly as the model it is derived from grants none.

### REST API

| Operation | Endpoint |
| :--- | :--- |
| Render the ontology document | `GET /api/metalakes/{metalake}/semantic-models/{model}/ossie-ontology` |
| Render the mapping document | `GET /api/metalakes/{metalake}/semantic-models/{model}/ossie-ontology/mapping` |

Both accept `?format=json|yaml`, reusing `OssieFormat`. Both are read-only, so there is no create, alter or
delete, and no status endpoint.

### Relationship to Domains and Glossary Terms

Domains and terms are metalake-level objects; the export inherits that. The document's `name` is the model
name, and the vocabulary it draws from is the metalake's, not a domain's. A domain-scoped export is not
offered, because a term may belong to zero or more domains and a document that silently dropped the terms
outside one domain would be misleading.

The term object, its hierarchy and its bindings are owned by the Business Glossary design. This design
only reads them.

## Alternatives Considered

**Store an `Ontology` metadata object with its own status, versions, privileges and lifecycle.** This was
an earlier revision of this document, and it is rejected. Four reasons:

1. **It has no counterpart in the reference.** Databricks' computed layer is not an object, and the four
   modeled components Gravitino mirrors are Semantic Models, Domains and Glossary Terms. A stored ontology
   would be a component justified only by the Ossie specification's own shape, not by a user need anyone
   stated.
2. **It duplicates the authoring surface.** The glossary already owns the words. A stored ontology would
   own a second set of concepts that must be kept aligned with terms, and every alignment bug is a document
   that disagrees with the glossary.
3. **Its governance cost is disproportionate.** A stored object needs a scope, a status vocabulary, a
   version snapshot table, a privilege family, ownership, events and a delete policy. That machinery
   exists to govern state; a rendered document has no state.
4. **Most of its hard questions disappear.** Concept ids across renames, relationship ids, version
   migration and the placement of the mapping were all real problems — and all of them were caused by
   storage. Rendering answers them by construction.

**Define a `Page` object alongside `GlossaryTerm`.** Rejected. Both would answer "what does this word
mean", giving one term two definitions, two owners and two approval states.

**Embed the mapping in the ontology document.** Rejected. Ossie marks that form deprecated and directs
writers to standalone mapping documents.

**Address the ontology at the metalake, not per model.** Rejected for now. The concept half is
metalake-scoped, but the mapping half is per model by construction, and one document carrying several
models' mappings would not validate. This is Open Question 4.

## Prior Art

Unity Catalog semantics (Databricks): metric views, domains, pages and certification, forming the
human-modeled layer of the Genie Ontology. That documentation is explicit that the Genie Ontology itself is
a computed context layer combining modeled and inferred context, which is why this design stops at
exporting the modeled half. A Page is a governed definition of a business concept and belongs to a domain;
the glossary term is its Gravitino counterpart. Databricks has no separate glossary object, which is why
the granularity question is worth settling explicitly rather than by analogy.

Apache Ossie: `core-spec/spec.md` defines Semantic Models, datasets, fields, metrics, dimensions and
relationships; `ontology/ontology.json` defines the concept graph, and `ontology/mapping.json` the mapping
from a semantic model onto it. The ontology section is separate from the core specification, which is why
this design is separate from the Semantic Model design.

In-flight Gravitino designs this one depends on: [Domain](./domain-design.md) for the metalake-level
precedent, and the [Business Glossary](https://github.com/apache/gravitino/pull/14239) for the term object,
its hierarchy and its bindings.

## Open Questions

1. **Where concepts come from.** The likely answer is that a term becomes a concept, but a term has no
   place for `iri`, no `conceptType`, and no way to express a concept that is not a term. Whether to carry
   both in the term's free-form `properties`, to add typed fields to `GlossaryTerm`, or to accept that the
   export only ever covers terms, is undecided. Phase 2 waits on it; Phase 1 does not.
2. **How a `RELATED` relation becomes an Ossie relationship.** Ossie requires `verbalizes` and allows a
   `multiplicity`; the glossary relation has neither. Either the glossary gains a relation type, or those
   relations are omitted from the export.
3. **Whether the mapping document ships in the first version.** Rendering it requires term bindings to be
   matched against model datasets and metrics, which is more derivation than the ontology document needs.
4. **Whether a metalake-level export should exist** for callers who want the vocabulary without a model,
   accepting that it carries no mapping.

## Task Breakdown

Tracked as an epic in #14309, one issue per item below. Issues are opened as the work starts rather than
up front, as the Domain epic does, so this list is the plan and not a status board.

### Phase 0: Prerequisites

- [ ] Land the Business Glossary design, which Phases 2 and 3 depend on
- [x] Decide the shape: an export rather than a stored entity, recorded in Alternatives Considered

### Phase 1: Document model and rendering

Independent of the glossary: it takes concepts in and produces a validated document out.

- [ ] Ossie ontology document model: concepts, relationships, roles, the two concept types, the two
      multiplicities, and the closed-root validation
- [ ] Rendering to JSON and YAML, reusing `OssieFormat`, with the export re-parsed before it is returned

### Phase 2: The mapping document

- [ ] Derivation of `concept_mappings` from a Semantic Model's datasets, fields and metrics
- [ ] The `ossie-ontology/mapping` endpoint and its tests

### Phase 3: The concept document

- [ ] Derivation of `ontology[]` from glossary terms, once Open Questions 1 and 2 are settled
- [ ] The `ossie-ontology` endpoint and its tests

### Phase 4: Clients, MCP and documentation

- [ ] Java and Python client methods for the two endpoints
- [ ] A read-only MCP tool that returns the document
- [ ] OpenAPI definitions and user documentation