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

# Design: Ontology, Mappings and Glossary Terms

## Background

Gravitino stores [Semantic Models](./gravitino-semantic-model-design.md) that conform to the Apache
Ossie core specification, and [Domains](./domain-design.md) that group assets by business purpose
through tags. Neither answers two questions a business user or an agent asks next: *what concepts
exist in this business and how do they relate*, and *what does this business word mean*.

The Semantic Model design defers the first question explicitly:

> **Ontology management.** Apache Ossie Ontology definitions and mappings are separate metadata
> concepts and require their own design.

This document is that design.

The second question is already covered by the [Business Glossary design](https://github.com/apache/gravitino/pull/14239)
(issue #14238, in review), which defines the `GlossaryTerm` object, its hierarchy, bindings and
privileges. **This design adopts `GlossaryTerm` as the term model and does not introduce a competing
one.** It adds the two pieces the glossary design does not have: the machine-readable concept graph
that Apache Ossie specifies, and an explicit reference between a term and the concepts it elaborates.
Once the glossary design merges, these links become relative paths to `glossary-design.md`.

The reference for the overall shape is Databricks' Unity Catalog semantics, whose metric views,
domains, pages and certification form the human-modeled layer of the Genie Ontology. Two facts about
that reference shape the scope:

1. **The Genie Ontology is not a stored object.** It is the union of modeled context and context an
   engine infers from assets and usage. Only the modeled half is metadata.
2. **Ossie specifies a concept graph, not a term directory.** It has concepts, relationships and a
   mapping document, with no definition, synonyms, hierarchy, bindings, status or owner. Those are the
   glossary's contract.

## Goals

1. **Ossie conformance**: an `Ontology` is stored and exported as a document valid against the Ossie
   ontology schema, as a Semantic Model is stored and exported against the core schema.
2. **Mappings are first-class documents**: binding a Semantic Model's datasets, fields and
   relationships onto ontology concepts is its own entity, because Ossie defines it as its own document
   and deprecates embedding it in the ontology.
3. **One term model**: a business term is a `GlossaryTerm` as specified by the Business Glossary
   design, not a second entity defined here.
4. **A bridge between the two**: a term can reference the concepts it elaborates, so a human reads the
   term and a machine resolves the concept.
5. **Privileges consistent with Domain**: ontology and mapping access use `MANAGE_ONTOLOGY` and
   `USE_ONTOLOGY`, the shape Domain establishes with `MANAGE_DOMAIN` and `USE_DOMAIN`, and none of
   them grants access to data. Term access keeps the glossary's own privileges, because a term is a
   glossary object.
6. **Lifecycle consistency with Domain**: ontologies use `DRAFT` / `PUBLISHED`, the same two states
   as Domain, rather than inventing a third state set. A term keeps the glossary's three-state
   lifecycle.

## Non-Goals

1. **Inference.** Deriving concepts or terms from assets, usage or query logs. Engine and agent feature,
   not catalog metadata.
2. **Query planning and execution.** Unchanged from the Semantic Model design.
3. **Term CRUD, hierarchy, bindings, domains and resolve.** Specified and implemented by the Business
   Glossary design; this document does not restate them.
4. **Approval workflow, version history API and CSV import/export.** Enterprise, per the glossary
   design.
5. **A rich-text term body.** The glossary term carries a `definition`. If a long-form body is needed,
   it is a field on `GlossaryTerm` in the glossary design, not a second entity here.
6. **Materialization.**

## Usage Scenario

1. A steward with `MANAGE_ONTOLOGY` creates ontology Finance, whose concepts include `Account` and
   `Balance` and whose relationships carry natural-language phrasings.
2. The steward maps the `marketing` Semantic Model onto that ontology: dataset `orders` populates
   `Account`, field `amount` identifies `Balance`.
3. A steward creates the term "Attributed revenue" in domain Finance, binds it to the metric
   `active_customers` and to a column, and a `MANAGE_DEFINITIONS` holder sets it to `APPROVED`.
4. The steward links that term to the ontology concept `Revenue`.
5. A user asks a question; an agent resolves the word to the term, reads its bindings to find the data,
   and reads its concepts to understand what it is. Draft terms are invisible to the agent.

## Design

### Data model

This design adds three objects and one reference:

```
Ontology        { id, name, comment, ossieVersion, definition,
                  status: DRAFT | PUBLISHED, properties, audit_info }
OntologyVersion { ontologyId, version, definition, audit_info }
OntologyMapping { id, name, ontologyId, semanticModelRef, definition, audit_info }
TermConceptRef  { termId, ontologyId, concept, iri?, audit_info }
```

`GlossaryTerm`, `TermRelation`, `TermDomain`, `TermBinding`, `TermVersion` and their tables are
specified in the [Business Glossary design](https://github.com/apache/gravitino/pull/14239) and are not repeated here.

### Scope and identity

| Entity | Scope | Why |
| :--- | :--- | :--- |
| `Ontology` | Metalake | A business vocabulary spans catalogs and schemas, the same reasoning that puts `Domain` at the metalake. |
| `OntologyMapping` | Metalake, referencing a schema-scoped Semantic Model | Ossie models a mapping as a standalone document; making it a child of the model would prevent one ontology from mapping several models. |
| `TermConceptRef` | Metalake, between a `GlossaryTerm` and an ontology concept | Both endpoints are metalake-scoped. |

Ontologies are addressed by name alone within a metalake, follow the tag and domain name rules (unique
per metalake, up to 128 characters, `/` allowed and URL-encoded in paths), and use the same
case-insensitive uniqueness as glossary terms.

### Ontology documents and Ossie conformance

An `Ontology` stores a document valid against
[`ontology/ontology.json`](https://github.com/apache/ossie/blob/main/ontology/ontology.json). The
schema is closed: `additionalProperties` is `false` and required properties are `version`, `name` and
`ontology`. The root carries `version`, `name`, `description`, `ai_context`, `requires`, `ontology`
and `prefixes`.

Each component of `ontology` carries:

- `concept` and `type`, both required, where `type` is `EntityType` or `ValueType`.
- Optional `description`, `extends`, `derived_by` (ANSI SQL expressions), `identify_by`, `requires`,
  `relationships` and `iri`. An `iri` is a full IRI or a QName resolved against `prefixes`.

Each relationship carries:

- `name` and `verbalizes`, both required. At least one natural-language phrasing is part of the
  contract.
- Optional `description`, `roles` (a concept, plus an optional role name when a concept plays more than
  one role), `multiplicity` (`ManyToOne` or `OneToOne`), `derived_by`, `requires` and `iri`.

Three consequences follow, and they are validation rules rather than documentation:

- **A relationship without `verbalizes` is invalid.** It is rejected, not defaulted.
- **`type` is a closed enum of two values.** A glossary term is therefore not expressible as an
  ontology concept, which is the structural reason the term is a separate object rather than a field on
  a concept.
- **The document has no place for external references.** `additionalProperties` is `false`, so a
  term-to-concept link cannot be stored inside the Ossie document. It has to be its own metadata, which
  is what `TermConceptRef` is.

The specification version is pinned to `0.2.0.dev0`, matching `OssieVersion.DEFAULT_VERSION`, so an
ontology and a Semantic Model agree on the version they were written against.

Ontologies are always managed by Gravitino and never persisted in an underlying catalog, and one Ossie
document maps to one `Ontology`, as one Ossie core document maps to one Semantic Model. Import and
export reuse `OssieDocument`, `OssieFormat` and the existing import/export event pattern. One addition
is needed: the accepted root properties of an ontology document differ from those of a core document,
so the converter needs a second entry point rather than a flag on the existing one.

`Ontology` keeps version snapshots, because the document carries a `version` and a vocabulary changes
over time. As in the glossary design, the OSS exposes no version history API and the snapshot exists so
Enterprise does not need its own storage.

### Mappings

Ossie defines a mapping as a standalone document validated against `ontology/mapping.json`, and marks
the embedded form in the ontology document as deprecated:

> **Deprecated.** Embedded ontology maps, accepted only so existing documents continue to validate;
> write mappings as standalone documents validated against `ontology/mapping.json` instead.

A mapping document contains:

- `name`, `description`, `semantic_model` and `concept_mappings`.
- A `ConceptMapping` names a `concept` and carries `object_mappings` (valid only when the concept is an
  `EntityType`) or `link_mappings`, or both.
- An `ObjectMapping` names a `concept` and carries either an `expression` or `referent_mappings`.
- A `LinkMapping` names a `relationship` and carries an `object_mapping` and optional nested
  `children`.
- A `ReferentMapping` names a `relationship` and carries either an `expression` or nested
  `referent_mappings`.

`OntologyMapping` stores such a document together with a stable reference to one `Ontology` and one
Semantic Model. Because the document embeds the model identity, the mapping references the model by
identifier and version at creation time rather than duplicating its definition.

**Visibility follows both sides.** A mapping is returned only to a caller who can read the ontology and
the Semantic Model it binds. It is never more visible than the less visible of the two.

### Glossary terms

The term object is specified in the [Business Glossary design](https://github.com/apache/gravitino/pull/14239). This design
depends on the following parts of that contract and adopts them unchanged:

| Aspect | Adopted behavior |
| :--- | :--- |
| Identity | Name unique per metalake, case-insensitive; a synonym cannot equal another term's name |
| Lifecycle | `DRAFT` → `APPROVED` → `DEPRECATED`, set through the API by a `MANAGE_DEFINITIONS` holder |
| Hierarchy | Multi-parent acyclic `BROADER`, symmetric `RELATED`, cycles rejected |
| Domains | Explicit `TermDomain` relation, not tags; domain grants govern the term |
| Bindings | Direct bindings to catalogs, schemas, tables, views, columns, filesets, topics, models, and metrics as `(SEMANTIC_MODEL, metricName)` |
| Lookup | Exact, case-insensitive resolve from a business word to approved terms |

Two deliberate consequences of adopting it rather than defining a `Page` here:

- **A term has a `definition`, not a rich body.** A long-form body is a glossary change if it is ever
  needed. Duplicating the term as a second object would give the same word two definitions, two owners
  and two approval states.
- **Term bindings point at data, ontology mappings point at semantics.** A binding says which column or
  metric implements a term. A mapping says which dataset populates a concept. They are different
  relations and are not merged.

### The term-to-concept bridge

A `GlossaryTerm` may reference zero or more ontology concepts:

- the reference names the ontology, the concept, and the concept's `iri` when the document supplies one;
- it never stores a position in the document, so reordering or re-versioning an ontology does not break
  it;
- the reverse lookup, concepts to terms, is the one an agent uses to find how people talk about a
  concept.

The two objects are complementary rather than redundant. A term is the human entry point; a concept is
the machine-readable node. Neither substitutes for the other:

| | `GlossaryTerm` | Ontology concept |
| :--- | :--- | :--- |
| Answers | What this word means to the business | What this concept consists of, and how it relates |
| Human-readable definition | Yes | No, only a description |
| Synonyms | Yes | No |
| Hierarchy | `BROADER` / `RELATED`, multi-parent | `extends`, typed `relationships` with cardinality |
| Typed | No | `EntityType` or `ValueType` |
| Sub-type detail | No | `identify_by`, `derived_by`, `roles` |
| Bound to data | Yes, directly | Through a mapping |
| Approval state | Yes | Yes, but of the whole vocabulary |
| Ossie conformance | Not applicable | Yes |

A term without a concept is a normal state: it is defined for the business before the vocabulary is
modelled. A concept without a term is also normal: a vocabulary is modelled before every term is
written.

### Relationship to Domains and Semantic Models

- **Domain** answers *where in the business does this asset belong*. Membership is a tag, and a domain
  grants no access to data.
- **Glossary term** answers *what does this business word mean*. It joins domains by an explicit
  relation, and it binds to data without owning semantics.
- **Ontology** answers *what concepts exist and how do they relate*. It is a machine-readable graph and
  holds no assets.
- **Ontology mapping** answers *which part of a Semantic Model implements a concept*.

All four sit above the same assets and none of them grants access to data. They are complements, not
layers that replace one another: a domain says where an asset belongs, an ontology says what the
business's concepts are, a mapping says which dataset populates a concept, and a term says what a word
means to people.

### Relationship to semantic query planning

The semantic query planning contract (issue #14308) lets a caller send metrics, dimensions and filters
against a Semantic Model and receive SQL, with Gravitino owning model resolution, authorization and the
response envelope and a `SemanticQueryPlanner` SPI owning SQL generation.

An ontology does not replace that contract and is not a prerequisite for it. The two meet at one point:
once `OntologyMapping` exists, a planner can accept concept names as the vocabulary of a query and
resolve them through the mapping to the datasets and fields of the Semantic Model. That is a
capability of the planner, behind the same endpoint, not a second planning API. Until mappings exist,
planning works against a Semantic Model alone, exactly as #14308 specifies.

### Status and visibility

`Ontology` follows Domain's two-state model rather than the glossary's three-state one. A vocabulary
is either being drafted or it is published; `APPROVED` and `DEPRECATED` exist in the glossary to
record review of a single term, and a whole vocabulary has no reviewer to record.

| Status | Visible to | Set by | Delete |
| :--- | :--- | :--- | :--- |
| `DRAFT` | `MANAGE_ONTOLOGY` | creation | creator, or `MANAGE_ONTOLOGY` |
| `PUBLISHED` | `USE_ONTOLOGY` | `MANAGE_ONTOLOGY` | not allowed while a mapping or term reference points at it |

A `DRAFT` ontology is omitted from lists, returns 404 on get, and is not returned by a mapping whose
caller could not otherwise see it. `TermConceptRef` rows pointing at a draft ontology are visible only
to a caller who can see that ontology. Publishing and unpublishing are status changes, not deletes, so
a published mapping can be unpublished without being lost.

### Access control

Ontologies use a privilege family of their own, in the shape Domain establishes with `MANAGE_DOMAIN`
and `USE_DOMAIN`. A Define object therefore does not reuse the glossary's privileges, which govern a
term rather than a vocabulary:

| Privilege | On | Allows |
| :--- | :--- | :--- |
| `USE_ONTOLOGY` | Metalake, ontology | Read published ontologies and the mappings that reference them |
| `MANAGE_ONTOLOGY` | Metalake, ontology | Create, edit, publish, unpublish and delete ontologies and mappings; see and edit drafts |

`MANAGE_ONTOLOGY` includes `USE_ONTOLOGY`; the creator, the ontology owner and the metalake owner
hold both. There is no separate `CREATE_ONTOLOGY`, for the same reason Domain has no `CREATE_DOMAIN`:
creating is a manage operation on the metalake, and the creator owns what it creates.

Three rules are part of the contract:

- **Creating a mapping needs both sides.** The caller needs `MANAGE_ONTOLOGY` on the ontology and
  `USE_SEMANTIC_MODEL` on the target model. Checking one side would let a caller attach a vocabulary
  they do not control to a model they do, or the reverse.
- **Referencing a concept needs the term edit privilege and ontology visibility.** Adding a
  `TermConceptRef` edits the term and reads the ontology; it does not need a write privilege on the
  ontology.
- **A referenced object is not dropped out from under its referrers.** Dropping an ontology that a
  mapping or a term reference points at fails with a conflict, in the shape of Domain's
  `NonEmptyDomainException`. Cascading would silently destroy a binding rather than surface the
  dependency.

None of these privileges grants access to data. Reading an ontology never implies reading the tables a
mapped model is built on.

### REST API

Ontology endpoints, under `/api/metalakes/{metalake}`:

| Operation | Endpoint |
| :--- | :--- |
| List / create | `GET /ontologies`, `POST /ontologies` |
| Get / alter / drop | `GET /ontologies/{ontology}`, `PUT /ontologies/{ontology}`, `DELETE /ontologies/{ontology}` |
| Import / export | `POST /ontologies/{ontology}/ossie`, `GET /ontologies/{ontology}/ossie` |
| Set status | `PATCH /ontologies/{ontology}` |
| List / create mapping | `GET /ontology-mappings`, `POST /ontology-mappings` |
| Get / alter / drop mapping | `GET`, `PUT`, `DELETE /ontology-mappings/{mapping}` |
| Terms of an ontology | `GET /ontologies/{ontology}/terms` |
| Concepts of a term | `GET /glossary/terms/{term}/concepts` |
| Link / unlink concept | `POST /glossary/terms/{term}/concepts` |

Term, hierarchy, binding, resolve and domain-term endpoints are owned by the Business Glossary design
and are not duplicated. The concept endpoints extend the glossary paths rather than adding a second
term namespace.

### MCP tools

The `glossary` tool tag is owned by the Business Glossary design. This design adds read-only tools that
need the concept graph:

| Tool | Endpoint | Use |
| :--- | :--- | :--- |
| `list_ontologies` | `GET /ontologies?details=true` | Find the vocabularies available |
| `get_ontology_by_name` | `GET /ontologies/{ontology}` | Read concepts and relationships |
| `list_concepts_for_term` | `GET /glossary/terms/{term}/concepts` | Move from a business word to its concept |
| `list_terms_for_concept` | `GET /ontologies/{ontology}/terms?concept=` | Move from a concept to how people describe it |

Draft ontologies are filtered by the same rules as everywhere else, because the MCP server forwards the
caller's `Authorization` header.

### Events and audit

Ontology create, alter, status, import, export and drop emit pre, success and failure events through an
`OntologyEventDispatcher`, mirroring `GlossaryEventDispatcher`. Mapping create, alter and drop emit
their own events, because they change what an agent can resolve. Linking or unlinking a concept is
audited as part of the term's change history.

## Alternatives Considered

**Define a `Page` object alongside `GlossaryTerm`.** Rejected. Both would answer "what does this word
mean", giving one term two definitions, two owners and two approval states. The glossary design already
covers the term; the missing piece was the concept graph, not another document object.

**Model terms as ontology concepts.** Rejected. Ossie's concept `type` is a closed enum of `EntityType`
and `ValueType`, and a concept has no definition, synonyms, hierarchy, bindings, status or owner. A term
would either not validate or would overload a value type to mean something else.

**Store the term-to-concept link inside the Ossie document.** Rejected. `additionalProperties` is
`false` and the schema has no such field, so this would produce a non-conformant ontology.

**Embed the mapping in the ontology document.** Rejected. Ossie marks that form deprecated and directs
writers to standalone mapping documents.

**Put ontologies at the schema scope, like Semantic Models.** Rejected for now. A vocabulary describes a
business, not a catalog, and a schema-scoped ontology would be pinned to the catalog whose schema holds
it while mapping models from elsewhere. This is the most likely point of disagreement and is recorded as
an open question.

## Prior Art

Unity Catalog semantics (Databricks): metric views, domains, pages and certification, forming the
human-modeled layer of the Genie Ontology. A Page is a governed definition of a business concept, belongs
to a domain, and is the source Genie cites when it answers using a defined term. Notably, that
documentation has no separate glossary object, which is why the granularity question is worth settling
explicitly rather than by analogy.

Apache Ossie: `core-spec/spec.md` defines Semantic Models, datasets, fields, metrics, dimensions and
relationships; `ontology/ontology.json` defines the concept graph, and `ontology/mapping.json` the
mapping from a semantic model onto it. The ontology section is separate from the core specification,
which is why this design is separate from the Semantic Model design.

In-flight Gravitino designs this one depends on: [Domain](./domain-design.md) for metalake-level scope,
tag-backed membership and the `NonEmptyDomainException` precedent, and the
[Business Glossary](https://github.com/apache/gravitino/pull/14239) for the term object, hierarchy, bindings and `DEFINITION`
privileges.

## Resolved Questions

Three questions this design first raised are settled by the Domain precedent, which lands first and
which an ontology should not contradict.

1. **Scope of `Ontology`.** Metalake, as argued here. A vocabulary describes a business, not a catalog,
   and Domain already establishes the metalake as the home for business-level objects.
2. **Status vocabulary.** `DRAFT` / `PUBLISHED`, Domain's two states. The glossary's three states exist
   to record review of a single term; a whole vocabulary has no reviewer to record.
3. **Privilege family.** `MANAGE_ONTOLOGY` / `USE_ONTOLOGY`, ontology-specific like Domain's
   `MANAGE_DOMAIN` / `USE_DOMAIN`, and not the glossary's `DEFINITION` family. A Define object owns its
   own privileges; reusing the glossary's would couple a vocabulary to a term's lifecycle, and the
   glossary's privileges are not implemented.

## Open Questions

1. **Placement of `OntologyMapping`.** A metalake object, or a child of the Semantic Model it maps? The
   Ossie document embeds `semantic_model`, which argues for the child; reuse across models argues for the
   object.
2. **Owner of the term-to-concept link.** Should `TermConceptRef` be specified here or in the glossary
   design, so that only one document owns it? Landing the glossary first and adding the link there may
   be cleaner than cross-referencing.
3. **Stable identity of a concept.** A concept is identified by name, with an optional IRI. A rename
   breaks every reference. Should Gravitino mint an internal concept id, and how would it appear in the
   exported document?
4. **Specification version.** `0.2.0.dev0` is pre-release. Does a stored ontology keep the version it was
   written against and get migrated on write, or is it pinned as Semantic Models are?

## Task Breakdown

### Phase 0: Prerequisites

- [x] Settle the scope, the status vocabulary and the privilege family on the Domain precedent, as
      recorded in Resolved Questions
- [ ] Decide whether the term-to-concept link belongs in this design or the glossary design
- [ ] Land the Business Glossary design, which Phase 3 depends on

### Phase 1: Ontology

- [ ] `Ontology` identity, status, Java API, exceptions, change types and `OntologyEventDispatcher`
- [ ] EntityStore persistence, relational tables and upgrade scripts, version snapshots
- [ ] Ossie ontology validation: closed root, required `verbalizes`, closed concept `type`
- [ ] Ossie import and export for ontology documents
- [ ] Server services, REST resources and OpenAPI definitions
- [ ] `MANAGE_ONTOLOGY` / `USE_ONTOLOGY` privileges, ownership and authorization filtering

### Phase 2: Mappings

- [ ] `OntologyMapping` entity, storage and API
- [ ] Mapping document validation against the Ossie mapping schema
- [ ] Dual-side authorization and reference integrity on drop
- [ ] REST endpoints and tests

### Phase 3: Term-to-concept bridge

- [ ] `TermConceptRef` storage and indexes
- [ ] Concept endpoints on the term, and the reverse lookup from an ontology
- [ ] Visibility rules for references to draft ontologies
- [ ] Tests, including reference integrity on ontology drop

### Phase 4: Clients, MCP and documentation

- [ ] Java and Python clients
- [ ] MCP ontology tools and the two concept-lookup tools
- [ ] OpenAPI specification and user documentation

### Phase 5: Concept-aware query planning

- [ ] A planner behind the #14308 `SemanticQueryPlanner` SPI that resolves concept names through
      `OntologyMapping` to semantic-model datasets and fields
- [ ] Tests that plan the same query by metric name and by concept name and compare the generated SQL