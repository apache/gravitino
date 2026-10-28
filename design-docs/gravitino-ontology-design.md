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

# Design: Ontology, Mappings and Pages

## Background

Gravitino stores [Semantic Models](./gravitino-semantic-model-design.md) that conform to the Apache
Ossie core specification, and [Domains](./domain-design.md) that group assets by business purpose
through tags. Neither answers two questions a business user or an agent asks next: *what concepts
exist in this business and how do they relate*, and *what does this term mean, in prose, and who
vouches for it*.

The Semantic Model design defers the first question explicitly:

> **Ontology management.** Apache Ossie Ontology definitions and mappings are separate metadata
> concepts and require their own design.

This document is that design, and it also covers the second question, which Ossie does not address.

The reference implementation is Databricks' Unity Catalog semantics. Its components are *metric
views*, *domains*, *pages* and *certification*, and together they form the human-modeled layer of the
Genie Ontology. Three facts about that reference set the scope of this design:

1. **There is no separate Glossary entity.** A glossary is not among the components; Pages is the
   mechanism that defines terms. Gravitino should not add a second store for the same thing.
2. **The Genie Ontology is not a stored object.** It is the union of modeled context and context an
   engine infers from assets and usage. Only the modeled half is metadata.
3. **Ossie specifies a concept graph, not a document store.** It has concepts, relationships and a
   mapping document, with no freeform body, owner, draft state or comments.

The work therefore covers two different kinds of thing, and this design keeps them apart: a
machine-readable concept graph, and a governed narrative document.

## Goals

1. **Ossie conformance**: an `Ontology` is stored and exported as a document valid against the Ossie
   ontology schema, as a Semantic Model is stored and exported against the core schema.
2. **Mappings are first-class documents**: binding a Semantic Model's datasets, fields and
   relationships onto ontology concepts is its own entity.
3. **A narrative layer**: a `Page` defines a business term for people and agents, with description,
   synonyms, body, related assets, sources and an owner.
4. **Separate privileges**: managing a vocabulary is not the same permission as belonging to a
   domain, and neither grants access to data.
5. **Draft and published**, so a vocabulary or a term can be prepared before it is visible.

## Non-Goals

1. **Inference.** Deriving concepts from assets, usage or query logs. That is an engine or agent
   feature, not catalog metadata.
2. **Query planning and execution.** Unchanged from the Semantic Model design.
3. **Rich-text rendering.** The server stores a body as Markdown; rendering is a client concern.
4. **Editorial collaboration in v1.** Comments, suggestion batches and reactions are deferred; they
   do not change the entity contract.
5. **A separate Glossary entity.** A glossary is a listing of Pages.
6. **Materialization.**

## Usage Scenario

1. A steward with `MANAGE_ONTOLOGY` creates ontology Finance, whose concepts include `Account` and
   `Balance` and whose relationships carry natural-language phrasings.
2. The steward maps the `marketing` Semantic Model onto that ontology: dataset `orders` populates
   `Account`, field `amount` identifies `Balance`.
3. A steward with `MANAGE_PAGE` writes a Page "Attributed revenue" under domain Finance, links the
   semantic model and the source table as related assets, and publishes it.
4. A user with `USE_PAGE` on Finance browses published Pages, and an agent retrieves the same Page and
   its concepts to ground an answer, citing it.
5. A second steward keeps a draft Page for a term that is not agreed yet. It is invisible to everyone
   else until published.

## Design

### Data model

```
Ontology        { id, name, comment, ossieVersion, definition, status: DRAFT | PUBLISHED,
                  properties, audit_info }
OntologyVersion { ontologyId, version, definition, audit_info }
OntologyMapping { id, name, ontologyId, semanticModelRef, definition, audit_info }
Page            { id, name, domainId?, owner, description, synonyms, body,
                  status: DRAFT | PUBLISHED, audit_info }
PageConceptRef  { pageId, concept, iri?, audit_info }
PageAssetRef    { pageId, type, fullName, relation: RELATED | SOURCE, audit_info }
```

### Scope and identity

| Entity | Scope | Why |
| :--- | :--- | :--- |
| `Ontology` | Metalake | A business vocabulary spans catalogs and schemas, the same reasoning that puts `Domain` at the metalake. |
| `OntologyMapping` | Metalake, referencing a schema-scoped Semantic Model | Ossie models a mapping as a standalone document; making it a child of the model would prevent one ontology from mapping several models. |
| `Page` | Metalake, optionally attached to a `Domain` | Pages belong to domains and subdomains in the reference implementation. |

Ontologies and Pages are addressed by name alone within a metalake. Their names follow the constraints
of tag and domain names: unique within the metalake, up to 128 characters, and `/` allowed, which must
be URL-encoded as `%2F` in every path segment. A Page with no domain is not curated yet; a Page that
belongs to a domain is reachable from that domain.

Page names and ontology names are separate namespaces: a Page may share a name with an ontology, and
neither collides with a table or a Semantic Model.

### Ontology documents and Ossie conformance

An `Ontology` stores a document valid against
[`ontology/ontology.json`](https://github.com/apache/ossie/blob/main/ontology/ontology.json). The
schema is closed: `additionalProperties` is `false` and required properties are `version`, `name` and
`ontology`. The root carries `version`, `name`, `description`, `ai_context`, `requires`, `ontology`,
and `prefixes`.

Each entry of `ontology` is a component:

- `concept` and `type` are required, where `type` is `EntityType` or `ValueType`.
- Optional: `description`, `extends`, `derived_by` (ANSI SQL expressions), `identify_by`, `requires`,
  `relationships`, and `iri`. An `iri` is either a full IRI or a QName resolved against `prefixes`.

Each relationship carries:

- `name` and `verbalizes` are required. At least one natural-language phrasing is part of the contract.
- Optional: `description`, `roles` (a concept, and an optional role name when a concept plays more than
  one role), `multiplicity` (`ManyToOne` or `OneToOne`), `derived_by`, `requires`, and `iri`.

Two consequences follow, and both are validation rules rather than documentation:

- **A relationship without `verbalizes` is invalid.** It is rejected, not defaulted.
- **`type` is a closed enum of two values.** A Page is therefore not expressible as an ontology
  concept, which is the structural reason Pages are a separate entity rather than a field on a
  concept.

The specification version is pinned to `0.2.0.dev0`, matching `OssieVersion.DEFAULT_VERSION`, so an
ontology and a Semantic Model agree on the version they were written against.

Ontologies are always managed by Gravitino and never persisted in an underlying catalog, and one Ossie
document maps to one `Ontology`, as one Ossie core document maps to one Semantic Model. Import and
export reuse `OssieDocument`, `OssieFormat` and the existing import/export event pattern. One addition
is needed: the accepted root properties of an ontology document differ from those of a core document,
so the converter needs a second entry point rather than a flag on the existing one.

`Ontology` keeps version snapshots, because the document carries a `version` and a vocabulary changes
over time.

### Mappings

Ossie defines a mapping as a standalone document validated against `ontology/mapping.json`, and marks
the embedded form in the ontology document as deprecated:

> **Deprecated.** Embedded ontology maps, accepted only so existing documents continue to validate;
> write mappings as standalone documents validated against `ontology/mapping.json` instead.

A mapping document contains:

- `name`, `description`, `semantic_model`, and `concept_mappings`.
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

### Pages

A Page is the only entity here without an Ossie counterpart.

| Field | Purpose |
| :--- | :--- |
| `name` | The term, unique within the metalake |
| `domainId` | Owning domain; grants visibility through the domain |
| `owner` | Accountable person, defaulting to the creator |
| `description` | One-line summary |
| `synonyms` | Alternative names, used to match question text |
| `body` | Markdown: definition, business usage, examples |
| `status` | `DRAFT` or `PUBLISHED` |
| `PageConceptRef` | Ontology concepts this Page elaborates |
| `PageAssetRef` | Assets under `RELATED` or `SOURCE` |

The body is Markdown and is stored verbatim. Inline references to assets are links in the body; the
structured `PageAssetRef` rows are what a client or an agent can query, and an asset may appear under
`RELATED` and `SOURCE` on the same Page.

Concept references store the concept name and, when the ontology supplies one, its IRI. A reference
never stores a position in the document, so reordering or re-versioning an ontology does not break it.

### Relationship to Domains and Semantic Models

The three entities can look alike, so their boundaries are stated once:

- **Domain** answers *where in the business does this asset belong*. Membership is a tag, and a domain
  grants no access to data.
- **Ontology** answers *what concepts exist and how do they relate*. It is a machine-readable graph
  with cardinality and mappings, and it holds no assets.
- **Page** answers *what does this term mean, in prose, and who vouches for it*. It is a governed
  document.

A Page may belong to a domain and may reference concepts. Neither implies the other: a Page is not a
domain member, and a domain does not require any Page. A Semantic Model gains meaning for an agent
only once a mapping binds it to concepts, and a Page may elaborate concepts that no model maps yet.

### Status and visibility

Ontologies and Pages are created as `DRAFT`, and their owner or a manager publishes and unpublishes them
at any time, without approval. A `DRAFT` entity is returned only to its owner and to holders of the
managing privilege, consistently on REST, MCP and any UI: lists omit it, getting it returns 404, and it
is not in a domain's roll-up.

Known channels, documented rather than solved:

- creating an entity with the name of a draft fails with 409;
- a draft Page may reference published assets, and those assets are visible on their own terms;
- a draft ontology's name appears in a mapping the caller can see, if that caller can also see the
  mapping.

Assets can be tagged or referenced into a draft Page to prepare it, since that requires only the
privilege to edit the Page.

### Access control

| Privilege | Grantable on | Allows |
| :--- | :--- | :--- |
| `MANAGE_ONTOLOGY` | Metalake, ontology | Create, edit, import, publish, delete; see while `DRAFT` |
| `USE_ONTOLOGY` | Metalake, ontology | Read published ontologies and their documents |
| `MANAGE_PAGE` | Metalake, page, domain | Create, edit, publish, delete; see while `DRAFT` |
| `USE_PAGE` | Metalake, page, domain | Read published pages and their assets |

Two rules are part of the contract:

- **Creating a mapping needs both sides.** The caller needs `USE_SEMANTIC_MODEL` on the target model
  and `USE_ONTOLOGY` on the target ontology. Checking one side would let a caller attach a vocabulary
  they do not control to a model they do, or the reverse.
- **A referenced entity is not dropped out from under its referrers.** Dropping an ontology or a
  Semantic Model that a mapping references fails with a conflict, in the shape of Domain's
  `NonEmptyDomainException`. Cascading would silently destroy a binding rather than surface the
  dependency.

None of these privileges grants access to data. Reading a Page or an ontology never implies reading the
tables a model is built on. Owners are managed through the existing `/owners/{type}/{name}` shape, with
`ONTOLOGY` and `PAGE` added as securable types.

### REST API

```
/metalakes/{metalake}/ontologies                                GET, POST
/metalakes/{metalake}/ontologies/{ontology}                     GET, PUT, DELETE
/metalakes/{metalake}/ontologies/{ontology}/ossie               GET, POST
/metalakes/{metalake}/ontologies/{ontology}/publish             POST
/metalakes/{metalake}/ontologies/{ontology}/unpublish           POST
/metalakes/{metalake}/ontology-mappings                         GET, POST
/metalakes/{metalake}/ontology-mappings/{mapping}               GET, PUT, DELETE
/metalakes/{metalake}/pages                                     GET, POST
/metalakes/{metalake}/pages/{page}                              GET, PUT, DELETE
/metalakes/{metalake}/pages/{page}/publish                      POST
/metalakes/{metalake}/pages/{page}/unpublish                    POST
/metalakes/{metalake}/pages/{page}/assets                       GET, POST, DELETE
/metalakes/{metalake}/objects/{type}/{fullName}/pages           GET
```

Ontologies and Pages live under the metalake, not under a catalog or a schema, because they are not
assets. The last endpoint is a reverse lookup from an asset to the Pages that reference it, which is
what a client needs to show context next to a table.

### MCP tools

Two read-only tool categories, enabled with `--include-tool-tags`:

| Tool | Endpoint |
| :--- | :--- |
| `list_ontologies` | `GET /ontologies?details=true` |
| `get_ontology_by_name` | `GET /ontologies/{ontology}` |
| `list_pages` | `GET /pages?details=true` |
| `get_page_by_name` | `GET /pages/{page}` |
| `list_pages_for_metadata` | `GET /objects/{type}/{fullName}/pages` |

A calling agent gets the modeled context a question needs: the Page that defines the term, and the
concepts it elaborates. Draft entities are filtered by the same rules as everywhere else, because the
MCP server forwards the caller's `Authorization` header.

### Events and audit

Creating, altering, dropping, publishing and unpublishing an ontology or a Page emits the same kind of
event as the corresponding Semantic Model or Domain operation, so existing listeners and the audit log
see them without new plumbing. Mapping creation and deletion emit their own event, because they change
what an agent can resolve.

## Alternatives Considered

**Store Pages as ontology concepts.** Rejected. Ossie's concept `type` is a closed enum of
`EntityType` and `ValueType`, and a concept has no body, owner, synonyms, status or comments. A Page
would either not validate or would overload a value type to mean something else.

**Embed the mapping in the ontology document.** Rejected. Ossie marks that form deprecated and directs
writers to standalone mapping documents.

**Add a separate Glossary entity.** Rejected. The reference implementation defines terms through Pages,
and a glossary over Pages is a listing, not a second store.

**Make Pages members of a domain by tagging them.** Rejected. Domain members are leaf assets — tables,
views, filesets, topics, models and semantic models. A Page is not an asset and has no data behind it;
modelling it as a member would make "domain contains a document" indistinguishable from "domain
contains data".

**Put ontologies at the schema scope, like Semantic Models.** Rejected for now. A vocabulary describes
a business, not a catalog, and a schema-scoped ontology would be pinned to the catalog whose schema
holds it while mapping models from elsewhere. This is the most likely point of disagreement and is
recorded as an open question.

## Prior Art

Unity Catalog semantics (Databricks): metric views, domains, pages and certification, forming the
human-modeled layer of the Genie Ontology. A domain groups assets by governed tags; a Page is a
governed definition of a business concept, belongs to a domain, and is the source Genie cites when it
answers using a defined term.

Apache Ossie: `core-spec/spec.md` defines Semantic Models, datasets, fields, metrics, dimensions and
relationships; `ontology/ontology.json` defines the concept graph, and `ontology/mapping.json` the
mapping from a semantic model onto it. The Ontology section is separate from the core specification,
which is why this design is separate from the Semantic Model design.

## Open Questions

1. **Scope of `Ontology`.** Metalake, as argued here, or schema like Semantic Model?
2. **Placement of `OntologyMapping`.** A metalake object, or a child of the Semantic Model it maps? The
   Ossie document embeds `semantic_model`, which argues for the child; reuse across models argues for the
   object.
3. **Page and concepts.** Does a Page reference concepts, as designed, or is a Page the narrative of one
   concept? Referencing keeps the ontology document conformant.
4. **Privileges for Pages.** Reuse `MANAGE_DOMAIN` / `USE_DOMAIN` because Pages live under domains, or
   keep `PAGE` privileges distinct?
5. **Stable identity of a concept.** A concept is identified by name, with an optional IRI. A rename
   breaks every reference. Should Gravitino mint an internal concept id, and how would it appear in the
   exported document?
6. **Specification version.** `0.2.0.dev0` is pre-release. Does a stored ontology keep the version it was
   written against and get migrated on write, or is it pinned as Semantic Models are?

## Task Breakdown

### Phase 0: Prerequisites

- [ ] Confirm the scope split between the concept graph and the narrative layer, so reviewers do not
      read Pages as Ossie conformance
- [ ] Agree the open questions above before implementation

### Phase 1: Ontology

- [ ] `Ontology` identity, status, Java API, exceptions and change types
- [ ] EntityStore persistence, relational tables and upgrade scripts, version snapshots
- [ ] Ossie ontology validation: closed root, required `verbalizes`, closed concept `type`
- [ ] Ossie import and export for ontology documents
- [ ] Server services, REST resources and OpenAPI definitions
- [ ] Privileges, ownership and authorization filtering

### Phase 2: Mappings

- [ ] `OntologyMapping` entity, storage and API
- [ ] Mapping document validation against the Ossie mapping schema
- [ ] Dual-side authorization and reference integrity on drop
- [ ] REST endpoints and tests

### Phase 3: Pages

- [ ] `Page` entity, status, owner, concept references and asset references
- [ ] Storage, domain attachment and reverse lookup from an asset
- [ ] REST endpoints and tests

### Phase 4: Collaboration

- [ ] Comments on published Pages
- [ ] Suggestion batches, accepted or rejected as a whole
- [ ] Visibility rules for draft Pages shared with collaborators

### Phase 5: Clients, MCP and documentation

- [ ] Java and Python clients
- [ ] MCP tool categories `ontology` and `page`
- [ ] OpenAPI specification and user documentation
- [ ] Web UI for browsing and authoring Pages