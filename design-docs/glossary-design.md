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

# Design: Business Glossary

| Version | Author | Date       | Status |
| ------- | ------ | ---------- | ------ |
| 0.3     | Qi Yu  | 2026-10-27 | Draft  |

This design covers the open-source part of the Business Glossary PRD (requirements G1–G10) and
builds on the Domain design (apache/gravitino#13867). Enterprise features (G11–G18: approval
workflow, versions, CSV import/export, UI) plug in through the hooks described here.

## Goals

1. **Term object** (G1): a metalake-level `GlossaryTerm` with name, definition, synonyms, owner,
   status, domains, properties and an optional source reference. There is no glossary container;
   terms are organized by domain and by the term hierarchy.
2. **Hierarchy** (G2): `BROADER` (a term can have several, in any domain; narrower is derived) and
   `RELATED` (symmetric). The server rejects cycles.
3. **Bindings** (G3, G4): a term binds to catalogs, schemas, tables, views, columns, filesets,
   topics, models and semantic-model metrics. A binding never grants access.
4. **Domains** (G5): a term belongs to zero or more domains through an explicit relation, and
   domain grants govern it.
5. **Status** (G6): `DRAFT`, `APPROVED`, `DEPRECATED`, set through the API. Agents see `APPROVED`
   terms only.
6. **Privileges** (G7): `VIEW_DEFINITION`, `CREATE_DEFINITION`, `MANAGE_DEFINITIONS` on the
   metalake or a domain, inherited by subdomains, deny over allow, the same on REST and MCP.
7. **REST, clients, MCP and audit** (G8–G10), including an exact, case-insensitive lookup from a
   business word to approved terms and their bindings.

## Non-Goals

1. **Approval workflow, versions, CSV import/export, KB articles, UI** (G11–G13, G15, G16):
   Enterprise 2.0. OSS edits apply at once; the hook is `TermChangeHandler` (see Status).
2. **Ranked or fuzzy lookup, SKOS, connectors** (G14, G17, G18): Enterprise 2.1.
3. **Access through terms**: a term or binding never grants or denies data access and carries no
   policies.
4. **Agent writes**: the MCP tools are read-only.

## Usage Scenario

1. A steward with `MANAGE_DEFINITIONS` on domain Sales creates term `Customer` in Sales. It starts
   as `DRAFT`, visible only to `CREATE_DEFINITION` and `MANAGE_DEFINITIONS` holders.
2. An analyst with `CREATE_DEFINITION` on Sales creates `Active Customer` with broader term
   `Customer` and synonym `active user`, and binds it to column `dw.sales.customers.is_active` and
   metric `active_customers` of semantic model `dw.sales.sales_model`. The analyst owns this term
   and can edit it, but cannot edit `Customer`.
3. The steward sets both terms to `APPROVED`; users with `VIEW_DEFINITION` (by default `PUBLIC`)
   now see them.
4. An agent asks about "active users". `resolve_business_term` returns `Active Customer` (matched
   by synonym), its definition and the column and metric it is bound to, filtered to what the
   agent can read.
5. The term is later replaced. The steward sets it to `DEPRECATED`, so agents stop seeing it, and
   then deletes it.

## Design

### Data model

```
GlossaryTerm { id, name, definition, synonyms[], status: DRAFT | APPROVED | DEPRECATED,
               sourceRef?, properties, audit_info }
TermRelation { fromTermId, toTermId, type: BROADER | RELATED, audit_info }
TermDomain   { termId, domainId, audit_info }
TermBinding  { termId, objectId, objectType, metricName?, audit_info }
```

| Table                      | Unique key                                                                     | Other index                              |
| -------------------------- | ------------------------------------------------------------------------------ | ---------------------------------------- |
| `glossary_term_meta`       | `(metalake_id, name_key, deleted_at)`, `(metalake_id, source_ref, deleted_at)` |                                          |
| `glossary_term_synonym`    | `(term_id, synonym_key, deleted_at)`                                           | `(metalake_id, synonym_key, deleted_at)` |
| `glossary_term_rel`        | `(from_term_id, to_term_id, rel_type, deleted_at)`                             | `(to_term_id, rel_type)`                 |
| `glossary_term_domain_rel` | `(term_id, domain_id, deleted_at)`                                             | `(domain_id)`                            |
| `glossary_term_binding`    | `(term_id, object_type, object_id, metric_name, deleted_at)`                   | `(object_type, object_id)`               |

- Tables go into the H2, MySQL and PostgreSQL schemas and the `upgrade-1.3.0-to-2.0.0` scripts.
- `name_key` and `synonym_key` are the trimmed, lower-cased values, so uniqueness and lookup are
  case-insensitive on every backend; responses return the name as created.
- `RELATED` is stored once per pair with `from_term_id < to_term_id`. `BROADER` is stored on the
  child: A → B means "A is a kind of B".
- `MetadataObject.Type.GLOSSARY_TERM` is added for owners, events and audit. It is **not
  taggable** and **not grantable**: the role APIs reject privileges on a single term.
- Terms, their relations and bindings are not cached; every request reads the store, as for
  domains in #13867.

### Names and synonyms

- Names are unique per metalake, case-insensitively, and follow the tag and domain name rules (up
  to 128 characters, `/` allowed and URL-encoded in paths).
- Two terms may share a synonym. A synonym cannot equal another term's name: adding a synonym
  checks names, and creating or renaming a term checks other terms' synonyms (400).
- `sourceRef` is optional and unique per metalake when set; Enterprise import matches on it first,
  then on the name.

### Relations

- Setting A's broader or related term is a change to A only. It needs the edit privilege on A and
  `VIEW_DEFINITION` on the other term.
- **Cycle check**: adding A → B fails with 400 if A is reachable from B through `BROADER` edges.
  The walk reads ancestors of B breadth-first, under the hierarchy lock (see Locking).
- No self-edges. Hierarchy reads take `depth` (default 1, at most 10).
- Deprecating a term leaves its children unchanged. Deleting a term removes its edges; each child
  that loses a broader term gets its own audit record, and keeps its other parents or becomes
  top-level.

### Domains

- A term with no domain is governed by metalake grants only.
- Adding a domain needs the edit privilege on the term and `CREATE_DEFINITION` on that domain, so
  nobody can move a term into a domain they cannot write.
- **Terms of a domain** use the Domain design's scope: terms linked to the domain or to a
  subdomain the caller can see, each reported as `Direct` or `Via subdomain`.
- Dropping a domain that still has terms fails with 409 `NonEmptyDomainException`, like a domain
  with subdomains; unlinking them silently would change who manages the terms. This is one added
  check in `DomainManager#dropDomain`.

### Bindings

- Targets: `CATALOG`, `SCHEMA`, `TABLE`, `VIEW`, `COLUMN`, `FILESET`, `TOPIC`, `MODEL`, and a
  metric as (`SEMANTIC_MODEL`, `metricName`). Binding a missing object or metric fails with 404.
- A binding is direct only: binding a schema does not bind the tables in it.
- Binding needs the edit privilege on the term **and** read access to the object.
- Reads are filtered both ways: a term's bindings list only objects the caller can access
  (`filterMetadataObject`), and an object's terms list only terms the caller can see.
- Dropping an object soft-deletes its bindings, in the same places that clean up
  `tag_relation_meta` (by object, schema, catalog and metalake). Renames keep them, since bindings
  refer to IDs.

### Status

| Status            | Visible to                                  | Set by               | Delete                                                  |
| ----------------- | ------------------------------------------- | -------------------- | ------------------------------------------------------- |
| `DRAFT` (initial) | `CREATE_DEFINITION` or `MANAGE_DEFINITIONS` | creation             | owner with `CREATE_DEFINITION`, or `MANAGE_DEFINITIONS` |
| `APPROVED`        | `VIEW_DEFINITION`; MCP                      | `MANAGE_DEFINITIONS` | not allowed; deprecate first                            |
| `DEPRECATED`      | `VIEW_DEFINITION`; not MCP                  | `MANAGE_DEFINITIONS` | `MANAGE_DEFINITIONS`                                    |

- In OSS, a `MANAGE_DEFINITIONS` holder on any of the term's domains or on the metalake sets the
  status directly with `PATCH`. There are no submissions, so no self-approval rule.
- Edits apply at once in any status and are audited. OSS keeps only the current term.
- **Enterprise hook**: `GlossaryManager` sends every change through a `TermChangeHandler`. The OSS
  implementation applies it; Enterprise replaces it to store proposals, add `IN_REVIEW`, block
  self-approval and keep versions (G12, G13).

### Locking

Term operations use the store's existing primitives, in one order: the live-endpoint fence
(`LiveEndpointService.lockLiveEndpoint`, shared: the metalake, plus the bound object's chain when
binding), the per-metalake hierarchy lock if needed, term rows in ID order (exclusive), then domain
rows (shared). Each row is locked once, in its final mode.

| Operation                       | Locks                                                        | Conflict it excludes                 |
| ------------------------------- | ------------------------------------------------------------ | ------------------------------------ |
| Create                          | metalake (shared) → domains (shared)                         | metalake or domain drop              |
| Edit content, synonyms, domains | term (exclusive) → added domains (shared)                    | concurrent edits; domain drop        |
| Add a `BROADER` edge            | metalake (shared) → hierarchy lock → child (exclusive)       | two edges that together form a cycle |
| Add a `RELATED` edge            | both terms (exclusive, ID order)                             | the other term being deleted         |
| Bind                            | object chain (`lockLiveEndpoint`, shared) → term (exclusive) | object drop                          |
| Set status, delete              | term (exclusive)                                             | concurrent edits                     |

- The **hierarchy lock** is a `SELECT … FOR UPDATE` on a per-metalake row in
  `glossary_hierarchy_lock`, inserted on first use. Locking only the two terms is not enough: the
  edges A → B and C → D can be added concurrently, then B → C and D → A, each passing its own
  check. `BROADER` edits are rare, so serializing them per metalake is cheap.
- Name-addressed operations resolve the name, lock the row, then re-check the name and
  `deleted_at`; otherwise 404. The name unique key backs up concurrent creates (409).

### Access control

| Privilege            | On the metalake or a domain                                                             |
| -------------------- | --------------------------------------------------------------------------------------- |
| `VIEW_DEFINITION`    | Read `APPROVED` and `DEPRECATED` terms, relations and bindings; resolve                 |
| `CREATE_DEFINITION`  | Create terms; see `DRAFT` terms; edit and bind terms the caller owns; delete own drafts |
| `MANAGE_DEFINITIONS` | Edit and bind any term; set status; delete deprecated terms                             |

- `MANAGE_DEFINITIONS` includes `CREATE_DEFINITION`, which includes `VIEW_DEFINITION`. The
  privileges are shared with the other Define objects. `PUBLIC` holds `VIEW_DEFINITION` on the
  metalake by default, and an admin can revoke it.
- **Levels of a term**: the metalake, each of its domains, and each domain's current parent. A
  privilege holds when it is allowed on at least one level and denied on none. If Finance allows
  `VIEW_DEFINITION` and Sales denies it, a role cannot see a term in both. The metalake owner
  always passes.
- **Owner** (user or group) through `/owners/glossary_term/{term}`. Changing it needs the current
  owner, or `MANAGE_GRANTS` on the metalake or one of the term's domains.
- **Create** needs `CREATE_DEFINITION` on every listed domain, or on the metalake when none is
  listed; the creator becomes the owner.
- **Implementation**: a `TermScopeResolver` returns a term's levels from
  `glossary_term_domain_rel` and the Domain design's parent resolver, batched for list filtering.
  `JcasbinAuthorizer#hasMetadataPrivilegePermission` uses it for `GLOSSARY_TERM` where it walks to
  the parent today, combining the levels in one check as for catalog, schema and table. Domains
  and terms named in a request body are checked imperatively.

### REST API

Under `/api/metalakes/{metalake}`, with the Domain and Tag conventions: `?details=` on lists, `PUT`
with updates to alter, `PATCH` to set status, `DropResponse` on delete.

| Operation          | Endpoint                                                                | Request → response                                                 |
| ------------------ | ----------------------------------------------------------------------- | ------------------------------------------------------------------ |
| List               | `GET /glossary/terms?details=&domain=&status=&nameContains=&sourceRef=` | → `NameListResponse` / `TermListResponse`                          |
| Create             | `POST /glossary/terms`                                                  | `TermCreateRequest` → `TermResponse`                               |
| Get                | `GET /glossary/terms/{term}`                                            | → `TermResponse`                                                   |
| Alter              | `PUT /glossary/terms/{term}`                                            | `TermUpdatesRequest` → `TermResponse`                              |
| Set status         | `PATCH /glossary/terms/{term}`                                          | `TermSetRequest{status}` → `TermResponse`                          |
| Drop               | `DELETE /glossary/terms/{term}`                                         | → `DropResponse`                                                   |
| Hierarchy          | `GET /glossary/terms/{term}/hierarchy?direction=&depth=`                | → `TermTreeResponse`                                               |
| List bindings      | `GET /glossary/terms/{term}/objects?types=`                             | → `TermBindingListResponse`                                        |
| Bind / unbind      | `POST /glossary/terms/{term}/objects`                                   | `TermBindingsRequest{toAdd, toRemove}` → `TermBindingListResponse` |
| Terms of an object | `GET /objects/{type}/{fullName}/terms`                                  | → `TermListResponse`                                               |
| Terms of a domain  | `GET /domains/{domain}/terms?details=`                                  | → `TermListResponse`                                               |
| Resolve            | `GET /glossary/resolve?text=&domain=`                                   | → `TermMatchListResponse`                                          |

```
TermCreateRequest  { name, definition, synonyms, domains, broader, related, sourceRef, properties }
TermUpdatesRequest { updates: [rename, updateDefinition, addSynonym, removeSynonym, addDomain,
                     removeDomain, addBroader, removeBroader, addRelated, removeRelated,
                     setSourceRef, setProperty, removeProperty] }
TermDTO            { name, definition, synonyms, status, domains, broader, narrower, related,
                     owner, sourceRef, properties, audit }
TermBindingDTO     { object: MetadataObjectDTO, metricName? }
TermMatchDTO       { term: TermDTO, matchedBy: NAME | SYNONYM, bindings: [TermBindingDTO] }
```

- **Alter** applies all updates in one transaction. `narrower` is derived and read-only.
- **Resolve** compares the trimmed text with `name_key` and `synonym_key` and returns every
  `APPROVED` match the caller can see, with its visible bindings; `domain` narrows to a domain's
  scope. A word that matches only `DEPRECATED` terms returns an empty list.
- **Errors**: 404 `NoSuchTermException` (missing or not visible); 409 `TermAlreadyExistsException`
  (name or source reference) and `NonEmptyDomainException`; 400 for a cycle, a self-edge, a synonym
  equal to another term's name, or deleting an `APPROVED` term; 403 `ForbiddenException`.
- **Clients** (Java, Python): `GlossaryOperations` on `GravitinoMetalake` (list, get, create, alter,
  setStatus, drop, resolve, hierarchy, bind); `SupportsTerms#listTerms()` on the bindable objects
  and on `Domain`.

### MCP tools

A read-only `glossary` tool tag in `mcp-server/`, added to `SUPPORTED_TOOL_TAGS`. Every tool
returns `APPROVED` terms only; the MCP server forwards the caller's `Authorization` header, so the
server's filtering applies.

| Tool                      | Endpoint                                           | Use                                                                      |
| ------------------------- | -------------------------------------------------- | ------------------------------------------------------------------------ |
| `resolve_business_term`   | `GET /glossary/resolve`                            | Map a word in a question to terms and the columns or metrics behind them |
| `get_term`                | `GET /glossary/terms/{term}`                       | Definition, synonyms, domains, relations                                 |
| `list_terms`              | `GET /glossary/terms?details=true&status=APPROVED` | Browse a domain's vocabulary (`domain`, `name_contains`)                 |
| `get_term_hierarchy`      | `GET /glossary/terms/{term}/hierarchy`             | Widen or narrow a concept                                                |
| `list_metadata_by_term`   | `GET /glossary/terms/{term}/objects`               | Find the data that implements a term                                     |
| `list_terms_for_metadata` | `GET /objects/{type}/{fullName}/terms`             | Explain what an object means                                             |

Together with the `domain` tools of #13867, an agent can go from a domain to its terms to the data.

### Events and audit

- Every operation emits pre, success and failure events through a `GlossaryEventDispatcher` and
  reaches the audit log (G8): create, alter, status, bind and unbind, drop. A drop also records one
  entry for each child that lost a broader term.
- `ResolveTermEvent{text, caller, defined}` is emitted on every resolve; `defined` is `true` when
  any term of any status matches. Enterprise records lookups with `defined = false` as undefined
  terms.

## Alternatives Considered

1. **A glossary container** (`metalake.glossary.term`). It adds a second grouping axis next to
   Domain, and product chose Domain as the only grouping.
2. **Domain membership through backing tags**, as for assets in #13867. Domain membership decides
   who manages a term, so anyone with `APPLY_TAG` on a backing tag could move a term into a domain
   and change who controls it; #13867 rejects data grants on domains for the same reason.
3. **Terms as tags on assets.** `APPLY_TAG` does not match the DEFINITION privileges, tags have no
   definition, relations or status, and tags carry policies while bindings must not affect access.

## Open Questions

1. **G5 wording**: G5 asks for "the same membership model as other domain members", which in
   #13867 is backing tags. This design uses an explicit relation (Alternative 2). Product and the
   Domain design need to agree.
2. **One privilege set for Define objects**: #13867 defines `MANAGE_DOMAIN` / `USE_DOMAIN`, while
   the 2.0 privilege model uses the DEFINITION privileges for every Define object.
3. **`PUBLIC` role**: Gravitino has none today; G7 needs it from the 2.0 privilege model work.

## Task Breakdown

### Phase 1: Terms, storage and authorization

- [ ] `GlossaryTerm` API and `MetadataObject.Type.GLOSSARY_TERM` in `api/`; DEFINITION privileges on metalake and domain
- [ ] Storage: the five glossary tables and `glossary_hierarchy_lock` (H2, MySQL, PostgreSQL, upgrade scripts)
- [ ] `GlossaryManager`: CRUD, name and synonym rules, relations with the cycle check, domain links, status, `TermChangeHandler`, locking
- [ ] Authorization: `TermScopeResolver`, the `GLOSSARY_TERM` case in `JcasbinAuthorizer`, draft visibility, owner-only edits for `CREATE_DEFINITION`
- [ ] REST endpoints for terms, status and hierarchy; `GlossaryEventDispatcher`; the domain drop check

### Phase 2: Bindings and lookup

- [ ] Binding storage, endpoints from both sides, cleanup on object drop
- [ ] Terms of a domain; resolve endpoint; `ResolveTermEvent`

### Phase 3: Clients, MCP and docs

- [ ] Java and Python clients
- [ ] MCP `glossary` tools
- [ ] OpenAPI spec (validated with `./gradlew :docs:build`) and user docs
