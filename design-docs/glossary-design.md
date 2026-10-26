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
| 0.1     | Qi Yu  | 2026-10-26 | Draft  |

Inputs: the Define/Glossary product prototype, product answers to the glossary questions
(Q1–Q9, `G*` numbers refer to the glossary PRD), "PRD Privilege Model 2.0", and the Domain
design (apache/gravitino#13867). Items marked **[pending]** wait for a product follow-up.

---

## Background

Gravitino can describe *where* data lives (catalogs, schemas, tables) and, with Domains
(#13867), *which business area* it belongs to. It cannot describe *what a business word
means*: "Active Customer", "Net Revenue" or "Churn" exist only in wikis, BI tools and people's
heads. As a result:

- Agents asked "what was churn last quarter?" guess which table, column or metric "churn"
  refers to, and different agents guess differently.
- The same word is defined differently by different teams, with no owner and no review.
- Nothing links a definition to the tables and metrics that implement it, so a steward cannot
  see the impact of changing a definition, and a user cannot find the data behind a term.

A business glossary is a governed set of **terms**: a name, a definition, synonyms, an owner,
the domains it belongs to, its relations to other terms, and the assets that implement it.

---

## Key Decisions

The decisions below shape everything else; reviewers should start here.

| #   | Decision                                                                                                                                                             | Source                  |
| --- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------- |
| D1  | **No glossary container.** Terms live directly under the metalake, organized by Domain and by the broader/narrower hierarchy                                         | Q1                      |
| D2  | **Names unique per metalake, case-insensitive**; synonyms may be shared, and lookup returns every match                                                              | Q2 (case **[pending]**) |
| D3  | **Two relations only**: `BROADER` (multi-parent, cross-domain, acyclic; narrower is derived) and `RELATED` (symmetric). No `CONTAINS`                                | Q3, Q4                  |
| D4  | **A term belongs to zero or more domains through an explicit relation**, not through tags, because domain membership decides who can manage the term                 | Q5                      |
| D5  | **Privileges follow the 2.0 PRD**: `VIEW_DEFINITION`, `CREATE_DEFINITION`, `MANAGE_DEFINITIONS`, grantable on the metalake or a domain, never on one term; deny wins | Q7                      |
| D6  | **OSS has no approval workflow**: edits apply at once, status is set through the API, every content change creates a readable version. Enterprise adds review on top | Q6                      |
| D7  | **Bindings are explicit** term → asset (or metric) links; a binding never grants access to the asset                                                                 | Q6, Q7                  |
| D8  | **Agents read only `APPROVED` terms** through read-only MCP tools; lookup is exact, case-insensitive on name and synonyms                                            | Q9                      |

---

## Goals

1. **Term model**: create, read, update, delete and version terms with name, definition,
   synonyms, owner, domains, documentation links, relations and source reference.
2. **Hierarchy**: a term can have several broader terms, in any domain; the server rejects
   cycles and returns the broader/narrower tree.
3. **Domain organization**: list the terms of a domain (including its subdomains), and the
   domains of a term.
4. **Bindings**: link a term to tables, views, filesets, topics, models, semantic models,
   columns and semantic-model metrics; list the bindings in both directions, filtered by the
   caller's access to each asset.
5. **Lifecycle**: `DRAFT` → `APPROVED` → `DEPRECATED` (with an optional replacement), set
   through the API; agents see only `APPROVED` terms.
6. **Privileges**: the three DEFINITION privileges on the metalake and on domains, inherited by
   subdomains, deny over allow, no self-approval.
7. **Business-term lookup**: one call resolves a word to the matching approved terms with their
   definitions and bindings, for REST and MCP.
8. **CSV import and export** with a dry-run preview (G16).

## Non-Goals

1. **Approval workflow in OSS** (G6): review states, pending versions and approver assignment
   are Enterprise features; OSS only provides the hooks (see Lifecycle and versions).
2. **Fuzzy or semantic lookup**: ranked and fuzzy matching is Enterprise 2.1 (G14).
3. **Recording undefined lookups**: an Enterprise 2.0 feature; OSS emits the lookup event it
   needs.
4. **Git-sourced terms**: out of 2.0 scope (Q8).
5. **Agent writes**: the 2.0 MCP tools are read-only; proposing terms is a later phase.
6. **Access through terms**: a term or binding never grants access to data, and terms carry no
   policies.
7. **UI**.

---

## Usage Scenario

1. A steward with `MANAGE_DEFINITIONS` on domain Sales creates term `Customer` in Sales.
   It starts as `DRAFT`.
2. An analyst with `CREATE_DEFINITION` on Sales creates `Active Customer` with broader term
   `Customer`, synonyms `active user`, and binds it to table `dw.sales.customers` and metric
   `sales_model.active_customers`.
3. The steward approves both. Self-approval is refused: the analyst cannot approve their own
   term even with `MANAGE_DEFINITIONS`.
4. Marketing also uses the term: the steward adds domain Marketing to `Customer`. Holders of
   `MANAGE_DEFINITIONS` on Marketing can now manage it too.
5. An agent asks for "active users". `resolve_business_term` returns `Active Customer` (matched by
   synonym), its definition, its domains, and the table and metric it is bound to, filtered to
   the ones the agent can read.
6. Later `Active Customer` is redefined as `Active Customer (90d)`. The old term is set to
   `DEPRECATED` with the new one as replacement; agents stop seeing the old term.

---

## Solution Investigations

### How terms are grouped (D1)

| Approach                                      | Pros                                                        | Cons                                                                        | Decision      |
| --------------------------------------------- | ----------------------------------------------------------- | --------------------------------------------------------------------------- | ------------- |
| Glossary container (`metalake.glossary.term`) | Familiar from DataHub/Atlas                                 | A second grouping axis next to Domain; unclear which one governs privileges | Rejected (Q1) |
| Domain + term hierarchy                       | One grouping axis shared with assets; matches the prototype | DataHub term groups need a mapping on import                                | **Chosen**    |

### How a term joins a domain (D4)

| Approach                                                                        | Pros                                                                                     | Cons                                                                                                                                                                                                         | Decision   |
| ------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------- |
| Make terms taggable and reuse domain backing tags (the path #13867 leaves open) | One membership mechanism for assets and terms                                            | Domain membership decides who manages a term; anyone with `APPLY_TAG` on a backing tag could move a term into their domain and gain control of it. #13867 rejects data grants on domains for the same reason | Rejected   |
| Explicit `term ↔ domain` relation, changed through the term API                 | Membership changes need term privileges (and review in Enterprise); cheap reverse lookup | A second membership mechanism next to tags                                                                                                                                                                   | **Chosen** |

### How a term links to assets (D7)

| Approach                | Pros                                                                               | Cons                                                                                                                                                                              | Decision   |
| ----------------------- | ---------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| Terms as tags on assets | Reuses tag association, inheritance and policies                                   | Tag privileges (`APPLY_TAG`) do not match DEFINITION privileges; tags have no definition, relations, versions or lifecycle; inheritance from schema to table is wrong for meaning | Rejected   |
| Explicit binding table  | Bindings follow term privileges, can be reviewed, and point at metrics and columns | New storage and cleanup on asset drop                                                                                                                                             | **Chosen** |

---

## Proposal

### Data model

```
GlossaryTerm  { id, name, definition, synonyms[], status, replacedBy?, docs[{label, url}],
                sourceRef?, properties, version, audit_info }
TermVersion   { termId, version, definition, synonyms[], broader[], related[], domains[],
                bindings[], audit_info }                 // immutable snapshot per version
TermRelation  { fromTermId, toTermId, type: BROADER | RELATED, audit_info }
TermDomain    { termId, domainId, audit_info }
TermBinding   { termId, objectId, objectType, metric?, audit_info }
```

| Table                        | Key / index                                                                                           | Notes                                                            |
| ---------------------------- | ----------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| `glossary_term_meta`         | unique `(metalake_id, lower(term_name), deleted_at)`                                                  | `current_version`, `status`, `replaced_by_term_id`, `source_ref` |
| `glossary_term_synonym`      | `(metalake_id, lower(synonym), deleted_at)`                                                           | one row per synonym, for exact lookup                            |
| `glossary_term_version_info` | unique `(term_id, version)`                                                                           | append-only; read by `?version=`                                 |
| `glossary_term_rel`          | unique `(from_term_id, to_term_id, rel_type, deleted_at)`                                             | `RELATED` stored once with `from < to`                           |
| `glossary_term_domain_rel`   | unique `(term_id, domain_id, deleted_at)`, index `(domain_id)`                                        | reverse lookup "terms of a domain"                               |
| `glossary_term_binding`      | unique `(term_id, object_id, object_type, metric_name, deleted_at)`, index `(object_id, object_type)` | `metric_name` only for `SEMANTIC_MODEL`                          |

- `GLOSSARY_TERM` becomes a `MetadataObject.Type` so the owner API, events and audit work. It
  is **not taggable** and **not grantable**: role APIs reject privileges on a single term (D5).
- Name rules follow tag and domain names (up to 128 characters, `/` allowed and URL-encoded in
  paths, as in #13867).
- **Owner** is a user or a group, through the existing owner API (`/owners/glossary_term/{term}`).
- **Metrics** are not metadata objects; a metric binding is `(semantic model, metric name)`.
- Asset drop soft-deletes its bindings, as tag relations are cleaned up today. Asset rename
  keeps them, since bindings refer to IDs.

### Relations and hierarchy

| Relation  | Stored                                        | Shown as                    | Rules                                               |
| --------- | --------------------------------------------- | --------------------------- | --------------------------------------------------- |
| `BROADER` | on the child (A → B means "A is a kind of B") | Broader on A, Narrower on B | several parents, any domain; no self-edge; no cycle |
| `RELATED` | once per pair                                 | Related on both sides       | symmetric; no self-edge                             |

- **Setting A's broader term changes A only** (Q6): it needs privileges on A, plus
  `VIEW_DEFINITION` on B. B's version does not change.
- **Cycle check**: adding A → B fails with 400 if A is reachable from B by `BROADER` edges.
  Concurrent edits could each pass the check and together form a cycle (A → B and B → A), so
  every `BROADER` change takes an exclusive per-metalake glossary-hierarchy lock before the
  check. Hierarchy edits are rare; other term edits do not take it.
- **Depth**: no hard limit; hierarchy reads take a `depth` parameter (default 1, max 10).
- **Deprecating a parent** leaves its children unchanged (Q3).
- **Deleting a term** removes its relations, domain links and bindings in the same transaction,
  each recorded in the delete event; children keep their other parents or become top-level (Q3).
- **Import mapping** (DataHub 2.1 connector): `Contains` → `RELATED`, `Inherits From` → `BROADER`.

### Domains

- A term has zero or more domains. A term with no domain is governed by metalake grants only.
- Adding or removing a domain is a term change: it needs privileges on the term and
  `CREATE_DEFINITION` on the target domain, so nobody can move a term into a domain they cannot
  write.
- **Terms of a domain** = terms linked to the domain or to one of its visible subdomains,
  matching the Domain design's *scope*. Each entry carries its relation (`Direct` or
  `Via subdomain`).
- A term can be linked to a `DRAFT` domain; the domain's own visibility rules still apply.
- **Dropping a domain that has terms is refused** (409 `NonEmptyDomainException`), like a domain
  with subdomains. Silently unlinking terms would change who can manage them.
  This extends #13867 **[needs agreement with the Domain design]**.

### Lifecycle and versions

| Status                        | Set by                                    | Visible to                                                                              |
| ----------------------------- | ----------------------------------------- | --------------------------------------------------------------------------------------- |
| `DRAFT` (initial)             | creator                                   | owner, holders of `CREATE_DEFINITION` or `MANAGE_DEFINITIONS` on the term **[pending]** |
| `APPROVED`                    | `MANAGE_DEFINITIONS`, not the last editor | `VIEW_DEFINITION`; agents                                                               |
| `DEPRECATED` (+ `replacedBy`) | `MANAGE_DEFINITIONS`                      | `VIEW_DEFINITION`; not agents                                                           |
| `IN_REVIEW`                   | Enterprise only                           | Enterprise only                                                                         |

- Status changes through `PATCH`; OSS has no review step (G6). `DEPRECATED` → `APPROVED` is
  allowed (un-deprecate); `APPROVED` → `DRAFT` is not.
- **No self-approval**: the caller who made the last content change cannot set `APPROVED`
  (PRD 2.0). Whether OSS enforces this is **[pending]**; the design enforces it.
- **Versions** (G12): every content change (definition, synonyms, relations, domains, bindings,
  rename) increments `version` and writes an immutable snapshot. Owner, docs and properties
  changes do not. `GET /glossary/terms/{term}?version=n` reads an old version.
- Editing an `APPROVED` term keeps it `APPROVED` in OSS. Enterprise intercepts the change and
  stores it as a pending version instead, through a `TermChangeHandler` extension point whose OSS
  implementation applies the change at once.
- **Delete** is allowed only for `DRAFT` and `DEPRECATED` terms (prototype) **[pending]**.

### Access control

**Privileges** (PRD 2.0). They cover every Define object; for the glossary they mean:

| Privilege            | Grantable on     | Glossary meaning                                                                |
| -------------------- | ---------------- | ------------------------------------------------------------------------------- |
| `VIEW_DEFINITION`    | metalake, domain | read approved and deprecated terms, their relations and bindings; resolve terms |
| `CREATE_DEFINITION`  | metalake, domain | create terms; edit and delete terms the caller owns **[pending]**               |
| `MANAGE_DEFINITIONS` | metalake, domain | edit, approve, deprecate and delete any term in scope                           |

`MANAGE_DEFINITIONS` ⊃ `CREATE_DEFINITION` ⊃ `VIEW_DEFINITION`. The built-in `PUBLIC` role holds
`VIEW_DEFINITION` on the metalake by default (needs the `PUBLIC` role from PRD 2.0; Gravitino has
none today).

**Evaluating a privilege on a term**

```
levels(term) = { metalake } ∪ { d, parent(d) : d ∈ domains(term) }
allowed(p)   = owner of term or metalake
               OR ( p allowed on some level AND p denied on no level )
```

- A domain grant applies to the domain's terms and to the terms of its subdomains, following the
  domain's current parent, as in #13867 **[pending: subdomain inheritance]**.
- **Union with deny precedence** **[pending: Q5 follow-up]**: Finance allows and Sales denies →
  a term in both is **denied**. This matches the PRD ("DENY beats ALLOW at any level") and the
  Domain design.
- Request-body targets are checked too: `CREATE_DEFINITION` on a domain being added, and
  `VIEW_DEFINITION` on a broader or related term being linked.

**What each operation needs**

| Operation                        | Required                                                                                                     |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| Get, list, hierarchy, resolve    | `VIEW_DEFINITION`; drafts per the Lifecycle table; terms the caller cannot see are omitted (get returns 404) |
| Create                           | `CREATE_DEFINITION` on every listed domain (on the metalake if no domain)                                    |
| Edit content, domains, relations | owner with `CREATE_DEFINITION`, or `MANAGE_DEFINITIONS`                                                      |
| Bind / unbind an asset           | the edit privilege above **and** read access to the asset                                                    |
| Approve, deprecate               | `MANAGE_DEFINITIONS`; not the last editor                                                                    |
| Delete                           | owner with `CREATE_DEFINITION`, or `MANAGE_DEFINITIONS` **[pending]**                                        |
| Set owner                        | current owner, or `MANAGE_GRANTS` on the metalake or one of its domains (Q6)                                 |
| Import                           | `CREATE_DEFINITION` per row's domains; updates as for edit                                                   |

**Binding results are filtered twice**: a term's bindings list only assets the caller can read,
and an asset's terms list only terms the caller can see. A binding never grants access (D7).

### REST API

Under `/api/metalakes/{metalake}`, following the Domain and Tag conventions.

| Operation         | Endpoint                                                                  | Request → response                                                     |
| ----------------- | ------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| List              | `GET /glossary/terms?details=&domain=&status=&nameContains=`              | → `NameListResponse` / `TermListResponse`                              |
| Create            | `POST /glossary/terms`                                                    | `TermCreateRequest` → `TermResponse`                                   |
| Get               | `GET /glossary/terms/{term}?version=`                                     | → `TermResponse`                                                       |
| Versions          | `GET /glossary/terms/{term}/versions`                                     | → `TermVersionListResponse`                                            |
| Alter             | `PUT /glossary/terms/{term}`                                              | `TermUpdatesRequest` → `TermResponse`                                  |
| Set status        | `PATCH /glossary/terms/{term}`                                            | `TermSetRequest{status, replacedBy?}` → `TermResponse`                 |
| Drop              | `DELETE /glossary/terms/{term}`                                           | → `DropResponse`                                                       |
| Hierarchy         | `GET /glossary/terms/{term}/hierarchy?direction=broader\|narrower&depth=` | → `TermTreeResponse`                                                   |
| Bound assets      | `GET /glossary/terms/{term}/objects?types=`                               | → `TermBindingListResponse`                                            |
| Bind / unbind     | `POST /glossary/terms/{term}/objects`                                     | `TermBindingsRequest{toAdd[], toRemove[]}` → `TermBindingListResponse` |
| Terms of an asset | `GET /objects/{type}/{fullName}/terms`                                    | → `TermListResponse`                                                   |
| Terms of a domain | `GET /domains/{domain}/terms?details=`                                    | → `TermListResponse` (with relation)                                   |
| Resolve           | `GET /glossary/resolve?text=&domain=`                                     | → `TermMatchListResponse`                                              |
| Import            | `POST /glossary/import?dryRun=true` (`text/csv`)                          | → `TermImportResponse` (per-row `CREATE` / `UPDATE` / `ERROR`)         |
| Export            | `GET /glossary/export?domain=`                                            | → `text/csv`                                                           |
| Owner             | existing `/owners/glossary_term/{term}`                                   |                                                                        |

```
TermCreateRequest { name, definition, synonyms[], domains[], broader[], related[],
                    docs[{label, url}], sourceRef?, properties }
TermUpdatesRequest{ updates: [rename, updateDefinition, addSynonym, removeSynonym,
                    addDomain, removeDomain, addBroader, removeBroader, addRelated,
                    removeRelated, setDoc, removeDoc, setProperty, removeProperty] }
TermDTO           { name, definition, synonyms, status, replacedBy, domains, broader,
                    narrower, related, docs, owner, sourceRef, version, properties, audit }
TermMatchDTO      { term: TermDTO, matchedBy: NAME | SYNONYM, bindings: [TermBindingDTO] }
TermBindingDTO    { object: MetadataObjectDTO, metric? }
```

- **Updates are applied in one transaction and create one version.** `narrower` is read-only.
- **Resolve** (G10): trims the text, compares it case-insensitively with names and synonyms, and
  returns every `APPROVED` match the caller can see, each with `matchedBy`, its domains and its
  visible bindings; `domain` narrows to a domain's scope. No match returns an empty list and
  emits `ResolveTermEvent{text, matched: false}` (Enterprise records it).
- **Import**: the CSV columns follow the prototype without `contains`:
  `name,definition,synonyms,domain,owner,broader,related,documentation,source_ref`. A row
  updates the term with the same `source_ref`, else the term with the same name, else creates one.
  `dryRun=true` validates every row and writes nothing; without it, the import is all or
  nothing. Export writes the same columns, so an export can be re-imported **[pending: Q8
  follow-up]**.
- **Errors**: 404 `NoSuchTermException` (missing or not visible); 409
  `TermAlreadyExistsException`, `NonEmptyDomainException`; 400 cycle, self-relation, illegal
  status transition, self-approval, deleting an `APPROVED` term; 403 `ForbiddenException`.
- **Clients** (Java, Python): `GlossaryOperations` on `GravitinoMetalake` (list, get, create,
  alter, setStatus, delete, resolve, import, export); `SupportsTerms` (`listTerms()`) on the leaf
  asset types and on `Domain`.

### MCP tools

A new read-only `glossary` tool tag in `mcp-server/`, added to `SUPPORTED_TOOL_TAGS`. Every tool
returns `APPROVED` terms only, and the server applies the caller's privileges through the
forwarded `Authorization` header.

| Tool                      | Endpoint                                                                         | Purpose for the agent                                                                        |
| ------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `resolve_business_term`   | `GET /glossary/resolve`                                                          | Main entry: map a word in the question to terms, definitions and the tables/metrics to query |
| `get_term`                | `GET /glossary/terms/{term}`                                                     | Full definition, synonyms, domains, relations                                                |
| `list_terms`              | `GET /glossary/terms?details=true&status=APPROVED` (+ `domain`, `name_contains`) | Browse the vocabulary of a domain                                                            |
| `get_term_hierarchy`      | `GET /glossary/terms/{term}/hierarchy`                                           | Widen ("Customer") or narrow ("Active Customer") a concept                                   |
| `list_metadata_by_term`   | `GET /glossary/terms/{term}/objects`                                             | Find the data that implements a term                                                         |
| `list_terms_for_metadata` | `GET /objects/{type}/{fullName}/terms`                                           | Explain what a table or column means                                                         |

With the `domain` tools of #13867 (`list_domains`, `list_metadata_by_domain`), an agent can go
from a domain to its vocabulary to the data. Deprecated terms are hidden (Q9); returning them
with their replacement is **[pending]**.

**Later phases**: `propose_term` (creates a `DRAFT` only), `bind_term_to_metadata` (pending
review in Enterprise), ranked/fuzzy resolve (Enterprise 2.1). Agents never approve.

### Events and audit

- Every term operation emits pre, success and failure events through a
  `GlossaryEventDispatcher` and reaches the audit log. Delete events list the relations,
  domain links and bindings removed with the term.
- `ResolveTermEvent` carries the text, the caller and whether anything matched.

### Backward compatibility

All additions: new tables, a new `GLOSSARY_TERM` object type, three new privileges and new
endpoints. Dropping a domain with terms is refused, which only affects metalakes that use the
glossary.

---

## Open Questions

1. **Domain privileges vs PRD 2.0**: #13867 defines `MANAGE_DOMAIN` / `USE_DOMAIN`, while the
   PRD says the three DEFINITION privileges cover every Define tab, Domains included. Both
   designs must use one set.
2. **[pending] Product follow-ups**: case-insensitive names; deny vs union across domains;
   subdomain inheritance; draft visibility; `CREATE_DEFINITION` edit and delete scope;
   self-approval in OSS; who approves a term in several domains (Enterprise); the source
   reference format; deprecated terms for agents.
3. **Domain drop with terms**: refuse (proposed) or unlink with an audit record.

---

## Task Breakdown

### Phase 1: Model, storage and authorization

- [ ] `GlossaryTerm` API in `api/`; `MetadataObject.Type.GLOSSARY_TERM` (not taggable, not grantable)
- [ ] `VIEW_DEFINITION`, `CREATE_DEFINITION`, `MANAGE_DEFINITIONS` privileges on metalake and domain (shared with the Domain design)
- [ ] Storage: the six tables above (H2, MySQL, PostgreSQL, upgrade scripts)
- [ ] Manager: CRUD, relations with the cycle check and hierarchy lock, domain links, status and versions, `TermChangeHandler`
- [ ] Authorization: term privilege evaluation over metalake, domains and parents with deny precedence; no self-approval; draft visibility
- [ ] REST endpoints for terms, status, versions, hierarchy; events

### Phase 2: Bindings and lookup

- [ ] Binding storage and endpoints in both directions, with cleanup on asset drop
- [ ] Terms of a domain (scope) and the domain drop check
- [ ] Resolve endpoint and `ResolveTermEvent`

### Phase 3: Clients, MCP, import/export, docs

- [ ] Java and Python clients
- [ ] MCP `glossary` tools
- [ ] CSV import (dry run) and export
- [ ] OpenAPI spec (validated with `./gradlew :docs:build`) and user docs
