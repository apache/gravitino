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
| 0.2     | Qi Yu  | 2026-10-27 | Draft  |

Inputs: the Business Glossary PRD (requirements `G1`–`G18`), product answers to the glossary
questions (`Q1`–`Q9`), the 2.0 privilege model PRD, and the Domain design (apache/gravitino#13867).
This doc covers the open-source part (PRD release "OSS 2.0") and the hooks the Enterprise part
needs.

---

## Background

Gravitino can describe *where* data lives (catalogs, schemas, tables) and, with Domains
(#13867), *which business area* it belongs to. It cannot describe *what a business word
means*: "Active Customer", "Net Revenue" or "Churn" exist only in wikis, BI tools and people's
heads. As a result:

- Agents asked "what was churn last quarter?" guess which table, column or metric "churn"
  refers to, and different agents guess differently.
- The same word is defined differently by different teams, with no owner and no status.
- Nothing links a definition to the tables and metrics that implement it, so a steward cannot
  see the impact of changing a definition, and a user cannot find the data behind a term.

A business glossary is a governed set of **terms**: a name, a definition, synonyms, an owner, a
status, the domains it belongs to, its relations to other terms, and the assets and metrics that
implement it (G1–G5).

---

## Key Decisions

The decisions below shape everything else; reviewers should start here.

| #   | Decision                                                                                                                                                                                                                            | Source                       |
| --- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------- |
| D1  | **No glossary container.** Terms live directly under the metalake, organized by Domain and by the broader/narrower hierarchy                                                                                                        | Q1                           |
| D2  | **Names unique per metalake, case-insensitive**; a term keeps the case it was created with. Synonyms may be shared between terms, but a synonym cannot equal another term's name                                                    | Q2                           |
| D3  | **Two relations only**: `BROADER` (several parents, any domain, acyclic; narrower is derived) and `RELATED` (symmetric). No `CONTAINS`                                                                                              | G2, Q3, Q4                   |
| D4  | **A term joins domains through an explicit relation**, not through tags, because domain membership decides who can manage the term                                                                                                  | G5, Q5 (see Open Question 1) |
| D5  | **Privileges**: `VIEW_DEFINITION`, `CREATE_DEFINITION`, `MANAGE_DEFINITIONS`, grantable on the metalake or a domain (inherited by subdomains), never on one term; allows add up across domains, any deny wins                       | G7, Q5, Q7                   |
| D6  | **OSS has a plain status, no workflow**: `DRAFT`, `APPROVED`, `DEPRECATED`, set through the API by `MANAGE_DEFINITIONS`. Edits apply at once and are audited; no versions. Review, self-approval checks and versions are Enterprise | G6, G8, G12, G13, Q6         |
| D7  | **Bindings are explicit** term → asset or metric links; a binding never grants access to the asset                                                                                                                                  | G3, G4, G7                   |
| D8  | **Agents read only `APPROVED` terms** through read-only MCP tools; lookup is exact and case-insensitive on names and synonyms                                                                                                       | G10, Q9                      |

---

## Goals

1. **Term object** (G1): create, read, update and delete terms with name, definition, synonyms,
   owner, status, domains, custom properties and an optional source reference.
2. **Hierarchy** (G2): a term can have several broader terms, in any domain; the server rejects
   cycles and returns the broader/narrower tree.
3. **Bindings** (G3, G4): bind a term to catalogs, schemas, tables, views, columns, filesets,
   topics, models and semantic-model (Ossie) metrics; list bindings from both sides, filtered by
   the caller's access to each asset.
4. **Domains** (G5): a term belongs to zero or more domains; list the terms of a domain, including
   its subdomains.
5. **Status** (G6): `DRAFT` → `APPROVED` → `DEPRECATED`, set through the API.
6. **Privileges** (G7): the three DEFINITION privileges on the metalake and domains, with
   `VIEW_DEFINITION` granted to `PUBLIC` by default and read results filtered by privilege.
7. **Audit** (G8): every create, edit, status change and binding change is audited.
8. **REST and MCP** (G9, G10): CRUD for terms, relations and bindings; list by domain; reverse
   lookup from an asset; one lookup call that resolves a word to approved terms and their bindings.

## Non-Goals

1. **Approval workflow, versions and certification** (G12, G13): Enterprise 2.0. OSS provides the
   extension point (see Status).
2. **CSV import and export** (G16): Enterprise 2.0. OSS stores the source reference that import
   matches on.
3. **KB articles** (G11), **notifications** (G13a), **SKOS export** (G17) and **connectors** (G18):
   Enterprise.
4. **Ranked or fuzzy lookup** (G14): Enterprise 2.1.
5. **Recording undefined lookups**: Enterprise 2.0. OSS emits the event it needs.
6. **Agent writes**: the 2.0 MCP tools are read-only.
7. **Access through terms**: a term or binding never grants or denies access, and terms carry no
   policies.
8. **Git-managed glossaries and UI**.

---

## Usage Scenario

1. A steward with `MANAGE_DEFINITIONS` on domain Sales creates term `Customer` in Sales.
   It starts as `DRAFT`, visible only to `CREATE_DEFINITION` and `MANAGE_DEFINITIONS` holders.
2. An analyst with `CREATE_DEFINITION` on Sales creates `Active Customer` with broader term
   `Customer` and synonym `active user`, and binds it to column `dw.sales.customers.is_active`
   and metric `sales_model.active_customers`. The analyst owns the term and can edit it; they
   cannot edit `Customer`.
3. The steward sets both terms to `APPROVED`. Users with `VIEW_DEFINITION`, including `PUBLIC`,
   now see them.
4. Marketing also uses `Customer`: the steward adds domain Marketing. `MANAGE_DEFINITIONS` holders
   on Marketing can now manage it too, unless a deny on Sales or Marketing blocks them.
5. An agent asks for "active users". `resolve_business_term` returns `Active Customer` (matched by
   synonym), its definition, its domains, and the column and metric it is bound to, filtered to
   the ones the agent can read.
6. `Active Customer` is later replaced by a new term. The steward sets the old one to
   `DEPRECATED`; agents stop seeing it, and the steward can then delete it.

---

## Solution Investigations

### How terms are grouped (D1)

| Approach                                      | Pros                                 | Cons                                                                        | Decision      |
| --------------------------------------------- | ------------------------------------ | --------------------------------------------------------------------------- | ------------- |
| Glossary container (`metalake.glossary.term`) | Familiar from DataHub term groups    | A second grouping axis next to Domain; unclear which one governs privileges | Rejected (Q1) |
| Domain + term hierarchy                       | One grouping axis shared with assets | DataHub term groups need a mapping on import                                | **Chosen**    |

### How a term joins a domain (D4)

| Approach                                                        | Pros                                                                                                    | Cons                                                                                                                                                                                                                                                                       | Decision   |
| --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| Make terms taggable and reuse domain backing tags (#13867)      | The same membership model as assets                                                                     | Domain membership decides who manages a term (G7); anyone with `APPLY_TAG` on a backing tag could move a term into a domain and change who controls it. #13867 rejects data grants on domains for the same reason. Catalog and schema inheritance has no meaning for terms | Rejected   |
| Explicit `term ↔ domain` relation, changed through the term API | Membership changes need term privileges; cheap reverse lookup; same storage pattern as tag associations | A second membership mechanism next to tags                                                                                                                                                                                                                                 | **Chosen** |

### How a term links to assets (D7)

| Approach                                                      | Pros                                                                                    | Cons                                                                                                                                                            | Decision   |
| ------------------------------------------------------------- | --------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| Terms as tags on assets                                       | Reuses tag association, inheritance and policies                                        | `APPLY_TAG` does not match DEFINITION privileges; tags have no definition, relations or status; tags carry policies, while bindings must not affect access (G7) | Rejected   |
| Explicit binding table, following the tag association pattern | Bindings follow term privileges, point at metrics and columns, and never touch policies | New storage and cleanup on asset drop                                                                                                                           | **Chosen** |

---

## Proposal

### Data model

```
GlossaryTerm  { id, name, definition, synonyms[], status, sourceRef?, properties, audit_info }
TermRelation  { fromTermId, toTermId, type: BROADER | RELATED, audit_info }
TermDomain    { termId, domainId, audit_info }
TermBinding   { termId, objectId, objectType, metric?, audit_info }
```

| Table                      | Key / index                                                                                           | Notes                                                        |
| -------------------------- | ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------ |
| `glossary_term_meta`       | unique `(metalake_id, name_key, deleted_at)`; unique `(metalake_id, source_ref, deleted_at)`          | `name_key` is the lower-cased name; `source_ref` is nullable |
| `glossary_term_synonym`    | `(metalake_id, synonym_key, deleted_at)`                                                              | one row per synonym, for lookup and the name check           |
| `glossary_term_rel`        | unique `(from_term_id, to_term_id, rel_type, deleted_at)`                                             | `RELATED` stored once, with `from < to`                      |
| `glossary_term_domain_rel` | unique `(term_id, domain_id, deleted_at)`, index `(domain_id)`                                        | reverse lookup "terms of a domain"                           |
| `glossary_term_binding`    | unique `(term_id, object_id, object_type, metric_name, deleted_at)`, index `(object_id, object_type)` | `metric_name` only for `SEMANTIC_MODEL`                      |

- `GLOSSARY_TERM` becomes a `MetadataObject.Type` so the owner API, events and audit work. It
  is **not taggable** and **not grantable**: role APIs reject privileges on a single term (D5).
- **Names** follow tag and domain name rules (up to 128 characters, `/` allowed and URL-encoded
  in paths, as in #13867). Uniqueness uses `name_key`, so `Churn` and `churn` collide on every
  backend; responses return the name as created.
- **Synonyms**: a synonym cannot equal another term's name, compared case-insensitively. The
  check runs both ways: adding a synonym checks term names, and creating or renaming a term checks
  other terms' synonyms. Two terms may share a synonym.
- **Owner** is a user or a group, through the existing owner API (`/owners/glossary_term/{term}`).
- **Source reference** (G1, Q8): optional, unique per metalake when present. Enterprise import
  matches on it first and falls back to the name.
- **Metrics** are not metadata objects; a metric binding is `(semantic model, metric name)`.
- Dropping an asset soft-deletes its bindings, including the bindings of the objects under a
  dropped catalog or schema, as tag relations are cleaned up today. Renaming keeps them, since
  bindings refer to IDs.

### Relations and hierarchy

| Relation  | Stored                                        | Shown as                    | Rules                                               |
| --------- | --------------------------------------------- | --------------------------- | --------------------------------------------------- |
| `BROADER` | on the child (A → B means "A is a kind of B") | Broader on A, Narrower on B | several parents, any domain; no self-edge; no cycle |
| `RELATED` | once per pair                                 | Related on both sides       | symmetric; no self-edge                             |

- **Setting A's broader term changes A only** (Q6): it needs the edit privilege on A, plus
  `VIEW_DEFINITION` on B.
- **Cycle check**: adding A → B fails with 400 if A is reachable from B by `BROADER` edges.
  Concurrent edits could each pass the check and together form a cycle (A → B and B → A), so
  every `BROADER` change takes an exclusive per-metalake glossary-hierarchy lock before the
  check. Hierarchy edits are rare; other term edits do not take it.
- **Depth**: no hard limit; hierarchy reads take a `depth` parameter (default 1, max 10).
- **Deprecating a parent** leaves its children unchanged (Q3).
- **Deleting a term** removes its relations, domain links and bindings in the same transaction.
  Each child that loses a broader term gets its own audit record (Q3); children keep their other
  parents or become top-level.
- **Import mapping** (DataHub connector, Enterprise 2.1): top-level term groups become domains,
  nested term groups become parent terms (the user can override per group), `Contains` →
  `RELATED`, `Inherits From` → `BROADER`.

### Domains

- A term has zero or more domains (G5). A term with no domain is governed by metalake grants only.
- Adding or removing a domain is a term edit: it needs the edit privilege on the term and
  `CREATE_DEFINITION` on the target domain, so nobody can move a term into a domain they cannot
  write.
- **Terms of a domain** = terms linked to the domain or to one of its visible subdomains,
  matching the Domain design's *scope*. Each entry carries its relation (`Direct` or
  `Via subdomain`).
- A term can be linked to a `DRAFT` domain; the domain's own visibility rules still apply.
- **Dropping a domain that has terms is refused** (409 `NonEmptyDomainException`), like a domain
  with subdomains, because silently unlinking terms would change who can manage them. This extends
  #13867 (see Open Question 3).

### Status

| Status            | Meaning                                   | Visible to                                              |
| ----------------- | ----------------------------------------- | ------------------------------------------------------- |
| `DRAFT` (initial) | being written                             | `CREATE_DEFINITION` or `MANAGE_DEFINITIONS` on the term |
| `APPROVED`        | authoritative; the only status agents see | `VIEW_DEFINITION`                                       |
| `DEPRECATED`      | kept for reference; hidden from agents    | `VIEW_DEFINITION`                                       |

- In OSS a `MANAGE_DEFINITIONS` holder on any of the term's domains, or on the metalake, sets
  the status directly through `PATCH` (G6, Q6). There are no submissions, so there is no
  self-approval check in OSS.
- Edits apply at once, whatever the status, and are audited (G8). OSS keeps only the current
  term; there is no version number or history (G12 is Enterprise).
- **Enterprise hook**: every change goes through a `TermChangeHandler`. The OSS implementation
  applies it at once; Enterprise replaces it to store proposals, add `IN_REVIEW`, enforce
  "a submitter cannot approve their own change" and keep versions (G12, G13).
- **Delete** (Q3, Q7): only `DRAFT` and `DEPRECATED` terms. The owner deletes a `DRAFT` term;
  deleting a `DEPRECATED` term takes `MANAGE_DEFINITIONS`. `APPROVED` terms must be deprecated
  first.

### Access control

**Privileges** (G7). They are shared with the other Define objects; for the glossary they mean:

| Privilege            | Grantable on     | Glossary meaning                                                                |
| -------------------- | ---------------- | ------------------------------------------------------------------------------- |
| `VIEW_DEFINITION`    | metalake, domain | read approved and deprecated terms, their relations and bindings; resolve terms |
| `CREATE_DEFINITION`  | metalake, domain | create terms; see drafts; edit terms the caller owns; delete own drafts         |
| `MANAGE_DEFINITIONS` | metalake, domain | edit any term in scope, set status, delete deprecated terms                     |

`MANAGE_DEFINITIONS` ⊃ `CREATE_DEFINITION` ⊃ `VIEW_DEFINITION`. The built-in `PUBLIC` role holds
`VIEW_DEFINITION` on the metalake by default, and an admin can revoke it. Gravitino has no
`PUBLIC` role today; it comes from the 2.0 privilege model and is a prerequisite.

**Evaluating a privilege on a term**

```
levels(term) = { metalake } ∪ { d and its parent : d ∈ domains(term) }
allowed(p)   = owner of the metalake
               OR ( p allowed on some level AND p denied on no level )
```

- A domain grant applies to the terms of its subdomains, following the domain's current parent,
  as in #13867 (Q5).
- **Allows add up, any deny wins** (Q5): Finance allows `VIEW_DEFINITION` and Sales denies it →
  a role cannot see a term in both, even though Finance allows it.
- Request-body targets are checked too: `CREATE_DEFINITION` on a domain being added, and
  `VIEW_DEFINITION` on a broader or related term being linked.

**What each operation needs**

| Operation                                                 | Required                                                                                                         |
| --------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| Get, list, hierarchy, resolve                             | `VIEW_DEFINITION`; drafts need `CREATE_DEFINITION`. Terms the caller cannot see are omitted, and get returns 404 |
| Create                                                    | `CREATE_DEFINITION` on every listed domain (on the metalake if no domain); the creator becomes the owner         |
| Edit definition, synonyms, domains, relations, properties | owner with `CREATE_DEFINITION`, or `MANAGE_DEFINITIONS` (Q7)                                                     |
| Bind / unbind an asset or metric                          | the edit privilege above **and** read access to the asset                                                        |
| Set status                                                | `MANAGE_DEFINITIONS`                                                                                             |
| Delete                                                    | `DRAFT`: owner with `CREATE_DEFINITION`, or `MANAGE_DEFINITIONS`; `DEPRECATED`: `MANAGE_DEFINITIONS`             |
| Set owner                                                 | current owner, or `MANAGE_GRANTS` on the metalake or one of the term's domains (Q6)                              |

**Binding results are filtered twice**: a term's bindings list only assets the caller can read,
and an asset's terms list only terms the caller can see. A binding never grants access (G7).

### REST API

Under `/api/metalakes/{metalake}`, following the Domain and Tag conventions.

| Operation         | Endpoint                                                                  | Request → response                                                     |
| ----------------- | ------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| List              | `GET /glossary/terms?details=&domain=&status=&nameContains=&sourceRef=`   | → `NameListResponse` / `TermListResponse`                              |
| Create            | `POST /glossary/terms`                                                    | `TermCreateRequest` → `TermResponse`                                   |
| Get               | `GET /glossary/terms/{term}`                                              | → `TermResponse`                                                       |
| Alter             | `PUT /glossary/terms/{term}`                                              | `TermUpdatesRequest` → `TermResponse`                                  |
| Set status        | `PATCH /glossary/terms/{term}`                                            | `TermSetRequest{status}` → `TermResponse`                              |
| Drop              | `DELETE /glossary/terms/{term}`                                           | → `DropResponse`                                                       |
| Hierarchy         | `GET /glossary/terms/{term}/hierarchy?direction=broader\|narrower&depth=` | → `TermTreeResponse`                                                   |
| Bound assets      | `GET /glossary/terms/{term}/objects?types=`                               | → `TermBindingListResponse`                                            |
| Bind / unbind     | `POST /glossary/terms/{term}/objects`                                     | `TermBindingsRequest{toAdd[], toRemove[]}` → `TermBindingListResponse` |
| Terms of an asset | `GET /objects/{type}/{fullName}/terms`                                    | → `TermListResponse`                                                   |
| Terms of a domain | `GET /domains/{domain}/terms?details=`                                    | → `TermListResponse` (with relation)                                   |
| Resolve           | `GET /glossary/resolve?text=&domain=`                                     | → `TermMatchListResponse`                                              |
| Owner             | existing `/owners/glossary_term/{term}`                                   |                                                                        |

```
TermCreateRequest  { name, definition, synonyms[], domains[], broader[], related[],
                     sourceRef?, properties }
TermUpdatesRequest { updates: [rename, updateDefinition, addSynonym, removeSynonym,
                     addDomain, removeDomain, addBroader, removeBroader, addRelated,
                     removeRelated, setSourceRef, setProperty, removeProperty] }
TermDTO            { name, definition, synonyms, status, domains, broader, narrower, related,
                     owner, sourceRef, properties, audit }
TermMatchDTO       { term: TermDTO, matchedBy: NAME | SYNONYM, bindings: [TermBindingDTO] }
TermBindingDTO     { object: MetadataObjectDTO, metric? }
```

- **Updates** are applied in one transaction. `narrower` is read-only.
- **Resolve** (G10): trims the text and compares it case-insensitively with names and synonyms. It
  returns every `APPROVED` match the caller can see, each with `matchedBy`, its domains and its
  visible bindings; `domain` narrows to a domain's scope. Deprecated terms are not returned, and
  there is no replacement pointer (Q9).
- **Errors**: 404 `NoSuchTermException` (missing or not visible); 409
  `TermAlreadyExistsException` (name or source reference), `NonEmptyDomainException`; 400 cycle,
  self-relation, a synonym equal to another term's name, deleting an `APPROVED` term; 403
  `ForbiddenException`.
- **Clients** (Java, Python): `GlossaryOperations` on `GravitinoMetalake` (list, get, create,
  alter, setStatus, delete, resolve); `SupportsTerms` (`listTerms()`) on the bindable asset types
  and on `Domain`.

### MCP tools

A new read-only `glossary` tool tag in `mcp-server/`, added to `SUPPORTED_TOOL_TAGS`. Every tool
returns `APPROVED` terms only, and the server applies the caller's privileges through the
forwarded `Authorization` header.

| Tool                      | Endpoint                                                                         | Purpose for the agent                                                                            |
| ------------------------- | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| `resolve_business_term`   | `GET /glossary/resolve`                                                          | Main entry: map a word in the question to terms, definitions and the columns or metrics to query |
| `get_term`                | `GET /glossary/terms/{term}`                                                     | Full definition, synonyms, domains, relations                                                    |
| `list_terms`              | `GET /glossary/terms?details=true&status=APPROVED` (+ `domain`, `name_contains`) | Browse the vocabulary of a domain                                                                |
| `get_term_hierarchy`      | `GET /glossary/terms/{term}/hierarchy`                                           | Widen ("Customer") or narrow ("Active Customer") a concept                                       |
| `list_metadata_by_term`   | `GET /glossary/terms/{term}/objects`                                             | Find the data that implements a term                                                             |
| `list_terms_for_metadata` | `GET /objects/{type}/{fullName}/terms`                                           | Explain what a table or column means                                                             |

With the `domain` tools of #13867 (`list_domains`, `list_metadata_by_domain`), an agent can go
from a domain to its vocabulary to the data.

**Later**: ranked resolution with term version and steward for citation (G14, Enterprise 2.1);
agent proposals of `DRAFT` terms. Agents never approve.

### Events and audit

- Every term operation (create, edit, status change, binding change, delete) emits pre, success
  and failure events through a `GlossaryEventDispatcher` and reaches the audit log (G8).
- A delete also writes one audit record per child that loses its broader term.
- `ResolveTermEvent` carries the text, the caller, and whether the word is defined: `true` when
  any term of any status matches. A word that matches only a `DEPRECATED` term is defined, so
  Enterprise does not record it as undefined (Q9).

### Backward compatibility

All additions: new tables, a new `GLOSSARY_TERM` object type, three new privileges and new
endpoints. Dropping a domain with terms is refused, which only affects metalakes that use the
glossary.

---

## Open Questions

1. **Domain membership model**: G5 asks for "the same membership model as other domain members",
   which in #13867 is backing tags. This design uses an explicit relation (D4), because domain
   membership decides who manages a term. Product and the Domain design need to agree.
2. **Domain privileges vs the 2.0 privilege model**: #13867 defines `MANAGE_DOMAIN` /
   `USE_DOMAIN`, while the privilege PRD says the three DEFINITION privileges cover every Define
   object, Domains included. Both designs must use one set.
3. **Dropping a domain that has terms**: refuse (proposed) or unlink with an audit record.

---

## Task Breakdown

### Phase 1: Model, storage and authorization

- [ ] `GlossaryTerm` API in `api/`; `MetadataObject.Type.GLOSSARY_TERM` (not taggable, not grantable) (G1)
- [ ] `VIEW_DEFINITION`, `CREATE_DEFINITION`, `MANAGE_DEFINITIONS` on metalake and domain, and the `PUBLIC` default (G7, shared with the privilege model work)
- [ ] Storage: the five tables above (H2, MySQL, PostgreSQL, upgrade scripts)
- [ ] Manager: CRUD, name and synonym rules, relations with the cycle check and hierarchy lock, domain links, status, `TermChangeHandler` (G2, G5, G6)
- [ ] Authorization: evaluation over metalake, domains and their parents with deny precedence; draft visibility; owner-only edit for `CREATE_DEFINITION`
- [ ] REST endpoints for terms, status and hierarchy; events and audit (G8, G9)

### Phase 2: Bindings and lookup

- [ ] Binding storage and endpoints from both sides, with cleanup on asset drop (G3, G4)
- [ ] Terms of a domain (scope) and the domain drop check
- [ ] Resolve endpoint and `ResolveTermEvent` (G10)

### Phase 3: Clients, MCP and docs

- [ ] Java and Python clients
- [ ] MCP `glossary` tools
- [ ] OpenAPI spec (validated with `./gradlew :docs:build`) and user docs
