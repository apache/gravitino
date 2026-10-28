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
| 0.4     | Qi Yu  | 2026-10-28 | Draft  |

## Background

Gravitino records where data lives (catalogs, schemas, tables) and, with Domains
(apache/gravitino#13867), which business area it belongs to. It does not record what a business
word means. "Active Customer" or "Net Revenue" are defined in wikis and BI tools, so each user and
each agent maps the word to tables and metrics on its own, and nobody owns the definition.

A business glossary fixes this: a governed set of **terms**, each with a definition, synonyms, an
owner and a status, organized by domain and by a term hierarchy, and bound to the columns, tables
and metrics that implement it. Agents resolve a word in a question to an approved term and follow
its bindings to the data.

## Goals

1. **Term object**: a metalake-level `GlossaryTerm` with name, definition, synonyms, owner, status,
   domains, properties and an optional source reference. There is no glossary container; terms are
   organized by domain and by the term hierarchy.
2. **Hierarchy**: `BROADER` (a term can have several, in any domain; narrower is derived) and
   `RELATED` (symmetric). The server rejects cycles.
3. **Bindings**: a term binds to catalogs, schemas, tables, views, columns, filesets, topics,
   models and semantic-model metrics, and lists them; an object lists its terms. A binding never
   grants access.
4. **Domains**: a term belongs to zero or more domains, and domain grants govern it.
5. **Status**: `DRAFT`, `APPROVED`, `DEPRECATED`, set through the API. Agents see `APPROVED` terms
   only.
6. **Privileges**: `VIEW_DEFINITION`, `CREATE_DEFINITION` and `MANAGE_DEFINITIONS` on the metalake
   or a domain, inherited by subdomains, deny over allow, the same on REST and MCP.
7. **Versions**: every content change records a version snapshot, so an approval workflow or a
   history API can be added without a schema change.
8. **APIs**: REST, Java and Python clients, read-only MCP tools, events and audit, including an
   exact, case-insensitive lookup from a business word to approved terms and their bindings.

## Non-Goals

1. **Approval workflow and version history API**: edits apply at once. A workflow can plug in
   through `TermChangeHandler` (see Status and versions).
2. **Bulk import and export, external glossary connectors, SKOS export**: they can build on the
   REST API and the source reference.
3. **Ranked, fuzzy or semantic lookup**: lookup is an exact match on names and synonyms.
4. **Access through terms**: a term or binding never grants or denies data access, and terms carry
   no policies.
5. **Agent writes and UI**: the MCP tools are read-only.

## Usage Scenario

1. A steward with `MANAGE_DEFINITIONS` on domain Sales creates term `Customer` in Sales. It starts
   as `DRAFT`, visible only to `CREATE_DEFINITION` and `MANAGE_DEFINITIONS` holders.
2. An analyst with `CREATE_DEFINITION` on Sales creates `Active Customer` with broader term
   `Customer` and synonym `active user`, and binds it to column `dw.sales.customers.is_active`
   (Implemented by). The analyst owns this term and can edit it, but cannot edit `Customer`.
3. The steward sets both terms to `APPROVED`; users with `VIEW_DEFINITION` now see them.
4. The owner of semantic model `dw.sales.sales_model` links its metric `active_customers` to
   `Active Customer` from the metric side; the term now lists the metric under Used by.
5. An agent asks about "active users". `resolve_business_term` returns `Active Customer` (matched
   by synonym), its definition and the column and metric it is bound to, filtered to what the
   agent can read.
6. The term is later replaced. The steward sets it to `DEPRECATED`, so agents stop seeing it, and
   then deletes it.

## Design

### Data model

```
GlossaryTerm { id, name, definition, synonyms[], status: DRAFT | APPROVED | DEPRECATED,
               sourceRef?, properties, currentVersion, audit_info }
TermRelation { fromTermId, toTermId, type: BROADER | RELATED, audit_info }
TermDomain   { termId, domainId, audit_info }                // Option A in Domain membership
TermBinding  { termId, objectId, objectType, metricName?, audit_info }
TermVersion  { termId, version, state: PENDING | CURRENT | SUPERSEDED, snapshot, audit_info }
```

| Table                        | Unique key                                                                     | Other index                              |
| ---------------------------- | ------------------------------------------------------------------------------ | ---------------------------------------- |
| `glossary_term_meta`         | `(metalake_id, name_key, deleted_at)`, `(metalake_id, source_ref, deleted_at)` |                                          |
| `glossary_term_synonym`      | `(term_id, synonym_key, deleted_at)`                                           | `(metalake_id, synonym_key, deleted_at)` |
| `glossary_term_rel`          | `(from_term_id, to_term_id, rel_type, deleted_at)`                             | `(to_term_id, rel_type)`                 |
| `glossary_term_domain_rel`   | `(term_id, domain_id, deleted_at)`                                             | `(domain_id)`                            |
| `glossary_term_binding`      | `(term_id, object_type, object_id, metric_name, deleted_at)`                   | `(object_type, object_id)`               |
| `glossary_term_version_info` | `(term_id, version, deleted_at)`                                               | `(term_id, state)`                       |

- Tables go into the H2, MySQL and PostgreSQL schemas and the `upgrade-1.3.0-to-2.0.0` scripts.
- **Names** are unique per metalake, case-insensitively: `name_key` is the trimmed, lower-cased
  name, so `Churn` and `churn` collide on every backend, and responses return the name as created.
  Names follow the tag and domain name rules (up to 128 characters, `/` allowed and URL-encoded in
  paths).
- **Synonyms** are stored one per row with a lower-cased `synonym_key`. Two terms may share a
  synonym, but a synonym cannot equal another term's name; adding a synonym checks names, and
  creating or renaming a term checks other terms' synonyms (400).
- **Source reference** is optional and unique per metalake when set. It records where an imported
  term came from, so a re-import can update the term instead of duplicating it.
- `MetadataObject.Type.GLOSSARY_TERM` is added for owners, events and audit. It is **not
  grantable**: the role APIs reject privileges on a single term.
- Terms, relations and bindings are not cached; every request reads the store, as for domains.

### Relations

- `BROADER` is stored on the child (A → B means "A is a kind of B"); narrower is derived. `RELATED`
  is stored once per pair with `from_term_id < to_term_id` and shown on both sides.
- Setting A's broader or related term is a change to A only. It needs the edit privilege on A and
  `VIEW_DEFINITION` on the other term.
- **Cycle check**: adding A → B fails with 400 if A is reachable from B through `BROADER` edges.
  The walk reads B's ancestors breadth-first under the hierarchy lock (see Locking). No self-edges.
- Hierarchy reads take `depth` (default 1, at most 10).
- Deprecating a term leaves its children unchanged. Deleting a term removes its edges; each child
  that loses a broader term gets its own audit record, and keeps its other parents or becomes
  top-level.

### Domain membership

A term belongs to zero or more domains, and domain grants govern it; a term with no domain is
governed by metalake grants only. There are two ways to record the membership.

**Option A: explicit relation (proposed)**

- `glossary_term_domain_rel (term_id, domain_id)`, changed only through the term API (`domains` on
  create, `addDomain`, `removeDomain`), and through `TermChangeHandler` like any term change.
- Adding a domain needs the edit privilege on the term and `CREATE_DEFINITION` on that domain, so
  nobody can move a term into a domain they cannot write.
- **Terms of a domain** (`GET /terms?domain=`) use the Domain design's scope: terms linked to the
  domain or to a subdomain the caller can see.
- Dropping a domain that still has terms fails with 409 `NonEmptyDomainException`, like a domain
  with subdomains, since unlinking them would change who manages the terms. This is one added
  check in `DomainManager#dropDomain`.

**Option B: backing tags, as for assets in #13867**

- `GLOSSARY_TERM` becomes taggable and is added to `DomainMember.SUPPORTED_TYPES`. A term is in
  domain D when it carries D's backing (tag, value); terms appear in `GET /domains/{domain}/objects`
  and in the domains of an object. No relation table.
- Plain tag rules are not enough. For an asset, read access plus `APPLY_TAG` on a backing tag adds
  it to a domain, and that grants nothing. For a term it decides who manages the term: a user with
  only `VIEW_DEFINITION` and `APPLY_TAG` could add a term to Finance and hand it to Finance's
  managers. The tag association path therefore needs two special cases for `GLOSSARY_TERM`:
  1. besides `APPLY_TAG`, the edit privilege on the term and `CREATE_DEFINITION` on the domain the
     (tag, value) backs, found through the Domain design's reverse lookup;
  2. a backing-tag change on a term goes through `TermChangeHandler`, so a workflow can hold it
     like any other term change.
- Changing or deleting a domain's backing tag silently moves terms out of the domain, so who
  manages them changes with no term change. Dropping a domain works as for assets.

**Comparison**

|                              | Option A: explicit relation                                             | Option B: backing tags                                                     |
| ---------------------------- | ----------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| Meaning                      | "Term T belongs to domain D" is stored as such                          | Derived: T carries tag X, and X backs D                                    |
| Who can change it            | Term privileges + `CREATE_DEFINITION` on D, in one place                | `APPLY_TAG`, plus the two term-specific checks                             |
| Approval workflow            | Same path as other term edits                                           | Needs a hook in the tag association API                                    |
| Stable governance            | Changes only through the term API                                       | Also changes when a backing tag is replaced or deleted                     |
| Authorization lookup         | One indexed read of the term's domains                                  | Term tags → reverse lookup of the domains they back                        |
| Consistency with assets      | A second membership mechanism; terms are listed by `GET /terms?domain=` | One mechanism; terms appear in domain member listings                      |
| Changes to the Domain design | Drop check only                                                         | Taggable `GLOSSARY_TERM`, a new member type, special cases in the tag code |

Option A is proposed. For a term, domain membership decides who governs it, so it should be stored
explicitly and changed only by people who govern the term. Option B reuses one membership
mechanism, but needs term-specific rules inside the generic tag code, which #13867 keeps free of
special cases.

### Bindings

A binding links a term to the data behind it. A term page shows two kinds, and an object page
shows the terms linked to it:

| Kind               | Meaning                                                 | Targets                                                                     | Example                                                                                        |
| ------------------ | ------------------------------------------------------- | --------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| **Implemented by** | The object stores or computes the term                  | `CATALOG`, `SCHEMA`, `TABLE`, `VIEW`, `COLUMN`, `FILESET`, `TOPIC`, `MODEL` | `Customer` is implemented by table `sales.public.customers` and column `customers.customer_id` |
| **Used by**        | A semantic-model metric is defined in terms of the term | a metric: (`SEMANTIC_MODEL`, `metricName`)                                  | metric `net_revenue` uses `Net revenue` and `Order`                                            |

Both kinds live in `glossary_term_binding`; the kind follows from `metric_name` (set only for
Used by). A binding is direct only: binding a schema does not bind the tables in it.

**One write endpoint, two ways to be allowed**

Bindings are written on the object, like tags and policies: `POST /objects/{type}/{fullName}/terms`,
or `POST /objects/semantic_model/{fullName}/metrics/{metric}/terms` for a metric. Two different
people know about a binding: the object's curator knows which terms the object represents, and the
term's owner knows which data implements the term. Either may write it, per term in the request:

| Caller         | Needs                                                             | Terms allowed                                            |
| -------------- | ----------------------------------------------------------------- | -------------------------------------------------------- |
| Object curator | Curate privilege on the object, and `VIEW_DEFINITION` on the term | `APPROVED` only                                          |
| Term editor    | Edit privilege on the term, and read access to the object         | Any status, so a draft can be prepared with its bindings |

- **Curate privilege on an object**: for tables and columns, the existing
  `MODIFY_TABLE_AUTHORIZATION_EXPRESSION` (an owner of the table or of an ancestor, or
  `MODIFY_TABLE` with `USE_CATALOG` and `USE_SCHEMA`); for metrics,
  `MODIFY_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION`. The other types have no modify privilege, so
  they need ownership of the object or of one of its ancestors (metalake, catalog, schema).
- The endpoint checks the object once and each term in the body imperatively; if any term fails
  both checks, the request fails with 403 and nothing changes. Removing a binding needs the same
  privilege as adding it.
- Each changed term records a version and goes through `TermChangeHandler`, so a workflow can hold
  a binding change like any other term change.
- Binding a missing object or metric fails with 404; an object curator binding a term that is not
  `APPROVED` gets 400.
- A term that is later deprecated keeps its bindings; it stops appearing in matches and MCP
  results.

**Reads**

- `GET /terms/{term}/objects?kind=IMPLEMENTED_BY|USED_BY&types=` lists a term's bindings, keeping
  only objects and metrics the caller can access (`filterMetadataObject`; a metric needs access to
  its semantic model).
- `GET /objects/{type}/{fullName}/terms` and `GET /objects/semantic_model/{fullName}/metrics/{metric}/terms`
  list an object's or a metric's terms, keeping only terms the caller can see.

**Cleanup**

- Dropping an object soft-deletes its bindings, in the same places that clean up
  `tag_relation_meta` (by object, schema, catalog and metalake). Renames keep them, since bindings
  refer to IDs.
- Dropping a column in `alterTable` soft-deletes the column's bindings in the same transaction.
- Metrics have no IDs: altering a semantic model soft-deletes the bindings of metrics that no
  longer exist. A renamed metric loses its bindings and must be linked again.

### Status and versions

| Status            | Visible to                                  | Set by               | Delete                                                  |
| ----------------- | ------------------------------------------- | -------------------- | ------------------------------------------------------- |
| `DRAFT` (initial) | `CREATE_DEFINITION` or `MANAGE_DEFINITIONS` | creation             | owner with `CREATE_DEFINITION`, or `MANAGE_DEFINITIONS` |
| `APPROVED`        | `VIEW_DEFINITION`; MCP                      | `MANAGE_DEFINITIONS` | not allowed; deprecate first                            |
| `DEPRECATED`      | `VIEW_DEFINITION`; not MCP                  | `MANAGE_DEFINITIONS` | `MANAGE_DEFINITIONS`                                    |

- A `MANAGE_DEFINITIONS` holder on any of the term's domains or on the metalake sets the status
  directly with `PATCH`. Edits apply at once in any status and are audited.
- **Versioned changes**: name, definition, synonyms, properties, relations, domains and bindings.
  Owner and status changes are audited only.
- **Snapshot**: each version row stores the whole term as JSON (fields, synonyms, broader and
  related term IDs, domain IDs, bindings). The live tables hold only the current state, so the
  snapshot is what lets a change be stored before it is applied. This follows the existing
  `current_version` + `table_column_version_info` pattern.
- **Write path**: in the transaction of the change, under the term's exclusive lock, increment
  `current_version`, insert a `CURRENT` row and mark the previous one `SUPERSEDED`. Cost: one insert
  and one update per edit; a snapshot grows with the term's bindings.
- **Read path**: no version API in this design; adding `GET /terms/{term}/versions` later
  needs no storage change.
- **Workflow hook**: `GlossaryManager` sends every content change through a `TermChangeHandler`.
  The default implementation applies it at once as above. An approval workflow can replace it to
  write a `PENDING` row without touching the live tables, then apply the snapshot and promote the
  row to `CURRENT` on approval.
- Dropping a term soft-deletes its version rows.

### Access control

| Privilege            | On the metalake or a domain                                                             |
| -------------------- | --------------------------------------------------------------------------------------- |
| `VIEW_DEFINITION`    | Read `APPROVED` and `DEPRECATED` terms, relations and bindings; match                   |
| `CREATE_DEFINITION`  | Create terms; see `DRAFT` terms; edit and bind terms the caller owns; delete own drafts |
| `MANAGE_DEFINITIONS` | Edit and bind any term; set status; delete deprecated terms                             |

- `MANAGE_DEFINITIONS` includes `CREATE_DEFINITION`, which includes `VIEW_DEFINITION`. The names
  are generic so that other business-definition objects can share them.
- **Levels of a term**: the metalake, each of its domains, and each domain's current parent. A
  privilege holds when it is allowed on at least one level and denied on none. If Finance allows
  `VIEW_DEFINITION` and Sales denies it, a role cannot see a term in both. The metalake owner
  always passes.
- **Bindings** are written on the object, by the object's curator (`APPROVED` terms only) or by the
  term's editor; see Bindings.
- **Owner** (user or group) through `/owners/glossary_term/{term}`. Changing it needs the current
  owner, or `MANAGE_GRANTS` on the metalake or one of the term's domains.
- **Create** needs `CREATE_DEFINITION` on every listed domain, or on the metalake when none is
  listed; the creator becomes the owner.
- **Implementation**: a `TermScopeResolver` returns a term's levels from its domains and the Domain
  design's parent resolver, batched for list filtering. `JcasbinAuthorizer#hasMetadataPrivilegePermission`
  uses it for `GLOSSARY_TERM` where it walks to the parent today, combining the levels in one check
  as for catalog, schema and table. Domains and terms named in a request body are checked
  imperatively.

### Locking

Term operations use the store's existing primitives, in one order: the live-endpoint fence
(`LiveEndpointService.lockLiveEndpoint`, shared: the metalake, plus the bound object's chain when
binding), the per-metalake hierarchy lock if needed, term rows in ID order (exclusive), then domain
rows (shared). Each row is locked once, in its final mode.

| Operation                       | Locks                                                                   | Conflict it excludes                 |
| ------------------------------- | ----------------------------------------------------------------------- | ------------------------------------ |
| Create                          | metalake (shared) → domains (shared)                                    | metalake or domain drop              |
| Edit content, synonyms, domains | term (exclusive) → added domains (shared)                               | concurrent edits; domain drop        |
| Add a `BROADER` edge            | metalake (shared) → hierarchy lock → child (exclusive)                  | two edges that together form a cycle |
| Add a `RELATED` edge            | both terms (exclusive, ID order)                                        | the other term being deleted         |
| Bind / unbind                   | object chain (`lockLiveEndpoint`, shared) → terms (exclusive, ID order) | object drop; concurrent term edits   |
| Set status, delete              | term (exclusive)                                                        | concurrent edits                     |

- The **hierarchy lock** is a `SELECT … FOR UPDATE` on a per-metalake row in
  `glossary_hierarchy_lock`, inserted on first use. Locking only the two terms is not enough: the
  edges A → B and C → D can be added concurrently, then B → C and D → A, each passing its own
  check. `BROADER` edits are rare, so serializing them per metalake is cheap.
- Name-addressed operations resolve the name, lock the row, then re-check the name and
  `deleted_at`; otherwise 404. The name unique key backs up concurrent creates (409).

### REST API

Under `/api/metalakes/{metalake}`, following the Tag, Policy and Domain conventions: a top-level
`/terms` collection (there is no glossary container, so no `/glossary` level), `?details=` on lists,
`PUT` with updates to alter, `PATCH` to set status, `DropResponse` on delete, and associations
written on the object.

| Operation              | Endpoint                                                              | Request → response                                                      |
| ---------------------- | --------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| List, filter, match    | `GET /terms?details=&domain=&status=&nameContains=&sourceRef=&match=` | → `NameListResponse` / `TermListResponse`                               |
| Create                 | `POST /terms`                                                         | `TermCreateRequest` → `TermResponse`                                    |
| Get                    | `GET /terms/{term}`                                                   | → `TermResponse`                                                        |
| Alter                  | `PUT /terms/{term}`                                                   | `TermUpdatesRequest` → `TermResponse`                                   |
| Set status             | `PATCH /terms/{term}`                                                 | `TermSetRequest{status}` → `TermResponse`                               |
| Drop                   | `DELETE /terms/{term}`                                                | → `DropResponse`                                                        |
| Hierarchy              | `GET /terms/{term}/hierarchy?direction=BROADER\|NARROWER&depth=`      | → `TermTreeResponse`                                                    |
| Bindings of a term     | `GET /terms/{term}/objects?kind=&types=`                              | → `TermBindingListResponse`                                             |
| Terms of an object     | `GET /objects/{type}/{fullName}/terms`                                | → `NameListResponse` / `TermListResponse`                               |
| Bind / unbind          | `POST /objects/{type}/{fullName}/terms`                               | `TermsAssociateRequest{termsToAdd, termsToRemove}` → `NameListResponse` |
| Terms of a metric      | `GET /objects/semantic_model/{fullName}/metrics/{metric}/terms`       | → `NameListResponse` / `TermListResponse`                               |
| Bind / unbind a metric | `POST /objects/semantic_model/{fullName}/metrics/{metric}/terms`      | `TermsAssociateRequest` → `NameListResponse`                            |
| Owner                  | existing `/owners/glossary_term/{term}`                               |                                                                         |

```
TermCreateRequest     { name, definition, synonyms, domains, broader, related, sourceRef, properties }
TermUpdatesRequest    { updates: [rename, updateDefinition, addSynonym, removeSynonym, addDomain,
                        removeDomain, addBroader, removeBroader, addRelated, removeRelated,
                        setSourceRef, setProperty, removeProperty] }
TermDTO               { name, definition, synonyms, status, domains, broader, narrower, related,
                        owner, sourceRef, properties, version, audit, matchedBy? }
TermBindingDTO        { kind: IMPLEMENTED_BY | USED_BY, object: MetadataObjectDTO, metric? }
TermsAssociateRequest { termsToAdd: [name], termsToRemove: [name] }
```

- **One list endpoint.** `domain` returns the terms in the domain's scope (the domain and the
  subdomains the caller can see). `match` is the business-word lookup: the trimmed text is compared
  with `name_key` and `synonym_key`, and each returned term carries `matchedBy: NAME | SYNONYM`. A
  word that matches only `DEPRECATED` terms returns nothing when `status=APPROVED` is set.
- **Alter** applies all updates in one transaction and records one version. `narrower` is derived
  and read-only; `version` is the current version.
- **Bind / unbind** follow the tag association request shape; the response is the object's terms
  after the change.
- **Errors**: 404 `NoSuchTermException` (missing or not visible) and `NoSuchMetadataObjectException`;
  409 `TermAlreadyExistsException` (name or source reference) and `NonEmptyDomainException`; 400 for
  a cycle, a self-edge, a synonym equal to another term's name, deleting an `APPROVED` term, or an
  object curator binding a term that is not `APPROVED`; 403 `ForbiddenException`.
- **Encoding**: term names may contain `/`, URL-encoded as `%2F` in `{term}`, as for domain and tag
  names.
- **Clients** (Java, Python):
  - `GlossaryOperations` on `GravitinoMetalake`: `listTerms`, `listTermsInfo`, `getTerm`,
    `createTerm`, `alterTerm`, `setTermStatus`, `dropTerm`, `matchTerms`.
  - On a `Term`: `broader(depth)`, `narrower(depth)`, `listBindings(kind)`.
  - `SupportsTerms` (`listTerms()`, `associateTerms(add, remove)`) on catalogs, schemas, tables,
    views, columns, filesets, topics and models, and `listMetricTerms` / `associateMetricTerms` on
    `SemanticModel`.

### MCP tools

A read-only `glossary` tool tag in `mcp-server/`, added to `SUPPORTED_TOOL_TAGS`. Every tool asks
for `status=APPROVED`, and the MCP server forwards the caller's `Authorization` header, so the
server's filtering applies.

| Tool                      | REST calls                                                                                   | Use                                                                                        |
| ------------------------- | -------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `resolve_business_term`   | `GET /terms?match=&status=APPROVED&details=true`, then `GET /terms/{term}/objects` per match | Map a word in a question to terms and the columns or metrics behind them, in one tool call |
| `get_term`                | `GET /terms/{term}`                                                                          | Definition, synonyms, domains, relations                                                   |
| `list_terms`              | `GET /terms?details=true&status=APPROVED` (+ `domain`, `name_contains`)                      | Browse a domain's vocabulary                                                               |
| `get_term_hierarchy`      | `GET /terms/{term}/hierarchy`                                                                | Widen or narrow a concept                                                                  |
| `list_metadata_by_term`   | `GET /terms/{term}/objects`                                                                  | Find the objects that implement a term and the metrics that use it                         |
| `list_terms_for_metadata` | `GET /objects/{type}/{fullName}/terms`, or the metric form                                   | Explain what an object or metric means                                                     |

`resolve_business_term` composes two calls instead of a dedicated REST endpoint; a word usually
matches one to three terms. Together with the `domain` tools of #13867, an agent can go from a
domain to its terms to the data.

### Events and audit

- Every operation emits pre, success and failure events through a `GlossaryEventDispatcher` and
  reaches the audit log: create, alter, status, bind and unbind, drop. A drop also records one entry
  for each child that lost a broader term.
- `MatchTermsEvent{text, caller, defined}` is emitted on every `match` lookup; `defined` is `true`
  when any term of any status matches, so a listener can collect business words that have no term
  yet.

## Alternatives Considered

1. **A glossary container** (`metalake.glossary.term`). It adds a second grouping axis next to
   Domain and raises the question of which one governs privileges; Domain plus the term hierarchy
   is enough.
2. **Domain membership through backing tags**: see Option B in Domain membership.
3. **Terms as tags on assets.** `APPLY_TAG` does not match the definition privileges, tags have no
   definition, relations or status, and tags carry policies while bindings must not affect access.

## Open Questions

1. **Domain membership of terms**: Option A (proposed) or Option B; to be agreed with the Domain
   design.
2. **One privilege set**: #13867 defines `MANAGE_DOMAIN` / `USE_DOMAIN` for domains. Should domains
   and terms share the definition privileges instead?
3. **Default read access**: terms should be readable by everyone by default, but Gravitino has no
   built-in role that every user holds. Add one (for example `PUBLIC`) holding `VIEW_DEFINITION` on
   the metalake, or ask admins to grant it?

## Task Breakdown

### Phase 1: Terms, storage and authorization

- [ ] `GlossaryTerm` API and `MetadataObject.Type.GLOSSARY_TERM` in `api/`; definition privileges on metalake and domain
- [ ] Storage: the six glossary tables and `glossary_hierarchy_lock` (H2, MySQL, PostgreSQL, upgrade scripts)
- [ ] `GlossaryManager`: CRUD, name and synonym rules, relations with the cycle check, domain membership, status, version rows, `TermChangeHandler`, locking
- [ ] Authorization: `TermScopeResolver`, the `GLOSSARY_TERM` case in `JcasbinAuthorizer`, draft visibility, owner-only edits for `CREATE_DEFINITION`
- [ ] REST endpoints for terms, status and hierarchy; `GlossaryEventDispatcher`; the domain drop check

### Phase 2: Bindings and lookup

- [ ] Binding storage; term-side and object-side endpoints with their privilege checks; cleanup on object drop, column drop and metric removal
- [ ] `domain` and `match` filters on the list endpoint; `MatchTermsEvent`

### Phase 3: Clients, MCP and docs

- [ ] Java and Python clients
- [ ] MCP `glossary` tools
- [ ] OpenAPI spec (validated with `./gradlew :docs:build`) and user docs
