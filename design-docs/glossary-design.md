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
| 0.5     | Qi Yu  | 2026-10-29 | Draft  |

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
   domains, documentation links, properties and an optional source reference. There is no glossary
   container; terms are organized by domain and by the term hierarchy.
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
8. **APIs**: REST, Java and Python clients, read-only MCP tools, events and audit. Every view of
   the glossary page loads with one or two REST calls, and an agent resolves the business words of
   a question to approved terms and their bindings in one call.

## Non-Goals

1. **Approval workflow and version history API**: edits apply at once. A workflow can plug in
   through `TermChangeHandler` (see Status and versions).
2. **Bulk import and export, external glossary connectors, SKOS export**: they can build on the
   REST API and the source reference.
3. **Ranked, fuzzy or semantic lookup**: lookup is an exact match on names and synonyms.
4. **Access through terms**: a term or binding never grants or denies data access, and terms carry
   no policies.
5. **Agent writes and the UI itself**: the MCP tools are read-only. The UI is built on the REST API;
   Web UI support lists the calls behind each part of the page.

## Usage Scenario

1. An admin with `MANAGE_DEFINITIONS` on domain Sales creates term `Customer` in Sales. It starts
   as `DRAFT`, visible only to `CREATE_DEFINITION` and `MANAGE_DEFINITIONS` holders.
2. An analyst with `CREATE_DEFINITION` on Sales creates `Active Customer` with broader term
   `Customer` and synonym `active user`, and binds it to column `dw.sales.customers.is_active`
   (Implemented by). The analyst owns this term and can edit it, but cannot edit `Customer`.
3. The admin sets both terms to `APPROVED`; users with `VIEW_DEFINITION` now see them.
4. The owner of semantic model `dw.sales.sales_model` links its metric `active_customers` to
   `Active Customer` from the metric side; the term now lists the metric under Used by.
5. A user asks an agent for "revenue from active users last month". The agent calls
   `resolve_business_term(["revenue", "active users"])` once: `active users` returns
   `Active Customer` (matched by synonym) with its column and metric, filtered to what the user can
   read, and `revenue` comes back unmatched, so the agent asks which revenue is meant instead of
   guessing a column. It then loads the table or semantic model with the existing tools.
6. The term is later replaced. The admin sets it to `DEPRECATED`, so agents stop seeing it, and
   then deletes it.

## Design

### Data model

```
GlossaryTerm { id, name, definition, synonyms[], status: DRAFT | APPROVED | DEPRECATED,
               links[{label, url}], sourceRef?, properties, currentVersion, audit_info }
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
- **Normalized key**: `key(s)` trims `s`, collapses runs of whitespace to one space and lower-cases
  it (`Locale.ROOT`). Names, synonyms and lookups all use it, so they compare the same way.
- **Names** are unique per metalake by `name_key = key(name)`, so `Churn` and `churn` collide on
  every backend, and responses return the name as created.
  Names follow the tag and domain name rules (up to 128 characters, `/` allowed and URL-encoded in
  paths).
- **Synonyms** are stored one per row with `synonym_key = key(synonym)`. Two terms may share a
  synonym, but a synonym cannot equal another term's name; adding a synonym checks names, and
  creating or renaming a term checks other terms' synonyms (400).
- **Documentation links** (label and URL, shown on the term page) are stored as a JSON column of
  `glossary_term_meta`; they are never queried.
- **Source reference** is optional and unique per metalake when set. It records where an imported
  term came from as `<system>:<id>` (for example `datahub:urn:li:glossaryTerm:Customer`), so a
  re-import can update the term instead of duplicating it. A term without one was created in
  Gravitino.
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
- A hierarchy read returns all ancestors or all descendants of a term; the graph is acyclic, so it ends.
- Deprecating a term leaves its children unchanged. Deleting a term removes its edges; each child
  that loses a broader term gets its own audit record, and keeps its other parents or becomes
  top-level.

### Domain membership

A term belongs to zero or more domains, and domain grants govern it; a term with no domain is
governed by metalake grants only. There are two ways to record the membership.

**Option A: explicit relation (recommended)**

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

**Recommendation: Option A.** The reasons:

1. **For a term, the domain decides who can read and manage it.** For an asset, being in a domain
   gives nobody any permission. That is why the Domain design lets anyone with `APPLY_TAG` add an
   asset, and why it refuses access grants on domains. Terms are the opposite case: the grants on
   a term's domains decide who can see, edit and approve it. With Option B, adding a tag would
   change those permissions.
2. **Tag permissions go to other people.** In the Domain design, admins give `APPLY_TAG` on a
   domain's tag to the owners of the tables in that domain. With Option B, any of those table
   owners could move a term into the domain, which gives the domain's admins control of the term,
   or hides it from a role the domain denies. One tag cannot allow this for tables and forbid it
   for terms.
3. **The benefits of tags do not apply to terms.** The Domain design uses tags for inheritance
   from catalogs and schemas, for tag-based policies and for the existing tag checks. Terms are not
   under a catalog or schema, carry no policies, and need stricter checks than `APPLY_TAG`.
4. **Permissions do not change by accident.** With Option B, replacing or deleting a domain's tag
   silently removes its terms, so who can see or edit them changes although nobody edited a term.
   With Option A, membership changes only through the term API, and is checked, versioned and
   audited like any other term edit.
5. **Smaller change to the Domain design.** Option A adds one check when a domain is dropped.
   Option B makes terms taggable, adds a member type and puts term-only rules in the shared tag
   code.

The cost: a domain's terms are listed by `GET /terms?domain=` instead of
`GET /domains/{domain}/objects`. Both follow the same rules: a term can be in several domains, and
a domain includes its subdomains.

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
people know about a binding: the object's admin knows which terms the object represents, and the
term's owner knows which data implements the term. Either may write it, per term in the request:

| Caller       | Needs                                                            | Terms allowed                                            |
| ------------ | ---------------------------------------------------------------- | -------------------------------------------------------- |
| Object admin | Admin privilege on the object, and `VIEW_DEFINITION` on the term | `APPROVED` only                                          |
| Term editor  | Edit privilege on the term, and read access to the object        | Any status, so a draft can be prepared with its bindings |

- **Admin privilege on an object**: for tables and columns, the existing
  `MODIFY_TABLE_AUTHORIZATION_EXPRESSION` (an owner of the table or of an ancestor, or
  `MODIFY_TABLE` with `USE_CATALOG` and `USE_SCHEMA`); for metrics,
  `MODIFY_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION`. The other types have no modify privilege, so
  they need ownership of the object or of one of its ancestors (metalake, catalog, schema).
- The endpoint checks the object once and each term in the body imperatively; if any term fails
  both checks, the request fails with 403 and nothing changes. Removing a binding needs the same
  privilege as adding it.
- Each changed term records a version and goes through `TermChangeHandler`, so a workflow can hold
  a binding change like any other term change.
- Binding a missing object or metric fails with 404; an object admin binding a term that is not
  `APPROVED` gets 400.
- A term that is later deprecated keeps its bindings; it stops appearing in matches and MCP
  results.

**Reads**

- `GET /terms/{term}/objects?kind=IMPLEMENTED_BY|USED_BY&types=` lists a term's bindings, keeping
  only objects and metrics the caller can access (`filterMetadataObject`; a metric needs access to
  its semantic model).
- `GET /objects/{type}/{fullName}/terms` and `GET /objects/semantic_model/{fullName}/metrics/{metric}/terms`
  list an object's or a metric's terms, keeping only terms the caller can see.
- `?children=true` on a table, view or semantic model also returns the terms of each of its
  columns or metrics, so a schema tab or a metrics table loads in one call instead of one per
  column. It is one query on `glossary_term_binding` by the object and its column IDs (or by
  `object_id` for metrics).

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
- **Visibility is the same on every surface** (REST, clients, MCP). A term the caller cannot see is
  omitted from lists, and get returns 404. The same filter applies inside responses: a term's
  `broader`, `narrower` and `related` lists, an object's terms and a domain's terms omit terms the
  caller cannot see, so a hidden draft or a term in a denied domain never shows up as a relation.
- **Known channels**: hiding a term is not confidentiality. Its existence can show through a 409 on
  creating a term with the same name, and a 400 on adding a synonym equal to its name.
- **Versioned changes**: name, definition, synonyms, links, properties, relations, domains and
  bindings.
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
- Dropping a term soft-deletes its synonyms, relations, domain links, bindings and version rows in
  the same transaction. Dropping a metalake soft-deletes all of its glossary rows in
  `MetalakeMetaService#deleteMetalake`, next to the tag cleanup.

### Access control

| Privilege            | On the metalake or a domain                                                             |
| -------------------- | --------------------------------------------------------------------------------------- |
| `VIEW_DEFINITION`    | Read `APPROVED` and `DEPRECATED` terms, relations and bindings; match                   |
| `CREATE_DEFINITION`  | Create terms; see `DRAFT` terms; edit and bind terms the caller owns; delete own drafts |
| `MANAGE_DEFINITIONS` | Edit and bind any term; set status; delete deprecated terms                             |

- `MANAGE_DEFINITIONS` includes `CREATE_DEFINITION`, which includes `VIEW_DEFINITION`. The names
  are generic so that other business-definition objects can share them.
- **Registration**: three new `Privilege.Name` values whose `canBindTo` accepts `METALAKE` and
  `DOMAIN` only; granting them on any other securable, or granting anything on `GLOSSARY_TERM`,
  fails with 400 in the role APIs.
- **Levels of a term**: the metalake, each of its domains, and each domain's current parent. A
  privilege holds when it is allowed on at least one level and denied on none. If Finance allows
  `VIEW_DEFINITION` and Sales denies it, a role cannot see a term in both. The metalake owner
  always passes.
- **Bindings** are written on the object, by the object's admin (`APPROVED` terms only) or by the
  term's editor; see Bindings.
- **Owner** (user or group) through `/owners/glossary_term/{term}`. When a group owns a term, its
  members count as the owner. Changing the owner needs the current owner, or `MANAGE_GRANTS` on the
  metalake or one of the term's domains.
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

| Operation              | Endpoint                                                                 | Request → response                                                      |
| ---------------------- | ------------------------------------------------------------------------ | ----------------------------------------------------------------------- |
| List, filter, match    | `GET /terms?details=&domain=&status=&nameContains=&match=&bindings=`     | → `NameListResponse` / `TermListResponse`                               |
| Create                 | `POST /terms`                                                            | `TermCreateRequest` → `TermResponse`                                    |
| Get                    | `GET /terms/{term}`                                                      | → `TermResponse`                                                        |
| Alter                  | `PUT /terms/{term}`                                                      | `TermUpdatesRequest` → `TermResponse`                                   |
| Set status             | `PATCH /terms/{term}`                                                    | `TermSetRequest{status}` → `TermResponse`                               |
| Drop                   | `DELETE /terms/{term}`                                                   | → `DropResponse`                                                        |
| Hierarchy              | `GET /terms/{term}/hierarchy?direction=BROADER\|NARROWER`                | → `TermTreeResponse`                                                    |
| Bindings of a term     | `GET /terms/{term}/objects?kind=&types=`                                 | → `TermBindingListResponse`                                             |
| Terms of an object     | `GET /objects/{type}/{fullName}/terms?details=&children=`                | → `NameListResponse` / `ObjectTermsResponse`                            |
| Bind / unbind          | `POST /objects/{type}/{fullName}/terms`                                  | `TermsAssociateRequest{termsToAdd, termsToRemove}` → `NameListResponse` |
| Terms of a metric      | `GET /objects/semantic_model/{fullName}/metrics/{metric}/terms?details=` | → `NameListResponse` / `TermListResponse`                               |
| Bind / unbind a metric | `POST /objects/semantic_model/{fullName}/metrics/{metric}/terms`         | `TermsAssociateRequest` → `NameListResponse`                            |
| Owner                  | existing `/owners/glossary_term/{term}`                                  |                                                                         |

```
TermCreateRequest     { name, definition, synonyms, domains, broader, related, links, sourceRef,
                        properties }
TermUpdatesRequest    { updates: [rename, updateDefinition, addSynonym, removeSynonym, addDomain,
                        removeDomain, addBroader, removeBroader, addRelated, removeRelated,
                        updateLinks, setSourceRef, setProperty, removeProperty] }
TermDTO               { name, definition, synonyms, status, domains, broader, narrower, related,
                        links, owner, sourceRef, properties, version, audit,
                        matchedBy?: [{word, field: NAME | SYNONYM}],
                        bindings?: [TermBindingDTO], bindingsTruncated? }
TermBindingDTO        { kind: IMPLEMENTED_BY | USED_BY, object: MetadataObjectDTO, metric? }
ObjectTermsResponse   { terms: [TermDTO], children: [{ name, terms: [TermDTO] }] }
TermsAssociateRequest { termsToAdd: [name], termsToRemove: [name] }
```

- **List** serves both browsing and lookup:
  - `domain` returns the terms in the domain's scope (the domain and the subdomains the caller can
    see); `nameContains` is a case-insensitive substring match on the name, for search boxes.
  - `match` (repeatable, up to 20) is the business-word lookup: `key(word)` is compared with
    `name_key` and `synonym_key`, and each returned term carries `matchedBy`. With
    `status=APPROVED`, a word that matches only `DEPRECATED` terms returns nothing.
  - `bindings=true` (only with `match` and `details=true`) adds each term's bindings, at most 20 per
    term, with `bindingsTruncated` when there are more. Lookup then takes one call; a word usually
    matches one to three terms.
  - A dedicated path such as `/terms/resolve` would collide with a term named `resolve`, so lookup
    stays a filter of the list.
- **Alter** applies all updates in one transaction and records one version. `narrower` is derived
  and read-only; `version` is the current version; `updateLinks` replaces the whole link list.
- **Terms of an object**: `children=true` is accepted on `table`, `view` and `semantic_model`, and
  fills `children` with one entry per column or metric that has visible terms. A semantic model
  itself has no terms, only its metrics do.
- **Bind / unbind** follow the tag association request shape; the response is the object's terms
  after the change.
- **Errors**: 404 `NoSuchTermException` (missing or not visible) and `NoSuchMetadataObjectException`;
  409 `TermAlreadyExistsException` (name or source reference) and `NonEmptyDomainException`; 400 for
  a cycle, a self-edge, a synonym equal to another term's name, deleting an `APPROVED` term, an
  object admin binding a term that is not `APPROVED`, or more than 20 `match` values; 403
  `ForbiddenException`.
- **Concurrency**: changes apply to the current state and the last write wins, as for other
  Gravitino metadata; there is no version check on write. Responses carry `version`, so a client
  can tell that a term changed since it read it.
- **Paging**: none, like the other core list APIs; it can be added with the Domain design's paging.
- **Encoding**: term names may contain `/`, URL-encoded as `%2F` in `{term}`, as for domain and tag
  names.
- **Clients** (Java, Python):
  - `GlossaryOperations` on `GravitinoMetalake`: `listTerms`, `listTermsInfo`, `getTerm`,
    `createTerm`, `alterTerm`, `setTermStatus`, `dropTerm`, and `matchTerms(words, domain)`, which
    returns approved terms with their bindings for agents that do not use MCP.
  - On a `Term`: `broader()`, `narrower()`, `listBindings(kind)`.
  - `SupportsTerms` (`listTerms()`, `associateTerms(add, remove)`) on catalogs, schemas, tables,
    views, columns, filesets, topics and models, plus `listColumnTerms()` on tables and views, and
    `listMetricTerms` / `associateMetricTerms` on `SemanticModel`.

### Web UI support

Each part of the glossary page and of the pages that show terms maps to the calls below. Names,
domains, status, version, owner, relations and links all come with the term, so the term page needs
no per-field call.

| Page                         | Part                                                                    | Calls                                                                                                 |
| ---------------------------- | ----------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| Glossary                     | Term tree (domain › subdomain › term, narrower terms nested, No domain) | `GET /terms?details=true` and `GET /domains?details=true`; built from `domains` and `broader`         |
| Glossary                     | Search terms                                                            | `GET /terms?nameContains=`                                                                            |
| Glossary, term               | Header, Definition, Details, Related terms                              | `GET /terms/{term}`; the source label comes from the `sourceRef` system, else "Gravitino"             |
| Glossary, term               | Implemented by, Used by                                                 | `GET /terms/{term}/objects?kind=IMPLEMENTED_BY`, `?kind=USED_BY`                                      |
| Glossary, term               | Approve, Deprecate                                                      | `PATCH /terms/{term}`                                                                                 |
| Glossary, term               | Delete term (dialog counts relations and asset links)                   | `DELETE /terms/{term}`; counts from the term and its bindings                                         |
| Glossary, Add or Edit term   | Name, Definition, Synonyms, Domain, Broader, Related, Documentation     | `POST /terms` or `PUT /terms/{term}` with the changed fields; pickers from `/domains` and `/terms`    |
| Glossary, Add term           | Owner other than the creator                                            | `POST /terms`, then `PUT /owners/glossary_term/{term}` (the creator is the owner, so this is allowed) |
| Explore, table or view       | Terms N, Update terms drawer, column chips and the term filter          | `GET /objects/{type}/{fullName}/terms?details=true&children=true`; `POST .../terms` per change        |
| Explore, Update terms drawer | Term picker (approved, grouped by domain)                               | `GET /terms?details=true&status=APPROVED&nameContains=`                                               |
| Semantic model               | Glossary terms of each metric                                           | `GET /objects/semantic_model/{fullName}/terms?details=true&children=true`                             |
| Metric editor                | Terms (approved only)                                                   | the picker call above; `POST /objects/semantic_model/{fullName}/metrics/{metric}/terms`               |
| Domain, Definitions tab      | Terms of the domain and its subdomains, name search                     | `GET /terms?details=true&domain=&nameContains=`                                                       |
| Domain, create               | Readable by all users, Glossary editors                                 | existing role APIs: grant the definition privileges on the domain                                     |
| Define, Overview             | Terms by domain, terms by status                                        | counted from `GET /terms?details=true`                                                                |

Not covered by this design, so these parts need another source or stay hidden:

- **Review**: Submit for review, Reject, In review, the version diff and History belong to an
  approval workflow; `TermChangeHandler` and the version rows are where it plugs in.
- **Counts that need an aggregate query**: terms without bindings, assets without a glossary term,
  and an object's "N restricted terms", which would also reveal hidden terms.
- **Undefined lookups**, **Import**, **KB articles** and the **managing group** label.
- **Buttons by privilege**: Gravitino has no API that returns the actions a caller may take on an
  object, so the page shows the 403 message instead of disabling a button in advance.

### Agent integration

A read-only `glossary` tool tag in `mcp-server/`, added to `SUPPORTED_TOOL_TAGS`. The MCP server
forwards the caller's `Authorization` header, so agents see exactly what the caller can see.

```
question ──▶ resolve_business_term(words) ──▶ terms + bindings
                                                │
               column / table binding ──────────┼──▶ get_table_metadata_details ──▶ SQL
               metric binding ──────────────────┴──▶ load_semantic_model ─────────▶ metric query
```

**`resolve_business_term`** is the tool an agent calls first:

- Input: `words` (the business phrases of the question, up to 20) and an optional `domain`.
- One REST call: `GET /terms?match=w1&match=w2&status=APPROVED&details=true&bindings=true[&domain=]`.
- Output, per word: `matches` (term, definition, `matched_by`, domains, broader and related names,
  and bindings as `{kind, type, catalog, schema, name, column?, metric?}`, plus
  `bindings_truncated`), and an `unmatched` list, so the agent knows which words have no governed
  meaning.
- Bindings are split into the arguments that `get_table_metadata_details` and
  `load_semantic_model` take, so the next call needs no parsing or extra lookup.

**Rules in the tool descriptions**, so agents use the result the same way:

1. Only `APPROVED` terms are returned. A word in `unmatched` has no agreed meaning: say so, and do
   not guess a column.
2. When a word matches several terms, use the domains to pick one, or ask the user.
3. A binding says where a meaning is implemented. It never grants access; reading the data goes
   through the usual privileges.

| Tool                      | REST calls                                                              | Use                                                              |
| ------------------------- | ----------------------------------------------------------------------- | ---------------------------------------------------------------- |
| `resolve_business_term`   | `GET /terms?match=&status=APPROVED&details=true&bindings=true`          | Map the words of a question to terms and the data behind them    |
| `get_term`                | `GET /terms/{term}`                                                     | Definition, synonyms, domains, relations                         |
| `list_terms`              | `GET /terms?details=true&status=APPROVED` (+ `domain`, `name_contains`) | Browse a domain's vocabulary                                     |
| `get_term_hierarchy`      | `GET /terms/{term}/hierarchy`                                           | Widen or narrow a concept                                        |
| `list_metadata_by_term`   | `GET /terms/{term}/objects`                                             | All objects and metrics of a term, when the lookup was truncated |
| `list_terms_for_metadata` | `GET /objects/{type}/{fullName}/terms?details=true&children=true`       | Explain what a table, its columns or a model's metrics mean      |

The tools other than `resolve_business_term` drop terms that are not `APPROVED` from their output.
Together with the `domain` tools of #13867, an agent can go from a domain to its terms to the data.

### Events and audit

- Every operation emits pre, success and failure events through a `GlossaryEventDispatcher` and
  reaches the audit log: create, alter, status, bind and unbind, drop. A drop also records one entry
  for each child that lost a broader term.

## Alternatives Considered

1. **A glossary container** (`metalake.glossary.term`). It adds a second grouping axis next to
   Domain and raises the question of which one governs privileges; Domain plus the term hierarchy
   is enough.
2. **Domain membership through backing tags**: see Option B in Domain membership.
3. **Terms as tags on assets.** `APPLY_TAG` does not match the definition privileges, tags have no
   definition, relations or status, and tags carry policies while bindings must not affect access.

## Prior Art

- **DataHub** glossary terms carry a definition, owners and a source reference, are grouped in
  nested term groups, relate through typed relations (Inherits From, Contains), and attach to
  datasets and columns. Term groups map to domains and parent terms here; Contains maps to
  `RELATED` and Inherits From to `BROADER`.
- **Unity Catalog** pages define a term inside a domain, with draft and published states and
  related assets; access is inherited from the domain, with no grants on a page. This design
  likewise grants on domains, never on a single term.

## Open Questions

1. **Domain membership of terms**: Option A (recommended) or Option B; to be agreed with the Domain
   design.
2. **One privilege set**: #13867 defines `MANAGE_DOMAIN` / `USE_DOMAIN` for domains. Should domains
   and terms share the definition privileges instead?
3. **Default read access**: terms should be readable by everyone by default, but Gravitino has no
   built-in role that every user holds. Add one (for example `PUBLIC`) holding `VIEW_DEFINITION` on
   the metalake, or ask admins to grant it?

## Task Breakdown

### Phase 0: Prerequisites

- [ ] Domain design (#13867) merged: domain storage, the parent resolver and domain authorization
- [ ] Decision on default read access (Open Question 3)

### Phase 1: Terms, storage and authorization

- [ ] `GlossaryTerm` API and `MetadataObject.Type.GLOSSARY_TERM` in `api/`; definition privileges on metalake and domain
- [ ] Storage: the six glossary tables and `glossary_hierarchy_lock` (H2, MySQL, PostgreSQL, upgrade scripts)
- [ ] `GlossaryManager`: CRUD, name and synonym rules, relations with the cycle check, domain membership, status, version rows, `TermChangeHandler`, locking
- [ ] Authorization: `TermScopeResolver`, the `GLOSSARY_TERM` case in `JcasbinAuthorizer`, draft visibility, owner-only edits for `CREATE_DEFINITION`
- [ ] REST endpoints for terms (including links), status and hierarchy; `GlossaryEventDispatcher`; the domain drop check
- [ ] Tests on H2, MySQL and PostgreSQL: name and synonym rules, cycle check under concurrent edits, deny across domains, draft visibility in relation lists, and a multi-node test (edit or move on one server, read on another)

### Phase 2: Bindings and lookup

- [ ] Binding storage; bind and unbind endpoints on objects and metrics with both privilege paths; cleanup on object drop, column drop and metric removal
- [ ] Binding reads: terms of a term's objects, of an object, of a metric, and `children=true` for tables, views and semantic models
- [ ] `domain`, `nameContains` and `match` filters on the list endpoint, and `bindings=true` with the per-term cap

### Phase 3: Clients, MCP and docs

- [ ] Java and Python clients, including `matchTerms`
- [ ] MCP `glossary` tools, with the agent rules in the tool descriptions and tests for unmatched and ambiguous words
- [ ] OpenAPI spec (validated with `./gradlew :docs:build`) and user docs
