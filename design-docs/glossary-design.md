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

Gravitino knows where data lives: catalogs, schemas and tables. With Domains
(apache/gravitino#13867), it also knows the business area of the data. But it does not know what a
business word means. Words like "Active Customer" or "Net Revenue" are defined in wikis and BI
tools. Each user and each agent maps the word to tables and metrics in its own way. Nobody owns the
definition.

A business glossary solves this. It is a managed list of **terms**. Each term has a definition,
synonyms, an owner and a status. Terms are grouped by domain and by a parent-child hierarchy. A term
is linked to the columns, tables and metrics behind it. An agent can look up a word from a question,
find the approved term, and follow its links to the data.

## Goals

1. **Term object**: a `GlossaryTerm` under the metalake. It has a name, definition, synonyms, owner,
   status, domains, documentation links, properties and an optional source reference. There is no
   glossary container. Domains and the term hierarchy group the terms.
2. **Hierarchy**: a term can have several broader (parent) terms, in any domain. Narrower (child)
   terms come from the other side. `RELATED` links two terms both ways. The server rejects cycles.
3. **Bindings**: a term can be linked to catalogs, schemas, tables, views, columns, filesets,
   topics, models and semantic-model metrics. A term lists its objects, and an object lists its
   terms. A binding never gives access to data.
4. **Domains**: a term belongs to zero or more domains. Grants on these domains control the term.
   The first version allows at most one domain per term. The storage and the API already support
   several domains, so the limit can be removed later without a migration or an API change.
5. **Status**: `DRAFT`, `APPROVED` or `DEPRECATED`, set through the API. Agents only see `APPROVED`
   terms.
6. **Privileges**: `VIEW_DEFINITION`, `CREATE_DEFINITION` and `MANAGE_DEFINITIONS`, granted on the
   metalake or a domain. Subdomains inherit them. Deny wins over allow. REST and MCP use the same
   rules.
7. **Versions**: every content change saves a version snapshot. An approval workflow or a history
   API can be added later without a schema change.
8. **APIs**: REST, Java and Python clients, read-only MCP tools, events and audit. Each view of the
   glossary page needs one or two REST calls. An agent can look up all business words of a question
   in one call.

## Non-Goals

1. **Approval workflow and version history API**: edits take effect at once. A workflow can be
   added later through `TermChangeHandler` (see Status and versions).
2. **Bulk import and export, connectors to other glossaries, SKOS export**: these tools can use the
   REST API and the source reference.
3. **Ranked, fuzzy or semantic lookup**: lookup is an exact match on names and synonyms.
4. **Access through terms**: a term or a binding never gives or blocks access to data. Terms have
   no policies.
5. **Agent writes and the UI itself**: the MCP tools are read-only. The UI uses the REST API. Web UI
   support lists the calls for each part of the page.

## Usage Scenario

1. An admin with `MANAGE_DEFINITIONS` on domain Sales creates term `Customer` in Sales. The term
   starts as `DRAFT`. Only users with `CREATE_DEFINITION` or `MANAGE_DEFINITIONS` can see it.
2. An analyst with `CREATE_DEFINITION` on Sales creates `Active Customer`. Its broader term is
   `Customer` and its synonym is `active user`. The analyst links it to column
   `dw.sales.customers.is_active` (Implemented by). The analyst owns this term and can edit it, but
   cannot edit `Customer`.
3. The admin sets both terms to `APPROVED`. Users with `VIEW_DEFINITION` can now see them.
4. The owner of semantic model `dw.sales.sales_model` links its metric `active_customers` to
   `Active Customer`. The term now shows the metric under Used by.
5. A user asks an agent for "revenue from active users last month". The agent calls
   `resolve_business_term(["revenue", "active users"])` once:
   - `active users` returns `Active Customer` (matched by synonym), with its column and metric. The
     result only contains what the user can read.
   - `revenue` has no match. The agent asks the user which revenue they mean. It does not guess a
     column.

   Then the agent loads the table or the semantic model with the existing tools.
6. Later the term is replaced. The admin sets it to `DEPRECATED`, and agents stop seeing it. Then
   the admin deletes it.

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

- The tables are added to the H2, MySQL and PostgreSQL schemas and to the `upgrade-1.3.0-to-2.0.0`
  scripts.
- **Normalized key**: `key(s)` trims `s`, turns each run of spaces into one space, and lower-cases
  it (`Locale.ROOT`). Names, synonyms and lookups all use `key()`, so they compare the same way.
- **Names** are unique in a metalake by `name_key = key(name)`. So `Churn` and `churn` are the same
  name on every database. Responses return the name as the user typed it. Names follow the tag and
  domain rules: up to 128 characters, and `/` is allowed (URL-encoded in paths).
- **Synonyms** are stored one per row, with `synonym_key = key(synonym)`. Two terms can share a
  synonym. A synonym cannot be the name of another term. The server checks this in both
  directions: when a synonym is added, and when a term is created or renamed (400).
- **Documentation links** (a label and a URL) are shown on the term page. They are stored in a JSON
  column of `glossary_term_meta`. No query reads inside them.
- **Source reference** is optional. When set, it is unique in the metalake. It records where an
  imported term came from, in the form `<system>:<id>`, for example
  `datahub:urn:li:glossaryTerm:Customer`. A new import can then update the term instead of creating
  a copy. A term without a source reference was created in Gravitino.
- `MetadataObject.Type.GLOSSARY_TERM` is added for owners, events and audit. Privileges **cannot be
  granted on a single term**: the role APIs reject them.
- Terms, relations and bindings are not cached. Every request reads the store, the same as for
  domains.

### Relations

- `BROADER` is stored on the child. A → B means "A is a kind of B". Narrower terms are not stored;
  the server finds them from the `BROADER` rows. `RELATED` is stored once per pair (with
  `from_term_id < to_term_id`) and shows on both terms.
- Setting the broader or related term of A only changes A. The caller needs the edit privilege on A
  and `VIEW_DEFINITION` on the other term.
- **Cycle check**: adding A → B fails with 400 if B already reaches A through `BROADER` links. The
  server walks up from B, level by level, under the hierarchy lock (see Locking). A term cannot be
  its own parent.
- A hierarchy read returns all ancestors or all descendants of a term. There are no cycles, so the
  walk always ends.
- Deprecating a term does not change its children. Deleting a term removes its links. Each child
  that loses a parent gets its own audit record. The child keeps its other parents, or becomes a
  top-level term.

### Domain membership

A term belongs to zero or more domains. The grants on these domains control the term. A term
without a domain is controlled by metalake grants only. There are two ways to store the membership.

**Option A: explicit relation (recommended)**

- A table `glossary_term_domain_rel (term_id, domain_id)`. Only the term API changes it: `domains`
  on create, `addDomain` and `removeDomain`. These changes go through `TermChangeHandler`, like any
  term change.
- To add a term to a domain, the caller needs the edit privilege on the term and
  `CREATE_DEFINITION` on that domain. So nobody can move a term into a domain where they cannot
  write.
- **Terms of a domain** (`GET /terms?domain=`) use the same scope as the Domain design: terms in
  the domain, and terms in its subdomains that the caller can see.
- Dropping a domain that still has terms fails with 409 `NonEmptyDomainException`. This is the same
  as a domain with subdomains. Removing the terms from the domain would change who manages them.
  The change is one more check in `DomainManager#dropDomain`.

**Option B: backing tags, the same as assets in #13867**

- `GLOSSARY_TERM` becomes taggable and is added to `DomainMember.SUPPORTED_TYPES`. A term is in
  domain D when it has D's backing (tag, value). Terms then show in `GET /domains/{domain}/objects`
  and in the domains of an object. There is no relation table.
- The normal tag rules are not enough here:
  - For an asset, read access plus `APPLY_TAG` on a backing tag adds the asset to a domain. This
    gives nobody any permission.
  - For a term, the domain decides who manages the term. A user with only `VIEW_DEFINITION` and
    `APPLY_TAG` could put a term into Finance, and the Finance admins would then control it.

  So the tag code needs two special rules for `GLOSSARY_TERM`:
  1. Besides `APPLY_TAG`, the caller also needs the edit privilege on the term and
     `CREATE_DEFINITION` on the domain behind the (tag, value). The domain is found with the Domain
     design's reverse lookup.
  2. A backing-tag change on a term goes through `TermChangeHandler`, like any other term change.
- If a domain's backing tag is changed or deleted, its terms silently leave the domain. Who manages
  them changes, but nobody edited a term. Dropping a domain works the same as for assets.

**Recommendation: Option A.** The reasons:

1. **For a term, the domain decides who can read and manage it.** For an asset, being in a domain
   gives nobody any permission. That is why the Domain design lets anyone with `APPLY_TAG` add an
   asset. It is also why the Domain design does not allow access grants on domains. Terms are the
   opposite case: the grants on a term's domains decide who can see, edit and approve it. With
   Option B, adding a tag would change these permissions.
2. **Tag permissions go to other people.** In the Domain design, admins give `APPLY_TAG` on a
   domain's tag to the owners of the tables in that domain. With Option B, any of these table
   owners could move a term into the domain. The domain admins would then control the term. Or the
   term would be hidden from a role that the domain denies. One tag cannot allow this for tables
   and forbid it for terms.
3. **The benefits of tags do not apply to terms.** The Domain design uses tags for three things:
   inheritance from catalogs and schemas, tag-based policies, and the existing tag checks. Terms
   are not under a catalog or schema. Terms have no policies. And terms need stricter checks than
   `APPLY_TAG`.
4. **Permissions do not change by accident.** With Option B, replacing or deleting a domain's tag
   silently removes its terms. Who can see or edit them changes, but nobody edited a term. With
   Option A, membership changes only through the term API. It is checked, versioned and audited
   like any other term edit.
5. **Smaller change to the Domain design.** Option A adds one check when a domain is dropped.
   Option B makes terms taggable, adds a member type, and adds term-only rules to the shared tag
   code.

The cost: the terms of a domain are listed by `GET /terms?domain=`, not by
`GET /domains/{domain}/objects`. Both use the same rules: a term can be in several domains, and a
domain includes its subdomains.

### Bindings

A binding links a term to the data behind it. A term page shows two kinds of bindings. An object
page shows the terms linked to the object.

| Kind               | Meaning                                        | Targets                                                                     | Example                                                                                        |
| ------------------ | ---------------------------------------------- | --------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| **Implemented by** | The object stores or computes the term         | `CATALOG`, `SCHEMA`, `TABLE`, `VIEW`, `COLUMN`, `FILESET`, `TOPIC`, `MODEL` | `Customer` is implemented by table `sales.public.customers` and column `customers.customer_id` |
| **Used by**        | A semantic-model metric is built from the term | a metric: (`SEMANTIC_MODEL`, `metricName`)                                  | metric `net_revenue` uses `Net revenue` and `Order`                                            |

Both kinds are stored in `glossary_term_binding`. `metric_name` is set only for Used by, so it
tells the two kinds apart. A binding is direct only: binding a schema does not bind its tables.

**One write endpoint, two ways to be allowed**

Bindings are written on the object, the same as tags and policies:
`POST /objects/{type}/{fullName}/terms`, or `POST /objects/semantic_model/{fullName}/metrics/{metric}/terms`
for a metric. Two kinds of people know about a binding:

- the object admin knows what the object means;
- the term owner knows which data implements the term.

Both can write bindings. The server checks each term in the request separately:

| Caller       | Needs                                                            | Terms allowed                                     |
| ------------ | ---------------------------------------------------------------- | ------------------------------------------------- |
| Object admin | Admin privilege on the object, and `VIEW_DEFINITION` on the term | `APPROVED` only                                   |
| Term editor  | Edit privilege on the term, and read access to the object        | Any status, so a draft can get its bindings early |

- **Admin privilege on an object**:
  - tables and columns: the existing `MODIFY_TABLE_AUTHORIZATION_EXPRESSION` (owner of the table or
    of a parent, or `MODIFY_TABLE` with `USE_CATALOG` and `USE_SCHEMA`);
  - metrics: `MODIFY_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION`;
  - other types have no modify privilege, so the caller must own the object or one of its parents
    (metalake, catalog, schema).
- The endpoint checks the object once, and then checks each term in the request body in code. If
  one term fails both checks, the whole request fails with 403 and nothing changes. Removing a
  binding needs the same privilege as adding it.
- Each changed term saves a version and goes through `TermChangeHandler`. So a workflow can hold a
  binding change, like any other term change.
- Binding a missing object or metric fails with 404. If an object admin binds a term that is not
  `APPROVED`, the request fails with 400.
- When a term is deprecated later, it keeps its bindings. It no longer shows in lookups or MCP
  results.

**Reads**

- `GET /terms/{term}/objects?kind=IMPLEMENTED_BY|USED_BY&types=` lists the bindings of a term. It
  only returns objects and metrics the caller can access (`filterMetadataObject`). For a metric,
  the caller needs access to its semantic model.
- `GET /objects/{type}/{fullName}/terms` and `GET /objects/semantic_model/{fullName}/metrics/{metric}/terms`
  list the terms of an object or a metric. They only return terms the caller can see.
- `?children=true` on a table, view or semantic model also returns the terms of each column or
  metric. A schema tab or a metric table then loads in one call, not one call per column. On the
  server it is one query on `glossary_term_binding`, by the object and its column IDs (or by
  `object_id` for metrics).

**Cleanup**

- Dropping an object soft-deletes its bindings. This happens in the same places that clean up
  `tag_relation_meta` (by object, schema, catalog and metalake). Renames keep the bindings, because
  bindings use IDs.
- Dropping a column in `alterTable` soft-deletes the bindings of the column, in the same
  transaction.
- Metrics have no IDs. When a semantic model is altered, the bindings of removed metrics are
  soft-deleted. A renamed metric loses its bindings and must be linked again.

### Status and versions

| Status            | Who can see it                              | Set by               | Who can delete it                                       |
| ----------------- | ------------------------------------------- | -------------------- | ------------------------------------------------------- |
| `DRAFT` (initial) | `CREATE_DEFINITION` or `MANAGE_DEFINITIONS` | creation             | owner with `CREATE_DEFINITION`, or `MANAGE_DEFINITIONS` |
| `APPROVED`        | `VIEW_DEFINITION`; MCP                      | `MANAGE_DEFINITIONS` | nobody; deprecate it first                              |
| `DEPRECATED`      | `VIEW_DEFINITION`; not MCP                  | `MANAGE_DEFINITIONS` | `MANAGE_DEFINITIONS`                                    |

- A user with `MANAGE_DEFINITIONS` on the metalake or on one of the term's domains sets the status
  with `PATCH`. Edits take effect at once in any status, and are audited.
- **Every API shows the same terms** (REST, clients, MCP). If the caller cannot see a term, lists
  leave it out and get returns 404. The same rule applies inside responses. The `broader`,
  `narrower` and `related` lists of a term, the terms of an object, and the terms of a domain all
  leave out terms the caller cannot see. So a hidden draft, or a term in a denied domain, never
  shows up as a relation.
- **Known leaks**: hiding a term does not make it secret. A user can still find out that it exists
  in two ways: creating a term with the same name returns 409, and adding a synonym equal to its
  name returns 400.
- **Changes that create a version**: name, definition, synonyms, links, properties, relations,
  domains and bindings. Owner and status changes are only audited.
- **Snapshot**: each version row stores the whole term as JSON: fields, synonyms, broader and
  related term IDs, domain IDs and bindings. The live tables only hold the current state. The
  snapshot makes it possible to store a change before it is applied. This is the same pattern as
  the existing `current_version` + `table_column_version_info`.
- **Write path**: in the same transaction as the change, and under the exclusive lock of the term:
  increase `current_version`, insert a `CURRENT` row, and mark the old row `SUPERSEDED`. Cost: one
  insert and one update per edit. A snapshot gets bigger when the term has more bindings.
- **Read path**: this design has no version API. `GET /terms/{term}/versions` can be added later
  without a storage change.
- **Workflow hook**: `GlossaryManager` sends every content change to a `TermChangeHandler`. The
  default handler applies the change at once, as above. An approval workflow can replace it:
  1. on a change, write a `PENDING` row and leave the live tables alone;
  2. on approval, apply the snapshot and mark the row `CURRENT`.
- Dropping a term soft-deletes its synonyms, relations, domain links, bindings and version rows, in
  the same transaction. Dropping a metalake soft-deletes all its glossary rows in
  `MetalakeMetaService#deleteMetalake`, next to the tag cleanup.

### Access control

| Privilege            | Granted on the metalake or a domain; allows                                   |
| -------------------- | ----------------------------------------------------------------------------- |
| `VIEW_DEFINITION`    | Read `APPROVED` and `DEPRECATED` terms, relations and bindings; look up words |
| `CREATE_DEFINITION`  | Create terms; see `DRAFT` terms; edit and bind own terms; delete own drafts   |
| `MANAGE_DEFINITIONS` | Edit and bind any term; set status; delete deprecated terms                   |

- `MANAGE_DEFINITIONS` includes `CREATE_DEFINITION`, and `CREATE_DEFINITION` includes
  `VIEW_DEFINITION`. The names are general, so other business-definition objects can use them too.
- **Registration**: three new `Privilege.Name` values. Their `canBindTo` accepts `METALAKE` and
  `DOMAIN` only. The role APIs return 400 when these privileges are granted on any other object,
  or when anything is granted on `GLOSSARY_TERM`.
- **Levels of a term**: the metalake, each domain of the term, and the current parent of each
  domain. A privilege holds when at least one level allows it and no level denies it. Example: if
  Finance allows `VIEW_DEFINITION` and Sales denies it, a role cannot see a term that is in both.
  The metalake owner always passes.
- **Bindings** are written on the object, by the object admin (`APPROVED` terms only) or by the
  term editor. See Bindings.
- **Owner** (a user or a group), set through `/owners/glossary_term/{term}`. When a group owns a
  term, all its members count as the owner. To change the owner, the caller must be the current
  owner, or have `MANAGE_GRANTS` on the metalake or on one of the term's domains.
- **Create** needs `CREATE_DEFINITION` on every domain in the request. With no domain, it needs
  `CREATE_DEFINITION` on the metalake. The creator becomes the owner.
- **Implementation**: `TermScopeResolver` returns the levels of a term, from its domains and the
  Domain design's parent resolver. It works in batches for list filtering.
  `JcasbinAuthorizer#hasMetadataPrivilegePermission` uses it for `GLOSSARY_TERM`, at the place where
  it walks to the parent today. All levels are combined in one check, the same as for catalog,
  schema and table. Domains and terms in a request body are checked in code.

### Locking

Term operations use the existing lock tools of the store, always in this order:

1. the live-endpoint fence (`LiveEndpointService.lockLiveEndpoint`, shared): the metalake, plus the
   chain of the bound object when binding;
2. the per-metalake hierarchy lock, if needed;
3. term rows, in ID order (exclusive);
4. domain rows (shared).

Each row is locked only once, in its final mode.

| Operation                       | Locks                                                                   | Conflict it prevents                     |
| ------------------------------- | ----------------------------------------------------------------------- | ---------------------------------------- |
| Create                          | metalake (shared) → domains (shared)                                    | metalake or domain drop                  |
| Edit content, synonyms, domains | term (exclusive) → added domains (shared)                               | edits at the same time; domain drop      |
| Add a `BROADER` link            | metalake (shared) → hierarchy lock → child (exclusive)                  | two links that together form a cycle     |
| Add a `RELATED` link            | both terms (exclusive, ID order)                                        | the other term being deleted             |
| Bind / unbind                   | object chain (`lockLiveEndpoint`, shared) → terms (exclusive, ID order) | object drop; term edits at the same time |
| Set status, delete              | term (exclusive)                                                        | edits at the same time                   |

- The **hierarchy lock** is a `SELECT … FOR UPDATE` on one row per metalake in
  `glossary_hierarchy_lock`. The row is inserted on first use. Locking only the two terms is not
  enough. Example: A → B and C → D are added at the same time, then B → C and D → A. Each link
  passes its own check, but together they form a cycle. `BROADER` edits are rare, so running them
  one at a time per metalake is cheap.
- For operations by name, the server finds the row by name, locks it, and then checks the name and
  `deleted_at` again. If they changed, it returns 404. The unique key on the name protects
  concurrent creates (409).

### REST API

All paths are under `/api/metalakes/{metalake}`. They follow the Tag, Policy and Domain APIs:

- a top-level `/terms` collection (there is no glossary container, so no `/glossary` level);
- `?details=` on lists;
- `PUT` with a list of updates to alter, and `PATCH` to set the status;
- `DropResponse` on delete;
- bindings are written on the object.

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

- **List** is used both to browse and to look up words:
  - `domain` returns the terms of the domain and of the subdomains the caller can see.
  - `nameContains` finds names that contain the text, ignoring case. It is for search boxes.
  - `match` looks up business words. It can be repeated, up to 20 times. The server compares
    `key(word)` with `name_key` and `synonym_key`. Each returned term has `matchedBy`. With
    `status=APPROVED`, a word that only matches `DEPRECATED` terms returns nothing.
  - `bindings=true` adds the bindings of each term, at most 20 per term. If there are more,
    `bindingsTruncated` is set. It only works together with `match` and `details=true`. A lookup
    then needs only one call. A word usually matches one to three terms.
  - Why not a separate path like `/terms/resolve`? It would clash with a term named `resolve`. So
    lookup is a filter of the list.
- **Alter** applies all updates in one transaction and saves one version. `narrower` is computed
  and cannot be set. `version` is the current version. `updateLinks` replaces the whole link list.
- **Terms of an object**: `children=true` works on `table`, `view` and `semantic_model`. It adds
  one entry to `children` for each column or metric with visible terms. A semantic model itself
  has no terms; only its metrics do.
- **Bind / unbind** use the same request format as tag association. The response is the object's
  terms after the change.
- **Errors**:
  - 404: `NoSuchTermException` (missing, or the caller cannot see it), `NoSuchMetadataObjectException`;
  - 409: `TermAlreadyExistsException` (same name or same source reference), `NonEmptyDomainException`;
  - 400: a cycle, a term as its own parent, a synonym equal to another term's name, deleting an
    `APPROVED` term, an object admin binding a term that is not `APPROVED`, or more than 20 `match`
    values;
  - 403: `ForbiddenException`.
- **Concurrency**: changes apply to the current state, and the last write wins, the same as other
  Gravitino metadata. Writes do not check the version. Responses include `version`, so a client
  can see that a term changed after it read it.
- **Paging**: none, the same as the other core list APIs. It can come later with the paging of the
  Domain design.
- **Encoding**: a term name can contain `/`. In `{term}` it must be URL-encoded as `%2F`, the same
  as domain and tag names.
- **Clients** (Java, Python):
  - `GlossaryOperations` on `GravitinoMetalake`: `listTerms`, `listTermsInfo`, `getTerm`,
    `createTerm`, `alterTerm`, `setTermStatus`, `dropTerm`, and `matchTerms(words, domain)`.
    `matchTerms` returns approved terms with their bindings. It is for agents that do not use MCP.
  - On a `Term`: `broader()`, `narrower()`, `listBindings(kind)`.
  - `SupportsTerms` (`listTerms()`, `associateTerms(add, remove)`) on catalogs, schemas, tables,
    views, columns, filesets, topics and models. Tables and views also have `listColumnTerms()`.
    `SemanticModel` has `listMetricTerms` and `associateMetricTerms`.

### Web UI support

The table shows the REST calls behind each part of the glossary page, and of the other pages that
show terms. A term comes with its name, domains, status, version, owner, relations and links. So
the term page does not need a call per field.

| Page                         | Part                                                                    | Calls                                                                                                     |
| ---------------------------- | ----------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| Glossary                     | Term tree (domain › subdomain › term, narrower terms nested, No domain) | `GET /terms?details=true` and `GET /domains?details=true`; the tree is built from `domains` and `broader` |
| Glossary                     | Search terms                                                            | `GET /terms?nameContains=`                                                                                |
| Glossary, term               | Header, Definition, Details, Related terms                              | `GET /terms/{term}`; the source label is the `sourceRef` system, or "Gravitino" if empty                  |
| Glossary, term               | Implemented by, Used by                                                 | `GET /terms/{term}/objects?kind=IMPLEMENTED_BY`, `?kind=USED_BY`                                          |
| Glossary, term               | Approve, Deprecate                                                      | `PATCH /terms/{term}`                                                                                     |
| Glossary, term               | Delete term (the dialog counts relations and asset links)               | `DELETE /terms/{term}`; the counts come from the term and its bindings                                    |
| Glossary, Add or Edit term   | Name, Definition, Synonyms, Domain, Broader, Related, Documentation     | `POST /terms` or `PUT /terms/{term}` with the changed fields; pickers use `/domains` and `/terms`         |
| Glossary, Add term           | Owner other than the creator                                            | `POST /terms`, then `PUT /owners/glossary_term/{term}` (allowed, because the creator is the owner)        |
| Explore, table or view       | Terms N, Update terms drawer, column chips and the term filter          | `GET /objects/{type}/{fullName}/terms?details=true&children=true`; `POST .../terms` per change            |
| Explore, Update terms drawer | Term picker (approved, grouped by domain)                               | `GET /terms?details=true&status=APPROVED&nameContains=`                                                   |
| Semantic model               | Glossary terms of each metric                                           | `GET /objects/semantic_model/{fullName}/terms?details=true&children=true`                                 |
| Metric editor                | Terms (approved only)                                                   | the picker call above; `POST /objects/semantic_model/{fullName}/metrics/{metric}/terms`                   |
| Domain, Definitions tab      | Terms of the domain and its subdomains, name search                     | `GET /terms?details=true&domain=&nameContains=`                                                           |
| Domain, create               | Readable by all users, Glossary editors                                 | existing role APIs: grant the definition privileges on the domain                                         |
| Define, Overview             | Terms by domain, terms by status                                        | counted from `GET /terms?details=true`                                                                    |

This design does not cover the parts below. They need another source, or the page hides them:

- **Review**: Submit for review, Reject, In review, the version diff and History. They belong to an
  approval workflow. The workflow can use `TermChangeHandler` and the version rows.
- **Counts that need an aggregate query**: terms without bindings, and assets without a glossary
  term. Also the "N restricted terms" count on an object, because it would show that hidden terms
  exist.
- **Undefined lookups**, **Import**, **KB articles** and the **managing group** label.
- **Buttons by privilege**: Gravitino has no API that tells which actions a caller can take on an
  object. So the page cannot disable a button in advance. It shows the 403 message instead.

### Agent integration

The `mcp-server/` gets a read-only `glossary` tool tag, added to `SUPPORTED_TOOL_TAGS`. The MCP
server forwards the caller's `Authorization` header. So an agent sees exactly what its user can
see.

```
question ──▶ resolve_business_term(words) ──▶ terms + bindings
                                                │
               column / table binding ──────────┼──▶ get_table_metadata_details ──▶ SQL
               metric binding ──────────────────┴──▶ load_semantic_model ─────────▶ metric query
```

**`resolve_business_term`** is the first tool an agent calls:

- Input: `words` (the business phrases in the question, up to 20), and an optional `domain`.
- It makes one REST call:
  `GET /terms?match=w1&match=w2&status=APPROVED&details=true&bindings=true[&domain=]`.
- Output, for each word:
  - `matches`: the term, definition, `matched_by`, domains, broader and related names, and the
    bindings as `{kind, type, catalog, schema, name, column?, metric?}`, plus `bindings_truncated`;
  - `unmatched`: the words with no match, so the agent knows these words have no agreed meaning.
- The binding fields are the same arguments that `get_table_metadata_details` and
  `load_semantic_model` take. The agent can make the next call directly, with no parsing and no
  extra lookup.

**Rules in the tool descriptions**, so all agents use the result the same way:

1. Only `APPROVED` terms are returned. A word in `unmatched` has no agreed meaning. Tell the user,
   and do not guess a column.
2. If a word matches several terms, use the domains to pick one, or ask the user.
3. A binding shows where a meaning is stored. It never gives access. Reading the data still needs
   the normal privileges.

| Tool                      | REST calls                                                              | Use                                                              |
| ------------------------- | ----------------------------------------------------------------------- | ---------------------------------------------------------------- |
| `resolve_business_term`   | `GET /terms?match=&status=APPROVED&details=true&bindings=true`          | Map the words of a question to terms and their data              |
| `get_term`                | `GET /terms/{term}`                                                     | Definition, synonyms, domains, relations                         |
| `list_terms`              | `GET /terms?details=true&status=APPROVED` (+ `domain`, `name_contains`) | Browse the terms of a domain                                     |
| `get_term_hierarchy`      | `GET /terms/{term}/hierarchy`                                           | Find wider or narrower terms                                     |
| `list_metadata_by_term`   | `GET /terms/{term}/objects`                                             | All objects and metrics of a term, when the lookup was truncated |
| `list_terms_for_metadata` | `GET /objects/{type}/{fullName}/terms?details=true&children=true`       | Explain a table, its columns or the metrics of a model           |

The other tools also remove terms that are not `APPROVED` from their output. With the `domain`
tools of #13867, an agent can go from a domain to its terms, and then to the data.

### Events and audit

- Every operation sends pre, success and failure events through a `GlossaryEventDispatcher`, and
  is written to the audit log: create, alter, status, bind and unbind, drop. When a term is
  dropped, each child that lost a parent also gets its own audit entry.

## Alternatives Considered

1. **A glossary container** (`metalake.glossary.term`). It adds a second way to group terms, next
   to domains. Then it is unclear which one controls privileges. Domains plus the term hierarchy
   are enough.
2. **Domain membership through backing tags**: see Option B in Domain membership.
3. **Terms as tags on assets.** `APPLY_TAG` is a different privilege from the definition
   privileges. Tags have no definition, relations or status. Tags can carry policies, but a binding
   must never change access.

## Prior Art

- **DataHub**: a glossary term has a definition, owners and a source reference. Terms are grouped
  in nested term groups. They have typed relations (Inherits From, Contains), and are attached to
  datasets and columns. In this design, term groups become domains and parent terms. Contains
  becomes `RELATED`, and Inherits From becomes `BROADER`.
- **Unity Catalog**: a page defines a term inside a domain. It has draft and published states, and
  related assets. Access comes from the domain; nobody grants access on a single page. This design
  works the same way: grants are on domains, never on a single term.

## Open Questions

1. **Domain membership of terms**: Option A (recommended) or Option B? This needs agreement with
   the Domain design.
2. **One privilege set**: #13867 uses `MANAGE_DOMAIN` / `USE_DOMAIN` for domains. Should domains
   and terms use the same definition privileges instead?
3. **Default read access**: everyone should be able to read terms by default. But Gravitino has no
   built-in role for all users. Should we add one (for example `PUBLIC`) with `VIEW_DEFINITION` on
   the metalake, or should admins grant it?

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
- [ ] Tests on H2, MySQL and PostgreSQL: name and synonym rules, cycle check with concurrent edits, deny across domains, hidden drafts in relation lists, and a multi-node test (edit or move on one server, read on another)

### Phase 2: Bindings and lookup

- [ ] Binding storage; bind and unbind endpoints on objects and metrics with both privilege paths; cleanup on object drop, column drop and metric removal
- [ ] Binding reads: objects of a term, terms of an object or a metric, and `children=true` for tables, views and semantic models
- [ ] `domain`, `nameContains` and `match` filters on the list endpoint, and `bindings=true` with the limit per term

### Phase 3: Clients, MCP and docs

- [ ] Java and Python clients, including `matchTerms`
- [ ] MCP `glossary` tools, with the agent rules in the tool descriptions and tests for unmatched and ambiguous words
- [ ] OpenAPI spec (validated with `./gradlew :docs:build`) and user docs
