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
| 0.6     | Qi Yu  | 2026-11-09 | Draft  |

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

The glossary is built in two steps. **Step 1** is this design: terms, relations, bindings, status,
privileges on the metalake, APIs and agent lookup. **Step 2** adds review of changes, version
history and grants on domains. Implementation Steps lists what each step contains.

## Goals

1. **Term object**: a `GlossaryTerm` under the metalake. It has a name, definition, synonyms, owner,
   status, domains, documentation links, properties and an optional source reference. There is no
   glossary container. Domains and the term hierarchy group the terms. Terms can carry tags through
   the existing tag APIs.
2. **Hierarchy**: a term can have several broader (parent) terms, in any domain. Narrower (child)
   terms come from the other side. `RELATED` links two terms both ways. The server rejects cycles.
3. **Bindings**: a term can be linked to catalogs, schemas, tables, views, columns, filesets,
   topics and models (where the data is), and to semantic-model metrics (what is computed from it).
   Column bindings in Step 1 support table columns only; views themselves can be bound. A term
   lists its objects, and an object lists its terms. A binding never gives access to data.
4. **Domains**: a term belongs to zero or more domains. A domain is optional when a term is
   created. In Step 1, domains group and filter terms. They do not change who can see or manage a
   term.
5. **Status**: `DRAFT`, `APPROVED` or `DEPRECATED`. New terms are `APPROVED`. Anyone who can edit a
   term can set its status, in the create or update call. There are no transition rules. Agents
   only see `APPROVED` terms they can read. Step 1 has no approval decision; `APPROVED` means
   available for agent use.
6. **Privileges**: `VIEW_DEFINITION`, `CREATE_DEFINITION` and `MANAGE_DEFINITIONS`, granted on the
   metalake. CREATE permits creation and maintenance of own terms; MANAGE permits reading,
   editing and deleting any term, without an approval role in Step 1. Drafts are visible only to
   their owner with CREATE and to MANAGE. Deny wins over allow. REST and MCP use the same rules.
7. **Revisions**: every content change increases the revision number of the term, and is audited.
   There are no content snapshots and no history API in Step 1.
8. **APIs**: REST, Java and Python clients, read-only MCP tools, events and audit. Each view of the
   glossary page needs one or two REST calls. An agent can look up all business words of a question
   in one call.

## Non-Goals

1. **Approval workflow, version history and grants on domains**: these come in Step 2 (see
   Implementation Steps). In Step 1, edits take effect at once. Step 2 plugs in through
   `TermChangeHandler` (see Status and versions).
2. **CSV import/export in Step 1, external connectors and SKOS export**: Step 1 provides APIs
   and a source reference for future importers. CSV is retained as Step 2 task S2.6, not dropped
   from the feature plan. External connectors and SKOS export remain later work.
3. **Ranked, fuzzy or semantic lookup in Step 1**: Step 1 only matches names and synonyms exactly
   (ignoring case and extra spaces). This is enough for words that the glossary defines. Better
   matching can come later without an API change: `matchedBy` can get new values (for example
   `FUZZY` or `SEMANTIC`) and a score, and `resolve_business_term` keeps the same input and output.
4. **Access through terms**: a term or a binding never gives or blocks access to data. Terms have
   no policies.
5. **Agent writes and the UI itself**: the MCP tools are read-only. The UI uses the REST API. Web UI
   support lists the calls for each part of the page.

## Delivery Scope and Tracking

This is a design PR. T1–T19 describe future implementation work, not runtime functionality
delivered by merging this document. [Issue #14238](https://github.com/apache/gravitino/issues/14238)
also lists design requirements assigned to Step 2, including CSV import/export. The scope is:

| Requirement in #14238                                                                                   | Step 1 design                                                                | Later work and tracking                                             |
| ------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| Terms, explicit domain membership, hierarchy, asset/metric bindings, REST, clients and exact MCP lookup | Covered by this document and T1–T19; membership uses selected Option A       | Step 2 extends the same identities and live store                   |
| Definition grants on domains                                                                            | Metalake grants only                                                         | S2.3: domain grants and inherited authorization                     |
| Version snapshots and history                                                                           | Revision and audit only; no snapshots                                        | S2.2: approved snapshots, history and diffs                         |
| Independent review and no self-approval                                                                 | No review; default APPROVED and immediate edits                              | S2.1: proposed changes, reviewer authorization and no self-approval |
| CSV import/export                                                                                       | No CSV endpoint or format; APIs and source references are prerequisites only | S2.6: a separate CSV design and implementation task                 |

Until the remaining design requirements are accepted or explicitly split into linked follow-up
issues, #14238 remains open. The PR description should use `Related to #14238`, rather than
`Fixes #14238`. The S2 task IDs below track the deferred scope locally; they are not GitHub issue
numbers. Filing the follow-up issues and linking them from #14238 is still a separate step.
CSV remains planned, with its scope tracked in S2.6.

## Usage Scenario

1. An admin with `MANAGE_DEFINITIONS` on the metalake creates term `Customer` in domain Sales. New
   terms are `APPROVED`, so users with `VIEW_DEFINITION` can see it at once.
2. An analyst with `CREATE_DEFINITION` on the metalake creates `Active Customer` with status
   `DRAFT`, so agents do not use it yet. Its broader term is `Customer` and its synonym is
   `active user`. The analyst links it to column `dw.sales.customers.is_active` (Implemented by).
3. When the definition is ready, the analyst sets `Active Customer` to `APPROVED`. The analyst owns
   this term, so the analyst can edit it and set its status. The analyst cannot edit `Customer`.
4. The owner of semantic model `dw.sales.sales_model` links its metric `active_customers` to
   `Active Customer`. The term now shows the metric under Used by.
5. A user asks an agent for "net revenue from VIP active users last month". The agent picks the
   business words and calls `resolve_business_term(["net revenue", "VIP", "active user"])` once:
   - `active user` returns `Active Customer` (matched by synonym), with its column and metric.
     `net revenue` returns `Net revenue`, with its metric. The result only contains what the user
     can read.
   - `VIP` has no match. The agent asks the user what VIP means. It does not guess a column.

   Then the agent loads the table or the semantic model with the existing tools. The full flow is
   in Agent integration.
6. Later the term is replaced. The owner or the admin can set it to `DEPRECATED`, and agents stop
   seeing it. The admin can delete it in any status; deprecation is optional before deletion.

## Design

### Data model

```
GlossaryTerm { id, name, definition, synonyms[], status: DRAFT | APPROVED | DEPRECATED,
               links[{label, url}], sourceRef?, properties, currentVersion, audit_info }
               // tags: the existing tag relation tables
TermRelation { fromTermId, toTermId, type: BROADER | RELATED, audit_info }
TermDomain   { termId, domainId, audit_info }                // Option A in Domain membership
TermBinding  { termId, targetId, targetType, audit_info }
TermVersion  { termId, version, state, snapshot, audit_info }   // created now; rows from Step 2
```

| Table                        | Unique key                                                                     | Other index                              |
| ---------------------------- | ------------------------------------------------------------------------------ | ---------------------------------------- |
| `glossary_term_meta`         | `(metalake_id, name_key, deleted_at)`, `(metalake_id, source_ref, deleted_at)` |                                          |
| `glossary_term_synonym`      | `(term_id, synonym_key, deleted_at)`                                           | `(metalake_id, synonym_key, deleted_at)` |
| `glossary_term_rel`          | `(from_term_id, to_term_id, rel_type, deleted_at)`                             | `(to_term_id, rel_type)`                 |
| `glossary_term_domain_rel`   | `(term_id, domain_id, deleted_at)`                                             | `(domain_id)`                            |
| `glossary_term_binding`      | `(term_id, target_type, target_id, deleted_at)`                                | `(target_type, target_id)`               |
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
  directions: when a synonym is added, and when a term is created or renamed (400). Create, rename
  and synonym changes take the same per-metalake hierarchy lock before checking (see Locking).
- **Documentation links** (a label and a URL) are shown on the term page. They are stored in a JSON
  column of `glossary_term_meta`. No query reads inside them.
- **Source reference** is optional. When set, it is unique in the metalake. It records where an
  imported term came from, in the form `<system>:<id>`, for example
  `datahub:urn:li:glossaryTerm:Customer`. A new import can then update the term instead of creating
  a copy. A term without a source reference was created in Gravitino.
- `MetadataObject.Type.GLOSSARY_TERM` is added for owners, tags, events and audit. Privileges
  **cannot be granted on a single term**: the role APIs reject them.
- Terms, relations and bindings are not cached. Every request reads the store, the same as for
  domains.

### Relations

- `BROADER` is stored on the child. A → B means "A is a kind of B". Narrower terms are not stored;
  the server finds them from the `BROADER` rows. `RELATED` is stored once per pair (with
  `from_term_id < to_term_id`) and shows on both terms.
- Setting the broader or related term of A only changes A. The caller needs the edit privilege on A
  and read access to the other term, including its draft visibility check (see Access control).
- **Cycle check**: adding A → B fails with 400 if B already reaches A through `BROADER` links. The
  server walks up from B, level by level, under the hierarchy lock (see Locking). A term cannot be
  its own parent.
- **Depth limit**: a `BROADER` chain has at most `L` links, where `L` is the release depth-limit
  constant in `GlossaryManager`. A new link that makes a chain longer fails with 400. This bounds
  the cycle check and the hierarchy read. 20 is the provisional value; T18 in Testing selects the
  final release value. Boundary tests use the loaded limit, not the provisional number.
- **No overlap**: two terms cannot be `RELATED` if one is an ancestor of the other. Adding such a
  `RELATED` link, or a `BROADER` link that creates such a pair, fails with 400. This follows the
  SKOS rule. For a new `RELATED` pair, check ancestry in both directions. Before adding A → B,
  collect A and all its descendants, and B and all its ancestors. Reject the link if any
  `RELATED` pair crosses these two sets, regardless of how that pair is stored. For example,
  X → A, B → Y and X `RELATED` Y make A → B invalid. Walking only upward from B misses X.
  Both walks and the pair check run under the hierarchy lock, with visited IDs deduplicated
  across each walk. An alter validates its complete proposed relation graph before committing.
- **No recursive SQL**: the walk runs in code, with one batched query per level. The backend still
  supports MySQL 5.7, which has no `WITH RECURSIVE`.
- A hierarchy read returns all ancestors or all descendants of a term. There are no cycles, so the
  walk always ends.
- Deprecating a term does not change its children. Deleting a term removes its links. Each child
  that loses a parent, and each surviving term that loses a `RELATED` link, gets one revision
  increment and its own audit record in the cleanup transaction (see Status and versions). The
  child keeps its other parents, or becomes a top-level term.

How other systems store term relations:

| System           | Where relations are stored                     | Parents per term | Cycle check         |
| ---------------- | ---------------------------------------------- | ---------------- | ------------------- |
| DataHub          | a list of outgoing links on the source term    | one term group   | not documented      |
| OpenMetadata     | one shared relation table for all entities     | one              | tree, so not needed |
| Apache Atlas     | edges in a graph database                      | many (`isA`)     | self-link only      |
| Gravitino Domain | `DomainRel { parentId, childId }` edge table   | one, one level   | not needed          |
| This design      | `glossary_term_rel` edge table, like DomainRel | many, any depth  | yes, under a lock   |

All of them store one direction and derive the other. The only new part here is many parents at
any depth. That is why this design needs the cycle check and the hierarchy lock.

### Domain membership

A term belongs to zero or more domains. In Step 1 the definition privileges are granted on the
metalake only, so a domain groups and filters terms but does not change who can see or manage them.
In Step 2, domains also become the scope of grants (see Implementation Steps). Then membership
decides who manages a term. **Decision: Option A, explicit relations.** Jerry selected it in the
[November 9 review](https://github.com/apache/gravitino/pull/14239#discussion_r4512415347), and
the author acknowledged the choice. Step 1 and Step 2 use this single membership source.
Backing tags (Option B) are retained under [Alternatives Considered](#alternatives-considered),
not as an implementation prerequisite.

**Selected design: explicit relation (Option A)**

- A table `glossary_term_domain_rel (term_id, domain_id)`. Only the term API changes it: `domains`
  on create, `addDomain` and `removeDomain`. These changes go through `TermChangeHandler`, like any
  term change.
- To add a term to a domain, the caller needs the edit privilege on the term, and must be able to
  see the domain (`USE_DOMAIN`). In Step 2, the caller also needs `CREATE_DEFINITION` on that
  domain, so nobody can move a term into a domain where they cannot write.
- **Terms of a domain** (`GET /terms?domain=`) use the same scope as the Domain design: terms in
  the domain, and terms in its subdomains that the caller can see.
- **Domains in a response**: every `TermDTO.domains` contains only domains the caller can see
  through the ordinary domain-read check (`USE_DOMAIN`). Apply this to direct gets, list and
  lookup results, nested terms, and create/alter responses. A readable term remains readable
  even if some or all of its domains are hidden; its returned domain list can be empty. Filtering
  a response never changes stored membership or reveals a hidden-domain count.
- Dropping a domain that still has terms fails with 409 `NonEmptyDomainException`. This is the same
  as a domain with subdomains. Removing the terms silently would change how they are grouped, and in
  Step 2 who manages them. The change is one more check in `DomainManager#dropDomain`.
- **Descriptive tags** on terms use the existing tag APIs and `APPLY_TAG`. A tag on a term never
  changes its domains, even when the tag backs a domain.

**Rationale for Option A.** Most reasons are about Step 2:

1. **In Step 2, the domain decides who can read and manage a term.** For an asset, being in a domain
   gives nobody any permission. That is why the Domain design lets anyone with `APPLY_TAG` add an
   asset. It is also why the Domain design does not allow access grants on domains. Terms are the
   opposite case: in Step 2, the grants on a term's domains decide who can see, edit and approve
   it. With Option B, adding a tag would change these permissions.
2. **Tag permissions go to other people.** In the Domain design, admins give `APPLY_TAG` on a
   domain's tag to the owners of the tables in that domain. With Option B in Step 2, any of these
   table owners could move a term into the domain. The domain admins would then control the term. Or
   the term would be hidden from a role that the domain denies. One tag cannot allow this for tables
   and forbid it for terms.
3. **The benefits of tags do not apply to terms.** The Domain design uses tags for three things:
   inheritance from catalogs and schemas, tag-based policies, and the existing tag checks. Terms
   are not under a catalog or schema. Terms have no policies. And in Step 2 terms need stricter
   checks than `APPLY_TAG`.
4. **Permissions do not change by accident.** With Option B, replacing or deleting a domain's tag
   silently removes its terms. In Step 2, who can see or edit them changes, but nobody edited a
   term. With
   Option A, membership changes only through the term API. It is checked and audited like any
   other term edit, and in Step 2 it is reviewed.
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
| **Used by**        | A semantic-model metric is built from the term | `METRIC`, identified by its existing `metric_id`                            | metric `net_revenue` uses `Net revenue` and `Order`                                            |

**Column scope in Step 1**: `COLUMN` means a table column. Resolve its identity as
`catalog.schema.table.column`, using the table and column IDs, and authorize through that table.
The endpoint fence is metalake -> catalog -> schema -> table, followed by the existing column
identity check. Table alteration fences column changes and cleans up removed-column bindings;
table/column renames preserve the IDs and bindings. A COLUMN lookup whose parent resolves only
as a view returns 404; do not reinterpret a view as a table. View-column bindings are outside
Step 1. Binding a view itself remains supported, but `children=true` on a view returns 400.

Both kinds are stored in `glossary_term_binding`, using the target's own stable ID:

- `target_type` identifies one of the eight Implemented by asset types, or `METRIC` for Used by.
  This is a binding-specific type; it does not add `METRIC` to `MetadataObject.Type`.
- `target_id` is the asset's ID, the stable column ID for `COLUMN` (not a version-row ID), or the
  existing `semantic_model_metric_meta.metric_id` for `METRIC`. It is never the containing
  semantic-model ID for a metric binding. The containing table or semantic model is resolved
  from the target identity for authorization, endpoint locking and cleanup.
- All columns of `(term_id, target_type, target_id, deleted_at)` are `NOT NULL`, with
  `deleted_at = 0` for a live row. This key prevents duplicate bindings on H2, MySQL and PostgreSQL
  without nullable fields, empty-string markers or metric names in the key.
- REST and client inputs still identify a metric by semantic model and exact metric name. The
  server resolves that input to a live metric ID before binding. Responses resolve the ID back
  to its current model/name; the existing `TermBindingDTO` and MCP argument shapes are unchanged.

A binding is direct only: binding a table does not bind its columns. `glossary_term_rel` stores
term-to-term relations; `glossary_term_binding` stores term-to-asset and term-to-metric bindings.

- There is no semantic view type in Gravitino. A semantic view from another system (for example
  Snowflake) is stored as a semantic model, so its metrics use Used by.

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
  - views: the existing `VIEW_OWNER_AUTHORIZATION_EXPRESSION` (owner of the view or of a parent);
  - metrics: `MODIFY_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION`;
  - other types have no modify privilege, so the caller must own the object or one of its parents
    (metalake, catalog, schema).
- The endpoint checks the object once, and then checks each term in the request body in code.
  A missing or hidden term returns 404 before checking its status. For a visible term, allow the
  term-editor path in any status, even when the caller is also an object admin. Otherwise, an
  object admin can use only `APPROVED` terms; another status returns 400. A caller satisfying
  neither authorization path gets 403. Failure on any term rejects the whole request and
  changes nothing. Removing a binding uses the same checks as adding it.
- Each changed term gets a new revision and goes through `TermChangeHandler`. So the Step 2 workflow
  can hold a binding change, like any other term change.
- Binding a missing object or metric fails with 404. Binding any other object type fails with 400.
  The status restriction on the object-admin path applies only after term visibility is checked
  and only when the term-editor path does not allow the operation.
- When a term is deprecated later, it keeps its bindings. It no longer shows in lookups or MCP
  results.

**Reads**

- `GET /terms/{term}/objects?kind=IMPLEMENTED_BY|USED_BY&types=` lists the bindings of a term. It
  first checks read access to the root term, then returns only objects and metrics the caller
  can access (`filterMetadataObject`). A table column requires read access to its table. A metric
  requires read access to its semantic model.
- `GET /objects/{type}/{fullName}/terms` and `GET /objects/semantic_model/{fullName}/metrics/{metric}/terms`
  first authorize reading the target using its ordinary metadata-read expression, then list only
  terms the caller can see. For COLUMN, authorize its table; for a metric, authorize its semantic
  model and verify the metric exists. Glossary VIEW or MANAGE never replaces target access.
  Insufficient target read access returns 403, and a missing target returns 404, with no terms,
  child names or counts in the error. Check target access before enumerating children or reading
  their bindings. Reuse `LOAD_TABLE_AUTHORIZATION_EXPRESSION` for tables and table columns,
  `LOAD_VIEW_AUTHORIZATION_EXPRESSION` for views, `LOAD_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION`
  for models' metrics, and the existing load expression for each other supported type.
- `?children=true` on a table or semantic model also returns the terms of each table column or
  metric that has readable terms. A schema tab or a metric table then loads in one call, not one
  call per column. On the server it is one query on `glossary_term_binding`, by the table and its
  column IDs. For a semantic model, resolve its live metric IDs in a batch and query bindings
  with `target_type = METRIC` and those IDs; do not query one metric at a time. Other types,
  including VIEW, reject `children=true` with 400. Views still support listing their own terms
  with no children.

**Cleanup**

- Dropping an object soft-deletes its bindings. This happens in the same places that clean up
  `tag_relation_meta` (by object, schema, catalog and metalake). Renames keep the bindings, because
  bindings use IDs.
- Dropping a column in `alterTable` soft-deletes the bindings of the column, in the same
  transaction.
- Metrics already have internal IDs. When a semantic model is altered, clean up bindings by
  `target_type = METRIC` and the retired metric IDs. Reuse the existing metric identity lifecycle:
  content edits and semantic-model renames preserve a metric ID; removing a metric retires its ID.
  A metric rename is currently removal of the old name and addition of the new name, with a new
  ID: its old bindings are removed and must be linked again. Reintroducing a removed name also
  gets a new ID and must not revive old bindings. The cleanup writes one audit record per
  affected term, listing its ID and name, the semantic-model ID and name, the retired metric IDs
  and names, the triggering alter operation and the resulting term revision. This also applies
  to metric removal without a rename; bindings are never silently transferred to a new identity.
- Every surviving term that loses bindings gets one revision increment in the same transaction
  as the object change and binding cleanup, even if it loses several bindings. Unaffected terms
  do not change. The transaction locks all affected term rows in ID order before deleting the
  bindings (see Locking). Failure rolls back the object change, bindings and revisions together,
  with no successful cleanup audit record.

### Status and versions

The following status table and immediate-update rules describe Step 1. Step 2 publication and
pending-content rules are specified separately below.

| Status               | Who can see it                                                   | Who can delete it    |
| -------------------- | ---------------------------------------------------------------- | -------------------- |
| `DRAFT`              | owner with `CREATE_DEFINITION`, or `MANAGE_DEFINITIONS`; not MCP | `MANAGE_DEFINITIONS` |
| `APPROVED` (default) | `VIEW_DEFINITION`; MCP also requires this privilege              | `MANAGE_DEFINITIONS` |
| `DEPRECATED`         | `VIEW_DEFINITION`; not MCP                                       | `MANAGE_DEFINITIONS` |

- **Status is an ordinary field in Step 1.** It is set in the create request (default `APPROVED`) or with
  `setStatus` in the update request. Anyone who can edit the term can set it: the owner with
  `CREATE_DEFINITION`, or `MANAGE_DEFINITIONS`. There are no transition rules and no separate
  status endpoint.
- **What `APPROVED` means in Step 1**: the term can be used by agents. It does not mean that a
  reviewer accepted it. Creation defaults to this status and makes the term immediately visible
  to callers with read access. Content edits to an `APPROVED` term also take effect immediately.
  An owner with CREATE can explicitly create a draft, or set any status on an existing own term;
  MANAGE can do the same for any term. Neither action is an approval decision or requires a
  reviewer in Step 1. Step 2 introduces review separately.
- **Delete** requires `MANAGE_DEFINITIONS` in every status, including `APPROVED`. Being the owner
  with CREATE never permits deletion, even after changing a term back to `DRAFT`. Deprecation
  before deletion is optional. MANAGE is an administration privilege in Step 1, not an approval
  privilege.
- **Why three states**: each term is used on its own. `DRAFT` keeps a term away from agents while
  it is written. Bindings and people keep using a term after it is replaced, so `DEPRECATED` keeps
  it readable, which a delete cannot do. In Step 2, `APPROVED` also records that a reviewer
  accepted the term. A domain or an ontology (#14310) is published as a whole and has no review per
  item, so `DRAFT` / `PUBLISHED` is enough there.
- **Every API applies the same term-read authorization** (REST, clients, MCP); MCP additionally
  excludes non-`APPROVED` terms. If the caller cannot see a term, lists leave it out and get
  returns 404. The same rule applies inside responses. The `broader`,
  `narrower` and `related` lists of a term, the terms of an object, and the terms of a domain all
  leave out terms the caller cannot see. So a hidden draft never shows up as a relation.
- **Known leaks**: hiding a term does not make it secret. A user can still find out that it exists
  in two ways: creating a term with the same name returns 409, and adding a synonym equal to its
  name returns 400.
- **Revision**: each change to the name, definition, synonyms, links, properties, relations,
  domains, bindings or status increases `current_version` by one, in the same transaction.
  Responses return it as `version`, so a client can see that a term changed after it read it.
  Owner and tag changes are only audited.
- **Cleanup revisions**: deleting a term, object, column or metric also increments each surviving
  term that loses a broader relation, a `RELATED` relation or a binding. Each affected term gets
  exactly one increment per cleanup transaction, regardless of the number of removed rows, and
  an audit record of the losses with its resulting revision. A change to a derived `narrower`
  list alone does not increment the parent, just as adding or removing a broader relation only
  changes the child. Terms deleted in the same transaction need no increment. No-op cleanup
  changes no revisions; failed cleanup commits neither losses nor revision increments. These
  are required cleanup effects, applied with the endpoint deletion rather than left pending for
  review in Step 2.
- **No snapshots in Step 1**: the audit log holds the change record. There is no history API.
  `glossary_term_version_info` is created with the other tables, but Step 1 writes no rows to it.
  Term reads and writes do not need it. A revision is not an approved version.
- **Workflow hook**: `GlossaryManager` sends every change to a `TermChangeHandler`: content, status,
  relations, domains, and both binding paths. The default handler applies the change at once. In
  Step 2, a review handler keeps the proposed change apart from the live tables until it is
  approved.
- **Step 2 publication**: new terms default to `DRAFT`; a create request specifying another
  initial status is rejected. A create request or ordinary update,
  including `setStatus(APPROVED)`, cannot publish a term without review. This applies to CREATE,
  MANAGE and the metalake owner; publication requires an explicit approval decision on a
  submitted proposal by another principal. A request that tries to publish directly is rejected,
  rather than silently treated as approved. No initial-status setting may bypass this rule.
- **Step 2 live and pending content**: an edit to an approved term creates proposed content;
  its existing approved version and bindings remain live for ordinary readers and agents until
  approval. Pending content is visible only to the current owner with effective CREATE, or to
  an authorized MANAGE reviewer. Being a former owner or the submitter does not independently
  grant continued read access. Proposal status is separate from the
  live term's status. Rejection leaves the approved version unchanged; approval replaces it.
- Dropping a term soft-deletes its synonyms, relations, domain links, bindings and version rows, in
  the same transaction. Dropping a metalake soft-deletes all its glossary rows in
  `MetalakeMetaService#deleteMetalake`, next to the tag cleanup.

### Access control

In Step 1, the three privileges are granted on the metalake:

| Action                                                    | `VIEW_DEFINITION` | `CREATE_DEFINITION` | `MANAGE_DEFINITIONS` |
| --------------------------------------------------------- | ----------------- | ------------------- | -------------------- |
| Read `APPROVED` and `DEPRECATED` terms; look up words     | ✓                 | ✓                   | ✓                    |
| Create a term (the creator becomes the owner)             |                   | ✓                   | ✓                    |
| See own `DRAFT` terms                                     |                   | ✓                   | ✓                    |
| See other users' `DRAFT` terms                            |                   |                     | ✓                    |
| Edit, bind and set the status of own terms, in any status |                   | ✓                   | ✓                    |
| Edit, bind and set the status of any term                 |                   |                     | ✓                    |
| Delete any term, in any status                            |                   |                     | ✓                    |

- `MANAGE_DEFINITIONS` includes `CREATE_DEFINITION`, and `CREATE_DEFINITION` includes
  `VIEW_DEFINITION`. The names are general, so other business-definition objects can use them too.
- **Owner versus any term**: `CREATE_DEFINITION` lets a user write and maintain their own terms.
  `MANAGE_DEFINITIONS` lets an admin read, edit, bind, set the status of and delete any term,
  including other users' drafts. It is not limited to list and delete. Ordinary VIEW callers
  can also list terms, subject to visibility; MANAGE additionally exposes all statuses in its
  authorized scope. Ownership means the current
  owner, including group members, not necessarily the original creator. Changing status is an
  ordinary edit in Step 1; CREATE and MANAGE have no separate approval capability. Step 2 adds
  submission and review, including reviewer authorization and no self-approval.
- **Read privilege on a term**: `VIEW_DEFINITION` for `APPROVED` and `DEPRECATED`; for `DRAFT`,
  `MANAGE_DEFINITIONS`, or being the owner with `CREATE_DEFINITION`. CREATE alone gives no access
  to another owner's draft. Apply this rule to direct gets, lists and every nested term result.
- **Edit privilege on a term**, used by Relations, Domain membership and Bindings:
  `MANAGE_DEFINITIONS`, or being the owner with `CREATE_DEFINITION`.
- **Delete privilege on a term**: `MANAGE_DEFINITIONS` only, independent of status or ownership.
- **Registration**: three new `Privilege.Name` values. In Step 1 their `canBindTo` accepts
  `METALAKE` only. The role APIs return 400 when these privileges are granted on any other object,
  or when anything is granted on `GLOSSARY_TERM`. Step 2 adds `DOMAIN`.
- **Deny** works as for other privileges: expand the allowed grant implications above, then
  apply explicit denies to each required capability. Every glossary operation requires effective
  VIEW, so denying VIEW blocks both reads and writes, even for a term owner. Creating requires
  effective CREATE; editing uses effective MANAGE or the owner-with-effective-CREATE path;
  deleting requires effective MANAGE. A deny on MANAGE removes the all-term path, but leaves
  effective CREATE usable on own terms. The metalake owner always passes these privilege checks;
  Step 2 workflow conditions still apply.
- **Bindings** are written on the object, by the object admin (`APPROVED` terms only) or by the
  term editor. See Bindings.
- **Owner** (a user or a group), set through `/owners/glossary_term/{term}`. When a group owns a
  term, all its members count as the owner. To change the owner, the caller must be the current
  owner, or have `MANAGE_GRANTS` on the metalake.
- **Create** needs `CREATE_DEFINITION` on the metalake, and `USE_DOMAIN` on each domain in the
  request. The creator becomes the owner.
- **Implementation**: add shared glossary authorization checks for effective VIEW, CREATE and
  MANAGE on the metalake, and for reading, editing and deleting a term using the rules above.
  REST endpoints and request-body checks use these same checks. Do not use
  `JcasbinAuthorizer#hasMetadataPrivilegePermission` as the term-edit check: it controls managing
  grants and ownership, not glossary content. Owner-transfer and role-grant APIs retain their
  existing authorization; MANAGE does not grant `MANAGE_GRANTS`. In Step 2, a `TermScopeResolver`
  adds the domains of a term, and their parents, as more levels of the glossary checks.

**Step 2: MANAGE retains administration and adds review**

The three privilege names and their implications are retained. CREATE remains the contributor
capability; MANAGE remains the administrator capability and additionally authorizes approval
and rejection. MANAGE does not become an approval-only privilege.

| Privilege            | Step 1                                                                | Step 2                                                                                           |
| -------------------- | --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| `VIEW_DEFINITION`    | Read visible published terms; no draft access                         | Read authorized APPROVED/DEPRECATED live terms; no draft or proposal access                      |
| `CREATE_DEFINITION`  | Create and maintain own terms; changes take effect immediately        | Create drafts, maintain own content and submit changes; cannot approve                           |
| `MANAGE_DEFINITIONS` | Read all statuses, create, edit, bind, set status and delete any term | Retain management, inspect pending changes, and approve or reject another principal's submission |

- **Scope**: Step 1 grants are metalake-wide. In Step 2, all-term administration and review
  mean all terms in the caller's effective authorized scope. A domain grant does not confer
  metalake-wide rights; S2.3 defines domain inheritance and deny handling. Glossary MANAGE still
  grants neither access to bound assets nor permission to administer role grants.
- **No workflow bypass**: ordinary edits, binding changes and requested deletion follow the
  Step 2 review workflow, even when submitted by MANAGE. Required cleanup after endpoint deletion
  remains the exception described in Cleanup revisions. Ownership, domain ownership and metalake
  ownership do not waive the workflow or the no-self-approval rule.
- **No self-approval**: the approval endpoint checks effective MANAGE for the proposal's scope
  and rejects approval by the authenticated principal that submitted it. Owner/group membership
  or an additional role does not turn that principal into a different reviewer. A MANAGE user's
  own submission needs another authorized MANAGE reviewer; an installation with only one such
  user cannot approve that user's submissions. Editing submitted content requires a new
  submission with its own recorded submitter and content revision; approval refers to that
  submitted revision, not mutable content attributed to an earlier submitter.
- **PRD alignment**: the enterprise prototype separates `MODIFY_GLOSSARY` and
  `APPROVE_GLOSSARY`; neither implies the other. This design deliberately combines all-term
  administration and review in MANAGE while keeping own-term contribution in CREATE. It does
  not offer an independent all-term editor without approval rights, or an approval-only role.
  Those roles would require a later privilege split rather than being claimed as supported by
  these three privileges.

### Locking

Before writing, plan the locks for the whole request, including every update in an alter and
every relation and domain in a create. Acquire their union using the existing store tools,
always in this order:

1. the live-endpoint fence (`LiveEndpointService.lockLiveEndpoint`, shared): the metalake, plus the
   chain of the bound object when binding;
2. the per-metalake hierarchy lock, if needed;
3. term rows, in ID order (exclusive);
4. domain rows (shared).

Each row is locked only once, in its final mode. Do not process updates one by one and acquire a
hierarchy lock after a term lock. Once locked, reread and validate the current state, apply all
updates in one transaction, and hold the locks until commit or rollback. If a reread shows that
the plan needs an earlier lock or an additional term row that would break ID order, roll back
and rebuild the plan before writing.

| Operation                                       | Locks                                                                                                               | Conflict it prevents                                    |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------- |
| Create, including initial relations and domains | metalake (shared) -> hierarchy lock -> referenced terms (exclusive, ID order) -> domains (shared)                   | name/synonym conflict; invalid relations; endpoint drop |
| Rename or change synonyms                       | metalake (shared) -> hierarchy lock -> term (exclusive)                                                             | name/synonym conflict; concurrent edits                 |
| Edit other content, status or domains           | metalake (shared) -> term (exclusive) -> added domains (shared)                                                     | concurrent edits; endpoint drop                         |
| Add or remove a relation                        | metalake (shared) -> hierarchy lock -> all referenced terms (exclusive, ID order) -> added domains (shared), if any | a cycle or overlap; endpoint drop; mixed alters         |
| Bind / unbind                                   | object chain (`lockLiveEndpoint`, shared) -> terms (exclusive, ID order)                                            | object drop; concurrent term edits                      |
| Delete a term                                   | metalake (shared) -> hierarchy lock -> deleted and affected surviving terms (exclusive, ID order)                   | new links; lost cleanup revisions                       |
| Object, column or metric cleanup                | endpoint locks in the deleting or altering operation's final mode -> affected surviving terms (exclusive, ID order) | new bindings; lost cleanup revisions                    |

For an alter that combines rows of this table, use the union of locks in the global order. For
example, rename plus `addBroader` plus `addDomain` takes the metalake fence, hierarchy lock, the
edited and referenced terms in ID order, then the added domain rows. Create with relations locks
the existing referenced terms before inserting the new term and its relations. Relation cleanup
finds the affected surviving terms after taking the hierarchy lock; binding cleanup finds them
after taking the endpoint deletion or alteration locks, which must exclude concurrent binding
writes. A metalake drop deletes all its terms, so there are no surviving terms to increment.

- The **hierarchy lock** is a `SELECT … FOR UPDATE` on one row per metalake in
  `glossary_hierarchy_lock`. The row is inserted on first use. Locking only the two terms is not
  enough. Example: A → B and C → D are added at the same time, then B → C and D → A. Each link
  passes its own check, but together they form a cycle. Every relation change and term delete
  takes this lock. Every create, rename and synonym change also takes it, so checking names in
  `glossary_term_meta` and synonyms in `glossary_term_synonym` cannot race. These writes run one
  at a time per metalake; other content edits do not take this lock.
- Name/synonym checks and hierarchy walks run after the lock is taken and read the latest
  committed state, including a transaction that committed while this request waited for the
  lock. They must not reuse an earlier transaction snapshot or a MyBatis cached result. If a term
  was deleted before the lock was taken, the server sees that when it locks the term row, and
  returns 404.
- For operations by name, the server finds the row by name, locks it, and then checks the name and
  `deleted_at` again. If they changed, it returns 404. The unique key on the name protects
  concurrent creates (409).

### REST API

All paths are under `/api/metalakes/{metalake}`. They follow the Tag, Policy and Domain APIs:

- a top-level `/terms` collection (there is no glossary container, so no `/glossary` level);
- `?details=` on lists;
- `PUT` with a list of updates to alter, including the status;
- `DropResponse` on delete;
- bindings are written on the object.

| Operation              | Endpoint                                                                         | Request -> response                                                      |
| ---------------------- | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| List, filter, match    | `GET /terms?details=&domain=&status=&nameContains=&match=&bindings=`             | -> `NameListResponse` / `TermListResponse`                               |
| Create                 | `POST /terms`                                                                    | `TermCreateRequest` -> `TermResponse`                                    |
| Get                    | `GET /terms/{term}?status=`                                                      | -> `TermResponse`                                                        |
| Alter                  | `PUT /terms/{term}`                                                              | `TermUpdatesRequest` -> `TermResponse`                                   |
| Drop                   | `DELETE /terms/{term}`                                                           | -> `DropResponse`                                                        |
| Hierarchy              | `GET /terms/{term}/hierarchy?direction=BROADER\|NARROWER&status=`                | -> `TermTreeResponse`                                                    |
| Bindings of a term     | `GET /terms/{term}/objects?kind=&types=&status=`                                 | -> `TermBindingListResponse`                                             |
| Terms of an object     | `GET /objects/{type}/{fullName}/terms?details=&children=&status=`                | -> `NameListResponse` / `ObjectTermsResponse`                            |
| Bind / unbind          | `POST /objects/{type}/{fullName}/terms`                                          | `TermsAssociateRequest{termsToAdd, termsToRemove}` -> `NameListResponse` |
| Terms of a metric      | `GET /objects/semantic_model/{fullName}/metrics/{metric}/terms?details=&status=` | -> `NameListResponse` / `TermListResponse`                               |
| Bind / unbind a metric | `POST /objects/semantic_model/{fullName}/metrics/{metric}/terms`                 | `TermsAssociateRequest` -> `NameListResponse`                            |
| Owner                  | existing `/owners/glossary_term/{term}`                                          |                                                                          |
| Tags                   | existing `/objects/glossary_term/{term}/tags`                                    |                                                                          |

```
TermCreateRequest     { name, definition, synonyms, status?, domains, broader, related, links,
                        sourceRef, properties }
TermUpdatesRequest    { updates: [rename, updateDefinition, setStatus, addSynonym, removeSynonym,
                        addDomain, removeDomain, addBroader, removeBroader, addRelated,
                        removeRelated, updateLinks, setSourceRef, setProperty, removeProperty] }
TermDTO               { name, definition, synonyms, status, domains, broader, narrower, related,
                        links, owner, sourceRef, properties, version, audit,
                        matchedBy?: [{word, field: NAME | SYNONYM}],
                        bindings?: [TermBindingDTO], bindingsTruncated? }
TermBindingDTO        { kind: IMPLEMENTED_BY | USED_BY, object: MetadataObjectDTO, metric? }
ObjectTermsResponse   { terms: [TermDTO], children: [{ name, terms: [TermDTO] }] }
TermsAssociateRequest { termsToAdd: [name], termsToRemove: [name] }
```

- **Status filtering on reads**: `status` is optional on the term collection, get, hierarchy,
  term-binding and reverse object/metric GET endpoints. With no filter, return any term the
  caller is allowed to read. With a filter, apply both
  authorization and that status to all terms: roots, list items, `broader`, `narrower`, `related`,
  hierarchy nodes and terms inside object/metric responses. A named root that is hidden, missing
  or in another status returns 404, including on `/terms/{term}/objects`, even though that
  response contains only bindings. A list omits nonmatching terms; a hierarchy stops at each
  nonmatching or hidden node and does not traverse through it. An invalid status returns 400.
  Read root status and response data from one consistent database snapshot, using one query or
  a read transaction for multiple queries. Do not check status in a separate request and then
  fetch unguarded bindings or relations. This requires no write locks; each response reflects
  its read snapshot, and later requests can observe subsequent status changes.
- **Domain filtering on responses**: every occurrence of a term uses the domain visibility rule
  in Domain membership, including terms embedded by hierarchy, object and metric endpoints.
- **List** is used both to browse and to look up words:
  - `domain` returns the terms of the domain and of the subdomains the caller can see.
  - `nameContains` finds names that contain the text, ignoring case. It is for search boxes.
  - `match` looks up business words: one `match` per word, up to 20. Each word is looked up on its
    own, and a term is returned when it matches any word (OR). The server compares `key(word)`
    with `name_key` and `synonym_key`. Each returned term has `matchedBy`. With
    `status=APPROVED`, a word that only matches `DEPRECATED` terms returns nothing.
  - `bindings=true` adds at most 20 authorized bindings per term. Apply target-read authorization
    before the limit: collect up to 21 visible bindings, return the first 20, and set
    `bindingsTruncated` only when a 21st visible binding exists. Hidden bindings do not consume
    the limit or affect the flag. A query may fetch candidates in batches, but must continue
    through hidden rows until it finds 21 visible bindings or exhausts the candidates. It only
    works together with `match` and `details=true`. A lookup then needs only one REST call. A word
    usually matches one to three terms.
  - Why not a separate path like `/terms/resolve`? It would clash with a term named `resolve`. So
    lookup is a filter of the list.
- **Alter** applies all updates in one transaction and increases the revision once. `narrower` is
  computed and cannot be set. `version` is the current revision. `updateLinks` replaces the whole
  link list. `status` defaults to `APPROVED` on create.
- **Terms of an object**: `children=true` works on `table` and `semantic_model` after target-read
  authorization. It adds one entry to `children` for each table column or metric with terms
  passing the authorization and optional status filter. A semantic model itself has no terms;
  only its metrics do. A view supports its own terms, without `children=true`.
- **Bind / unbind** use the same request format as tag association. The response is the object's
  terms after the change.
- **Errors**:
  - 404: `NoSuchTermException` (missing, hidden, or a named root excluded by the status filter),
    `NoSuchMetadataObjectException`;
  - 409: `TermAlreadyExistsException` (same name or same source reference), `NonEmptyDomainException`;
  - 400: a cycle, a term as its own parent, binding an unsupported object type, a synonym equal to
    another term's name, an object admin binding a visible non-`APPROVED` term without the
    term-editor path, invalid `status`, `children=true` on an unsupported type, or more than 20
    `match` values;
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
    `createTerm`, `alterTerm` (also sets the status), `dropTerm`, and `matchTerms(words, domain)`.
    `matchTerms` returns approved terms with their bindings. It is for agents that do not use MCP.
  - On a `Term`: `broader()`, `narrower()`, `listBindings(kind)`.
  - `SupportsTerms` (`listTerms()`, `associateTerms(add, remove)`) on catalogs, schemas, tables,
    views, table columns, filesets, topics and models. Tables also have `listColumnTerms()`;
    views do not support column-term operations in Step 1.
    `SemanticModel` has `listMetricTerms` and `associateMetricTerms`.

### Web UI support

The table shows the REST calls behind each part of the glossary page, and of the other pages that
show terms. A term comes with its name, domains, status, version, owner, relations and links. So
the term page does not need a call per field.

| Page                         | Part                                                                            | Calls                                                                                                     |
| ---------------------------- | ------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| Glossary                     | Term tree (domain › subdomain › term, narrower terms nested, No visible domain) | `GET /terms?details=true` and `GET /domains?details=true`; the tree is built from `domains` and `broader` |
| Glossary                     | Search terms                                                                    | `GET /terms?nameContains=`                                                                                |
| Glossary, term               | Header, Definition, Details, Related terms                                      | `GET /terms/{term}`; the source label is the `sourceRef` system, or "Gravitino" if empty                  |
| Glossary, term               | Implemented by, Used by                                                         | `GET /terms/{term}/objects?kind=IMPLEMENTED_BY`, `?kind=USED_BY`                                          |
| Glossary, term               | Set status (immediate in Step 1; no approval decision)                          | `PUT /terms/{term}` with `setStatus`                                                                      |
| Glossary, term               | Delete term (the dialog counts relations and asset links)                       | `DELETE /terms/{term}`; the counts come from the term and its bindings                                    |
| Glossary, Add or Edit term   | Name, Definition, Synonyms, Domain, Broader, Related, Documentation             | `POST /terms` or `PUT /terms/{term}` with the changed fields; pickers use `/domains` and `/terms`         |
| Glossary, Add term           | Owner other than the creator                                                    | `POST /terms`, then `PUT /owners/glossary_term/{term}` (allowed, because the creator is the owner)        |
| Explore, table               | Terms N, Update terms drawer, column chips and the term filter                  | `GET /objects/table/{fullName}/terms?details=true&children=true`; `POST .../terms` per change             |
| Explore, view                | Terms N, Update terms drawer and the term filter; no view-column chips          | `GET /objects/view/{fullName}/terms?details=true`; `POST .../terms` per change                            |
| Explore, Update terms drawer | Term picker (approved, grouped by domain)                                       | `GET /terms?details=true&status=APPROVED&nameContains=`                                                   |
| Semantic model               | Glossary terms of each metric                                                   | `GET /objects/semantic_model/{fullName}/terms?details=true&children=true`                                 |
| Metric editor                | Terms (approved only)                                                           | the picker call above; `POST /objects/semantic_model/{fullName}/metrics/{metric}/terms`                   |
| Domain, Definitions tab      | Terms of the domain and its subdomains, name search                             | `GET /terms?details=true&domain=&nameContains=`                                                           |
| Define, Overview             | Terms by domain, terms by status                                                | counted from `GET /terms?details=true`                                                                    |

This design does not cover the parts below. They need another source, or the page hides them:

- **Step 2 parts**: Submit for review, Reject, In review, the version diff and History, and the
  domain create options "Readable by all users" and "Glossary editors", which grant on a domain.
- **Counts that need an aggregate query**: terms without bindings, and assets without a glossary
  term. Also the "N restricted terms" count on an object, because it would show that hidden terms
  exist.
- **Undefined lookups**, **Import**, **KB articles** and the **managing group** label.
- **Buttons by privilege**: Gravitino has no API that tells which actions a caller can take on an
  object. So the page cannot disable a button in advance. It shows the 403 message instead.

### Agent integration

The `mcp-server/` gets a read-only `glossary` tool tag, added to `SUPPORTED_TOOL_TAGS`. All glossary
tools use `GravitinoContext.rest_client()` and its existing identity selection:

| Mode                                                                                | Identity used for the Gravitino request                                                                                             |
| ----------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| HTTP with a non-empty `Authorization` header                                        | Forward that header verbatim, ahead of any configured service credential. Authentication failure does not trigger service fallback. |
| HTTP without a header, with a service identity and fallback enabled                 | Use the configured static `--token`, or OAuth client-credentials when no token is set.                                              |
| HTTP without a header, with a service identity and `--no-service-identity-fallback` | Reject the tool call with `ServiceIdentityFallbackDisabled` before issuing a Gravitino request.                                     |
| HTTP without a header and without a service identity                                | Send no authorization credential; Gravitino applies its anonymous/unauthenticated behavior.                                         |
| stdio                                                                               | Use the configured service identity, or send no credential if none is configured. The HTTP-only fallback flag does not apply.       |

Authorization applies to that effective principal, with an additional APPROVED-only restriction
for glossary tools. User credentials therefore limit results to the user's authorized approved
terms; service-identity modes limit results to the service principal's authorized approved terms.
Do not claim end-user impersonation in service or unauthenticated modes. Reuse the existing audit
attribution and client isolation by identity and metalake.

**End-to-end flow**

Example question: "net revenue from VIP active users last month".

```
User question
   │
   ▼
1 Agent (LLM) picks the business words ──▶ ["net revenue", "VIP", "active user"]
   │
   ▼
2 resolve_business_term(words)                                  (MCP tool)
   │
   ▼
3 GET /terms?match=net%20revenue&match=VIP&match=active%20user
            &status=APPROVED&details=true&bindings=true          (Gravitino server)
   │
   ▼
4 Result per word ──▶ "net revenue": term Net revenue,    metric dw.sales.sales_model/net_revenue
   │                  "active user": term Active Customer, column dw.sales.customers.is_active,
   │                                 metric dw.sales.sales_model/active_customers
   │                  unmatched:     ["VIP"]
   ▼
5 Agent decides per word
   ├── column or table ──▶ get_table_metadata_details ──▶ write SQL
   ├── metric ───────────▶ load_semantic_model ─────────▶ metric query
   ├── other object ─────▶ the tool for that object type (for example a fileset or a model)
   └── unmatched ────────▶ ask the user: "What do you mean by VIP?"

6 Agent reads data through tools with their own access checks and answers
```

| Step                    | Who                     | What happens                                                                                                                                                                                                                                                                                            |
| ----------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1. Pick business words  | Agent (LLM)             | Gravitino does not parse the question; the agent does. The tool description tells it how: keep a phrase whole ("active user", not "active" and "user"); use the singular form; leave out time ranges, numbers and query words ("last month", "total", "top 10"). A question usually gives 1 to 5 words. |
| 2. Call the tool        | Agent -> MCP server     | One call with all the words. Add `domain` when the user or the context names one.                                                                                                                                                                                                                       |
| 3. Look up              | MCP server -> Gravitino | One REST call, with one `match` per word. The server compares `key(word)` with term names and synonyms, and keeps only `APPROVED` terms the caller can see (and in `domain`, if given). It adds the bindings of each term.                                                                              |
| 4. Group                | MCP server              | Groups the terms by the word they matched (`matchedBy`), and lists the words with no match in `unmatched`.                                                                                                                                                                                              |
| 5. Decide               | Agent                   | One term for a word: use it. Several terms: pick one by domain, or ask the user. No term: ask the user, and do not guess a column. Then follow the bindings with the existing tools.                                                                                                                    |
| 6. Read data and answer | Agent -> existing tools | Data-access tools authenticate and authorize their own effective principals. Glossary bindings grant no additional access. The answer names the terms and definitions it used.                                                                                                                          |

**How `match` works**

- **One `match` per word.** One word gives one `match`, three words give three. The number is not
  fixed.
- **OR, not AND.** A term is returned when it matches any of the words. Each word is looked up on
  its own. The server never combines words: "VIP" and "active user" are two lookups, and combining
  their meanings in the query is the agent's job.
- **`matchedBy`** says which word matched which field (`NAME` or `SYNONYM`). So the MCP tool can
  put each term under the right word. If one term matches two words, it is returned once, with two
  `matchedBy` entries.
- **At most 20 words per call.** If the agent passes more than 20 words, the MCP tool splits them
  into calls of 20 and merges the results, so the agent never sees the limit. A direct REST caller
  gets 400 and must split the words itself.
- **Exact match only** (ignoring case and extra spaces). "customers" does not find `Customer`.
  That is why step 1 uses the singular form, and why admins write names and synonyms in the
  singular form.

**`resolve_business_term`**

- Input: `words` (the business phrases from step 1), and an optional `domain`.
- Output for the example above:

```json
{
  "results": [
    {
      "word": "net revenue",
      "matches": [
        {
          "term": "Net revenue",
          "definition": "Revenue after refunds and discounts.",
          "matched_by": "NAME",
          "domains": ["Sales"],
          "broader": ["Revenue"],
          "related": ["Order"],
          "bindings": [
            {"kind": "USED_BY", "type": "SEMANTIC_MODEL", "catalog": "dw", "schema": "sales",
             "name": "sales_model", "metric": "net_revenue"}
          ],
          "bindings_truncated": false
        }
      ]
    },
    {
      "word": "active user",
      "matches": [
        {
          "term": "Active Customer",
          "definition": "A customer with at least one order in the last 90 days.",
          "matched_by": "SYNONYM",
          "domains": ["Sales"],
          "broader": ["Customer"],
          "related": [],
          "bindings": [
            {"kind": "IMPLEMENTED_BY", "type": "COLUMN", "catalog": "dw", "schema": "sales",
             "name": "customers", "column": "is_active"},
            {"kind": "USED_BY", "type": "SEMANTIC_MODEL", "catalog": "dw", "schema": "sales",
             "name": "sales_model", "metric": "active_customers"}
          ],
          "bindings_truncated": false
        }
      ]
    }
  ],
  "unmatched": ["VIP"]
}
```

- The binding fields are the same arguments that `get_table_metadata_details` and
  `load_semantic_model` take. The agent can make the next call directly, with no parsing and no
  extra lookup.
- If `bindings_truncated` is true, the agent calls `list_metadata_by_term` for the full list.

**Rules in the tool descriptions**, so all agents use the result the same way:

1. Pass each business phrase whole and in the singular form. Leave out time ranges, numbers and
   query words.
2. Only `APPROVED` terms are returned. A word in `unmatched` has no agreed meaning. Tell the user,
   and do not guess a column.
3. If a word matches several terms, use the domains to pick one, or ask the user.
4. A binding shows where a meaning is stored. It never gives access. Reading the data still needs
   the normal privileges.

| Tool                      | REST calls                                                                                                                | Use                                                              |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| `resolve_business_term`   | `GET /terms?match=&status=APPROVED&details=true&bindings=true`                                                            | Map the words of a question to terms and their data              |
| `get_term`                | `GET /terms/{term}?status=APPROVED`                                                                                       | Definition, synonyms, authorized domains and approved relations  |
| `list_terms`              | `GET /terms?details=true&status=APPROVED` (+ `domain`, `name_contains`)                                                   | Browse the terms of a domain                                     |
| `get_term_hierarchy`      | `GET /terms/{term}/hierarchy?status=APPROVED`                                                                             | Find approved wider or narrower terms                            |
| `list_metadata_by_term`   | `GET /terms/{term}/objects?status=APPROVED`                                                                               | All authorized bindings of an approved term                      |
| `list_terms_for_metadata` | `GET /objects/{type}/{fullName}/terms?details=true&status=APPROVED`; add `children=true` only for table or semantic model | Explain an authorized object, its table columns or model metrics |

Every tool passes `status=APPROVED` to the server. The server applies the read contract in REST
API to the root and every nested term, including name-only relations. A direct draft or deprecated
name passed to `get_term`, `get_term_hierarchy` or `list_metadata_by_term` returns 404 with no term
details or bindings, even for MANAGE. Hierarchy traversal stops at an unapproved or unauthorized
node. The bindings endpoint checks root status in the same read snapshot as its payload, so a
separate preflight get followed by an unfiltered bindings request is not sufficient. Object
results enforce target-read authorization, omit filtered terms and empty children, and never
return hidden domains. The MCP server must not retry a rejected filtered request without the
status filter. With the `domain` tools of #13867, an agent can go from a domain to its terms, and
then to the data.

### Events and audit

- Every operation sends pre, success and failure events through a `GlossaryEventDispatcher`, and
  is written to the audit log: create, alter, status, bind and unbind, drop. When a term is
  dropped, each surviving term that loses a parent or a `RELATED` link also gets its own audit
  entry with its new revision. Object, column and metric cleanup similarly records lost bindings
  per surviving term. A metric rename records the old metric names and affected terms as specified
  in Bindings. Successful cleanup records are committed with the cleanup; rollback must not leave
  a record claiming that bindings or relations were removed.

## Alternatives Considered

1. **A glossary container** (`metalake.glossary.term`). It adds a second way to group terms, next
   to domains. Then it is unclear which one controls privileges. Domains plus the term hierarchy
   are enough.
2. **Domain membership through backing tags (Option B, not selected)**: see the alternative below.
3. **Terms as tags on assets.** `APPLY_TAG` is a different privilege from the definition
   privileges. Tags have no definition, relations or status. Tags can carry policies, but a binding
   must never change access.

### Domain membership through backing tags (Option B, not selected)

How the Domain design puts a table into a domain: each domain has a tag, called its backing tag.
For example, domain Finance has tag `finance`. A table with tag `finance` is in Finance. There is
no member table: the tag alone decides it.

Option B uses the same rule for terms. Term `Revenue` gets tag `finance`, so it is in Finance.
Terms then also show in `GET /domains/{domain}/objects`. There is no relation table.

In Step 1 this is safe, because grants are on the metalake. The problem comes in Step 2: tags are
added with `APPLY_TAG`, and the Domain design gives `APPLY_TAG` to many table owners, so they can
put their tables into a domain. For a table this is safe, because the domain does not change who
can read the table. For a term in Step 2 it is not safe, because **the domain decides who can see,
edit and approve the term**:

| What happens                                                                            | Result                                                                              |
| --------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| A table owner with `APPLY_TAG` on `finance` adds `finance` to the Sales term `Customer` | The Finance admins now also manage `Customer`. Nobody in Sales agreed               |
| Someone deletes tag `finance`, or gives Finance another backing tag                     | All Finance terms leave Finance, and who manages them changes. Nobody edited a term |

To block the first case, the tag code needs special rules only for terms: besides `APPLY_TAG`, the
caller also needs the edit privilege on the term and `CREATE_DEFINITION` on the domain behind the
tag. The second case cannot be blocked without changing how tags work.

## Prior Art

- **DataHub**: a glossary term has a definition, owners and a source reference. Terms are grouped
  in nested term groups. They have typed relations (Inherits From, Contains), and are attached to
  datasets and columns. In this design, term groups become domains and parent terms. Contains
  becomes `RELATED`, and Inherits From becomes `BROADER`.
- **Unity Catalog**: a page defines a term inside a domain. It has draft and published states, and
  related assets. Access comes from the domain; nobody grants access on a single page. This design
  never grants on a single term either. Grants are on the metalake in Step 1, and also on domains
  in Step 2. Unlike a page, a term can have no domain: a term used by several areas does not have
  to pick one.

## Open Questions and Decisions

1. **Domain membership (decided)**: use explicit `glossary_term_domain_rel` relations (Option A),
   as agreed in the November 9 review. Option B is an alternative, not a pending choice or a
   prerequisite for T7.
2. **Draft visibility (decided)**: only the owner with `CREATE_DEFINITION`, or a caller with
   `MANAGE_DEFINITIONS`, can read a draft. This is fixed for Step 1 (see Access control).
3. **Default read access**: everyone should be able to read terms by default. But Gravitino has no
   built-in role for all users. Should we add one (for example `PUBLIC`) with `VIEW_DEFINITION` on
   the metalake, or should admins grant it?
4. **Two-step governance (decided)**: Step 1 defaults to APPROVED with immediate changes and no
   review. Step 2 defaults to DRAFT and adds approval/rejection to MANAGE's existing administration
   rights, with no self-approval or direct-publication bypass. Combining administration and review
   is a deliberate simplification of the PRD's independent modify and approve privileges.

## Testing

AI agents will write most of the Step 1 code and tests, and few people will read the tests. So
this chapter is the test spec. It says where each test goes, how to run it, and what it must check.

- A task is done when every case listed for it has a test, all of them pass, and the rules below
  hold.
- If the code and this chapter disagree, fix the code, or change this chapter in the same PR and
  say why in the PR description. Never change a test only to make it match the code.
- Each case has an ID, like `T5.4`: the task number and the case number. Case IDs do not change.
  New cases get new numbers.

### Rules for every test

1. **Trace each case.** The Javadoc (or docstring) of a test names the cases it covers, for
   example `Covers T5.4, T5.5.` Each case ID of the task must appear in the test code. The PR
   checks this with `grep`.
2. **Check the exact failure.** A negative test checks the exception class, or the HTTP code and
   the error type. "Something failed" is not enough.
3. **Check that nothing changed.** After a failed write, read the term again. Its fields, `version`,
   relations, domains and bindings must be the same as before. No success event was sent.
4. **Check the invariants.** Every database test of the glossary calls
   `GlossaryInvariants.assertHolds(metalake)` in `@AfterEach` (see Invariants).
5. **Prove that guards work.** Some cases are marked **(guard)**. For each one, the author removes
   the check from the code, runs the test, sees it fail, and puts the check back. The PR description
   lists each guard, the check that was removed and the test that failed.
6. **No flaky tricks.** No `@Disabled`, no retries, and no `Thread.sleep` to order threads. Use
   `CountDownLatch` or `CyclicBarrier`. Wrap every concurrent test in `assertTimeoutPreemptively`,
   so a hang fails the test instead of the build. Run each race at least 20 rounds.
7. **Keep old tests.** Do not change an existing test to make new code pass. If one must change,
   the PR description says why.
8. **Fixed data.** Use fixed names and values. A test must not depend on the time zone, the
   locale or the test order. Tests of `key()` also run with the default locale set to Turkish
   (`tr-TR`), where `"I".toLowerCase()` is not `"i"`.
9. **All three databases.** Database tests extend `TestJDBCBackend` (`@CoreBackend.All`) and pass
   on H2, MySQL and PostgreSQL. Integration tests pass with `-PjdbcBackend=h2`, `mysql` and
   `postgresql`.
10. **Show the results.** The PR description lists each command it ran and, for each backend, the
    number of tests run and passed, from the JUnit XML files in `build/test-results/`. A class that
    ran 0 tests counts as a failure.
11. **Run only what changed.** Use `--tests` filters. Never run a whole module suite such as
    `:core:test`.

### Invariants

`GlossaryInvariants` is a test helper in `core/src/test`. T2 adds it, and T3, T7 and T11 extend it.
For one metalake, it reads the glossary tables directly and checks the live rows
(`deleted_at = 0`):

1. Every `name_key` equals `key(name)`, and every `synonym_key` equals `key(synonym)`. No two terms
   have the same `name_key`.
2. No synonym equals the `name_key` of another term.
3. No synonym, relation, domain link or binding points to a deleted or missing term, domain or
   object.
4. The `BROADER` links have no cycle. The longest chain has at most the depth limit of links.
5. No `RELATED` pair has one term as an ancestor of the other. Each `RELATED` row has
   `from_term_id < to_term_id`.
6. Every binding has non-NULL `term_id`, `target_type` and `target_id`, and resolves to a live
   target of that type. `METRIC` uses the metric's own ID, not the containing model's ID. No two
   live bindings have the same `(term_id, target_type, target_id)`.
7. `glossary_term_version_info` has no rows.
8. `glossary_hierarchy_lock` has at most one row for the metalake.

Integration tests cannot read the tables. They use the REST form of the helper. It reads all terms
and their hierarchies as the metalake owner, and checks rules 2, 4 and 5.

### Rules the tests fix

The design does not say these things. The tests use the answers below. If a reviewer wants a
different answer, change it here first.

1. **Order of checks**: if the caller cannot see the term, 404. If the caller can see it but has
   no privilege for the action, 403. For example, CREATE dropping its own term gets 403 in every
   status, while MANAGE can delete it in every status. If the caller has the privilege but the
   change breaks a rule, 400 or 409. Binding uses the two-path checks in Bindings: a hidden draft
   gets 404; a visible `DEPRECATED` term gets 400 through the object-admin-only path, but a term
   editor with object read access can bind it.
2. **Revision**: a new term has `version` 1. Each successful alter call adds exactly 1, even when
   it has several updates. A bind or unbind call adds 1 to each term whose bindings changed. Owner
   and tag changes add nothing. A cleanup transaction adds 1 to each surviving term that loses
   bindings, broader relations or `RELATED` relations, once per term rather than per removed
   row. Derived `narrower` changes alone add nothing. No-op and failed cleanup add nothing.
3. **Repeats are ignored**: adding a synonym, link, domain or binding that already exists, or
   removing one that does not exist, is not an error and writes nothing. It still counts as a
   successful call for the revision of an alter.
4. **Depth** is the number of `BROADER` links on the longest chain. With loaded limit `L`, a
   chain of `L + 1` terms has `L` links and is allowed; `L + 1` links exceed the limit. A link
   joining two chains counts the links above and below it.
5. **Hidden terms in a hierarchy**: the hierarchy read stops at a term that the caller cannot see.
   It shows neither that term nor the terms behind it.
6. **Names**: the server trims spaces at both ends before it stores a name or a synonym. The case
   is kept. An empty name, or one longer than 128 characters, fails with 400. A synonym equal to
   the name of its own term fails with 400.
7. **`key()`** treats every whitespace character (space, tab, new line) as a space.
8. **`nameContains`** treats `%`, `_` and `\` as plain characters.
9. **`bindings=true`** without `match` and `details=true` fails with 400. Its 20-binding limit
   and `bindingsTruncated` count authorized bindings only, after filtering target access.
10. **The depth limit** is a constant in `GlossaryManager`. Manager tests can inject a test limit
    and run boundary cases at 10, 20 and 50. CUJ 6 reads the actual release limit without overriding
    it. Users cannot configure the limit.
11. **Read status filters** apply to roots and nested terms, and stop hierarchy traversal at a
    nonmatching node. A named nonmatching root returns 404, including on the bindings endpoint.
12. **Column scope** is table columns only. Views have their own term bindings, but no column
    operations or `children=true` in Step 1.

### Where tests go and how to run them

All Gradle commands below take a `--tests` filter for the new classes. Database and Docker runs
also need `-PskipDockerTests=false` and a running Docker.

| Layer                | Test classes                                                                     | Command                                                                  |
| -------------------- | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| Storage              | `core/.../storage/relational/service/TestGlossaryTerm*MetaService`               | `./gradlew :core:coreH2Test`, then `coreMySQLTest`, `corePostgreSQLTest` |
| Schema and upgrade   | the existing `core/.../storage/TestSQLScripts`                                   | the same three tasks                                                     |
| Manager              | `core/.../glossary/TestGlossaryManager*`                                         | the same three tasks                                                     |
| Authorization rules  | `server-common/.../authorization/jcasbin/TestJcasbinAuthorizer`                  | `./gradlew :server-common:test`                                          |
| REST                 | `server/.../web/rest/TestGlossaryOperations`, `TestMetadataObjectTermOperations` | `./gradlew :server:test`                                                 |
| Java client          | `clients/client-java/.../client/TestGlossary*` (mock server)                     | `./gradlew :clients:client-java:test -PskipITs`                          |
| Python client        | `clients/client-python/tests/unittests/test_glossary*.py`                        | `./gradlew :clients:client-python:unitTests`                             |
| MCP server           | `mcp-server/tests/unit/tools/test_glossary.py`                                   | `./gradlew :mcp-server:testPython`                                       |
| Integration and CUJs | `clients/client-java/.../integration/test/Glossary*IT`                           | `./gradlew :clients:client-java:test -PskipTests -PjdbcBackend=<db>`     |
| Performance          | `core/.../glossary/GlossaryHierarchyPerformanceTest`                             | run by hand; not part of CI                                              |

### Cases per task

**T1. API**

- T1.1 Each new enum, privilege and type has a `fromString` / name round trip.
- T1.2 `MetadataObject.Type.GLOSSARY_TERM` parses a full name that contains `/`.
- T1.3 The three new privileges accept `METALAKE` in `canBindTo` and reject every other type.
  **(guard)**
- T1.4 The existing tests of `Privilege` and `MetadataObject` still pass without change.

**T2. Term storage**

- T2.1 Insert, get by name, list and update a term with every field set. All fields read back the
  same, including links, properties and the source reference.
- T2.2 `Churn` exists. Insert ` churn ` and `CHURN`: both fail as a duplicate. The stored name is
  still `Churn`. **(guard)**
- T2.3 `Active   Customer` (three spaces), `active customer` and `Active\tCustomer` have the same
  key.
- T2.4 With the default locale `tr-TR`, `key("TITLE")` is `title`.
- T2.5 `Cafe` and `Café` are two different terms, on all three databases. (This checks the
  collation of the new tables.)
- T2.6 Names with `/`, Chinese characters and emoji read back the same on all three databases.
- T2.7 A name of 128 characters works on all three databases.
- T2.8 Two terms can have no source reference. Two terms with the same source reference fail as a
  duplicate.
- T2.9 Two terms can share a synonym. Each synonym is one row.
- T2.10 Soft-delete a term, then create a term with the same name. The new term has a new ID, and
  the old rows stay soft-deleted.
- T2.11 Drop the metalake. All glossary rows of the metalake are soft-deleted. The terms of a second
  metalake are not touched.
- T2.12 Garbage collection removes soft-deleted glossary rows after the retention time.
- T2.13 `TestSQLScripts`: the 2.0.0 schema and the result of the `upgrade-1.3.0-to-2.0.0` scripts
  have the same glossary tables, columns and keys. Running the MySQL upgrade script twice works.

**T3. Relation storage**

- T3.1 Insert and remove `BROADER` and `RELATED` links. List the broader, narrower and related
  terms of a term.
- T3.2 `RELATED` from A to B and from B to A is one row, with `from_term_id < to_term_id`. Adding
  the second one is a duplicate.
- T3.3 Diamond: A → B, A → C, B → D, C → D. Walking up from A returns B, C and D, each once.
- T3.4 The walk sends one query per level. For a chain of 5 levels, it sends 5 queries. (Count the
  calls on the mapper.) **(guard: a version with one query per term fails this test.)**
- T3.5 The first use of the hierarchy lock inserts its row. A second use does not insert another
  row. Two threads using it for the first time at the same time both succeed.

**T4. Manager**

- T4.1 Create with only a name and a definition. Status is `APPROVED`, `version` is 1, and the
  owner is the caller.
- T4.2 Create with status `DRAFT` and with `DEPRECATED`. The status is stored as given.
- T4.3 Each update type changes only its field and adds 1 to `version`. An alter with three
  updates adds 1 in total.
- T4.4 An alter where one update fails (for example, a rename to a taken name) changes nothing.
  **(guard)**
- T4.5 B has synonym `client`. Creating term `Client`, or renaming A to `client`, fails with 400.
  **(guard)**
- T4.6 Term `Client` exists. Adding synonym ` CLIENT ` to another term fails with 400. **(guard)**
- T4.7 Adding synonym `Customer` to term `Customer` fails with 400.
- T4.8 Empty name, a name of only spaces, and a name of 129 characters fail with 400.
- T4.9 `updateLinks` replaces the whole list. A link with an empty label or URL fails with 400.
- T4.10 Rename keeps the ID, synonyms, relations, domains and bindings.
- T4.11 Race: two threads add different synonyms to the same term at the same time. Both synonyms
  are stored and `version` grows by 2.
- T4.12 `TermChangeHandler`: a test handler records each change. Every write sends exactly one
  change of the right kind. A test handler that rejects the change leaves the store unchanged.
- T4.13 As MANAGE, drop a `DRAFT` term. Its synonyms, relations, domain links and bindings are
  soft-deleted in the same transaction.
- T4.14 As MANAGE, drop an `APPROVED` term without first changing its status. It succeeds and
  cleans up the same rows as T4.13. Repeat with a `DEPRECATED` term. **(guard)**
- T4.15 Race: create `X` while adding synonym ` x ` to another term. Exactly one succeeds; the
  other fails with 400, and the name/synonym invariant holds. Use two independent store sessions,
  hold the winning request after its checks, and assert that the other request waits on the
  hierarchy lock. Release the winner to commit, then check that the waiter sees its changes.
  Run with each request winning first, and also with the synonym supplied in a concurrent create.
  **(guard: removing the hierarchy lock from either write lets both checks pass.)**
- T4.16 Repeat T4.15 with a rename to `X` instead of a create. A rejected rename keeps the old
  name and revision. **(guard)**

**T5. Relations in the manager**

- T5.1 A → A fails with 400. **(guard)**
- T5.2 A → B exists. B → A fails with 400. **(guard)**
- T5.3 A → B → C → D exists. D → A fails with 400.
- T5.4 Read the loaded depth limit `L`. A chain of `L` links works; adding one link at either
  end would create `L + 1` links and fails with 400, without changing the graph or revisions.
  Parameterize manager tests with limits 10, 20 and 50. **(guard)**
- T5.5 With loaded limit `L`, chain X has `floor(L / 2)` links and chain Y has
  `L - floor(L / 2)` links. Both are valid separately. Join the top of X to the bottom of Y:
  the resulting longest chain would have `L + 1` links, so the write fails with 400 and changes
  nothing. Run for each limit of T5.4. A check that only walks up from the new parent accepts
  this invalid link. **(guard)**
- T5.6 C is an ancestor of A. Adding `RELATED` between A and C fails with 400, in both directions.
  **(guard)**
- T5.7 A `RELATED` C exists. Adding A → B, where B → C, fails with 400. **(guard)**
- T5.8 Race: A → B and C → D exist. Two threads add B → C and D → A at the same time. Exactly one
  succeeds and the other fails with 400, in every round. **(guard: without the hierarchy lock,
  some rounds end with a cycle.)**
- T5.9 Race: B is a `DRAFT`. One thread adds A → B while another drops B. Either the link is added and then removed
  by the drop, or the add fails with 404. No live link to B is left.
- T5.10 A has parents B and C, and B is `DEPRECATED`. Drop B. A keeps C and its revision grows by
  1. The audit has one entry for A losing B, with the new revision. C's derived `narrower` list
  and revision are unchanged.
- T5.11 The hierarchy read of a diamond returns each term once, in both directions.
- T5.12 The hierarchy read stops at a term the caller cannot see.
- T5.13 Setting the broader term of A needs read access to the parent. A `DRAFT` parent owned
  by another user fails with 404 for CREATE, even when that caller can edit A. The parent owner
  with CREATE, and any caller with MANAGE, can read that parent.
- T5.14 Mixed load: 8 threads run 500 random relation adds, relation removes, edits and deletes on
  30 terms, on MySQL and PostgreSQL. No deadlock or lock-wait timeout happens, and the invariants
  hold at the end.
- T5.15 X → A, B → Y and X `RELATED` Y exist. Adding A → B fails with 400 and changes nothing.
  Cover both ID orders of the stored `RELATED` pair and an alter that combines content and
  relation updates. **(guard: checking only ancestors of B accepts the invalid link.)**
- T5.16 Race a mixed alter (content, rename or synonym, and `addBroader`) against deleting a
  referenced `DEPRECATED` term. Also race create with initial relations against that delete.
  Assert the lock order: endpoint, hierarchy, then all term IDs in order. No deadlock occurs;
  either the write commits before cleanup or it fails with 404 and writes nothing. Use
  independent sessions and latches at lock acquisition to exercise both orders. T7.7 extends
  these cases with domains.
- T5.17 A → B, B → C and B `RELATED` D exist, with no broader path between B and D. Drop
  `DEPRECATED` B. A and D each get one revision increment and one audit record of their loss.
  C loses B from its derived `narrower` list but keeps its revision. Cover either ID order of
  the `RELATED` row. Inject a failure after cleanup starts: links, revisions and successful
  cleanup audit records all roll back. **(guard)**

**T6. Authorization**

The users for these cases:

| User       | Privileges on the metalake                                              |
| ---------- | ----------------------------------------------------------------------- |
| `viewer`   | `VIEW_DEFINITION`                                                       |
| `writer`   | `CREATE_DEFINITION`; owns term `Own`                                    |
| `writer2`  | `CREATE_DEFINITION`; owns nothing                                       |
| `admin`    | `MANAGE_DEFINITIONS`                                                    |
| `denied`   | `MANAGE_DEFINITIONS` from one role, deny `VIEW_DEFINITION` from another |
| `outsider` | none                                                                    |
| `owner`    | owner of the metalake, no grants                                        |

- T6.1 Matrix: for each user, each status (`DRAFT`, `APPROVED`, `DEPRECATED`), and the terms `Own`
  and `Other` (owned by `admin`), run get, list, alter, set status and drop. The expected result
  of each cell comes from the Access control and Status tables, and from "Order of checks". The
  test keeps the expected results in one table in the test code. **(guard: remove the owner check,
  and the `writer2` cells fail.)**
- T6.2 `denied` sees nothing: get returns 404 and list is empty.
- T6.3 `owner` passes every check.
- T6.4 A group owns a term. A member of the group can edit it. A user outside the group with
  `CREATE_DEFINITION` cannot. A group member with CREATE, or any caller with MANAGE, can read
  its draft; other CREATE users cannot. Losing group membership removes the member's owner-based
  read and edit access.
- T6.5 Granting any definition privilege on a catalog, or anything on `GLOSSARY_TERM`, fails with
  400.
- T6.6 `Child` has a hidden `DRAFT` parent and a visible parent. `viewer` sees only the visible one
  in `broader`. `admin` sees both.
- T6.7 Known leak: `writer` owns DRAFT term `Hidden`; `writer2` has CREATE and owns nothing.
  First assert that `writer2` gets 404 for `getTerm("Hidden")` and cannot find it in a list.
  Then creating ` hidden ` as `writer2` fails with 409. Its error contains no definition, owner
  or domains of the hidden term. Read back as `writer`: ID, name, content, owner and revision
  are unchanged, and no new term or success event was created. **(guard)**
- T6.8 For each status, `writer` cannot delete its own term (403), while `admin` can delete
  either its own or another user's term. Also try `APPROVED` → `DRAFT` → delete and
  `DEPRECATED` → `DRAFT` → delete as `writer`: status edits succeed, deletion still fails with
  403, and the term remains. **(guard: deletion must check MANAGE, not ownership or status.)**
- T6.9 CREATE can explicitly create in each status, with default `APPROVED` when omitted, and
  can edit content and set any status on its own term without a reviewer. An edit to an
  `APPROVED` term is visible immediately to `viewer`; a draft is visible to its owner and
  `admin`, but get returns 404 and lists omit it for `viewer` and `writer2`. `admin` can edit
  any term. No approval operation or reviewer privilege is consulted in these Step 1 calls.
- T6.10 An allow on CREATE alone gives VIEW; an allow on MANAGE alone gives CREATE and VIEW.
  A deny on VIEW blocks reads and writes for both, including term owners: get returns 404,
  list is empty, and create returns 403. A deny on CREATE blocks creation even with an allow
  on MANAGE, but effective MANAGE still permits editing and deleting existing terms.
- T6.11 Allow MANAGE and deny MANAGE through different roles. The caller cannot use the
  all-term edit/delete or draft-read paths, but retains effective CREATE from the allowed
  grant's implications: it can create and maintain its own terms, and read its own drafts.
  It still cannot delete its own terms. **(guard: do not short-circuit on an allow or owner
  before checking the required denies.)**
- T6.12 Transfer a draft from `writer` to `writer2` through the existing owner API. `writer2`
  gains owner-based read and edit access; `writer` gets 404 for that draft and cannot edit it.
  Neither can delete it. `admin` retains read, edit and delete access throughout.

**T7. Domain membership**

- T7.1 Create with two domains the caller can read. Get returns both.
- T7.2 Create in a domain without `USE_DOMAIN` on it fails with 403, and no term is created.
- T7.3 `domain=Sales` returns the terms of Sales and of its subdomains that the caller can see. It
  leaves out the terms of hidden subdomains.
- T7.4 Dropping a domain with terms fails with 409 (`NonEmptyDomainException`). After
  `removeDomain`, the drop works. **(guard)**
- T7.5 Race: one thread adds a term to a domain while another drops the domain. Either the term is
  added and the drop fails with 409, or the drop wins and the add fails with 404.
- T7.6 Under the selected explicit-relation design, a tag on a term never changes its domains,
  even if the tag backs a domain.
- T7.7 Extend T5.16 with `addDomain` in the mixed alter and domains in the create. Domain rows
  are locked only after all term rows, and a rejected request leaves membership, content,
  relations and revisions unchanged.
- T7.8 A readable term belongs to Sales and a domain the caller cannot see. Only Sales appears
  in its returned domains, using the shared domain-filtering check. With both domains hidden,
  the term stays readable and its domain list is empty. Returned data contains no hidden-domain
  name, ID or count; stored membership stays intact, and reads do not change the revision.
  Endpoint wiring is covered by T8.8, T12.8 and T13.12. **(guard)**

**T8. REST**

- T8.1 Each endpoint returns the response type in the REST API table, and each error in the Errors
  list maps to its HTTP code.
- T8.2 Term `a/b` works in every path as `a%2Fb`.
- T8.3 A term named `resolve` works with get, alter and drop.
- T8.4 An unknown update type, and an empty update list, fail with 400.
- T8.5 Each DTO turns into JSON and back with no loss. Missing optional fields keep their defaults.
- T8.6 `./gradlew :docs:build` passes, and the spec has every endpoint in the REST API table.
- T8.7 As MANAGE, an unfiltered get reads a DRAFT or DEPRECATED term, while get and hierarchy
  with `status=APPROVED` return 404 for the same root. A matching root works; nested terms obey
  the filter, including name-only lists. An invalid status returns 400. **(guard)**
- T8.8 Exercise T7.8 through direct get, list, hierarchy and alter responses. Every nested
  `TermDTO.domains` filters hidden domains; altering content without changing membership does
  not reveal or remove a hidden domain. Create responses include only authorized domains.

**T9. Tags on terms**

- T9.1 Tag a term, list its tags, list the terms of a tag, and remove the tag.
- T9.2 Tagging a term does not change its `version`.
- T9.3 Dropping the term or the metalake removes its tag relations.

**T10. Events and audit**

- T10.1 Each write sends a pre event and a success event with the term name and the new `version`.
- T10.2 A failed write sends a pre event and a failure event, and no success event.
- T10.3 Dropping a term with two children writes one audit entry for each child, including its
  lost parent and new revision. A surviving `RELATED` neighbor also gets a cleanup entry.

**T11. Bindings**

- T11.1 Bind and unbind a term on each of the 8 object types and on a metric.
- T11.2 Binding a tag, a role or a term fails with 400. Binding a missing object or metric fails
  with 404.
- T11.3 An object admin with VIEW but without term edit access binds an `APPROVED` term: it
  works. Binding another user's `DRAFT` returns 404; binding a visible `DEPRECATED` term returns
  400. A visible term with neither authorization path returns 403. Every failure leaves the
  bindings and revisions unchanged. **(guard)**
- T11.4 A term editor with only read access to the object binds a `DRAFT` term: it works.
  Repeat as a caller who is both object admin and term editor: the term-editor path still
  permits DRAFT and DEPRECATED bindings. MANAGE does not replace the required object read access.
- T11.5 One request binds two terms, and the caller passes neither check for the second one. The
  request fails with 403, and the first term is not bound either. **(guard)**
- T11.6 Binding the same term to the same object twice keeps one row.
- T11.7 Drop the table. Its bindings and its column bindings are soft-deleted. Dropping the schema,
  the catalog and the metalake works the same way.
- T11.8 Drop one column with `alterTable`. Its bindings are gone. The bindings of the other columns
  and of the table stay. Each term losing bindings gets one revision increment; terms whose
  bindings do not change keep their revisions.
- T11.9 Rename the table, and rename a column. The bindings stay.
- T11.10 Alter a semantic model to remove a metric. Its bindings are gone. A renamed metric loses
  its bindings, with the audit details of T11.15. Each affected term's revision grows by 1, even
  when several metrics bound to it are removed in one alter. Unaffected terms keep their revisions.
- T11.11 Race: one thread binds a term to a table while another drops the table. No live binding
  to the dropped table is left.
- T11.12 At the storage layer, insert the same live `(term_id, target_type, target_id)` twice:
  the unique key rejects the second insert. Repeat for TABLE, COLUMN and METRIC, using each
  target's own ID. Different metric IDs on the same model work. A NULL in any key column fails,
  and a soft-deleted binding does not block a new live binding to a still-live target. Run on
  H2, MySQL and PostgreSQL. **(guard)**
- T11.13 Drop a table with several bound columns and a table binding to the same term. That term
  gets exactly one revision increment and one audit record listing the lost bindings. Repeat
  for schema and catalog drop. A metalake drop leaves no surviving terms. Inject failure during
  column or metric cleanup: the object change, bindings, revisions and successful cleanup audit
  records all roll back. No-op cleanup leaves revisions unchanged. **(guard)**
- T11.14 Race column or metric removal against binding writes and ordinary term edits. Endpoint
  locks exclude new bindings before affected terms are collected; all affected term rows are
  then locked in ID order. No deadlock, dangling binding or lost revision increment occurs.
- T11.15 Rename a metric bound to two terms. Each term gets one audit entry with its ID and name,
  the semantic-model ID and name, the retired metric ID and name, the triggering alter operation
  and its new revision. Removing multiple metrics in one alter groups the losses per term. A
  failed alter leaves no successful cleanup entry.
- T11.16 Bind a term to a view itself: it works. A COLUMN request under a view-only parent name
  returns 404 and writes nothing. A table-column request resolves table and column IDs and
  authorizes and locks the table chain. Verify its rename/drop behavior through T11.8 and
  T11.9; do not resolve a view as a table or create a view-column binding. **(guard)**
- T11.17 Bind metrics with long names, including two names sharing a long prefix, through their
  model/name API inputs. Stored bindings use their distinct metric IDs; responses return the
  correct model/name without truncation. Content edits and a semantic-model rename preserve
  bindings. Remove a metric and reintroduce the same name: it has a new ID and no old bindings.
  Explicitly binding it again does not conflict with the retired identity. **(guard)**

**T12. Binding reads**

- T12.1 `GET /terms/{term}/objects` leaves out objects the caller cannot access. For a metric, the
  caller needs access to the semantic model; for a column, access to its table. A hidden root
  term returns 404 before any bindings are returned.
- T12.2 `GET /objects/.../terms` requires target read access and leaves out terms the caller
  cannot see. The metric endpoint checks semantic-model read access and metric existence.
- T12.3 `children=true` on a table with 3 columns, where 2 have terms, returns 2 entries in
  `children`. Repeat for a semantic model with 3 metrics, where 2 have terms. Each case sends
  one query to `glossary_term_binding`; metric IDs are resolved in a batch, not per metric.
- T12.4 `children=true` on a type other than table or semantic model, including VIEW, fails
  with 400. A view's own terms can still be listed without children.
- T12.5 A glossary viewer guesses a restricted table or model name. Reverse reads, including
  `children=true` and the metric endpoint, return 403 with no terms, child names or counts.
  Assert that child enumeration and binding queries are not called before target authorization.
  Repeat as glossary MANAGE without target read access: MANAGE grants no asset access. **(guard)**
- T12.6 As MANAGE, use `status=APPROVED` on reverse object/metric reads. Only APPROVED terms
  appear, and table-column/metric children with only DRAFT or DEPRECATED terms are omitted.
  Without the filter, ordinary term visibility applies. All nested DTOs filter hidden domains.
- T12.7 Call `/terms/{term}/objects?status=APPROVED` directly as the draft owner and as MANAGE:
  a DRAFT or DEPRECATED root returns 404 and no bindings. With an APPROVED root, pause the read
  after its root check while another session changes status and bindings. The response must
  use one consistent snapshot for both checks and payload; a later request sees the new status.
  Exercise both commit orders and assert that reads take no write locks. **(guard: independent
  root and payload reads can return bindings from an unapproved state.)**
- T12.8 Exercise T7.8 through reverse object and metric reads, including table-column and
  metric children. Each nested term filters its domains, independently of target and term
  visibility; readable terms remain in the result when all their domains are hidden.

**T13. Lookup**

- T13.1 `match=  ACTIVE   user ` finds `Active Customer` by its synonym `active user`, with
  `matchedBy` `SYNONYM`.
- T13.2 `match=customers` does not find `Customer`.
- T13.3 One term matches two words. It is returned once, with two `matchedBy` entries.
- T13.4 A word that only matches a `DEPRECATED` term returns nothing with `status=APPROVED`.
- T13.5 20 words work. 21 words fail with 400. **(guard)**
- T13.6 A term with 21 authorized bindings returns 20, with `bindingsTruncated` true. A term
  with 20 authorized bindings returns 20, with `bindingsTruncated` false.
- T13.7 `nameContains=%` and `nameContains=_` return only names that contain those characters.
  **(guard)**
- T13.8 `domain` and `match` together return only matching terms of the domain and its visible
  subdomains.
- T13.9 `bindings=true` without `match`, or without `details=true`, fails with 400.
- T13.10 A term has 20 hidden bindings followed by one visible binding. Return that binding
  with `bindingsTruncated=false`. With all bindings hidden, return an empty list and false.
  Hidden rows neither consume the limit nor affect the flag. **(guard)**
- T13.11 Put hidden bindings across candidate-batch boundaries before, between and after
  visible ones. With 21 visible bindings return 20 and true; with 20 visible bindings and any
  number of hidden ones return 20 and false. The query continues until it finds a 21st visible
  binding or exhausts candidates, rather than stopping after a fixed number of raw rows.
- T13.12 Exercise T7.8 through lookup and match responses, including embedded bindings. Hidden
  domains are filtered without removing a readable matching term or changing stored membership.

**T14 and T15. Clients**

- T14.1 / T15.1 Each client method sends the expected REST call (checked with a mock server) and
  reads the response.
- T14.2 / T15.2 Each REST error turns into the matching client exception.
- T14.3 / T15.3 `matchTerms` / `match_terms` returns only `APPROVED` terms, with their bindings.
- T14.4 / T15.4 View bindings use the view itself, without column operations or `children=true`.
  Table-column operations use the table parent; `listColumnTerms` is exposed for tables only.

**T16. MCP tools**

- T16.1 With the fixture of the example in Agent integration, `resolve_business_term` returns the
  JSON in that example. The test compares it with a stored JSON file.
- T16.2 25 words send two REST calls (20 and 5). The results keep the order of the words.
- T16.3 No successful tool result contains a DRAFT or DEPRECATED term, including root names,
  name-only relations, hierarchy nodes and reverse object results. Run every tool as MANAGE
  and as the draft owner, who can read the drafts through unfiltered REST. Direct draft or
  deprecated roots fail with 404; a hierarchy walk from an APPROVED root through a DRAFT
  toward another APPROVED node stops before the draft and all nodes reached only through it.
  Test both traversal directions. Domains and binding targets are authorization-filtered.
  **(guard)**
- T16.4 An HTTP call with `Authorization` forwards that exact credential, ahead of configured
  service credentials. Cover Basic, Bearer and Negotiate headers and assert that failed user
  authentication is not retried with a service identity.
- T16.5 The tool descriptions contain the four rules for agents.
- T16.6 The tools exist only when the `glossary` tool tag is enabled.
- T16.7 Cover every mode in the identity table: HTTP with credentials, service fallback,
  fallback disabled, no configured credentials, and stdio with and without a service identity.
  Check the effective principal, outgoing credentials, rejection before any backend call when
  applicable, and audit attribution. Concurrent callers with different identities or metalakes
  do not share authorization state. All successful modes still enforce APPROVED-only.
- T16.8 Capture REST requests for all glossary tools: each term read passes `status=APPROVED`,
  including root-bindings and hierarchy calls. An error never triggers an unfiltered retry.
  `list_terms_for_metadata` uses `children=true` for tables and semantic models, and lists a
  view's own terms without it. **(guard)**

**T17. CUJ integration tests**: the cases are the CUJs in Critical user journeys below.

**T18. Performance**

- T18.1 Measure adding a link and reading the hierarchy, for chains of 10, 20 and 50 links, with
  3 parents per term, on MySQL and PostgreSQL. Report p50 and p95 over 100 runs.
- T18.2 The final depth limit is the largest of 10, 20 and 50 whose p95 for adding a link is under
  100 ms on both databases for the chain and wide-DAG fixtures of T18.1 and T18.3. The PR
  description has the result table.
- T18.3 Repeat the measurements on layered DAGs of 10, 20 and 50 links, with 10 and 100 terms
  per level and up to 3 parents per term. Include overlapping ancestor and descendant paths,
  and `RELATED` pairs that make a proposed link invalid only across its descendant/ancestor
  sets. Measure both accepted and rejected additions, report node and edge counts and p50/p95,
  and verify that the walks deduplicate visited IDs and batch queries per level.
- T18.4 After choosing the release constant, rerun T5.4, T5.5 and CUJ 6 using that value on
  H2, MySQL and PostgreSQL. Record the selected limit and results. The release check must prove
  that `L` links work and `L + 1` links fail; a fixed 21-link fixture is insufficient.

**T19. User documentation**

- T19.1 Every request and response example in `docs/glossary.md` comes from a run of the
  integration tests, not from hand writing.
- T19.2 `./gradlew :docs:build` passes.

### Critical user journeys

A critical user journey (CUJ) is a main path a user takes through the feature, from start to end.
T17 turns each CUJ into one end-to-end test. Step 1 ships only when all of them pass on H2, MySQL
and PostgreSQL.

- **Fixture**: one metalake with the users of T6; domains `Sales` (with subdomain `Retail`) and
  `Finance`; the catalogs that `TagIT` uses (Hive for tables and columns, plus fileset and model
  catalogs); and a semantic model with metrics `net_revenue` and `active_customers`. Each CUJ
  starts from this fixture and creates its own terms.
- **Clients**: each CUJ runs through the Java client. CUJs 1, 2 and 3 also run through the Python
  client, and CUJ 3 also runs through MCP.
- **Checks**: after each CUJ, the REST form of `GlossaryInvariants` must pass.

1. **Define terms.** `admin` creates `Customer` in `Sales`. `writer` creates `Active Customer`
   with synonym `active user`, broader term `Customer`, one link and one tag. Expected: both terms
   are `APPROVED`, `viewer` can read both, and the hierarchy read of `Customer` shows
   `Active Customer` as its child.
2. **Link terms to data.** Bind `Customer` to a table, `Active Customer` to a column, and
   `Active Customer` to metric `active_customers`. Expected: Implemented by and Used by list the
   objects; each object lists its term; the table read with `children=true` shows the column term.
3. **Agent lookup.** Call `resolve_business_term(["net revenue", "active user", "VIP"])` as
   `viewer`. Expected: `active user` returns `Active Customer` by synonym, with its column and
   metric; `VIP` is in `unmatched`; a `DRAFT` term named `VIP` and a `DEPRECATED` term never show.
   Repeat as `admin` and the owner of VIP, who can read it through unfiltered REST. All MCP
   tools still exclude it; a direct `list_metadata_by_term(VIP)` fails with 404. An approved
   term belonging to a hidden domain omits that domain, and inaccessible bindings are absent.
4. **Privileges.** `viewer`, `writer2` and `admin` each try to read, edit, set the status of and
   drop `Active Customer`. Expected: the results match the T6.1 table.
5. **Status and delete.** `writer` creates a term with no status: it is `APPROVED`, and `viewer`
   can read it immediately. The writer edits its definition, and the viewer sees the edit with
   no review step. Then `writer` sets it to `DRAFT`, `APPROVED`, `DEPRECATED` and `DRAFT` again,
   trying to delete it in each state: all deletes return 403. `writer2` and `viewer` cannot read
   it while it is a draft; `writer` and `admin` can. `admin` can delete it in any state; use a
   fresh term for each state. Expected: no status change bypasses the deletion privilege, and
   `version` grows by 1 on each content or status alter. No approval decision is required.
6. **Bad relations.** Read the server's actual release depth limit `L`, without a test override.
   Build a valid chain of `L` links, then try adding a link that would make `L + 1` links.
   Also try a cycle and a `RELATED` link between a term and its ancestor. Expected: each invalid
   write fails with 400, leaving data and revisions unchanged. Then two servers add the two
   links of T5.8 at the same time, 20 rounds: exactly one succeeds each round.
7. **Cleanup.** Drop a bound column, a bound table, a domain that has terms, a term with two
   parents and one child (as `admin`, while it is `APPROVED`), and then the metalake. Expected: the
   bindings go away with the objects; the domain drop fails with 409; the child keeps its other
   parent; each surviving term losing bindings or its parent gets one revision increment per
   cleanup transaction and an audit record of its losses. Also rename a bound metric and check
   that its bindings disappear, the affected term's revision grows by 1, and the audit names the
   removed metric and term. After the metalake drop, no glossary data of the metalake can be read.
8. **Edit on one server, read on another.** Start two servers on the same database. Edit, rename,
   relate and bind a term on server 1, and read it on server 2 right after each change. Expected:
   server 2 returns the new data each time (nothing is cached). If the test framework can start
   only one server, T17 adds a way to start a second one. It must not replace this CUJ with a
   single-server test.

## Task Breakdown

Step 1 is split into one issue per PR, under one epic. Each PR adds the tests of its cases in
Testing, and follows the rules there. REST PRs update the OpenAPI spec (`./gradlew :docs:build`). Tasks in the same group can be done in
parallel once the tasks they depend on are merged.

### Prerequisites

- [ ] Domain code merged to `main` (it is on `branch-domain-dev` now). Only T7 needs it.
- [ ] Decision on Open Question 3 (PUBLIC role) before T8. Domain membership is already decided:
      T7 implements explicit relations (Option A).

### Group A: API and storage

- [ ] **T1. Add the Glossary API** (`api/`). `GlossaryTerm` with `Status`, `TermChange`,
      `GlossaryOperations` on the metalake, the relation and binding types, `GLOSSARY_TERM` in
      `MetadataObject.Type`, the three definition privileges (metalake only), and the exceptions.
      Depends on: none.
- [ ] **T2. Add term entity and storage** (`core/`). `EntityType.GLOSSARY_TERM`,
      `GlossaryTermEntity`, and the tables `glossary_term_meta`, `glossary_term_synonym` and
      `glossary_term_version_info` (created, no rows) on H2, MySQL and PostgreSQL with upgrade
      scripts. POs, mappers, SQL providers and `GlossaryTermMetaService`: insert, get, list,
      update, soft delete, GC, and cleanup on metalake drop. Depends on: T1.
- [ ] **T3. Add term relation storage** (`core/`). `glossary_term_rel` and `glossary_hierarchy_lock`;
      insert and remove links; list broader, narrower and related terms; the per-level batched
      walk. Depends on: T2.

### Group B: Core logic

- [ ] **T4. Add the Glossary manager** (`core/`). Create, get, list, alter and drop; name and
      synonym rules; links, properties and source reference; status; revision; `TermChangeHandler`
      with the default handler; whole-request lock planning and name/synonym checks under the
      hierarchy lock. Depends on: T2, T3.
- [ ] **T5. Add term relations to the manager** (`core/`). Set broader and related terms; cycle
      check, depth limit and descendant/ancestor no-overlap checks under the hierarchy lock;
      hierarchy reads with authorization and optional status filtering at every node; cleanup
      on delete with surviving-term revisions and loss details for audit. Depends on: T3, T4.
- [ ] **T6. Add Glossary authorization** (`core/`, `server/`). The `GLOSSARY_TERM` case in
      shared glossary authorization checks, grant implications and deny precedence; owner checks
      for CREATE edits and draft reads; MANAGE-only deletion in every status; filtering of hidden
      terms inside relation lists. No Step 1 approval capability. Depends on: T4.
- [ ] **T7. Add domain membership** (`core/`). Explicit `glossary_term_domain_rel` storage
      (selected Option A), add and remove domains on create and alter, and the 409 check on
      domain drop.
      Shared domain filtering on term results and mixed relation/domain lock-order tests.
      Depends on: T4, T5, T6, prerequisites.

### Group C: REST and events

- [ ] **T8. Add Glossary REST APIs and OpenAPI** (`common/`, `server/`). DTOs, requests and
      responses; endpoints for terms, status, relations, hierarchy and domains; optional read
      status filters on roots and nested terms; domain filtering on every term DTO; error
      mapping. Depends on: T5, T6, T7.
- [ ] **T9. Support tags on terms** (`core/`, `server/`). `GLOSSARY_TERM` as a taggable type in
      the existing tag APIs. Depends on: T8.
- [ ] **T10. Add Glossary events and audit** (`core/`). `GlossaryEventDispatcher` with pre, post
      and failure events for every write, plus cleanup audit records with lost relations and
      the resulting term revisions. T11 adds binding-loss records. Depends on: T8.

### Group D: Bindings and lookup

- [ ] **T11. Add term bindings** (`core/`). `glossary_term_binding` storage; bind and unbind on all
      supported objects and on metrics, with both privilege paths; COLUMN means table columns
      only, with table identity, authorization and fencing; binding-specific target types and
      target IDs, reusing existing metric IDs; non-NULL unique keys and storage-level duplicate
      tests on all backends; cleanup on object drop, column drop and
      metric removal, with surviving-term revisions and audit details for metric rename and
      removal. Depends on: T6, T10.
- [ ] **T12. Add binding REST APIs and reads** (`server/`). Bind and unbind endpoints; objects of a
      term with root status guards in a consistent read snapshot; target authorization for
      terms of an object or metric; nested domain and status filtering; `children=true` for
      tables and semantic models only; OpenAPI. Depends on: T8, T11.
- [ ] **T13. Add term lookup filters** (`core/`, `server/`). `domain`, `status`, `nameContains`
      and `match` on the list endpoint, `matchedBy` in the result, and `bindings=true` with the limit
      and truncation flag computed after authorization. Depends on: T12.

### Group E: Clients and agents

- [ ] **T14. Add Glossary to the Java client**, including `matchTerms`. Depends on: T13.
- [ ] **T15. Add Glossary to the Python client**, including `match_terms`. Depends on: T13.
- [ ] **T16. Add Glossary MCP tools** (`mcp-server/`), with the agent rules in the tool
      descriptions, APPROVED filtering through every REST read, transport-specific effective
      identity and fallback behavior, and tests for nested filtering, direct unapproved roots,
      unmatched and ambiguous words. Depends on: T15.

### Group F: Release checks

- [ ] **T17. Add CUJ integration tests.** End-to-end tests for CUJs 1–8 as described in Critical
      user journeys, on H2, MySQL and PostgreSQL (Docker). Depends on: T14, T16.
- [ ] **T18. Add a hierarchy performance test** and set the final depth limit from it. Depends on:
      T5.
- [ ] **T19. Add Glossary user documentation** (`docs/`). Depends on: T14, T15.

Step 1 ships when T1–T19 are merged and all CUJs pass, including the depth-boundary rerun after
T18 selects the release limit. This completes Step 1 only, not the later scope of #14238.

### Step 2 tasks (not filed yet)

These are tracked here until separate GitHub issues are filed and linked. The local IDs below
are not issue numbers, and no follow-up issues have been created by this document update.

- [ ] **S2.1. Review and no self-approval**: proposed-change storage, the review handler for
      `TermChangeHandler`, explicit submit/approve/reject APIs, scoped MANAGE reviewer
      authorization and rejection of self-approval, including content, status, relations,
      membership, both binding paths and requested deletion. Ordinary MANAGE writes cannot
      bypass review; its own submissions need another authorized MANAGE reviewer.
- [ ] **S2.2. Version snapshots and history**: snapshots in `glossary_term_version_info`, the
      history and diff APIs, and atomic commit with the approved live change.
- [ ] **S2.3. Domain grants**: grants on domains, inherited by subdomains, with deny handling
      and revalidation when membership changes.
- [ ] **S2.4. Creation defaults and upgrade behavior**: default new terms to DRAFT and reject
      direct publication through create/update, including for MANAGE. No initial-status setting
      bypasses review. Preserve existing term IDs and statuses when enabling Step 2; an existing
      Step 1 APPROVED term is not relabeled as human-reviewed or given a fabricated decision.
- [ ] **S2.5. Web UI**: term pages, hierarchy, binding drawers, review queue and history.
- [ ] **S2.6. CSV import/export**: define the CSV schema and validation,
      permission-filtered exports, repeat-import identity using `sourceRef`, batch failure
      semantics, and imports through the same review and no-self-approval checks as other
      writes. This remains planned work, outside Step 1; public APIs alone do not deliver CSV.

**Step 2 acceptance requirements** (implemented with S2.1, S2.2, S2.3 and S2.4, not Step 1):

- CREATE creates a draft and submits its own changes; it cannot read another owner's draft,
  decide a proposal or publish by passing APPROVED in create/update.
- MANAGE can inspect drafts and pending changes in its authorized scope, and approve/reject a
  different principal's submission. MANAGE outside that scope cannot inspect or decide it.
- Two MANAGE users submit separate changes. Neither can approve its own submission, including
  through another role or group ownership; each can approve the other's authorized submission.
  Repeat the self-approval and direct-publication rejection as the metalake owner.
- Submit a content or binding change to an approved term. Ordinary reads and MCP continue to
  return the previous approved state, never pending content. Rejection preserves that state;
  approval exposes the new state. A requested deletion remains pending until approval.
- Enabling Step 2 preserves existing IDs/statuses and records no invented reviewer decisions;
  newly created terms default to DRAFT, regardless of the caller's management privileges.

## Implementation Steps

The glossary is built in two steps. Step 1 is this design. Step 2 adds review, history and grants
on domains on top of it. Both steps use the same term IDs, relations and storage, so Step 2 needs
no data migration.

| Area                 | Step 1 (this design)                                                                                                      | Step 2                                                                                   |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| Term model           | Terms with definitions, synonyms, owner, links, properties and source reference                                           | Same terms                                                                               |
| Relations            | Several broader terms, related terms, cycle check, cleanup                                                                | Relation changes are reviewed                                                            |
| Domain membership    | Explicit relations (Option A); zero or more domains, optional on create; filters and cleanup                              | Same membership source; changes are reviewed and domains become the scope of grants      |
| Tags                 | Ordinary tags on terms through the existing tag APIs                                                                      | Same                                                                                     |
| Bindings             | Catalogs, schemas, tables, views, table columns, filesets, topics, models and metrics; both privilege paths               | Binding changes are reviewed, on both paths                                              |
| Privileges           | Metalake grants; CREATE maintains own terms; MANAGE reads, edits and deletes any term; private drafts, deny and filtering | MANAGE retains scoped administration and adds approval/rejection; grants also on domains |
| Status               | Default `APPROVED`, immediately readable with VIEW; ordinary status edits; no approval decision                           | Default `DRAFT`; explicit approval required, including for MANAGE submissions            |
| Changes              | Take effect at once; `TermChangeHandler` is the write hook                                                                | Always reviewed; proposed changes are kept apart; nobody approves their own change       |
| Versions and history | Revision number and audit only                                                                                            | A snapshot of each approved change, readable history and diffs                           |
| APIs and clients     | REST, Java and Python clients, OpenAPI                                                                                    | Proposal, decision and history APIs                                                      |
| CSV import/export    | Not delivered; APIs and source references support future importers                                                        | Planned as S2.6, with permissions and review                                             |
| Agent lookup         | Exact match on names and synonyms; approved terms only; ambiguous and unmatched words                                     | Same lookup on the current approved state while proposed changes await review            |
| Audit and events     | Create, edit, status, relation, membership, binding and delete                                                            | Submit, approve, reject, and changes of the metalake setting                             |
| UI                   | None; the REST calls behind each page are listed in Web UI support                                                        | Term pages, hierarchy, binding drawers, review queue and history                         |

### Storage for Step 2

- `glossary_term_version_info` is in the Step 1 schema, but Step 1 writes no rows to it. Step 2
  writes a snapshot there for each approved change.
- Step 2 keeps term IDs, relation keys and existing data. It adds storage for proposed changes.
- The write hook covers content, status, relations, membership and both binding paths. So Step 2
  can review every change.
- On approval, Step 2 checks effective MANAGE in scope, rejects the submitting principal, checks
  the revision the change was based on, and the objects it
  refers to again. Then it writes the live change, the snapshot and the decision in one
  transaction. If the snapshot fails, the live change is rolled back.
- `APPROVED` in Step 1 means "agents can use it", not "a reviewer accepted it". Step 2 history
  tells a reviewer's approval apart from the default status.
