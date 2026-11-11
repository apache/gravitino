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
   A term lists its objects, and an object lists its terms. A binding never gives access to data.
4. **Domains**: a term belongs to zero or more domains. A domain is optional when a term is
   created. In Step 1, domains group and filter terms. They do not change who can see or manage a
   term.
5. **Status**: `DRAFT`, `APPROVED` or `DEPRECATED`. New terms are `APPROVED`. Anyone who can edit a
   term can set its status, in the create or update call. There are no transition rules. Agents
   only see `APPROVED` terms.
6. **Privileges**: `VIEW_DEFINITION`, `CREATE_DEFINITION` and `MANAGE_DEFINITIONS`, granted on the
   metalake. Deny wins over allow. REST and MCP use the same rules.
7. **Revisions**: every content change increases the revision number of the term, and is audited.
   There are no content snapshots and no history API in Step 1.
8. **APIs**: REST, Java and Python clients, read-only MCP tools, events and audit. Each view of the
   glossary page needs one or two REST calls. An agent can look up all business words of a question
   in one call.

## Non-Goals

1. **Approval workflow, version history and grants on domains**: these come in Step 2 (see
   Implementation Steps). In Step 1, edits take effect at once. Step 2 plugs in through
   `TermChangeHandler` (see Status and versions).
2. **Bulk import and export, connectors to other glossaries, SKOS export**: these tools can use the
   REST API and the source reference.
3. **Ranked, fuzzy or semantic lookup in Step 1**: Step 1 only matches names and synonyms exactly
   (ignoring case and extra spaces). This is enough for words that the glossary defines. Better
   matching can come later without an API change: `matchedBy` can get new values (for example
   `FUZZY` or `SEMANTIC`) and a score, and `resolve_business_term` keeps the same input and output.
4. **Access through terms**: a term or a binding never gives or blocks access to data. Terms have
   no policies.
5. **Agent writes and the UI itself**: the MCP tools are read-only. The UI uses the REST API. Web UI
   support lists the calls for each part of the page.

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
6. Later the term is replaced. The owner or the admin sets it to `DEPRECATED`, and agents stop
   seeing it. Then the admin deletes it.

## Design

### Data model

```
GlossaryTerm { id, name, definition, synonyms[], status: DRAFT | APPROVED | DEPRECATED,
               links[{label, url}], sourceRef?, properties, currentVersion, audit_info }
               // tags: the existing tag relation tables
TermRelation { fromTermId, toTermId, type: BROADER | RELATED, audit_info }
TermDomain   { termId, domainId, audit_info }                // Option A in Domain membership
TermBinding  { termId, objectId, objectType, metricName?, audit_info }
TermVersion  { termId, version, state, snapshot, audit_info }   // created now; rows from Step 2
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
- `MetadataObject.Type.GLOSSARY_TERM` is added for owners, tags, events and audit. Privileges
  **cannot be granted on a single term**: the role APIs reject them.
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
- **Depth limit**: a `BROADER` chain has at most 20 levels. A new link that makes a chain longer
  fails with 400. This bounds the cycle check and the hierarchy read. 20 is a first estimate. The
  performance test in Critical User Journeys sets the final value.
- **No overlap**: two terms cannot be `RELATED` if one is an ancestor of the other. Adding such a
  `RELATED` link, or a `BROADER` link that creates such a pair, fails with 400. This follows the
  SKOS rule. The check uses the same walk as the cycle check.
- **No recursive SQL**: the walk runs in code, with one batched query per level. The backend still
  supports MySQL 5.7, which has no `WITH RECURSIVE`.
- A hierarchy read returns all ancestors or all descendants of a term. There are no cycles, so the
  walk always ends.
- Deprecating a term does not change its children. Deleting a term removes its links. Each child
  that loses a parent gets its own audit record. The child keeps its other parents, or becomes a
  top-level term.

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
decides who manages a term, and the choice below matters. There are two ways to store the
membership. The choice is still open (Open Question 1).

**Option A: explicit relation (recommended)**

- A table `glossary_term_domain_rel (term_id, domain_id)`. Only the term API changes it: `domains`
  on create, `addDomain` and `removeDomain`. These changes go through `TermChangeHandler`, like any
  term change.
- To add a term to a domain, the caller needs the edit privilege on the term, and must be able to
  see the domain (`USE_DOMAIN`). In Step 2, the caller also needs `CREATE_DEFINITION` on that
  domain, so nobody can move a term into a domain where they cannot write.
- **Terms of a domain** (`GET /terms?domain=`) use the same scope as the Domain design: terms in
  the domain, and terms in its subdomains that the caller can see.
- Dropping a domain that still has terms fails with 409 `NonEmptyDomainException`. This is the same
  as a domain with subdomains. Removing the terms silently would change how they are grouped, and in
  Step 2 who manages them. The change is one more check in `DomainManager#dropDomain`.
- **Descriptive tags** on terms use the existing tag APIs and `APPLY_TAG`. With Option A, a tag on a
  term never changes its domains, even when the tag backs a domain.

**Option B: backing tags, the same as assets in #13867**

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

**Recommendation: Option A.** Most reasons are about Step 2:

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
| **Used by**        | A semantic-model metric is built from the term | a metric: (`SEMANTIC_MODEL`, `metricName`)                                  | metric `net_revenue` uses `Net revenue` and `Order`                                            |

Both kinds are stored in `glossary_term_binding`. `metric_name` is set only for Used by, so it
tells the two kinds apart. A binding is direct only: binding a table does not bind its columns.

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
- The endpoint checks the object once, and then checks each term in the request body in code. If
  one term fails both checks, the whole request fails with 403 and nothing changes. Removing a
  binding needs the same privilege as adding it.
- Each changed term gets a new revision and goes through `TermChangeHandler`. So the Step 2 workflow
  can hold a binding change, like any other term change.
- Binding a missing object or metric fails with 404. Binding any other object type fails with 400.
  If an object admin binds a term that is not `APPROVED`, the request fails with 400.
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

| Status               | Who can see it                              | Who can delete it                                       |
| -------------------- | ------------------------------------------- | ------------------------------------------------------- |
| `DRAFT`              | `CREATE_DEFINITION` or `MANAGE_DEFINITIONS` | owner with `CREATE_DEFINITION`, or `MANAGE_DEFINITIONS` |
| `APPROVED` (default) | `VIEW_DEFINITION`; MCP                      | nobody; deprecate it first                              |
| `DEPRECATED`         | `VIEW_DEFINITION`; not MCP                  | `MANAGE_DEFINITIONS`                                    |

- **Status is an ordinary field.** It is set in the create request (default `APPROVED`) or with
  `setStatus` in the update request. Anyone who can edit the term can set it: the owner with
  `CREATE_DEFINITION`, or `MANAGE_DEFINITIONS`. There are no transition rules and no separate
  status endpoint.
- **What `APPROVED` means in Step 1**: the term can be used by agents. It does not mean that a
  reviewer accepted it. An owner who wants to keep a term away from agents sets it to `DRAFT` or
  `DEPRECATED`. In Step 2, review separates writing a term from approving it.
- **Why three states**: each term is used on its own. `DRAFT` keeps a term away from agents while
  it is written. Bindings and people keep using a term after it is replaced, so `DEPRECATED` keeps
  it readable, which a delete cannot do. In Step 2, `APPROVED` also records that a reviewer
  accepted the term. A domain or an ontology (#14310) is published as a whole and has no review per
  item, so `DRAFT` / `PUBLISHED` is enough there.
- **Every API shows the same terms** (REST, clients, MCP). If the caller cannot see a term, lists
  leave it out and get returns 404. The same rule applies inside responses. The `broader`,
  `narrower` and `related` lists of a term, the terms of an object, and the terms of a domain all
  leave out terms the caller cannot see. So a hidden draft never shows up as a relation.
- **Known leaks**: hiding a term does not make it secret. A user can still find out that it exists
  in two ways: creating a term with the same name returns 409, and adding a synonym equal to its
  name returns 400.
- **Revision**: each change to the name, definition, synonyms, links, properties, relations,
  domains, bindings or status increases `current_version` by one, in the same transaction.
  Responses return it as `version`, so a client can see that a term changed after it read it.
  Owner and tag changes are only audited.
- **No snapshots in Step 1**: the audit log holds the change record. There is no history API.
  `glossary_term_version_info` is created with the other tables, but Step 1 writes no rows to it.
  Term reads and writes do not need it. A revision is not an approved version.
- **Workflow hook**: `GlossaryManager` sends every change to a `TermChangeHandler`: content, status,
  relations, domains, and both binding paths. The default handler applies the change at once. In
  Step 2, a review handler keeps the proposed change apart from the live tables until it is
  approved.
- Dropping a term soft-deletes its synonyms, relations, domain links, bindings and version rows, in
  the same transaction. Dropping a metalake soft-deletes all its glossary rows in
  `MetalakeMetaService#deleteMetalake`, next to the tag cleanup.

### Access control

In Step 1, the three privileges are granted on the metalake:

| Action                                                    | `VIEW_DEFINITION` | `CREATE_DEFINITION` | `MANAGE_DEFINITIONS` |
| --------------------------------------------------------- | ----------------- | ------------------- | -------------------- |
| Read `APPROVED` and `DEPRECATED` terms; look up words     | ✓                 | ✓                   | ✓                    |
| Create a term (the creator becomes the owner)             |                   | ✓                   | ✓                    |
| See `DRAFT` terms                                         |                   | ✓                   | ✓                    |
| Edit, bind and set the status of own terms, in any status |                   | ✓                   | ✓                    |
| Edit, bind and set the status of any term                 |                   |                     | ✓                    |
| Delete own `DRAFT` terms                                  |                   | ✓                   | ✓                    |
| Delete any `DRAFT` or `DEPRECATED` term                   |                   |                     | ✓                    |

- `MANAGE_DEFINITIONS` includes `CREATE_DEFINITION`, and `CREATE_DEFINITION` includes
  `VIEW_DEFINITION`. The names are general, so other business-definition objects can use them too.
- **Owner versus any term**: `CREATE_DEFINITION` lets a user write and maintain their own terms.
  `MANAGE_DEFINITIONS` lets an admin fix any term. In Step 1 an owner can approve their own term.
  Step 2 keeps the same three privileges and adds review: `CREATE_DEFINITION` then submits changes,
  and only `MANAGE_DEFINITIONS` approves them.
- **Edit privilege on a term**, used by Relations, Domain membership and Bindings:
  `MANAGE_DEFINITIONS`, or being the owner with `CREATE_DEFINITION`.
- **Registration**: three new `Privilege.Name` values. In Step 1 their `canBindTo` accepts
  `METALAKE` only. The role APIs return 400 when these privileges are granted on any other object,
  or when anything is granted on `GLOSSARY_TERM`. Step 2 adds `DOMAIN`.
- **Deny** works as for other privileges: a deny on the metalake wins over an allow. The metalake
  owner always passes.
- **Bindings** are written on the object, by the object admin (`APPROVED` terms only) or by the
  term editor. See Bindings.
- **Owner** (a user or a group), set through `/owners/glossary_term/{term}`. When a group owns a
  term, all its members count as the owner. To change the owner, the caller must be the current
  owner, or have `MANAGE_GRANTS` on the metalake.
- **Create** needs `CREATE_DEFINITION` on the metalake, and `USE_DOMAIN` on each domain in the
  request. The creator becomes the owner.
- **Implementation**: `JcasbinAuthorizer#hasMetadataPrivilegePermission` checks `GLOSSARY_TERM`
  against the metalake, plus the owner check. Domains and terms in a request body are checked in
  code. In Step 2, a `TermScopeResolver` adds the domains of a term, and their parents, as more
  levels of the same check.

### Locking

Term operations use the existing lock tools of the store, always in this order:

1. the live-endpoint fence (`LiveEndpointService.lockLiveEndpoint`, shared): the metalake, plus the
   chain of the bound object when binding;
2. the per-metalake hierarchy lock, if needed;
3. term rows, in ID order (exclusive);
4. domain rows (shared).

Each row is locked only once, in its final mode.

| Operation                     | Locks                                                                    | Conflict it prevents                     |
| ----------------------------- | ------------------------------------------------------------------------ | ---------------------------------------- |
| Create                        | metalake (shared) -> domains (shared)                                    | metalake or domain drop                  |
| Edit content, status, domains | term (exclusive) -> added domains (shared)                               | edits at the same time; domain drop      |
| Add or remove a relation      | metalake (shared) -> hierarchy lock -> both terms (exclusive, ID order)  | a cycle or overlap; the other term drop  |
| Bind / unbind                 | object chain (`lockLiveEndpoint`, shared) -> terms (exclusive, ID order) | object drop; term edits at the same time |
| Delete                        | metalake (shared) -> hierarchy lock -> term (exclusive)                  | a new link to the term being deleted     |

- The **hierarchy lock** is a `SELECT … FOR UPDATE` on one row per metalake in
  `glossary_hierarchy_lock`. The row is inserted on first use. Locking only the two terms is not
  enough. Example: A → B and C → D are added at the same time, then B → C and D → A. Each link
  passes its own check, but together they form a cycle. So every relation change takes the lock,
  and so does a delete. Without it, A → B could be added while B is being deleted, and A would
  point to a deleted term. Relation changes and deletes are rare, so running them one at a time per
  metalake is cheap.
- The walk runs after the lock is taken, so it reads the committed links. If a term was deleted
  before the lock was taken, the server sees that when it locks the term row, and returns 404.
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

| Operation              | Endpoint                                                                 | Request -> response                                                      |
| ---------------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------------------ |
| List, filter, match    | `GET /terms?details=&domain=&status=&nameContains=&match=&bindings=`     | -> `NameListResponse` / `TermListResponse`                               |
| Create                 | `POST /terms`                                                            | `TermCreateRequest` -> `TermResponse`                                    |
| Get                    | `GET /terms/{term}`                                                      | -> `TermResponse`                                                        |
| Alter                  | `PUT /terms/{term}`                                                      | `TermUpdatesRequest` -> `TermResponse`                                   |
| Drop                   | `DELETE /terms/{term}`                                                   | -> `DropResponse`                                                        |
| Hierarchy              | `GET /terms/{term}/hierarchy?direction=BROADER\|NARROWER`                | -> `TermTreeResponse`                                                    |
| Bindings of a term     | `GET /terms/{term}/objects?kind=&types=`                                 | -> `TermBindingListResponse`                                             |
| Terms of an object     | `GET /objects/{type}/{fullName}/terms?details=&children=`                | -> `NameListResponse` / `ObjectTermsResponse`                            |
| Bind / unbind          | `POST /objects/{type}/{fullName}/terms`                                  | `TermsAssociateRequest{termsToAdd, termsToRemove}` -> `NameListResponse` |
| Terms of a metric      | `GET /objects/semantic_model/{fullName}/metrics/{metric}/terms?details=` | -> `NameListResponse` / `TermListResponse`                               |
| Bind / unbind a metric | `POST /objects/semantic_model/{fullName}/metrics/{metric}/terms`         | `TermsAssociateRequest` -> `NameListResponse`                            |
| Owner                  | existing `/owners/glossary_term/{term}`                                  |                                                                          |
| Tags                   | existing `/objects/glossary_term/{term}/tags`                            |                                                                          |

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

- **List** is used both to browse and to look up words:
  - `domain` returns the terms of the domain and of the subdomains the caller can see.
  - `nameContains` finds names that contain the text, ignoring case. It is for search boxes.
  - `match` looks up business words: one `match` per word, up to 20. Each word is looked up on its
    own, and a term is returned when it matches any word (OR). The server compares `key(word)`
    with `name_key` and `synonym_key`. Each returned term has `matchedBy`. With
    `status=APPROVED`, a word that only matches `DEPRECATED` terms returns nothing.
  - `bindings=true` adds the bindings of each term, at most 20 per term. If there are more,
    `bindingsTruncated` is set. It only works together with `match` and `details=true`. A lookup
    then needs only one call. A word usually matches one to three terms.
  - Why not a separate path like `/terms/resolve`? It would clash with a term named `resolve`. So
    lookup is a filter of the list.
- **Alter** applies all updates in one transaction and increases the revision once. `narrower` is
  computed and cannot be set. `version` is the current revision. `updateLinks` replaces the whole
  link list. `status` defaults to `APPROVED` on create.
- **Terms of an object**: `children=true` works on `table`, `view` and `semantic_model`. It adds
  one entry to `children` for each column or metric with visible terms. A semantic model itself
  has no terms; only its metrics do.
- **Bind / unbind** use the same request format as tag association. The response is the object's
  terms after the change.
- **Errors**:
  - 404: `NoSuchTermException` (missing, or the caller cannot see it), `NoSuchMetadataObjectException`;
  - 409: `TermAlreadyExistsException` (same name or same source reference), `NonEmptyDomainException`;
  - 400: a cycle, a term as its own parent, binding an unsupported object type, a synonym equal to
    another term's name, deleting an `APPROVED` term, an object admin binding a term that is not
    `APPROVED`, or more than 20 `match` values;
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
| Glossary, term               | Approve, Deprecate                                                      | `PUT /terms/{term}` with `setStatus`                                                                      |
| Glossary, term               | Delete term (the dialog counts relations and asset links)               | `DELETE /terms/{term}`; the counts come from the term and its bindings                                    |
| Glossary, Add or Edit term   | Name, Definition, Synonyms, Domain, Broader, Related, Documentation     | `POST /terms` or `PUT /terms/{term}` with the changed fields; pickers use `/domains` and `/terms`         |
| Glossary, Add term           | Owner other than the creator                                            | `POST /terms`, then `PUT /owners/glossary_term/{term}` (allowed, because the creator is the owner)        |
| Explore, table or view       | Terms N, Update terms drawer, column chips and the term filter          | `GET /objects/{type}/{fullName}/terms?details=true&children=true`; `POST .../terms` per change            |
| Explore, Update terms drawer | Term picker (approved, grouped by domain)                               | `GET /terms?details=true&status=APPROVED&nameContains=`                                                   |
| Semantic model               | Glossary terms of each metric                                           | `GET /objects/semantic_model/{fullName}/terms?details=true&children=true`                                 |
| Metric editor                | Terms (approved only)                                                   | the picker call above; `POST /objects/semantic_model/{fullName}/metrics/{metric}/terms`                   |
| Domain, Definitions tab      | Terms of the domain and its subdomains, name search                     | `GET /terms?details=true&domain=&nameContains=`                                                           |
| Define, Overview             | Terms by domain, terms by status                                        | counted from `GET /terms?details=true`                                                                    |

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

The `mcp-server/` gets a read-only `glossary` tool tag, added to `SUPPORTED_TOOL_TAGS`. The MCP
server forwards the caller's `Authorization` header. So an agent sees exactly what its user can
see.

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

6 Agent runs the query with the user's own privileges and answers
```

| Step                    | Who                     | What happens                                                                                                                                                                                                                                                                                            |
| ----------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1. Pick business words  | Agent (LLM)             | Gravitino does not parse the question; the agent does. The tool description tells it how: keep a phrase whole ("active user", not "active" and "user"); use the singular form; leave out time ranges, numbers and query words ("last month", "total", "top 10"). A question usually gives 1 to 5 words. |
| 2. Call the tool        | Agent -> MCP server     | One call with all the words. Add `domain` when the user or the context names one.                                                                                                                                                                                                                       |
| 3. Look up              | MCP server -> Gravitino | One REST call, with one `match` per word. The server compares `key(word)` with term names and synonyms, and keeps only `APPROVED` terms the caller can see (and in `domain`, if given). It adds the bindings of each term.                                                                              |
| 4. Group                | MCP server              | Groups the terms by the word they matched (`matchedBy`), and lists the words with no match in `unmatched`.                                                                                                                                                                                              |
| 5. Decide               | Agent                   | One term for a word: use it. Several terms: pick one by domain, or ask the user. No term: ask the user, and do not guess a column. Then follow the bindings with the existing tools.                                                                                                                    |
| 6. Read data and answer | Agent -> existing tools | The query runs with the user's own privileges. The answer names the terms and definitions it used.                                                                                                                                                                                                      |

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
  never grants on a single term either. Grants are on the metalake in Step 1, and also on domains
  in Step 2. Unlike a page, a term can have no domain: a term used by several areas does not have
  to pick one.

## Open Questions

1. **Domain membership of terms**: Option A (explicit relation) or Option B (backing tags)? In
   Step 1 both are safe, because grants are on the metalake. The difference shows in Step 2, when a
   domain decides who manages its terms. This needs agreement with the Domain design.
2. **Who can see drafts**: this design shows `DRAFT` terms to every user with `CREATE_DEFINITION` or
   `MANAGE_DEFINITIONS`. Should it be only the owner and `MANAGE_DEFINITIONS`?
3. **Default read access**: everyone should be able to read terms by default. But Gravitino has no
   built-in role for all users. Should we add one (for example `PUBLIC`) with `VIEW_DEFINITION` on
   the metalake, or should admins grant it?

## Critical User Journeys

A critical user journey (CUJ) is a main path a user takes through the feature, from start to end.
Each CUJ below becomes one end-to-end test. Step 1 ships only when all of them pass on H2, MySQL
and PostgreSQL.

| #   | Journey                             | Steps                                                                                                                         | Expected result                                                                                                      |
| --- | ----------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| 1   | Define terms                        | An admin creates `Customer` in a domain. An analyst creates `Active Customer` with a synonym, a broader term, links and a tag | Both terms are `APPROVED` by default and visible to `VIEW_DEFINITION`; the hierarchy read shows the parent and child |
| 2   | Link terms to data                  | Bind a term to a table, a column and a metric; then list the terms of each object                                             | Implemented by and Used by show the objects; each object lists the term                                              |
| 3   | Agent lookup                        | Call `resolve_business_term` with a synonym, a name and an unknown word                                                       | The first two return the term and its data; the unknown word is in `unmatched`; `DRAFT` and `DEPRECATED` never show  |
| 4   | Privileges                          | Users with only `VIEW_`, only `CREATE_` and with `MANAGE_DEFINITIONS` try to read and edit terms                              | `VIEW_` reads only; `CREATE_` edits only its own terms; `MANAGE_` edits any term; a hidden draft never shows         |
| 5   | Status and delete                   | Set a term to `DRAFT`, back to `APPROVED`, then to `DEPRECATED`; try to delete it in each state                               | Who can see and delete it follows Status and versions; the revision grows on each change                             |
| 6   | Bad relations                       | Add a link that makes a cycle, a chain longer than the limit, and a `RELATED` link between a term and its ancestor            | Each one fails with 400 and nothing changes; the same holds when two servers add the links at the same time          |
| 7   | Cleanup                             | Drop a bound column and table, a domain that has terms, a term with children, and the metalake                                | Bindings go away with the object; the domain drop fails with 409; children keep other parents; all rows go away      |
| 8   | Edit on one server, read on another | Edit, move or bind a term on one server, then read it on a second server                                                      | The second server returns the new data at once (nothing is cached)                                                   |

A performance test also measures the cycle check and the hierarchy read for chains of 10, 20 and
50 levels, with many parents per term. The result sets the final depth limit (20 for now).

## Task Breakdown

Step 1 is split into one issue per PR, under one epic. Each PR has its own unit tests, and REST
PRs update the OpenAPI spec (`./gradlew :docs:build`). Tasks in the same group can be done in
parallel once the tasks they depend on are merged.

### Prerequisites

- [ ] Domain code merged to `main` (it is on `branch-domain-dev` now). Only T7 needs it.
- [ ] Decision on Open Question 1 (membership) before T7, and on Open Question 3 (PUBLIC role)
      before T8.

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
      with the default handler; row locks. Depends on: T2.
- [ ] **T5. Add term relations to the manager** (`core/`). Set broader and related terms; cycle
      check, depth limit and the no-overlap rule under the hierarchy lock; hierarchy read; cleanup
      on delete. Depends on: T3, T4.
- [ ] **T6. Add Glossary authorization** (`core/`, `server/`). The `GLOSSARY_TERM` case in
      `JcasbinAuthorizer`, owner checks for `CREATE_DEFINITION`, draft visibility, and filtering of
      hidden terms inside relation lists. Depends on: T4.
- [ ] **T7. Add domain membership** (`core/`). Membership storage (Option A or B, per Open
      Question 1), add and remove domains on create and alter, and the 409 check on domain drop.
      Depends on: T4, prerequisites.

### Group C: REST and events

- [ ] **T8. Add Glossary REST APIs and OpenAPI** (`common/`, `server/`). DTOs, requests and
      responses; endpoints for terms, status, relations, hierarchy and domains; error mapping.
      Depends on: T5, T6.
- [ ] **T9. Support tags on terms** (`core/`, `server/`). `GLOSSARY_TERM` as a taggable type in
      the existing tag APIs. Depends on: T8.
- [ ] **T10. Add Glossary events and audit** (`core/`). `GlossaryEventDispatcher` with pre, post
      and failure events for every write. Depends on: T8.

### Group D: Bindings and lookup

- [ ] **T11. Add term bindings** (`core/`). `glossary_term_binding` storage; bind and unbind on all
      supported objects and on metrics, with both privilege paths; cleanup on object drop, column
      drop and metric removal. Depends on: T6.
- [ ] **T12. Add binding REST APIs and reads** (`server/`). Bind and unbind endpoints; objects of a
      term; terms of an object or a metric; `children=true`; OpenAPI. Depends on: T8, T11.
- [ ] **T13. Add term lookup filters** (`core/`, `server/`). `domain`, `status`, `nameContains`
      and `match` on the list endpoint, `matchedBy` in the result, and `bindings=true` with the limit
      per term. Depends on: T12.

### Group E: Clients and agents

- [ ] **T14. Add Glossary to the Java client**, including `matchTerms`. Depends on: T13.
- [ ] **T15. Add Glossary to the Python client**, including `match_terms`. Depends on: T13.
- [ ] **T16. Add Glossary MCP tools** (`mcp-server/`), with the agent rules in the tool
      descriptions, and tests for unmatched and ambiguous words. Depends on: T15.

### Group F: Release checks

- [ ] **T17. Add CUJ integration tests.** End-to-end tests for CUJs 1–8 through the Java client,
      the REST API and MCP, on H2, MySQL and PostgreSQL (Docker). Depends on: T14, T16.
- [ ] **T18. Add a hierarchy performance test** and set the final depth limit from it. Depends on:
      T5.
- [ ] **T19. Add Glossary user documentation** (`docs/`). Depends on: T14, T15.

Step 1 ships when T1–T19 are merged and all CUJs pass.

### Step 2 tasks (not filed yet)

These are filed as a separate epic after Step 1 ships. Their scope is in Implementation Steps.

- [ ] Proposed-change storage and the review handler for `TermChangeHandler`
- [ ] Snapshots in `glossary_term_version_info`, and the history and diff APIs
- [ ] Grants on domains, inherited by subdomains
- [ ] The metalake setting for the default status
- [ ] Web UI: term pages, hierarchy, binding drawers, review queue and history

## Implementation Steps

The glossary is built in two steps. Step 1 is this design. Step 2 adds review, history and grants
on domains on top of it. Both steps use the same term IDs, relations and storage, so Step 2 needs
no data migration.

| Area                 | Step 1 (this design)                                                                                  | Step 2                                                                                     |
| -------------------- | ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| Term model           | Terms with definitions, synonyms, owner, links, properties and source reference                       | Same terms                                                                                 |
| Relations            | Several broader terms, related terms, cycle check, cleanup                                            | Relation changes are reviewed                                                              |
| Domain membership    | Zero or more domains, optional on create; filters and cleanup (how it is stored: Open Question 1)     | Membership changes are reviewed; domains become the scope of grants                        |
| Tags                 | Ordinary tags on terms through the existing tag APIs                                                  | Same                                                                                       |
| Bindings             | Catalogs, schemas, tables, views, columns, filesets, topics, models and metrics; both privilege paths | Binding changes are reviewed, on both paths                                                |
| Privileges           | The three definition privileges on the metalake; owner checks, deny, filtered results                 | Same privileges, also granted on domains and inherited by subdomains                       |
| Status               | Default `APPROVED`; anyone who can edit a term sets its status in the update call                     | Default `DRAFT`, or `APPROVED` by an audited metalake setting; status changes are reviewed |
| Changes              | Take effect at once; `TermChangeHandler` is the write hook                                            | Always reviewed; proposed changes are kept apart; nobody approves their own change         |
| Versions and history | Revision number and audit only                                                                        | A snapshot of each approved change, readable history and diffs                             |
| APIs and clients     | REST, Java and Python clients, OpenAPI                                                                | Proposal, decision and history APIs                                                        |
| Agent lookup         | Exact match on names and synonyms; approved terms only; ambiguous and unmatched words                 | Same lookup, on the approved state                                                         |
| Audit and events     | Create, edit, status, relation, membership, binding and delete                                        | Submit, approve, reject, and changes of the metalake setting                               |
| UI                   | None; the REST calls behind each page are listed in Web UI support                                    | Term pages, hierarchy, binding drawers, review queue and history                           |

### Storage for Step 2

- `glossary_term_version_info` is in the Step 1 schema, but Step 1 writes no rows to it. Step 2
  writes a snapshot there for each approved change.
- Step 2 keeps term IDs, relation keys and existing data. It adds storage for proposed changes.
- The write hook covers content, status, relations, membership and both binding paths. So Step 2
  can review every change.
- On approval, Step 2 checks privileges, the revision the change was based on, and the objects it
  refers to again. Then it writes the live change, the snapshot and the decision in one
  transaction. If the snapshot fails, the live change is rolled back.
- `APPROVED` in Step 1 means "agents can use it", not "a reviewer accepted it". Step 2 history
  tells a reviewer's approval apart from the default status.
