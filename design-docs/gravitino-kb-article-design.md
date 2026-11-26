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

# Design: Knowledge Base Articles

## Background

Gravitino manages data assets and their metadata. A [Glossary Term](glossary-design.md) gives a
short definition of a business concept. Teams also need longer explanations: how revenue is
calculated, why it differs from GMV, and which tables feed it. Today these explanations live in
wikis and documents, apart from the terms and data they describe. Users and agents cannot find
them through the same metadata APIs or follow managed links from an explanation to its data.

A Knowledge Base (KB) Article is a text document managed under a metalake. It has a title, a
Markdown or HTML body, an owner and a status. It can belong to several
[Domains](domain-design.md) and link to terms and data assets across catalogs. Users and agents
can search for an article, read it, and follow its bindings under their existing privileges.

---

## Goals

1. **Article object**: add `KnowledgeBaseArticle` as metalake-scoped metadata with an immutable
   name and stable numeric ID. A title can change without breaking references to the article.
2. **Current content storage**: store the title and Markdown/HTML body directly on the Article
   row. Edits update that current content; no historical-content copies are retained.
3. **Bindings**: link Articles to Terms and metadata objects, and list Articles by bound
   Term or asset. A binding never grants access to data.
4. **Domains**: organize and filter articles by zero or more Domains. Membership does not change
   article privileges in this design.
5. **Status and privileges**: support `DRAFT`, `PUBLISHED` and `DEPRECATED`, with immediate edits
   and definition privileges granted on the metalake. `PUBLISHED` makes content available to
   authorized readers and agents.
6. **Discovery**: provide basic title, synonym and body search, with Domain, status and owner
   filters. Read-only MCP tools return readable `PUBLISHED` articles and
   support progressive discovery through bindings, summaries and selected article reads.
7. **APIs and governance**: provide REST, Java and Python APIs, ownership, events and audit.
   Concurrent writes must not leave current content and relations out of sync.

---

## Non-Goals

1. **Review and publication approval**: content and status changes take effect immediately;
   approval workflows and review APIs are outside the scope.
2. **Domain-scoped or Article-scoped definition grants**: this release supports metalake grants.
   A reader with effective metalake `VIEW_DEFINITION` can read every published Article in that
   metalake, regardless of Domain membership.
   Domain-scoped grants will follow the shared rules described in [Access control](#access-control).
3. **Web UI, collaborative editing and agent writes**: the design provides REST APIs and read-only
   MCP tools. A web editor, real-time co-editing and agent writes are deferred.
4. **File ingestion, attachments and image upload**: the body is text. It may contain ordinary
   external links, but Gravitino does not host files, extract text or fetch linked content.
5. **Ranked, fuzzy or semantic search and RAG**: this design provides basic discovery. Advanced
   document retrieval belongs in a future integration with a dedicated document knowledge base;
   the current scope adds no embedding index or answer-generation service.
6. **Inferred or transitive bindings**: words and links in the body do not create bindings.
   Binding an article to a term does not bind it to that term's assets; binding a table does not
   bind its columns.
7. **Metric bindings and version pinning**: bind a Semantic Model as an object, not an individual
   metric. A binding points to the target's identity and resolves its current state.
8. **Content history, comparison, rollback and article hierarchy**: only current content is stored
   and exposed. There is no history table or history API. Articles form a flat list.

---

## Scope and Dependencies

The design covers the following capabilities. The task references link each capability to its
implementation work in [Task Breakdown](#task-breakdown).

| Capability                                      | Scope                                            | Tasks   |
| ----------------------------------------------- | ------------------------------------------------ | ------- |
| Article API, entity and storage                 | First-class metadata and current content         | T1–T3   |
| Definition privileges and ownership             | Metalake grants; platform Owner checks           | T4      |
| Domain membership                               | Shared member table; filter and drop guard       | T5      |
| Term and asset bindings                         | Stable IDs, target filters and lifecycle cleanup | T6      |
| REST, editing and search                        | DTOs, errors, summaries and OpenAPI              | T7–T8   |
| Events and audit                                | Existing listener and audit mechanisms           | T9      |
| Java, Python and read-only MCP                  | Same contracts and visibility as REST            | T10–T12 |
| Integration, upgrade, performance and user docs | Release checks and documented operating limits   | T13–T14 |

Domain integration requires stable Domain IDs, hierarchy lookup, access checks and deletion
fencing. Term bindings require stable Term IDs, current-status and registered-liveness lookup.

Current Article title, body and metadata live directly in the Article row. Current Domain
memberships and bindings use relation tables, updated with that row in one transaction.
OSS retains no historical body copies, version pointers or retention configuration. Stable
Article IDs, an internal revision counter and the shared transaction boundary support storage
extensions without reserving history-specific columns.

---

## Usage Scenario

1. An admin grants an analyst `CREATE_DEFINITION` and a reader `VIEW_DEFINITION` on a metalake.
   The analyst can manage Domain `Finance` and read term `net-revenue` and table
   `iceberg.sales.orders`.
2. The analyst creates "Revenue Calculation Guide", with a Markdown body, Domain `Finance`,
   the term and the table. The server generates a name if none is supplied. The analyst becomes
   the owner; the default status is `PUBLISHED`, and its current content takes effect immediately.
3. The reader searches for a word from the body and loads the article. Its bindings lead to
   the current term and table; each target still requires its own read privilege.
4. The analyst loads the Article, then sends its `revisionToken` as `expectedRevision` in one
   alter request with `updateBody` and `addObjects`. The body and new binding take effect together.
5. An agent acting as the reader calls `search_kb_articles`, then `get_kb_article`. It receives
   the stored body with its format and only the bound objects it can read.
6. To prepare content privately, the analyst can create or set the article to `DRAFT`. Only its
   owner with CREATE, or a caller with MANAGE, can read it. Setting it to `PUBLISHED` makes it
   available again without review.
7. When the explanation is outdated, the owner sets it to `DEPRECATED`. It remains readable,
   but agents stop returning it. The owner with CREATE or an admin with MANAGE can delete it
   directly in any status. Deleting it removes neither the term nor the table.

---

## Design

### Data model

`KnowledgeBaseArticle` is directly under a metalake, alongside Tag, Policy and Domain. It is not
under a catalog or schema, because an article can explain data from several catalogs.

```text
KnowledgeBaseArticle {
  name, title, body: { format: MARKDOWN | HTML, content },
  status: DRAFT | PUBLISHED | DEPRECATED, domains[], comment?, synonyms[],
  sourceReference?, properties, audit_info, revisionToken
}
ArticleBinding { articleId, targetType, targetId, audit_info }
ArticleDomain  { articleId, domainId, audit_info }
```

The Article row stores current content. Its internal `revision` is the basis of the editing token.

- **Identity**: `metalake.articleName`. The name is immutable and unique in the metalake; the
  internal numeric article ID is stable. Add `KNOWLEDGE_BASE_ARTICLE` to `MetadataObject.Type`
  and `Entity.EntityType`, including the single-level object handling used by owner APIs.
- **Name normalization**: use Unicode NFC for explicit and generated Article names before
  storage and for every name-based lookup, including generic metadata APIs. Return the normalized
  name. For example, `café` with a precomposed `é` and the spelling with `e` plus a combining acute
  accent identify the same name. NFC does not change the existing case rules or transliterate
  Chinese. This rule applies to Article names, not titles or body content.
- **Explicit name**: normalize to NFC before validation. Accept 1 to 128 Unicode code points,
  using letters, digits, combining marks,
  `_` and `-`, with at least one letter or digit. Reject other characters with `400`, including
  `/`, backslash, whitespace and control characters. Preserve case and store the NFC spelling.
- **Generated name**: `name` is optional on create. Lower-case the title using `Locale.ROOT`,
  preserve Unicode letters, digits and combining marks, and replace each run of other characters
  with `-`. Normalize the result to NFC, trim leading/trailing `-` and cut to 128 Unicode code
  points without splitting a character. Chinese text is preserved: `收入计算说明` becomes `收入计算说明`, and
  `收入 Calculation Guide` becomes `收入-calculation-guide`. Do not transliterate to Pinyin.
  Use `article-<shortId>` only when the result contains no Unicode letter or digit, such as an
  emoji-only or punctuation-only title. The suffix is 8 random lower-case alphanumeric
  characters, regenerated on collision. A derived-name collision adds `-2`, `-3`, and so on,
  cutting the base further to keep the whole name within 128 code points. A duplicate explicit
  name is `409`. Changing the title later never changes the generated name.
- **Concurrent name allocation**: the unique key `(metalake_id, article_name, deleted_at)` is
  the final arbiter. If a generated candidate loses a concurrent insert race on that key, roll
  back the complete create transaction, choose the next available suffix (or a fresh random
  fallback), and retry in a new transaction. Allow at most five retries after the initial
  attempt; exhaustion returns `409`. Failed attempts leave no Article, Owner or relation
  rows. Explicit-name conflicts return `409` immediately. Retry only generated Article-name
  unique-key conflicts. Other failures end the attempt.
- **Title**: required, nonblank after trimming, at most 512 Unicode code points. Articles can share
  a display title within the same Domain or without a Domain. Use the immutable `name` to
  address an Article. Generated names from equal titles use the normal collision suffixes.
- **Owner**: when authorization is enabled, the creator becomes the initial owner. Owners may be
  users or groups. Persist the relation in the existing `owner_meta` table: `metadata_object_id` is
  the stable article ID, `metadata_object_type` is `KNOWLEDGE_BASE_ARTICLE`, and `metalake_id`
  identifies its metalake. `owner_id` references the user or group ID and `owner_type` is `USER`
  or `GROUP`; owner names are resolved from `user_meta` or `group_meta`. Integrate the Article
  type with `OwnerMetaService` and the existing Owner APIs. No owner column is added to the
  article tables; ownership uses its own relation and audit path.
- **Other fields**: `comment` is at most 256 Unicode code points. `synonyms` are trimmed, at most 128
  characters each and at most 20 per article; remove duplicates ignoring case. They need not be
  unique across articles. `sourceReference` is optional free text of at most 1,024 Unicode code points,
  such as a wiki URL. It is not validated as a URL, followed or fetched. Render it as text unless
  a client separately validates it as a safe link. `properties` is a string map.

#### Generic metadata API integration

Article uses the existing Owner, audit and generic relation-cleanup mechanisms with these
capability boundaries:

| Capability                  | Article behavior                                                                     |
| --------------------------- | ------------------------------------------------------------------------------------ |
| Owner and audit             | Supported through the existing APIs and authorization rules                          |
| Tag and Policy associations | Not supported in this scope; Article is not added to their supported-type allowlists |
| Definition grants           | Metalake scope only; Article scope is rejected by shared grant-object validation     |
| Orphan cleanup              | Remove generic relations left behind after Article deletion                          |

Register `KNOWLEDGE_BASE_ARTICLE` with `OrphanedMetadataObjectRelationService`, using
`kb_article_meta` and `article_id` to determine Article liveness. This clears residual Owner and
other generic relations referring to a deleted Article. It is separate from asynchronous
Article-to-target binding cleanup, which removes bindings whose targets were deleted.

#### Body and input validation

`body.format` is `MARKDOWN` or `HTML`, stored on the current Article. Store Markdown as submitted text.
Markdown consumers render embedded HTML as escaped text. They render Markdown first, then sanitize
the generated HTML with a maintained library, including inline links, reference links and image
URLs; see [CommonMark rendering guidance](https://github.com/commonmark/commonmark-java#usage).
For HTML input, use [OWASP Java HTML Sanitizer](https://owasp.org/projects/java-html-sanitizer)
on write, removing scripts, active elements and event-handler attributes while preserving safe
formatting. For both formats, sanitize the final rendered HTML before display and use a URL
protocol allowlist that excludes `javascript:`, `data:` and encoded variants.

Responses, including MCP, return the stored body and format. Compare stored values for no-op
detection. Process body input only on create or `updateBody`; unrelated edits and library upgrades
do not rewrite stored bodies.

Limit both submitted and stored `body.content` by UTF-8 byte length, including HTML after
sanitization. The configurable limit defaults to 1 MiB (1,048,576 bytes); allowed settings are
1 through 16,777,215 bytes, the
MySQL `MEDIUMTEXT` capacity. An invalid setting prevents startup. An oversized request returns
`400`, without truncation or a stored change. Use the same bounds for create and alter.

#### Storage

Persist articles through EntityStore on H2, MySQL and PostgreSQL. Glossary T7 introduces the
shared `domain_member_rel` schema and mapper as a standalone storage change. Article T2 reuses
them and supplies the Article tables below. Use the existing Owner and audit storage.

| Table                      | Fields beyond common row metadata                                                                                                                                          | Unique key                                         | Other index                                                                    |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------- | ------------------------------------------------------------------------------ |
| `kb_article_meta`          | `article_id`, `metalake_id`, `article_name`, `title`, `body_format`, `body_content`, `revision`, `status`, `article_comment`, `synonyms`, `source_reference`, `properties` | `(metalake_id, article_name, deleted_at)`          | Primary key: `article_id`                                                      |
| `kb_article_relation_meta` | `id`, `metalake_id`, `article_id`, `target_id`, `target_type`                                                                                                              | `(article_id, target_id, target_type, deleted_at)` | `(target_id, target_type, deleted_at)`; `(metalake_id, deleted_at)`            |
| `domain_member_rel`        | `id`, `metalake_id`, `domain_id`, `member_type`, `member_id`                                                                                                               | `(member_type, member_id, domain_id, deleted_at)`  | `(domain_id, deleted_at, member_type, member_id)`; `(metalake_id, deleted_at)` |

- `kb_article_meta` has one current row per Article. Title, format and body are stored directly
  on that row. Article IDs are stable and never reused, including after same-name recreation.
  There is no content-version table, version pointer or historical body copy.
- Article reads use dedicated relational queries and bypass the generic EntityStore cache for
  full Article entities. Authorization lookups retain the shared authorizer's cache behavior.
- Use numeric IDs, `audit_info`, `deleted_at`, and `utf8mb4_bin` on MySQL. The Article ID is the
  entity primary key; relation rows have an auto-increment `id`. Relation key columns are non-NULL.
- `revision` starts at 1 and advances once per effective Article edit, including content,
  status, descriptive-field and explicit membership/binding changes. It tracks writes to the
  current row. Failed writes, no-ops, Owner transfers and background orphan cleanup leave it
  unchanged. Relation rows retain the usual store metadata columns.
- `kb_article_relation_meta.metalake_id` is non-NULL and comes from the owning Article.
  Metalake cleanup uses `(metalake_id, deleted_at)` without joining Article rows. Term and asset
  filters select live bindings through `(target_id, target_type, deleted_at)`.
- For name, title, comment and source reference, use `VARCHAR(128)`, `VARCHAR(512)`,
  `VARCHAR(256)` and `VARCHAR(1024)` respectively on MySQL and PostgreSQL. On H2, use
  `VARCHAR(256)`, `VARCHAR(1024)`, `VARCHAR(512)` and `VARCHAR(2048)` respectively, allowing two
  UTF-16 code units per code point. REST and the Java and Python clients use the same code-point
  limits on every backend. Name length is checked after NFC normalization.
- Use `VARCHAR(32)` for status and format and `VARCHAR(64)` for target and member types.
  On MySQL, body, properties and audit JSON use `MEDIUMTEXT` and synonyms use `TEXT`.
  Other databases use their native equivalents.
- Relation rows hold current Domain memberships and bindings. DTOs resolve endpoint IDs to
  current names and apply visibility filtering. Services validate and fence endpoints in the
  transaction without database foreign keys.
- `domain_member_rel` contains only memberships in real Domains. `member_type` is
  `GLOSSARY_TERM` or `KNOWLEDGE_BASE_ARTICLE`. Member reads, writes and deletion match both
  `member_type` and `member_id`. Validate the member and Domain against `metalake_id`.
  Domain filters and nonempty checks read the membership table.
- Add schemas and upgrade scripts for all three dialects. Use the 2.0.0 scripts while unreleased;
  otherwise use the next release. `IF NOT EXISTS` does not make a whole upgrade safely rerunnable.
- Keep one transactional write boundary for current Article fields, relation changes and any
  storage extensions. An extension can capture the old row and full relation sets before
  replacement and commit its own records in that transaction, without changing the shared
  Article schema. Post-commit listeners are not a substitute for transactional storage work.

### Status and current content

The ordinary-read privileges in this table apply when authorization is enabled. See
[Lists and search](#lists-and-search) for the Term and asset filters.

| Article status | Ordinary reads                       | Lists filtered by Term or asset    | MCP      |
| -------------- | ------------------------------------ | ---------------------------------- | -------- |
| `DRAFT`        | Current owner with CREATE, or MANAGE | Excluded                           | Excluded |
| `PUBLISHED`    | Effective VIEW                       | Included                           | Included |
| `DEPRECATED`   | Effective VIEW                       | Only with `includeDeprecated=true` | Excluded |

`PUBLISHED` denotes direct publication, matching Domain terminology. New Articles default to it.
Create and `updateStatus` may set any of the three statuses without an approval step. An Article
can be edited or deleted in any status with the normal privileges.

An edit updates the current row and changed relation sets atomically. Read the complete stored
state before applying deltas, preserving unrelated fields and hidden memberships/bindings.
Body replacement keeps the current Article status and becomes effective immediately. Ordinary
reads use the current row and current relations. Previous body, title and relation states are
not retained; audit records describe operations and do not provide content restoration.

### Domain membership

An article belongs to zero or more Domains, using `domain_member_rel` rows with
`member_type = KNOWLEDGE_BASE_ARTICLE` and `member_id = article_id`. Terms use the same table with
`member_type = GLOSSARY_TERM`. Article membership is current relation state and changes
through Article operations; Term changes continue through `TermChangeHandler`. Sharing storage
does not change either feature's authorization or lifecycle rules, or add a generic membership
write API. Membership is independent of backing-tag assignments, so tag changes cannot silently
move an article.

- Requests and responses use Domain names. Resolve names to stable IDs on write and return the
  current names on read. A rename changes neither membership nor stable identity. An unknown
  Domain returns `404`.
- Follow the Domain model's two levels and shared metalake namespace. A Domain filter includes
  the visible requested Domain and only its subdomains readable by the caller. Resolve that
  visible ID set before selecting Article candidates, pagination and `hasMore`; an Article
  belonging only to hidden subdomains is excluded even if the Article itself is readable.
  Domain membership is optional at creation and may change later.
- Creating an Article with Domains or changing membership through `updateDomains` requires the
  usual Article create or edit authority and effective `MANAGE_DOMAIN` on every added or
  removed Domain. Compute changes from the full stored set, so a caller cannot remove an unseen
  membership by submitting only the filtered response. Without `expectedRevision`, concurrent
  replacements run sequentially and the later replacement wins. Returned Domain lists apply
  the ordinary visibility checks (`USE_DOMAIN` and draft visibility) without revealing hidden
  names or counts.
- A Domain administrator can explicitly detach a named Article from that Domain through the
  detach endpoint with effective `MANAGE_DOMAIN`. The operation changes only that membership
  and does not require Article edit permission. It advances the Article revision once and
  records the actor on an effective change. A missing membership is a no-op.
- `GET /domains/{domain}/definition-members` requires effective `MANAGE_DOMAIN` on the
  requested Domain. It lists live, directly assigned Article and Term members, including
  hidden drafts. Each entry contains only `type` (`KNOWLEDGE_BASE_ARTICLE` or `GLOSSARY_TERM`)
  and the current `name`. Content reads use the respective Article and Term permissions.
  Query `domain_member_rel` by Domain, resolve live names by member type and ID, and order
  by `(member_type, member_id)` before paging. This list uses the same `limit`, `offset` and
  `hasMore` rules as other lists below.
- Domain deletion follows the existing authorization, status and subdomain checks. An authorized
  attempt to delete a `PUBLISHED` Domain returns `400` with an instruction to unpublish it first.
  Once a `DRAFT` Domain passes those checks, check `domain_member_rel` for any live member type
  under the same Domain lock, using the shared Domain lookup index. A remaining membership
  returns `409 NonEmptyDomainException` without member names or counts.
  The Domain administrator can use the member list to find and explicitly detach remaining
  members before retrying the drop.
- Changing or deleting a backing tag does not change Article membership.

### Bindings and target lifecycle

| Binding         | Supported targets                                                                 |
| --------------- | --------------------------------------------------------------------------------- |
| Article → Term  | `GLOSSARY_TERM`, by stable numeric ID                                             |
| Article → Asset | Catalog, Schema, Table, View, table Column, Fileset, Topic, Model, Semantic Model |

An article may have no bindings. Bindings point to object identity, not to one target version.
Article→Term, Article→Asset and Term→Asset are independent. There is no inheritance from a table
to columns or from a term to its assets. A binding may cross catalogs or Domains.

- Adding a binding requires Article edit permission and read access to the target. A missing or
  invisible target fails the complete request; no partial binding changes are stored.
- A new Term binding requires the Term to be `APPROVED`; otherwise return `409`. Check target
  visibility before returning details of that conflict. Existing bindings remain if a Term
  becomes `DRAFT` or `DEPRECATED`; returned Term details carry its current status.
- Add/remove sets apply atomically. Duplicate adds and absent removals are no-ops; an effective
  mixed request advances the Article revision once.
- Reads resolve target identity, liveness and visibility before pagination, previews and
  continuation. Missing targets are omitted even before background cleanup; unreadable live
  targets remain stored, with no hidden-target counts in responses.
- Extend `OrphanedMetadataObjectRelationService` and its mappers to batch-soft-delete Article
  bindings to authoritatively retired targets. Target deletion does not lock or update Articles.
  Same-name replacement targets have new IDs and do not inherit old bindings.
- The sweep rechecks each relation identity and target retirement. Authorization failures,
  status changes and transient catalog errors are not retirement evidence. Cleanup is idempotent,
  takes no Article-row lock and does not change its revision or body; normal deleted-data GC
  later reclaims retired rows. Register a liveness lookup for every supported target type.
- Table-column bindings use the registered column ID and the containing table's authorization
  and endpoint fencing. Reuse platform column-identity reconciliation, including load-triggered
  reconciliation; retired column bindings are filtered on read and collected asynchronously.
  Concurrent external column rename has the inherited identity
  limitation tracked by [#14279](https://github.com/apache/gravitino/issues/14279); this design
  does not promise unconditional preservation when the platform replaces the column ID.
- Catalog cascades and other target drops use their normal endpoint lifecycle without collecting
  linked Articles in the drop transaction. Delayed or failed binding cleanup does not change a
  target-drop result; it is retried by the background sweep. External DDL can still succeed before
  local registrations are updated. Keep existing DDL responses and platform reconciliation and
  recovery behavior; a stale registration or transient catalog error must not be treated as
  confirmed target retirement. A local rollback cannot undo external DDL.
- Links written in the body are ordinary text URLs, independent of bindings. Renaming a target
  updates resolved binding names, but does not rewrite URLs in article text.

Deleting an article soft-deletes its current row, bindings and Domain memberships in one
transaction. It never deletes targets. Dropping a metalake removes articles through the normal
parent lifecycle. Removing a user does not delete their articles; the metalake owner can reassign
ownership through the existing Owner API.

### Access control

With `gravitino.authorization.enable=false`, create Articles without an initial Owner. Direct
reads and lists without a Term or asset filter can include `DRAFT` Articles subject to their
status filters. Lists filtered by Term or asset exclude drafts, and MCP only returns
`PUBLISHED` Articles.

When authorization is enabled, use the metalake privileges `VIEW_DEFINITION`,
`CREATE_DEFINITION` and `MANAGE_DEFINITIONS`, abbreviated VIEW, CREATE and MANAGE in this section.
`MANAGE_DEFINITIONS` implies CREATE, and CREATE implies VIEW. Expand those implications and
apply explicit Deny rules to every required capability. Article operations require effective VIEW.
The Domain definition member list and the dedicated detach endpoint are exceptions and require
effective `MANAGE_DOMAIN` on the requested Domain. Ownership never bypasses Deny on Article
operations.

| Operation                         | Required authority                                                          |
| --------------------------------- | --------------------------------------------------------------------------- |
| List and load                     | VIEW; drafts also require current owner with CREATE, or MANAGE              |
| Create                            | CREATE                                                                      |
| Edit, set status, change bindings | Current owner with CREATE, or MANAGE                                        |
| Change Domains in create or alter | Article create or edit authority and `MANAGE_DOMAIN` on each changed Domain |
| List Domain definition members    | `MANAGE_DOMAIN` on the requested Domain                                     |
| Detach from one Domain            | `MANAGE_DOMAIN` on that Domain, through the detach endpoint                 |
| Delete in any status              | Current owner with CREATE, or MANAGE                                        |
| Lists filtered by Term or asset   | VIEW on the Article and read access to each filter target                   |

CREATE alone cannot read another owner's draft, and ownership alone cannot replace CREATE.
Use the same Article visibility rules on direct reads, Article lists, nested results and MCP.
Shared securable-object validation rejects Article as a grant scope, covering role creation,
privilege grants and subsequent updates across the generic authorization APIs.
Administrators explicitly grant VIEW to reader roles on the metalake; there is no implicit
`PUBLIC` or anonymous read grant.

This release evaluates definition grants at the metalake, as in Glossary Step 1. Domain
membership is used for classification and filtering. When Domain-scoped grants are introduced
for Articles, use the same `*_DEFINITION` privileges, scope resolution, inheritance from
parent Domains and Deny rules as [Glossary Step 2](glossary-design.md#step-2-tasks-not-filed-yet).
Reevaluate access when Domain membership changes.

Use the shared authorizer for Article identity, Owner and group-based checks, including its
existing lookup caches and owner-change invalidation. Ownership changes become visible across
nodes through the platform's normal cache refresh mechanism. Group membership follows the
platform's authenticated-principal freshness rules; Gravitino adds no user-to-group membership store.

Ownership transfer uses the existing Owner API and `CAN_SET_OWNER` authorization. For a
metalake-scoped Article, the current Article owner (including members of an owner group) or
the metalake owner may transfer ownership. `MANAGE_DEFINITIONS` alone does not grant that right.
The recipient must be an existing user or group in the metalake, but need not hold
`CREATE_DEFINITION`. Transfer does not grant definition privileges: the new owner still needs
effective VIEW and CREATE to edit or read its draft. Owner changes use the existing owner
relation and audit path, without advancing the Article revision.

Article lists, search and nested summaries use the same visibility predicate. Hidden Articles are
excluded before pagination and do not affect `hasMore`. The Domain definition member list is an
exception. To support cleanup, it returns the type and name of directly assigned members,
including hidden drafts, to callers with effective `MANAGE_DOMAIN`. Article reads still require
the usual Article permissions. Reading an Article does not authorize its targets. Lists filtered
by Term or asset check access to the target before filtering Articles.

### Locking and writes

When authorization is enabled, require the creator to be a registered user in the metalake.
Use `OwnerMetaService` to persist the initial Owner in the same transaction as the Article
row and its initial Domain and binding relations. An Owner write failure rolls back the
creation, including the name and relations. With authorization disabled, skip the Owner write
and the registered-creator requirement.

Read the latest Article state and `revision` under the Article row lock. Check any supplied
`expectedRevision` under that lock before applying changes. Each effective write advances
`revision` once in the same transaction. Writes commit sequentially. Updates to different
fields and binding additions/removals preserve each other. A no-op leaves the revision unchanged
and emits no content-change success event. Validate identity and authorization before detecting a no-op.
Compare persisted values, treating memberships and bindings as sets; updates that cancel out
are a no-op.

Plan the complete lock set before acquiring locks. Take the metalake fence first, then bound
asset endpoint chains in their established order, then Term, Domain and Article rows in that
order. Within each row type, lock by ID. This follows the Term-before-Domain order in
[Glossary locking](glossary-design.md#locking). Revalidate the resolved identities and
relation sets under the locks. If planning discovers a stale set, restart lock planning with a
bounded retry and carry the original editing token into each attempt.

The metalake endpoint fence uses a shared lock on MySQL and PostgreSQL. H2 uses `FOR UPDATE`,
so unrelated writes that take the fence can still serialize within a metalake.

- A membership write holds a shared Domain-row lock until commit. Domain drop takes its
  exclusive lock, then checks the latest committed memberships without taking Article locks
  while holding that exclusive Domain lock. A concurrent add either commits first and blocks
  the drop, or fails because the Domain was dropped. A detach that commits first can let
  the drop proceed.
- For Article creation with Domains, `updateDomains` and the dedicated detach, recheck
  `MANAGE_DOMAIN` in the write transaction for each affected Domain after locking it. Use each
  Domain's current parent to evaluate inherited privileges. Apply changes only after all checks
  pass and hold the Domain locks through commit.
- New bindings use the target's normal endpoint fence and revalidate its identity. Target removal
  takes its existing endpoint locks without acquiring linked Article locks. Background orphan
  cleanup conditionally removes only stale binding rows and does not update Article state.
- Current Article fields and relation changes commit in one transaction.

Without `expectedRevision`, apply the requested operations to the latest Article state under the
row lock. `updateBody` replaces the body and its format. Body, Domain and synonym replacements
can overwrite intervening changes to the same field. Gravitino cannot restore an overwritten body.

### Read consistency

Read the Article identity, status, metadata, body when requested, and visible relation previews
from one database snapshot. Use the Article state in that snapshot for visibility filtering.
Use the same snapshot boundary for direct reads, Article and Domain member list pages,
and binding pages. Use a single SQL statement when it can read the required data together.
For multiple queries, use the shared
`SqlSessions.readSnapshot()` helper provided by T2. Open the snapshot transaction before
reading the root and run each query in that session. Use `REPEATABLE_READ` on MySQL and
PostgreSQL and `SERIALIZABLE` on H2. A default `READ_COMMITTED` session can see a new commit
between queries.

Reads take no write locks. An in-flight request can return the complete earlier state if a write
commits after its snapshot starts. Each later request, including a request for another page,
takes its own snapshot.

### REST API

All paths below start with `/api/metalakes/{metalake}`. Successful calls use HTTP `200` and the
normal Gravitino response envelope with `code: 0` unless stated otherwise. Errors use the
usual error envelope.

Article names are stored and returned as NFC-normalized Unicode strings, not URL-encoded strings. In every
single-Article route, clients UTF-8 percent-encode the name as one path segment; server routing
decodes it once, then the service applies NFC before name validation and lookup. Java and Python
clients must preserve Chinese and mixed-language names through
create, load, alter, delete and binding calls. Percent-encoded length is not the name length.

| Method and path                                  | Request                                                     | Response fields         | Behavior                                           |
| ------------------------------------------------ | ----------------------------------------------------------- | ----------------------- | -------------------------------------------------- |
| `POST /kb-articles`                              | Create fields below                                         | `article`               | Create current content and initial relations       |
| `GET /kb-articles`                               | List filters below                                          | `articles`, `hasMore`   | Read visible summaries only                        |
| `GET /kb-articles/{article}`                     | Binding `limit`, `offset`                                   | `article`               | Read current content with bounded binding previews |
| `PUT /kb-articles/{article}`                     | `updates` with optional `expectedRevision`                  | `article`               | Apply Article and binding changes atomically       |
| `DELETE /kb-articles/{article}`                  | No body                                                     | `DropResponse` (`200`)  | Soft-delete Article and related rows atomically    |
| `DELETE /kb-articles/{article}/domains/{domain}` | No body                                                     | `code: 0` (`200`)       | Detach the Article from this Domain                |
| `GET /kb-articles/{article}/terms`               | `limit`, `offset`                                           | `terms`, `hasMore`      | Page visible current Term bindings                 |
| `GET /kb-articles/{article}/objects`             | `limit`, `offset`                                           | `objects`, `hasMore`    | Page visible current asset bindings                |
| `GET /domains/{domain}/definition-members`       | `limit`, `offset`                                           | `members`, `hasMore`    | List names and types of direct definition members  |

#### Pagination and binding previews

Use ordinary `limit` and `offset`, without a cursor token. Article and binding lists
default to `limit=20`, `offset=0`; accept limits from 1 through 1,000 and nonnegative
offsets, rejecting invalid values with `400`. Order bindings by stable `(target_id, target_type)`.
Article summaries retain the title/name ordering described below. Apply root authorization,
status filters, target liveness and visibility before offset and limit. A page contains only readable
results, without hidden-object counts. Determine `hasMore` by finding up to `limit + 1` readable
results after the offset, returning at most `limit`; the extra readable result sets `hasMore=true`.
Do not infer continuation merely from a full page or from unreadable rows. Clients continue with
the same filters, adding the returned item count to offset. Concurrent edits can move results
between pages.

Article list pages return `articles`; binding pages return `terms` or `objects`. All
pages include `hasMore`, without a `total` or total-page count. Term entries contain
only name and current status, not full definitions or nested Term bindings; object entries use
`ArticleObjectIdentifier`. Query the relation tables for the bounded page rather than loading
the Article body or all target DTOs and then truncating them. Batch target resolution for
the selected page. No exact-count query is required for pagination.

An `ArticleSummary` embeds at most 20 readable Term entries and `termsHasMore`. A full Article
response embeds Term and object previews with `termsHasMore` and `objectsHasMore`. A load accepts
`limit` and `offset` for both binding sets, defaulting to 20 and 0, with the same maximum of 1,000.
These parameters page bindings, not the body. Each binding set can also be paged independently
through its GET endpoint. Create and alter return the full Article with the default binding
previews, including after an alter that changes only bindings.

#### Create and alter

| Create field      | Type                            | Required | Description                                          |
| ----------------- | ------------------------------- | -------- | ---------------------------------------------------- |
| `name`            | string                          | No       | Immutable name; generated when omitted               |
| `title`           | string                          | Yes      | Display title, which may be shared by other Articles |
| `body`            | `{format, content}`             | Yes      | Text body; format-specific handling and byte limit   |
| `status`          | string enum                     | No       | Defaults to `PUBLISHED`                              |
| `domains`         | string array                    | No       | Domain names; defaults to an empty set               |
| `terms`           | string array                    | No       | Approved Term names; defaults to an empty set        |
| `objects`         | `ArticleObjectIdentifier` array | No       | Asset identifiers; defaults to an empty set          |
| `comment`         | string                          | No       | Short description                                    |
| `synonyms`        | string array                    | No       | Other words for the topic; defaults to empty         |
| `sourceReference` | string                          | No       | Source text, without URL fetching                    |
| `properties`      | string map                      | No       | Custom properties; defaults to empty                 |

Example create request:

```json
{
  "name": "revenue-calculation-guide",
  "title": "Revenue Calculation Guide",
  "body": {
    "format": "MARKDOWN",
    "content": "# Revenue\n\nRevenue excludes refunds and taxes."
  },
  "domains": ["Finance"],
  "terms": ["net-revenue"],
  "objects": [
    { "type": "TABLE", "namespace": ["iceberg", "sales"], "name": "orders" }
  ]
}
```

Create, load and alter return `{ "code": 0, "article": ... }`, where `article` has the typed
fields in [Data model](#data-model), plus the bounded binding previews described above. Bound
Term entries contain name and current status; object entries use `ArticleObjectIdentifier`.
Resolve Owner information through the existing Owner model. When no Owner relation exists,
`owner` is `null`. `audit_info` follows the standard AuditInfo DTO. These responses expose
current content only.

The full `article` includes an opaque string `revisionToken` from the same snapshot as its content.
The token identifies the metalake, the stable Article ID and its current revision. Summary
and binding-page responses omit it.

Alter follows the existing `updates` convention. Supported changes are `updateTitle`,
`updateBody`, `updateDomains`, `updateStatus`, `updateComment`, `updateSynonyms`,
`updateSourceReference`, `setProperty`, `removeProperty`, `addTerms`, `removeTerms`, `addObjects`
and `removeObjects`. Domain and synonym updates replace their complete sets. Term changes carry
a `terms` array of names; object changes carry an `objects` array of `ArticleObjectIdentifier`.
Use `PUT /kb-articles/{article}` for all binding changes after creation. The `updates` array can
contain only binding changes or combine them with Article edits. Rename is unsupported. Apply a mixed
request to the full stored state and validate all requested changes before persisting them in
one transaction. An effective change advances revision once. Any validation, authorization or
storage failure leaves the body, bindings, other fields and revision unchanged.

Any alter request may send the last read's `revisionToken` in the top-level `expectedRevision`
field. After checking visibility and edit permissions, compare the token's identity and
revision with the locked Article before writes or no-op detection. A malformed token returns
`400`. A stale revision, a token from another Article or metalake, or a token for an Article
deleted and recreated under the same name returns `409 ArticleRevisionConflictException`.
The entire request fails on a conflict. A matching no-op keeps the same token.
The revision covers the entire Article. For example, a binding edit can make a subsequent
conditional body save stale, and a body edit can make a subsequent conditional binding change stale.

```json
{
  "expectedRevision": "opaque-revision-token-from-load",
  "updates": [
    {
      "@type": "updateBody",
      "newBody": { "format": "MARKDOWN", "content": "# Revenue\nIncludes refunds." }
    },
    {
      "@type": "addObjects",
      "objects": [
        { "type": "TABLE", "namespace": ["iceberg", "sales"], "name": "refunds" }
      ]
    }
  ]
}
```

#### Structured object identifiers

`ArticleObjectIdentifier` contains `type`, an ordered `namespace` array and `name`. Each component
is an exact string. Resolve them directly to stable IDs; do not concatenate and split a dotted
name through `MetadataObjects.parse`.

| Type                                                           | Namespace                  | Name              |
| -------------------------------------------------------------- | -------------------------- | ----------------- |
| `CATALOG`                                                      | `[]`                       | Catalog name      |
| `SCHEMA`                                                       | `[catalog]`                | Schema name       |
| `TABLE`, `VIEW`, `FILESET`, `TOPIC`, `MODEL`, `SEMANTIC_MODEL` | `[catalog, schema]`        | Object name       |
| `COLUMN`                                                       | `[catalog, schema, table]` | Table column name |

This conditional binding-only alter uses the column's literal name `total.amount` as one
identifier component:

```json
{
  "expectedRevision": "opaque-revision-token-from-load",
  "updates": [
    {
      "@type": "addObjects",
      "objects": [
        { "type": "COLUMN", "namespace": ["iceberg", "sales", "orders"], "name": "total.amount" }
      ]
    }
  ]
}
```

To filter the Article list by an asset, pass `objectType` and `name`, and repeat `namespace` in
component order:

```http
GET /api/metalakes/demo/kb-articles?objectType=COLUMN&namespace=iceberg&namespace=sales&namespace=orders&name=total.amount
```

Omit `namespace` for a catalog. Encode each path or query value normally; never split a decoded
component or name on `.`. An incomplete asset filter, unsupported type or invalid component
count returns `400`. View columns and individual metrics are outside the supported set.

#### Lists and search

`GET /kb-articles` accepts `q`, `domain`, `status`, `owner`, `limit`, `offset` and the target filters below.

- `q` matches a substring of the current title, a synonym or the current body, ignoring case.
  Without `q`, list summaries without reading bodies for matching. Domain scope remains optional
  in the API. Narrow candidates by any supplied Domain or binding filters before matching
  their current Article bodies.
- Body matching is a basic substring search over the stored text, including markup; an HTML
  attribute can match. The Article body and its format remain the source for editing and reads,
  subject to the existing body-validation rules. Search processing must not replace that source
  with extracted text or rewrite its format. Any future search projection is derived data.
- Use bound parameters with `LOWER(value) LIKE LOWER(?) ESCAPE '!'` on all three databases,
  following `SchemaPOStorageOps.escapeLikeMetacharacters`. Escape the input in order: `!` to
  `!!`, `%` to `!%`, then `_` to `!_`; wrap the result in `%` for substring matching. Backslash
  remains a literal character in the bound parameter. Use the explicit escape clause rather than
  default backslash escaping. Case matching follows the database's `LOWER()` behavior; test and
  document non-ASCII differences under the configured database locale.
- `domain` resolves the visible Domain and only its caller-readable subdomains to candidate
  membership IDs before SQL filtering, pagination and continuation. Hidden subdomains cannot
  contribute matching Articles or affect `hasMore`. It combines with all other filters.
- `term` takes one exact Term name, for example `GET /kb-articles?term=net-revenue&domain=Finance`.
  The asset filter uses `objectType`, `namespace` and `name` as described above. Resolve each
  supplied target to its stable ID and check read access before querying Articles. A missing
  or hidden target returns `404`. Match only live direct bindings to the supplied targets and
  apply the other list filters together with them.
- With a Term or asset filter, the list returns `PUBLISHED` Articles by default and also
  includes `DEPRECATED` when `includeDeprecated=true`. Drafts are always excluded from
  target-filtered lists. `status` selects one status from those eligible for the request.
  Without a target filter, the ordinary list includes deprecated Articles and readable drafts
  unless narrowed by `status`.
- `owner` is `user:<name>` or `group:<name>`, matching the direct owner. Group members are not
  expanded. A missing or invalid prefix is `400`.
- Sort by title, then immutable name. Use the name in each result to distinguish Articles
  that share a title. Offset paging is stable while the data is unchanged. Writes between page
  requests can shift results.
- Each `ArticleSummary` contains name, title, status, Domains, owner, the bounded Term preview
  and `termsHasMore` and audit information. Summary queries select only the required columns
  and omit `body_content` from the projection, even when `q` uses it in the search predicate.
  Batch Owner and Term lookups for the page.
- Return `{ "code": 0, "articles": [...], "hasMore": true }`. Apply Article visibility and query
  filters in SQL before pagination, and determine `hasMore` from one additional readable result.

Keep search execution behind a service boundary, separate from Article source storage and API
handling, so a dedicated document knowledge base can later provide retrieval candidates without
changing editable bodies. Preserve stable Article IDs and freshness checks for that
integration; recheck current Article visibility and status before returning candidate results.
This phase keeps a simple relational implementation, with no additional search-text column or
indexing pipeline. Large-scale document retrieval belongs in the dedicated integration.

#### Errors

| HTTP code | Meaning                                                                                           |
| --------- | ------------------------------------------------------------------------------------------------- |
| `400`     | Invalid fields, type, format, size, status or identifier components                               |
| `403`     | Visible object, but insufficient privilege for the operation                                      |
| `404`     | Missing or hidden Article, Term, asset or Domain, subject to the deletion rules below             |
| `409`     | Name conflict, Article edit token mismatch, nonapproved new Term binding, or nonempty Domain drop |
| `500`     | Unexpected internal error                                                                         |
| `502`     | Failed connection to a federated backend                                                          |
| `503`     | Configured dependency temporarily unavailable                                                     |

Article DELETE follows Domain DELETE. Return HTTP `200` with `dropped=true` when the deletion
succeeds. Return `200` with `dropped=false` when the Article is absent, already deleted or hidden
from the caller. A visible Article still requires delete permission and returns `403` if the
caller lacks it.

For the Domain definition member list and the dedicated detach endpoint, resolve the Domain and
check `MANAGE_DOMAIN` before looking up members. Return HTTP `200` with `{"code":0}` for both a
successful detach and an absent Article or membership. For ordinary Article operations, check
root visibility before action permission, and permissions before content conflicts. An unsuccessful
mutation leaves the local Article, relations and revision unchanged.
External DDL outcomes have the separate limits described in
[Bindings and target lifecycle](#bindings-and-target-lifecycle).

### Java and Python clients

Add article operations on the metalake, using typed DTOs and structured object identifiers.
The operations are:

```text
KnowledgeBaseArticleOperations
  listArticles(filter): ArticlePage
  loadArticle(name, limit = 20, offset = 0): KnowledgeBaseArticle
  createArticle(name?, title, body, domains?, status?, comment?, synonyms?,
                sourceReference?, properties?, terms?, objects?): KnowledgeBaseArticle
  alterArticle(name, changes, expectedRevision?): KnowledgeBaseArticle
  deleteArticle(name): boolean
  detachDomain(name, domain): void
  listTermBindings(name, limit = 20, offset = 0): TermBindingPage
  listAssetBindings(name, limit = 20, offset = 0): AssetBindingPage
  listArticlesForTerm(term, includeDeprecated = false, limit = 20, offset = 0): ArticlePage
  listArticlesForObject(objectIdentifier, includeDeprecated = false, limit = 20, offset = 0): ArticlePage
```

`ArticlePage` contains summaries and `hasMore`; binding pages contain their typed entries and
`hasMore`. Clients use the common default of 20 and maximum of 1,000, preserve the flags and expose
limit/offset directly.

Both clients return the `dropped` boolean from Article DELETE. Retrying a successful deletion
returns `false` if the Article remains absent.

The list filter accepts a Term name and a structured object identifier. `listArticlesForTerm`
and `listArticlesForObject` are convenience methods that send those filters to `GET /kb-articles`.

Python exposes the equivalent snake_case methods. Both clients use `alterArticle` for
binding changes and let callers pass `expectedRevision` on any alter. On
`ArticleRevisionConflictException`, reload the Article, reconcile the edit and explicitly resubmit
with the new token.

Both clients expose current Article content; content history is outside this API.

### Agent integration

Add four read-only tools to `mcp-server/`, beside the existing metadata tools:

| Tool                          | Result                                                                   |
| ----------------------------- | ------------------------------------------------------------------------ |
| `search_kb_articles`          | Summaries matching title, synonyms or body, with supported filters       |
| `get_kb_article`              | Title, body with format, visible Domains, Terms and assets               |
| `list_kb_articles_for_term`   | Summaries of articles directly bound to the Term                         |
| `list_kb_articles_for_object` | Summaries of articles directly bound to the structured object identifier |

Tools use the existing MCP credential handling for REST calls:

- HTTP requests with `Authorization` forward that credential; Gravitino authorizes its principal.
- stdio uses the configured static credential or OAuth client-credentials identity. It does not
  infer the identity of the person operating the agent.
- HTTP requests without `Authorization` follow the configured service-identity fallback policy.
  When service credentials are configured, `--no-service-identity-fallback` rejects such requests;
  otherwise the configured identity is used. Without configured credentials, existing anonymous
  request behavior applies. Static credentials retain precedence over OAuth client credentials.

All modes return only `PUBLISHED` articles, including direct `get_kb_article`. A privileged
principal does not cause a draft or deprecated root to bypass that filter. Root status checks and
returned content come from the same read snapshot. Authorization errors must not trigger an
unfiltered or more privileged retry.

When the Term or asset is known, filter the Article list by that target. Otherwise, discover
relevant Domains and browse bounded summary pages without `q`, then load selected articles with
`get_kb_article`. Use keyword search after narrowing the Domain scope when further discovery is
needed. Avoid defaulting to a knowledge-base-wide body
LIKE scan or automatically broadening an empty result into one. This is retrieval guidance for
agents; the simple REST search contract has no additional parameter or mandatory Domain check.

The Article API returns the complete stored body with its `format`. How tool output is supplied
to model APIs, including content length and context management, is an MCP implementation concern.
It does not change Article source storage or editing reads. Do not render the source body or
convert HTML to Markdown.

Tools preserve list pagination and `hasMore`. `get_kb_article` accepts the load's binding limit
and offset and preserves both binding-preview flags, so an agent can request subsequent pages
without treating a preview as complete.
Tool descriptions tell agents to read an article for explanation, then use existing metadata
tools to load the authorized Term, table or Semantic Model. Translate structured identifier
components into those tools' declared arguments; do not blindly forward binding JSON as kwargs.
An Article binding never establishes a Term→Asset relation or access to data.

### Implementation flow, events and compatibility

```text
REST / Java / Python / MCP
           |
           v
Article dispatcher: authorization -> hooks -> events
           |
           v
Article manager: validate -> plan locks -> recheck identity / state -> apply change
           |
           v
EntityStore transaction: current Article row + current relation changes + storage extensions
```

All explicit writes and deletion use the common policy boundary. The default applies changes
directly; storage extensions participate in the same transaction before commit and can inspect
old/new Article state and full relation sets. This keeps final writes atomic without introducing
history-specific columns or tables in OSS. Background orphan cleanup is separate maintenance
and changes neither Article revision nor body.

Use existing listeners for pre/success/failure list, load, create, alter and drop events.
Binding events carry added/removed targets. Audit records content, status, Domain/binding,
ownership and deletion operations. Background cleanup records system identity, retired target
IDs, removed-row counts and failures. No-ops emit no content-change success event. Existing
event delivery rules apply.

Register the Article type with Owner, audit and relation-cleanup mechanisms. Generic grant
validation rejects Article scopes. The optional body-size configuration keeps its documented
default, startup validation and UTF-8 bounds; no content-history retention setting is added.

---

## Alternatives Considered

| Approach                           | Benefit                                | Limitation and decision                                                                                         |
| ---------------------------------- | -------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| Keep only external wiki links      | No managed body storage                | Bodies cannot be searched or read through Article APIs; store current content and retain source references.     |
| Put long explanations in Terms     | Reuses Term metadata                   | An Article can explain several Terms/assets and needs its own identity, title, Owner and status.                |
| Store bodies in Tag properties     | Reuses generic metadata                | Tag privileges and association behavior do not model managed document content.                                  |
| Retain historical content in OSS   | Enables old-content reads and recovery | Adds snapshot storage and lifecycle work outside this scope; keep only current content.                         |
| Infer membership from backing tags | Reuses asset membership                | Tag operations would bypass Article authorization and transactional edits; use explicit shared membership rows. |
| Use dotted target names            | Short identifiers                      | Literal dots in components are ambiguous; use structured identifiers.                                           |

The selected model stores current Article content and explicit relations. It supports ordinary
reads and transactional edits without a historical-content lifecycle.

---

## Design Decisions

1. **Current-state storage**: keep title, format and body directly in `kb_article_meta`; current
   relation tables hold memberships and bindings. `revision` is an internal write counter.
   Earlier content is overwritten rather than retained. The transactional storage boundary
   permits extensions to add their own records without changing the Article schema.
2. **Immediate status and deletion**: default to `PUBLISHED`, allow direct status changes, and
   allow owner-with-CREATE or MANAGE editing/deletion in every status, without approval.
3. **Explicit Domain membership**: share `domain_member_rel` with Glossary. Typed keys isolate
   feature writes. Domain administrators control membership through the checks above. The drop
   check protects every member type.
4. **Explicit reader grants**: grant VIEW on the metalake; there is no implicit public read.
5. **Cross-object integration**: require Domain visibility/fencing and stable target identity.
   Reads filter retired targets and background cleanup collects stale bindings asynchronously.
6. **Basic discovery with an extension boundary**: keep substring search and original body format.
   Agents narrow through bindings and summaries. Search execution can later integrate a dedicated
   document knowledge base; implementation checks measure supported operating limits.

---

## Testing

Validation covers Article storage, authorization, lifecycle integration and REST/client/MCP
contracts.

Record the case IDs from [Task Breakdown](#task-breakdown) in the corresponding implementation
issues, and link each issue from its task entry when filed.

For release, use the same 10,000-Article fixture on MySQL and PostgreSQL, with 1% of bodies at
1 MiB and the remaining 99% averaging 10 KB. Compare a rare keyword found only in
body text with and without a Domain filter, using the same caller and page size. Record latency,
candidate count and bodies examined for each query. Document the measured operating limits for
both search modes.

### Key invariants

Storage and lifecycle operations preserve the following invariants:

1. A live Article has one current row with its stable identity, title, body format and body.
   There is no historical-content table or pointer, and same-name recreation gets a new Article ID.
2. Relation tables hold current bindings and Domain memberships. Reads omit retired or unreadable
   targets; orphan rows can remain until cleanup. Article deletion removes its relations atomically.
3. Article names are unique within a metalake. Articles may share display titles. Only actual
   memberships appear in the shared table.
4. One effective Article edit advances `revision` once. Failed writes, no-ops, Owner transfer
   and background orphan cleanup leave it unchanged.
5. Unrelated edits preserve full stored relation sets, including unreadable live targets.
   Visibility filtering changes responses, not stored content or membership.
6. Article fields, explicit relation changes and participating storage extensions commit together.
   A failure rolls back all of them. Source content remains in its stored format.

### Critical user journeys

1. **Create, edit and read**: pause a VIEW-only reader after it reads a published Article and
   before it loads bindings. In another session, save a new body, add a binding and set the
   Article to `DRAFT` in one commit. The paused read returns only the old body and bindings.
   A subsequent direct read returns `404`. Repeat for list previews, where a subsequent list
   request omits the draft.
2. **Draft, publish, deprecate and delete**: an author can change status directly. Visibility
   follows that status; deletion removes the Article and its relations, leaving targets intact.
3. **Save edits**: two editors load the same revision and submit conditional Domain replacements.
   The first effective save succeeds and the second fails before any write. Without a token,
   a replacement can overwrite the previous value, while independent deltas preserve unrelated fields.
4. **Follow target lifecycles**: renames preserve stable-ID bindings; reads omit retired targets
   before asynchronous cleanup. Same-name replacements do not inherit old bindings.
5. **Manage Domains**: readers filter by visible Domains and subdomains. Renames preserve IDs.
   A Domain administrator can list and explicitly detach members, including hidden drafts, and
   can drop the Domain once it is a draft with no members or subdomains.
6. **Ownership**: inject an initial Owner write failure with authorization enabled. Verify
   the Article and its relations roll back and creation can be retried with the same name.
   The current owner or metalake owner uses the existing Owner API to transfer ownership.
   With authorization disabled, create a draft without an Owner. Direct reads can return
   the draft, and MCP excludes it.
7. **Agent discovery**: agents narrow through Domains, summaries and direct bindings, then read
   current published content in its stored format and follow only authorized targets.

---

## Task Breakdown

Tasks are ordered by dependency. Independent tasks can proceed in parallel. REST tasks include
OpenAPI definitions and validation with `./gradlew :docs:build`.

### Prerequisites

- T1 supplies the shared metalake privileges `VIEW_DEFINITION`, `CREATE_DEFINITION` and
  `MANAGE_DEFINITIONS` before Article authorization is implemented. Reuse any definitions already
  introduced by Glossary, with the same names and implication rules. Article depends on the
  shared privilege definitions, not completion of the Glossary runtime.
- Term integration requires stable IDs, current status and registered-liveness lookup. Asset bindings are
  independent of Term integration.
- Domain integration requires endpoint fencing, hierarchy lookup and membership checks on drop.
- Readers receive explicit definition grants on the metalake.

### Group A: API and persistence

- [ ] **T1. Add Article APIs** (`api/`, DTO contracts in `common/`). Define Article, body, status,
      current-content and summary contracts, structured object identifiers, operations, changes
      and errors, including the editing token and conditional alters. Add the metadata/entity
      type integration and metalake access point. Add missing shared definition privileges
      in `Privilege.java`, or reuse them if already introduced by
      Glossary. Both features use the same definitions and implication rules from Access control.
      Depends on: none.
- [ ] **T2. Add Article storage** (`core/`, `scripts/`). Implement entities, POs, mappers and
      services for the Article tables and reuse `domain_member_rel` from Glossary T7. Bypass the
      generic entity cache for full Article reads. Add dialect upgrade scripts, atomic writes to
      current rows and relations, transactional storage-extension hooks, soft deletion and
      parent cleanup. Register the Article type for orphan-relation cleanup. Provide a shared
      mechanism for consistent reads across multiple queries.
      Depends on: T1 and the shared schema and mapper from Glossary T7.

### Group B: Core behavior

- [ ] **T3. Add the Article manager and policy boundary** (`core/`). Implement CRUD, generated
      names, NFC name normalization, bounded name-allocation retries, lock planning, sanitization,
      body-limit configuration and status. Apply atomic writes and the no-op/revision rules.
      Route Article edits and explicit drop through the common policy boundary.
      Depends on: T2.
- [ ] **T4. Add Article authorization and ownership** (`core/`, `server/`). Implement definition
      privilege implications and Deny, shared-authorizer Owner checks, group ownership, draft
      visibility and filtering for ordinary Article reads. Integrate Owner transfer and reject
      Article grant scopes through shared securable-object validation. Verify generic capability
      boundaries. Depends on: T3.
- [ ] **T5. Add Domain integration** (`core/`). Implement typed shared membership writes, parent
      filters, current-name resolution, Domain visibility and one nonempty-drop check for all
      live member types. Apply Domain management checks and permit explicit detach by the
      Domain administrator. Add Article name resolution to the shared list introduced by
      Glossary T7. Fence membership changes and verify cross-object lock order.
      For **T5.3**, create a Domain with no subdomains and use the Glossary API to add its sole
      Term member. Publish the Domain and verify that dropping it returns `400` with an
      instruction to unpublish. Unpublish it and verify `409 NonEmptyDomainException`. Verify
      that the administrator can list and detach the Term, then drop the Domain.
      Test a concurrent move of a `PUBLISHED` Domain with both a dedicated detach and an alter
      combining `updateBody` and `updateDomains` to add the Domain. For each case, pause the
      write after its initial permission check. Before it takes the Domain lock, commit a move
      that removes the caller's inherited `MANAGE_DOMAIN`. Verify that the write returns `403`
      and leaves the body, memberships and Article `revision` unchanged.
      Depends on: T4, Domain prerequisites.
- [ ] **T6. Add bindings and asynchronous cleanup** (`core/`). Resolve structured asset and Term
      targets to stable IDs. Implement binding changes, paged binding reads, Term and asset list
      filters, target authorization, approved-Term checks and read-time liveness filtering.
      Implement batched orphan collection through `OrphanedMetadataObjectRelationService` and
      column reconciliation. Preserve external DDL failure/recovery limits.
      Depends on: T4, target-specific prerequisites; coordinate lock
      ordering with T5 before release.

### Group C: REST and events

- [ ] **T7. Add Article REST and OpenAPI** (`common/`, `server/`, `docs/open-api/`). Implement all
      current-resource routes, Domain detach, binding reads, list target filters, envelopes,
      structured identifiers, error mapping, bounded offset pages/previews and conditional alters.
      For **T7.2**, verify REST error mappings and that Article DELETE returns `dropped=false`
      on retry or for a hidden draft.
      Depends on: T5, T6.
- [ ] **T8. Add filtered lists and search** (`core/`, `server/`). Implement escaped case-insensitive
      title/synonym/body matching and optional Domain/status/typed-owner/target filters,
      SQL visibility filtering, common pagination/hasMore, stable tie-break ordering and bounded
      summary queries with explicit column projections and SQL escape clauses.
      Separate search execution from source storage for future
      document knowledge-base integration; retain editable body formats.
      Depends on: T7.
- [ ] **T9. Add Article events and audit** (`core/`). Integrate lifecycle and binding dispatchers,
      authenticated actors and background-cleanup diagnostics without adding delivery guarantees.
      Depends on: T6, T7.

### Group D: Clients and agents

- [ ] **T10. Add Java client operations** (`clients/client-java/`). Expose CRUD, current content,
      bindings, list filters including Term and asset, conditional alters and mutation errors.
      Depends on: T8.
- [ ] **T11. Add Python client operations** (`clients/client-python/`). Match the Java/REST
      contracts, identifier components and ordinary alter behavior.
      Depends on: T8.
- [ ] **T12. Add read-only Article MCP tools** (`mcp-server/`). Reuse HTTP/stdio credential handling,
      reads of PUBLISHED Articles, nested visibility, stored body formats and list continuation.
      Document progressive discovery, scoped keyword search and binding-to-data-tool argument mapping.
      Depends on: T11.

### Group E: Release checks and documentation

- [ ] **T13. Add integration and release checks**. Implement the critical journeys, two-node
      races, schema upgrade/restore tests and list/search performance fixtures. For **T13.3**,
      measure summary browsing and body keyword search with and without a Domain filter, and
      document the operating limits.
      Depends on: T9, T10, T12.
- [ ] **T14. Add user documentation** (`docs/`). Document Article lifecycle, metalake grants,
      Domain membership, current content, history limitations, search, clients, MCP,
      ordinary edits, body-limit configuration and external cleanup/recovery limits.
      If the Article owner has been removed, ask the metalake owner to reassign ownership
      through the existing Owner API. `MANAGE_DEFINITIONS` alone does not grant permission
      to transfer ownership.
      Explain that draft privacy depends on enabling authorization.
      Explain that the Domain definition member list exposes the type and name of hidden drafts
      to Domain administrators for cleanup. Article reads still require the usual permissions.
      Explain that metalake `VIEW_DEFINITION` gives access to all published Articles in the
      metalake, and point to [Access control](#access-control) for the future Domain-scoped model.
      Show how to use `expectedRevision` on an alter and reconcile a conflict. Explain that
      replacements without a token can overwrite intervening changes and that Gravitino cannot
      recover overwritten body content.
      Validate examples against the APIs.
      Depends on: T10, T12, T13.
