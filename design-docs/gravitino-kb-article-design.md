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
3. **Bindings**: link an article to terms and metadata objects. Support reverse lookup from
   either target. A binding never grants access to data.
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
2. **Domain-scoped or article-scoped definition grants**: definition privileges are granted on
   the metalake. Domains group articles; they do not authorize access to an article.
3. **Web UI, collaborative editing and agent writes**: the design provides REST APIs and read-only
   MCP tools. A web editor, stale-editor conflict detection and agent writes are deferred.
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
| Term and asset bindings                         | Stable IDs, reverse lookup and lifecycle cleanup | T6      |
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
   The analyst also has access to Domain `Finance`, term `net-revenue` and table
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
- **Title**: required, nonblank after trimming, at most 512 characters. Articles can share
  a display title within the same Domain or without a Domain. Use the immutable `name` to
  address an Article. Generated names from equal titles use the normal collision suffixes.
- **Owner**: the creator becomes the owner through the existing Owner APIs. Owners may be users
  or groups. Persist the relation in the existing `owner_meta` table: `metadata_object_id` is
  the stable article ID, `metadata_object_type` is `KNOWLEDGE_BASE_ARTICLE`, and `metalake_id`
  identifies its metalake. `owner_id` references the user or group ID and `owner_type` is `USER`
  or `GROUP`; owner names are resolved from `user_meta` or `group_meta`. Integrate the Article
  type with `OwnerMetaService` and the existing Owner APIs. No owner column is added to the
  article tables; ownership uses its own relation and audit path.
- **Other fields**: `comment` is at most 256 characters. `synonyms` are trimmed, at most 128
  characters each and at most 20 per article; remove duplicates ignoring case. They need not be
  unique across articles. `sourceReference` is optional free text of at most 1024 characters,
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
- Use numeric IDs, `audit_info`, `deleted_at`, and `utf8mb4_bin` on MySQL. The Article ID is the
  entity primary key; relation rows have an auto-increment `id`. Relation key columns are non-NULL.
- `revision` starts at 1 and advances once per effective Article edit, including content,
  status, descriptive-field and explicit membership/binding changes. It tracks writes to the
  current row. Failed writes, no-ops, Owner transfers and background orphan cleanup leave it
  unchanged. Relation rows retain the usual store metadata columns.
- `kb_article_relation_meta.metalake_id` is non-NULL and comes from the owning Article.
  Metalake cleanup uses `(metalake_id, deleted_at)` without joining Article rows. Reverse lookup
  selects live bindings through `(target_id, target_type, deleted_at)`.
- Names use `VARCHAR(128)` on MySQL/PostgreSQL and `VARCHAR(256)` on H2 to accommodate
  supplementary-plane characters stored as two UTF-16 code units. All APIs still enforce
  1–128 Unicode code points after NFC normalization. Use `VARCHAR(512)` for title,
  `VARCHAR(256)` for comment, `VARCHAR(1024)` for source reference, `VARCHAR(32)` for
  status/format, and `VARCHAR(64)` for target/member types. Body, properties and audit JSON
  use MySQL `MEDIUMTEXT`; synonyms use
  `TEXT`. Other databases use their native equivalents.
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

| Article status | Ordinary reads                       | Reverse lookup                     | MCP      |
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
- Article definition privileges remain metalake-scoped. Each added or removed Domain also
  requires its ordinary access check, including `USE_DOMAIN` and draft-Domain visibility.
  Filter every returned Domain list by that check, without names or counts for hidden Domains.
- `updateDomains` replaces the complete set. Compute additions and removals from the full stored
  set and check each changed Domain. An unrelated edit preserves every membership. A caller
  cannot silently remove an unseen membership by submitting only the filtered response.
  Concurrent replacements run sequentially and the later replacement wins; they are not merged.
- Domain deletion checks `domain_member_rel` for any live member type under the Domain lock,
  using the shared Domain lookup index and fencing concurrent membership additions. Any remaining
  membership returns `409 NonEmptyDomainException`, without member names or counts.
  `MANAGE_DOMAIN` alone cannot detach articles. An authorized Article editor first removes
  membership through the Article API.
- This extends the current Domain drop path, which already checks its own lifecycle constraints.
  Changing or deleting a backing tag does not change Article membership.

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

Use the definition privileges on the metalake: `VIEW_DEFINITION`, `CREATE_DEFINITION`
and `MANAGE_DEFINITIONS`, abbreviated VIEW, CREATE and MANAGE in this section.
`MANAGE_DEFINITIONS` implies CREATE, and CREATE implies VIEW. Expand those implications and
apply explicit Deny rules to every required capability. Every Article operation below requires effective VIEW;
ownership never bypasses Deny.

| Operation                         | Required definition privilege                                  |
| --------------------------------- | -------------------------------------------------------------- |
| List and load                     | VIEW; drafts also require current owner with CREATE, or MANAGE |
| Create                            | CREATE                                                         |
| Edit, set status, change bindings | Current owner with CREATE, or MANAGE                           |
| Change Domain membership          | Current owner with CREATE, or MANAGE; Domain checks also apply |
| Delete in any status              | Current owner with CREATE, or MANAGE                           |
| Reverse lookup                    | VIEW on the article and read access to the lookup root         |

CREATE alone cannot read another owner's draft, and ownership alone cannot replace CREATE.
Use the same rules on direct reads, lists, nested results, reverse lookups and MCP.
Shared securable-object validation rejects Article as a grant scope, covering role creation,
privilege grants and subsequent updates across the generic authorization APIs.
Administrators explicitly grant VIEW to reader roles on the metalake; there is no implicit
`PUBLIC` or anonymous read grant.

Article definition checks evaluate metalake grants only. Domain membership classifies and
filters Articles without granting or inheriting read or edit privileges. Domain-scoped grants
with the same privilege names used by other features do not satisfy Article definition checks.

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

Lists and search use the same visibility predicate. A hidden article must not appear in nested
summaries or candidate lists, or affect `hasMore`. Reading an article does not authorize its targets;
loading a reverse lookup first checks the Term or object root, then filters the articles.

### Locking and writes

Read the latest Article state and `revision` under the Article row lock. Check any supplied
`expectedRevision` under that lock before applying changes. Each effective write advances
`revision` once in the same transaction. Writes commit sequentially. Updates to different
fields and binding additions/removals preserve each other. A no-op leaves the revision unchanged
and emits no content-change success event. Validate identity and authorization before detecting a no-op.
Compare persisted values, treating memberships and bindings as sets; updates that cancel out
are a no-op.

Plan the complete lock set before acquiring locks. Follow the existing endpoint fences and a
single order: metalake, Domains in ID order, bound target endpoint chains in their established
order, then Article rows in ID order. Revalidate the resolved identities and relation sets under
the locks. If planning discovers a stale set, restart lock planning with a bounded retry and
carry the original editing token into each attempt.

The metalake endpoint fence uses a shared lock on MySQL and PostgreSQL. H2 uses `FOR UPDATE`,
so unrelated writes that take the fence can still serialize within a metalake.

- A membership write holds a shared Domain-row lock until commit. Domain drop takes its
  exclusive lock, then checks the latest committed memberships without taking Article locks
  while holding that exclusive Domain lock. Either the membership commits and drop fails, or
  drop commits and the membership write fails; neither leaves a dangling membership.
- New bindings use the target's normal endpoint fence and revalidate its identity. Target removal
  takes its existing endpoint locks without acquiring linked Article locks. Background orphan
  cleanup conditionally removes only stale binding rows and does not update Article state.
- Current Article fields and relation changes commit in one transaction. Reads
  use a consistent store snapshot for root status, authorization and payload; they do not need
  write locks. A read that began before a concurrent commit may return the earlier complete state.

An alter request containing `updateBody` replaces the complete body, including its format.
With `expectedRevision`, the entire request is conditional on the Article state the caller
read. When the token is omitted, apply the replacement under the row lock to the latest Article
state. The last successful save can overwrite someone else's changes, and Gravitino cannot
restore the overwritten body.

### REST API

All paths below start with `/api/metalakes/{metalake}`. Domain is a field or filter, not a path
segment. Successful calls use HTTP `200` and the normal Gravitino response envelope with
`code: 0`, as the Tag APIs do. The table names the other response fields; errors use the usual
error envelope.

Article names are stored and returned as NFC-normalized Unicode strings, not URL-encoded strings. In every
single-Article route, clients UTF-8 percent-encode the name as one path segment; server routing
decodes it once, then the service applies NFC before name validation and lookup. Java and Python
clients must preserve Chinese and mixed-language names through
create, load, alter, delete and binding calls. Percent-encoded length is not the name length.

| Method and path                          | Request                                                     | Response fields         | Behavior                                           |
| ---------------------------------------- | ----------------------------------------------------------- | ----------------------- | -------------------------------------------------- |
| `POST /kb-articles`                      | Create fields below                                         | `article`               | Create current content, Owner and relations        |
| `GET /kb-articles`                       | List filters below                                          | `articles`, `hasMore`   | Read visible summaries only                        |
| `GET /kb-articles/{article}`             | Binding `limit`, `offset`                                   | `article`               | Read current content with bounded binding previews |
| `PUT /kb-articles/{article}`             | `updates` with optional `expectedRevision`                  | `article`               | Apply all changes atomically                       |
| `DELETE /kb-articles/{article}`          | No body                                                     | Existing `DropResponse` | Soft-delete article and related rows atomically    |
| `GET /kb-articles/{article}/terms`       | `limit`, `offset`                                           | `terms`, `hasMore`      | Page visible current Term bindings                 |
| `POST /kb-articles/{article}/terms`      | `termsToAdd`, `termsToRemove`: name arrays                  | `terms`, `hasMore`      | Change bindings; return the first bounded page     |
| `GET /kb-articles/{article}/objects`     | `limit`, `offset`                                           | `objects`, `hasMore`    | Page visible current asset bindings                |
| `POST /kb-articles/{article}/objects`    | `objectsToAdd`, `objectsToRemove`: identifiers              | `objects`, `hasMore`    | Change bindings; return the first bounded page     |
| `GET /glossary/terms/{term}/kb-articles` | `includeDeprecated`, `limit`, `offset`                      | `articles`, `hasMore`   | Authorize Term root, then page summaries           |
| `GET /objects/{type}/kb-articles`        | `namespace`, `name`, `includeDeprecated`, `limit`, `offset` | `articles`, `hasMore`   | Authorize asset root, then page summaries          |

The two reverse lookups return only `PUBLISHED` articles unless `includeDeprecated=true`, which
also includes `DEPRECATED`. They never return drafts. Each returned article, Term, asset and
Domain passes its own visibility check. Reverse lookup does not traverse Term→Asset bindings.

#### Pagination and binding previews

Use ordinary `limit` and `offset`, without a cursor token. List, reverse-lookup and binding-list
reads default to `limit=20`, `offset=0`; accept limits from 1 through 1,000 and nonnegative
offsets, rejecting invalid values with `400`. Order bindings by stable `(target_id, target_type)`.
Article summaries retain the title/name ordering described below. Apply root authorization,
status filters, target liveness and visibility before offset and limit. A page contains only readable
results, without hidden-object counts. Determine `hasMore` by finding up to `limit + 1` readable
results after the offset, returning at most `limit`; the extra readable result sets `hasMore=true`.
Do not infer continuation merely from a full page or from unreadable rows. Clients continue with
the same filters, adding the returned item count to offset. Pages describe unchanged
data; concurrent edits may move results between pages, as with the Article list.

Article list/reverse pages return `articles`; binding pages return `terms` or `objects`. All
pages include `hasMore`, without a `total` or total-page count. Term entries contain
only name and current status, not full definitions or nested Term bindings; object entries use
`ArticleObjectIdentifier`. Query the relation tables for the bounded page rather than loading
the Article body or all target DTOs and then truncating them. Batch target resolution for
the selected page. No exact-count query is required for pagination.

An `ArticleSummary` embeds at most 20 readable Term entries and `termsHasMore`. A full Article
response embeds Term and object previews with `termsHasMore` and `objectsHasMore`. A load accepts
`limit` and `offset` for both binding sets, defaulting to 20 and 0, with the same maximum of 1,000.
These parameters page bindings, not the body. Clients can instead page each set independently
through the dedicated binding endpoints. Create and alter return the default previews; binding
mutations return the first 20 readable entries of the updated set and `hasMore`. Pagination never changes the stored relation set or the atomic mutation.

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
Owner information is resolved through the existing Owner model, stored separately from the Article row. `audit_info`
follows the standard AuditInfo DTO. These responses expose current content only.

The full `article` includes an opaque string `revisionToken` from the same snapshot as its content.
The token identifies the metalake, the stable Article ID and its current revision. Summary
and binding-page responses omit it.

Alter follows the existing `updates` convention. Supported changes are `updateTitle`,
`updateBody`, `updateDomains`, `updateStatus`, `updateComment`, `updateSynonyms`,
`updateSourceReference`, `setProperty`, `removeProperty`, `addTerms`, `removeTerms`, `addObjects`
and `removeObjects`. Domain and synonym updates replace their complete sets. Term changes carry
a `terms` array of names; object changes carry an `objects` array of `ArticleObjectIdentifier`.
Binding deltas use the same target validation, permissions and add/remove semantics as the
standalone binding endpoints, which remain available. Rename is unsupported. Apply a mixed
request to the full stored state and validate all requested changes before persisting them in
one transaction. An effective change advances revision once. Any validation, authorization or
storage failure leaves the body, bindings, other fields and revision unchanged.

For a request containing `updateBody`, clients may send the last read's `revisionToken` in the
top-level `expectedRevision` field. After checking visibility and edit permissions, compare
the token's identity and revision with the locked Article before writes or no-op detection.
A malformed token or a condition on an alter without `updateBody` returns `400`. A stale revision,
a token from another Article or metalake, or a token for a deleted Article replaced under the
same name returns `409 ArticleRevisionConflictException`. The whole request fails, including
any accompanying binding or metadata changes. A matching no-op keeps the same token.
Token checks apply only to alter requests with `updateBody`. Dedicated binding and delete routes
keep their existing request contracts. A metadata or binding edit that advances the Article's
revision also invalidates an earlier edit token.

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

For example, a column whose literal name is `total.amount` stays one name through create,
binding changes, responses, clients and MCP:

```json
{
  "objectsToAdd": [
    { "type": "COLUMN", "namespace": ["iceberg", "sales", "orders"], "name": "total.amount" }
  ],
  "objectsToRemove": []
}
```

The object reverse lookup repeats `namespace` in component order and provides `name` separately:

```http
GET /api/metalakes/demo/objects/column/kb-articles?namespace=iceberg&namespace=sales&namespace=orders&name=total.amount
```

Omit `namespace` for a catalog. Encode each path or query value normally; never split a decoded
component or name on `.`. A missing name, unsupported type or invalid component count is `400`.
View columns and individual metrics are outside the supported set.

#### Lists and search

`GET /kb-articles` accepts `q`, `domain`, `status`, `owner`, `limit` and `offset`.

- `q` matches a substring of the current title, a synonym or the current body, ignoring case.
  Without `q`, list summaries without reading bodies for matching. Domain scope remains optional
  in the API. When supplied, resolve authorized Domain membership to candidate Article IDs before
  matching their current Article bodies.
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
- `status` accepts one article status; the unfiltered list includes deprecated articles and those
  drafts the caller may read.
- `owner` is `user:<name>` or `group:<name>`, matching the direct owner. Group members are not
  expanded. A missing or invalid prefix is `400`.
- Sort by title, then immutable name. Use the name in each result to distinguish Articles
  that share a title. Offset paging is stable while the data is unchanged. Writes between page
  requests can shift results.
- Each `ArticleSummary` contains name, title, status, Domains, owner, the bounded Term preview
  and `termsHasMore` and audit information, without a body. Batch owner and Term
  lookups for the page rather than querying each article separately.
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
| `404`     | Missing or hidden article, Term, asset or Domain                                                  |
| `409`     | Name conflict, Article edit token mismatch, nonapproved new Term binding, or nonempty Domain drop |
| `500`     | Unexpected internal error                                                                         |
| `502`     | Failed connection to a federated backend                                                          |
| `503`     | Configured dependency temporarily unavailable                                                     |

Check root visibility before action permission, and permissions before content conflicts. An
unsuccessful mutation leaves the local Article, relations and revision
unchanged. External DDL outcomes have the separate limits described in
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
  listTermBindings(name, limit = 20, offset = 0): TermBindingPage
  alterTermBindings(name, termsToAdd, termsToRemove): TermBindingPage
  listAssetBindings(name, limit = 20, offset = 0): AssetBindingPage
  alterAssetBindings(name, assetsToAdd, assetsToRemove): AssetBindingPage
  listArticlesForTerm(term, includeDeprecated = false, limit = 20, offset = 0): ArticlePage
  listArticlesForObject(objectIdentifier, includeDeprecated = false, limit = 20, offset = 0): ArticlePage
```

`ArticlePage` contains summaries and `hasMore`; binding pages contain their typed entries and
`hasMore`. Clients use the common default of 20 and maximum of 1,000, preserve the flags and expose
limit/offset directly.

Python exposes the equivalent snake_case methods. Both clients return the editing token and
support optional conditional body edits, including mixed field and binding changes. On
`ArticleRevisionConflictException`, reload the Article, reconcile the edit and explicitly
resubmit with the new token.

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

Tool descriptions guide progressive retrieval: use Term/object reverse lookups when metadata
context is known. Otherwise, discover relevant Domains and browse bounded summary pages without
`q`, then load selected articles with `get_kb_article`. Use keyword search after narrowing the
Domain scope when further discovery is needed. Avoid defaulting to a knowledge-base-wide body
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
event delivery and cache consistency apply.

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
   feature writes; membership changes use the Article write path and nonempty-drop protection.
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

1. **Create, edit and read**: an author creates a published Article with Domains and bindings.
   Readers load current content; one edit atomically updates the row and current relations.
2. **Draft, publish, deprecate and delete**: an author can change status directly. Visibility
   follows that status; deletion removes the Article and its relations, leaving targets intact.
3. **Save edits**: a body replacement with a stale token fails before any write. Without a
   token, the last successful save becomes current. Independent deltas preserve unrelated fields.
4. **Follow target lifecycles**: renames preserve stable-ID bindings; reads omit retired targets
   before asynchronous cleanup. Same-name replacements do not inherit old bindings.
5. **Manage Domains**: readers filter by visible Domains/subdomains. Renames preserve IDs;
   a nonempty Domain requires authorized membership removal before deletion.
6. **Transfer ownership**: the current owner or metalake owner uses the existing Owner API.
   Definition privileges and Deny still apply, with platform cache consistency.
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
      and errors, including the editing token and conditional body edits. Add the metadata/entity
      type integration and metalake access point. Add missing shared definition privileges
      in `Privilege.java`, or reuse them if already introduced by
      Glossary. Both features use the same definitions and implication rules from Access control.
      Depends on: none.
- [ ] **T2. Add Article storage** (`core/`, `scripts/`). Implement entities, POs, mappers and
      services for the Article tables and reuse `domain_member_rel` from Glossary T7. Add dialect
      upgrade scripts, atomic writes to current rows and relations, transactional storage-extension
      hooks, soft deletion and parent cleanup. Register the Article type for orphan-relation cleanup.
      Depends on: T1 and the shared schema and mapper from Glossary T7.

### Group B: Core behavior

- [ ] **T3. Add the Article manager and policy boundary** (`core/`). Implement CRUD, generated
      names, NFC name normalization, bounded name-allocation retries, lock planning, sanitization,
      body-limit configuration and status. Apply atomic writes and the no-op/revision rules.
      Route Article edits and explicit drop through the common policy boundary.
      Depends on: T2.
- [ ] **T4. Add Article authorization and ownership** (`core/`, `server/`). Implement definition
      privilege implications and Deny, shared-authorizer Owner checks, group ownership, draft
      visibility and filtering across all reads; integrate Owner transfer and reject Article
      grant scopes through shared securable-object validation. Verify generic capability
      boundaries. Depends on: T3.
- [ ] **T5. Add Domain integration** (`core/`). Implement typed shared membership writes, parent
      filters, current-name resolution, Domain visibility and one nonempty-drop check for all
      live member types. Fence membership changes and verify cross-object lock
      order. Depends on: T4, Domain prerequisites.
- [ ] **T6. Add bindings and asynchronous cleanup** (`core/`). Resolve structured asset and Term
      targets to stable IDs; implement binding mutation/read/reverse paths, target authorization,
      approved-Term checks, read-time liveness filtering, batched orphan collection through
      `OrphanedMetadataObjectRelationService` and column reconciliation. Preserve external DDL
      failure/recovery limits. Depends on: T4, target-specific prerequisites; coordinate lock
      ordering with T5 before release.

### Group C: REST and events

- [ ] **T7. Add Article REST and OpenAPI** (`common/`, `server/`, `docs/open-api/`). Implement all
      current-resource routes, binding and reverse routes, envelopes, structured identifiers,
      error mapping, bounded offset pages/previews and conditional body edits.
      Depends on: T5, T6.
- [ ] **T8. Add filtered lists and search** (`core/`, `server/`). Implement escaped case-insensitive
      title/synonym/body matching and optional Domain/status/typed-owner filters,
      SQL visibility filtering, common pagination/hasMore, stable tie-break ordering and bounded,
      batched summary lookups with
      explicit SQL escape clauses. Separate search execution from source storage for future
      document knowledge-base integration; retain editable body formats.
      Depends on: T7.
- [ ] **T9. Add Article events and audit** (`core/`). Integrate lifecycle and binding dispatchers,
      authenticated actors and background-cleanup diagnostics without adding delivery guarantees.
      Depends on: T6, T7.

### Group D: Clients and agents

- [ ] **T10. Add Java client operations** (`clients/client-java/`). Expose CRUD, current content,
      bindings, reverse lookups, filters, conditional body saves and mutation errors.
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
      races, schema upgrade/restore tests and list/search performance fixtures. Report measured
      results and operating limits for basic discovery and keyword search with and without a Domain.
      Depends on: T9, T10, T12.
- [ ] **T14. Add user documentation** (`docs/`). Document Article lifecycle, metalake grants,
      Domain membership, current content, history limitations, search, clients, MCP,
      ordinary edits, body-limit configuration and external cleanup/recovery limits.
      If the Article owner has been removed, ask the metalake owner to reassign ownership
      through the existing Owner API. `MANAGE_DEFINITIONS` alone does not grant permission
      to transfer ownership.
      Show how to save with `expectedRevision` and reconcile a conflict. Explain that saving
      without a token can overwrite an intervening edit and that overwritten content cannot
      be recovered from Gravitino.
      Validate examples against the APIs.
      Depends on: T10, T12, T13.
