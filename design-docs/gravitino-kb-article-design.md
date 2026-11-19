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
2. **Content and version storage**: store Markdown or HTML. Keep an internal snapshot of the
   title, body, Domains and bindings for each effective content change, and expose the current
   content only. Historical version reads are deferred.
3. **Bindings**: link an article to terms and metadata objects. Support reverse lookup from
   either target. A binding never grants access to data.
4. **Domains**: organize and filter articles by zero or more Domains. Membership does not change
   article privileges in this design.
5. **Status and privileges**: support `DRAFT`, `APPROVED` and `DEPRECATED`, with immediate edits
   and definition privileges granted on the metalake. `APPROVED` means available to agents;
   it does not record a reviewer's decision.
6. **Discovery**: search titles, synonyms and bodies, with Domain, status and owner filters.
   Read-only MCP tools return readable `APPROVED` articles.
7. **APIs and governance**: provide REST, Java and Python APIs, ownership, events and audit.
   Concurrent writes must not leave content, snapshots and effective relations out of sync.

---

## Non-Goals

1. **Review and publication approval**: changes take effect immediately. This proposal defines
   no proposal, reviewer, approval queue or review API.
2. **Domain-scoped or article-scoped definition grants**: definition privileges are granted on
   the metalake. Domains group articles; they do not authorize access to an article.
3. **Web UI and agent writes**: clients use REST; MCP tools are read-only. An article editor is
   outside this contribution.
4. **File ingestion, attachments and image upload**: the body is text. It may contain ordinary
   external links, but Gravitino does not host files, extract text or fetch linked content.
5. **Ranked, fuzzy or semantic search and RAG**: substring search is sufficient for this scope.
   There is no embedding index or answer-generation service.
6. **Inferred or transitive bindings**: words and links in the body do not create bindings.
   Binding an article to a term does not bind it to that term's assets; binding a table does not
   bind its columns.
7. **Metric bindings and version pinning**: bind a Semantic Model as an object, not an individual
   metric. A binding points to the target's identity and resolves its current state.
8. **Historical version reads, article hierarchy, comparison and rollback**: this delivery has
   no history-list or individual-version read API, including in Java and Python clients.
   Articles form a flat list; comparison and rollback are also outside this delivery.

---

## Delivery Scope and Tracking

This is a design contribution. Merging it delivers no runtime feature. The local task IDs
`T1`–`T14` in [Task Breakdown](#task-breakdown) describe implementation PRs; they are not GitHub
issue numbers and do not imply that issues have been filed.

| Capability                                      | Scope                                             | Tasks   |
| ----------------------------------------------- | ------------------------------------------------- | ------- |
| Article API, entity and four storage tables     | New first-class metadata and retained snapshots   | T1–T3   |
| Definition privileges and ownership             | Metalake grants; current-owner checks             | T4      |
| Domain membership                               | Explicit Article relations; filter and drop guard | T5      |
| Term and asset bindings                         | Stable IDs, reverse lookup and lifecycle cleanup  | T6      |
| REST, conditional editing and search            | DTOs, errors, summaries and OpenAPI               | T7–T8   |
| Events and audit                                | Existing listener and audit mechanisms            | T9      |
| Java, Python and read-only MCP                  | Same contracts and visibility as REST             | T10–T12 |
| Integration, upgrade, performance and user docs | Release checks and documented operating limits    | T13–T14 |

The Domain APIs are present on `main`. Article membership and the nonempty-Domain drop guard
still need integration work in T5. Term bindings depend on the stable IDs, status checks and
deletion hooks described in the Glossary design; that document is not evidence that the runtime
interfaces have shipped. Article storage, ordinary asset bindings and other independent work
can proceed before the Term integration is ready.

Keep the four-table layout, internal snapshots and `live_version` from the start, but defer
public history reads. This differs from Glossary Step 1's storage, which has a revision and
audit but no snapshots; both initial public APIs expose current content only. Retaining
snapshots still requires version-write, transaction and retention logic in this delivery.

Historical version lists and individual-version reads, their authorization, public DTOs and
Java/Python client methods are separate follow-up work. They can read the retained snapshots
without changing these four tables or migrating current content. No follow-up issue is created
by this document. Future history reads must apply the article's current read check and resolve
deleted target IDs without attaching them to same-name replacements.

---

## Usage Scenario

1. An admin grants an analyst `CREATE_DEFINITION` and a reader `VIEW_DEFINITION` on a metalake.
   The analyst also has access to Domain `Finance`, term `net-revenue` and table
   `iceberg.sales.orders`.
2. The analyst creates "Revenue Calculation Guide", with a Markdown body, Domain `Finance`,
   the term and the table. The server generates a name if none is supplied. The analyst becomes
   the owner; the default status is `APPROVED`, and version 1 takes effect immediately.
3. The reader searches for a word from the body and loads the article. Its bindings lead to
   the current term and table; each target still requires its own read privilege.
4. The analyst loads the article and saves a new body and another binding with its `ETag`.
   One new version becomes effective atomically. The response shows the current version;
   the previous snapshot remains in storage without a public history-read API.
5. An agent acting as the reader calls `search_kb_articles`, then `get_kb_article`. It receives
   the stored body with its format and only the bound objects it can read.
6. To prepare content privately, the analyst can create or set the article to `DRAFT`. Only its
   owner with CREATE, or a caller with MANAGE, can read it. Setting it to `APPROVED` makes it
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
  status: DRAFT | APPROVED | DEPRECATED, domains[], comment?, synonyms[],
  sourceReference?, properties, version, audit_info
}
ArticleSnapshot { articleId, version, title, body, domainIds[], bindings[], audit_info }
ArticleBinding { articleId, targetType, targetId, audit_info }
ArticleDomain  { articleId, domainId, audit_info }
```

`ArticleSnapshot` describes internal storage, not a public historical-version DTO. The Article's
`version` identifies its current content snapshot; its ETag separately guards the store revision.

- **Identity**: `metalake.articleName`. The name is immutable and unique in the metalake; the
  internal numeric article ID is stable. Add `KNOWLEDGE_BASE_ARTICLE` to `MetadataObject.Type`
  and `Entity.EntityType`, including the single-level object handling used by owner APIs.
- **Explicit name**: accept 1 to 128 Unicode code points, using letters, digits, combining marks,
  `_` and `-`, with at least one letter or digit. Reject other characters with `400`, including
  `/`, backslash, whitespace and control characters. Preserve the supplied spelling and case.
- **Generated name**: `name` is optional on create. Lower-case the title using `Locale.ROOT`,
  preserve Unicode letters, digits and combining marks, and replace each run of other characters
  with `-`. Trim leading/trailing `-` and cut to 128 Unicode code points without splitting a
  character. Chinese text is preserved: `收入计算说明` becomes `收入计算说明`, and
  `收入 Calculation Guide` becomes `收入-calculation-guide`. Do not transliterate to Pinyin.
  Use `article-<shortId>` only when the result contains no Unicode letter or digit, such as an
  emoji-only or punctuation-only title. The suffix is 8 random lower-case alphanumeric
  characters, regenerated on collision. A derived-name collision adds `-2`, `-3`, and so on,
  cutting the base further to keep the whole name within 128 code points. A duplicate explicit
  name is `409`. Changing the title later never changes the generated name.
- **Title**: required, nonblank after trimming, at most 512 characters. Compare titles after
  trimming and ignoring case, independent of database collation. Among articles that are not
  `DEPRECATED`, titles are unique within the metalake. Deprecation releases the title; leaving
  `DEPRECATED` checks it again. A conflict is `409`. Different titles can generate the same
  name, which is why generated names also need collision suffixes.
- **Owner**: the creator becomes the owner through the existing Owner APIs. Owners may be users
  or groups. Persist the relation in the existing `owner_meta` table: `metadata_object_id` is
  the stable article ID, `metadata_object_type` is `KNOWLEDGE_BASE_ARTICLE`, and `metalake_id`
  identifies its metalake. `owner_id` references the user or group ID and `owner_type` is `USER`
  or `GROUP`; owner names are resolved from `user_meta` or `group_meta`. Integrate the Article
  type with `OwnerMetaService` and the existing Owner APIs. No owner column is added to the
  article tables, and ownership is not part of a content snapshot.
- **Other fields**: `comment` is at most 256 characters. `synonyms` are trimmed, at most 128
  characters each and at most 20 per article; remove duplicates ignoring case. They need not be
  unique across articles. `sourceReference` is optional free text of at most 1024 characters,
  such as a wiki URL. It is not validated as a URL, followed or fetched. Render it as text unless
  a client separately validates it as a safe link. `properties` is a string map.

#### Body and input validation

`body.format` is `MARKDOWN` or `HTML`, stored with each version. The server sanitizes every body
on write, including raw HTML embedded in Markdown. Both formats use the same HTML allowlist:
remove scripts, active elements, event-handler attributes and unsafe URLs, including
`javascript:` and `data:` URLs and encoded variants. Preserve ordinary formatting and the safe
parts of the original content. Clients also sanitize HTML when rendering.

Limit `body.content` by UTF-8 byte length both before and after sanitization. The configurable
limit defaults to 1 MiB (1,048,576 bytes); allowed settings are 1 through 16,777,215 bytes, the
MySQL `MEDIUMTEXT` capacity. An invalid setting prevents startup. An oversized request returns
`400`, without truncation or a stored change. Use the same bounds for create and alter.

#### Storage

Persist articles through EntityStore. Add four tables to H2, MySQL and PostgreSQL schemas and
upgrade scripts; use the existing owner and audit storage.

| Table                      | Fields beyond common row metadata                                                                                                      | Unique key                                         | Other index                           |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------- | ------------------------------------- |
| `kb_article_meta`          | `article_id`, `metalake_id`, `article_name`, `status`, `live_version`, `article_comment`, `synonyms`, `source_reference`, `properties` | `(metalake_id, article_name, deleted_at)`          | Primary key: `article_id`             |
| `kb_article_version_info`  | `id`, `metalake_id`, `article_id`, `version`, `title`, `body_format`, `body_content`, `domains`, `bindings`, `version_status`          | `(article_id, version, deleted_at)`                | `(metalake_id)`                       |
| `kb_article_relation_meta` | `id`, `article_id`, `target_id`, `target_type`                                                                                         | `(article_id, target_id, target_type, deleted_at)` | `(target_id, target_type)`            |
| `kb_article_domain_rel`    | `id`, `article_id`, `domain_id`                                                                                                        | `(article_id, domain_id, deleted_at)`              | `(domain_id, deleted_at, article_id)` |

- Follow `tag_meta`, `tag_relation_meta` and `semantic_model_version_info`: numeric IDs,
  `audit_info`, `deleted_at`, and `utf8mb4_bin` for MySQL. The article ID is an entity ID; the
  other tables have an auto-increment primary key `id`. Relation key columns are non-NULL.
- Article, binding and Domain-relation rows have the usual `current_version` and `last_version`
  store columns. Version rows are locked through the article row. These store revisions are
  distinct from the article content-version number.
- Use `VARCHAR(128)` for the name, `VARCHAR(512)` for the title, `VARCHAR(256)` for the comment,
  `VARCHAR(1024)` for the source reference, `VARCHAR(32)` for status and format, and `VARCHAR(64)`
  for target type. The body, properties, bindings and audit JSON use MySQL `MEDIUMTEXT`;
  synonyms and Domain snapshots use `TEXT`. Other databases use their native equivalents.
- `domains` is a JSON array of stable Domain IDs. `bindings` is a JSON array of target type and
  stable target ID. These snapshots are authoritative; relation tables are projections used by
  filters and reverse lookups. There are no database foreign keys; services validate and fence
  endpoints in the transaction.
- `live_version` points to the effective snapshot. All creates set it; all content versions have
  `version_status=APPROVED`, even when the article's own status is `DRAFT`. The column follows the
  `semantic_model_version_info` convention and is reserved for extensions; this delivery never
  writes another value. The private `live_version` column may be nullable for extensions, but this
  implementation never writes a null live pointer.
- Add the tables to `schema-2.0.0-*.sql` and `upgrade-1.3.0-to-2.0.0-*.sql` while 2.0.0 is
  unreleased. If it is released before implementation lands, use the next version's scripts.
  Table creation uses `IF NOT EXISTS`; that does not make the whole upgrade script re-runnable.
- Snapshot JSON is private storage, not a client contract. Compatible additions are optional
  with documented defaults for older rows. An unrelated edit preserves unknown fields. Reject
  an unsupported structure before changing data. An incompatible change needs an explicit
  migration; no `schema_version` field is introduced in advance.

### Status and versions

| Article status | Ordinary reads                       | Reverse lookup                     | MCP      |
| -------------- | ------------------------------------ | ---------------------------------- | -------- |
| `DRAFT`        | Current owner with CREATE, or MANAGE | Excluded                           | Excluded |
| `APPROVED`     | Effective VIEW                       | Included                           | Included |
| `DEPRECATED`   | Effective VIEW                       | Only with `includeDeprecated=true` | Excluded |

New articles default to `APPROVED`. Create may explicitly set any of the three statuses, and
`updateStatus` may set any of them without transition rules or lifecycle endpoints. Setting a
status creates no content version. Leaving `DEPRECATED` rechecks title uniqueness; an article
may be edited or deleted in any status with the normal privileges.

The title, body, Domains and bindings are versioned together, following Semantic Model storage.
Owner, article status, comment, synonyms, source reference and properties change directly and
are audited, without a content snapshot. Properties must not be used to hide content that
belongs in the versioned fields.

- Create writes version 1 with its initial Domains and bindings.
- An effective change to one or more versioned fields writes exactly one new snapshot, using
  the highest version number plus one. It advances the live pointer and rewrites effective
  bindings and Domain memberships in the same transaction. Any failure rolls back all of them.
- Copy the complete stored snapshot before applying changes, including relations the editor
  cannot see. Filter only response data. A body-only edit must not remove hidden bindings or
  Domain memberships.
- Public loads, summaries and bindings read the current effective version only. Older snapshots
  remain internal; this delivery adds no history service operation, REST route or client method.
  Stored target and Domain IDs remain unchanged after deletion for later historical resolution.
- Keep every version until article deletion. `JDBCBackend.deleteOldVersionData` returns `0` for
  this entity type, so the normal old-version sweep does not remove history. Article deletion
  soft-deletes all its versions atomically; normal deletion garbage collection still applies.

### Domain membership

An article belongs to zero or more Domains, using explicit `kb_article_domain_rel` rows. Do not
reuse `glossary_term_domain_rel`, and do not infer Article membership from backing tags. Domain
and Glossary integration share lifecycle rules, not physical membership rows.

- Requests and responses use Domain names. Resolve names to stable IDs on write and return the
  current names on read. A rename changes neither membership nor history identity. An unknown
  Domain returns `404`.
- Follow the Domain model's two levels and shared metalake namespace. A Domain filter includes
  the Domain and its subdomains. Domain membership is optional at creation and may change later.
- Article definition privileges remain metalake-scoped. Each added or removed Domain also
  requires its ordinary access check, including `USE_DOMAIN` and draft-Domain visibility.
  Filter every returned Domain list by that check, without names or counts for hidden Domains.
- `updateDomains` replaces the complete set. Compute additions and removals from the full stored
  set and check each changed Domain. An unrelated edit preserves every membership. A caller
  cannot silently remove an unseen membership by submitting only the filtered response.
- Domain deletion must check effective Article and Term memberships under the Domain lock. Any
  remaining membership returns `409 NonEmptyDomainException`, without member names or counts.
  `MANAGE_DOMAIN` alone cannot detach articles. An authorized Article editor first removes
  membership through the Article API. Historical snapshots alone do not prevent Domain deletion.
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
- Add and remove sets apply atomically. Adding an existing binding or removing an absent one
  is a no-op and creates no version. A mixed request creates one version if its final set differs.
- Responses omit unreadable targets and any hidden-target count. Stored snapshots remain
  complete. A binding never grants or blocks access to the target.
- Rename preserves a binding when the target's registered ID is preserved. Deleting a target
  removes effective binding rows, keeps the article and leaves historical snapshots intact.
  Each affected surviving article gets one store-revision increment and an audit record per
  cleanup transaction, without a new content version. Rebuilding effective relations excludes
  deleted targets rather than resurrecting them from snapshots.
- Table-column bindings use the registered column ID and the containing table's authorization
  and endpoint fencing. Reuse the platform's column reconciliation and cleanup paths, including
  load-triggered reconciliation. Concurrent external column rename has the inherited identity
  limitation tracked by [#14279](https://github.com/apache/gravitino/issues/14279); this design
  does not promise unconditional preservation when the platform replaces the column ID.
- Store cleanup is atomic with local registered-object changes. External catalog DDL is outside
  that database transaction. Keep existing external DDL responses, record cleanup failures, and
  use the platform's reconciliation/recovery path; a local rollback cannot undo external DDL.
- Links written in the body are ordinary text URLs, independent of bindings. Renaming a target
  updates resolved binding names, but does not rewrite URLs in old or current article text.

Deleting an article soft-deletes its identity, versions, bindings and Domain memberships in one
transaction. It never deletes targets. Dropping a metalake removes articles through the normal
parent lifecycle. Removing a user does not delete their articles; MANAGE can reassign ownership.

### Access control

Use the Glossary definition privileges on the metalake: `VIEW_DEFINITION`, `CREATE_DEFINITION`
and `MANAGE_DEFINITIONS`, abbreviated VIEW, CREATE and MANAGE in this section.
`MANAGE_DEFINITIONS` implies CREATE, and CREATE implies VIEW. Expand those implications and
apply explicit Deny rules to every required capability. Every operation requires effective VIEW;
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
Definition grants on a single article are rejected. Reader roles need only VIEW; until the
platform provides an all-users role, admins grant it explicitly rather than assuming `PUBLIC`.

Resolve current article identity, owner and the authenticated principal's group IDs from the
store/principal for each owner-based decision. Do not use shared name-to-ID or owner caches for
that decision. Recheck ownership in write transactions and apply the same rule to generic Owner
transfer authorization. After a committed transfer, new requests on another server cannot use
the former owner's cached rights. Already-authorized in-flight requests are outside that
guarantee. Group membership follows the platform's authenticated-principal freshness rules;
Gravitino does not add a user-to-group membership store.

Lists, search and counts use the same visibility predicate. A hidden article must not appear in
`total`, nested summaries or candidate lists. Reading an article does not authorize its targets;
loading a reverse lookup first checks the Term or object root, then filters the articles.

### Locking and conditional editing

Every effective write advances the article row's `current_version` using the store's
compare-and-set (CAS) check. A concurrent store revision returns `409` with
`OPTIMISTIC_LOCK_CONFLICT_CODE`. A no-op advances neither the store revision nor the content
version, and emits no content-change success event. Validate identity, authorization and any
condition before detecting a no-op. Compare persisted values, treating memberships and bindings
as sets; updates that cancel out are a no-op.

Plan the complete lock set before acquiring locks. Follow the existing endpoint fences and a
single order: metalake, Domains in ID order, bound target endpoint chains in their established
order, then Article rows in ID order. Revalidate the resolved identities and relation sets under
the locks. If planning discovered a stale set, restart lock planning with a bounded retry;
never retry a failed CAS as an unconditional overwrite.

- Create, title changes and status changes away from `DEPRECATED` claim a title under an
  exclusive metalake-row lock (`SELECT ... FOR UPDATE` on `metalake_meta`). Take its final lock
  mode initially rather than upgrading a shared metalake fence. This serializes title claims
  within a metalake, including two different article IDs claiming the same title.
- A membership write holds a shared Domain-row lock until commit. Domain drop takes its
  exclusive lock, then checks the latest committed memberships, without taking Article locks
  while holding that exclusive Domain lock. Either the membership commits and drop fails, or
  drop commits and the membership write fails; neither leaves a dangling membership.
- Target removal fences the endpoint against new bindings before collecting affected articles,
  then locks Article rows in ID order. Cleanup and ordinary edits cannot lose revision increments.
- Content snapshots, the live pointer and effective relations commit in one transaction. Reads
  use a consistent store snapshot for root status, authorization and payload; they do not need
  write locks. A read that began before a concurrent commit may return the earlier complete state.

REST also accepts an optional `If-Match` header, using the last load or write's strong `ETag`.
The token is opaque and unambiguously includes stable metalake ID, article ID and store revision.
It covers all article writes, not just content versions. Names and content-version numbers do
not distinguish a deleted article from a same-name replacement with the same numeric revision.

| Request                   | Concurrency headers                    |
| ------------------------- | -------------------------------------- |
| Create or load an article | Response `ETag`                        |
| Alter or change bindings  | Request `If-Match`; response `ETag`    |
| Delete                    | Request `If-Match`; no response `ETag` |
| Lists and reverse lookups | No `ETag`; load before editing         |

Under the article lock, resolve and authorize the current identity before comparing the token,
and compare before writes or no-op detection. Missing or hidden roots return `404` first; a
malformed token is `400`, and a stale identity or revision is `412`. A failed check changes no
data, revision or success event. Tokens are concurrency guards, not credentials.

An omitted header or `If-Match: *` skips only token comparison. CAS still applies and can return
`409`; there is no automatic retry. A successful write returns the new token, including the
unchanged token for a valid no-op. `If-Match` handling is new work in this proposal and must be
implemented in the server and clients rather than assumed to exist on current resources.

### REST API

All paths below start with `/api/metalakes/{metalake}`. Domain is a field or filter, not a path
segment. Successful calls use HTTP `200` and the normal Gravitino response envelope with
`code: 0`, as the Tag APIs do. The table names the other response fields; errors use the usual
error envelope. Article and binding writes also carry the concurrency headers above.

Article names are stored and returned as Unicode strings, not URL-encoded strings. In every
single-Article route, clients UTF-8 percent-encode the name as one path segment; server routing
decodes it once. Java and Python clients must preserve Chinese and mixed-language names through
create, load, alter, delete and binding calls. Percent-encoded length is not the name length.

| Method and path                          | Request                                                     | Response fields                | Behavior                                           |
| ---------------------------------------- | ----------------------------------------------------------- | ------------------------------ | -------------------------------------------------- |
| `POST /kb-articles`                      | Create fields below                                         | `article`                      | Create identity, owner, version 1 and relations    |
| `GET /kb-articles`                       | List filters below                                          | `articles`, `total`, `hasMore` | Read visible summaries only                        |
| `GET /kb-articles/{article}`             | Binding `limit`, `offset`                                   | `article`                      | Read current content with bounded binding previews |
| `PUT /kb-articles/{article}`             | `updates` array                                             | `article`                      | Apply all changes atomically                       |
| `DELETE /kb-articles/{article}`          | No body                                                     | Existing `DropResponse`        | Soft-delete article and related rows atomically    |
| `GET /kb-articles/{article}/terms`       | `limit`, `offset`                                           | `terms`, `hasMore`             | Page visible current Term bindings                 |
| `POST /kb-articles/{article}/terms`      | `termsToAdd`, `termsToRemove`: name arrays                  | `terms`, `hasMore`             | Change bindings; return the first bounded page     |
| `GET /kb-articles/{article}/objects`     | `limit`, `offset`                                           | `objects`, `hasMore`           | Page visible current asset bindings                |
| `POST /kb-articles/{article}/objects`    | `objectsToAdd`, `objectsToRemove`: identifiers              | `objects`, `hasMore`           | Change bindings; return the first bounded page     |
| `GET /glossary/terms/{term}/kb-articles` | `includeDeprecated`, `limit`, `offset`                      | `articles`, `total`, `hasMore` | Authorize Term root, then page summaries           |
| `GET /objects/{type}/kb-articles`        | `namespace`, `name`, `includeDeprecated`, `limit`, `offset` | `articles`, `total`, `hasMore` | Authorize asset root, then page summaries          |

The two reverse lookups return only `APPROVED` articles unless `includeDeprecated=true`, which
also includes `DEPRECATED`. They never return drafts. Each returned article, Term, asset and
Domain passes its own visibility check. Reverse lookup does not traverse Term→Asset bindings.

#### Pagination and binding previews

Use ordinary `limit` and `offset`, without a cursor token. List, reverse-lookup and binding-list
reads default to `limit=100`, `offset=0`; accept limits from 1 through 10,000 and nonnegative
offsets, rejecting invalid values with `400`. Order bindings by stable `(target_id, target_type)`.
Article summaries retain the title/name ordering described below. Apply root authorization,
status filters and target visibility before offset and limit. A page contains only readable
results, without hidden-object counts. `hasMore` means another readable result exists; clients
continue with the same filters, adding the returned item count to offset. Pages describe unchanged
data; concurrent edits may move results between pages, as with the Article list.

Binding pages return `terms` or `objects` plus `hasMore`, with no total. Term entries contain
only name and current status, not full definitions or nested Term bindings; object entries use
`ArticleObjectIdentifier`. Query the relation tables for the bounded page rather than loading
the complete snapshot or all target DTOs and then truncating them. Batch target resolution for
the selected page. Article list/reverse pages retain their authorized `total` and add `hasMore`.

An `ArticleSummary` embeds at most 20 readable Term entries and `termsHasMore`. A full Article
response embeds Term and object previews with `termsHasMore` and `objectsHasMore`. A load accepts
`limit` and `offset` for both binding sets, defaulting to 20 and 0, with the same maximum of 10,000.
These parameters page bindings, not the body. Clients can instead page each set independently
through the dedicated binding endpoints. Create and alter return the default previews; binding
mutations return the first 100 readable entries of the updated set and `hasMore`, plus the
Article ETag. Pagination never changes the complete stored snapshot or the atomic mutation.

#### Create and alter

| Create field      | Type                            | Required | Description                                   |
| ----------------- | ------------------------------- | -------- | --------------------------------------------- |
| `name`            | string                          | No       | Immutable name; generated when omitted        |
| `title`           | string                          | Yes      | Unique nondeprecated display title            |
| `body`            | `{format, content}`             | Yes      | Text body, sanitized and byte-limited         |
| `status`          | string enum                     | No       | Defaults to `APPROVED`                        |
| `domains`         | string array                    | No       | Domain names; defaults to an empty set        |
| `terms`           | string array                    | No       | Approved Term names; defaults to an empty set |
| `objects`         | `ArticleObjectIdentifier` array | No       | Asset identifiers; defaults to an empty set   |
| `comment`         | string                          | No       | Short description                             |
| `synonyms`        | string array                    | No       | Other words for the topic; defaults to empty  |
| `sourceReference` | string                          | No       | Source text, without URL fetching             |
| `properties`      | string map                      | No       | Unversioned properties; defaults to empty     |

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
Owner information is resolved through the existing Owner model, not stored in the version snapshot. `audit_info`
follows the standard AuditInfo DTO. These responses expose current content only.

Alter follows the existing `updates` convention. Supported changes are `updateTitle`,
`updateBody`, `updateDomains`, `updateStatus`, `updateComment`, `updateSynonyms`,
`updateSourceReference`, `setProperty` and `removeProperty`. Domain and synonym updates replace
their complete sets. Rename is unsupported. A request changing several versioned fields
creates one version, and an unversioned-only change creates none.

```json
{
  "updates": [
    { "@type": "updateTitle", "newTitle": "Revenue Calculation and Refunds" },
    { "@type": "updateDomains", "newDomains": ["Finance", "Tax"] }
  ]
}
```

#### Structured object identifiers

`ArticleObjectIdentifier` contains `type`, an ordered `namespace` array and `name`. Each component
is an exact string. Resolve them directly to stable IDs; do not concatenate and split a dotted
name through `MetadataObjects.parse`. No existing Tag route or parser changes.

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

- `q` matches any substring of the current title, a synonym or the current body, ignoring case.
  Search the stored body, including markup: a word in an HTML attribute can match. There is no
  relevance ranking or conversion to plain text.
- Use bound parameters with `LOWER(value) LIKE LOWER(?) ESCAPE '!'` on all three databases,
  following `SchemaPOStorageOps.escapeLikeMetacharacters`. Escape the input in order: `!` to
  `!!`, `%` to `!%`, then `_` to `!_`; wrap the result in `%` for substring matching. Backslash
  remains a literal character in the bound parameter. Do not rely on database collation or
  default backslash escaping. A full-text index may replace this implementation without an
  API change.
- `domain` resolves the visible Domain and includes its subdomains. It combines with all other
  filters. `status` accepts one article status; the unfiltered list includes deprecated articles
  and those drafts the caller may read.
- `owner` is `user:<name>` or `group:<name>`, matching the direct owner. Group members are not
  expanded. A missing or invalid prefix is `400`.
- Sort by title, then immutable name. This breaks ties when a deprecated article shares a title
  with its replacement. Offset paging is stable for unchanged data, not a snapshot across
  independent requests while articles change.
- Each `ArticleSummary` contains name, title, status, Domains, owner, the bounded Term preview
  and `termsHasMore`, content version and audit information, without a body. Batch owner and Term
  lookups for the page rather than querying each article separately.
- Return `{ "code": 0, "articles": [...], "total": 57, "hasMore": true }`. `total` applies all
  filters and authorization, independently of limit and offset. It never counts hidden articles.

#### Errors

| HTTP code | Meaning                                                                                        |
| --------- | ---------------------------------------------------------------------------------------------- |
| `400`     | Invalid fields, type, format, size, status, identifier components or malformed condition token |
| `403`     | Visible object, but insufficient privilege for the operation                                   |
| `404`     | Missing or hidden article, Term, asset or Domain                                               |
| `409`     | Name/title conflict, CAS conflict, nonapproved new Term binding, or nonempty Domain drop       |
| `412`     | Stale article identity or store revision in `If-Match`                                         |
| `502`     | Backend failure, following the existing REST error conventions                                 |

Check root visibility before action permission, and permissions before content conflicts or
condition details. An unsuccessful mutation leaves the local article, snapshots, effective
relations and revision unchanged. External DDL outcomes have the separate limits described in
[Bindings and target lifecycle](#bindings-and-target-lifecycle).

### Java and Python clients

Add article operations on the metalake, using typed DTOs and structured object identifiers.
The following are operation sketches, not replacements for an existing Article interface:

```text
KnowledgeBaseArticleOperations
  listArticles(filter): ArticlePage
  loadArticle(name, limit = 20, offset = 0): KnowledgeBaseArticle
  createArticle(name?, title, body, domains?, status?, comment?, synonyms?,
                sourceReference?, properties?, terms?, objects?): KnowledgeBaseArticle
  alterArticle(name, changes...): KnowledgeBaseArticle
  deleteArticle(name): boolean
  listTermBindings(name, limit = 100, offset = 0): TermBindingPage
  alterTermBindings(name, termsToAdd, termsToRemove): TermBindingPage
  listAssetBindings(name, limit = 100, offset = 0): AssetBindingPage
  alterAssetBindings(name, assetsToAdd, assetsToRemove): AssetBindingPage
  listArticlesForTerm(term, includeDeprecated = false, limit = 100, offset = 0): ArticlePage
  listArticlesForObject(objectIdentifier, includeDeprecated = false, limit = 100, offset = 0): ArticlePage
```

`ArticlePage` contains summaries, authorized total and `hasMore`; binding pages contain their
typed entries and `hasMore`. Clients preserve the flags and expose limit/offset directly.

Python exposes the equivalent snake_case methods. Both clients retain the opaque ETag from load
or write in their Article handle, send it for conditional mutations and retain the next token
after binding writes as well as alter. Convenience calls without a loaded token may omit the
header; they retain server CAS behavior. A `412` surfaces as a concurrent-modification error
requiring reload. Neither client retries a mutation unconditionally after a conflict.

Both clients expose the current Article version but no historical-version listing or loading
method in this delivery. Those methods are added with the deferred history-read APIs.

### Agent integration

Add four read-only tools to `mcp-server/`, beside the existing metadata tools:

| Tool                          | Result                                                                   |
| ----------------------------- | ------------------------------------------------------------------------ |
| `search_kb_articles`          | Summaries matching title, synonyms or body, with supported filters       |
| `get_kb_article`              | Title, body with format, version, visible Domains, Terms and assets      |
| `list_kb_articles_for_term`   | Summaries of articles directly bound to the Term                         |
| `list_kb_articles_for_object` | Summaries of articles directly bound to the structured object identifier |

Tools use REST under the effective caller's identity and return only `APPROVED` articles,
including for direct `get_kb_article`. A privileged caller does not cause a draft or deprecated
root to bypass that filter. Root status checks and returned content come from the same read
snapshot. Authorization errors must not trigger an unfiltered or more privileged retry.

Return the sanitized body as stored, with its `format`; do not convert HTML to Markdown. Tools
preserve list pagination, total and `hasMore`. `get_kb_article` accepts the load's binding limit
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
Article manager: validate -> plan locks -> recheck identity / ETag -> apply change
           |
           v
EntityStore transaction: article row + version snapshot + effective relations
```

Route every version-producing write through a version-policy boundary whose default applies
the complete new version immediately. Route explicit article deletion through its policy
boundary too. Bindings and Domain changes must use the same path rather than editing projection
tables independently. Extensions can replace that policy without forking the manager; this
proposal introduces no extra workflow, storage table or endpoint for them. Required target
cleanup stays store maintenance and updates revisions and audit without a content version.

Use the existing listener framework for list, load, create, alter and drop pre/success/failure
events. Dedicated `AlterArticleBindings` events carry added and removed targets. Audit covers
content, status, Domain and binding changes, ownership, cleanup losses and deletion. No-op
writes emit no content-change success event. There is no new delivery or ordering guarantee.

This adds an object type, APIs and storage; existing resources keep their request contracts.
Owner and audit dispatchers must recognize the new type. Role routes reject article-scoped
definition grants.
No required configuration is added. The body limit has a default; its concrete configuration
key and documentation are delivered with T3/T14 using the ordinary OSS configuration namespace.

---

## Alternatives Considered

| Approach                                  | Benefit                               | Limitation and decision                                                                                                                                            |
| ----------------------------------------- | ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Keep only links to external wiki pages    | No article storage or editor required | Bodies cannot be searched or read through the metadata API under Article privileges. Keep source references, but store managed content.                            |
| Put long explanations in Term fields      | Reuses Term identity and privileges   | One explanation can cover several terms and assets; it needs its own title, owner, status and history. Use a separate Article entity.                              |
| Store content in Tag properties           | Reuses an existing metadata object    | Tag assignment, policy behavior and privileges do not model document content or snapshots. Reuse storage conventions, not Tag semantics.                           |
| Update the body in place, with audit      | Fewer rows and simpler writes         | Audit does not preserve the exact title, body and bindings a reader previously saw. Keep complete content snapshots from the start.                                |
| Infer Domain membership from backing tags | Matches ordinary asset membership     | Backing-tag changes could silently move articles and bypass their version history. Use explicit Article membership rows, following the selected Glossary approach. |
| Split targets from dotted full names      | Shorter URL and one string per target | A literal dot in a column or namespace component is ambiguous. Use structured components throughout the new Article APIs.                                          |

The selected model adds storage and atomic projection updates, but gives each article its own
identity and retained snapshots for future history reads. It also keeps bindings independent of
access policies and lets ordinary Domain backing-tag behavior remain unchanged.

---

## Open Questions and Decisions

1. **Version storage now, history reads later (decided)**: keep the four-table layout,
   `live_version`, snapshot writes and retention in the first implementation. Public reads and
   clients expose current content only; add historical reads separately without changing this
   schema. Unlike Glossary Step 1, internal content snapshots are retained from the start.
2. **Immediate status and deletion (decided)**: default to `APPROVED`, allow direct status changes
   and edits, and allow owner-with-CREATE or MANAGE deletion in every status. These are Article
   rules; do not copy the Glossary's MANAGE-only deletion rule.
3. **Domain membership (decided)**: use `kb_article_domain_rel`, with explicit nonempty-drop
   protection. Do not add Article rows to the Term relation table or infer them from asset tags.
4. **Default reader access (platform question)**: an all-users role is outside this design.
   Until one exists, use explicit VIEW grants on reader roles. Implementation does not depend
   on inventing `PUBLIC` or silently giving anonymous users access.
5. **Integration prerequisites (to verify in implementation)**: T5 verifies Domain fencing,
   hierarchy and deletion hooks against runtime code. T6 verifies stable Term IDs, approved-Term
   detection and deletion cleanup. Missing hooks are explicit implementation work; a merged
   design document is not a completed dependency.
6. **Search implementation (conditional decision)**: start with escaped case-insensitive
   substring queries. T13 measures the release target; if they cannot meet it, replace the
   query implementation before release while keeping its matching and permission semantics.

---

## Testing

This section defines acceptance for future implementation PRs. This design-only contribution
checks Markdown, examples, references and contract consistency; it does not implement or run the
runtime scenarios below.

### Rules and invariants

Each task's tests name its case IDs, such as `T3.1`. Negative tests assert the exact exception or
HTTP code and error type, then reload all affected local state to prove rollback. Concurrency
tests use barriers or latches, with timeouts, rather than sleeps. Storage tests run on H2,
MySQL and PostgreSQL; use Docker tags and backend selection following existing project tests.

Every storage mutation test checks these invariants for its metalake:

1. A live Article row has exactly one effective version selected by `live_version`. Every stored
   content version has a unique article/version key and `version_status=APPROVED`.
2. Effective bindings and Domain memberships equal the selected snapshot projected onto targets
   that still exist. No live relation refers to a deleted article, target or Domain.
3. Titles of nondeprecated articles are unique after the shared trimming and case comparison.
4. One effective request advances the store revision once, and advances the content version once
   if and only if it changes versioned content. Cleanup advances each surviving affected article
   once without a content version. Failed writes and no-ops advance neither.
5. An unrelated edit preserves the complete stored relation sets, including unreadable targets.
   Authorization filtering changes only the response, never the stored snapshot.
6. Retained snapshots survive old-version garbage collection and are deleted with their article.
   They preserve old target IDs rather than substituting same-name replacement objects.

### Cases per task

#### T1: API contracts

- **T1.1**: entity type, immutable identity, change types and DTO round trips.
- **T1.2**: required fields; title/comment/source lengths; 20-synonym limit and case-insensitive
  deduplication.
- **T1.3**: structured object component counts, literal dots and unsupported metric/view-column
  targets.

#### T2: Persistence

- **T2.1**: CRUD and unique live keys in all four tables on every backend.
- **T2.2**: soft deletion, name reuse, target lookup indexes and complete deletion cleanup.
- **T2.3**: old-version GC returns zero for retained Article snapshots; deletion GC still removes
  deleted data.
- **T2.4**: older snapshot defaults, unknown-field preservation and rejection of unsupported
  structures without writes.

#### T3: Manager and content

- **T3.1**: generated names preserve Chinese, other Unicode letters/digits and combining marks;
  mixed-language names and separators; code-point-safe truncation and collision suffixes;
  emoji/punctuation-only fallback; explicit-name conflicts; title edits leave the name unchanged.
  Explicit names reject slashes, backslash, whitespace and controls while accepting Unicode names.
- **T3.2**: title conflicts ignoring case/spaces; concurrent create, title change and leaving
  DEPRECATED have one successful title claim.
- **T3.3**: one version per mixed content request; no version for status/property-only changes or
  no-ops.
- **T3.4**: HTML and embedded Markdown HTML XSS vectors, encoded unsafe URLs and preserved safe
  formatting.
- **T3.5**: multibyte UTF-8 body limits before/after sanitization and startup configuration
  boundaries.
- **T3.6**: injected projection-write failure rolls back snapshot, pointer, relations and revision.

#### T4: Authorization and ownership

- **T4.1**: VIEW/CREATE/MANAGE implications, explicit Deny, user/group ownership and
  owner-without-CREATE rejection.
- **T4.2**: draft visibility on every read path; hidden counts and target details never leak.
- **T4.3**: committed ownership transfer on server 1 revokes the previous owner on new server-2
  reads, edits and Owner-transfer calls before cache refresh.
- **T4.4**: user removal keeps articles; MANAGE reassigns ownership.
- **T4.5**: article-scoped definition grants are rejected.

#### T5: Domain integration

- **T5.1**: zero/multiple Domains, parent filter, rename and current-name resolution.
- **T5.2**: hidden/draft Domains are filtered; body-only edits preserve them, while unauthorized
  membership removal fails atomically.
- **T5.3**: Article or Term membership blocks Domain drop without exposing members; MANAGE_DOMAIN
  alone cannot detach articles.
- **T5.4**: two-server membership-add/drop race and mixed Domain/Article writes obey lock ordering
  without a dangling relation. Backing-tag changes leave Article membership intact.

#### T6: Bindings and cleanup

- **T6.1**: supported asset types, approved-Term-only new bindings and target authorization.
- **T6.2**: atomic add/remove sets, duplicates and absent-removal no-ops.
- **T6.3**: Term deprecation or return to DRAFT keeps existing bindings and exposes current status
  only when readable.
- **T6.4**: rename, target deletion, column reconciliation, dotted column names and same-name target
  replacement preserve identity and retained-snapshot rules.
- **T6.5**: cleanup racing ordinary edits or new bindings leaves no dangling rows or lost revision
  increment; several losses increment each article once.
- **T6.6**: external DDL succeeds but local cleanup fails; assert actual DDL outcome, local rollback
  and recovery diagnostics separately, including the inherited column-rename limit.

#### T7: REST and conditional editing

- **T7.1**: each route's request, success envelope, validation and error mapping; no historical
  version-list or individual-version read route is registered. Chinese and mixed-language
  Article names round-trip through UTF-8 path encoding without double decoding.
  Verify page defaults/bounds, stable order and visibility-before-pagination; all load, summary
  and mutation previews are bounded and report accurate continuation without hidden counts.
- **T7.2**: stale/malformed ETag, another metalake's token and same-name replacement with an equal
  revision; hidden roots return 404 first.
- **T7.3**: stale no-op returns 412; valid no-op preserves ETag; successful writes return the next
  ETag.
- **T7.4**: omitted/`*` conditions still use CAS; two headerless writers reading the same revision
  cannot both commit.
- **T7.5**: create, alter and binding writes apply complete snapshots atomically; structured reverse
  lookup preserves literal dots.

#### T8: Lists and search

- **T8.1**: title/synonym/body matches, case, non-ASCII text, stored HTML and literal `%`, `_`, `\`
  and `!` on all databases, using an explicit `ESCAPE '!'` clause and mixed escape characters.
- **T8.2**: combined Domain/status/typed-owner filters; invalid owner prefix rejected.
- **T8.3**: summaries omit bodies, batch nested lookups and report authorized total independent of
  page size; Term previews contain only bounded names/statuses with `termsHasMore`.
- **T8.4**: title/name tie-break paging with a deprecated title reused by another article.

#### T9: Events and audit

- **T9.1**: pre/success/failure lifecycle events and binding deltas.
- **T9.2**: status, ownership, membership and cleanup audit include authenticated actor and losses;
  failed/no-op writes emit no content-change success event.

#### T10: Java client

- **T10.1**: Java CRUD, current content version, bindings, reverse reads, filters and full error
  mapping against REST; preserve limit/offset, page/preview flags and mutation-page responses;
  no public history-read operation.
- **T10.2**: ETag round trips across alter/binding writes, stale-token errors without retry, and
  structured dotted-column identifiers; Chinese and mixed-language Article names survive every
  name-addressed operation without double encoding.

#### T11: Python client

- **T11.1**: Python parity with T10.1.
- **T11.2**: token propagation, errors and exact identifier-component round trips with T10.2.

#### T12: MCP tools

- **T12.1**: all four tools use effective caller identity and return APPROVED articles only, even
  for privileged direct gets.
- **T12.2**: drafts/deprecated roots, hidden targets, Domain filtering and status changes racing
  reads; no unfiltered retry.
- **T12.3**: stored-format body, pagination/total, incomplete-result continuation and target-tool
  argument mapping; agents can page both binding sets through load's limit/offset without a token.

#### T13: Integration and release

- **T13.1**: critical journeys below on H2, MySQL and PostgreSQL.
- **T13.2**: fresh install, upgrade, backup/restore and two-server consistency.
- **T13.3**: list/search performance at the release data size, with permission filtering.

#### T14: User documentation

- **T14.1**: executable documentation examples agree with REST and clients; OpenAPI validates.
- **T14.2**: user docs cover direct publication, draft visibility, current versions, conditional
  edits, configuration, pagination and external cleanup limits; historical reads are deferred.

### Critical user journeys

Use fixed fixtures: Domain `Finance` with `Tax` and `Audit` subdomains; approved Terms
`net-revenue` and `revenue-recognition`; draft Term `churn`; deprecated Term `gmv`; table
`iceberg.sales.orders` with column `total.amount`; and a Semantic Model. Definition grants are
on the metalake. Grant Domain/target access separately.

| Principal      | Definition grants and ownership                      |
| -------------- | ---------------------------------------------------- |
| `admin`        | MANAGE; not dependent on Article ownership           |
| `author_a`     | CREATE; member of `data-eng`                         |
| `author_b`     | VIEW only initially; later CREATE for transfer tests |
| `reader`       | VIEW                                                 |
| `outsider`     | No definition grant                                  |
| `domain_admin` | Domain administration, but no Article edit grant     |

1. **Create, edit and read current content**: `author_a` creates the revenue guide with its Domain and
   initial bindings. The default is APPROVED and version 1. Search and both reverse paths find
   it as `reader`, but not `outsider`. Change title, body and Domains in one alter: version 2 and
   effective relations appear together. Reload the article and verify version 2 and its complete
   effective content. Storage tests verify retention of both snapshots; no history API is used.
2. **Draft, deprecate and delete**: create a draft and prove `reader` and another CREATE user
   cannot read it. Its owner sets APPROVED directly. Deprecate it, create a replacement using
   its old title, and get 409 when restoring the original until it has a different title. The
   owner with CREATE deletes it directly; versions and relations disappear, targets remain.
3. **Conditional editing**: two callers load token X. One save returns token Y; the other gets
   412 with X and succeeds after reload. Repeat without headers with synchronized store reads:
   one CAS commits and one fails. Delete and recreate the same name; X cannot modify it even
   when the replacement has the same numeric revision. Repeat with stale no-op requests.
4. **Term and asset lifecycle**: reject new bindings to `churn` and `gmv`. Deprecate an already
   bound Term, then return another to DRAFT: old bindings remain, new unapproved bindings fail.
   Delete a Term and a bound table; effective bindings disappear, while stored snapshots retain
   their former IDs.
   Bind and reverse-query `total.amount` without splitting the literal name. Exercise column
   rename separately from the inherited concurrent external-rename limitation.
5. **Domain lifecycle**: parent filtering finds subdomain articles. Rename Finance and verify
   current names without changing IDs. Drop Audit while it has members and get 409; a Domain
   admin cannot silently detach them. Remove memberships through authorized object APIs and
   drop the empty Domain. Stored snapshots retain its former ID. Race add against drop
   on two servers and verify either legal commit order.
6. **Ownership and Deny**: transfer to `author_b`; the former owner cannot edit. Without CREATE,
   the new owner cannot edit either. Grant CREATE, then transfer draft ownership across servers
   and verify immediate revocation on new requests. Group ownership permits the current group
   members; an explicit VIEW Deny blocks reads and writes regardless of ownership.
7. **Agent and search**: search a body word, load the article in its stored format, and follow
   only visible bindings. Exercise all four tools as reader, outsider and MANAGE. Drafts and
   deprecated articles never appear in agent results. Search case variants, a Chinese word and
   literal wildcard characters, then combine Domain, status and user/group-owner filters.

### Upgrade and release checks

- Fresh schemas create the four tables on H2, MySQL and PostgreSQL. Upgrade a 1.3.0 store with
  existing catalogs, tags and policies; that data remains unchanged and Article journeys pass.
  Take a backup before upgrade and rehearse restoring it after an injected failure. Do not
  recover by assuming the whole upgrade script is safe to rerun.
- With two nodes sharing one database, a committed content change on one node is visible as a
  complete new version on the other, following platform cache consistency. Owner-based Article
  authorization has the stricter current-store guarantee described in Access control.
- Exercise the default 1 MiB body limit, both valid configuration boundaries, invalid startup
  values and oversized UTF-8 requests on all three databases.
- On MySQL and PostgreSQL, use 10,000 articles with average body size 10 KB and 1% at 1 MiB.
  With metalake VIEW and a representative mix of inaccessible drafts and targets, a list page
  returns within 1 second and a body search within 3 seconds. If substring queries miss the
  target, replace them before release and rerun matching and authorization tests.
- Release only after all task cases, critical journeys and upgrade checks pass, including
  MySQL/PostgreSQL Docker tests and `./gradlew :docs:build` for the implemented OpenAPI changes.

---

## Task Breakdown

Each task is one implementation PR with its cases from Testing. Dependencies come first;
independent tasks may proceed in parallel once their dependencies are merged. REST PRs update
OpenAPI and run `./gradlew :docs:build`. No task in this section is delivered by this design PR.

### Prerequisites

- Verify runtime Term identity, status and cleanup contracts before merging T6's Term bindings.
  Keep asset-only work independent of missing Term code.
- Verify Domain endpoint fencing and drop integration against the current implementation in T5.
  Explicit Article membership is an extension, not existing backing-tag membership behavior.
- Use explicit metalake reader grants. An all-users role is not a release prerequisite.

### Group A: API and persistence

- [ ] **T1. Add Article APIs** (`api/`, DTO contracts in `common/`). Define Article, body, status,
      current-version and summary contracts, structured object identifiers, operations, changes
      and errors;
      add the metadata/entity type integration and metalake access point. Define the three
      metalake definition privileges with the Glossary work if they are not already present.
      Depends on: none. Cases: T1.1–T1.3.
- [ ] **T2. Add Article storage** (`core/`, `scripts/`). Implement entities, POs, mappers and
      services for all four tables and dialect upgrade scripts, atomic snapshots/projections,
      soft deletion, snapshot compatibility, retained-snapshot GC and parent cleanup.
      Depends on: T1. Cases: T2.1–T2.4.

### Group B: Core behavior

- [ ] **T3. Add the Article manager and policy boundary** (`core/`). Implement CRUD, generated
      names, title claims and lock planning, sanitization and body-limit configuration, status,
      atomic version creation, retained snapshots and no-op/revision rules. Route explicit drop
      and all version-producing writes through the common policy boundaries with immediate defaults.
      Depends on: T2. Cases: T3.1–T3.6.
- [ ] **T4. Add Article authorization and ownership** (`core/`, `server/`). Implement definition
      privilege implications and Deny, current-store owner checks, group ownership, draft
      visibility and filtering across all reads; integrate Owner transfer and reject article-scoped
      definition grants. Depends on: T3. Cases: T4.1–T4.5.
- [ ] **T5. Add Domain integration** (`core/`). Implement explicit membership writes, parent
      filters, current-name resolution, Domain visibility and nonempty-drop checks shared with
      Term membership where applicable. Fence membership changes and verify cross-object lock
      order. Depends on: T4, Domain prerequisites. Cases: T5.1–T5.4.
- [ ] **T6. Add bindings and endpoint cleanup** (`core/`). Resolve structured asset and Term
      targets to stable IDs; implement binding mutation/read/reverse paths, target authorization,
      approved-Term checks, lifecycle cleanup and column reconciliation. Preserve external DDL
      failure/recovery limits. Depends on: T4, target-specific prerequisites; coordinate lock
      ordering with T5 before release. Cases: T6.1–T6.6.

### Group C: REST and events

- [ ] **T7. Add Article REST and OpenAPI** (`common/`, `server/`, `docs/open-api/`). Implement all
      current-resource routes, binding and reverse routes, envelopes, structured identifiers,
      error mapping, bounded offset pages/previews, optional identity/revision ETag handling
      and conditional mutations.
      Depends on: T5, T6. Cases: T7.1–T7.5.
- [ ] **T8. Add filtered lists and search** (`core/`, `server/`). Implement escaped case-insensitive
      matching, Domain/status/typed-owner filters, authorized totals, stable tie-break ordering
      and bounded, batched summary lookups with explicit SQL escape clauses.
      Depends on: T7. Cases: T8.1–T8.4.
- [ ] **T9. Add Article events and audit** (`core/`). Integrate lifecycle and binding dispatchers,
      authenticated actors and cleanup-loss records without adding delivery guarantees.
      Depends on: T6, T7. Cases: T9.1–T9.2.

### Group D: Clients and agents

- [ ] **T10. Add Java client operations** (`clients/client-java/`). Expose CRUD, current versions,
      bindings, reverse lookups and filters, including ETag retention and conditional mutation errors.
      Depends on: T8. Cases: T10.1–T10.2.
- [ ] **T11. Add Python client operations** (`clients/client-python/`). Match the Java/REST
      contracts, identifier components and conditional-edit behavior.
      Depends on: T8. Cases: T11.1–T11.2.
- [ ] **T12. Add read-only Article MCP tools** (`mcp-server/`). Use effective caller identity,
      APPROVED-only snapshot reads, nested visibility, stored body formats and list continuation;
      document binding-to-data-tool argument mapping. Depends on: T11. Cases: T12.1–T12.3.

### Group E: Release checks and documentation

- [ ] **T13. Add integration and release checks**. Implement the critical journeys, two-node
      races, schema upgrade/restore tests and list/search performance fixtures. Report measured
      results and resolve any search implementation change before release.
      Depends on: T9, T10, T12. Cases: T13.1–T13.3.
- [ ] **T14. Add user documentation** (`docs/`). Document Article lifecycle, metalake grants,
      Domain membership, current versions, deferred history reads, search, clients, MCP,
      conditional edits, body-limit configuration and external cleanup/recovery limits.
      Validate examples against the APIs.
      Depends on: T10, T12, T13. Cases: T14.1–T14.2.
