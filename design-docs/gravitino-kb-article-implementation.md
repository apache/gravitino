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

# Implementation: Knowledge Base Articles

This document contains the acceptance cases and release checks for
[Knowledge Base Articles](gravitino-kb-article-design.md). Task IDs follow the design's
[Task Breakdown](gravitino-kb-article-design.md#task-breakdown).

## Test conventions

Each task's tests name its case IDs, such as `T3.1`. Negative tests assert the exact exception or
HTTP code and error type, then reload all affected local state to prove rollback. Concurrency
tests use barriers or latches, with timeouts, rather than sleeps. Storage tests run on H2,
MySQL and PostgreSQL; use Docker tags and backend selection following existing project tests.

Every storage mutation test checks the design's
[key invariants](gravitino-kb-article-design.md#key-invariants).

## Cases per task

### T1: API contracts

- **T1.1**: entity type, immutable identity, change types and DTO round trips; shared definition
  privilege names and serialized values are defined once and reused.
- **T1.2**: required fields; title/comment/source lengths; 20-synonym limit and case-insensitive
  deduplication.
- **T1.3**: structured object component counts, literal dots and unsupported metric/view-column
  targets.

### T2: Persistence

- **T2.1**: CRUD and unique live keys in the two Article tables and shared membership table on
  every backend; binary title-key comparison, expanding case conversion, multiple deprecated
  NULL keys and title-key recalculation on existing membership rows. Term rows have NULL title
  keys. With equal numeric member IDs of different types, Article reads, writes, deletion and
  title-key recalculation leave Term rows unchanged. Verify the metalake-prefixed title unique
  key and zero-ID default rows, including deprecated NULL keys; never create a Domain zero.
- **T2.2**: soft deletion, name reuse, target lookup indexes and complete deletion cleanup.
  On every backend, verify binding `metalake_id` on writes, metalake cleanup without an
  Article-table join, and both relation indexes. Reverse lookups exclude soft-deleted bindings.
  Register the Article type for generic orphan cleanup; remove residual Owner relations for
  deleted Articles while preserving relations to live Articles. Domain-liveness checks preserve
  default-scope rows; Article deletion and metalake cleanup remove them.
- **T2.3**: create writes one current row with title/format/body and revision 1. Effective edits
  update that row and advance revision once; no-ops, failed writes, Owner transfer and orphan
  cleanup leave it unchanged. Verify there are no history rows, pointers or retention settings.
- **T2.4**: a participating storage extension observes the old Article and complete relation
  sets before replacement. Inject failures in the extension and relation writes; the Article,
  revision, relations and extension records roll back together on every backend.
### T3: Manager and content

- **T3.1**: generated names preserve Chinese, other Unicode letters/digits and combining marks;
  mixed-language names and separators; code-point-safe truncation and collision suffixes;
  emoji/punctuation-only fallback; explicit-name conflicts; title edits leave the name unchanged.
  Explicit names reject slashes, backslash, whitespace and controls while accepting Unicode names.
  Canonically equivalent composed/decomposed explicit and generated names resolve to the same
  NFC spelling; validate lengths after normalization and preserve Chinese and existing case rules.
  Round-trip an explicit name of 128 U+20000 (`𠀀`) letters through create/load on H2, MySQL and
  PostgreSQL. Verify H2 name capacity is 256 UTF-16 code units, while all APIs accept at most
  128 normalized code points and reject 129.
  Race two creates for the same generated candidate: the loser retries a new suffix after full
  rollback. Cover random fallback collisions, five-retry exhaustion with `409`, immediate explicit
  conflicts and no retry for title conflicts or other errors; failed attempts leave no partial rows.
- **T3.2**: title conflicts ignoring case/spaces within a direct Domain and among unassigned
  Articles in the same metalake return `409`; equal titles in distinct scopes or different
  metalakes succeed. Cover title edits, adding the first Domain, removing the last Domain,
  deprecation releasing claims and restoration reclaiming them. Concurrent create/title change,
  Domain changes and restoration have one successful claim of the same scope/title key.
  Conflicts in any destination scope roll back all title keys, Article fields, relations and
  revision, without exposing hidden Domains. Recheck these constraints on H2, MySQL and PostgreSQL.
- **T3.3**: one revision increment per effective mixed request, including status/property changes;
  no-ops do not increment it. Current content and changed relations commit together.
- **T3.4**: unchanged Markdown storage, embedded HTML rendered as text, safe rendering of inline,
  reference and image URLs, and HTML sanitization with encoded unsafe URLs and preserved safe
  formatting. Unrelated edits or library upgrades do not rewrite stored bodies.
- **T3.5**: multibyte UTF-8 input/storage limits, HTML after sanitization and startup configuration
  boundaries.
- **T3.6**: injected relation-write failure rolls back current Article fields, relations and revision.

### T4: Authorization and ownership

- **T4.1**: VIEW/CREATE/MANAGE implications, explicit Deny, user/group ownership and
  owner-without-CREATE rejection.
- **T4.2**: draft visibility on every read path; hidden counts and target details never leak.
- **T4.3**: shared `CAN_SET_OWNER` checks allow the current Article owner or metalake owner,
  not MANAGE alone. Existing user/group recipients without CREATE can receive ownership but
  cannot edit. Cross-node Owner changes follow platform cache refresh; transfer advances no
  Article revision.
- **T4.4**: user removal keeps articles; the metalake owner reassigns ownership.
- **T4.5**: Article is rejected as a grant scope during role creation, privilege grants
  and updates through shared validation. Generic Tag/Policy associations reject Article as an
  unsupported type. Domain grants do not substitute for metalake Article definition privileges.

### T5: Domain integration

- **T5.1**: zero/multiple Domains, parent filter, rename and current-name resolution. Shared
  membership queries filter by member type, and reject members or real Domains from another
  metalake. Default rows yield an empty public Domain list and never enter Domain resolution,
  authorization, filtering, locks or nonempty checks. First/last-Domain transitions atomically
  remove/add the default row; hidden real memberships do not trigger a default row.
- **T5.2**: hidden/draft Domains are filtered; body-only edits preserve them, while unauthorized
  membership removal fails atomically. Given visible Finance, hidden draft child Tax and an
  otherwise readable published Article belonging only to Tax, a Finance filter excludes that
  Article from the page and `hasMore`, both with and without `q`. A readable child membership
  can match; hiding only the Domain in response DTOs is insufficient.
- **T5.3**: a Domain containing only Articles, only Terms, or both blocks drop through the shared
  membership check without exposing members. Removing one type leaves the other's guard intact;
  drop succeeds only after all memberships and other Domain lifecycle constraints are cleared.
  MANAGE_DOMAIN alone cannot detach articles.
- **T5.4**: two-server membership-add/drop race and mixed Domain/Article writes obey lock ordering
  without a dangling relation. Backing-tag changes leave Article membership intact.

### T6: Bindings and cleanup

- **T6.1**: supported asset types, approved-Term-only new bindings and target authorization.
- **T6.2**: atomic add/remove sets, duplicates and absent-removal no-ops.
- **T6.3**: Term deprecation or return to DRAFT keeps existing bindings and exposes current status
  only when readable.
- **T6.4**: rename and column reconciliation preserve registered identity. After target deletion,
  reads, previews and continuation omit stale bindings before the sweep;
  same-name replacements gain no bindings. Cover dotted column names and catalog cascades.
- **T6.5**: batched orphan cleanup is idempotent and safe against edits, unbind/rebind and Article
  deletion. It does not lock Article rows or change revision or current body.
  New targets and unreadable live targets survive cleanup.
- **T6.6**: a delayed or failed sweep does not fail target deletion, and retry removes stale rows.
  Transient lookup failures cause no cleanup. External DDL/local-registration divergence follows
  platform reconciliation, including the inherited column-rename limit.

### T7: REST and writes

- **T7.1**: each route's request, success envelope, validation and error mapping; no historical
  version-list or individual-version read route is registered. Chinese and mixed-language
  Article names round-trip through UTF-8 path encoding without double decoding.
  Verify page defaults/bounds, stable order and visibility-before-pagination; all load, summary
  and mutation previews are bounded and report accurate continuation without hidden counts.
  Cover default 20, limits 1 and 1,000, invalid limits/offsets, and fixed 20-entry mutation previews.
- **T7.2**: missing or hidden roots return 404; permissions are checked before content conflicts.
  A failed store CAS returns 409 without partial writes or a success event.
- **T7.3**: an effective write advances revision once; a no-op leaves it unchanged and emits no
  content-change success event. No prior body copy or public content-version field is created.
- **T7.4**: two different body replacements serialize under the Article lock; both may succeed,
  each effective save updates the current row, and the last successful save is current.
  Status/property and binding deltas preserve independent changes. Domain-set replacements run
  sequentially with the later set winning.
- **T7.5**: create, alter and binding writes apply current fields/relations atomically. Round-trip
  all four binding change types through alter. Mix `updateBody` with Term and asset adds/removals:
  success saves all changes with one revision increment; an unreadable/missing target, unapproved
  new Term or injected relation-write failure leaves body, fields, bindings and revision unchanged
  on reload, with no success event. Structured reverse lookup preserves literal dots.
  Equivalent Unicode spellings address the same Article through
  load, alter, delete, binding and generic metadata APIs, returning its NFC name.

### T8: Lists and search

- **T8.1**: title/synonym/body matches with optional Domain scope; summary browsing without `q`
  does not read bodies for matching. Cover case, non-ASCII locale behavior, stored HTML and literal
  `%`, `_`, `\` and `!` on all databases with `ESCAPE '!'`. Searches preserve source body/format.
- **T8.2**: combined Domain/status/typed-owner filters; invalid owner prefix rejected.
  Parent-Domain expansion uses only caller-readable children; repeat the T5.2 hidden-child case
  at page boundaries to verify filtering precedes pagination and continuation.
- **T8.3**: summaries omit bodies and batch nested lookups. Pages return `hasMore` without `total`;
  SQL visibility/filter predicates apply before paging, and continuation depends on an additional
  readable result. Cover empty pages, a full final page, a following readable result and hidden
  candidates that must not affect continuation. Term previews contain at most 20 names/statuses
  with `termsHasMore`.
- **T8.4**: title/name tie-break paging with equal titles across Domains, parent/child searches
  and a deprecated title reused by another article in the same Domain.

### T9: Events and audit

- **T9.1**: pre/success/failure lifecycle events and binding deltas.
- **T9.2**: status, ownership and membership audit include the authenticated actor. Orphan cleanup
  logs system identity, retired target IDs, removed-row counts and failures without an Article
  content-change event; failed/no-op writes emit no content-change success event.

### T10: Java client

- **T10.1**: Java CRUD, current content, bindings, reverse reads, filters and full error
  mapping against REST; preserve limit/offset, page/preview flags and mutation-page responses;
  no public history-read operation.
- **T10.2**: body replacement mixed with all four Term/asset binding change types,
  atomic success/failure, no-op/revision behavior and store-conflict mapping;
  structured dotted-column identifiers; Chinese and mixed-language Article names survive every
  name-addressed operation without double encoding.

### T11: Python client

- **T11.1**: Python parity with T10.1.
- **T11.2**: error handling, body saves and exact identifier-component round trips with T10.2.

### T12: MCP tools

- **T12.1**: all four tools follow existing HTTP/stdio credential selection: forwarded credentials,
  configured static/OAuth identities, HTTP fallback enabled/disabled, and anonymous handling when
  no credentials are configured. Authorization uses the selected credential; audit follows the
  platform's existing identity attribution.
  Return PUBLISHED articles only, even for privileged direct gets.
- **T12.2**: drafts/deprecated roots, hidden targets, Domain filtering and status changes racing
  reads; no unfiltered retry.
- **T12.3**: stored-format body, common pagination/hasMore, incomplete-result continuation and
  target-tool
  argument mapping; agents can page both binding sets through load's limit/offset.
  Tool descriptions and examples follow bindings/scoped summaries before selected reads, and
  avoid automatically retrying an empty result with a knowledge-base-wide body search.

### T13: Integration and release

- **T13.1**: critical journeys below on H2, MySQL and PostgreSQL.
- **T13.2**: fresh install, upgrade, backup/restore and two-server consistency.
- **T13.3**: summary browsing and Domain-scoped keyword-search performance at the
  release data size, with permission filtering and the measured candidate count.

### T14: User documentation

- **T14.1**: executable documentation examples agree with REST and clients; OpenAPI validates.
- **T14.2**: user docs cover direct publication, draft visibility, current content, ordinary
  edits, configuration, pagination and external cleanup limits; content history is not provided.

## Critical user journeys

Use fixed fixtures: Domain `Finance` with `Tax` and `Audit` subdomains; approved Terms
`net-revenue` and `revenue-recognition`; draft Term `churn`; deprecated Term `gmv`; table
`iceberg.sales.orders` with column `total.amount`; and a Semantic Model. Definition grants are
on the metalake. Grant Domain/target access separately.

| Principal      | Definition grants and ownership                      |
| -------------- | ---------------------------------------------------- |
| `admin`        | MANAGE; not dependent on Article ownership           |
| `lake_owner`   | Metalake owner; may transfer Article ownership       |
| `author_a`     | CREATE; member of `data-eng`                         |
| `author_b`     | VIEW only initially; later CREATE for transfer tests |
| `reader`       | VIEW                                                 |
| `outsider`     | No definition grant                                  |
| `domain_admin` | Domain administration, but no Article edit grant     |

1. **Create, edit and read current content**: `author_a` creates the revenue guide with its Domain
   and initial bindings. It defaults to PUBLISHED. Search and reverse lookups find it for `reader`
   but not `outsider`. Change title, body and Domains together; reload the complete new current
   state and verify the old body was overwritten without a historical-content copy.
2. **Draft, deprecate and delete**: create a draft and prove `reader` and another CREATE user
   cannot read it. Its owner sets PUBLISHED directly. Deprecate it, create a replacement in the
   same Domain using its old title, and get 409 when restoring the original until it has a
   different title. The owner with CREATE deletes it directly; current row and relations disappear,
   targets remain.
3. **Saving edits**: two callers save different body replacements. Both may succeed sequentially;
   the last successful save is current. Independent deltas preserve unrelated fields/relations.
   An unchanged save leaves revision unchanged. Inject a storage-extension failure and verify
   complete rollback of the current row, relations and extension writes.
4. **Term and asset lifecycle**: reject new bindings to `churn` and `gmv`. Deprecate an already
   bound Term, then return another to DRAFT: old bindings remain, new unapproved bindings fail.
   Pause the orphan sweep, then delete a Term and a bound table. Reads omit their bindings while
   stale relation rows still exist, and revision remains unchanged. Resume the sweep and
   verify stale rows are retired without changing the Article row or attaching same-name replacements.
   Bind and reverse-query `total.amount` without splitting the literal name. Exercise column
   rename separately from the inherited concurrent external-rename limitation.
5. **Domain lifecycle**: parent filtering finds subdomain articles. Rename Finance and verify
   current names without changing IDs. Drop Audit while it has members and get 409; a Domain
   admin cannot silently detach them. Remove memberships through authorized object APIs and
   drop the empty Domain. Race add against drop
   on two servers and verify either legal commit order.
6. **Ownership and Deny**: the current owner transfers to `author_b`; after platform cache refresh,
   the former owner cannot edit. Without CREATE, the new owner cannot edit either. Grant CREATE,
   then transfer draft ownership across servers and verify the normal Owner cache propagation.
   `admin` cannot transfer ownership using MANAGE alone; `lake_owner` can recover an article whose
   owner was removed. Group ownership permits the current group members; an explicit VIEW Deny
   blocks Article reads and writes regardless of ownership.
7. **Agent and search**: discover scoped summaries or Term/object bindings, then load selected
   articles in their stored format and follow only visible bindings. Exercise keyword search
   within `Finance`; an empty result does not trigger an automatic knowledge-base-wide body scan.
   Exercise all four tools as reader, outsider and MANAGE. Drafts and deprecated articles
   never appear in agent results. Search case variants, a Chinese word and literal wildcard
   characters, then combine Domain, status and user/group-owner filters.

## Upgrade and release checks

- Fresh schemas create the two Article tables and shared membership table once on H2, MySQL
  and PostgreSQL; integrating both features reuses the same schema and mappers. Upgrade a 1.3.0 store with
  existing catalogs, tags and policies; that data remains unchanged and Article journeys pass.
  Take a backup before upgrade and rehearse restoring it after an injected failure. Do not
  recover by assuming the whole upgrade script is safe to rerun.
- With two nodes sharing one database, a committed content change on one node is visible as the
  complete new current state on the other, following platform cache consistency. Owner-based Article
  authorization uses the shared authorizer's cache invalidation and refresh behavior.
- Exercise the default 1 MiB body limit, both valid configuration boundaries, invalid startup
  values and oversized UTF-8 requests on all three databases.
- On MySQL and PostgreSQL, use 10,000 articles with average body size 10 KB and 1% at 1 MiB.
  With metalake VIEW and a representative mix of inaccessible drafts and targets, measure list,
  summary browsing and Domain-scoped keyword-search latency, candidate counts and
  body reads. Use the results to document supported data sizes and scoped-search operating limits.
  Verify browsing without `q` does not scan bodies for matching; larger-scale document retrieval uses the future
  dedicated knowledge-base integration.
- Release only after all task cases, critical journeys and upgrade checks pass, including
  MySQL/PostgreSQL Docker tests and `./gradlew :docs:build` for the implemented OpenAPI changes.
