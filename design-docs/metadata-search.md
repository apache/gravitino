<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied. See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Design of Permission-Aware Metadata Search in Gravitino

## Background

Gravitino has no search. A user or agent who wants "tables about orders", "columns named
`email`" or "everything tagged `pii`" has three options, and none of them scales:

| Approach | Cost |
|----------|------|
| Walk catalogs, schemas and tables with list calls | `1 + catalogs + schemas` requests, each one a live call to the underlying catalog |
| `GET /tags/{tag}/objects` | Exact tag only, one metalake, no name or comment matching |
| Domain member listing, Glossary `nameContains` | Scoped to one domain or to glossary terms |

The crawl is what PR [#12114](https://github.com/apache/gravitino/pull/12114) implemented as an
MCP `find_metadata` tool. Its review asked for "a Gravitino Server search API backed by an indexed
metadata inventory" that defines "search scope, match mode, pagination, authorization filtering,
and consistency semantics", with the MCP tool as a thin wrapper. This design is that API.

Two facts about the current server shape it:

1. **List calls go to the live catalog.** `TableOperationDispatcher.listTables` delegates to the
   catalog. The relational store holds an object only once it has been created, loaded or imported
   through Gravitino. A search over the store therefore covers *recorded* objects, the same scope
   the Domain design accepts for member listing.
2. **Every store write already passes through the relational meta services.** Create, import,
   alter, rename, column changes and cascade drops all end in `TableMetaService`,
   `TableColumnMetaService`, `SchemaMetaService`, `CatalogMetaService` and their siblings, inside a
   database transaction.

Related: [#13575](https://github.com/apache/gravitino/discussions/13575) (trusted discovery
proposal), [#2172](https://github.com/apache/gravitino/issues/2172) (original request, closed in a
stale-issue sweep), [#13864](https://github.com/apache/gravitino/issues/13864) (Domain),
[#14913](https://github.com/apache/gravitino/issues/14913) (Glossary),
[#13313](https://github.com/apache/gravitino/issues/13313) (reconciling non-managed entities).

---

## Goals

1. **One search call**: `POST /api/metalakes/{metalake}/search` matches catalogs, schemas,
   tables, views, columns, filesets, topics and models by name and comment, in one request with a
   bounded number of database queries.
2. **Structured filters**: Results can be restricted by object type, catalog, schema, tag (with
   optional tag value, including tags inherited from a catalog or schema) and owner.
3. **No leakage**: A caller never learns the name, existence or count of an object they cannot
   load. There are no totals or facet counts, and a column is returned only if its table is
   visible.
4. **Read-your-writes consistency**: An object created, renamed, altered or dropped through
   Gravitino is searchable in its new state as soon as the write returns, on every server.
5. **Predictable ranking**: Results are ordered by documented match tiers, then by name, with
   stable cursor pagination.
6. **Client coverage**: Java client, Python client and an MCP `search_metadata` tool that
   replaces the crawl in #12114.

---

## Non-Goals

1. **Objects Gravitino has not recorded**: Tables created directly in an underlying catalog and
   never loaded through Gravitino are not found. Importing them is inventory sync, tracked with
   #13313, and listed here as a later phase.
2. **Natural-language or semantic search**: No embeddings or LLM ranking. #13575 asks for
   predictable structured search first.
3. **Classification**: Detecting PII and applying tags is #13576. Search only filters on tags
   that exist.
4. **Facet counts**: Counts per tag or catalog cannot be computed without either leaking hidden
   objects or authorizing every match. Deferred until a cheap safe method exists.
5. **Domain and glossary filters**: Domain Phase 1 adds no domain filter to other list APIs. A
   `domains` filter can be added with the same scope semantics once Domain lands.
6. **Searching properties, partitions or data**: Property values often hold locations and
   connection settings. Only names and comments are indexed.

---

## Solution Investigations

### Where the index lives

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| A. Query the existing `*_meta` and `*_version_info` tables directly | No new table | Comments are CLOBs in per-type version tables; one query per type; case-insensitive matching differs across H2, MySQL and PostgreSQL | Rejected |
| B. Embedded Lucene index on each server | Real ranking and tokenization | Each server holds its own copy that must be fed and rebuilt; divergence between servers breaks Goal 4; a new dependency | Rejected as default |
| C. Search table in the relational store, written in the same transaction as the entity | Consistent on every server at commit; no new infrastructure; cascade deletes are one statement | `LIKE` substring matching scans the table; ranking is tiered, not scored | **Chosen** |
| D. External OpenSearch | Scales to very large inventories | Another system to run and secure; async feed breaks Goal 4 | Rejected as default. The SPI below allows it. |

### How the index is kept current

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| Event listener on dispatcher events | Decoupled | Imports during `loadTable` emit no create event; async; lost events need a reconcile job | Rejected |
| Consume `entity_change_log` | Already polled by every server | Records only names and operation types, not comments or columns; listeners are best effort and not retried | Rejected |
| Write in the meta services, in the entity's transaction | Covers create, import, alter, rename and cascade drop through one code path; rolls back with the entity | Adds rows to every write | **Chosen** |

### How tags and owners are filtered

Tag and owner assignments already live in `tag_relation_meta` and `owner_meta`, keyed by object
ID and type. Copying them into the search table would require updating it on every tag and owner
change. The search query joins these tables instead, so tag and owner changes need no index
update at all.

### How authorization is applied

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| Push privileges into SQL | One query | Duplicates jCasbin, ownership, deny and inheritance rules in SQL; wrong the moment either changes | Rejected |
| Fetch ranked candidates, filter with `MetadataAuthzHelper`, refill until the page is full | Reuses the exact rules list endpoints use, including the parent-scope short circuit | A caller who can see few matches may need several batches | **Chosen**, with a scan cap |

---

## Proposal

### Data model

One table, added to `scripts/{h2,mysql,postgresql}` schema and upgrade scripts.

```
metadata_search_doc (
  object_id        BIGINT        -- entity id; column_id for columns
  object_type      VARCHAR(64)   -- CATALOG | SCHEMA | TABLE | VIEW | COLUMN | FILESET | TOPIC | MODEL
  metalake_id      BIGINT
  catalog_id       BIGINT        -- 0 for a catalog itself
  schema_id        BIGINT        -- 0 for a catalog or schema
  table_id         BIGINT        -- set for COLUMN, otherwise 0
  name_lc          VARCHAR(300)  -- lower-cased name
  comment_lc       VARCHAR(1024) -- lower-cased comment, truncated
  deleted_at       BIGINT        -- 0 while live, as in other tables
  PRIMARY KEY (object_id, object_type, deleted_at),
  INDEX (metalake_id, name_lc),
  INDEX (catalog_id), INDEX (schema_id), INDEX (table_id)
)
```

- Only leaf names are stored. Full names are built at read time by joining `catalog_meta` and
  `schema_meta`, so renaming a catalog or schema touches no search rows.
- Columns are indexed at the table's current version only. A column change replaces the table's
  column rows.
- Lower-casing happens in Java with `Locale.ROOT` before the write, so matching behaves the same
  on all three databases.
- The upgrade script backfills rows from the existing tables with `INSERT ... SELECT`.

### SPI

```java
/** Stores searchable metadata and answers ranked candidate queries. */
public interface MetadataSearchIndex {

  /** Upserts or removes documents. Called inside the entity's store transaction. */
  void apply(List<SearchDocChange> changes);

  /** Returns candidates in rank order, after the cursor, without authorization. */
  List<SearchCandidate> candidates(SearchQuery query, @Nullable SearchCursor after, int limit);
}
```

`JdbcMetadataSearchIndex` is the only implementation. An external index would also need its own
consistency story, which is why Goal 4 is stated against the default.

### REST API

#### POST /api/metalakes/{metalake}/search

`POST` is used because filters are structured and tag names and values may contain characters
that are awkward in query strings. The request does not change state.

**Request:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `query` | string | no | Text to match against names and comments. 1 to 256 characters. Omit to filter only. |
| `types` | string[] | no | Object types to return. Default: all supported. |
| `catalogs` | string[] | no | Restrict to these catalogs. |
| `schemas` | string[] | no | Restrict to these `catalog.schema` names. |
| `tags` | object[] | no | `{"name": "pii", "value": "email"}`. `value` is optional. Multiple entries are OR. |
| `owner` | object | no | `{"name": "alice", "type": "USER"}` |
| `limit` | int | no | Page size, 1 to 100. Default 20. |
| `pageToken` | string | no | Opaque cursor from a previous response. |

At least one of `query`, `tags` or `owner` is required, so a request cannot enumerate the whole
metalake.

**Response:** `200 OK`

```json
{
  "code": 0,
  "results": [
    {"type": "TABLE", "fullName": "lake.sales.orders", "comment": "One row per order",
     "match": "NAME_EXACT"},
    {"type": "COLUMN", "fullName": "lake.sales.customers.email", "comment": "",
     "match": "NAME_PREFIX"}
  ],
  "nextPageToken": "eyJ0IjoyLCJuIjoiZW1haWwiLCJpZCI6MTIzfQ",
  "incomplete": false
}
```

**Behavior:**

- The caller needs `CAN_ACCESS_METADATA` on the metalake, as for other metalake-level reads.
- Each result is checked with the same filter expression its type uses in list endpoints. A
  column needs its table to pass.
- `400` for an empty request, an unknown type, a `limit` out of range, or a malformed
  `pageToken`. A filter naming a catalog, schema, tag or owner that does not exist returns an
  empty result, not `404`, so the response does not reveal which ones exist.
- `incomplete` is `true` when the scan cap stopped the refill loop before the page was full.
  The caller can still follow `nextPageToken`.
- Pre, success and failure events are emitted through the existing listener framework.

### Matching and ranking

The query text is lower-cased and matched as a literal substring. `%` and `_` are escaped.

| Tier | Match | Index use |
|------|-------|-----------|
| 1 `NAME_EXACT` | `name_lc = q` | `(metalake_id, name_lc)` |
| 2 `NAME_PREFIX` | `name_lc LIKE 'q%'` | `(metalake_id, name_lc)` |
| 3 `NAME_CONTAINS` | `name_lc LIKE '%q%'` | scan within the metalake |
| 4 `COMMENT_CONTAINS` | `comment_lc LIKE '%q%'` | scan within the metalake |

Each object appears once, at its best tier. Within a tier, results are ordered by `name_lc`
then `object_id`, which makes the cursor `(tier, name_lc, object_id)` stable under concurrent
writes. Tiers 3 and 4 scan, which is acceptable for recorded inventories in the hundreds of
thousands. Larger deployments are the case the SPI exists for.

### Tag filter with inheritance

The filter follows the inheritance rules in `docs/tags.md`, so search agrees with what a tag
listing shows for the same object.

- **Ancestors**: an object's ancestors are its table (for a column), its schema, every
  intermediate schema in catalogs with multi-level schemas, and its catalog.
- **Name only** (`{"name": "pii"}`): matches when `tag_relation_meta` has the tag on the object
  or on any ancestor.
- **Name and value** (`{"name": "data_domain", "value": "risk"}`): matches when the *nearest*
  assignment of that tag, starting at the object itself, carries that value. A table assigned
  `risk` under a catalog assigned `finance` matches `risk` and does not match `finance`.

Ancestors are resolved in SQL from the IDs on the document and the existing schema hierarchy, not
by loading each candidate.

### Client and MCP API

| Surface | Addition |
|---------|----------|
| Java client | `GravitinoMetalake#search(MetadataSearchRequest request)` returning `MetadataSearchPage` |
| Python client | `GravitinoMetalake.search(query, types, catalogs, schemas, tags, owner, limit, page_token)` |
| MCP server | `search_metadata` tool in `mcp_server/tools/metadata.py` |

No existing signature changes.

### User process

1. An operator upgrades. The upgrade script creates `metadata_search_doc` and backfills it from
   recorded objects.
2. A user searches `{"query": "email", "types": ["COLUMN"]}` and gets the columns named or
   commented with "email" in tables they can load.
3. A steward searches `{"tags": [{"name": "pii"}], "catalogs": ["lake"]}` and sees every recorded
   object under `lake` that carries `pii`, directly or through its schema or catalog.
4. An agent calls the MCP `search_metadata` tool instead of listing catalogs and schemas, and
   receives only objects its identity may load.
5. A table renamed through Gravitino is returned under its new name on the next search, on any
   server.

### Implementation process

**Write path**

```
TableOperationDispatcher / SchemaOperationDispatcher / ...
  -> EntityStore -> JDBCBackend -> TableMetaService (one transaction)
                                     insert/update table_meta, table_version_info, columns
                                     MetadataSearchIndex.apply(changes)   <- new
```

| Store operation | Search change |
|-----------------|---------------|
| Insert or import entity | Upsert its row, and its column rows for a table |
| Alter comment or columns | Update its row, replace its column rows |
| Rename leaf | Update `name_lc` |
| Rename catalog or schema | None; full names are joined at read time |
| Soft delete entity | Set `deleted_at` on its row and its column rows |
| Cascade delete of catalog or schema | One update by `catalog_id` or `schema_id` |
| Hard delete by garbage collector | Delete rows past the same `deleted_at` timeline |

**Read path**

```
MetadataSearchOperations (REST)
  -> MetadataSearchService
       loop:
         batch = index.candidates(query, cursor, batchSize)     // ranked, filtered by SQL
         visible = MetadataAuthzHelper.filterByExpression(batch) // per type
         append visible; advance cursor
       until page full, candidates exhausted, or scanned > scanCap
  -> SearchResponse
```

`batchSize` is `2 * limit` and `scanCap` defaults to `20 * limit`. Both are server
configuration. Because the cursor sits after the last *scanned* candidate, a page never skips or
repeats a visible result.

### Backward compatibility

- New table and new endpoint only. No change to existing endpoints, entities or events.
- Every store write gains one extra statement per object, or one per column for table writes.

### Open questions

1. **Inventory sync**: Should a scheduled job import objects from catalogs so search covers them,
   or should that stay with the reconciliation work in #13313?
2. **Comment length**: Is 1024 lower-cased characters enough, or should long comments be indexed
   fully, which rules out a portable `VARCHAR`?
3. **Query-string form**: Is a `GET` with repeatable parameters preferred over `POST` for
   consistency with Domain listing, accepting the encoding rules for tag values?
4. **Semantic Models and functions**: Should they be searchable in Phase 1, given that they are
   recorded objects already?

---

## Task Breakdown

### Phase 1: Index and API

- [ ] Add `metadata_search_doc` to schema and upgrade scripts, with backfill, for H2, MySQL and
      PostgreSQL
- [ ] Add PO, mapper and per-backend SQL providers in `core`
- [ ] Define `MetadataSearchIndex` and implement `JdbcMetadataSearchIndex.apply`
- [ ] Call `apply` from catalog, schema, table, column, view, fileset, topic and model meta
      services, including cascade and hard delete, with transaction rollback tests
- [ ] Implement `candidates` with tiers, tag inheritance, owner filter and cursor
- [ ] Implement `MetadataSearchService` refill loop with authorization filtering and scan cap
- [ ] Add `POST /api/metalakes/{metalake}/search` and its events
- [ ] Update `docs/open-api/*.yaml` and validate with `./gradlew :docs:build` (parallel)
- [ ] Integration test on H2, MySQL and PostgreSQL covering rename, cascade drop, tag inheritance
      and a restricted user

### Phase 2: Clients

- [ ] Add Java client support in `clients/client-java/` (parallel)
- [ ] Add Python client support in `clients/client-python/` (parallel)
- [ ] Add MCP tool `search_metadata` in `mcp-server/` (parallel)
- [ ] Add user documentation in `docs/`

### Phase 3: Follow-ups, each needing its own design

- [ ] Inventory sync for objects not yet recorded, with #13313
- [ ] `domains` filter once Domain lands
- [ ] Web UI search box in `web-v2`
- [ ] External index implementation for very large inventories
