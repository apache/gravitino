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

# Design: Pagination for the List Endpoints of the Gravitino REST API

| Field   | Value                                                                  |
| ------- | ---------------------------------------------------------------------- |
| Status  | Draft                                                                  |
| Authors | @markhoerth                                                            |
| Created | 2026-11-09                                                             |
| Issue   | #14453                                                                 |
| Module  | `server`, `server-common`, `common`, `api`, `clients`, `core` (phase 2) |

---

## 1. Background

Every list endpoint under `/api/metalakes/...` returns its complete result in one response. A
schema with tens of thousands of tables, or a metalake with thousands of users, roles or tags,
produces one very large response, and the server authorizes every entry in it before replying.

The handlers share one shape. `TableOperations.listTables` is representative:

1. The dispatcher returns the whole list (`NameIdentifier[] listTables(Namespace)`).
2. `MetadataAuthzHelper.filterByExpression` authorizes every entry in memory.
3. The handler wraps the survivors in one `EntityListResponse`.

The API interfaces return whole arrays (`TableCatalog.listTables(Namespace)` and its peers), so
neither the server nor the clients can ask for less.

### 1.1 What already exists

| Where | What it does | Relevance |
| ----- | ------------ | --------- |
| Iceberg REST service (`IcebergPaginationHelper`, #10669) | `pageToken`/`pageSize` on list namespaces, tables and views. The token is the name of the last item returned. The full list is loaded and sorted, then cut. | Parameter names and the name-keyed cursor are reused here. |
| Lance REST service (#11134) | `pageToken`/`limit` on namespace and table listing. | Same model. |
| Core `listUsers(metalake, offset, limit)` / `listGroups(...)` (#11833) | Offset/limit SQL ordered by `user_id`/`group_id`, returning `PagedResult` with a total count. No REST handler calls it. | Offset paging skips or repeats rows when rows are inserted or deleted mid-walk, and the id order differs from the name order every other list uses. Phase 2 replaces it with keyset paging. |
| #13631 (epic #13621) | Paginated listing and counting in `EntityStore`. | Phase 2 builds on it. |

### 1.2 Where the lists come from

For metalakes, catalogs, users, groups, roles, tags, policies, job templates, jobs and the
entities of Gravitino-managed catalogs (filesets, models), the list is read from the entity
store. For schemas, tables, views, topics and partitions of an external catalog (Hive, JDBC,
Iceberg, Kafka and the others), the dispatcher calls the catalog connector, which asks the
source system. Most source systems have no ordered, resumable listing (the Hive Metastore has
none), so the server cannot assume it can push a page request down to the source.

## 2. Goals

1. Every list endpoint under `/api` accepts `pageToken` and `pageSize` query parameters and
   returns `nextPageToken` when more results remain.
2. A request without `pageSize` or `pageToken` behaves exactly as today.
3. Results come back in a stable order, so that walking the pages returns every entity exactly
   once while nothing is created or dropped.
4. Authorization filtering happens before the page is cut. A page is never short or empty
   because hidden entities were dropped from it.
5. The `details=true` variants page the same way.
6. Invalid paging input is rejected with a 400 that names the parameter.
7. The Java and Python clients expose paged listing alongside the existing methods.

### Non-Goals

- Total counts in list responses. Counting visible entities requires authorizing all of them,
  which is the cost paging exists to avoid. #13631 covers counting in the store.
- Snapshot isolation across a walk. Section 4.3 states the guarantee under concurrent change.
- Sorting or filtering options beyond the existing `details` and job filters.
- Changes to the Iceberg REST and Lance REST services, which already page.

## 3. API

### 3.1 Request parameters

| Parameter   | Type    | Meaning |
| ----------- | ------- | ------- |
| `pageSize`  | integer | Maximum number of entries in the page. Must be at least 1. |
| `pageToken` | string  | Opaque token from a previous response's `nextPageToken`. |

Behavior by combination:

| `pageSize` | `pageToken` | Result |
| ---------- | ----------- | ------ |
| absent | absent | Full list, as today. No `nextPageToken`. |
| present | absent | First page of up to `min(pageSize, maxPageSize)` entries. |
| present | present | Next page after the token's position. |
| absent | present | Next page of up to `maxPageSize` entries. A client holding a token is walking pages, so the server keeps the page bounded. |

A `pageSize` above the server maximum is clamped to the maximum rather than rejected, so a client
that asks for large pages keeps working against a server with a smaller cap. The client sees the
smaller page and a `nextPageToken`.

The names match the Iceberg REST service. Unlike the Iceberg REST spec, a first page is requested
with `pageSize` alone. The Iceberg REST rule (an empty `pageToken` starts paging) exists for spec
compatibility with Iceberg clients and has no reason to exist in the main API.

### 3.2 Response

Every list response gains one optional field:

```json
{
  "code": 0,
  "identifiers": [
    {"namespace": ["lake", "hive", "sales"], "name": "orders_2024"},
    {"namespace": ["lake", "hive", "sales"], "name": "orders_2025"}
  ],
  "nextPageToken": "eyJ2IjoxLCJrIjoib3JkZXJzXzIwMjUiLCJzIjoiOWQ0YjE3ZTBjMmFhNTFmMyJ9"
}
```

`nextPageToken` is omitted when no visible entity remains. Existing clients are unaffected: the
Java client's `ObjectMapper` disables `FAIL_ON_UNKNOWN_PROPERTIES`, and the Python client's
`dataclasses_json` decoding drops unknown keys.

### 3.3 Page token

The token is opaque to clients. Internally it is base64url (no padding) of a small JSON object:

```json
{"v": 1, "k": "orders_2025", "s": "9d4b17e0c2aa51f3"}
```

| Field | Meaning |
| ----- | ------- |
| `v` | Token format version. A server rejects a version it does not know. |
| `k` | Sort key of the last entry returned. The next page starts strictly after it. |
| `s` | First 16 hex characters of the SHA-256 of the list scope: the endpoint path with its path parameters, the `details` flag, and any filter parameters (for example `jobTemplateName` on job runs). |

The token holds no server state, so it never expires and any server node can serve the next
page. The scope hash makes a token from one list fail loudly when presented to another, instead
of silently resuming at an unrelated position. The token is not signed: a client that edits it
only moves its own cursor, and every returned entry is still authorized for the caller.

### 3.4 Errors

All of these return HTTP 400 with an `IllegalArgumentException` error response:

| Condition | Message |
| --------- | ------- |
| `pageSize` below 1 | `pageSize must be at least 1, got: 0` |
| `pageSize` not an integer | Rejected by JAX-RS parameter conversion as today for integer query parameters. |
| `pageToken` not decodable, unknown version, or missing fields | `Invalid pageToken: the token is malformed` |
| `pageToken` scope does not match the request | `Invalid pageToken: the token was issued for a different list` |

Because tokens are stateless and carry no expiry, there is no expired-token case.

### 3.5 Configuration

| Key | Default | Meaning |
| --- | ------- | ------- |
| `gravitino.server.list.maxPageSize` | `1000` | Largest page the server returns. Applies only to paged requests; an unpaged request returns the full list as today. |

## 4. Server design

### 4.1 Stable order

Each list has a sort key, compared as Java `String.compareTo` (UTF-16 code unit order, which
matches binary UTF-8 order for the characters Gravitino accepts in names). Locale collation is
never used, so the order is the same on every node and every JVM.

| Endpoint | Sort key |
| -------- | -------- |
| listMetalakes, listCatalogs, listSchemas, listTables, listViews, listFunctions, listFilesets, listModels, listTopics, listSemanticModels, listMetrics | entity name |
| listUsers, listGroups, listRoles, listTags, listPolicies, listJobTemplates, listSecretProviders | name |
| listJobs | job ID |
| listModelVersions | version number, compared numerically (encoded zero-padded in `k`) |
| listFiles | file name within the listed sub path |
| listPartitionNames, listPartitionStatistics | partition name |
| listStatistics | statistic name |
| tags, policies, roles for a metadata object | tag, policy or role name |
| policies for a tag, tags for a policy | policy or tag name |
| metadata objects for a tag | object type, then full name |

A list whose entries share a sort key is impossible for every endpoint above, because the key is
the entry's unique name within its parent (or the object type plus full name).

### 4.2 Authorization before the cut

The pager walks the sorted entries after the cursor in chunks and authorizes each chunk with the
same `MetadataAuthzHelper` call the handler uses today:

1. Skip entries whose key is not greater than the cursor key.
2. Take the next chunk, authorize it, and append the visible entries to the page.
3. Repeat until the page holds `pageSize + 1` visible entries or the list is exhausted.
4. Return the first `pageSize` entries. If a visible entry beyond them was found,
   `nextPageToken` carries the key of the last returned entry; otherwise it is omitted.

The look-ahead entry in step 3 is what makes "`nextPageToken` is absent only when nothing visible
remains" true: a page that ends just before a run of hidden entries does not hand out a token
that leads to an empty page. The first chunk is `pageSize + 1` entries; each further chunk
doubles, capped at `maxPageSize`, which keeps the number of authorizer calls logarithmic when
most entries are hidden. When the parent-scope short-circuit in `MetadataAuthzHelper` applies,
the first chunk is entirely visible and the loop ends after one authorizer call.

Authorization cost per page is now bounded by the entries the walk visits rather than the whole
list, which also limits the per-entry cost reported in #11775 for paged callers.

### 4.3 Guarantee under concurrent change

The cursor is a key, not an offset. For a walk that runs while entities are created and dropped:

- An entity that exists for the whole walk is returned exactly once.
- No entity is returned twice.
- An entity created during the walk is returned if its key sorts after the cursor at the time
  the page containing that position is served, and is not returned otherwise.
- An entity dropped during the walk is not returned if it is dropped before the walk reaches it.
- A rename is a drop of the old name and a create of the new one, and follows the two rules above.

A permission change during the walk applies from the next page on.

### 4.4 Components

| Component | Module | Role |
| --------- | ------ | ---- |
| `PageToken` | `server-common` | Encodes and decodes the token, computes and checks the scope hash. |
| `ListPager` | `server-common` | Validates parameters, sorts, applies the cursor, runs the chunked authorization loop, builds the next token. |
| `PaginatedResponse` | `common` | Interface for list response DTOs exposing `nextPageToken()`; each list response class gains the field with `@JsonInclude(NON_NULL)`. |
| `Configs.LIST_MAX_PAGE_SIZE` | `core` | The cap in section 3.5. |
| `Page<T>`, `PageRequest` | `api` | Client-facing paging types (section 5). |

Each handler gains the two `@QueryParam`s and replaces its filter-and-wrap step with a
`ListPager` call. When neither parameter is present, the handler runs exactly the code path it
runs today, so the unpaged behavior and its performance do not change.

### 4.5 Phase 2: pushing pages down to the source

Phase 1 still loads and sorts the full list for every page request. That bounds response size
and authorization cost but not the cost of reading the list from the store or the source system.
Phase 2 removes it where the source can serve ordered, resumable reads:

- **Entity store.** A keyset read, `WHERE <name> > #{afterKey} ORDER BY <name> LIMIT #{limit}`,
  for every store-backed list, built on the #13631 work. MySQL tables already use
  `utf8mb4_bin`, so MySQL order matches section 4.1. PostgreSQL columns use the database's
  default collation, so the query uses `COLLATE "C"` and adds a matching index. H2 compares
  binary by default. The core `listUsers`/`listGroups` offset methods move to the same keyset
  read, ordered by name.
- **Catalog connectors.** An optional connector interface, `SupportsPagedListing`, with
  `Page<NameIdentifier> listTables(Namespace, String afterKey, int limit)` and the same for
  schemas and views. JDBC catalogs implement it with an ordered, limited query against the
  source's information schema. Connectors that do not implement it (the Hive Metastore has no
  paged listing) keep the phase 1 behavior.

When the source pages, the chunked loop in section 4.2 fetches each chunk from the source
instead of from an in-memory list. The REST contract does not change between phases.

## 5. Clients

### 5.1 Java

New types in `org.apache.gravitino.pagination` in the `api` module:

```java
public final class PageRequest {
  public static PageRequest ofSize(int pageSize);
  public PageRequest withToken(String pageToken);
  public int pageSize();
  public Optional<String> pageToken();
}

public interface Page<T> {
  List<T> items();
  Optional<String> nextPageToken();
}
```

Each list method gains an overload taking a `PageRequest`, as a default method on the API
interface that throws `UnsupportedOperationException`, implemented in the Java client:

```java
Page<NameIdentifier> page = catalog.asTableCatalog().listTables(ns, PageRequest.ofSize(500));
while (true) {
  page.items().forEach(this::process);
  if (!page.nextPageToken().isPresent()) break;
  page = catalog.asTableCatalog()
      .listTables(ns, PageRequest.ofSize(500).withToken(page.nextPageToken().get()));
}
```

The existing array-returning methods are unchanged.

### 5.2 Python

Each list method gains a `_page` variant returning a `Page` with `items` and `next_page_token`:

```python
page = catalog.as_table_catalog().list_tables_page(namespace, page_size=500)
while page.next_page_token:
    page = catalog.as_table_catalog().list_tables_page(
        namespace, page_size=500, page_token=page.next_page_token)
```

## 6. Endpoint coverage

| Group | Endpoints |
| ----- | --------- |
| Metadata | listMetalakes, listCatalogs, listSchemas, listTables, listViews, listFunctions, listFilesets, listFiles, listModels, listModelVersions, listTopics, listPartitionNames, listSemanticModels, listMetrics |
| Access control | listUsers, listGroups, listRoles |
| Tags, policies, jobs, secrets | listTags, listPolicies, listJobTemplates, listJobs, listSecretProviders |
| Statistics | listStatistics, listPartitionStatistics |
| Associations | tags, policies and roles for a metadata object; metadata objects and policies for a tag; tags for a policy |

## 7. Implementation phases

| PR | Content |
| -- | ------- |
| 1 | This design. `PageToken`, `ListPager`, `PaginatedResponse`, the config key, the client paging types. `listTables` end to end on the server, Java client and Python client, with the unit, REST and client tests in section 8. |
| 2 | The remaining metadata lists, and the integration tests in section 8. |
| 3 | Access control, tags, policies, jobs, secrets and statistics. |
| 4 | Association lists. |
| 5 | Phase 2 keyset reads in the entity store, replacing the offset paging in core. |
| 6 | `SupportsPagedListing` for JDBC catalogs. |

PRs 2 to 4 are mechanical once PR 1 lands: two query parameters and one `ListPager` call per
handler, one field per response DTO, one overload per client method.

## 8. Testing

Unit tests for `PageToken` and `ListPager`:

- Round trip of the token, and rejection of a malformed token, an unknown version and a token
  from another scope, each with a message naming `pageToken`.
- Rejection of `pageSize` 0 and -1 with a message naming `pageSize`; clamping above the cap.
- A walk across several pages returns every entry once, in order.
- A page boundary that falls on entries the caller cannot see: the page is full, the next page
  starts at the next visible entry, and a trailing run of hidden entries yields no
  `nextPageToken`.
- A list where every entry is hidden returns an empty page with no token.
- Creates and drops between page requests, including entries before and after the cursor,
  satisfy section 4.3.

Server REST tests for the paged handlers, covering unpaged requests (unchanged responses), paged
walks, `details=true`, and the 400 cases.

Integration tests against a running server, through the Java and Python clients, walking a
schema with more tables than one page and with tables hidden from the caller, and running the
walk while another client creates and drops tables concurrently.
