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

# Design: Server-Side Scan Planning Results (`plan-tasks`) for the Gravitino Iceberg REST Server

| Field   | Value                                                                                     |
| ------- | ----------------------------------------------------------------------------------------- |
| Status  | In review                                                                                 |
| Authors | @laserninja                                                                               |
| Created | 2026-07-28                                                                                |
| Issue   | [#11284](https://github.com/apache/gravitino/issues/11284)                                |
| PR      | [#12194](https://github.com/apache/gravitino/pull/12194)                                  |
| Module  | `iceberg/iceberg-rest-server`, `iceberg/iceberg-common`, `catalogs/catalog-common`, `core` |

---

## 1. Background

Server-side scan planning in the Iceberg REST specification is a **two-step**
protocol. A client submits a scan, and the server may answer with results
inline, with opaque `plan-tasks` tokens, or with both:

```
1. POST /v1/{prefix}/namespaces/{ns}/tables/{t}/plan    → status, file-scan-tasks, plan-tasks
2. POST /v1/{prefix}/namespaces/{ns}/tables/{t}/tasks    → file-scan-tasks for one plan-task
```

Gravitino implements step 1 only. `CatalogWrapperForREST.planTableScan`
plans the scan, returns `COMPLETED` with every `file-scan-task` inline, and
step 2 has no route at all, so `POST .../tasks` returns `404` because the
path does not exist and `/v1/config` does not advertise it.

Two concrete problems follow.

**Clients gate on the advertised endpoint set, not on probing.** pyiceberg
with `scan-planning-mode=server` reads `/v1/config`, sees that
`POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks` is missing,
and refuses to use server-side planning at all:

```text
NotImplementedError: Server does not support endpoint:
  POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks
```

So the half of the protocol Gravitino *does* implement is unreachable for
those clients, which is the report in
[#11284](https://github.com/apache/gravitino/issues/11284).

**One response has to carry the whole plan.** Returning every task inline is
fine for a table with tens of data files and increasingly bad as tables
grow: a scan of a 100,000-file table serializes 100,000 structured tasks
into a single HTTP response, on one request thread, with the client unable to
start work until the last byte arrives. Batched results are exactly what
step 2 of the protocol exists for.

A first iteration of PR #12194 added the route and the dispatcher plumbing
but left the handler throwing `NoSuchPlanTaskException` unconditionally,
since no token was ever issued. Review feedback
([#12194](https://github.com/apache/gravitino/pull/12194#discussion_r3655295585))
was that an endpoint which always fails is not support for the interface.
This document covers the design that makes both steps real.

### 1.1 Current state, in one table

| Aspect                                       | Today                                        |
| -------------------------------------------- | -------------------------------------------- |
| `POST .../plan`                              | Implemented, always `COMPLETED`, all inline  |
| `POST .../tasks`                             | No route; `404` from Jersey                  |
| `plan-tasks` in a plan response              | Never emitted                                |
| `/v1/config` endpoints                       | `.../plan` advertised, `.../tasks` absent    |
| pyiceberg `scan-planning-mode=server`        | Fails with `NotImplementedError`             |
| Plan size                                    | Unbounded in one response                    |
| Scan plan cache (`scan-plan-cache-impl`)     | Caches whole plan responses, disabled by default |

---

## 2. Goals

1. **Complete the two-step protocol**: `POST .../tasks` is implemented,
   advertised in `/v1/config`, and returns the file scan tasks a `plan-task`
   token covers. Verifiable: an end-to-end HTTP test redeems a token issued
   by `POST .../plan` and gets `200` with the expected tasks.
2. **Bounded plan responses**: no scan planning response carries more than a
   configured number of `file-scan-tasks`; the remainder is offered as
   `plan-tasks`. Verifiable: a scan with N tasks and batch size B returns B
   tasks inline and `ceil(N/B) - 1` tokens.
3. **Correct task coverage**: the tasks reachable through a plan response
   plus all of its `plan-tasks` are exactly the tasks of the planned
   snapshot, each appearing once. Verifiable by a unit test over a table
   whose plan spans several batches.
4. **Tokens survive restart and replica failover**: a token issued by one
   Gravitino instance is redeemable after that instance restarts and on any
   other instance serving the same catalog. Verifiable: token resolution
   depends on no in-process state.
5. **Snapshot stability**: a token resolves against the snapshot that was
   planned, even if the table is committed to in between. Verifiable: append
   to the table after planning, then redeem the tokens and see the original
   snapshot's tasks.
6. **Spec-conformant errors**: an unknown, foreign or no-longer-resolvable
   token returns `404` with `NoSuchPlanTaskException`; a missing table
   returns `404` with `NoSuchTableException`.
7. **No regression for small scans**: a plan that fits in one batch is
   byte-identical to today's response and needs no second call.
8. **Observability parity**: the new operation emits pre/post/failure events
   and an audit operation type, like every other Iceberg REST operation.

---

## 3. Non-Goals

1. **Asynchronous planning (`SUBMITTED` / `planId`)**: planning stays
   synchronous. The plan lifecycle endpoints
   `GET`/`DELETE .../plan/{planId}` are the subject of a separate change
   ([#11635](https://github.com/apache/gravitino/pull/11635)); this design
   deliberately does not touch `PlanStatus` other than `COMPLETED`.
2. **Honoring `min-rows-requested`**: the spec's row-count hint for sizing
   plan tasks is not implemented. Batches are sized in tasks, not rows;
   supporting the hint requires row-count accounting we can add later
   without changing the token format.
3. **A distributed scan plan cache**: `ScanPlanCache` stays a pluggable
   interface with an in-memory implementation. Making a shared cache the
   default is a separate operational decision (see §8).
4. **Signed or encrypted tokens**: tokens carry no secret and grant no
   access (§5.5.4), so cryptographic protection would add key management for
   no privilege boundary.
5. **Legacy Iceberg clients (< 1.11)**: responses use structured
   `file-scan-tasks` per the 1.11 REST API. Clients expecting the older
   JSON-string task encoding remain unsupported, as before this change.
6. **Changing scan planning semantics**: filters, projections, snapshot
   selection, incremental scans and credential vending behave exactly as
   they do today; only how results are delivered changes.

---

## 4. Solution Investigations

The endpoint itself is not the interesting part; how a `plan-task` token is
represented and resolved is. Four options were considered.

| Approach                                                    | Pros                                                                                        | Cons                                                                                                                                 | Decision                                                                    |
| ----------------------------------------------------------- | ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------- |
| **A.** Keep returning everything inline; `.../tasks` always `404` | No new concepts; smallest diff                                                              | The endpoint is decoration, not support (review feedback); plan responses stay unbounded                                              | **Rejected** — does not meet Goals 1–2                                      |
| **B.** In-memory plan-task store, random token ids          | Fastest redemption (no re-planning); trivial token                                          | Token dies with the process and is meaningless on another replica, so a scan fails mid-way after a restart or behind a load balancer  | **Rejected** — violates Goal 4                                              |
| **C.** Self-describing token: scan request + pinned snapshot + task range | No cross-request state, so restart-safe and replica-safe; reuses the existing plan cache for speed | Redeeming a token re-plans the snapshot on a cache miss; token is larger than an opaque id                                            | **Chosen**                                                                  |
| **D.** Asynchronous plan (`SUBMITTED` + `planId`) with server-side plan state | Matches the part of the spec built for very large plans                                     | Needs a plan lifecycle (states, expiry, cancellation, storage) — that is #11635's scope, and it does not remove the need for `.../tasks` | **Deferred** — orthogonal; this design stays synchronous                     |

**Why not B, in more detail.** Gravitino's Iceberg REST server is routinely
run as more than one replica behind a load balancer, and the Iceberg Java
client fetches plan tasks *concurrently* (`ScanTaskIterable` uses a worker
pool), so consecutive requests of one scan land on different replicas by
design. With node-local token state, a client's scan fails with `404` for
reasons that have nothing to do with its own behavior, and the failure is
timing-dependent — the worst kind to debug. Sticky sessions or a shared
store would fix it, but that is either an operator burden or option D's
plan-state problem in disguise.

**Cost of C, stated plainly.** A token redemption needs the full ordered
task list for its snapshot. With the scan plan cache enabled that is one
cache lookup; with the cache disabled (today's default) it re-plans the
pinned snapshot. For a plan of N tasks and batch size B the client makes
`ceil(N/B)` requests, and the server computes 1 plan with the cache on and
`ceil(N/B)` plans with it off:

| Plan size | Batch size | Client requests | Plans computed (cache on) | Plans computed (cache off) |
| --------- | ---------- | --------------- | ------------------------- | -------------------------- |
| 80        | 100        | 1               | 1                         | 1                          |
| 1,000     | 100        | 10              | 1                         | 10                         |
| 100,000   | 100        | 1,000           | 1                         | 1,000                      |

Re-planning is not free but it is also not new work: it reads the same
manifests the first plan read, for a snapshot that is pinned and therefore
immutable. The mitigation is documented (enable `scan-plan-cache-impl`), and
§8 asks whether Gravitino should go further and couple batching to the cache
being enabled.

**Alternative considered for ordering.** A token could carry the identity of
the files it covers instead of an index range, which would remove the
dependency on a stable order. Rejected: it makes tokens grow with batch size
(kilobytes per token) and duplicates in the token what the plan already
knows, when a total order over `(file location, start, length)` gives
stability for free (§5.6).

---

## 5. Proposal

### 5.1 Overview

```
                POST .../plan  {filter, select, snapshot-id, …}
                          │
                          ▼
        ┌───────────────────────────────────────────────┐
        │ CatalogWrapperForREST.planTableScan           │
        │  1. loadTable                                 │
        │  2. pin snapshot (if the request left it open) │
        │  3. plan full scan  ── cache hit ─▶ cached plan│
        │     miss ─▶ plan, sort tasks, put in cache    │
        │  4. split: first B tasks inline,              │
        │            one plan-task token per later batch│
        │  5. inject vended credentials (if requested)  │
        └───────────────────────────────────────────────┘
                          │
     status: COMPLETED, file-scan-tasks[0..B), plan-tasks[…]
                          │
                          ▼
                POST .../tasks  {plan-task}
                          │
                          ▼
        ┌───────────────────────────────────────────────┐
        │ CatalogWrapperForREST.fetchScanTasks          │
        │  1. decode token (not ours ⇒ 404)             │
        │  2. loadTable (missing ⇒ 404)                 │
        │  3. plan full scan for the token's scan       │
        │     request ── cache hit ─▶ cached plan       │
        │     miss ─▶ re-plan pinned snapshot, sort     │
        │  4. slice [offset, offset+limit)              │
        └───────────────────────────────────────────────┘
                          │
              file-scan-tasks for that batch
```

Nothing is stored between the two calls. The token *is* the state.

### 5.2 New REST API: `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks`

**Request** (`FetchScanTasksRequest`, Iceberg 1.11):

| Field       | Type   | Required | Description                                            |
| ----------- | ------ | -------- | ------------------------------------------------------ |
| `plan-task` | string | yes      | An opaque token from a prior plan response (§5.5)      |

```json
{ "plan-task": "eyJ0YWJsZSI6ImRiLnRibCIsIm9mZnNldCI6MTAwLCJsaW1pdCI6MTAwLCJzY2FuIjp7…" }
```

**Response** `200 OK` (`FetchScanTasksResponse`):

| Field             | Type   | Present                     | Description                                                    |
| ----------------- | ------ | --------------------------- | -------------------------------------------------------------- |
| `file-scan-tasks` | array  | always                      | The tasks this token covers: data file, residual, delete refs   |
| `delete-files`    | array  | when the batch's tasks have deletes | Delete files the batch's tasks reference by index (§5.11)   |
| `specs-by-id`     | object | always                      | Partition specs, required to deserialize the tasks              |
| `plan-tasks`      | array  | never in this implementation | Reserved by the spec for a server that sub-divides further      |

```json
{
  "file-scan-tasks": [
    { "data-file": { "file-path": "s3://bucket/db/tbl/data/00001.parquet", "…": "…" },
      "residual-filter": { "type": "eq", "term": "day", "value": "2026-07-28" } }
  ],
  "specs-by-id": { "0": { "spec-id": 0, "fields": [] } }
}
```

**Behavior.** Authorization runs first, from the table in the request path,
with the same expression as `POST .../plan` (owner, or `USE_CATALOG` +
`USE_SCHEMA` + `SELECT_TABLE`/`MODIFY_TABLE`). Then the table is loaded, so
a request against a missing table reports the missing table rather than
masking it as a bad token. Then the token is decoded and resolved.

| Condition                                                      | Status | Error type                 |
| -------------------------------------------------------------- | ------ | -------------------------- |
| Token resolves to a batch of the plan                          | `200`  | —                          |
| Missing or empty request body                                  | `400`  | `IllegalArgumentException` |
| Table does not exist                                           | `404`  | `NoSuchTableException`     |
| Token not issued by this server, or issued for another table   | `404`  | `NoSuchPlanTaskException`  |
| Token's snapshot no longer exists (expired, rolled back)       | `404`  | `NoSuchPlanTaskException`  |
| Token's range starts past the end of the plan                  | `404`  | `NoSuchPlanTaskException`  |
| Caller lacks table privileges                                  | `403`  | `ForbiddenException`       |

Tokens are **not** single-use: redeeming one twice returns the same tasks,
which keeps client retries safe.

### 5.3 Changed REST API: `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan`

**Old behavior.** `COMPLETED` with every `file-scan-task` inline and no
`plan-tasks`, whatever the plan's size.

**New behavior.** `COMPLETED` with at most `scan-plan-task-batch-size`
`file-scan-tasks` inline; every later batch is offered as a `plan-task`
token. Below the threshold the response is unchanged.

```json
{
  "status": "completed",
  "file-scan-tasks": [ "… 100 tasks …" ],
  "plan-tasks": [ "eyJ0YWJsZSI6…MjAwLCJsaW1pdCI6MTAw…", "eyJ0YWJsZSI6…MzAwLCJsaW1pdCI6MTAw…" ],
  "specs-by-id": { "0": { "…": "…" } }
}
```

**Migration impact.**

| Client                                            | Impact                                                                                         |
| ------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Iceberg Java 1.11+ (`ScanTaskIterable`)           | None; it already consumes `plan-tasks`, now concurrently against `.../tasks`                    |
| pyiceberg with `scan-planning-mode=server`        | Fixed: server-side planning becomes usable at all                                              |
| A client that ignores `plan-tasks`                | Would silently see only the first batch. Such a client is not spec-conformant, but operators can restore the old shape with `scan-plan-task-batch-size=0` |
| Clients not using server-side planning            | None                                                                                           |

Credential vending is unchanged: credentials belong to the plan response,
and a client that received them at plan time keeps using them for the tasks
it fetches later. `FetchScanTasksResponse` has no credential field in the
spec.

### 5.4 `/v1/config`

`Endpoint.V1_FETCH_TABLE_SCAN_PLAN_TASKS` is added to the advertised
endpoint list next to the existing plan endpoint, which is what unblocks
clients that gate on the endpoint set.

### 5.5 The `plan-task` token

#### 5.5.1 Payload

The token is `base64url(JSON)`, unpadded. Four fields, nothing else:

| Field    | Type   | Description                                                                  |
| -------- | ------ | ---------------------------------------------------------------------------- |
| `table`  | string | `TableIdentifier.toString()` of the planned table                             |
| `offset` | int    | Index of the first task the token covers, in the plan's total order (§5.6)     |
| `limit`  | int    | Maximum number of tasks the token covers                                      |
| `scan`   | object | The planned request, serialized by Iceberg's `PlanTableScanRequestParser`, with the snapshot pinned |

```json
{
  "table": "db.tbl",
  "offset": 100,
  "limit": 100,
  "scan": {
    "snapshot-id": 3821550127947089009,
    "case-sensitive": true,
    "use-snapshot-schema": false,
    "filter": { "type": "eq", "term": "day", "value": "2026-07-28" }
  }
}
```

Reusing `PlanTableScanRequestParser` for the `scan` member means filters,
projections and stats fields round-trip through Iceberg's own serializer
rather than a hand-rolled one, and a new scan request field is carried
without touching this design.

#### 5.5.2 Encoding and decoding

`PlanTaskToken.encode(table, scanRequest, offset, limit)` produces the
string; `PlanTaskToken.decode(planTask)` returns `Optional<PlanTaskToken>`
and is empty for anything this server would not have issued: not base64,
not a JSON object, a missing or wrongly typed field, `offset < 0`, or
`limit <= 0`. `fetchScanTasks` additionally requires
`token.matchesTable(tableIdentifier)`. Every rejection path becomes one
`404 NoSuchPlanTaskException`, so a malformed token and an expired one are
indistinguishable to a client, as the spec intends.

`decode` returning `Optional` rather than throwing keeps "this is not our
token" as an ordinary outcome, and leaves the mapping to a REST error in the
one place that knows the request context.

#### 5.5.3 Forward compatibility

An earlier revision carried a `version` field; review feedback
([#12194](https://github.com/apache/gravitino/pull/12194#discussion_r3664512613))
asked for it to be dropped, and it was. It is redundant: a token from a
future format that this server cannot decode into these four fields is
already reported as an unknown plan task, which is the same behavior the
version check produced. A future format change should therefore keep these
field names meaning what they mean here, and may add fields freely.

#### 5.5.4 Security properties

The token is opaque to clients but is **not a capability**:

- Authorization is evaluated from the **table in the request path**, never
  from the token, so a token cannot widen access.
- A token minted for another table is rejected, so it cannot be used to read
  a table the caller happens to be authorized for by swapping paths.
- A forged token can at most express a scan the caller could already submit
  directly through `POST .../plan` (filters and projections are not
  privileges).
- The token contains no credential and no user identity; it names a table, a
  snapshot id, and a scan.

The token does reveal the snapshot id and the client's own filter to anyone
who sees the token, which is the same information that client already sent
and received in the plan response.

### 5.6 Determinism: pinned snapshots and a total order over tasks

An index range only identifies tasks if the plan it indexes is reproducible.
Two properties make it so.

**Snapshot pinning.** If a request does not name a snapshot (and is not an
incremental scan, which pins a range), `planTableScan` resolves the table's
current snapshot and plans *that*, embedding it in the tokens. Without this,
a commit between plan and fetch would move the token to a different
snapshot: tasks would be dropped or duplicated, and a client could produce a
result set that never existed at any point in time. Requests that already
pin a snapshot, incremental requests, and tables with no current snapshot
are planned exactly as they arrive.

**Total order.** Iceberg plans manifests in parallel, so `planFiles()` does
not return tasks in a reproducible order. Tasks are therefore sorted by
`(data file location, start, length)` before batching or caching. The triple
is a total order because two tasks over the same file cover disjoint byte
ranges.

Both properties are needed together: pinning without ordering still lets a
re-plan shuffle the batches; ordering without pinning still lets the
underlying data change.

### 5.7 Interaction with the scan plan cache

`ScanPlanCache` already caches whole plan responses under a
`ScanPlanCacheKey` that includes the resolved snapshot id and every scan
parameter. Two consequences fall out for free:

- The cache key computed for a *pinned* request equals the key for the
  original *unpinned* request, because the key resolves a null snapshot id
  from the table. A plan cached by `POST .../plan` is therefore hit by the
  matching `POST .../tasks`.
- The cache stores the **full** plan, and batching is applied per response,
  so a cached entry serves both the inline first batch and every later
  batch.

### 5.8 Internal interfaces

`fetchScanTasks` is added along the existing dispatcher chain, so it gets
the same authorization, event, hook and audit treatment as every other
table operation. No existing signature changes.

```java
// iceberg/iceberg-rest-server .../service/dispatcher/IcebergTableOperationDispatcher.java
FetchScanTasksResponse fetchScanTasks(
    IcebergRequestContext context,
    TableIdentifier tableIdentifier,
    FetchScanTasksRequest request);
```

Implemented by `IcebergTableOperationExecutor` (delegates to the catalog
wrapper), `IcebergTableEventDispatcher` (events around the call) and
`IcebergTableHookDispatcher` (pass-through today, for symmetry).

```java
// .../service/CatalogWrapperForREST.java
public FetchScanTasksResponse fetchScanTasks(
    TableIdentifier tableIdentifier, FetchScanTasksRequest request);

// package-private helpers introduced in the same class
private PlanTableScanResponse planFullScan(
    TableIdentifier tableIdentifier, Table table, PlanTableScanRequest scanRequest);
private PlanTableScanResponse splitIntoPlanTasks(
    TableIdentifier tableIdentifier, PlanTableScanRequest scanRequest,
    PlanTableScanResponse fullPlan);
private static List<DeleteFile> referencedDeleteFiles(List<FileScanTask> fileScanTasks);

// .../service/PlanTaskToken.java (new, package-private)
static String encode(TableIdentifier table, PlanTableScanRequest scan, int offset, int limit);
static Optional<PlanTaskToken> decode(String planTask);
boolean matchesTable(TableIdentifier tableIdentifier);
PlanTableScanRequest scanRequest();
int offset();
int limit();
```

`planTableScan` keeps its signature; internally it now pins the snapshot,
delegates to `planFullScan`, and batches through `splitIntoPlanTasks`.

### 5.9 Events and audit

| Addition                                                        | Purpose                              |
| --------------------------------------------------------------- | ------------------------------------ |
| `IcebergFetchScanTasksPreEvent`                                 | Before the operation                 |
| `IcebergFetchScanTasksEvent`                                    | After success                        |
| `IcebergFetchScanTasksFailureEvent`                             | After failure, carrying the exception |
| `OperationType.FETCH_SCAN_TASKS`                                | Audit log operation type             |
| `IcebergExceptionMapper`: `NoSuchPlanTaskException` → `404`      | Spec-conformant error status         |

### 5.10 Federated catalogs

When the backend is another REST catalog, `planTableScan` already delegates
upstream, so the `plan-tasks` a client receives are the **remote** catalog's
tokens and are opaque to Gravitino. `FederatedCatalogWrapper.fetchScanTasks`
therefore forwards the token unchanged to the remote `.../tasks` and returns
what the remote answers; local token decoding is never involved. Response
deserialization needs the table's partition specs, which are supplied
through a `ParserContext` built from the loaded table, mirroring the existing
federated plan path.

### 5.11 Delete files and merge-on-read tables

Iceberg serializes a task's deletes as `delete-file-references`, indexes into
the response's top-level `delete-files` array. Batching slices the task list,
so a naive implementation could leave a task in one batch referencing a
delete file listed in another.

No extra code is needed to avoid that:
`BaseScanTaskResponse.Builder.withFileScanTasks` derives the response's
`delete-files` from the tasks it is given (`DeleteFileSet.of(...)`), so every
response — the inline first batch and each fetched batch — lists exactly the
delete files its own tasks reference. `withDeleteFiles` is deprecated in
1.11 for that reason and is not called. The constraint is covered by a
merge-on-read test asserting that the batch holding a deleted-from data file
carries the delete file and serializes with `"delete-file-references":[0]`.

### 5.12 Configuration

| Property                                             | Default | Since | Description                                                                                                                     |
| ---------------------------------------------------- | ------- | ----- | ------------------------------------------------------------------------------------------------------------------------------- |
| `gravitino.iceberg-rest.scan-plan-task-batch-size`   | `100`   | 1.3.0 | Maximum `file-scan-tasks` returned inline by one scan planning response; later batches become `plan-tasks`. `0` disables batching. |

The default matches the Iceberg side, as agreed in review
([#12194](https://github.com/apache/gravitino/pull/12194#discussion_r3662630664)).
Setting `0` restores the pre-change response shape exactly, which is the
escape hatch for an operator with a non-conformant client.

### 5.13 User process

1. The client reads `/v1/config` and sees both `.../plan` and `.../tasks`
   advertised, so it enables server-side scan planning.
2. The client submits a scan:
   `POST /v1/{prefix}/namespaces/db/tables/tbl/plan` with its filter,
   projection and optionally `snapshot-id`.
3. The server answers `COMPLETED` with up to `scan-plan-task-batch-size`
   tasks inline plus one `plan-task` per remaining batch. If the plan fits in
   one batch there are no tokens and the client is done.
4. The client starts work on the inline tasks and, for each token, calls
   `POST /v1/{prefix}/namespaces/db/tables/tbl/tasks` with
   `{"plan-task": "<token>"}` — the Iceberg Java client does this
   concurrently from a worker pool.
5. Each response carries that batch's `file-scan-tasks` (plus `delete-files`
   for a merge-on-read table). The union across the plan response and all
   tokens is the complete task set for the planned snapshot.
6. If the client presents a token after the planned snapshot has expired, it
   gets `404 NoSuchPlanTaskException` and re-plans from step 2.

### 5.14 Implementation process

```
POST .../tasks
      │
      ▼
IcebergTableOperations.fetchScanTasks
      │   authorization on the request thread; empty body ⇒ 400
      ▼
IcebergTableEventDispatcher            PreEvent … Event | FailureEvent
      ▼
IcebergTableHookDispatcher
      ▼
IcebergTableOperationExecutor
      ▼
CatalogWrapperForREST.fetchScanTasks
      │
      ├── PlanTaskToken.decode ──── empty / other table ──▶ 404 NoSuchPlanTaskException
      ├── catalog.loadTable ─────── missing ──────────────▶ 404 NoSuchTableException
      ├── planFullScan(token.scanRequest())
      │        ├── scan plan cache hit ─▶ cached full ordered plan
      │        └── miss ─▶ plan pinned snapshot, sort, cache
      │        └── snapshot gone (IllegalArgumentException) ─▶ 404 NoSuchPlanTaskException
      ├── offset >= task count ───────────────────────────▶ 404 NoSuchPlanTaskException
      └── slice [offset, min(offset+limit, count)) ─▶ FetchScanTasksResponse
                                                      + specs-by-id, + delete-files
```

`FederatedCatalogWrapper` overrides the last step and posts to the remote
catalog instead.

---

## 6. Backward compatibility

| Surface                                  | Compatible? | Note                                                                          |
| ---------------------------------------- | ----------- | ----------------------------------------------------------------------------- |
| Iceberg REST wire protocol               | Yes         | Both steps are spec-defined; only which of them Gravitino uses changes         |
| `POST .../plan` for plans ≤ batch size   | Yes         | Identical response                                                            |
| `POST .../plan` for larger plans         | Shape change | Spec-conformant; `scan-plan-task-batch-size=0` restores the old shape          |
| Gravitino Java/Python clients            | Unaffected  | Scan planning is an Iceberg REST surface, not a Gravitino client API           |
| `ScanPlanCache` interface                | Yes         | No signature change; entries still hold whole plans                           |
| Existing configuration                   | Yes         | One new property, defaulted                                                   |
| Event listeners                          | Additive    | Three new event classes and one new `OperationType`                            |

---

## 7. Testing

| Level                    | Coverage                                                                                                                                              |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| Token unit tests         | Round-trip of snapshot, filter, projection, stats fields and range; rejection of foreign, malformed, wrongly typed and non-object payloads; URL safety |
| Wrapper unit tests       | Batching arithmetic; every task reachable exactly once; resolution against a pinned snapshot after a later append; cache-served path; batching disabled; merge-on-read batches carrying their own delete files; unknown/foreign/stale tokens; missing table |
| REST end-to-end          | With batch size 1: `/plan` advertises the expected tokens and each `POST .../tasks` returns `200` with its task; unknown token returns `404` with the right error type; empty body returns `400` |
| Federation               | The federated path posts to the remote `.../tasks` with the token forwarded untouched                                                                  |
| Events and config        | Pre/failure event dispatch for the new operation; `/v1/config` advertises the endpoint                                                                 |

---

## 8. Open questions for review

1. **Should batching depend on the scan plan cache?** With the cache
   disabled (today's default), an N-batch plan costs N full plans (§4). An
   alternative is to keep everything inline unless a cache is configured, so
   the protocol's second step is only used when redemption is cheap. That
   trades Goal 2 away in the default configuration; the current design
   prefers bounded responses and documents the cache recommendation.
2. **Is 100 the right default?** It matches the Iceberg side and was agreed
   in review, but it means a 100,000-file scan becomes 1,000 requests. A
   larger default, or sizing batches by serialized bytes rather than task
   count, may serve big tables better.
3. **Should tokens carry an expiry?** Today a token stays valid as long as
   its snapshot exists. A short expiry would bound how stale a client's view
   can be, at the cost of an extra failure mode.

---

## 9. Task Breakdown

Delivered in [#12194](https://github.com/apache/gravitino/pull/12194):

- [x] Add `fetchScanTasks` to `IcebergTableOperationDispatcher` and implement it in `IcebergTableOperationExecutor`
- [x] Add the `POST {table}/tasks` endpoint in `IcebergTableOperations` with metrics and authorization
- [x] Map `NoSuchPlanTaskException` to `404` in `IcebergExceptionMapper`
- [x] Advertise `Endpoint.V1_FETCH_TABLE_SCAN_PLAN_TASKS` in `IcebergConfigOperations`
- [x] Add `IcebergFetchScanTasks{Pre,,Failure}Event` and `OperationType.FETCH_SCAN_TASKS`, and wire them in `IcebergTableEventDispatcher` and `IcebergTableHookDispatcher`
- [x] Add `PlanTaskToken` (encode, decode, table match)
- [x] Add `scan-plan-task-batch-size` to `IcebergConstants` and `IcebergConfig`
- [x] Split plan responses into inline tasks plus `plan-tasks` in `CatalogWrapperForREST.planTableScan`
- [x] Pin the planned snapshot and order planned tasks totally
- [x] Resolve a token to its batch in `CatalogWrapperForREST.fetchScanTasks`
- [x] Forward `POST {table}/tasks` upstream in `FederatedCatalogWrapper`
- [x] Unit tests: `TestPlanTaskToken`, `TestScanPlanTaskBatching`
- [x] REST tests: `TestIcebergFetchScanTasksEndpoint`, `TestIcebergTableOperations`, `TestIcebergConfig`
- [x] Federation test in `TestCatalogWrapperForREST`
- [x] Document the endpoint, token semantics and the new property in `docs/iceberg-rest-service.md`

Follow-ups, each its own issue:

- [ ] Integration test with pyiceberg `scan-planning-mode=server` against a multi-batch table
- [ ] Honor `min-rows-requested` when sizing batches (Non-Goal 2)
- [ ] Ship a shared `ScanPlanCache` implementation so token redemption avoids re-planning across replicas (Non-Goal 3)
- [ ] Revisit the default batch size once real workloads are measured (§8.2)
