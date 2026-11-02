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

Implemented by [#12194](https://github.com/apache/gravitino/pull/12194) for
[#11284](https://github.com/apache/gravitino/issues/11284), in
`iceberg/iceberg-rest-server`, `iceberg/iceberg-common`,
`catalogs/catalog-common` and `core`.

---

## 1. Background

Server-side scan planning in the Iceberg REST specification is a **two-step**
protocol. A client submits a scan, and the server may answer with results
inline, with opaque `plan-tasks`, or with both:

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
since no plan task was ever issued. Review feedback
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
   covers. Verifiable: an end-to-end HTTP test redeems a `plan-task` issued by
   `POST .../plan` and gets `200` with the expected tasks.
2. **Bounded plan responses**: no scan planning response carries more than a
   configured number of `file-scan-tasks`; the remainder is offered as
   `plan-tasks`. Verifiable: a scan with N tasks and batch size B returns B
   tasks inline and `ceil(N/B) - 1` plan tasks.
3. **Correct task coverage**: the tasks reachable through a plan response
   plus all of its `plan-tasks` are exactly the tasks of the planned
   snapshot, each appearing once. Verifiable by a unit test over a table
   whose plan spans several batches.
4. **Plan tasks survive restart and replica failover**: a plan task issued by one
   Gravitino instance is redeemable after that instance restarts and on any
   other instance serving the same catalog. Verifiable: plan task resolution
   depends on no in-process state.
5. **Snapshot stability**: a plan task resolves against the snapshot that was
   planned, even if the table is committed to in between. Verifiable: append
   to the table after planning, then redeem the plan tasks and see the original
   snapshot's tasks.
6. **Spec-conformant errors**: an unknown, foreign or no-longer-resolvable
   plan task returns `404` with `NoSuchPlanTaskException`; a missing table
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
   without changing the plan task format.
3. **Scan plan cache defaults and implementations**: `ScanPlanCache` stays a
   pluggable interface with an in-memory implementation, disabled by default.
   Whether Gravitino should enable a cache by default, and whether it should
   ship a shared one, is tracked separately (§8.1).
4. **Signed or encrypted plan tasks**: a plan task carries no secret and grants no
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

The endpoint itself is not the interesting part; how a `plan-task` is
represented and resolved is. Four options were considered.

| Approach                                                    | Pros                                                                                        | Cons                                                                                                                                 | Decision                                                                    |
| ----------------------------------------------------------- | ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------- |
| **A.** Keep returning everything inline; `.../tasks` always `404` | No new concepts; smallest diff                                                              | The endpoint is decoration, not support (review feedback); plan responses stay unbounded                                              | **Rejected** — does not meet Goals 1–2                                      |
| **B.** In-memory plan-task store, random ids                | Fastest redemption (no re-planning); trivial to produce                                     | The state dies with the process and means nothing on another replica, so a scan fails mid-way after a restart or behind a load balancer | **Rejected** — violates Goal 4                                              |
| **C.** Self-describing plan task: scan request + pinned snapshot + task range | No cross-request state, so restart-safe and replica-safe; reuses the existing plan cache for speed | Redeeming one re-plans the snapshot on a cache miss; the string is longer than a random id                                          | **Chosen**                                                                  |
| **D.** Asynchronous plan (`SUBMITTED` + `planId`) with server-side plan state | Matches the part of the spec built for very large plans                                     | Needs a plan lifecycle (states, expiry, cancellation, storage) — that is #11635's scope, and it does not remove the need for `.../tasks` | **Deferred** — orthogonal; this design stays synchronous                     |

**Why not B, in more detail.** Gravitino's Iceberg REST server is routinely
run as more than one replica behind a load balancer, and the Iceberg Java
client fetches plan tasks *concurrently* (`ScanTaskIterable` uses a worker
pool), so consecutive requests of one scan land on different replicas by
design. With node-local plan task state, a client's scan fails with `404` for
reasons that have nothing to do with its own behavior, and the failure is
timing-dependent — the worst kind to debug. Sticky sessions or a shared
store would fix it, but that is either an operator burden or option D's
plan-state problem in disguise.

**Cost of C, stated plainly.** A plan task redemption needs the full ordered
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
immutable. The mitigation is documented (enable `scan-plan-cache-impl`);
coupling batching to the cache being enabled was considered and rejected in
review (§8.1). What this costs in a replicated deployment, and what would remove
the cost rather than shrink it, is §5.15 and §8.5.

**Alternative considered for ordering.** A plan task could carry the identity of
the files it covers instead of an index range, which would remove the
dependency on a stable order. Rejected: it makes plan tasks grow with batch size
(kilobytes per plan task) and duplicates in the plan task what the plan already
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
        │            one plan task per later batch│
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
        │  1. decode plan task (not ours ⇒ 404)             │
        │  2. loadTable (missing ⇒ 404)                 │
        │  3. plan full scan for the plan task's scan       │
        │     request ── cache hit ─▶ cached plan       │
        │     miss ─▶ re-plan pinned snapshot, sort     │
        │  4. slice [offset, offset+limit)              │
        └───────────────────────────────────────────────┘
                          │
              file-scan-tasks for that batch
```

Nothing is stored between the two calls. The plan task *is* the state.

### 5.2 New REST API: `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks`

**Request** (`FetchScanTasksRequest`, Iceberg 1.11):

| Field       | Type   | Required | Description                                            |
| ----------- | ------ | -------- | ------------------------------------------------------ |
| `plan-task` | string | yes      | An opaque string from a prior plan response (§5.5)     |

```json
{ "plan-task": "eyJ0YWJsZSI6ImRiLnRibCIsIm9mZnNldCI6MTAwLCJsaW1pdCI6MTAwLCJzY2FuIjp7…" }
```

**Response** `200 OK` (`FetchScanTasksResponse`):

| Field             | Type   | Present                     | Description                                                    |
| ----------------- | ------ | --------------------------- | -------------------------------------------------------------- |
| `file-scan-tasks` | array  | always                      | The tasks this plan task covers: data file, residual, delete refs   |
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
masking it as a bad plan task. Then the plan task is decoded and resolved.

| Condition                                                        | Status | Error type                 |
| ---------------------------------------------------------------- | ------ | -------------------------- |
| Plan task resolves to a batch of the plan                        | `200`  | —                          |
| Missing or empty request body                                    | `400`  | `IllegalArgumentException` |
| Table does not exist                                             | `404`  | `NoSuchTableException`     |
| Plan task not issued by this server, or issued for another table | `404`  | `NoSuchPlanTaskException`  |
| Plan task's snapshot no longer exists (expired, rolled back)     | `404`  | `NoSuchPlanTaskException`  |
| Plan task's range starts past the end of the plan                | `404`  | `NoSuchPlanTaskException`  |
| Caller lacks table privileges                                    | `403`  | `ForbiddenException`       |

Plan tasks are **not** single-use: redeeming one twice returns the same tasks,
which keeps client retries safe.

### 5.3 Changed REST API: `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan`

**Old behavior.** `COMPLETED` with every `file-scan-task` inline and no
`plan-tasks`, whatever the plan's size.

**New behavior.** `COMPLETED` with at most `scan-plan-task-batch-size`
`file-scan-tasks` inline; every later batch is offered as a `plan-task`
plan task. Below the threshold the response is unchanged.

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

### 5.5 The `plan-task` string

To a client a plan task is an opaque string, exactly as the Iceberg REST
specification defines it (§8.3); to the server it describes the unit of work it
stands for. `PlanTaskCodec` is the only place that reads or writes it.

#### 5.5.1 Payload

The plan task is `base64url(JSON)`, unpadded. Four fields, nothing else:

| Field    | Type   | Description                                                                                        |
| -------- | ------ | -------------------------------------------------------------------------------------------------- |
| `table`  | string | `TableIdentifier.toString()` of the planned table                                                   |
| `offset` | int    | Index of the first task it covers, in the plan's total order (§5.6)                                 |
| `limit`  | int    | Maximum number of tasks it covers                                                                  |
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

`PlanTaskCodec.encode(table, scanRequest, offset, limit)` produces the string;
`PlanTaskCodec.decode(planTask)` returns
`Optional<PlanTaskCodec.PlanTask>` — the unit of work the string stands for —
and is empty for anything this server would not have issued: not base64, not a
JSON object, a missing or wrongly typed field, `offset < 0`, or `limit <= 0`. `fetchScanTasks` additionally requires
`planTask.matchesTable(tableIdentifier)`. Every rejection path becomes one
`404 NoSuchPlanTaskException`, so a malformed plan task and an expired one are
indistinguishable to a client, as the spec intends.

`decode` returning `Optional` rather than throwing keeps "this is not our
plan task" as an ordinary outcome, and leaves the mapping to a REST error in the
one place that knows the request context.

#### 5.5.3 Forward compatibility

An earlier revision carried a `version` field; review feedback
([#12194](https://github.com/apache/gravitino/pull/12194#discussion_r3664512613))
asked for it to be dropped, and it was. It is redundant: a plan task from a
future format that this server cannot decode into these four fields is
already reported as an unknown plan task, which is the same behavior the
version check produced. A future format change should therefore keep these
field names meaning what they mean here, and may add fields freely.

#### 5.5.4 Security properties

The plan task is opaque to clients but is **not a capability**:

- Authorization is evaluated from the **table in the request path**, never
  from the plan task, so a plan task cannot widen access.
- A plan task encoded for another table is rejected, so it cannot be used to read
  a table the caller happens to be authorized for by swapping paths.
- A forged plan task can at most express a scan the caller could already submit
  directly through `POST .../plan` (filters and projections are not
  privileges).
- The plan task contains no credential and no user identity; it names a table, a
  snapshot id, and a scan.

The plan task does reveal the snapshot id and the client's own filter to anyone
who sees the plan task, which is the same information that client already sent
and received in the plan response.

### 5.6 Determinism: pinned snapshots and a total order over tasks

An index range only identifies tasks if the plan it indexes is reproducible.
Two properties make it so.

**Snapshot pinning.** If a request does not name a snapshot (and is not an
incremental scan, which pins a range), `planTableScan` resolves the table's
current snapshot and plans *that*, embedding it in the plan tasks. Without this,
a commit between plan and fetch would move the plan task to a different
snapshot: tasks would be dropped or duplicated, and a client could produce a
result set that never existed at any point in time. Requests that already
pin a snapshot, incremental requests, and tables with no current snapshot
are planned exactly as they arrive.

**Total order.** Iceberg plans manifests in parallel, so `planFiles()` does not
return tasks in a reproducible order. Tasks are therefore sorted before batching
or caching, by

```
(data file location, start, length,
 data sequence number, file sequence number,
 manifest location, manifest entry position)
```

An earlier revision of this design sorted on the first three fields only,
reasoning that two tasks over one file cover disjoint byte ranges. That is not
enough, and review on
[#12241](https://github.com/apache/gravitino/pull/12241#issuecomment-5129278423)
asked for the claim to be proved or fixed. It cannot be proved: **one path can be
referenced by several manifest entries.** Appending the same data file twice
produces two entries with the same location, offset and length, differing in
sequence number and potentially in the delete files attached to them:

```
location=…/dup.parquet start=0 length=10  dataSeq=2  manifest=…-m0.avro pos=0
location=…/dup.parquet start=0 length=10  dataSeq=1  manifest=…-m0.avro pos=0
```

Sorting is stable, so tasks that tie keep the order Iceberg planned them in,
which is exactly the order that is not reproducible. Two tied tasks either side
of a batch boundary could therefore swap places between the plan and a later
re-plan, and the client would receive one of them twice and never see the other -
a wrong result, not merely a slow one.

A manifest entry is unique within a snapshot, identified by its manifest and its
position in that manifest, so comparing those makes the order total for any two
tasks that are not interchangeable. Sequence numbers are compared first because
they survive a rewrite into new manifests, and they remain the discriminator if a
reader leaves the manifest fields unset. Tasks that tie on every key carry the
same file, range, sequence numbers and entry, so exchanging them is invisible to
a client.

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

// .../service/PlanTaskCodec.java (new, package-private)
static String encode(TableIdentifier table, PlanTableScanRequest scan, int offset, int limit);
static Optional<PlanTask> decode(String planTask);

// PlanTaskCodec.PlanTask, the unit of work a decoded plan-task stands for
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
plan tasks and are opaque to Gravitino. `FederatedCatalogWrapper.fetchScanTasks`
therefore forwards the plan task unchanged to the remote `.../tasks` and returns
what the remote answers; local plan task decoding is never involved. Response
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
([#12194](https://github.com/apache/gravitino/pull/12194#discussion_r3662630664)),
and keeps a response comfortably inside the ~1 MB body limit common to
gateways and load balancers: 100 file scan tasks serialize to roughly 0.3 MB
(§8.2). Setting `0` restores the pre-change response shape exactly, which is
the escape hatch for an operator with a non-conformant client.

### 5.13 User process

1. The client reads `/v1/config` and sees both `.../plan` and `.../tasks`
   advertised, so it enables server-side scan planning.
2. The client submits a scan:
   `POST /v1/{prefix}/namespaces/db/tables/tbl/plan` with its filter,
   projection and optionally `snapshot-id`.
3. The server answers `COMPLETED` with up to `scan-plan-task-batch-size`
   tasks inline plus one `plan-task` per remaining batch. If the plan fits in
   one batch there are no plan tasks and the client is done.
4. The client starts work on the inline tasks and, for each plan task, calls
   `POST /v1/{prefix}/namespaces/db/tables/tbl/tasks` with
   `{"plan-task": "<plan task>"}` — the Iceberg Java client does this
   concurrently from a worker pool.
5. Each response carries that batch's `file-scan-tasks` (plus `delete-files`
   for a merge-on-read table). The union across the plan response and all
   plan tasks is the complete task set for the planned snapshot.
6. If the client presents a plan task after the planned snapshot has expired, it
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
      ├── PlanTaskCodec.decode ──── empty / other table ──▶ 404 NoSuchPlanTaskException
      ├── catalog.loadTable ─────── missing ──────────────▶ 404 NoSuchTableException
      ├── planFullScan(planTask.scanRequest())
      │        ├── scan plan cache hit ─▶ cached full ordered plan
      │        └── miss ─▶ plan pinned snapshot, sort, cache
      │        └── snapshot gone (IllegalArgumentException) ─▶ 404 NoSuchPlanTaskException
      ├── offset >= task count ───────────────────────────▶ 404 NoSuchPlanTaskException
      └── slice [offset, min(offset+limit, count)) ─▶ FetchScanTasksResponse
                                                      + specs-by-id, + delete-files
```

`FederatedCatalogWrapper` overrides the last step and posts to the remote
catalog instead.

### 5.15 Multi-replica deployments

Gravitino's Iceberg REST server is routinely run as several replicas behind a
load balancer, and the Iceberg Java client fetches plan tasks **concurrently**
(`ScanTaskIterable` uses a worker pool), so the requests of one scan land on
different replicas by design. This section states what that costs and what it
does not.

**Correctness does not depend on which replica serves a request.** A plan task
carries everything needed to resolve it, so any replica that can load the table
can decode it, reproduce the plan of the pinned snapshot, and slice the same
range - provided the order in §5.6 is total, which is why that section matters
more than it first appears. No replica holds state the others lack, nothing
expires, and a restart mid-scan is invisible to the client.

**Cost does depend on it.** Redeeming a plan task needs the full ordered task
list for its snapshot. The scan plan cache is node-local, so `/plan` served by
replica A warms only A: concurrent `/tasks` on B, C and D each miss and re-plan
the whole pinned snapshot - reading the manifests again and sorting the full task
list - to return one batch. For a plan of N tasks and batch size B the worst case
is `ceil(N/B)` full plans spread across replicas, which for a large table
multiplies manifest reads against object storage.

Nothing about that is incorrect: the snapshot is pinned, so every re-plan of a
given plan task produces the same tasks. It is a load-amplification problem, and
the expected production posture is:

| Measure | Effect |
| ------- | ------ |
| Enable a scan plan cache ([#12254](https://github.com/apache/gravitino/issues/12254) proposes making it the default) | Removes re-planning for requests that land on a replica that already has the plan |
| Route a client's scan to one replica (load balancer affinity) | Makes the node-local cache effective for the whole scan |
| Raise `scan-plan-task-batch-size` for large tables | Fewer plan tasks, so proportionally fewer redemptions |
| Coalesce concurrent redemptions of one plan on a replica | Collapses the concurrent fetches of one scan into a single plan per replica |

The measures above reduce the multiplier; they do not remove it. Removing it
needs a plan task whose redemption cost is independent of the size of the plan -
see the manifest-scoped alternative in §8.4 - or a cache shared by all replicas.

**On signing plan tasks.** §5.5.4 explains why plan tasks are not signed: they
grant nothing, and authorization comes from the request path. Review raised the
consequence for this section, which is worth recording: if a future change signs
or HMACs a plan task to distinguish "issued by this server" from "well-formed but
forged", the key must be shared by every replica. A per-replica key would make a
plan task verifiable only on the replica that issued it, reintroducing exactly
the failure mode this design avoids, and it would do so silently - the client
would see intermittent `404`s that depend on load balancer routing.

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
| Plan task unit tests         | Round-trip of snapshot, filter, projection, stats fields and range; rejection of foreign, malformed, wrongly typed and non-object payloads; URL safety |
| Wrapper unit tests       | Batching arithmetic; every task reachable exactly once; resolution against a pinned snapshot after a later append; cache-served path; batching disabled; merge-on-read batches carrying their own delete files; unknown/foreign/stale plan tasks; missing table |
| REST end-to-end          | With batch size 1: `/plan` advertises the expected plan tasks and each `POST .../tasks` returns `200` with its task; unknown plan task returns `404` with the right error type; empty body returns `400` |
| Federation               | The federated path posts to the remote `.../tasks` with the plan task forwarded untouched                                                                  |
| Events and config        | Pre/failure event dispatch for the new operation; `/v1/config` advertises the endpoint                                                                 |

---

## 8. Decisions taken in review

Three questions were open when this design was first written and were settled in
review on [#12241](https://github.com/apache/gravitino/pull/12241) (§8.1 to
§8.3). The later sections record what a second round of review asked for: how
this compares with other implementations, and what a full fix for re-planning
cost would look like.

### 8.1 Batching does not depend on the scan plan cache

With the cache disabled — today's default — an N-batch plan costs N full plans
(§4), which argued for keeping everything inline unless a cache is configured.

**Decision:** leave caching out of this design. Batching always applies, and
whether Gravitino should enable a scan plan cache by default is a separate
question, raised as its own issue so it can be discussed on its own terms
(with a stated preference for enabling it). This keeps Goal 2 intact in every
configuration, at the cost of re-planning on a cache miss, which the user
documentation calls out.

### 8.2 The default batch size stays 100

100 matches the Iceberg side, and the number also lines up with deployment
reality: many gateways and load balancers cap a response body at about 1 MB,
and 100 file scan tasks serialize to roughly 0.3 MB. It does mean a
100,000-file scan takes 1,000 requests.

**Decision:** keep 100. If real workloads show the default causing problems,
that is a separate issue — possibly sizing batches by serialized bytes rather
than by task count.

### 8.3 A plan task does not expire

Neither the Iceberg specification nor its implementation gives a plan task any
lifetime. The OpenAPI definition calls it "an opaque string provided by the
REST server that represents a unit of work for generating file scan tasks for
scan planning" — an ordinary request parameter, like a table or catalog name,
not a credential.

**Decision:** no expiry. A plan task stays redeemable as long as the snapshot
it pins exists, and the class that produces and reads it is named
`PlanTaskCodec` rather than a token type, to keep the code aligned with that
framing.

### 8.4 How other implementations compare

Review asked how this compares with what other catalogs are doing
([#12241](https://github.com/apache/gravitino/pull/12241#issuecomment-5129106422)).

**Apache Polaris (incubating): not implemented.** Polaris deliberately excludes
all three scan planning paths from the catalog service spec it generates its API
from - `.../plan`, `.../plan/{plan-id}` and `.../tasks` appear only as commented
placeholders marked "Not implemented in Polaris" in
[`spec/polaris-catalog-service.yaml`](https://github.com/apache/polaris/blob/main/spec/polaris-catalog-service.yaml),
and its advertised `/v1/config` endpoint list omits them, so a conformant client
falls back to client-side planning. The tracking issue,
[apache/polaris#966](https://github.com/apache/polaris/issues/966), has been open
since February 2025 with no comments, across spec refreshes up to Iceberg 1.11.
There is therefore no Polaris design to borrow from or contrast against: it has
neither the load-amplification problem nor the ordering problem, because it does
not plan scans server side.

**Iceberg's own reference handler: in-memory, single node.** The closest existing
Java implementation is `CatalogHandlers` with `RESTCatalogAdapter`, the fixture
behind the `iceberg-rest-fixture` image. It keeps plans in a static
`InMemoryPlanningState`, hands out plan tasks derived from a plan id and an index,
and resolves them by map lookup. That inverts this design's trade-off exactly:
redemption is free, but a plan task means nothing after a restart or on another
process, and the fixture is not intended for a replicated deployment. It also
consults `min-rows-requested`, which this design does not (Non-Goal 2).

So the comparison is: redemption cost is the price this design pays for working
across replicas and restarts, and the reference implementation pays the opposite
price. Neither problem is solved for free by an existing implementation.

### 8.5 Manifest entry-range plan tasks, the follow-up that removes re-planning

The measures in §5.15 shrink the re-planning multiplier without removing it. The
way to remove it is to change what a plan task *is*: instead of a range of
positions in a global task list, have it name the work directly, as manifest
entry ranges. Redeeming it then reads those manifest slices rather than
re-planning the snapshot, so the cost is proportional to the batch and not to the
plan, on any replica, with no shared cache. It also retires §5.6: manifests are
immutable and a position inside one is fixed, so there is no global order left to
stabilise, and the duplicate-entry hazard §5.6 describes cannot arise.

@lasdf1234 proposed a payload for this on
[#12241](https://github.com/apache/gravitino/pull/12241#issuecomment-5178530018),
and the batching it describes is the model below. What follows records the payload
this design would use, the reasons it keeps three fields that proposal dropped,
and the one piece of work that makes it more than a refactor.

#### 8.5.1 Batching model

A batch is a run of manifest entries, which may span manifests. With a batch size
of 100 over manifests holding 60, 60, 60 and 70 entries:

```text
Batch 0 (100):  m0[0,60) + m1[0,40)             → inline in the plan response
Batch 1 (100):  m1[40,60) + m2[0,60) + m3[0,20) → plan task
Batch 2 (50):   m3[20,70)                       → plan task
```

Ranges are half-open. `ManifestFile.existingFilesCount()` and
`addedFilesCount()` give the per-manifest counts without reading a manifest,
though they count entries before the scan filter is applied, so a batch of 100
entries can yield fewer than 100 tasks. Batch size becomes a bound rather than an
exact count.

#### 8.5.2 Payload

```json
{
  "table": "db.t",
  "snapshot-id": 42,
  "scan": { "filter": "…", "case-sensitive": true, "select": [], "stats-fields": [] },
  "ranges": [
    { "manifest": "s3://wh/db/t/metadata/snap-42-m1.avro", "entry-start": 40, "entry-end": 60 },
    { "manifest": "s3://wh/db/t/metadata/snap-42-m2.avro", "entry-start": 0, "entry-end": 60 },
    { "manifest": "s3://wh/db/t/metadata/snap-42-m3.avro", "entry-start": 0, "entry-end": 20 }
  ]
}
```

`ranges` is the new part. `table`, `snapshot-id` and `scan` are carried over from
the current payload, and each earns its place:

| Field | Why the ranges alone are not enough |
| ----- | ----------------------------------- |
| `snapshot-id` | Delete manifests are reachable only through a snapshot (`Snapshot.deleteManifests`); a manifest path does not say which snapshot referenced it. Entries also inherit their sequence numbers from the `ManifestFile` in the snapshot's manifest list, and `ManifestFiles.read` takes that `ManifestFile`, not a path. Without it, a merge-on-read table returns tasks with no deletes attached and the client reads deleted rows. |
| `scan` | `ManifestReader.iterator()` yields entries *after* `filterRows`, `filterPartitions` and `caseSensitive` are applied, so an entry position means one thing with the client's filter and another without it. `residual-filter`, which the engine applies per data file, comes from the same filter through `ResidualEvaluator`; with no filter the server would have to emit always-true residuals. |
| `table` | Authorization is evaluated from the table in the request path, but manifest paths arrive in the request body. Without binding the plan task to a table and checking each manifest against that snapshot's manifest list, a caller authorized on one table could name another table's manifest and receive its data file paths and column statistics. |

#### 8.5.3 What it costs

Plan tasks stop being a fixed size. One batch can name several manifests, at
roughly 100 bytes of object-store path each, and tables written by frequent small
commits have many small manifests, so a plan task can reach kilobytes and the plan
response carries all of them. Today a plan task is a few hundred bytes whatever
the plan size (§5.5.1). Capping the manifests named by one plan task bounds it, at
the price of uneven batches.

#### 8.5.4 The work that is not a refactor

The data side is buildable on public API: `ManifestFiles.read`,
`ManifestReader.filterRows`/`select`/`caseSensitive`, and `ResidualEvaluator`.

Delete attachment is not. `DeleteFileIndex`, which matches delete files to data
files by partition and sequence number, is package-private in `iceberg-core`, so
this design would have to reimplement those rules - equality deletes, positional
deletes, deletion vectors - where a subtle mistake means returning deleted rows.

A staged first cut avoids that risk while taking most of the win: use entry ranges
when the pinned snapshot has no delete manifests, and fall back to the re-planning
path in §5.5 when it does. Copy-on-write tables, which are the common case and the
ones with the largest plans, get redemption proportional to the batch immediately;
merge-on-read keeps today's behaviour until Iceberg exposes delete indexing or the
rules are ported deliberately, with tests.

#### 8.5.5 Why this need not block the current change

A plan task is opaque and nothing persists it: no client stores one, and none
outlives the scan that produced it. Its encoding can therefore change in any later
release without a migration or a compatibility flag, which is what makes the
offset form a reasonable thing to ship first and this a follow-up rather than a
prerequisite.

---

## 9. Task Breakdown

Delivered in [#12194](https://github.com/apache/gravitino/pull/12194):

- [x] Add `fetchScanTasks` to `IcebergTableOperationDispatcher` and implement it in `IcebergTableOperationExecutor`
- [x] Add the `POST {table}/tasks` endpoint in `IcebergTableOperations` with metrics and authorization
- [x] Map `NoSuchPlanTaskException` to `404` in `IcebergExceptionMapper`
- [x] Advertise `Endpoint.V1_FETCH_TABLE_SCAN_PLAN_TASKS` in `IcebergConfigOperations`
- [x] Add `IcebergFetchScanTasks{Pre,,Failure}Event` and `OperationType.FETCH_SCAN_TASKS`, and wire them in `IcebergTableEventDispatcher` and `IcebergTableHookDispatcher`
- [x] Add `PlanTaskCodec` (encode, decode, table match)
- [x] Add `scan-plan-task-batch-size` to `IcebergConstants` and `IcebergConfig`
- [x] Split plan responses into inline tasks plus `plan-tasks` in `CatalogWrapperForREST.planTableScan`
- [x] Pin the planned snapshot and order planned tasks totally
- [x] Resolve a plan task to its batch in `CatalogWrapperForREST.fetchScanTasks`
- [x] Forward `POST {table}/tasks` upstream in `FederatedCatalogWrapper`
- [x] Unit tests: `TestPlanTaskCodec`, `TestScanPlanTaskBatching`
- [x] REST tests: `TestIcebergFetchScanTasksEndpoint`, `TestIcebergTableOperations`, `TestIcebergConfig`
- [x] Federation test in `TestCatalogWrapperForREST`
- [x] Document the endpoint, plan task semantics and the new property in `docs/iceberg-rest-service.md`

Follow-ups, each its own issue:

- [ ] Decide whether the scan plan cache should be enabled by default (§8.1)
- [ ] Coalesce concurrent redemptions of one plan on a replica, so the concurrent fetches of one scan cost one plan (§5.15)
- [ ] Implement manifest entry-range plan tasks, copy-on-write first (§8.5), and raise the delete-indexing gap with Iceberg upstream
- [ ] Integration test with pyiceberg `scan-planning-mode=server` against a multi-batch table
- [ ] Honor `min-rows-requested` when sizing batches (Non-Goal 2)
- [ ] Ship a shared `ScanPlanCache` implementation so plan task redemption avoids re-planning across replicas (Non-Goal 3)
- [ ] Revisit the default batch size if real workloads show 100 causing problems (§8.2)
