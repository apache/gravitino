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

# Design of Lineage Store and Query API in Gravitino

## Background

Gravitino receives OpenLineage `RunEvent`s at `POST /api/lineage`, validates and authorizes every
input and output dataset, runs the event through a `LineageProcessor`, and hands it to one or more
`LineageSink`s on isolated asynchronous queues.

```
Spark + Gravitino OpenLineage plugin
        |  RunEvent (job.namespace = metalake, dataset name = catalog.schema.table)
        v
POST /api/lineage -> LineageEventValidator -> LineageAuthorizationExecutor
        v
LineageService -> LineageProcessor -> LineageSinkManager -> [ log sink | http sink ]
```

The two sinks that ship are a log file and an HTTP forwarder. Nothing keeps an event, so nothing
can be asked of Gravitino afterwards:

| Question | Today |
|----------|-------|
| Which jobs and tables read `sales.orders`? | Not answerable |
| Which columns derive from `orders.amount`? | Not answerable |
| What is affected if I drop or retype this column? | Not answerable |
| Where did this PII-tagged column flow? | Not answerable |

The expensive part is already done and then discarded. The Gravitino Spark plugin extracts
column-level lineage and names datasets with Gravitino identifiers across Hive, Iceberg, Paimon,
JDBC, fileset and model catalogs. Operators who want to use that information must run a second
system such as Marquez behind the HTTP sink, and that system, not Gravitino, then owns the graph.
It has no knowledge of Gravitino privileges, tags, owners or renames.

Related discussions: [#12835](https://github.com/apache/gravitino/discussions/12835) (this
proposal) and [#10850](https://github.com/apache/gravitino/discussions/10850) (OpenLineage dataset
identifier mapping).

---

## Goals

1. **Opt-in persistence**: With `gravitino.lineage.sinks` containing `store`, dataset-level and
   column-level edges from accepted `RunEvent`s are persisted in the relational backend. With the
   default configuration no table is written and behavior is unchanged.
2. **Traversal API**: A REST endpoint returns the upstream and/or downstream graph of a table,
   view, fileset, topic or model to a bounded depth, at dataset or column granularity.
3. **Authorization-preserving reads**: A caller never receives the name of a dataset they are not
   allowed to load. Such nodes are returned masked so the graph shape stays correct.
4. **Idempotent, multi-node-safe ingestion**: Replaying the same event, or ingesting it on two
   servers, produces the same stored state.
5. **Bounded growth**: Storage grows with the number of distinct job-to-dataset relationships, not
   with the number of runs, and edges not observed within a retention window are removed.
6. **Lifecycle consistency**: A rename or drop performed through Gravitino is reflected in the
   stored graph without waiting for new lineage events.
7. **Client coverage**: The read API is available from the Java client, the Python client and the
   MCP server.

---

## Non-Goals

1. **Server-side lineage inference**: Gravitino will not parse SQL or derive lineage from metadata
   access events. Engine-side OpenLineage integrations remain the only producer, as argued in
   #10850.
2. **Identifier mapping for foreign producers**: Mapping datasets emitted by non-Gravitino
   OpenLineage integrations (for example `s3://bucket/path`) onto Gravitino objects belongs to
   #10850 and to `LineageProcessor`. This design stores such datasets as unresolved nodes.
3. **Raw event archive**: The full `RunEvent` JSON is not stored. The log and HTTP sinks already
   cover archival and forwarding.
4. **Per-run history**: Which run read or wrote which dataset is deferred to a later phase. It is
   the part whose volume grows with run count and needs its own retention design.
5. **Change blocking**: Rejecting a drop or alter because of downstream lineage is out of scope.
   Observed lineage is evidence, not a declared contract. Declared dependencies are handled by the
   Semantic Model source associations (#13334).
6. **Web UI graph**: A visual graph is useful only once the API semantics are settled. It is listed
   as a later phase.

---

## Solution Investigations

### Where lineage is stored

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| A. Append raw `RunEvent` JSON, build the graph at query time | Trivial write path, lossless | Every traversal scans events; volume grows with run count; no index on columns | Rejected |
| B. Aggregated edge tables in the relational backend, behind a `LineageStore` SPI | Indexed traversal; volume bounded by distinct relationships; no new infrastructure; upserts are naturally idempotent | Loses per-run detail; deep traversals cost one query per level | **Chosen** |
| C. Lineage edges as first-class `Entity` in `EntityStore` | Reuses existing CRUD plumbing | Pulls append-heavy data into the entity cache and lock machinery that #13730 and #13621 are reworking; edges have no name, owner or namespace | Rejected |
| D. Require an external graph store or Marquez | Purpose-built traversal | A second system to deploy; cannot apply Gravitino privileges; is the status quo | Rejected as default. The SPI leaves room for it. |

### How the store is wired into the framework

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| A new `LineageSink` named `store` | No change to `LineageService`; inherits the async queue, high-watermark back-pressure and per-sink isolation; opt-in through the existing `sinks` key | Persistence is asynchronous, so a `201` does not mean the edge is queryable yet | **Chosen** |
| A synchronous store step in `LineageService.dispatchLineageEvent` | Read-your-writes | Puts a database transaction on the ingestion request path; a slow database stalls every producer | Rejected |

### How traversal is executed

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| Recursive CTE in SQL | One round trip | Different syntax and limits on H2, MySQL and PostgreSQL; cycle handling and node caps are awkward; authorization cannot be applied per level | Rejected |
| Breadth-first search in the server, one batched query per level | Portable; visited-set handles cycles; node cap and depth cap are simple | `depth` round trips | **Chosen** |

### How a dataset is identified

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| Gravitino entity ID | Survives renames for free | Unresolved and external datasets have no ID; #13476 shows IDs of non-managed entities are not stable | Rejected |
| `(metalake, namespace, name, type)` as emitted, with renames and drops applied from Gravitino events | Works for resolved and unresolved datasets alike; matches what `LineageAuthorizationExecutor` already resolves | Changes made directly in the underlying catalog are not followed | **Chosen** |

---

## Proposal

### Configuration

The store is a sink, so it uses the existing per-sink prefix `gravitino.lineage.${sinkName}.`.

| Configuration item | Description | Default |
|--------------------|-------------|---------|
| `gravitino.lineage.sinks` | Add `store` to enable persistence, for example `log,store`. | `log` |
| `gravitino.lineage.store.sinkClass` | `org.apache.gravitino.lineage.store.LineageStoreSink` | none |
| `gravitino.lineage.store.impl` | `LineageStore` implementation class. | `JdbcLineageStore` |
| `gravitino.lineage.store.retentionDays` | Edges whose `last_seen` is older than this are deleted. `0` disables cleanup. | `90` |
| `gravitino.lineage.store.maxDepth` | Upper bound accepted for the `depth` query parameter. | `10` |
| `gravitino.lineage.store.maxNodes` | Maximum nodes returned by one traversal. | `1000` |

### Data model

Four tables, added through the usual `scripts/{h2,mysql,postgresql}` schema and upgrade scripts for
2.0.0. POs and mappers follow the existing per-backend SQL provider pattern in
`core/.../storage/relational`.

```
lineage_job_meta      (job_id PK, metalake_id, job_namespace, job_name,
                       last_run_id, last_seen,
                       UNIQUE (metalake_id, job_namespace, job_name))

lineage_dataset_meta  (dataset_id PK, metalake_id, dataset_namespace, dataset_name,
                       dataset_type,            -- TABLE | VIEW | FILESET | TOPIC | MODEL
                       dropped_at,              -- 0 while live
                       first_seen, last_seen,
                       UNIQUE (metalake_id, dataset_namespace, dataset_name, dataset_type))

lineage_edge          (job_id, dataset_id, direction,   -- INPUT | OUTPUT
                       first_seen, last_seen,
                       PRIMARY KEY (job_id, dataset_id, direction),
                       INDEX (dataset_id, direction))

lineage_column_edge   (job_id, output_dataset_id, output_column,
                       input_dataset_id, input_column,
                       transformation_type,     -- DIRECT | INDIRECT, from the OpenLineage facet
                       transformation_subtype,
                       first_seen, last_seen,
                       UNIQUE (job_id, output_dataset_id, output_column,
                               input_dataset_id, input_column),
                       INDEX (input_dataset_id, input_column),
                       INDEX (output_dataset_id, output_column))
```

- The metalake of an event is `job.namespace`, the same rule `LineageAuthorizationExecutor` uses.
- `dataset_type` comes from the `datasetType` facet and defaults to `TABLE`, again matching the
  executor.
- A dataset whose namespace equals the metalake name and whose name parses as a Gravitino
  identifier is *resolved*. Anything else is *unresolved* and is stored exactly as emitted.
- All timestamps are the OpenLineage `eventTime`, not server time, so replay is deterministic.
- There are no counters. `last_seen = max(last_seen, eventTime)` makes every write idempotent.

### SPI

```java
/** Stores lineage relationships and serves bounded traversals over them. */
public interface LineageStore extends Closeable {

  void initialize(Map<String, String> configs);

  /** Upserts the jobs, datasets and edges described by one event. Must be idempotent. */
  void record(String metalake, OpenLineage.RunEvent event);

  /** Returns the jobs and datasets directly connected to the given datasets. */
  List<LineageEdge> neighbors(
      String metalake, Collection<LineageDataset> frontier, LineageDirection direction);

  /** Returns the column edges directly connected to the given columns. */
  List<LineageColumnEdge> columnNeighbors(
      String metalake, Collection<LineageColumn> frontier, LineageDirection direction);

  /** Applies a rename, merging into the target dataset if one already exists. */
  void renameDataset(String metalake, LineageDataset from, LineageDataset to);

  /** Marks a dataset as dropped. Edges stay until retention removes them. */
  void markDropped(String metalake, LineageDataset dataset, long droppedAt);

  /** Deletes edges, and then orphaned jobs and datasets, last seen before the cutoff. */
  int expire(long cutoffMillis);
}
```

The store exposes one-hop lookups only. Traversal, caps and authorization live in one place above
it, so an alternative store does not have to reimplement them.

### REST API

The read API hangs off the existing metadata-object path, next to tags and statistics.

#### GET /api/metalakes/{metalake}/objects/{type}/{fullName}/lineage

**Request:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `type` | path | yes | `table`, `view`, `fileset`, `topic` or `model` |
| `fullName` | path | yes | `catalog.schema.name` |
| `direction` | query | no | `upstream`, `downstream` or `both`. Default `both`. |
| `depth` | query | no | Number of dataset hops. Default `3`, range `1..maxDepth`. |
| `column` | query | no | When set, traversal follows column edges starting at this column. |

**Response:** `200 OK`

```json
{
  "code": 0,
  "lineage": {
    "root": "n1",
    "nodes": [
      {"id": "n1", "kind": "DATASET", "type": "TABLE", "name": "lake.sales.orders",
       "resolved": true, "dropped": false, "lastSeen": "2026-11-10T08:00:00Z"},
      {"id": "n2", "kind": "JOB", "namespace": "prod", "name": "daily_revenue",
       "lastSeen": "2026-11-10T08:00:00Z"},
      {"id": "n3", "kind": "DATASET", "masked": true}
    ],
    "edges": [
      {"from": "n1", "to": "n2", "lastSeen": "2026-11-10T08:00:00Z"},
      {"from": "n2", "to": "n3", "lastSeen": "2026-11-10T08:00:00Z"}
    ],
    "columnEdges": [],
    "truncated": false
  }
}
```

With `column` set, `columnEdges` is populated:

```json
{"from": {"node": "n1", "column": "amount"},
 "to": {"node": "n3", "column": "revenue"},
 "job": "n2", "transformation": "DIRECT", "subtype": "AGGREGATION",
 "lastSeen": "2026-11-10T08:00:00Z"}
```

**Behavior:**

- The caller must be allowed to load the root object. Otherwise the standard `403` is returned.
  A root with no stored lineage returns `200` with only the root node.
- `400` for an unsupported `type`, a `depth` outside the range, or a `column` that does not exist
  on the root.
- `501` when the `store` sink is not enabled.
- `truncated` is `true` when `maxNodes` stopped the traversal. Nodes nearer the root are kept.
- Results reflect events already drained from the sink queue. Ingestion is asynchronous.
- Pre, success and failure events are emitted through the existing listener framework.

`POST /api/lineage` is unchanged.

### Authorization on read

Lineage reveals the existence and names of datasets, so every node is checked, not only the root.

| Node | Rule |
|------|------|
| Resolved dataset | Named only if the caller passes the existing load expression for that object type. Otherwise `{"kind": "DATASET", "masked": true}`. |
| Unresolved dataset | Named only for the metalake owner. There is no Gravitino object to authorize against. |
| Dropped dataset | Same rule as unresolved. |
| Job | Named if at least one dataset adjacent to it is named for the caller. |

Traversal continues through masked nodes. Stopping at them would make impact analysis silently
incomplete, and the alternative of dropping them would join unrelated nodes. Masked nodes carry no
name, type, timestamp or column names, and column edges touching them omit the column. Checks are
batched per BFS level through the same metadata filtering used by list endpoints.

### Client and MCP API

| Surface | Addition |
|---------|----------|
| Java API | `SupportsLineage#getLineage(LineageQuery query)` returning `LineageGraph`, reached through `supportsLineage()` on tables, views, filesets, topics and models, the same way `supportsStatistics()` and `supportsTags()` are |
| Python client | The matching `supports_lineage().get_lineage(direction, depth, column)` |
| MCP server | Tool `get_lineage(metalake, object_type, full_name, direction, depth, column)` in `mcp_server/tools/lineage.py` |

No existing signature changes.

### User process

1. The operator adds `store` to `gravitino.lineage.sinks`, sets `gravitino.lineage.store.sinkClass`
   and restarts the server. Schema scripts for 2.0.0 have created the four tables.
2. Spark jobs run with the Gravitino OpenLineage plugin, as documented today. No job changes.
3. Before altering `lake.sales.orders.amount`, a data engineer calls
   `GET /api/metalakes/lake/objects/table/lake.sales.orders/lineage?direction=downstream&column=amount`
   and receives the jobs and columns that consume it.
4. An agent answers "where does this PII column flow" by calling the MCP `get_lineage` tool and
   sees only the datasets its identity may load.
5. A table is renamed through Gravitino. The next lineage query returns it under its new name with
   its edges intact.

### Implementation process

**Write path**

```
LineageSinkManager queue -> LineageStoreSink.sink(event)
   1. skip unless eventType is COMPLETE or RUNNING
   2. metalake = job.namespace
   3. LineageStore.record(metalake, event), one transaction:
        upsert lineage_job_meta
        upsert lineage_dataset_meta for inputs and outputs
        upsert lineage_edge (INPUT per input, OUTPUT per output)
        upsert lineage_column_edge from each output's columnLineage facet
```

- `START`, `FAIL` and `ABORT` events are skipped: a run that did not finish did not establish a
  data flow. `RUNNING` is kept so streaming jobs, which never complete, are recorded.
- Upserts use the per-backend SQL providers (`ON DUPLICATE KEY UPDATE`, `ON CONFLICT`, `MERGE`).
  Two servers recording the same event converge on the same rows.
- A failed transaction is logged and dropped, matching the other sinks. The next run of the job
  re-establishes the edge.

**Read path**

```
LineageQueryOperations (REST) -> authorize root
  -> LineageGraphService
       frontier = {root}
       repeat depth times, while nodes < maxNodes:
         edges    = store.neighbors(frontier, direction)   // dataset -> job -> dataset
         frontier = newly discovered datasets not yet visited
       mask nodes per the authorization rules
  -> LineageGraphDTO
```

One dataset hop is two `lineage_edge` lookups, dataset to job and job to dataset, both served by
indexes. Column traversal uses `lineage_column_edge` directly and never needs the job hop.

**Lifecycle**

A `LineageLifecycleListener` registered on the existing event bus, only when the store is enabled:

| Gravitino event | Store action |
|-----------------|--------------|
| Table, view, fileset, topic or model renamed | `renameDataset` |
| Schema or catalog renamed | `renameDataset` for every stored dataset under the old prefix |
| Object dropped | `markDropped` |
| Metalake dropped | delete all rows for the metalake |

Changes made directly in an underlying catalog are not seen. The stale node keeps its old name and
ages out through retention, and the new name appears with the next run.

**Retention**

A scheduled task inside `JdbcLineageStore`, modeled on `RelationalGarbageCollector`, calls
`expire(now - retentionDays)` once a day. The delete is idempotent, so every server in a multi-node
deployment may run it. The job framework is deliberately not used: it would make lineage cleanup
depend on a configured job executor.

**Sizing**

Rows scale with distinct `(job, dataset)` and `(job, output column, input column)` pairs. The
OpenLineage Spark integration derives job names from the application name and plan node, so ad hoc
queries create many short-lived jobs. Retention is what bounds those, which is why it ships in the
first phase.

### Backward compatibility

- Default configuration is untouched: no new sink, no writes, read endpoint returns `501`.
- New tables only. No existing table or column changes.
- No change to `LineageSink`, `LineageProcessor`, `LineageSource` or `POST /api/lineage`.

### Open questions

1. **Unresolved and dropped nodes**: Is "metalake owner only" the right visibility, or should it
   follow an existing privilege?
2. **Traversing masked nodes**: This design continues through them. A stricter alternative stops
   and sets a `partial` flag.
3. **Module placement**: POs and mappers are proposed for `core` beside the other relational
   tables, with the sink, service and REST resource in `lineage`. The alternative keeps everything
   in `lineage` with its own mapper provider.
4. **External changes**: Should the reconciliation work in #13313 also mark lineage datasets whose
   source object has disappeared?

---

## Task Breakdown

### Phase 1: Store and dataset-level traversal

- [ ] Add `lineage_*` tables to `scripts/{h2,mysql,postgresql}` schema and upgrade scripts
- [ ] Add POs, mappers and per-backend SQL providers in `core`
- [ ] Define `LineageStore` SPI and model classes in `lineage`
- [ ] Implement `JdbcLineageStore` write path (`record`) with idempotency and concurrency tests
- [ ] Implement `LineageStoreSink` and its configuration
- [ ] Implement `neighbors` and `LineageGraphService` with depth cap, node cap and cycle handling
- [ ] Add `GET /api/metalakes/{metalake}/objects/{type}/{fullName}/lineage` with node masking
- [ ] Implement `expire` and the scheduled retention task
- [ ] Implement `LineageLifecycleListener` for rename and drop (parallel)
- [ ] Update `docs/open-api/*.yaml` and validate with `./gradlew :docs:build` (parallel)
- [ ] Integration test: Spark job with the OpenLineage plugin, then query lineage on H2, MySQL and
      PostgreSQL

### Phase 2: Column lineage and clients

- [ ] Record `columnLineage` facets into `lineage_column_edge`
- [ ] Implement `columnNeighbors` and the `column` query parameter
- [ ] Add Java client support in `clients/client-java/` (parallel)
- [ ] Add Python client support in `clients/client-python/` (parallel)
- [ ] Add MCP tool `get_lineage` in `mcp-server/` (parallel)
- [ ] Update `docs/lineage/` with configuration, API and authorization behavior

### Phase 3: Follow-ups, each needing its own design

- [ ] Per-run history with its own retention
- [ ] Lineage graph in `web-v2`
- [ ] Tag and classification propagation along column edges
