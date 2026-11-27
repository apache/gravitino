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

# Design of AutoMQ Table Topic Support and Iceberg Linkage in Gravitino

| Field  | Value |
| ------ | ----- |
| Status | Draft |
| Author | @brucearctor |
| Issue  | [#14969](https://github.com/apache/gravitino/issues/14969) |
| Scope  | `catalogs/catalog-kafka`, `mcp-server`, `docs` |

> **Note:** This document specifies the target design and configuration contract for [#14969](https://github.com/apache/gravitino/issues/14969). Implementation is split cleanly across the `catalog-kafka` and `mcp-server` tasks in [Task Breakdown](#task-breakdown).

---

## Background

Streaming engines compatible with Apache Kafka — specifically AutoMQ (1.5.x+) **Table Topics** — continuously materialize topic event streams directly into Apache Iceberg tables managed by an Iceberg REST catalog such as Gravitino's Iceberg REST server (`lakehouse-iceberg` catalog). Because AutoMQ exposes Table Topic configuration through standard Kafka topic configs (`automq.table.topic.*`) over Kafka `AdminClient`, operators can govern both the source messaging topic and the target Iceberg table inside the same Gravitino metalake.

Currently, `catalog-kafka` treats Kafka topics and their materialized Iceberg tables as disconnected assets:

1. **No Typed Topic Property Metadata or Validation**: `KafkaTopicPropertiesMetadata` only declares `partition-count` and `replication-factor`. Clients configuring `automq.table.topic.*` keys receive no schema validation, enum/type checking, or cross-property verification before calling Kafka `AdminClient`.
2. **No Catalog Default Namespace or Iceberg Linkage Metadata**: `KafkaCatalogPropertiesMetadata` only declares `bootstrap.servers`, and `KafkaCatalogOperations` provides no linkage from a Table Topic to its target `<metalake>.<iceberg-catalog>.<namespace>.<table>` identifier.
3. **No Structured AI / MCP Traversal**: `mcp-server` returns raw topic property maps from `load_topic` without resolving the target Iceberg coordinates needed for `get_table_by_fqn`.

```
Gravitino Metalake
  ├── messaging / kafka catalog (`catalog-kafka`)
  │     └── topic `orders` (properties: automq.table.topic.* via AdminClient)
  │           ✗ No typed validation, no default Iceberg namespace, no resolved FQN linkage
  └── lakehouse-iceberg catalog (`lakehouse-iceberg`)
        └── table `<namespace>.orders` (async-created & committed by AutoMQ brokers)
```

---

## Goals

1. **Typed AutoMQ Table Topic Configuration & Validation**: Add typed `PropertyEntry` descriptors, normalization, and cross-property checks (such as requiring `automq.table.topic.id.columns` when enabled with upsert or CDC mode) in `KafkaTopicPropertiesMetadata`.
2. **Engine-Agnostic Catalog Defaults & Synthetic Iceberg Linkage**: Add optional `table-topic.iceberg-catalog` and `table-topic.default-namespace` catalog properties in `KafkaCatalogPropertiesMetadata`, and compute reserved read-only `table-topic.*` Iceberg coordinates dynamically on `createTopic`, `loadTopic`, and `alterTopic` without persisting synthetic keys to Kafka or `EntityStore`.
3. **Zero Regression for Standard Apache Kafka Clusters**: Leave behavior unchanged when `automq.table.topic.*` properties are absent, avoid pushing Gravitino-only keys to `AdminClient`, and strip blank `DEFAULT_CONFIG` `automq.table.topic.*` broker defaults on topic reads.
4. **Structured MCP Discovery (Phase 2)**: Expose `get_topic_iceberg_linkage` and document linkage discovery in `mcp-server` so AI agents can resolve Iceberg table FQNs directly from Kafka topics.

---

## Non-Goals

1. **Synchronous Iceberg Table Provisioning or DDL Sync**: AutoMQ brokers create and evolve the target Iceberg table asynchronously on the first commit interval after valid records arrive. Gravitino will not create, alter, or drop Iceberg tables during topic mutations.
2. **Cross-Catalog Existence or RBAC Checks on Topic Reads**: `table-topic.iceberg-ident` is a declarative coordinate pointer derived from catalog/topic config. Verifying that the Iceberg catalog or table exists and enforcing Iceberg privileges remain the responsibility of the Iceberg catalog at table access time.
3. **Separate `catalog-automq` Provider**: AutoMQ uses the standard Kafka wire protocol and `AdminClient` topic config API; duplicating `catalog-kafka` into a separate provider module is unnecessary.
4. **OpenLineage or Persisted Lineage Graph Edges in V1**: Lineage emission or persisted topic-to-table relations in `lineage/` can build on the `table-topic.*` coordinate model in a future PR without changing topic properties.
5. **Non-AutoMQ Vendor Config Keys in V1**: Engine-specific keys for Redpanda Iceberg Topics or Confluent Tableflow are out of scope for V1, though the catalog-level `table-topic.*` namespace is intentionally engine-agnostic for future reuse.

---

## Solution Investigations

### 1. Provider Placement (`catalog-kafka` vs. Separate Provider)

| Option | Description | Pros | Cons | Decision |
| ------ | ----------- | ---- | ---- | -------- |
| **Option A: New `catalog-automq` / `catalogs-contrib` Provider** | Fork or wrap `catalog-kafka` with a dedicated `provider = "automq"`. | Isolates AutoMQ-specific topic keys from stock Kafka metadata. | Duplicates `KafkaCatalogOperations`, broker `AdminClient` lifecycle, and authorization hooks; forces migration for existing `kafka` catalogs. | Rejected |
| **Option B: Untyped Pass-Through Only** | Document `automq.table.topic.*` in docs and pass arbitrary strings through `KafkaCatalogOperations`. | Minimal code in `catalog-kafka`. | No validation of booleans, positive intervals, or `upsert`/`cdc` missing `id.columns`; no Iceberg linkage resolution. | Rejected |
| **Option C: Extend `catalog-kafka` with Opt-In `table-topic.*` Linkage + Typed Configs** | Add optional `table-topic.*` catalog properties and `automq.table.topic.*` topic descriptors inside `catalog-kafka`. | Works seamlessly on existing Kafka catalogs; engine-agnostic `table-topic.*` linkage prefix; zero `core` changes. | Includes AutoMQ config keys in `KafkaTopicPropertiesMetadata` (strictly optional). | **Chosen** |

### 2. Iceberg Linkage Representation

| Option | Description | Pros | Cons | Decision |
| ------ | ----------- | ---- | ---- | -------- |
| **Option A: First-Class `Topic` Interface Fields (`api` + `core`)** | Add Iceberg linkage fields to `Topic`, `TopicDTO`, and `TopicEntity` storage. | Strongly typed API object on every `Topic`. | Couples messaging `api` to Iceberg concepts and requires DTO/schema updates across clients. | Rejected |
| **Option B: Dynamic Cross-Catalog `CatalogManager` Lookup on `loadTopic`** | Query `CatalogManager` / `TableDispatcher` from `catalog-kafka` on every `loadTopic`. | Checks live Iceberg table existence. | Introduces cross-catalog lock ordering and fails whenever AutoMQ has not committed the first Iceberg snapshot yet. | Rejected |
| **Option C: Synthetic Reserved Properties (`table-topic.*`)** | Compute 5 reserved read-only `PropertyEntry.stringReservedPropertyEntry` keys on `createTopic`, `loadTopic`, and `alterTopic`. | Zero `api`/`core`/`EntityStore` changes; reflects external broker changes (e.g., lazy `loadTopic` import) automatically; strips before `AdminClient` calls. | Callers cloning topic property maps into `createTopic` must omit read-only `table-topic.*` keys. | **Chosen** |

### 3. Client-Side Validation Strictness vs. AutoMQ Version Drift

| Option | Description | Pros | Cons | Decision |
| ------ | ----------- | ---- | ---- | -------- |
| **Option A: Validate Known Constraints, Leave Unknown Keys Open** | Enforce typed validation (`commit.interval.ms > 0`, booleans, `id.columns`, documented enum modes, cross-property `id.columns` check) while keeping unknown `automq.table.topic.*` keys, `partition.by`, and `transform.value.type` as pass-through/open strings. | Catches common misconfigurations early with `400 Bad Request` while tolerating new AutoMQ parameters. | New values on closed enums (`schema.type`, `errors.tolerance`, `convert.*.type`) require updating `KafkaTopicPropertiesMetadata` (trivial one-file switch to non-blank string if maintainers prefer). | **Chosen** |
| **Option B: Strict Reject of All Unknown `automq.table.topic.*` Keys** | Block any property not listed in `KafkaTopicPropertiesMetadata`. | Rejects all typos. | Breaks upgrades whenever newer AutoMQ broker versions introduce additional `automq.table.topic.*` keys. | Rejected |

### 4. MCP Server Surfacing (Phase 2 Separation)

| Option | Description | Pros | Cons | Decision |
| ------ | ----------- | ---- | ---- | -------- |
| **Option A: Rely on `load_topic` Alone** | Update `load_topic` docstring and let callers inspect `properties["table-topic.*"]`. | No extra MCP tool in tool schema. | Exposes flat string maps; agents lack normalized boolean/field extraction and clear disabled vs. enabled structure. | Rejected |
| **Option B: Add Read-Only `get_topic_iceberg_linkage` Tool (Separate Phase / PR)** | Add an idempotent read-only MCP tool wrapping `load_topic` that returns a structured JSON dictionary (`iceberg_ident`, `schema_type`, `id_columns`, etc.). | Clean one-hop discovery to `get_table_by_fqn`; keeps `mcp-server` decoupled so it can land independently after `catalog-kafka`. | Adds one tool under tag `topic`. | **Chosen** |

---

## Proposal

### Catalog Properties (`KafkaCatalogPropertiesMetadata`)

| Property Name | Type | Description | Default | Required |
| ------------- | ---- | ----------- | ------- | -------- |
| `table-topic.iceberg-catalog` | Non-blank string (trimmed, mutable) | Target Iceberg catalog name in the same metalake where Table Topics materialize. | `(none)` | No |
| `table-topic.default-namespace` | Non-blank string (trimmed, mutable) | Fallback Iceberg namespace written to `automq.table.topic.namespace` when creating or enabling a Table Topic without an explicit namespace. | `(none)` | No |

### Topic Properties and Validation (`KafkaTopicPropertiesMetadata`)

#### Configured AutoMQ Properties (Optional, Mutable)

| Property Name | Validation Rules |
| ------------- | ---------------- |
| `automq.table.topic.enable` | Strict boolean (`true` or `false`, case-insensitive, trimmed). |
| `automq.table.topic.commit.interval.ms` | Long `> 0`. |
| `automq.table.topic.namespace` | Non-blank string (trimmed). |
| `automq.table.topic.schema.type` | Locale-safe lowercase/uppercase enum: `schemaless`, `schema`, `record_name`, `topic_name`. |
| `automq.table.topic.id.columns` | Non-blank column list: `col1,col2` or `[col1, col2]`; rejects empty elements, unbalanced/nested brackets, and duplicates. |
| `automq.table.topic.partition.by` | Non-blank string (trimmed). |
| `automq.table.topic.upsert.enable` | Strict boolean (`true` or `false`). |
| `automq.table.topic.cdc.field` | Non-blank string (trimmed). |
| `automq.table.topic.errors.tolerance` | Locale-safe enum: `none`, `invalid_data`, `all`. |
| `automq.table.topic.convert.key.type` | Locale-safe enum: `raw`, `string`, `by_schema_id`, `by_latest_schema`. |
| `automq.table.topic.convert.value.type` | Locale-safe enum: `raw`, `string`, `by_schema_id`, `by_latest_schema`. |
| `automq.table.topic.transform.value.type` | Non-blank string (trimmed, e.g., `debezium`). |

**Cross-Property Constraint (`validateTableTopicProperties`)**: When `automq.table.topic.enable=true` and either `automq.table.topic.upsert.enable=true` or `automq.table.topic.cdc.field` is non-blank, `automq.table.topic.id.columns` must be present and valid (`IllegalArgumentException` → `400 Bad Request`). Unknown `automq.table.topic.*` keys pass through unchecked to the broker.

#### Synthetic Read-Only Linkage Properties (Reserved, Computed)

Populated only when `automq.table.topic.enable=true` on `createTopic`, `loadTopic`, and `alterTopic` responses (never pushed to Kafka `AdminClient` or stored in `EntityStore`; direct client `set`/`alter`/`remove` is rejected with `IllegalArgumentException`):

| Property Name | Computed Value |
| ------------- | -------------- |
| `table-topic.enabled` | `"true"` |
| `table-topic.iceberg-catalog` | Configured catalog `table-topic.iceberg-catalog`, if non-blank (omitted otherwise) |
| `table-topic.iceberg-namespace` | Topic `automq.table.topic.namespace` (trimmed), falling back to `"default"` if unset on the broker |
| `table-topic.iceberg-table` | Topic name |
| `table-topic.iceberg-ident` | `<metalake>.<iceberg-catalog>.<namespace>.<table>` when `table-topic.iceberg-catalog` is set; `<namespace>.<table>` otherwise |

### Topic Lifecycle and Default Namespace Semantics (`KafkaCatalogOperations`)

- **`createTopic`**: Rejects reserved `table-topic.*` keys, runs `validateTableTopicProperties`, trims `automq.table.topic.*` values, injects `table-topic.default-namespace` into `automq.table.topic.namespace` when `enable=true` and `namespace` is absent/blank, strips Gravitino-only keys (`ID_KEY`, `partition-count`, `replication-factor`, `table-topic.*`) before `AdminClient.createTopics`, and enriches the returned topic with synthetic linkage keys.
- **`loadTopic` (and Lazy Import)**: Reads live topic configs via `AdminClient.describeConfigs`, ignores blank `ConfigSource.DEFAULT_CONFIG` `automq.table.topic.*` entries (avoiding validation failures if a future update touches an unrelated property), records whether a non-default explicit `automq.table.topic.namespace` is present, and enriches linkage keys dynamically so externally created Table Topics expose linkage immediately.
- **`alterTopic`**: Merges `TopicChange`s with live topic state (`LoadedTopicState`). Injects `table-topic.default-namespace` into `alterConfigOpsByName` only on a disabled → enabled transition when neither the alter batch nor an existing non-default broker config sets `automq.table.topic.namespace`. Re-enforces `validateTableTopicProperties` on the merged post-alter map *before* calling `AdminClient.incrementalAlterConfigs`, then returns enriched linkage keys.

### MCP Server Tool (`mcp-server`, Phase 2)

Add read-only, idempotent `get_topic_iceberg_linkage(ctx, catalog_name, schema_name, topic_name)` in `mcp-server/mcp_server/tools/topic.py` using `GravitinoContext.resolve_metalake()`:

- Calls `load_topic(catalog_name, schema_name, topic_name)` and returns structured JSON: `topic_name`, `table_topic_enabled`, `iceberg_catalog`, `iceberg_namespace`, `iceberg_table`, `iceberg_ident`, `schema_type`, `commit_interval_ms`, `upsert_enabled`, `id_columns`, `partition_by`, `cdc_field`, `errors_tolerance`, `convert_key_type`, `convert_value_type`, `transform_value_type`.
- When `table_topic_enabled` is `false` (or absent), sets `iceberg_catalog`, `iceberg_namespace`, `iceberg_table`, and `iceberg_ident` to `null` and `upsert_enabled` to `false`.

### User Process

1. Configure a `kafka` catalog with `bootstrap.servers`, `table-topic.iceberg-catalog`, and optional `table-topic.default-namespace`.
2. Create or alter a topic with `automq.table.topic.enable=true` (and optional schema/upsert/CDC properties).
3. Inspect the topic via Gravitino REST `loadTopic` or MCP `get_topic_iceberg_linkage` to retrieve `table-topic.iceberg-ident`.
4. Query or inspect the materialized Iceberg table via the linked `lakehouse-iceberg` catalog (or MCP `get_table_by_fqn`) once AutoMQ's first commit interval materializes the table.

### Implementation Process

```
Write (createTopic / alterTopic)
  Client ──► TopicOperationDispatcher ──► KafkaCatalogOperations
                                            ├── reject reserved table-topic.* keys
                                            ├── inject table-topic.default-namespace if enabled & unset
                                            ├── validateTableTopicProperties (cross-property rules)
                                            └── strip synthetic/Gravitino keys ──► Kafka AdminClient

Read (loadTopic / MCP get_topic_iceberg_linkage)
  Kafka AdminClient.describeConfigs ──► filter blank DEFAULT_CONFIG ──► enrichTableTopicLinkageProperties ──► Topic
```

### Backward Compatibility and Operational Trade-Offs

- **Zero API / Store Breakage**: No `api/`, `core/`, or `EntityStore` schema changes; existing Kafka catalogs without `table-topic.*` behave identically.
- **Asynchronous Table Materialization**: `table-topic.iceberg-ident` is returned immediately when enabled on Kafka, whereas AutoMQ creates the target Iceberg table lazily after valid records reach the first `commit.interval.ms`.
- **Enum Extensibility**: Keeping enum validation in `KafkaTopicPropertiesMetadata` catches invalid values early, and any closed enum can be widened cleanly in one place if AutoMQ adds new modes.

---

## Task Breakdown

### Phase 1: Kafka Catalog Table Topic Support (`catalog-kafka`)

- [ ] Add `table-topic.iceberg-catalog` and `table-topic.default-namespace` property definitions and blank checks in `KafkaCatalogPropertiesMetadata`.
- [ ] Add typed `automq.table.topic.*` descriptors, reserved `table-topic.*` linkage entries, and `validateTableTopicProperties` in `KafkaTopicPropertiesMetadata`.
- [ ] Implement reserved-key guards, `DEFAULT_CONFIG` filtering, `default-namespace` injection, and `enrichTableTopicLinkageProperties` across `createTopic`, `loadTopic`, and `alterTopic` in `KafkaCatalogOperations`.
- [ ] Add unit tests (`TestKafkaCatalogPropertiesMetadata`, `TestKafkaTopicPropertiesMetadata`, `TestKafkaCatalogOperations`) and document catalog/topic/linkage properties in `docs/kafka-catalog.md`.

### Phase 2: MCP Server Iceberg Linkage Discovery (`mcp-server`)

- [ ] Expose `GravitinoContext.resolve_metalake()` and implement read-only `get_topic_iceberg_linkage` (plus docstrings on `list_of_topics` / `load_topic`) in `mcp-server/mcp_server/tools/topic.py`.
- [ ] Add unit tests in `mcp-server/tests/unit/tools/test_topic.py` covering enabled, fallback, and disabled linkage responses and register the tool in `docs/gravitino-mcp-server.md`.
