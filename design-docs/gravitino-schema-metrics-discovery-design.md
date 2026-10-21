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

# Design of Schema-level Semantic Model Metrics Discovery in Gravitino

## Background

Metrics in Gravitino are defined within Semantic Models. To browse or select metrics across a
schema, clients currently list the Semantic Models, load each model, and extract its metrics.

This requires a separate request to load each model and returns the full definitions, including
datasets and relationships that metric discovery does not need. Clients must also implement the
collection logic themselves. The number of requests and volume of metadata transferred increase
as more models are added or their definitions grow.

## Goals

1. **Discovery.** Provide schema-level listing of metric identifiers and complete definitions through
   REST, Java, Python, and MCP, without requiring clients to load each Semantic Model separately.
2. **Identity.** Keep internal identities stable for metrics retained during Semantic Model updates.
3. **Consistency.** Keep discovered metric definitions consistent with the current committed
   definition of each Semantic Model.
4. **Visibility.** Apply the same visibility rules as the existing Semantic Model listing.

## Non-Goals

1. **Independent metric management.** Standalone APIs for creating, updating, deleting, or renaming
   metrics; metric-level ownership and privileges.
2. **Public identity and history.** Exposing internal metric IDs or revision identifiers, or querying
   historical metric definitions.

## Proposal

### API Contract

#### Java API

The `SemanticModelCatalog` interface adds separate methods for identifiers and complete definitions:

```java
NameIdentifier[] listMetrics(Namespace namespace) throws NoSuchSchemaException;

SemanticModelMetric[] listMetricInfos(Namespace namespace) throws NoSuchSchemaException;
```

`listMetrics` returns one identifier per metric. A full identifier has a four-level namespace
`[metalake, catalog, schema, semanticModel]` and the metric name as its `name`. Including the containing
model distinguishes metrics with the same name in different models. Catalog-scoped clients follow
the existing convention: callers pass `Namespace.of(schema)`, and returned identifiers contain the
namespace `[schema, semanticModel]` and the metric name. REST uses full identifiers.

`listMetricInfos` returns one `SemanticModelMetric` per metric:

```java
/** A metric's namespace and complete definition returned by discovery. */
public interface SemanticModelMetric {

  /**
   * Returns the metric namespace, whose last level is the containing Semantic Model.
   *
   * @return The metric namespace.
   */
  Namespace namespace();

  /**
   * Returns the complete metric definition.
   *
   * @return The metric definition.
   */
  Metric metric();
}
```

`namespace()` uses the same full or catalog-relative namespace as the identifiers from `listMetrics`.
In this response type, the metric name is stored only in the existing `Metric` definition.
Callers can reconstruct its identifier with `NameIdentifier.of(info.namespace(), info.metric().name())`.
`SemanticModelMetricDTO` contains a `namespace` array and an unchanged `MetricDTO` in `metric`.

#### REST API

The new route sits alongside `semantic-models`, avoiding a conflict with a model named `metrics`:

```http
GET /api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/metrics
```

The `details` query parameter defaults to `false`. With `details=false`, the endpoint implements
`listMetrics` and returns the existing `EntityListResponse`:

```json
{
  "code": 0,
  "identifiers": [
    {
      "namespace": ["example", "sales", "mart", "sales_model"],
      "name": "total_revenue"
    }
  ]
}
```

With `details=true`, it implements `listMetricInfos` and returns a `MetricInfoListResponse` containing
a `metrics` array of `SemanticModelMetricDTO` objects:

```json
{
  "code": 0,
  "metrics": [
    {
      "namespace": ["example", "sales", "mart", "sales_model"],
      "metric": {
        "name": "total_revenue",
        "expression": {
          "dialects": [
            {
              "dialect": "ANSI_SQL",
              "expression": "SUM(orders.amount)"
            }
          ]
        }
      }
    }
  ]
}
```

Both modes sort results by structured model identifier and then case-sensitive metric name. Models
without metrics contribute no entries. Empty results use `identifiers: []` or `metrics: []`, according
to `details`. Preserve existing errors for missing schemas and unsupported catalogs; never truncate
or return partial success on storage failures.

#### Python API and MCP

The Python API adds `list_metrics(namespace)` returning `list[NameIdentifier]` and
`list_metric_infos(namespace)` returning `list[SemanticModelMetric]`, with the same namespace
convention as the Java client.

Read-only MCP tools `list_metrics` and `list_metric_infos` accept catalog/schema with existing
metalake selection and call the corresponding endpoint mode. MCP results retain the full REST
namespaces. SDKs and MCP must not implement list-and-load loops.

### Authorization

Metrics inherit the visibility of their containing Semantic Model. Both `listMetrics` and
`listMetricInfos` reuse the [existing Semantic Model listing authorization rules][model-list],
including schema access checks and the existing ownership, use, and modify visibility rules.
No metric-level privileges are introduced.

The server applies these rules in the caller's authorization context for REST, SDK, and MCP requests.
Each model's metrics are included or excluded together in both listing modes.

The [existing visibility filter][visibility-filter] excludes a model on denial or an authorization
evaluation exception, logging exceptions. Request-level authorization failures, including denied
schema access, fail the request. When authorization is disabled, no authorization filtering is applied.
Discovery grants no access to underlying data.

### Alternatives

1. **Reuse existing storage**

   - **Approach.**

     - Collect metrics from complete model definitions in the server.
     - Alternatively, extract metrics from the stored JSON in the database.

   - **Benefits.** Provides schema-level discovery without changing the stored representation.

   - **Trade-offs.**

     - Loading complete definitions still reads and parses unrelated model content.
     - JSON extraction requires database-specific operations.
     - Neither approach persists metric identity across updates and removal/reintroduction, so
       the stable-identity goal requires a storage change.

2. **Add a current metric table**

   - **Approach.**

     - Keep complete model snapshots and maintain one live row per metric containing its stable ID,
       name, and complete payload.
     - Identifier listing reads names.
     - Detailed listing reads metric payloads directly.

   - **Benefits.**

     - Supports direct metric discovery and stable identity.
     - Preserves the existing model snapshot representation.

   - **Trade-offs.**

     - Stores current metric content in both model snapshots and the metric table.
     - Model writes must maintain both copies in the same transaction.

3. **Split definition fields and store metric identities**

   - **Approach.**

     - Store the definition's top-level fields as separate columns containing JSON in
       `semantic_model_version_info`, with all fields for one model version in the same row.
     - Keep metric IDs, names, and lifecycle state in a separate identity table without
       `metric_payload`.
     - Identifier listing reads the identity table.
     - Detailed listing reads the current snapshot's `metrics` column.

   - **Benefits.**

     - Avoids reading unrelated definition fields.
     - Avoids maintaining a second copy of current metric payloads.
     - Changes to metric content do not rewrite identity rows when metric names remain unchanged.
     - Preserves complete model snapshots and stable metric identity.

   - **Trade-offs.**

     - Metric names remain duplicated between the current model snapshot and the identity table.
       Model writes must keep the live name-to-ID mapping consistent with the current snapshot in
       the same transaction.
     - Requires changes to existing model storage and definition reconstruction.
     - Detailed listing still parses each model's metric array.

This proposal selects option 3. Separate definition columns support metric-only reads while keeping
each complete model snapshot in one row. The identity table stores names and stable IDs, and changes
only when metrics are added or removed.

### Internal Storage

Keep `semantic_model_meta` unchanged. Replace `semantic_model_definition` in
`semantic_model_version_info` with five columns containing JSON: `datasets`, `relationships`, `metrics`,
`ai_context`, and `custom_extensions`. Add `semantic_model_metric_meta` for stable metric identities,
without metric payloads.

The following MySQL-style DDL gives the complete proposed structures of the modified version table
and the new identity table. Other version-table columns and indexes follow the
[existing Semantic Model schema][semantic-design].

```sql
CREATE TABLE IF NOT EXISTS `semantic_model_version_info` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `semantic_model_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'semantic model id',
    `version` INT UNSIGNED NOT NULL COMMENT 'semantic model version',
    `semantic_model_name` VARCHAR(128) NOT NULL COMMENT 'semantic model name snapshot',
    `semantic_model_comment` TEXT DEFAULT NULL COMMENT 'semantic model comment snapshot',
    `datasets` MEDIUMTEXT NOT NULL COMMENT 'dataset definitions snapshot (JSON)',
    `relationships` MEDIUMTEXT DEFAULT NULL COMMENT 'relationship definitions snapshot (JSON)',
    `metrics` MEDIUMTEXT DEFAULT NULL COMMENT 'metric definitions snapshot (JSON)',
    `ai_context` MEDIUMTEXT DEFAULT NULL COMMENT 'model AI context snapshot (JSON)',
    `custom_extensions` MEDIUMTEXT DEFAULT NULL COMMENT 'model custom extensions snapshot (JSON)',
    `properties` MEDIUMTEXT DEFAULT NULL COMMENT 'semantic model properties snapshot (JSON)',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'semantic model version audit info',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'version deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_smid_ver_del` (`semantic_model_id`, `version`, `deleted_at`),
    KEY `idx_smvi_mid` (`metalake_id`),
    KEY `idx_smvi_cid` (`catalog_id`),
    KEY `idx_smvi_sid` (`schema_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT 'semantic model version information';

CREATE TABLE IF NOT EXISTS `semantic_model_metric_meta` (
    `metric_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'stable internal metric id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `semantic_model_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'containing semantic model id',
    `metric_name` MEDIUMTEXT NOT NULL COMMENT 'metric name',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'metric retired at',
    PRIMARY KEY (`metric_id`),
    KEY `idx_smmm_model_live` (`semantic_model_id`, `deleted_at`),
    KEY `idx_smmm_mid` (`metalake_id`),
    KEY `idx_smmm_cid` (`catalog_id`),
    KEY `idx_smmm_sid` (`schema_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT 'semantic model metric identities';
```

- **Definition snapshots.** Each version row stores the complete, authoritative definition.
  Preserve existing field representations, collection order, and public import/export formats.
- **Optional fields.** Use SQL `NULL` for absent optional fields and JSON `[]` for explicit empty
  collections.
- **Metric identities.** Store the current name-to-ID mapping and deletion state, without
  per-version rows. Allocate IDs through Gravitino's ID generator and never reuse them.
- **Names.** Use text to preserve supported name lengths. Keep `metric_name` consistent with the
  current snapshot's `metrics` column in the same model transaction.
- **Name uniqueness.** Enforce one live metric per exact name through
  [existing validation][metric-validation] and model concurrency controls, without a database
  name-uniqueness index.
- **Parent links.** Copy parent IDs from the containing model and resolve its name through
  `semantic_model_id`. Model and parent indexes support lookups and cascade deletion, without
  database foreign keys.
- **Deletion.** `deleted_at = 0` marks a live metric. Retired rows store the deletion timestamp in
  epoch milliseconds.
- **Audit.** Keep audit information only on the model and its version snapshots.
- **Database support.** H2 and PostgreSQL use equivalent types and indexes: `MEDIUMTEXT` becomes
  `CLOB` and `TEXT`, respectively, and PostgreSQL uses signed integer types.

#### Metric Identity and Lifecycle

Stable metric IDs can support future associations with other semantic assets. Those associations
are outside this proposal.

Match each replacement's metric names exactly against the same model's live identities:

| Change                                                            | Identity rule                                         |
| ----------------------------------------------------------------- | ----------------------------------------------------- |
| Same name, including formula, description, or array-order changes | Reuse the existing ID                                 |
| New name                                                          | Allocate a new ID                                     |
| Name removed                                                      | Retire its ID                                         |
| Removed name reintroduced in a later committed replacement        | Allocate a new ID                                     |
| Model comment or properties changed                               | Preserve metric IDs                                   |
| Model renamed                                                     | Preserve model and metric IDs; public locator changes |
| Model dropped and recreated under the same name                   | New model and metric IDs                              |

Changing a metric's name through replacement retires its ID and allocates a new one. Equal content
does not reliably identify a rename, because multiple metrics can share the same definition apart
from their names. A future explicit metric rename change could preserve the ID, but is outside this
proposal.

Retaining an ID does not imply unchanged business meaning.

#### Read and Write Paths

**Writes**

Extend the [existing model transaction][storage] to commit definition snapshots, model metadata,
the version pointer, and metric identities together. Retain parent-lifecycle locking and model-level
optimistic concurrency control (OCC).

- **Create and import.** Store the validated definition's five fields in the initial version row
  and allocate an identity for each metric. Absent or empty metrics produce no identity rows.
- **Replace and overwrite.** After the model concurrency check succeeds, read and reconcile live
  identities using the lifecycle rules above, and persist a complete new snapshot. Only added or
  removed metric names require identity writes.
- **Other changes.** Model rename, comment, and property changes retain existing snapshot behavior
  and require no identity writes.
- **Drop and cleanup.** Model drops and parent cascades retire identities while soft-deleting the
  model and snapshots in the same transaction. Apply existing retention rules to retired identities
  and preserve live identities during snapshot cleanup.

All writes commit in the same transaction. Any concurrency conflict or serialization or
persistence failure rolls back the entire transaction.

**Reads**

Use the following read paths, restricting discovery to live models in the requested schema:

- **Identifier listing.** Join live metric identities with `semantic_model_meta` by
  `semantic_model_id` to read metric and model names, without accessing the version table.
- **Detailed listing.** Join model metadata with its current `semantic_model_version_info` row,
  selecting only `metrics` from the definition columns and the current model name. Decode each
  array into `SemanticModelMetric` results, without accessing the metric identity table.
- **Model loading.** Reconstruct `SemanticModelDefinition` from all five columns of the same
  current snapshot.

Each discovery query reads model names and metrics in one consistent statement or read snapshot,
so concurrent changes cannot mix a model's states. Apply model authorization before decoding and
assembling results. Storage failures, or metric decoding failures for included models, fail the
request without partial results. Metadata and permission changes do not share a schema-wide
transaction.

## Development Plan

1. Split definition storage into five columns and update model serialization and reconstruction
   across supported databases.
2. Add the metric identity table and integrate transactional reconciliation, cascade deletion, and
   cleanup with the existing model lifecycle.
3. Implement authorized identifier and detail listings with REST, Java, Python, and MCP integration.
4. Validate definition round-trips, identity rules, and transactional consistency.

[metric-validation]: https://github.com/apache/gravitino/blob/62a948972d5b7e8762020ccc099a52f7cfc91331/core/src/main/java/org/apache/gravitino/catalog/SemanticModelValidator.java#L145-L165
[storage]: https://github.com/apache/gravitino/blob/62a948972d5b7e8762020ccc099a52f7cfc91331/core/src/main/java/org/apache/gravitino/storage/relational/service/SemanticModelMetaService.java
[semantic-design]: https://github.com/apache/gravitino/blob/62a948972d5b7e8762020ccc099a52f7cfc91331/design-docs/gravitino-semantic-model-design.md
[model-list]: https://github.com/apache/gravitino/blob/62a948972d5b7e8762020ccc099a52f7cfc91331/server/src/main/java/org/apache/gravitino/server/web/rest/SemanticModelOperations.java#L106-L132
[visibility-filter]: https://github.com/apache/gravitino/blob/62a948972d5b7e8762020ccc099a52f7cfc91331/server-common/src/main/java/org/apache/gravitino/server/authorization/MetadataAuthzHelper.java#L527-L570
