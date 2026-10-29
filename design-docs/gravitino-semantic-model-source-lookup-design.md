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

# Design of Semantic Model Dataset Lookup by Source in Gravitino

## Background

A Semantic Model dataset references a table or logical View through `Dataset.source`. Sources may
belong to another catalog or schema in the same metalake. Users browsing a source cannot currently
list its associated Semantic Models without collecting and inspecting model definitions themselves.

Gravitino should provide this lookup through its existing object relationship API pattern. The
result helps users and agents discover models built from a source and identify the datasets to
inspect when that source changes.

## Goals

1. **Discovery.** List fully qualified identifiers of Semantic Model datasets directly associated
   with a table or View through REST, Java, and Python, across catalogs and schemas in the same
   metalake.
2. **Visibility.** Require access to the source metadata and return datasets only from models the
   caller can load.
3. **Consistency.** Maintain source associations in the same transaction as model definition writes.
4. **Integration.** Follow existing object relationship APIs, client capability interfaces, and
   listener and audit conventions.

## Non-Goals

1. Transitive View dependencies, column or metric impact analysis, and validation of whether a
   model can currently compile or execute. These checks require additional lineage or compiler
   analysis beyond the source references explicitly declared by datasets.
2. Automatically changing model definitions, blocking source changes because models reference the
   source, detecting mismatches between model references and current sources, and sending
   notifications. These need separate decisions about detecting source changes, permissions to
   modify affected models, and handling update or notification failures.
3. Independent association management, exposing internal source IDs, and historical association
   queries. Associations are derived from model definitions and updated when those definitions are
   written. Editing associations separately could make them disagree with the definitions. Discovery
   of current datasets needs neither exposed source IDs nor retained association history.
4. Stable dataset IDs, dataset-specific privileges, and independent dataset lifecycle APIs. Dataset
   identifiers locate datasets in the current model definition. Tracking the same dataset across
   model versions or authorizing it separately requires rules for dataset identity and lifecycle.

## Proposal

### Association Contract

Each dataset has one association to the registered Gravitino source ID and type resolved during the
latest successful model definition write (create, import, overwrite, or replacement). The model ID
and exact dataset name identify which dataset an association belongs to. Only tables or Views named
in `Dataset.source` are indexed; lookup does not follow the dependencies of a referenced View.

`Dataset.name` is a case-sensitive alias unique within the model and need not match the table or
View name in `Dataset.source`.

Definition updates and association updates must commit in the same transaction. Each association's
`dataset_name` must match the corresponding `Dataset.name` in that committed definition.

Lookup returns one identifier per matching dataset whose model has not been deleted and can be
loaded by the caller. For example, model `analytics.semantic.employee_analysis` in metalake `example`
has two datasets referencing the same source:

| Model               | Dataset    | Source                |
| ------------------- | ---------- | --------------------- |
| `employee_analysis` | `employee` | `hr.public.employees` |
| `employee_analysis` | `manager`  | `hr.public.employees` |

Storing this model creates two association rows; looking up `hr.public.employees` returns both
dataset identifiers. Callers needing only models derive the containing model identifiers and remove
duplicates. Updates follow [Lifecycle Behavior](#lifecycle-behavior); drift and other constraints
are covered under [Known Limitations](#known-limitations).

### API Contract

#### Java API

Add a read-only capability in the semantic API package:

```java
/** Lists Semantic Model datasets associated with a source metadata object. */
public interface SupportsSemanticModels {

  /**
   * Lists datasets directly associated with this source from visible Semantic Models.
   *
   * @return The fully qualified dataset identifiers, or an empty array.
   * @throws NoSuchMetadataObjectException If the source does not exist.
   */
  NameIdentifier[] listSemanticModelDatasets() throws NoSuchMetadataObjectException;
}
```

Each result has five levels in total: namespace `[metalake, catalog, schema, semanticModel]` and the
exact dataset name as `name`, following the existing metric identifier convention. This identifies
a member of the current model definition and does not create a separate dataset metadata object.

Construct identifiers from components with `NameIdentifier.of(namespace, datasetName)` and preserve
the structured `namespace` and `name` during serialization. Dataset names may contain literal dots.
Joining components with `.` and splitting that string again would treat dots in dataset names as
namespace separators.

`Table` and `View` add the following default capability accessor, following `supportsTags()`:

```java
/**
 * Returns the Semantic Model dataset lookup operations for this source.
 *
 * @return The Semantic Model dataset lookup operations.
 * @throws UnsupportedOperationException If the implementation does not support lookup.
 */
default SupportsSemanticModels supportsSemanticModels() {
  throw new UnsupportedOperationException("Semantic Model dataset lookup is not supported");
}
```

The Gravitino Java client's table and View implementations provide this capability through shared
object relationship request handling. Underlying catalog connectors do not implement the lookup.

```java
NameIdentifier[] datasets = table.supportsSemanticModels().listSemanticModelDatasets();
```

Results can belong to a different catalog from the source. Java and Python therefore return the same
fully qualified five-level identifiers as REST, including metalake and catalog in the namespace.
For a result `dataset`, `NameIdentifier.of(dataset.namespace().levels())` gives its containing model
identifier. Remove duplicates from these model identifiers to obtain the associated model list.

#### REST API

Add a route alongside the existing object tag and policy routes:

```http
GET /api/metalakes/{metalake}/objects/{type}/{fullName}/semantic-model-datasets
```

`type` accepts `table` or `view`. `fullName` contains the source's catalog, schema, and name using the
existing `MetadataObject` format, such as `hr.public.employees`, with normal URL path encoding.
Limits on names containing literal dots are described under [Known Limitations](#known-limitations).

The endpoint implements `listSemanticModelDatasets` and always returns the existing
`EntityListResponse`, with one identifier per matching dataset. For the employee example above:

```json
{
  "code": 0,
  "identifiers": [
    {
      "namespace": ["example", "analytics", "semantic", "employee_analysis"],
      "name": "employee"
    },
    {
      "namespace": ["example", "analytics", "semantic", "employee_analysis"],
      "name": "manager"
    }
  ]
}
```

- **Uniqueness and ordering.** Return each dataset identifier once. Sort lexicographically by the
  model namespace components (`metalake`, `catalog`, `schema`, `semanticModel`), then by the
  case-sensitive dataset name. This order is independent of dataset order in the definition.
- **Empty results.** Return `identifiers: []` when the source exists but no matching datasets are
  visible.
- **Errors.** Unsupported source types return `400`. Missing or inaccessible sources follow the
  existing object relationship API error behavior, rather than becoming empty successful results.
  Storage and source lookup failures fail the request without partial results.

The response contains identifiers only. Callers select the result's catalog and use the existing
model load API to inspect the containing model and its datasets. No internal IDs, validity status,
hidden-model counts, or association mutation APIs are added.

#### Python API

The Python table and View APIs add `supports_semantic_models()`, returning a capability with
`list_semantic_model_datasets() -> list[NameIdentifier]`. It uses the same REST endpoint and full
four-level namespace plus dataset name as Java. Java and Python clients preserve identifier
components and server ordering and do not scan catalogs or load all models to perform lookup.

### Authorization

- **Source access.** The REST entry point uses `CAN_ACCESS_METADATA` for the source, as existing
  object relationship APIs do. Source type, name normalization, existence checks, and internal ID
  resolution reuse the existing table and View paths. Source existence is still checked when
  authorization is disabled.
- **Model visibility.** For each distinct candidate model, apply
  `LOAD_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION` in the caller's context using the containing model
  identifier, not the dataset identifier. Include or exclude all matching datasets from that model
  together. Apply this filtering in REST before constructing the response.
- **Catalog and schema access.** Apply the model load rules for ownership and use or modify
  privileges, including access to that model's catalog and schema. Schema listing checks the schema
  named in its request before filtering models. This lookup can return models from several schemas
  and catalogs, so it must check each model's own parents using the full load expression.
- **Failure handling.** Reuse `MetadataAuthzHelper.filterByExpression`: exclude a model if its
  authorization check denies access or throws an exception, and log evaluation exceptions. If the
  caller fails the source access check, fail the entire request. When authorization is disabled,
  skip model visibility filtering.
- **Association writes.** These are part of an already authorized model definition write and its
  source validation. They do not introduce a new privilege or require the source owner to modify
  other users' model definitions.

Dataset-level privileges are not introduced, and owning a source does not grant access to its
associated models. Lookup grants no data access or model modification privileges.

### Alternatives

| Approach                   | Trade-offs                                                                                              |
| -------------------------- | ------------------------------------------------------------------------------------------------------- |
| Scan stored definitions    | Avoids a new table, but reads and parses models across the metalake for each lookup.                    |
| Index source names         | Names must be updated after a rename and can refer to different objects after name reuse.               |
| Index source IDs and types | Keeps associations across source renames that preserve IDs; requires transactional association updates. |

This proposal stores source IDs and types in the relation. Model load, export, and compilation
continue to use the source names in the stored definition. The relation records the source objects
resolved when that definition was written; it does not rewrite or override those names.

A row containing only a model ID and source ID cannot identify which datasets reference that source.
Matching `Dataset.source` against the source's current name is unreliable after a source rename.
Store the dataset name in each association so lookup can return dataset identifiers. Callers can
derive the model list from those identifiers; no second model-level relation table is needed.

### Internal Storage

Add `semantic_model_source_rel` without changing the public definition or model snapshot format:

```sql
CREATE TABLE IF NOT EXISTS `semantic_model_source_rel` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'containing metalake id',
    `semantic_model_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'containing semantic model id',
    `dataset_name` MEDIUMTEXT NOT NULL COMMENT 'exact dataset name in the current definition',
    `source_type` VARCHAR(64) NOT NULL COMMENT 'TABLE or VIEW',
    `source_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'resolved source object id',
    PRIMARY KEY (`id`),
    KEY `idx_smsr_model` (`semantic_model_id`),
    KEY `idx_smsr_source` (`metalake_id`, `source_type`, `source_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT 'current semantic model dataset source associations';
```

Use equivalent auto-generated keys, types, and indexes for H2 and PostgreSQL.

- **Dataset names.** Existing validation requires non-empty names but sets no maximum name length.
  Store the full name as text without truncation or case normalization. Within each model,
  validation rejects duplicate dataset names using case-sensitive string comparison.
- **Row identity.** The primary key uses the auto-increment `id`, so `dataset_name` need not fit in
  an indexed key. This ID identifies a stored association row and is never returned by public APIs.
  Replacing a model's association set allocates new row IDs; these IDs do not track datasets across
  definition versions.
- **Model index.** The index on `semantic_model_id` supports replacing or deleting a model's
  complete association set.
- **History.** Association rows have no version, audit payload, or copy of the source name. The
  model's snapshots and audit records retain the definition history, but replacing or deleting
  associations removes the old rows. Keeping an old model snapshot does not keep its association
  rows.

The table contains one row per dataset, including separate rows when datasets share a source.
Resolve each distinct source once per definition write, then reuse that result in each dataset's
row.

#### Read and Write Paths

**Writes**

- **Resolve sources.** Source validation already loads each distinct dataset source under the
  caller. Extend this path to retain the actual resolved object type and ID. Reuse the resolved
  object for datasets sharing a source.
- **Build associations.** Pass each dataset's exact name and resolved source ID and type into model
  persistence. Use the existing source lookup path to normalize names and resolve objects. Storage
  must use that lookup result, without guessing TABLE versus VIEW or accepting IDs from public
  requests.
- **Pass update intent.** Carry an explicit `replace` or `preserve` instruction through the internal
  dispatcher and storage write path into the model transaction. `replace` carries the complete
  validated association set; `preserve` carries no replacement set. Do not infer this instruction by
  comparing old and new definitions. Keep this write context out of public request DTOs and model
  snapshots.
- **Commit together.** Create, import, explicit overwrite, and `replaceDefinition` replace the
  complete dataset association set together with the model metadata, complete snapshot, and
  current-version pointer. Reuse the existing model transaction and optimistic concurrency checks.
  This handles dataset addition, removal, rename, reordering, and source changes without separate
  dataset operations.
- **Explicit replacement.** Every explicit definition replacement resolves sources again, even if
  the submitted source names or serialized definition are unchanged. This permits an explicit
  replacement to record a different source ID when the same name now refers to a new object.
- **Metadata updates.** Model rename, comment, and property changes leave association rows unchanged
  and do not resolve dataset sources again.

Any write failure rolls back both the model changes and the association changes. Update association
rows inside the model transaction; do not defer these writes to post-write event listeners.

**Reads**

- **Source lookup.** Resolve the requested source to its current ID and type, then query
  `idx_smsr_source` using the metalake ID, source type, and source ID.
- **Relation query.** Add `SEMANTIC_MODEL_SOURCE_REL` to `SupportsRelationOperations.Type` and query
  it through `RelationQuery` with a table or View as the anchor. A dedicated relational service and
  mapper serve the read through the existing EntityStore relation query abstraction.
- **Consistent metadata.** Join associations with current model, schema, catalog, and metalake
  metadata, excluding deleted objects. Use one statement or a consistent read snapshot to read each
  matching dataset's exact name and its model's current identifier. No definition JSON or separate
  per-model loads are needed.
- **Internal query result.** Use a lightweight model object implementing `Entity` and
  `HasIdentifier`. It contains the model's ID, current identifier, `SEMANTIC_MODEL` type, and the
  matching dataset names.
- **Result identifiers.** Construct each five-level dataset identifier from the model's current
  identifier components and the recorded dataset name. No dataset entity type is required. After a
  model rename, returned dataset namespaces contain the new model name.

#### Lifecycle Behavior

| Change                                       | Association and lookup behavior                                                                         |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Source renamed with the same ID              | Keep the stored source ID and type; lookup through the new name returns the same dataset identifiers.   |
| Source deleted                               | Preserve the model's association; lookup through a missing source fails.                                |
| Source name reused by a different object     | Lookup resolves the reused name to the new ID and excludes datasets associated with the old ID.         |
| Definition explicitly replaced               | Resolve sources again and replace the association set atomically.                                       |
| Dataset renamed                              | Store the new dataset name and return its new identifier; the old identifier does not track the rename. |
| Dataset removed                              | Remove its row and identifier; other matching dataset results remain.                                   |
| Dataset source changed                       | Store the newly resolved source ID and type; lookup returns this dataset for that source.               |
| Datasets reordered                           | Replace the association set; order alone does not change the returned identifiers.                      |
| Model renamed                                | Keep association rows; returned dataset namespaces contain the new model name.                          |
| Model deleted directly or by parent deletion | Remove its associations in the same model deletion transaction.                                         |
| Model dropped and recreated                  | Create associations for the new model ID.                                                               |

Source rename or deletion does not rewrite definitions, block the source operation, or grant
additional privileges. Models remain loadable for inspection and explicit repair through the normal
model update API, following the [existing Semantic Model design][semantic-design].

Deleting a source must not delete association rows for models that still exist. Those rows remain
until the model definition is replaced or the model is deleted, including deletion caused by
dropping its schema, catalog, or metalake. Lookup excludes deleted models and models whose schema,
catalog, or metalake has been deleted. Do not add a source foreign key that deletes association rows
when the source is deleted.

Update the 2.0.0 schema and 1.3.0-to-2.0.0 upgrade scripts for all supported databases, following the
existing Semantic Model storage release boundary. Released versions have no Semantic Model data
to migrate.

## Known Limitations

These limitations describe the observable consequences of the association contract. They do not
relax the requirement to commit model definitions and their dataset associations atomically.
Each item includes **Current scope** handling through the proposed lookup and existing model
operations, plus optional **Extension** suggestions requiring a separate design. These extensions
are not included in the Development Plan.

1. **Source name drift and validity.**

   - **Source rename.** A source rename can leave `Dataset.source` pointing to its old name while the
     association still records the same source ID. For example, after renaming `hr.public.employees`
     to `hr.public.employees_v2`, lookup through the new name still returns identifiers for datasets
     `employee` and `manager`, while their stored source references can remain `hr.public.employees`.
   - **Validity.** Source column changes can also invalidate a model without changing the source ID
     in its association. Lookup does not revalidate definitions, detect outdated references, or
     verify that a model can compile or execute.

   - **Current scope:** Use lookup through the source's current name to locate matching datasets,
     load the current model, and have a caller with model modification permission repair references
     and affected fields through `replaceDefinition`. Applications that compile or execute the model
     must still validate it against current source metadata.

   - **Extension:** First add read-only checks for differences between stored associations, declared
     source names, and current source metadata. Automatic repair needs separate rules for who may
     edit affected models, how to handle concurrent edits, and what happens when a repair fails.

2. **Source name reuse can make execution use a different source.** If a different object takes an
   old source name, an application resolving `Dataset.source` by name may read the new object, while
   the stored association still contains the original source ID. Lookup for the new object therefore
   excludes those datasets. Explicitly replacing the definition resolves sources again and updates
   the associations; lookup itself does not change compilation or execution behavior.

   - **Current scope:** Confirm the intended source before replacing a definition. Replacement can
     change stored source IDs even when the submitted names are unchanged. Do not use replacement
     merely to check whether source references are outdated.

   - **Extension:** Add a server-side check before compilation or use: resolve `Dataset.source`
     again and compare the resolved source ID with the stored source ID. Report mismatches and
     require explicit rebinding (updating the stored source association) rather than silently
     switching objects.

3. **Registered IDs and external source changes.** Associations identify sources by their registered
   Gravitino IDs. A rename preserves an association only if it preserves that ID. How changes made
   directly in an external catalog affect registered IDs depends on the connector and metadata
   synchronization. The transaction that stores the model does not include external catalog changes.

   - **Current scope:** Prefer operations that preserve registered IDs where supported. After
     changes outside Gravitino, refresh metadata through supported connector mechanisms and verify
     the intended source before repairing models. Refreshing metadata alone does not guarantee that
     Gravitino recognizes the source as the same registered object.

   - **Extension:** Use connector-provided stable source IDs or reliable change events to reconcile
     associations with source changes. Report changes for review when it is unclear whether the
     source is still the same object.

4. **Lookup cannot recover associations for a deleted source.** Source deletion preserves
   association rows for models that still exist, but lookup requires an existing source. The API
   accepts neither historical source IDs nor requests to list associations whose sources are
   missing.

   - **Current scope:** Before a planned deletion, record the returned dataset identifiers for
     follow-up repairs. The list includes only datasets visible to the caller and does not prove
     that deletion is safe. After deletion, model APIs can still load affected models that exist,
     but source lookup cannot rediscover all of them.

   - **Extension:** Add an authorized API for inspecting a model's stored associations, including
     those whose sources no longer exist. Define who can read these associations and how long they
     are retained.

5. **Separate requests can observe different model states.** Lookup and a subsequent model load are
   independent requests. If the model is edited between them, the returned identifier may name a
   dataset that has been removed, renamed, or changed to use another source. The lookup response has
   no model revision token with which to verify that a later load returns the same definition
   version.

   - **Current scope:** Load the current definition for inspection and repeat source lookup if the
     model or dataset name is absent. The model can change again between the repeated lookup and
     load.

   - **Extension:** Return opaque model revision tokens from lookup and allow model-load requests
     to require a matching token. Reject the load if the model revision has changed, so callers can
     repeat lookup instead of combining results from different model versions.

6. **Lookup does not list all dependencies.** Results include only directly associated datasets
   from models the caller can load. Dependencies through Views and inaccessible models are omitted.
   A dataset-to-source association does not identify the metrics that depend on that dataset. An
   empty result therefore cannot establish that a source is unused or safe to change or delete.

   - **Current scope:** Use results for discovery and inspection; do not infer that deletion is safe
     from an empty list.

   - **Extension:** If source changes must be blocked when models depend on them, design a separate
     impact check and specify which dependencies it checks, including dependencies through Views.
     Account for models hidden from the caller without revealing their identities. An incomplete
     check must not conclude that no dependencies exist.

7. **Dots in source names cannot be represented reliably.** The endpoint uses dots to separate the
   catalog, schema, and source name in `fullName`. A literal dot inside any of those names is
   indistinguishable from a separator; URL encoding does not provide a way to quote individual name
   components. Returned dataset identifiers preserve their separate `namespace` and `name` fields.

   - **Current scope:** There is no reliable workaround for literal dots through this route;
     additional URL encoding does not resolve the ambiguity.

   - **Extension:** Introduce a shared structured identifier representation for object relationship
     APIs, preserving catalog, schema, and object name as separate components through REST and both
     SDKs.

## Development Plan

1. Add the relation table, source resolution result, and transactional maintenance for every model
   definition write and model deletion, including model deletion caused by dropping its schema,
   catalog, or metalake. Cover datasets sharing a source, dataset changes and reordering, and
   transaction rollback. Verify that replacing an identical definition updates bindings after
   same-name source recreation, while metadata-only changes preserve bindings after source rename
   or deletion.
2. Add the indexed relation query, REST endpoint using `EntityListResponse`, source access checks,
   and full model load authorization checks. Emit pre-operation, success, and failure events and
   add dataset lookup to the existing audit operation mapping.
3. Add `listSemanticModelDatasets` and its Python equivalent to the table/View capability, OpenAPI
   definitions, and usage documentation, with fully qualified five-level dataset identifiers.
4. Validate cross-catalog lookup, direct View references, model renames, source renames with unchanged
   definition references, source deletion and name reuse, one result per matching dataset, and parent
   model identifier construction from dataset identifiers. Cover structured identifier round-trips,
   dataset names containing dots or long names, hidden models, empty results, and lifecycle behavior
   across supported databases.

[semantic-design]: gravitino-semantic-model-design.md#validation
