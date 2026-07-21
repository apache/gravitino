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

# Design of Tag Assignment Values in Gravitino

## Background

Gravitino currently supports tags as metalake-scoped metadata objects. A tag has a name, comment,
properties, audit information, and can be associated with metadata objects such as catalogs,
schemas, tables, filesets, topics, models, and columns.

The current model has several important limitations:

1. A tag association is boolean. A metadata object either has a tag or does not have it.
2. Tag properties are global to the tag. They cannot represent values that differ by metadata
   object.
3. Tags cannot declare the values that are valid for assignments.
4. Because values do not exist as first-class metadata, users cannot find metadata objects by tag
   value.

For example, users can create a tag named `data_domain`, but they cannot assign
`data_domain = finance` to one table and `data_domain = risk` to another table without creating
separate tag names such as `data_domain_finance` and `data_domain_risk`. This causes tag
proliferation and makes policy integration harder.

The current relational storage shape is:

```text
tag_meta
  tag_id
  tag_name
  metalake_id
  properties
  audit_info

tag_relation_meta
  tag_id
  metadata_object_id
  metadata_object_type
  audit_info
  current_version
  last_version
  deleted_at
```

`tag_relation_meta` already represents the tag assignment between a tag and a metadata object, but
it has no field for assignment-level values.

---

## Goals

1. **Assignment-Level Values**: Allow a tag assignment on a metadata object to carry zero or more
   string values.
2. **Clear Inheritance Semantics**: Preserve the current inherited tag behavior and define how
   assignment values are inherited or overridden.
3. **Batch Assignment Updates**: Allow clients to add or remove multiple tag-value pairs on one
   metadata object in a single request.
4. **Allowed Values**: Allow a tag to declare an optional allowed value list and reject
   assignment values outside that list.
5. **Value Lookup**: Allow users to find metadata objects by exact tag value through indexed
   storage and the existing tag-associated-object REST/client APIs.
6. **Client and REST Visibility**: Return assignment values when clients get or list detailed tags
   for a metadata object.

---

## Non-Goals

1. **Typed Values**: Values are strings only. Numeric, boolean, and external-schema validation
   are outside this design. The allowed value list is a flat string whitelist, not a typed schema.
2. **Relation Payload for Non-Tag Relations**: Only tag-to-metadata-object relations carry values.
   Other relation types remain unchanged.
3. **Advanced Value Search**: Value lookup is exact string matching only. Prefix, substring,
   regular expression, full-text search, and multi-value matching are outside this design.
4. **Standalone Search API**: This design does not add a dedicated tag assignment search endpoint
   such as `POST /tags/{tag}/objects/search`. Value lookup is a filter on the existing tag objects
   listing API.
5. **Cross-Tag Boolean Lookup**: Queries such as `data_domain = finance AND retention = 7y` can be
   added later on top of single-tag value lookup, but are outside this design.
6. **Policy Semantics**: This design stores, exposes, and looks up values. How authorization plugins
   interpret those values is handled by policy-specific designs.
7. **Allowed Value Mutation**: Changing or clearing a tag's `allowedValues` after creation is
   outside the first implementation.
8. **Engine Pushdown**: This design does not push tag values to Hive, Iceberg, JDBC catalogs, or
   compute engines.

---

## Solution Investigations

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| Store values in tag `properties` | No storage schema change | Properties are global to the tag, so different objects cannot have different values | Rejected |
| Add a `tag_values` field to `tag_relation_meta` | Preserves the current one-row-per-assignment shape; minimizes write-path changes | Values are stored as JSON/CLOB, so exact lookup cannot use a normal relational index | Rejected |
| Add a separate assignment-value table | Preserves one assignment row per object/tag while storing each value in an indexed row | Adds a new table and extra joins even though the value belongs only to the tag relation | Rejected |
| Add `tag_value` to `tag_relation_meta` and store one row per value | Keeps values on the existing tag relation table; supports exact value lookup with a normal index; avoids an extra table | Changes `tag_relation_meta` from one row per assignment to one row per assignment value, so read paths must group rows by tag/object | **Chosen** |

---

## Proposal

### Terminology

- **Tag**: The metalake-scoped tag metadata stored in `tag_meta`.
- **Tag assignment**: The logical relation between one metadata object and one tag.
- **Assignment values**: The string values stored on a tag assignment.
- **Allowed values**: The optional tag-level whitelist that constrains assignment values for that
  tag.
- **Relation value row**: One physical row in `tag_relation_meta`. For valued assignments, each
  value has one row. For valueless assignments, one row has `tag_value` set to `NULL`.

One logical assignment continues to exist for each tuple:

```text
(tag_id, metadata_object_id, metadata_object_type)
```

The logical assignment may contain an empty value list or multiple values.

### Value Semantics

Assignment values follow these rules:

1. Values are strings.
2. In assignment update requests, a pair with an omitted or null `value` represents a valueless tag
   assignment request.
3. A logical assignment with one active null row means the tag is assigned without values, matching
   current tag behavior.
4. A logical assignment with one or more active non-null rows means the tag is assigned with those
   values. Public value arrays omit the null marker.
5. Blank individual assignment values are invalid. Non-null values are limited to 256 characters so
   they can be indexed consistently.
6. Duplicate pairs in one request are removed while preserving the first occurrence order. Read
   paths also de-duplicate duplicated active rows defensively if storage contains them.
7. Exact value lookup is case-sensitive, matching Gravitino's existing binary string comparison.
8. Adding a value is incremental. It does not replace the other active values for the same tag and
   metadata object.
9. Adding a non-null value to a tag that currently has only a valueless row converts that direct
   assignment to valued form by soft-deleting the null row and inserting the requested value row.
10. A tag may define optional allowed values. Missing allowed values means assignment values are
    unrestricted.
11. An empty allowed value list means the tag can only be assigned without values.
12. If allowed values are configured, every non-null assignment value must be in the allowed value
    list. Valueless assignment remains valid.
13. Allowed values use the same case-sensitive comparison and 256-character length limit as
    assignment values.

### Assignment Update Semantics

Assignment updates are expressed as pair deltas. Both `tagsToAdd` and `tagsToRemove` are arrays of
objects shaped as `(tag name, assignment value)`. Multiple values for one tag are represented by
multiple pairs with the same tag name and different values.

Pair identity is:

```text
(tag_id, metadata_object_id, metadata_object_type, tag_value)
```

A null `tag_value` is the valueless pair. Non-null values are normal indexed values.

The update semantics are:

1. Adding a non-null pair inserts that value row if it is not already active. Existing active
   non-null values for the same tag remain active.
2. Adding an already active pair is an idempotent no-op and does not create another physical row.
3. Adding a non-null pair when the same tag has an active valueless row converts the assignment to
   valued form by soft-deleting the null row and inserting the requested value row.
4. Adding a valueless pair inserts one null row only when the tag has no active non-null values
   after the request's removals are applied. If active non-null values remain, the request fails
   because the request is ambiguous.
5. Removing a non-null pair soft-deletes only the matching active value row. Removing a missing
   non-null pair is an idempotent no-op.
6. Removing a pair with null or omitted `value` removes all active rows for that tag on the
   metadata object. This preserves the old tag-name remove behavior while allowing value-specific
   removes.
7. Duplicate pairs inside `tagsToAdd` or `tagsToRemove` are de-duplicated while preserving first
   occurrence order.
8. If the same exact pair appears in both `tagsToAdd` and `tagsToRemove`, the two entries cancel
   each other and become a no-op for that pair.

Example sequence for table `t1`:

1. Add `(data_domain, finance)` and `(data_domain, risk)`.
   Active rows: `finance`, `risk`.
2. Add `(data_domain, ml)`.
   Active rows: `finance`, `risk`, `ml`; existing rows are not replaced.
3. Add `(data_domain, finance)` again.
   Result: no-op; active rows stay `finance`, `risk`, `ml`.
4. Remove `(data_domain, risk)`.
   Active rows: `finance`, `ml`.
5. Remove `(data_domain, risk)` again.
   Result: no-op; active rows stay `finance`, `ml`.
6. Remove `(data_domain, null)`.
   Result: all active rows for `data_domain` are removed.
7. Add `(data_domain, null)`.
   Result: one active valueless row with `tag_value IS NULL`.
8. Add `(data_domain, finance)`.
   Result: the null row is soft-deleted and `finance` becomes the active valued assignment.

### Inheritance Semantics

Inheritance continues to work by metadata hierarchy.

If a child object has a direct assignment for a tag, the direct assignment wins and inherited
assignments with the same tag name are not returned. The values returned for that tag are the values
from the winning assignment.

```text
catalog c1:        data_domain = ["finance"]
schema c1.s1:      no direct data_domain
table c1.s1.t1:    data_domain = ["risk", "ml"]

list tags for c1.s1:
  data_domain, inherited=true, values=["finance"]

list tags for c1.s1.t1:
  data_domain, inherited=false, values=["risk", "ml"]
```

The implementation should replace the current `Set<TagDTO>` deduplication in the REST layer with a
deterministic map keyed by tag name. Tags are added from the requested object to its ancestors, and
the first assignment wins. This avoids accidental merging of direct and inherited values.

### REST API Changes

#### POST /api/metalakes/{metalake}/tags

Create tag accepts an optional `allowedValues` field. Missing or null `allowedValues` means the tag
accepts any non-blank assignment value. An empty array means the tag can only be assigned without
values.

**Request addition:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `allowedValues` | array of string | no | Optional allowed assignment values for this tag. |

**Example:**

```json
{
  "name": "data_domain",
  "comment": "Business data domain",
  "allowedValues": ["finance", "risk", "ml"]
}
```

#### Allowed Value Mutation

The first implementation does not support changing or clearing `allowedValues` after tag creation.
`PATCH /api/metalakes/{metalake}/tags/{tag}` does not gain `setAllowedValues` or
`removeAllowedValues` operations in this design. Existing tag alter operations for current tag
fields are unchanged.

This avoids the need to validate or rewrite existing assignments when narrowing a whitelist. A later
design can add allowed value mutation with explicit conflict handling if needed.

#### GET /api/metalakes/{metalake}/tags and GET /api/metalakes/{metalake}/tags/{tag}

Detailed tag responses include `allowedValues` when the tag has a configured allowed value list. The
field is absent for unrestricted tags. An empty array means the tag is configured as valueless-only.

#### POST /api/metalakes/{metalake}/objects/{metadataObjectType}/{metadataObjectFullName}/tags

Add or remove tag-value pairs for a metadata object. Valueless assignments are represented by a
pair whose `value` is omitted or null. Multiple values for one tag are represented by multiple
pairs with the same `name` and different `value` fields.

**Request:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `tagsToAdd` | array of `TagValuePairRequest` | no | Add these tag-value pairs incrementally. |
| `tagsToRemove` | array of `TagValuePairRequest` | no | Remove these tag-value pairs incrementally. |

`TagValuePairRequest`:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Tag name. The tag must already exist in the metalake. |
| `value` | string or null | no | Assignment value for this pair. Missing or null represents the valueless row. Non-null values must satisfy the tag's `allowedValues` constraint. |

**Example:**

```json
{
  "tagsToAdd": [
    {
      "name": "pii"
    },
    {
      "name": "data_domain",
      "value": "finance"
    },
    {
      "name": "data_domain",
      "value": "risk"
    },
    {
      "name": "retention",
      "value": "7y"
    }
  ],
  "tagsToRemove": [
    {
      "name": "deprecated"
    },
    {
      "name": "data_domain",
      "value": "old"
    }
  ]
}
```

**Response:** `200 OK`

```json
{
  "code": 0,
  "names": ["pii", "data_domain", "retention"]
}
```

**Behavior:**

1. At least one of `tagsToAdd` or `tagsToRemove` must be present.
2. `tagsToAdd` adds pairs and preserves other active values for the same tag.
3. `tagsToRemove` removes only the requested pairs. It does not remove all values for the tag
   unless every active value pair is listed.
4. Duplicate pairs inside either array are de-duplicated.
5. The same exact pair cannot appear in both arrays in the same request.
6. A tag may appear multiple times in `tagsToAdd` or `tagsToRemove` when each pair has a different
   non-null value.
7. `tagsToAdd` cannot mix the valueless pair and non-null values for the same tag in one request.
8. The response returns distinct direct tag names associated with the metadata object after the
   operation.

#### GET /api/metalakes/{metalake}/objects/{metadataObjectType}/{metadataObjectFullName}/tags

Without `details=true`, the endpoint returns distinct tag names. With `details=true`, it returns
tag objects with global tag metadata, `allowedValues`, assignment-context `values`, and the
`inherited` flag.

**Response with `details=true`:**

```json
{
  "code": 0,
  "tags": [
    {
      "name": "data_domain",
      "comment": "Business data domain",
      "properties": {},
      "allowedValues": ["finance", "risk", "ml"],
      "values": ["finance", "risk"],
      "audit": {
        "creator": "gravitino",
        "createTime": "2026-07-16T00:00:00Z"
      },
      "inherited": false
    }
  ]
}
```

#### GET /api/metalakes/{metalake}/objects/{metadataObjectType}/{metadataObjectFullName}/tags/{tag}

Returns the tag object for the metadata object, checking ancestors when the tag is not directly
assigned. The response includes `allowedValues` and `values` from the direct or inherited winning
assignment.

#### GET /api/metalakes/{metalake}/tags/{tag}/objects

Returns metadata objects associated with the tag. The endpoint gains an optional `value` query
parameter for exact value lookup; no dedicated `objects/search` endpoint is added.

**Query parameters:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `value` | string | no | Exact assignment value filter. Missing means no value filter. |
| `details` | boolean | no | If true, return assignment details, including values. Default is false. |

**Example:**

```http
GET /api/metalakes/prod/tags/data_domain/objects?value=finance&details=true
```

**Response with `details=true`:**

```json
{
  "code": 0,
  "assignments": [
    {
      "metadataObject": {
        "fullName": "catalog1.schema1.table1",
        "type": "TABLE"
      },
      "values": ["finance", "risk"]
    }
  ]
}
```

**Behavior:**

1. Without `value`, the endpoint returns each metadata object associated with the tag once, even
   when the assignment has multiple physical value rows.
2. With `value`, the endpoint returns each metadata object whose direct assignment for the tag
   contains that exact non-null assignment value.
3. `value` must be non-blank and no longer than 256 characters.
4. Matching is exact and case-sensitive.
5. A valueless assignment row is not returned by a value-filtered lookup.
6. `details=true` returns the full assignment value array for each returned object, not only the
   matched value.
7. The first implementation does not add `ANY`, `ALL`, inherited value lookup, or multi-value query
   semantics to this endpoint.
8. Authorization filtering is applied before returning results. Objects the caller cannot access are
   omitted from the response.

### Java Client API

Object tag APIs:

```java
String[] listTags();
Tag[] listTagsInfo();
Tag getTag(String name);
String[] assignTags(TagValuePair[] tagsToAdd, TagValuePair[] tagsToRemove);
```

`TagValuePair` represents one assignment pair:

```java
TagValuePair.of(String name, String value);
TagValuePair.valueless(String name);
```

Global tag APIs include allowed value support only on create:

```java
Tag createTag(String name, String comment, Map<String, String> properties);
Tag createTag(
    String name,
    String comment,
    Map<String, String> properties,
    String[] allowedValues);
Tag alterTag(String name, TagChange... changes);
```

`alterTag` keeps the existing tag alteration behavior. This design does not add a Java client
operation for changing or clearing `allowedValues`.

Tag associated-object APIs add value lookup on the existing object listing API:

```java
MetadataObject[] objects();
MetadataObject[] objects(String value);
```

`assignTags` applies incremental pair deltas. Multiple values for one tag are passed as multiple
`TagValuePair` entries. The return value is all tag names directly associated with the metadata
object after the operation.

`objects(String value)` returns metadata objects whose direct assignment for this tag contains the
exact value. It uses the same associated-object response shape as `objects()` and does not introduce
`ANY`, `ALL`, inherited lookup, or a separate search-result model.

`Tag` gains tag-level allowed values and assignment-context values:

```java
Optional<String[]> allowedValues();
Optional<String[]> assignmentValues();
```

`allowedValues()` returns `Optional.empty()` when the tag is unrestricted and
`Optional.of(...)` when a whitelist is configured. `assignmentValues` is present only when the tag
is returned from a metadata-object tag API. A valueless assignment returns
`Optional.of(new String[0])`. Global tag APIs such as `getTag` return `Optional.empty()` for
`assignmentValues`.

### Python Client API

The Python client should mirror the Java client. It supports allowed values on tag creation, but
does not add an operation to change or clear `allowed_values` after creation:

```python
client.create_tag(
    name="data_domain",
    comment="Business data domain",
    allowed_values=["finance", "risk", "ml"],
)

supports_tags.assign_tags(
    tags_to_add=[
        {"name": "data_domain", "value": "finance"},
        {"name": "data_domain", "value": "risk"},
        {"name": "pii", "value": None},
    ],
    tags_to_remove=[
        {"name": "deprecated", "value": None},
        {"name": "data_domain", "value": "old"},
    ],
)

tag = client.get_tag("data_domain")
tag.associated_objects().objects(value="finance")
```

Detailed tag models expose `allowed_values` for tag-level constraints and `assignment_values`
when the tag was loaded from a metadata object. Value-filtered associated-object lookup returns the
same metadata object shape as unfiltered associated-object listing. When REST `details=true` is
requested, the response includes the full assignment value array for each returned object.

### Data Model

Allowed values are tag-level metadata. Add `allowed_values` to `tag_meta`:

- The column stores a JSON-serialized array of normalized strings.
- `NULL` means the tag is unrestricted.
- `[]` means the tag can only be assigned without values.
- The column is not indexed because allowed values are used for validation and display, not value
  lookup.

Target schema additions:

```sql
-- MySQL
ALTER TABLE `tag_meta`
  ADD COLUMN `allowed_values` MEDIUMTEXT DEFAULT NULL COMMENT 'tag allowed values';

-- PostgreSQL
ALTER TABLE tag_meta
  ADD COLUMN allowed_values TEXT DEFAULT NULL;

-- H2
ALTER TABLE `tag_meta`
  ADD COLUMN `allowed_values` CLOB DEFAULT NULL COMMENT 'tag allowed values';
```

Use `tag_relation_meta` for both logical tag assignments and assignment values. Add two columns:

- `tag_value`: the assignment value for this physical relation row. `NULL` means the tag is
  assigned without values. Non-null user-provided values cannot be blank.
- `value_order`: the display order for non-null values. Existing rows keep their order, and new
  values are appended after the current maximum order.

A logical assignment is represented as follows:

| Logical state | Active rows in `tag_relation_meta` |
|---------------|------------------------------------|
| Tag assigned without values | One row with `tag_value IS NULL` and `value_order = 0` |
| Tag assigned with values | One row per value, each with non-null `tag_value` |
| Tag not assigned | No active rows |

No replacement unique key is added for tag assignment values. The old logical-assignment unique key
must be dropped because it would block multiple value rows for the same tag and object. Duplicate
rows are prevented by request normalization and transactional write logic, and detailed reads
defensively group and de-duplicate rows.

`current_version` and `last_version` are used to avoid full delete-and-reinsert cycles for repeated
assignment updates. For a relation value row, `current_version` is the logical assignment version in
which the pair first became active. `last_version` is the latest logical assignment version that
touched the row. Active rows are still selected by `deleted_at = 0`.

For each changed `(tag_id, metadata_object_id, metadata_object_type)` logical assignment, the write
path computes `nextVersion = max(last_version) + 1` under the relation update lock. Existing pairs
that remain active keep their `tag_value`, `value_order`, `current_version`, and `deleted_at`; only
`last_version` is advanced to `nextVersion`. Newly added pairs are inserted with
`current_version = last_version = nextVersion`. Removed pairs keep their original `current_version`,
set `last_version = nextVersion`, and set `deleted_at` to the soft-delete timestamp. If a request is
entirely idempotent, the write path performs no relation-row update and does not bump versions.

`value_order` is also maintained incrementally. Existing values keep their order. Newly added
non-null values receive increasing order values after the current maximum. Removing values does not
renumber the remaining rows. Detailed reads sort by `value_order` and then by value for stable
output.

Target MySQL shape for `tag_relation_meta`:

```sql
CREATE TABLE IF NOT EXISTS `tag_relation_meta` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `tag_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'tag id',
    `metadata_object_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metadata object id',
    `metadata_object_type` VARCHAR(64) NOT NULL COMMENT 'metadata object type',
    `tag_value` VARCHAR(256) DEFAULT NULL COMMENT 'tag assignment value',
    `value_order` INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'tag assignment value order',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'tag relation audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'tag relation current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'tag relation last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'tag relation deleted at',
    PRIMARY KEY (`id`),
    KEY `idx_tid` (`tag_id`),
    KEY `idx_mid` (`metadata_object_id`),
    KEY `idx_trel_tag_value` (`tag_id`, `tag_value`, `deleted_at`),
    KEY `idx_trel_object` (`metadata_object_id`, `metadata_object_type`, `deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT 'tag metadata object relation';
```

Target PostgreSQL shape for `tag_relation_meta`:

```sql
CREATE TABLE IF NOT EXISTS tag_relation_meta (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY,
    tag_id BIGINT NOT NULL,
    metadata_object_id BIGINT NOT NULL,
    metadata_object_type VARCHAR(64) NOT NULL,
    tag_value VARCHAR(256) DEFAULT NULL,
    value_order INT NOT NULL DEFAULT 0,
    audit_info TEXT NOT NULL,
    current_version INT NOT NULL DEFAULT 1,
    last_version INT NOT NULL DEFAULT 1,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS tag_relation_meta_idx_tag_id
  ON tag_relation_meta (tag_id);
CREATE INDEX IF NOT EXISTS tag_relation_meta_idx_metadata_object_id
  ON tag_relation_meta (metadata_object_id);
CREATE INDEX IF NOT EXISTS tag_relation_meta_idx_tag_value
  ON tag_relation_meta (tag_id, tag_value, deleted_at);
CREATE INDEX IF NOT EXISTS tag_relation_meta_idx_object
  ON tag_relation_meta (metadata_object_id, metadata_object_type, deleted_at);
```

Target H2 shape for `tag_relation_meta`:

```sql
CREATE TABLE IF NOT EXISTS `tag_relation_meta` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `tag_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'tag id',
    `metadata_object_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metadata object id',
    `metadata_object_type` VARCHAR(64) NOT NULL COMMENT 'metadata object type',
    `tag_value` VARCHAR(256) DEFAULT NULL COMMENT 'tag assignment value',
    `value_order` INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'tag assignment value order',
    `audit_info` CLOB NOT NULL COMMENT 'tag relation audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'tag relation current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'tag relation last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'tag relation deleted at',
    PRIMARY KEY (`id`),
    KEY `idx_tid` (`tag_id`),
    KEY `idx_mid` (`metadata_object_id`),
    KEY `idx_trel_tag_value` (`tag_id`, `tag_value`, `deleted_at`),
    KEY `idx_trel_object` (`metadata_object_id`, `metadata_object_type`, `deleted_at`)
) ENGINE=InnoDB;
```

Schema changes add nullable `tag_value`, `value_order`, and `allowed_values`. The old tag relation
unique key is dropped and no replacement unique key is added.

Direct value lookup resolves `tag_name` to `tag_id` and uses `idx_trel_tag_value` when the
existing tag objects listing API receives a `value` filter. Detailed object
tag reads load relation rows by `(metadata_object_id, metadata_object_type)`, use `idx_trel_object`,
group rows by `(tag_id, metadata_object_id, metadata_object_type)`, ignore `NULL` values when
building the public value list, de-duplicate defensive duplicate rows, and order non-null values by
`value_order`.

### Storage and Core Changes

The current generic relation API returns target entities and does not carry relation payload. Tag
assignment values are payload on tag relations, so the implementation should add a tag-specific
relation update path and extend only the tag relation PO/mapper/provider shape.

Tag creation gains allowed value handling:

- `createTag` validates and stores normalized `allowedValues` in `tag_meta.allowed_values`.
- Tag alteration does not support changing or clearing `allowedValues` in the first implementation.
- Assignment writes load the stored allowed value list and reject non-null values outside it.

The proposed assignment write flow is:

```text
Client
  -> REST MetadataObjectTagOperations
  -> TagDispatcher / TagManager
  -> EntityStore tag-specific relation operation
  -> JDBCBackend
  -> TagMetaService
  -> tag_relation_meta
```

Add a tag relation operation that accepts:

- source metadata object identifier and type;
- `TagValuePair` entries to add;
- `TagValuePair` entries to remove.

Because there is no tag relation unique key, the write path must preserve logical-assignment
uniqueness with the relation update lock and one storage transaction. Assignment updates compute a
pair-level delta instead of replacing the complete value list:

1. Resolve the metadata object id and tag ids for every pair.
2. Normalize pair arrays: remove duplicate pairs, reject blank names, reject blank values, validate
   value length, and preserve pair order.
3. Reject the same exact pair in both `tagsToAdd` and `tagsToRemove`.
4. Reject mixed valueless and non-null additions for the same tag.
5. Load each tag's allowed values and reject non-null added values outside the allowed value list.
6. Load active relation rows for affected logical assignments under the relation update lock.
7. Apply removals first, soft-deleting only matching active pairs. Missing pairs are no-ops.
8. Apply additions, inserting only pairs that are not already active. Active pairs are no-ops.
9. Advance `current_version` and `last_version` only for logical assignments with effective
   changes, following the version-aware delta rules above.
10. Return the current direct tag assignments with grouped value arrays.

Adding a non-null pair to a tag with an active null row is a changed logical assignment: the null row
is soft-deleted and the non-null row is inserted in the same transaction. Adding a null pair to a tag
with active non-null rows is accepted only when the request's removals remove all remaining
non-null rows for that tag; otherwise it fails with `409 Conflict`.

Name-only reads must use distinct tag names because a valued assignment can now have multiple
physical rows. Detailed reads must group relation rows by logical assignment before building
`TagDTO`.

Value-filtered tag object lookup is implemented in `TagMetaService` by resolving `tag_name` to
`tag_id`, querying `tag_relation_meta` with `idx_trel_tag_value` for one exact non-null value,
grouping rows by metadata object, and de-duplicating defensive duplicates. The first implementation
returns direct assignments only and does not expand inherited effective assignments for value
lookup.

### Cache Behavior

Changing assignment values must invalidate the same relation cache entries as adding or removing a
tag:

1. The metadata-object-to-tag relation cache for the source metadata object.
2. The tag-to-metadata-object relation cache for every tag referenced by `tagsToAdd` and
   `tagsToRemove`.

This is required because a value update changes detailed tag responses even when the set of tag
names does not change.

Because allowed value mutation is not supported in the first implementation, no cache invalidation
path is needed for changing `allowedValues`. Tag metadata caches load the stored `allowedValues`
when a tag is created or read.

The value lookup result itself is not cached in the entity relation cache in the first
implementation. It should read from the `tag_relation_meta` value index so updates are visible after
the write transaction commits.

### Authorization

No new privilege is introduced.

Creating a tag with allowed values uses the same authorization path as creating the tag. Changing
allowed values is not supported in the first implementation. Setting assignment values uses the
same authorization expression as tag association:

```text
CAN_ACCESS_METADATA_AND_TAG
```

Reading allowed values uses the same authorization path as reading tag metadata. Reading assignment
values uses the same authorization path as reading detailed object tags. If a user cannot load a
tag, the tag, allowed values, and assignment values are filtered out together.

Value-filtered associated-object lookup requires access to the requested tag and filters every
returned metadata object through the same metadata access expression used by unfiltered object tag
listing. Unauthorized objects are omitted rather than returned with partial value data.

### Events

Tag create events should include allowed values through the tag metadata payload so listeners can
replicate tag definitions. Existing tag alter events remain unchanged because allowed value mutation
is not supported in the first implementation.

The tag association events should expose the incremental pair deltas so listeners can replicate tag
assignment state without interpreting a full replacement list. Because this design does not require
backward compatibility, the event model can replace string-array assignment accessors with pair
arrays.

For example:

```java
Optional<String[]> allowedValues();
TagValuePair[] tagsToAdd();
TagValuePair[] tagsToRemove();
```

### Error Handling

| Case | Response |
|------|----------|
| Tag name is blank | `400 Bad Request` |
| Allowed value is null, blank, or longer than 256 characters | `400 Bad Request` |
| Assignment value is blank or longer than 256 characters | `400 Bad Request` |
| Assignment value is not in the tag's configured allowed values | `400 Bad Request` |
| `tagsToAdd` and `tagsToRemove` contain the same exact pair | `400 Bad Request` |
| `tagsToAdd` contains both a valueless pair and non-null value pairs for the same tag | `400 Bad Request` |
| Adding a valueless pair while non-null values remain active for the same tag | `409 Conflict` |
| `value` query parameter is blank or longer than 256 characters | `400 Bad Request` |
| `tagsToAdd` or `tagsToRemove` references a tag that does not exist | `404 Not Found` |
| `tagsToAdd` targets an already active pair | `200 OK`; no relation row is inserted |
| `tagsToRemove` targets a missing pair | `200 OK`; no relation row is deleted |

---

## User Process

1. User creates a tag with allowed values:

   ```http
   POST /api/metalakes/prod/tags
   ```

   ```json
   {
     "name": "data_domain",
     "comment": "Business data domain",
     "allowedValues": ["finance", "risk", "ml"]
   }
   ```

2. User adds two values for the same tag by sending two pairs. The values must be in the tag's
   allowed value list:

   ```http
   POST /api/metalakes/prod/objects/table/lake.sales.orders/tags
   ```

   ```json
   {
     "tagsToAdd": [
       {
         "name": "data_domain",
         "value": "finance"
       },
       {
         "name": "data_domain",
         "value": "risk"
       }
     ]
   }
   ```

3. User lists detailed tags for the table:

   ```http
   GET /api/metalakes/prod/objects/table/lake.sales.orders/tags?details=true
   ```

4. User finds objects with a matching value through the existing tag objects lookup API:

   ```http
   GET /api/metalakes/prod/tags/data_domain/objects?value=finance&details=true
   ```

5. User incrementally adds another value. Existing values remain active:

   ```json
   {
     "tagsToAdd": [
       {
         "name": "data_domain",
         "value": "ml"
       }
     ]
   }
   ```

6. User removes one value without removing the other values:

   ```json
   {
     "tagsToRemove": [
       {
         "name": "data_domain",
         "value": "risk"
       }
     ]
   }
   ```

7. User removes the remaining values. After this request, the tag is no longer directly assigned to
   the table:

   ```json
   {
     "tagsToRemove": [
       {
         "name": "data_domain",
         "value": "finance"
       },
       {
         "name": "data_domain",
         "value": "ml"
       }
     ]
   }
   ```

8. The tag's allowed value list cannot be changed or cleared after creation in the first
   implementation.

---

## Implementation Process

Tag creation with allowed values follows this flow:

```text
REST request
  -> validate allowedValues: null/unset allowed, no null or blank entries, max 256 chars
  -> normalize allowedValues: remove duplicates while preserving order
  -> store allowedValues as JSON in tag_meta.allowed_values
  -> return TagDTO with allowedValues
```

Tag alteration does not add any allowed value change operation in the first implementation.

Assignment writes follow this flow:

```text
REST request
  -> validate TagsAssociateRequest with tagsToAdd/tagsToRemove pair arrays
  -> parse MetadataObject
  -> authorize metadata object and tags
  -> normalize pairs: remove duplicate pairs, reject blank names, reject blank values and >256 chars
  -> reject the same exact pair in both add and remove arrays
  -> reject mixed valueless and non-null additions for the same tag
  -> load allowedValues for tags being added
  -> reject added values outside configured allowedValues
  -> TagManager.assignTagsForMetadataObject(...)
  -> storage transaction:
       - resolve metadata object id
       - resolve tag ids
       - load active relation rows for affected logical assignments under lock
       - compute pair-level remove and add deltas
       - for changed logical assignments, compute nextVersion from max(last_version) + 1
       - soft-delete removed pair rows and advance their last_version
       - insert new pair rows with current_version = last_version = nextVersion
       - advance last_version for retained active rows in changed logical assignments
       - leave idempotent pairs untouched
       - return current direct tag assignments with grouped values
  -> invalidate relation caches only for logical assignments with effective changes
  -> return NameListResponse
```

Detailed reads follow this flow:

```text
REST list/get object tags
  -> load direct tag relation rows by object id and type
  -> group direct rows by logical assignment and build value arrays
  -> load tag metadata, including allowedValues
  -> load parent tag relation rows and values
  -> merge by tag name, nearest assignment wins
  -> set inherited flag
  -> filter tags through authorization
  -> return TagDTO with allowedValues and assignment values
```

Name-only reads follow the same relation lookup but return distinct tag names so a multi-value
assignment does not duplicate names.

Value lookup follows this flow:

```text
REST list tag objects with value filter
  -> validate one non-blank value, length <= 256
  -> authorize requested tag
  -> resolve tag id
  -> query tag_relation_meta by tag_id and exact non-null tag_value
  -> group matching rows by metadata object
  -> resolve metadata object full names
  -> filter metadata objects through authorization
  -> return metadata objects, or assignment details when details=true
```

---

## Task Breakdown

- [ ] Add nullable `allowed_values` to `tag_meta` in MySQL, PostgreSQL, and H2 schema and upgrade
      scripts.
- [ ] Add nullable `tag_value` and `value_order` columns to `tag_relation_meta` in MySQL,
      PostgreSQL, and H2 schema and upgrade scripts.
- [ ] Drop the existing tag relation unique key because multiple physical rows can now belong to
      one logical tag assignment. Do not add a replacement unique key.
- [ ] Add value lookup indexes on `tag_relation_meta` for `(tag_id, tag_value, deleted_at)` and
      object lookup indexes for `(metadata_object_id, metadata_object_type, deleted_at)`.
- [ ] Extend `TagPO`, `TagEntity`, `TagDTO`, `Tag`, mapper methods, SQL providers, and PO conversion
      utilities with `allowed_values`.
- [ ] Extend `TagMetadataObjectRelPO`, mapper methods, SQL providers, and PO conversion utilities
      with nullable `tag_value` and `value_order`.
- [ ] Add `allowedValues` to tag create responses and requests. Do not add allowed value
      update requests or `TagChange` variants in the first implementation.
- [ ] Add `TagValuePairRequest` and extend tag object listing response DTOs as needed for
      `details=true` assignment values.
- [ ] Extend `TagDTO` and `Tag` with assignment-context values.
- [ ] Add allowed value normalization and validation in tag create and valued assignment
      flows.
- [ ] Add tag-specific relation update support in `SupportsRelationOperations`,
      `RelationalEntityStore`, `JDBCBackend`, and `TagMetaService`.
- [ ] Update relation reads so name-only APIs return distinct tag names and detailed APIs group
      relation rows into logical assignments with value arrays.
- [ ] Add indexed direct value lookup support in `TagMetaService` using `tag_relation_meta`.
- [ ] Update `TagManager` and `TagDispatcher` with allowed values, valued assignment, and
      value-filtered tag object listing operations.
- [ ] Update REST tag operations to support `allowedValues`, `tagsToAdd` / `tagsToRemove` pair
      arrays, values in detailed object tag responses, and `GET /tags/{tag}/objects?value=...`.
- [ ] Replace object tag inheritance deduplication with nearest-assignment-wins merging by tag
      name.
- [ ] Update cache invalidation for value-only assignment updates.
- [ ] Update tag create events to include allowed values, and association events to include
      tag-value pair assignment deltas.
- [ ] Add Java client support for allowed values, `TagValuePair`, `assignTags`,
      `Tag.assignmentValues()`, and value-filtered associated-object lookup.
- [ ] Add Python client support for allowed values, tag-value pair assignment updates, and
      value-filtered associated-object lookup.
- [ ] Add unit tests for allowed value validation, request validation, DTO serialization, storage
      conversion, and inheritance merging.
- [ ] Add service tests for allowed value create validation, rejected allowed value mutation,
      repeated pair additions and removals, version-aware no-op updates, insert, update, remove,
      direct value lookup, distinct name reads, grouped detailed reads, and indexed value reads.
- [ ] Add REST tests for allowed values, rejected allowed value mutation, repeated pair
      assignments, valued assignments, value lookup validation, direct value lookup, conflicts, and
      authorization filtering.
- [ ] Add client tests for Java and Python allowed values, valued assignment, and value-filtered
      associated-object lookup APIs.
- [ ] Update `docs/open-api/tags.yaml` and validate with `./gradlew :docs:build`.
- [ ] Update user-facing tag documentation in `docs/manage-tags-in-gravitino.md`.
