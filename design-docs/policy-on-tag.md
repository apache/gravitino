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

# Design of Policy-on-Tag in Gravitino

---

## Background

Gravitino currently has two independent governance concepts:

| Concept | Current state |
|---------|---------------|
| Tag | A flat metalake-scoped metadata object used to classify or annotate metadata objects. Tags can be associated with catalogs, schemas, tables, filesets, topics, models, and columns. Tag listing follows the metadata object hierarchy, so a child object can receive tags from parent metadata objects. |
| Policy | A metalake-scoped metadata object with typed content, enabled state, and audit information. The current model allows policies to be associated directly with metadata objects. The system iceberg compaction policy is the first built-in policy type and is consumed by the table maintenance service. |

The current object-side governance model is:

```text
Tag    -> Metadata Object
Policy -> Metadata Object
```

This direct object policy model is understandable for a small number of objects, but it creates
problems when governance needs to scale across many catalogs, schemas, tables, and columns:

1. Users must manage both object tags and object policies on the same metadata object.
2. Policy assignment does not naturally follow classification. A table can be marked as
   `maintenance_standard`, but the maintenance policy still has to be attached separately.
3. New objects can be missed unless administrators attach policies to every new object or rely on
   ancestor-level direct policy assignment.
4. TMS needs maintenance policy selection now, while future ABAC needs tag-driven policy selection.
   Direct object policies do not provide a shared selection layer for both scenarios.
5. Keeping two object-side governance paths makes the user model harder to explain and document.

The proposed target model is:

```text
Policy -> Tag -> Metadata Object
```

Policy remains a first-class object. Tags become the only object-side governance attachment point.
An object policy is a read-only policy result for a metadata object, derived from the tags the
object has or inherits from parent metadata objects. Tags themselves are not nested.

---

## Goals

1. **Single Object-Side Attachment Point**: Metadata objects receive governance behavior only
   through tags, not through direct policy attachment.
2. **Reusable Policy Lifecycle**: Policies remain first-class objects with typed content, enabled
   state, audit information, and metalake-scoped lifecycle operations.
3. **Immutable Policy Definitions**: Policy definitions are not modified in place. To change policy
   behavior, administrators create a replacement policy and update policy-to-tag associations.
4. **Policy-to-Tag Association**: Administrators can associate policies with tags and inspect which
   tags carry a policy.
5. **Flat Tags**: Tags remain flat metalake-scoped objects. The design does not add parent tags,
   child tags, tag groups, or tag-to-tag inheritance.
6. **Object Policy Resolution**: Gravitino can compute object policies for a metadata object from
   its effective tags, including inherited tags.
7. **Read-Only Object Policies**: Object policies are derived results. Users cannot create, alter,
   enable, disable, delete, or associate policies directly on a metadata object.
8. **Explicit Visibility Privileges**: Tag and policy visibility are controlled by read-only
   privileges that are separate from mutation privileges.
9. **Secure Policy Enforcement**: Row filter and column mask enforcement does not depend on whether
   the end user can view policy details.
10. **TMS Integration**: TMS consumes the system iceberg compaction policy through object policy
    lookup, not through direct object policy relations.
11. **ABAC Evolution Path**: The resolver boundary can later support tag-expression policies without
    changing every policy consumer.
12. **Explicit Breaking Migration**: Existing direct object policy relations are migrated or retired
    explicitly; runtime behavior does not read direct object policy relations.

---

## Non-Goals

1. **Direct Object Policy Compatibility**: Gravitino will not preserve direct policy attachment to
   metadata objects in the target model. Object-side policy behavior comes only from tags.
2. **Object Policy Mutation**: Object policies are not mutable entities. The object policy API is a
   lookup API, not a create, update, delete, enable, disable, or association API.
3. **Nested Tags**: This design does not introduce tag hierarchy, tag groups, parent tags, child
   tags, tag-to-tag relations, or tag-to-tag inheritance. Policy selection is based on flat tags
   assigned to metadata objects.
4. **Tag-Expression ABAC in Phase 1**: The first phase does not introduce an expression language.
   Policy selection is a fixed relation from policy to tag.
5. **Full Explainability UI**: A dedicated UI is not required for the first milestone. APIs must
   still return enough source information to trace object policy sources.
6. **Multi-Value Tag Append Semantics**: This design does not append multiple values to the same tag
   key on one object. If assignment values are used, the same tag key has one effective value.
7. **Engine-Specific Enforcement Plugins**: This design defines policy selection inside Gravitino.
   Trino, Spark, Iceberg, or OPA enforcement integrations are separate designs.

---

## Solution Investigations

### Policy Assignment Model

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| Direct object policy | Simple to understand for one object; current implementation already exists. | Does not scale well; duplicates object-side tag and policy management; does not align classification with action. | Rejected |
| Controls embedded in tags | Simplest user model; objects only receive tags. | Tags become heavy governance objects; policy lifecycle, reuse, audit, enable/disable, and versioning are weaker; does not keep tag and policy concepts clear. | Rejected |
| Policy-on-tag | Keeps policies reusable and auditable; makes tags the only object-side attachment point; works for TMS without an expression engine; keeps a clean path to ABAC. | Requires a new relation table, resolver, APIs, and breaking migration away from direct object policy. | **Chosen** |
| Nested tags | Can model classification hierarchy directly in tag objects. | Adds a second hierarchy beside metadata object hierarchy; complicates policy resolution, authorization, migration, and explainability. | Rejected |
| Tag-expression ABAC | Most expressive; supports complex conditions over tag names, tag values, principals, and scopes. | Requires expression language, matching engine, and more complex UX; too large for the next milestone. | Future |

Policy-on-tag is the best next step because it is useful as a standalone model and keeps the
long-term ABAC path open. The consumer path can stay stable:

```text
Consumer -> ObjectPolicyResolver -> object policies
```

Only the policy selection layer needs to evolve later from fixed `policy -> tag` relations to tag
expressions.

### Row Filter and Column Mask Conflict Handling

| Approach | Example | Trade-off | Decision |
|----------|---------|-----------|----------|
| Restrict conflicts at configuration time | Snowflake allows only one directly assigned row access policy on a table or view, and evaluates row access policies before masking policies. | Simple and predictable, but stricter for administrators. | Rejected for tag-driven policy selection because conflicts can still arise from multiple effective tags. |
| Combine row filters with OR semantics | BigQuery combines multiple row-level access policies with OR semantics. | Flexible, but can broaden access and is risky as a default for tag-driven governance. | Rejected |
| Priority-based resolution | Some systems can choose a winning policy by priority. | Flexible, but effective access becomes harder to reason about and easier to misconfigure. | Rejected |
| Fail closed on ambiguity | Databricks ABAC blocks access when multiple distinct row filters or column masks apply to the same target. | Safest default, but administrators must fix overlapping tags or policy associations. | **Chosen** |

### Tag Value Semantics

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| Append values | Can express multiple values for the same tag key on one object. | Makes policy selection ambiguous and can trigger multiple policies for one logical classification dimension. | Rejected |
| Overwrite values | Keeps each tag key single-valued for one object; aligns with common tag and label systems. | Administrators must use separate tag keys for separate dimensions. | **Chosen** |

---

## Proposal

### Target Model

The target model has four concepts:

| Concept | Description |
|---------|-------------|
| Policy | A metalake-scoped governance rule with typed content, enabled state, audit information, and version history. |
| Tag | A flat metalake-scoped classification object associated with metadata objects. Tags do not have parent or child tags. |
| Policy-tag relation | A relation that binds one policy to one tag in the same metalake. |
| Object policy | A read-only policy result for a metadata object, derived from effective tags and policy-tag relations. |

Object-side governance becomes:

```text
Metadata Object -> Effective Tags -> Object Policies
```

Metadata objects do not store direct policy relations. Object policies are not persisted as separate
entities.

### Effective Tag Semantics

Policy-on-tag reuses the current metadata-object tag inheritance model. This is not tag nesting:
tags are flat, and only tag assignments flow through the metadata object hierarchy. Examples use
flat tag names without dot separators.

1. Tags associated with an object are direct tags.
2. Tags associated with parent metadata objects are inherited tags.
3. If a child object has a direct assignment for a tag name, that direct tag becomes the effective
   source for that tag and overrides inherited assignments with the same tag name.
4. Policy-on-tag uses tag presence for policy resolution. It does not need assignment values to
   resolve the phase-1 policy set.
5. The effective tag set is the de-duplicated result of walking from the object to its ancestors.
6. Policies bound to effective tags become object policy candidates.

### Policy Supported Object Types

`PolicyContent.supportedObjectTypes()` should mean "the metadata object types on which this policy
content can appear as an object policy." In the policy-on-tag model, the association target is
always a tag, so `supportedObjectTypes()` no longer means "the metadata object types to which the
policy can be directly attached."

Example:

```text
policy iceberg_compaction_standard
  policyType: system_iceberg_compaction
  supportedObjectTypes: [CATALOG, SCHEMA, TABLE]

tag maintenance_standard
  policies: [iceberg_compaction_standard]

catalog iceberg
  tags: [maintenance_standard]

table iceberg.db.orders
  effective tags: [maintenance_standard]
  object policies: [iceberg_compaction_standard]
```

### Policy Mutability

Policy definitions are immutable after creation. Users can create, view, delete, enable, disable,
and associate policies according to authorization rules, but they cannot update an existing policy
definition in place.

If policy behavior needs to change, administrators create a new policy and update the relevant
policy-to-tag association.

### Object Policy Mutability

Object policies are read-only derived results.

Users cannot perform these operations on a metadata object:

```text
create object policy
alter object policy
enable object policy
disable object policy
delete object policy
associate object policy
disassociate object policy
```

To change the object policy result, users must change one of the source inputs:

1. create a replacement policy and update the policy-to-tag relation;
2. enable or disable the policy object;
3. associate or disassociate the policy with a tag;
4. assign or remove the tag from the metadata object or one of its ancestors.

### Data Model

Existing policy metadata tables remain:

```text
policy_meta
policy_version_meta
```

Existing tag metadata and tag assignment tables remain:

```text
tag_meta
tag_relation_meta
```

There is no tag-to-tag relation table. The design does not add parent tag IDs, nested tag paths, or
tag hierarchy metadata.

The current policy-to-metadata-object relation table is not part of the target model:

```text
policy_relation_meta
```

Add a policy-to-tag relation table:

```text
policy_tag_relation_meta
  policy_id
  tag_id
  audit_info
  current_version
  last_version
  deleted_at
```

Constraints and indexes:

1. Active rows are unique by `(policy_id, tag_id)`.
2. Index `tag_id` for object policy lookup from tags.
3. Index `policy_id` for impact analysis from policies.
4. Policy and tag must belong to the same metalake.
5. The table follows the same soft-delete and version fields as existing relation tables.

### REST API Changes

#### New: `GET /api/metalakes/{metalake}/tags/{tag}/policies`

**Request:** No body.

| Query parameter | Type | Required | Description |
|-----------------|------|----------|-------------|
| `details` | boolean | no | If true, return full policy objects. If false, return policy names. |

**Response:** `200 OK`

```json
{
  "names": ["iceberg_compaction_standard"]
}
```

With `details=true`:

```json
{
  "policies": [
    {
      "name": "iceberg_compaction_standard",
      "policyType": "system_iceberg_compaction",
      "enabled": true,
      "content": {}
    }
  ]
}
```

**Behavior:** Lists policies directly associated with the tag. Returns `404 Not Found` if the tag
does not exist.

#### New: `POST /api/metalakes/{metalake}/tags/{tag}/policies`

**Request:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `policiesToAdd` | array of string | no | Policy names to associate with the tag. |
| `policiesToRemove` | array of string | no | Policy names to disassociate from the tag. |

```json
{
  "policiesToAdd": ["iceberg_compaction_standard"],
  "policiesToRemove": ["iceberg_compaction_legacy"]
}
```

**Response:** `200 OK`

```json
{
  "names": ["iceberg_compaction_standard"]
}
```

**Behavior:** Updates policy associations for one tag. The tag and all added policies must exist in
the same metalake. Adding an already associated policy returns a duplicate association error.
Removing a missing association is ignored. A policy listed in both arrays is ignored.

#### New: `GET /api/metalakes/{metalake}/policies/{policy}/tags`

**Request:** No body.

| Query parameter | Type | Required | Description |
|-----------------|------|----------|-------------|
| `details` | boolean | no | If true, return full tag objects. If false, return tag names. |

**Response:** `200 OK`

```json
{
  "names": ["maintenance_standard"]
}
```

**Behavior:** Lists tags that carry the policy. This is used for impact analysis before disabling
or deleting a policy. Returns `404 Not Found` if the policy does not exist.

#### Changed: `GET /api/metalakes/{metalake}/objects/{type}/{fullName}/policies`

**Request:** No body.

| Query parameter | Type | Required | Description |
|-----------------|------|----------|-------------|
| `details` | boolean | no | If true, return policy content and source information. If false, return policy names. |

**Response:** `200 OK`

```json
{
  "policies": [
    {
      "name": "iceberg_compaction_standard",
      "policyType": "system_iceberg_compaction",
      "enabled": true,
      "sourceTag": "maintenance_standard",
      "sourceTagInherited": true,
      "sourceObjectType": "CATALOG",
      "sourceObjectName": "iceberg"
    }
  ]
}
```

**Old behavior:** This API lists policies directly associated with a metadata object.

**New behavior:** This API becomes a read-only derived object policy lookup API. It resolves object
policies from effective tags and policy-to-tag relations. It does not read direct policy relations
and does not modify policy objects or object-policy relationships.

**Migration impact:** Callers must use policy-to-tag association APIs and read object policies from
`GET /api/metalakes/{metalake}/objects/{type}/{fullName}/policies`. Object-side policy association
calls must be replaced with tag assignment calls.

#### Removed: Direct Object Policy Mutation APIs

The target model removes direct object policy mutation and direct object-policy-detail APIs:

```text
GET  /api/metalakes/{metalake}/objects/{type}/{fullName}/policies/{policy}
POST /api/metalakes/{metalake}/objects/{type}/{fullName}/policies
```

**Old behavior:** These APIs get or associate policies directly on metadata objects.

**New behavior:** Object policies are derived from tags and are only exposed through the object
policy lookup API.

**Migration impact:** Callers must move direct policy association workflows to tag assignment and
policy-to-tag association workflows.

### Client API Changes

| Area | Old API | New API |
|------|---------|---------|
| Object policy association | `SupportsPolicies.associatePolicies(String[] add, String[] remove)` | Removed from metadata object mixins in the target model. |
| Object policy listing | `SupportsPolicies.listPolicies()` | Reinterpreted as read-only derived object policy lookup. |
| Tag policy association | None | New tag-scoped API such as `associatePoliciesForTag(tagName, add, remove)`. |
| Policy impact analysis | `Policy.associatedObjects()` | Replaced or supplemented by `Policy.associatedTags()`. |

The exact Java and Python method names can be finalized during implementation, but the API shape
must not expose direct object policy association or object policy mutation as target behavior.

### Object Policy Resolution Algorithm

```text
ObjectPolicyResolver
  input: metalake, metadata object
  output: object policies
```

Algorithm:

1. Validate that the metadata object exists.
2. Load effective tags for the object using current inheritance semantics.
3. Load policies associated with those tags in batch.
4. Drop disabled policies.
5. Drop policies whose `supportedObjectTypes()` does not contain the requested metadata object type.
6. Return the object policies and their source tag information.

### Authorization, Visibility, and Audit

Policy-on-tag makes tags part of the governance control plane. Assigning a high-impact tag can
change security, maintenance, or retention behavior.

Add two read-only visibility privileges:

```text
VIEW_TAG
VIEW_POLICY
```

Existing mutation privileges keep their current semantics:

```text
APPLY_TAG
APPLY_POLICY
```

Recommended binding scopes:

| Privilege | Binding Scope | Meaning |
|-----------|---------------|---------|
| `VIEW_TAG` | `METALAKE`, `TAG` | View, list, and get tag metadata. |
| `VIEW_POLICY` | `METALAKE`, `POLICY` | View, list, and get policy metadata and details. |
| `APPLY_TAG` | `METALAKE`, `TAG` | Attach a tag to an object. |
| `APPLY_POLICY` | `METALAKE`, `POLICY` | Associate a policy with a tag. |

Recommended authorization rules:

1. Creating and deleting policies keeps existing policy privileges.
2. Creating and altering tags keeps existing tag privileges.
3. Associating a policy with a tag is both a policy operation and a tag operation. It requires
   permission on both the policy and the tag, or metalake owner privilege.
4. Tag assignment permission alone is not enough to attach a policy to a tag. Policy permission
   alone is not enough to bind the policy to arbitrary tags.
5. Assigning a tag to a metadata object continues to require tag-application permission and access
   to the metadata object.
6. Object policy lookup requires access to the metadata object.
7. Detailed policy content requires `VIEW_POLICY`; source tag details require `VIEW_TAG`.

Policy enforcement must be independent from policy visibility. A user does not need `VIEW_POLICY`
for row filters or column masks to take effect. The trusted server-side enforcement path must always
resolve and enforce applicable policies, even when the end user cannot see policy details.

For object policy APIs, users without `VIEW_POLICY` may receive only a redacted governance summary,
such as policy type and `enforced=true`. Users with `VIEW_POLICY` may receive policy names, types,
definitions, and other policy details. Source tag information should only be returned when the user
also has `VIEW_TAG`.

Audit events should cover policy lifecycle changes, policy-to-tag relation changes, and
tag-to-object assignment changes.

### Row Filter and Column Mask Conflict Handling

For row filter and column mask policies, if multiple distinct effective policies of the same kind
apply to the same evaluation target, access should be denied. The system should not merge them,
union them, or choose one by priority in this design.

Multiple identical policies may be deduplicated and enforced once. Administrators must consolidate
tags or policy associations before access is allowed.

### Tag Value Semantics

Policy-on-tag phase 1 resolves policies by tag presence. If assignment-level values are used by
future policy selectors, the same tag key on the same object must use overwrite semantics, not
append semantics.

For a given object, each tag key can have at most one effective value. Assigning the same tag key
with a new value replaces the existing direct value. If inheritance is supported, a direct value
overrides the inherited value. Removing the direct value reveals the inherited value again.

Do not allow multi-value append behavior such as:

```text
sensitivity = high, restricted
```

Use one value per tag key:

```text
sensitivity = restricted
```

If multiple dimensions are needed, use multiple tag keys:

```text
sensitivity = restricted
domain = finance
retention = 7_years
```

### TMS Integration

TMS consumes object policies instead of direct object policies:

```text
TMS selects candidate table
  -> ObjectPolicyResolver(table)
  -> system_iceberg_compaction object policy
  -> generate strategy and job context
  -> schedule compaction job
```

Rules:

1. If no system iceberg compaction policy appears in the object policy result, TMS does not generate
   a compaction strategy from policy-on-tag.
2. If a system iceberg compaction policy appears in the object policy result, TMS uses its content.
3. TMS does not read direct object policy relations.

### User Process

1. A governance administrator creates a policy, such as `iceberg_compaction_standard`.
2. The administrator creates or reuses a tag, such as `maintenance_standard`.
3. The administrator associates the policy with the tag.
4. A data owner assigns the tag to a catalog, schema, table, or column.
5. Gravitino resolves object policies from the object's effective tags.
6. TMS or another consumer reads object policies through a read-only lookup API.
7. To change the object policy result, the administrator creates a replacement policy, changes a
   policy-to-tag relation, changes a tag assignment, or changes policy enabled state.

### Implementation Process

```text
Client
  -> REST Server
  -> PolicyTagOperations / ObjectPolicyOperations
  -> PolicyDispatcher / TagDispatcher
  -> ObjectPolicyResolver
  -> EntityStore relation operations
  -> policy_tag_relation_meta, tag_relation_meta, policy_meta
```

Object policy lookup data flow:

```text
Metadata Object
  -> parent metadata object hierarchy
  -> effective tags
  -> policy-tag relations
  -> policy metadata
  -> enabled + supported object type filtering
  -> object policy response
```

### Migration Process

This design is a breaking model change. The target runtime does not read direct object policy
relations.

1. Inventory existing direct policy relations from `policy_relation_meta`.
2. Ask administrators to decide which tags should carry each policy.
3. Create missing tags.
4. Associate policies with tags.
5. Assign those tags to the target metadata objects or their ancestors.
6. Run an object policy verification command that compares expected objects with resolved object
   policies.
7. Upgrade consumers such as TMS to read only object policies.
8. Retire the direct policy relation table and direct association APIs.

Example migration mapping:

```yaml
policyTagMappings:
  iceberg_compaction_standard:
    tags:
      - maintenance_standard
objectTagMappings:
  CATALOG:iceberg:
    tags:
      - maintenance_standard
```

### ABAC Evolution

Policy-on-tag is a milestone, not the final ABAC model.

Recommended evolution:

1. `Policy -> Tag -> Metadata Object`: fixed policy-to-tag relations.
2. Tag assignment values: object tag assignments can carry values such as `data_domain = finance`.
3. Tag-expression policies: policies match expressions such as `hasTag("pii")` or
   `tagValue("data_domain") == "finance"`.
4. Explainability: APIs and UI show which selector matched and which tag caused the match.
5. Type-specific validation: future ABAC or policy consumers may reject unsupported combinations of
   matching policies. This phase returns the derived object policy set and defines fail-closed
   behavior for ambiguous row filter and column mask enforcement.

---

## Task Breakdown

- [ ] Add `policy_tag_relation_meta` storage schema and entity store operations.
- [ ] Add core API and DTO contracts for policy-to-tag association and object policy source
      information.
- [ ] Implement `ObjectPolicyResolver` to resolve object policies from effective tags.
- [ ] Add REST endpoints for tag policy association and policy tag listing.
- [ ] Change object policy REST APIs to read-only derived lookup and remove direct object policy
      mutation behavior.
- [ ] Add Java client and Python client support for policy-to-tag association and derived object
      policy lookup.
- [ ] Add `VIEW_TAG` and `VIEW_POLICY` authorization privileges and enforce visibility redaction in
      object policy responses.
- [ ] Add fail-closed validation for multiple distinct row filter or column mask policies on the
      same evaluation target.
- [ ] Integrate TMS with `ObjectPolicyResolver` instead of direct object policy relations.
- [ ] Add migration tooling or documentation for moving direct object policy relations to
      policy-to-tag relations and tag assignments.
- [ ] Update OpenAPI specifications in `docs/open-api/*.yaml` and validate them with
      `./gradlew :docs:build`.
- [ ] Add unit and integration tests for storage, resolver, authorization, REST APIs, and TMS
      integration.
