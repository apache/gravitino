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

## Core Design

The core model is that policies are associated with tag names, not directly with tag assignment
values. A policy-to-tag relation may include a simple `selector` JSON object that decides whether
the relation matches a metadata object's effective tag assignment values. The selector refines when
a policy bound to a tag name applies; it does not make policy objects bind directly to individual
tag values.

For example, `retention_finance` is associated with the tag name `data_domain`. Its
`selector` can be `{ "type": "TAG_VALUE", "value": "finance" }`, so the policy applies only
when the object's effective `data_domain` tag assignment contains `finance`.

---

## Background

Gravitino currently has two independent governance concepts:

| Concept | Current state |
|---------|---------------|
| Tag | A flat metalake-scoped metadata object used to classify or annotate metadata objects. Tags can be associated with catalogs, schemas, tables, filesets, topics, models, and columns. Tag listing already follows metadata object hierarchy, so a child object can receive tags from parent metadata objects. |
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
4. TMS needs maintenance policy selection now, while future governance use cases need tag-driven
   policy selection. Direct object policies do not provide a shared selection layer for both
   scenarios.
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
3. **Policy-to-Tag Association**: Administrators can associate policies with tags and inspect which
   tags carry a policy.
4. **Flat Tags**: Tags remain flat metalake-scoped objects. The design does not add parent tags,
   child tags, tag groups, or tag-to-tag inheritance.
5. **Object Policy Resolution**: Gravitino can compute object policies for a metadata object from
   its effective tags, including inherited tags.
6. **Read-Only Object Policies**: Object policies are derived results. Users cannot create, alter,
   enable, disable, delete, or associate policies directly on a metadata object.
7. **TMS Integration**: TMS consumes the system iceberg compaction policy through object policy
   lookup, not through direct object policy relations.
8. **Selector Boundary**: The resolver boundary owns selector matching, so policy
   consumers do not need to understand tag assignment details.
9. **Explicit Breaking Migration**: Existing direct object policy relations are migrated or retired
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
4. **General Tag Expressions in Phase 1**: The first phase does not introduce a general
   expression language. Policy selection only supports a simple `selector` on the
   policy-to-tag relation.
5. **Full Explainability UI**: A dedicated UI is not required for the first milestone. Policy-to-tag
   relations remain inspectable through the tag and policy association APIs.
6. **Tag Assignment Value Storage**: Assignment-level tag value storage, allowed values, search, and
   update semantics are covered by `design-docs/tag-assignment-values.md`. This design consumes the
   resulting effective tag assignments but does not redefine their storage semantics.
7. **Engine-Specific Enforcement Plugins**: This design defines policy selection inside Gravitino.
   Trino, Spark, Iceberg, or OPA enforcement integrations are separate designs.

---

## Solution Investigations

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| Direct object policy | Simple to understand for one object; current implementation already exists | Does not scale well; duplicates object-side tag and policy management; does not align classification with action | Rejected |
| Controls embedded in tags | Simplest user model; objects only receive tags | Tags become heavy governance objects; policy lifecycle, reuse, audit, enable/disable, and versioning are weaker; does not match the requirement to keep Tag and Policy concepts clear | Rejected |
| Policy-on-tag | Keeps policies reusable and auditable; makes tags the only object-side attachment point; supports tag presence and simple tag value matching without an expression engine | Requires a new relation table, tag value validation, resolver, APIs, and breaking migration away from direct object policy | **Chosen** |
| Nested tags | Can model classification hierarchy directly in tag objects | Adds a second hierarchy beside metadata object hierarchy; complicates policy resolution, authorization, migration, and explainability | Rejected |

Policy-on-tag is the best next step because it is useful as a standalone model and supports both tag
presence and simple tag value matching. The consumer path can stay stable:

```text
Consumer -> ObjectPolicyResolver -> object policies
```

### Policy Match Key

| Approach | Example | Trade-off | Decision |
|----------|---------|-----------|----------|
| Policy on tag definition only | `policyA -> data_domain`; applies when an object has effective tag `data_domain` with any value | Simple, but cannot distinguish `data_domain=finance` from `data_domain=risk` | Rejected |
| Policy on tag with selector | `policyA -> data_domain` with selector `{"type": "TAG_VALUE", "value": "finance"}` | Supports tag presence and simple valued-tag matching without an expression language | **Chosen for phase 1** |

Phase 1 therefore makes an explicit behavior decision: a policy-to-tag relation is keyed by a tag
definition and carries one optional `selector`. If `selector` is omitted or null, the relation
matches by tag presence. If `selector.type` is `TAG_VALUE`, the relation matches only when the
object's effective tag assignment contains `selector.value`.

---

## Proposal

### Target Model

The target model has four concepts:

| Concept | Description |
|---------|-------------|
| Policy | A metalake-scoped governance rule with typed content, enabled state, audit information, and version history. |
| Tag | A flat metalake-scoped classification object associated with metadata objects. Tags do not have parent or child tags. |
| Policy-tag relation | A relation that binds one policy to one tag in the same metalake, optionally scoped by a simple selector. |
| Object policy | A read-only policy result for a metadata object, derived from effective tags and matching policy-tag relations. |

Object-side governance becomes:

```text
Metadata Object -> Effective Tags -> Matching Policy-Tag Relations -> Object Policies
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
4. Effective tags include assignment values from the winning assignment, following
   `design-docs/tag-assignment-values.md`. A tag may therefore have zero, one, or multiple values.
5. Policy-on-tag matches by effective tag definition and selector.
6. The effective tag set is the de-duplicated result of walking from the object to its ancestors.
7. Policies bound to effective tags become object policy candidates only when their `selector`
   matches the effective tag assignment.

For example:

```text
policy retention_finance
  policyType: custom_retention

tag data_domain
  policies:
    - retention_finance with selector TAG_VALUE("finance")

catalog iceberg
  direct tags: [data_domain = ["finance"]]

table iceberg.db.orders
  direct tags: []
  inherited tags: [data_domain = ["finance"]]
  effective tags: [data_domain = ["finance"]]
  object policies: [retention_finance]
```

If the table has a direct assignment `data_domain = ["risk", "ml"]`, the direct assignment wins.
The `retention_finance` policy is not selected because `TAG_VALUE("finance")` does not match the
effective values `["risk", "ml"]`. A policy-to-tag relation without a selector would match
either assignment by tag presence.

### Policy Supported Object Types

`PolicyContent.supportedObjectTypes()` is deprecated in the policy-on-tag model. Its current
purpose is to restrict the metadata object types to which a policy can be directly attached. The
target model associates policies with tags instead of metadata objects, so relation creation no
longer has a metadata object type to validate.

Object policy resolution therefore does not filter policies by `supportedObjectTypes()`. It returns
enabled policies associated with the effective tags. Type-specific consumers decide whether and how
to consume a policy type. For example, TMS requests object policies for tables and consumes only the
system iceberg compaction policy type. The deprecated field should be removed from the public API in
a compatible release according to the project's API evolution policy.

### Policy Mutability

Policy-on-tag does not change the existing policy lifecycle or mutation APIs. Users can continue to
create, alter, enable, disable, delete, and view policy objects according to authorization rules.
Existing operations such as `PolicyOperations.alterPolicy()` and `PolicyChange.updateContent()`
remain supported. Only object policies, which are derived from effective tags, are read-only.

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

1. update the policy object through metalake-scoped policy lifecycle APIs;
2. enable or disable the policy object through metalake-scoped policy lifecycle APIs;
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
  selector (JSON)
  audit_info
  current_version
  last_version
  deleted_at
```

Constraints and indexes:

1. Active rows are unique by `(policy_id, tag_id)`. A policy can be associated with a tag at most
   once, and updating `selector` updates that relation.
2. Index `tag_id` for object policy lookup from tags.
3. Index `policy_id` for impact analysis from policies.
4. Policy and tag must belong to the same metalake.
5. `selector` is the only selector-related storage column and stores a JSON object, such as
   `{"type": "TAG_VALUE", "value": "finance"}`. The schema must not add separate
   `selector_type` or `selector_value` columns. The JSON is canonicalized before storage and
   comparison.
6. The table follows the same soft-delete and version fields as existing relation tables.

`selector` rules:

1. An omitted or null `selector` matches the tag by presence.
2. `TAG_VALUE` matches when the effective tag assignment contains the same value as
   `selector.value`.
3. `TAG_VALUE` contains one non-blank `value` string.
4. If the tag defines allowed values, the selector value must be one of the allowed values.
5. Selector JSON is canonicalized before storage and comparison.
6. The first version supports only `TAG_VALUE`. It does not support value absence,
   negative matching, principals, scopes, or general expressions. Future versions can add new
   selector types in the same `selector` JSON field without changing the policy-to-tag relation
   model.

### Selector Evolution Examples

`TAG_VALUE` remains the basic selector for exact value matching. Future selector versions can make
the current tag-presence behavior explicit through `ALL_VALUES` and use `EXPRESSION` for conditions
that cannot be represented by one tag value.

```json
{
  "type": "ALL_VALUES"
}
```

`ALL_VALUES` matches whenever the effective tag assignment exists. It accepts a valueless
assignment, one value, or multiple values; it does not require the assignment to contain every
allowed value.

The basic exact-value form remains:

```json
{
  "type": "TAG_VALUE",
  "value": "pii"
}
```

A future expression selector can combine values from multiple effective tags:

```json
{
  "type": "EXPRESSION",
  "expression": {
    "operator": "ALL_OF",
    "conditions": [
      {
        "attribute": "effectiveTags.classification.values",
        "operator": "CONTAINS",
        "value": "pii"
      },
      {
        "attribute": "effectiveTags.data_domain.values",
        "operator": "CONTAINS",
        "value": "finance"
      }
    ]
  }
}
```

This expression matches effective tag assignments `classification = ["pii"]` and
`data_domain = ["finance"]`. It does not match if either tag is absent or its effective assignment
does not contain the required value. These examples describe selector shapes only. Phase 1 supports
only `TAG_VALUE`; the expression language and cross-tag lookup contract require a separate design.

### ABAC Column Masking Example

A future tag-based ABAC consumer can use a resolved object policy to mask a sensitive column. For
example:

```text
policy mask_finance_pii
  policyType: column_mask
  associated tag: classification
  selector: classification contains "pii" AND data_domain contains "finance"
  mask behavior: FULL_REDACT

column iceberg.sales.customers.email
  effective tags:
    classification = ["pii"]
    data_domain = ["finance"]
  value before enforcement: alice@example.com
```

The column's effective tags satisfy the policy selector, so object policy resolution includes
`mask_finance_pii`. A column-mask enforcement consumer then applies the policy:

```text
ObjectPolicyResolver(iceberg.sales.customers.email)
  -> mask_finance_pii
  -> column-mask enforcement
  -> value after enforcement: ****
```

The policy type name, mask behavior, and masked value are illustrative. This design covers policy
selection but does not define the column-mask policy content contract or engine-specific
enforcement behavior.

### REST API Changes

#### New: `GET /api/metalakes/{metalake}/tags/{tag}/policies`

**Request:** No body.

| Query parameter | Type | Required | Description |
|-----------------|------|----------|-------------|
| `details` | boolean | no | If true, return policy association details including selectors. If false, return policy names. |

**Response:** `200 OK`

```json
{
  "names": ["retention_finance"]
}
```

With `details=true`:

```json
{
  "associations": [
    {
      "policy": {
        "name": "retention_finance",
        "policyType": "custom_retention",
        "enabled": true,
        "content": {}
      },
      "selector": {
        "type": "TAG_VALUE",
        "value": "finance"
      }
    }
  ]
}
```

**Behavior:** Lists policies directly associated with the tag. Without `details=true`, `names`
returns policy names. With `details=true`, the response returns one association entry for each
policy-to-tag relation. The caller must have `VIEW_TAG` on the requested tag. Returned policies are
filtered by `VIEW_POLICY`; `APPLY_TAG` and `APPLY_POLICY` imply their corresponding view privileges.
Returns `404 Not Found` if the tag does not exist.

#### New: `PUT /api/metalakes/{metalake}/tags/{tag}/policies/{policy}`

**Request:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `selector` | object or null | no | Tag selector. Missing or null matches by tag presence. |

```json
{
  "selector": {
    "type": "TAG_VALUE",
    "value": "finance"
  }
}
```

**Response:** `200 OK`

```json
{
  "policy": "retention_finance",
  "tag": "data_domain",
  "selector": {
    "type": "TAG_VALUE",
    "value": "finance"
  }
}
```

**Behavior:** Creates one policy-to-tag relation or replaces the selector on the existing relation.
The policy and tag must exist in the same metalake, and the selector must be valid for the tag.
Repeating the same request is an idempotent no-op.

#### New: `DELETE /api/metalakes/{metalake}/tags/{tag}/policies/{policy}`

**Request:** No body.

**Response:** `204 No Content`

**Behavior:** Removes one policy-to-tag relation. Removing a missing relation is an idempotent
no-op. Returns `404 Not Found` if the policy or tag does not exist.

#### New: `GET /api/metalakes/{metalake}/policies/{policy}/tags`

**Request:** No body.

| Query parameter | Type | Required | Description |
|-----------------|------|----------|-------------|
| `details` | boolean | no | If true, return tag association details including selectors. If false, return tag names. |

**Response:** `200 OK`

```json
{
  "names": ["data_domain"]
}
```

With `details=true`:

```json
{
  "associations": [
    {
      "tag": {
        "name": "data_domain"
      },
      "selector": {
        "type": "TAG_VALUE",
        "value": "finance"
      }
    }
  ]
}
```

**Behavior:** Lists tags that carry the policy. Without `details=true`, `names` returns tag names.
With `details=true`, the response returns one association entry for each policy-to-tag relation.
This is used for impact analysis before altering, disabling, or deleting a policy. Returns
`404 Not Found` if the policy does not exist. The caller must have `VIEW_POLICY` on the requested
policy. Returned tags are filtered by `VIEW_TAG`; `APPLY_POLICY` and `APPLY_TAG` imply their
corresponding view privileges.

#### Changed: `GET /api/metalakes/{metalake}/objects/{type}/{fullName}/policies`

**Request:** No body.

| Query parameter | Type | Required | Description |
|-----------------|------|----------|-------------|
| `details` | boolean | no | If true, return full visible policy objects. If false, return visible policy names. |

**Response with `details=false`:** `200 OK`

```json
{
  "names": ["iceberg_compaction_standard"]
}
```

**Response with `details=true`:** `200 OK`

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

**Behavior:** Resolves object policies for one metadata object from its effective tags. Returns
`404 Not Found` if the metadata object does not exist. This endpoint is read-only and does not
modify policy objects or object-policy relationships. The caller must be authorized to access the
metadata object. Resolved policies for which the caller lacks `VIEW_POLICY` are filtered from both
response shapes. `APPLY_POLICY` implies `VIEW_POLICY` for backward compatibility.

A single policy can be associated with multiple effective tags on the same object. The resolver
deduplicates by policy entity and current version, not by equivalent policy content. Different
policy entities with equivalent content remain distinct policies.

#### Changed: Direct object policy APIs

```http
GET  /api/metalakes/{metalake}/objects/{type}/{fullName}/policies
GET  /api/metalakes/{metalake}/objects/{type}/{fullName}/policies/{policy}
POST /api/metalakes/{metalake}/objects/{type}/{fullName}/policies
```

**Old behavior:** These APIs list, get, or associate policies directly on metadata objects. The
current list API loads policies directly associated with the requested metadata object and its
parent metadata objects, marks parent results as inherited, returns policy names by default, and
returns policy details when `details=true`. The current get API first checks the policy relation on
the requested metadata object; if no direct relation exists, it searches parent metadata objects and
marks a parent result as inherited. The current post API atomically adds and removes direct policy
relations on the requested metadata object using `policiesToAdd` and `policiesToRemove`.

**New behavior:** `GET /api/metalakes/{metalake}/objects/{type}/{fullName}/policies` becomes a
read-only derived object policy lookup API. It does not read direct policy relations and does not
modify policy objects or object-policy relationships.

Direct object policy mutation and direct object-policy-detail APIs are removed from the target
model:

```http
GET  /api/metalakes/{metalake}/objects/{type}/{fullName}/policies/{policy}
POST /api/metalakes/{metalake}/objects/{type}/{fullName}/policies
```

**Migration impact:** Callers must use policy-to-tag association APIs and read object policies from
`GET /api/metalakes/{metalake}/objects/{type}/{fullName}/policies`. Object-side policy association
calls must be replaced with tag assignment calls.

### Client API Changes

| Area | Old API | New API |
|------|---------|---------|
| Object policy association | `SupportsPolicies.associatePolicies(String[] add, String[] remove)` | Removed from metadata object mixins in the target model |
| Object policy listing | `SupportsPolicies.listPolicies()` | Reinterpreted as read-only derived object policy lookup |
| Tag policy association | None | New single-relation APIs such as setPolicyForTag(tagName, policyName, selector) and removePolicyFromTag(tagName, policyName) |
| Policy impact analysis | `Policy.associatedObjects()` | Replaced or supplemented by Policy.associatedTags() |

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
2. Load effective tags for the object using current inheritance semantics, including assignment
   values from the winning direct or inherited tag assignment.
3. Load policy-tag relations associated with those effective tags in batch.
4. Evaluate each relation's `selector` against the effective tag assignment and drop
   non-matching relations.
5. Load matched policies and drop disabled policies.
6. Deduplicate policies by policy entity and current version.
7. Return the object policies.

### Authorization, Response Visibility, and Audit

Policy-on-tag makes tags part of the governance control plane. Assigning a high-impact tag can
change security, maintenance, or retention behavior.

This design introduces explicit `VIEW_POLICY` and `VIEW_TAG` privileges so reading governance
metadata is separate from applying it.

| Privilege | Bindable scopes | Grants |
|-----------|-----------------|--------|
| `VIEW_POLICY` | Metalake and policy | List a visible policy, get its metadata and content, and inspect its visible tag associations. |
| `VIEW_TAG` | Metalake and tag | List a visible tag, get its metadata and values, and inspect its visible policy or object associations. |

The new privileges follow the existing allow and deny model. A grant on a metalake applies to all
policies or tags in that metalake. A grant on one policy or tag applies only to that entity. Metalake
owners and entity owners retain view access.

For backward compatibility, an effective `APPLY_POLICY` grant implies `VIEW_POLICY`, and an
effective `APPLY_TAG` grant implies `VIEW_TAG`. Existing roles with apply privileges therefore keep
their current read access without a grant migration. A corresponding explicit deny of the view
privilege blocks response visibility but does not change server-side policy enforcement.

Recommended authorization rules:

1. Listing or getting policies requires `VIEW_POLICY`; listing or getting tags requires `VIEW_TAG`.
   List APIs filter unauthorized entities, while a get API for one named entity rejects an
   unauthorized request.
2. Creating and altering policies keeps existing policy privileges. Creating and altering tags
   keeps existing tag privileges.
3. Creating, updating, or deleting a policy-to-tag relation requires both policy-side and tag-side
   permission. A caller must be metalake owner, or must have `APPLY_POLICY` on the target policy
   and `APPLY_TAG` on the target tag.
4. `VIEW_POLICY` and `VIEW_TAG` are read-only. Neither privilege authorizes policy-to-tag
   association, tag-to-object assignment, policy mutation, or tag mutation.
5. Tag assignment permission alone is not enough to attach a policy to a tag. Policy permission
   alone is not enough to bind the policy to arbitrary tags.
6. Assigning a tag to a metadata object continues to require `APPLY_TAG` and access to the metadata
   object.
7. Object policy lookup requires access to the metadata object. Returned policies are filtered by
   `VIEW_POLICY` after resolution.
8. Policy enforcement must be independent from response visibility. The trusted server-side
   enforcement path must resolve all applicable policies even when the end user cannot read policy
   details.

Object policy response visibility is normative:

| Caller authorization | `details=false` response | `details=true` response |
|----------------------|--------------------------|-------------------------|
| No object access | `403 Forbidden` | `403 Forbidden` |
| Object access without `VIEW_POLICY` | `200 OK` with unauthorized policies filtered from `names` | `200 OK` with unauthorized policies filtered from `policies` |
| Object access with `VIEW_POLICY` | Visible policy names in `names` | Full visible policy fields including `content` |

Object policy responses do not expose matching tags or selectors, so tag read authorization does not
affect their shape. The trusted enforcement path consumes the unfiltered resolver result; REST
filtering never disables or bypasses an applicable policy.

Audit events should cover policy lifecycle changes, policy-to-tag relation changes, and tag-to-object
assignment changes.

### Event Listener

Policy-to-tag relation creation, selector update, and deletion must be exposed through the event
listener framework, matching the existing policy and tag lifecycle event pattern. Each operation
has pre-event, success-event, and failure-event types. Event payloads include the metalake, tag
name, policy name, previous and requested selector as applicable, actor, and request context. A
successful PUT event includes the resulting relation, and a successful DELETE event includes the
removed relation.

Object policy lookup is a read-only derived operation and does not create policy-relation events. It
may still be covered by normal REST access logs or audit logs if the project records read events.

### Policy Conflict Boundary

`ObjectPolicyResolver` is responsible for selection, disabled-policy filtering, and policy
deduplication. It does not make allow or deny decisions for row filters, column masks, or other
engine-enforced policy types.

The resolver must also detect mixed selector results for the same policy. This conflict exists when
one metadata object has multiple effective tags associated with the same policy and evaluation of
those policy-to-tag relation selectors produces at least one match and at least one non-match.

For example:

```text
mask_policy -> classification with TAG_VALUE("pii")
mask_policy -> data_domain with TAG_VALUE("finance")

object effective tags:
  classification = ["pii"]     -> match
  data_domain = ["risk"]       -> not match
```

For this object, `mask_policy` is both selected and not selected through different effective tag
relations, so the resolver detects a selector conflict. Relations for tags that are not in the
object's effective tag set do not participate in this detection. This design identifies the
conflict but does not define a precedence or merge rule for it.

For row filter and column mask policies, enforcement consumers must fail closed if multiple
distinct effective policies of the same kind apply to the same evaluation target. The system should
not merge them, union them, or choose one by priority in this design. Multiple matching relations
that reach the same policy entity are not a conflict; the resolver deduplicates them into one
returned policy.

When the first row-filter or column-mask consumer is implemented, the conflict check should be added
as a shared helper instead of being copied into each engine integration.

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
3. A system iceberg compaction policy can omit `selector` for tag-presence behavior or use a
   simple `TAG_VALUE` selector when maintenance behavior should depend on assignment values.
4. TMS does not read direct object policy relations.

### User Process

1. A governance administrator creates a policy, such as `retention_finance`.
2. The administrator creates or reuses a tag, such as `data_domain`.
3. The administrator associates the policy with the tag and a selector, such as
   `TAG_VALUE("finance")`.
4. A data owner assigns the tag and value to a catalog, schema, table, or column.
5. Gravitino resolves object policies from the object's effective tags.
6. TMS or another consumer reads object policies through a read-only lookup API.
7. To change the object policy result, the administrator updates the policy, policy-to-tag relation,
   tag assignment, or policy enabled state.

### Implementation Process

```text
Client
  -> REST Server
  -> PolicyTagOperations / ObjectPolicyOperations
  -> PolicyDispatcher / TagDispatcher
  -> ObjectPolicyResolver
  -> EffectiveTagResolver
  -> EntityStore relation operations
  -> policy_tag_relation_meta, tag_relation_meta, policy_meta
```

Object policy lookup data flow:

```text
Metadata Object
  -> parent metadata object hierarchy
  -> effective tags with assignment values
  -> policy-tag relations
  -> selector filtering
  -> policy metadata
  -> enabled policy filtering
  -> policy deduplication
  -> object policy response
```

Effective tag resolution should be extracted into a shared core component instead of being
reimplemented in each consumer. The object tag listing API, object policy resolver, tag assignment
value support, and tag-based authorization should use the same resolver so they agree on inherited
tag handling, direct-assignment override behavior, value grouping, and deterministic ordering.

### Migration Process

This design is a breaking model change. The target runtime does not read direct object policy
relations.

1. Inventory existing direct policy relations from `policy_relation_meta`, including inherited
   relations that users depend on through parent metadata objects.
2. Export a mapping from each direct policy relation to an administrator-approved tag. Existing tags
   may be reused, or new tags may be created only for migration.
3. For valued tags, decide the assignment values that should be written to each target object and
   the selector that should be attached to each policy-to-tag relation.
4. Create missing tags and configure allowed values if the tag assignment value design is enabled.
5. Associate policies with tags and selectors using the new tag-scoped policy association API.
6. Assign those tags, with values where needed, to the target metadata objects or their ancestors.
7. Run an object policy verification command that compares expected objects with resolved object
   policies.
8. Upgrade consumers such as TMS to read only object policies.
9. Stop writing direct object policy relations, then retire the direct relation table and direct
   association APIs after the compatibility window.

Example migration mapping:

```yaml
policyTagMappings:
  retention_finance:
    tags:
      - name: data_domain
        selector:
          type: TAG_VALUE
          value: finance
objectTagMappings:
  CATALOG:iceberg:
    tags:
      - name: data_domain
        values:
          - finance
```

---

## Task Breakdown

- [ ] Add `policy_tag_relation_meta` storage schema and entity store operations, including
      `selector` JSON storage.
- [ ] Add core API and DTO contracts for policy-to-tag association, selectors, and object
      policy responses.
- [ ] Deprecate `PolicyContent.supportedObjectTypes()`, stop using it during object policy
      resolution, and plan its removal according to the API evolution policy.
- [ ] Implement `ObjectPolicyResolver` to resolve object policies from effective tags and value
      selectors.
- [ ] Add single-relation PUT and DELETE endpoints for tag policy association, plus policy tag
      listing with selector payloads.
- [ ] Change object policy REST APIs to read-only derived lookup and remove direct object policy
      mutation behavior.
- [ ] Add Java client and Python client support for policy-to-tag association and derived object
      policy lookup.
- [ ] Add `VIEW_POLICY` and `VIEW_TAG` privileges for metalake and entity scopes, including
      backward-compatible view implication from `APPLY_POLICY` and `APPLY_TAG`.
- [ ] Apply view authorization to policy, tag, association, and object-policy APIs, including list
      filtering and named-entity rejection behavior.
- [ ] Add a shared fail-closed conflict helper for row filter or column mask enforcement consumers.
- [ ] Integrate TMS with `ObjectPolicyResolver` instead of direct object policy relations.
- [ ] Add migration tooling or documentation for moving direct object policy relations to
      policy-to-tag relations and tag assignments.
- [ ] Update OpenAPI specifications in `docs/open-api/*.yaml` and validate them with
      `./gradlew :docs:build`.
- [ ] Add unit and integration tests for storage, resolver, authorization, REST APIs, and TMS
      integration.
