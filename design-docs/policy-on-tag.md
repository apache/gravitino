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
8. **Selector Evolution Path**: The resolver boundary can later support richer tag selectors
   without changing every policy consumer.
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
   expression language. Policy selection uses an explicit tag value selector on the policy-to-tag
   relation.
5. **Full Explainability UI**: A dedicated UI is not required for the first milestone. APIs must
   still return enough source information to trace object policy sources.
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
| Policy-on-tag | Keeps policies reusable and auditable; makes tags the only object-side attachment point; supports tag presence and tag value matching without an expression engine | Requires a new relation table, selector validation, resolver, APIs, and breaking migration away from direct object policy | **Chosen** |
| Nested tags | Can model classification hierarchy directly in tag objects | Adds a second hierarchy beside metadata object hierarchy; complicates policy resolution, authorization, migration, and explainability | Rejected |
| General tag expression selector | Most expressive; supports complex conditions over tag names, tag values, and scopes | Requires expression language, matching engine, and more complex UX; too large for the next milestone | Future |

Policy-on-tag is the best next step because it is useful as a standalone model and supports both tag
presence and tag value matching. The consumer path can stay stable:

```text
Consumer -> ObjectPolicyResolver -> object policies
```

Only the policy selection layer needs to evolve later if Gravitino adds richer selector types.

### Policy Match Key

| Approach | Example | Trade-off | Decision |
|----------|---------|-----------|----------|
| Policy on tag definition only | `policyA -> data_domain`; applies when an object has effective tag `data_domain` with any value | Simple, but cannot distinguish `data_domain=finance` from `data_domain=risk` | Rejected |
| Policy on tag value selector | `policyA -> data_domain` with selector `VALUE_IN ["finance"]` | Supports valued tags without a general expression language; keeps matching local to one tag relation | **Chosen for phase 1** |
| General tag expression selector | `hasTag("pii") && tagValue("data_domain") contains "finance"` | More expressive, but requires expression parsing and validation rules | Future |

Phase 1 therefore makes an explicit behavior decision: a policy-to-tag relation is keyed by a tag
definition and carries a tag value selector. The selector decides whether the relation matches the
object's effective tag assignment. A selector can match any value for the tag, a configured set of
values, or a valueless assignment.

---

## Proposal

### Target Model

The target model has four concepts:

| Concept | Description |
|---------|-------------|
| Policy | A metalake-scoped governance rule with typed content, enabled state, audit information, and version history. |
| Tag | A flat metalake-scoped classification object associated with metadata objects. Tags do not have parent or child tags. |
| Policy-tag relation | A relation that binds one policy to one tag in the same metalake, with a tag value selector. |
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
5. Policy-on-tag matches by effective tag definition and tag value selector.
6. The effective tag set is the de-duplicated result of walking from the object to its ancestors.
7. Policies bound to effective tags become object policy candidates only when their selector matches
   the effective tag assignment.

For example:

```text
policy retention_finance
  policyType: custom_retention

tag data_domain
  policies:
    - retention_finance with selector VALUE_IN ["finance"]

catalog iceberg
  direct tags: [data_domain = ["finance"]]

table iceberg.db.orders
  direct tags: []
  inherited tags: [data_domain = ["finance"]]
  effective tags: [data_domain = ["finance"]]
  object policies: [retention_finance]
  policy source: tag data_domain, matched values ["finance"], assigned on CATALOG iceberg
```

If the table has a direct assignment `data_domain = ["risk", "ml"]`, the direct assignment wins.
The `retention_finance` policy is not selected because selector `VALUE_IN ["finance"]` does not
match the effective values `["risk", "ml"]`. A selector of `ANY` would match either assignment.

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
  tag_value_selector
  audit_info
  current_version
  last_version
  deleted_at
```

Constraints and indexes:

1. Active rows are unique by `(policy_id, tag_id, tag_value_selector)`.
2. Index `tag_id` for object policy lookup from tags.
3. Index `policy_id` for impact analysis from policies.
4. Policy and tag must belong to the same metalake.
5. `tag_value_selector` is a normalized JSON selector stored on the relation.
6. The table follows the same soft-delete and version fields as existing relation tables.

Tag value selector shape:

| Selector | Meaning | Match rule |
|----------|---------|------------|
| `ANY` | Match the tag by presence only | The effective tag assignment exists, with or without values. |
| `VALUE_IN` | Match one or more configured values | At least one effective tag assignment value is in the selector's `values`. |
| `VALUE_ABSENT` | Match a valueless tag assignment | The effective tag assignment exists and has no values. |

`ANY` is the default selector when clients omit the selector. `VALUE_IN` selectors must contain at
least one non-blank value. Each value must satisfy the tag's allowed value constraint when the tag
defines allowed values.

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
      "valueSelector": {
        "type": "VALUE_IN",
        "values": ["finance"]
      }
    }
  ]
}
```

**Behavior:** Lists policies directly associated with the tag. Without `details=true`, `names`
returns distinct policy names, so a policy associated through multiple selectors appears once. With
`details=true`, the response returns one association entry for each `(policy, selector)` relation.
Returns `404 Not Found` if the tag does not exist.

#### New: `POST /api/metalakes/{metalake}/tags/{tag}/policies`

**Request:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `policiesToAdd` | array of `PolicyTagAssociationRequest` | no | Policies and selectors to associate with the tag. |
| `policiesToRemove` | array of `PolicyTagAssociationRequest` | no | Policies and selectors to disassociate from the tag. |

`PolicyTagAssociationRequest`:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Policy name. |
| `valueSelector` | object | no | Tag value selector for this policy-tag relation. Missing means `ANY`. |

```json
{
  "policiesToAdd": [
    {
      "name": "retention_finance",
      "valueSelector": {
        "type": "VALUE_IN",
        "values": ["finance"]
      }
    }
  ],
  "policiesToRemove": [
    {
      "name": "iceberg_compaction_legacy",
      "valueSelector": {
        "type": "ANY"
      }
    }
  ]
}
```

**Response:** `200 OK`

```json
{
  "names": ["retention_finance"]
}
```

**Behavior:** Atomically updates policy associations for one tag. The request supports adding and
removing multiple policies so callers can change the complete relation set without issuing one
request per policy or exposing an intermediate partial state. The tag and all added policies must
exist in the same metalake. The selector must be valid for the tag. Adding an already associated
`(policy, selector)` pair is an idempotent no-op. Removing a missing pair is also an idempotent
no-op. The same `(policy, selector)` pair cannot appear in both arrays in one request; violations
return `400 Bad Request`. The same policy may be associated with the same tag through different
selectors when the selectors are distinct.

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
      "valueSelector": {
        "type": "VALUE_IN",
        "values": ["finance"]
      }
    }
  ]
}
```

**Behavior:** Lists tags that carry the policy. Without `details=true`, `names` returns distinct tag
names. With `details=true`, the response returns one association entry for each `(tag, selector)`
relation. This is used for impact analysis before altering, disabling, or deleting a policy. Returns
`404 Not Found` if the policy does not exist.

#### Changed: `GET /api/metalakes/{metalake}/objects/{type}/{fullName}/policies`

**Request:** No body.

| Query parameter | Type | Required | Description |
|-----------------|------|----------|-------------|
| `details` | boolean | no | If true, return policy content and source information according to the caller's policy and tag read authorization. If false, return policy names. |

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
      "content": {},
      "sources": [
        {
          "tagName": "maintenance_standard",
          "tagValues": [],
          "matchedValues": [],
          "valueSelector": {
            "type": "ANY"
          },
          "inherited": true,
          "objectType": "CATALOG",
          "objectName": "iceberg"
        }
      ]
    }
  ]
}
```

**Behavior:** Resolves object policies for one metadata object from its effective tags. Returns
`404 Not Found` if the metadata object does not exist. This endpoint is read-only and does not
modify policy objects or object-policy relationships.

A single policy can be associated with multiple effective tags on the same object. The resolver
deduplicates by policy entity and current version, not by equivalent policy content. Different
policy entities with equivalent content remain distinct policies. For each returned policy,
`sources` contains every effective tag and selector through which that policy was reached. For
`VALUE_IN`, `matchedValues` contains the effective values that matched the selector. For `ANY` and
`VALUE_ABSENT`, `matchedValues` is empty. Sources are ordered by `tagName`, normalized selector,
`objectType`, then `objectName` to keep responses deterministic.

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
| Tag policy association | None | New tag-scoped API such as associatePoliciesForTag(tagName, add, remove) |
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
4. Evaluate each relation's tag value selector against the effective tag assignment and drop
   non-matching relations.
5. Load matched policies and drop disabled policies.
6. Deduplicate policies by policy entity and current version, preserving all contributing matched
   sources.
7. Return the object policies and their source tag information.

### Authorization, Response Visibility, and Audit

Policy-on-tag makes tags part of the governance control plane. Assigning a high-impact tag can
change security, maintenance, or retention behavior.

This design does not introduce new `VIEW_TAG` or `VIEW_POLICY` privileges. It uses the existing tag
and policy authorization model. In the rules below, "tag read authorization" means the caller is
allowed to read tag details under the current authorization implementation, such as by ownership or
`APPLY_TAG`. "Policy read authorization" means the caller is allowed to read policy details, such as
by ownership or `APPLY_POLICY`.

Recommended authorization rules:

1. Creating and altering policies keeps existing policy privileges.
2. Creating and altering tags keeps existing tag privileges.
3. Associating a policy with a tag and value selector requires both policy-side and tag-side
   permission. A caller must be metalake owner, or must have authorization equivalent to
   `APPLY_POLICY` on the policy and `APPLY_TAG` on the tag.
4. Tag assignment permission alone is not enough to attach a policy to a tag. Policy permission
   alone is not enough to bind the policy to arbitrary tags.
5. Assigning a tag to a metadata object continues to require tag-application permission and access
   to the metadata object.
6. Object policy lookup requires access to the metadata object.
7. Policy enforcement must be independent from response visibility. The trusted server-side
   enforcement path must resolve all applicable policies even when the end user cannot read policy
   or tag details.

Object policy response visibility is normative:

| Caller authorization | `details=false` response | `details=true` response |
|----------------------|--------------------------|-------------------------|
| Object access only | Policy names in `names`; no content or sources | Redacted `policies` entries with `name`, `policyType`, `enabled`, and `enforced=true`; no `content`; no `sources` |
| Object access + policy read authorization | Policy names in `names` | Full policy fields including `content`; no `sources` unless tag read authorization is also present |
| Object access + tag read authorization | Policy names in `names` | Redacted policy fields with `sources`; no `content` unless policy read authorization is also present |
| Object access + policy and tag read authorization | Policy names in `names` | Full policy fields including `content` and `sources` |

Audit events should cover policy lifecycle changes, policy-to-tag relation changes, and tag-to-object
assignment changes.

### Event Listener

Policy-to-tag association must be exposed through the event listener framework, matching the
existing policy and tag lifecycle event pattern. The implementation should add pre-event,
success-event, and failure-event types for associating policies with a tag. Event payloads should
include the metalake, tag name, policy-selector pairs to add, policy-selector pairs to remove,
actor, request context, and the final associated policy-selector pairs on success.

Object policy lookup is a read-only derived operation and does not create policy-relation events. It
may still be covered by normal REST access logs or audit logs if the project records read events.

### Policy Conflict Boundary

`ObjectPolicyResolver` is responsible for selection, disabled-policy filtering, policy deduplication,
and provenance. It does not make allow or deny decisions for row filters, column masks, or other
engine-enforced policy types.

For row filter and column mask policies, enforcement consumers must fail closed if multiple distinct
effective policies of the same kind apply to the same evaluation target. The system should not merge
them, union them, or choose one by priority in this design. Multiple sources that reach the same
policy entity are not a conflict; they are represented as multiple `sources` entries on one returned
policy.

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
3. A system iceberg compaction policy can use selector `ANY` for tag-presence behavior or
   `VALUE_IN` when maintenance behavior should depend on an assignment value.
4. TMS does not read direct object policy relations.

### User Process

1. A governance administrator creates a policy, such as `retention_finance`.
2. The administrator creates or reuses a tag, such as `data_domain`.
3. The administrator associates the policy with the tag and a selector, such as
   `VALUE_IN ["finance"]`.
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
  -> tag value selector filtering
  -> policy metadata
  -> enabled policy filtering
  -> policy deduplication with all sources preserved
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
   the tag value selectors that should be attached to each policy-to-tag relation.
4. Create missing tags and configure allowed values if the tag assignment value design is enabled.
5. Associate policies with tags and selectors using the new tag-scoped policy association API.
6. Assign those tags, with values where needed, to the target metadata objects or their ancestors.
7. Run an object policy verification command that compares expected objects with resolved object
   policies and lists every policy source.
8. Upgrade consumers such as TMS to read only object policies.
9. Stop writing direct object policy relations, then retire the direct relation table and direct
   association APIs after the compatibility window.

Example migration mapping:

```yaml
policyTagMappings:
  retention_finance:
    tags:
      - name: data_domain
        valueSelector:
          type: VALUE_IN
          values:
            - finance
objectTagMappings:
  CATALOG:iceberg:
    tags:
      - name: data_domain
        values:
          - finance
```

---

## Task Breakdown

- [ ] Add `policy_tag_relation_meta` storage schema and entity store operations, including tag
      value selector storage.
- [ ] Add core API and DTO contracts for policy-to-tag association, tag value selectors, and object
      policy source information.
- [ ] Deprecate `PolicyContent.supportedObjectTypes()`, stop using it during object policy
      resolution, and plan its removal according to the API evolution policy.
- [ ] Implement `ObjectPolicyResolver` to resolve object policies from effective tags and tag value
      selectors.
- [ ] Add REST endpoints for tag policy association and policy tag listing with selector payloads.
- [ ] Change object policy REST APIs to read-only derived lookup and remove direct object policy
      mutation behavior.
- [ ] Add Java client and Python client support for policy-to-tag association and derived object
      policy lookup.
- [ ] Define object policy response redaction using the existing tag and policy authorization model.
- [ ] Add a shared fail-closed conflict helper for row filter or column mask enforcement consumers.
- [ ] Integrate TMS with `ObjectPolicyResolver` instead of direct object policy relations.
- [ ] Add migration tooling or documentation for moving direct object policy relations to
      policy-to-tag relations and tag assignments.
- [ ] Update OpenAPI specifications in `docs/open-api/*.yaml` and validate them with
      `./gradlew :docs:build`.
- [ ] Add unit and integration tests for storage, resolver, authorization, REST APIs, and TMS
      integration.
