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

# Design of Tag-Based Access Control in Gravitino

**Status:** draft for discussion. The [open questions](#open-questions) are deliberately left
undecided in this revision, each presented with its options; decisions will be folded in after
review.

Discussion: [#12619](https://github.com/apache/gravitino/discussions/12619)

---

## Summary

An access rule is a `Policy` of type `system_access_control` whose `content` carries an action and
a role condition. The policy is bound to a tag. Any object carrying that tag becomes subject to
the rule.

```json
POST /api/metalakes/prod/policies
{
  "name": "analyst_read_certified",
  "policyType": "system_access_control",
  "enabled": true,
  "content": {
    "action": "SELECT_TABLE",
    "role":   { "name": "analyst" }
  }
}
```

```
PUT  /api/metalakes/prod/tags/certified/policies/analyst_read_certified
     { "selector": { "type": "ALL_VALUES" } }

POST /api/metalakes/prod/objects/TABLE/lakehouse.finance.orders/tags
     { "tagsToAdd": [{ "name": "certified" }] }
```

Read together: *members of the role `analyst` may `SELECT` any table that carries the tag
`certified`.*

There is exactly one attachment — the policy-to-tag bind. The role condition is a value inside
`content`, not an association. No new user-facing entity, REST resource or client API is
introduced.

---

## Background

Gravitino authorizes metadata operations through RBAC. A grant names a securable object and a
privilege and binds them to a role; the authorization expression on each REST endpoint evaluates
those grants over the object's ancestor chain.

Tags are a separate subsystem. They apply to catalogs, schemas, tables, views, topics, filesets,
models, columns and functions, carry assignment values (see
[tag-assignment-values.md](tag-assignment-values.md)), and inherit down the object hierarchy.
Policy-on-tag ([policy-on-tag.md](policy-on-tag.md)) lets governance policies be selected by those
tags. Authorization does not read tags at all.

The consequence is that intent expressed as classification cannot drive access. An organization
that already labels tables `certified`, `pii` or `data_domain=finance` must still enumerate grants
object by object to act on those labels. New objects need new grants, dropped objects leave stale
ones, and the rule itself is written down nowhere — it exists only as the accumulated set of grants
someone remembered to issue.

---

## Scope

### In this version

- A rule of the form *(action, role condition)* bound to a tag.
- `ALLOW` only.
- Roles as the matched condition.
- Evaluation inside the existing authorization-expression path, composing with RBAC.
- Reuse of the `Policy` entity, the policy-to-tag relation and `PolicySelector`, so tag conditions
  are written identically for governance and for authorization.
- No new REST resource or client API. The only storage addition is an internal derived index, not
  written or read by any endpoint — see [Lifecycle](#lifecycle).

### Not in this version

| Excluded | Reason |
|---|---|
| `DENY` | Deny that cannot be suppressed by re-tagging a descendant is a separate problem. Allow-only keeps v1 evaluation total and order-independent. |
| Users and groups as the matched condition | The condition schema can gain them later without changing the model. |
| Row filtering and column masking | Distinct policy types; this design governs whole-object decisions. |
| Cross-tag conditions | A rule matches one tag. Conditions spanning several tags await the `EXPRESSION` selector type in [policy-on-tag.md](policy-on-tag.md). |
| Column-level decisions | A tag on a column does not affect decisions about its table. |
| A `scope` field in `content`, restricting a rule to a subtree | Not a security boundary: creating a policy needs metalake-wide `CREATE_POLICY`, so whoever writes the rule sets its reach. It is also not consulted when a tag is applied, so it filters effect rather than preventing a wrong tag. Tag assignment values with a value-sensitive selector cover the one-tag-different-subtrees case. Additive later — absent has always meant metalake-wide. |
| Replacing RBAC | Baseline privileges, ownership and traversal are unchanged. See [Composition with RBAC](#composition-with-rbac). |

---

## Alternatives considered

| Option | Pros | Cons | Status |
|---|---|---|---|
| **A `system_access_control` policy type bound to a tag** | Reuses the entity, relation, selector and resolver; no new REST or client surface; one governance model to learn | The role condition lives in `content` JSON, so lookup by role needs a derived index rather than a foreign key | **Proposed** |
| A dedicated `tag_access_policy` entity with action and role as columns | Foreign key on role; indexed lookup; cascade on role deletion falls out of the schema | New table across three dialects, new REST resource, new client and CLI surface, a second governance model alongside policies | Rejected |
| Extend RBAC grants with a tag predicate | No new concepts | The grant table is object-identified; a predicate has no object, and every grant read path would change | Rejected |
| Evaluate tags in an external engine (OPA and similar) | Arbitrary policy language | Moves the decision out of Gravitino, duplicates the tag hierarchy, and cannot use the existing expression path | Rejected |

The consequence of that one con — referential integrity maintained by the server rather than by the
schema — is addressed in [Lifecycle](#lifecycle).

---

## Model

### Content

`PolicyContent` is an interface, and each built-in policy type has a concrete implementation with
typed fields and a `validate()` that runs at write time. `IcebergDataCompactionContent` is the
existing example. `system_access_control` follows the same pattern with a new
`AccessControlContent`:

| Field | Type | Meaning |
|---|---|---|
| `action` | `Privilege.Name` | The privilege the rule confers. Must be one of the permitted names — see [Composition with RBAC](#composition-with-rbac) for the exclusions. |
| `role` | object with `name` | The **condition**. Matched against the caller's expanded roles. |

`validate()` rejects at creation rather than at evaluation:

- `action` parses to a permitted `Privilege.Name`.
- `role` name is non-blank.

Rejecting at write time matters because the alternative failure is silent: a policy naming an
action that does not parse simply grants nothing, and nothing surfaces until someone notices the
access they expected is missing.

Whether `validate()` also requires the named role to *exist* is part of
[OQ-3](#oq-3--deleting-a-referenced-role), not a separate decision.

### `role` is a condition, not a principal

The rule does not grant anything to `analyst`. It states that *if* the caller holds `analyst`
among their expanded roles *and* the object carries `certified`, then `SELECT_TABLE` is satisfied
for this request.

The distinction matters for two reasons. The rule is not a grant, so it does not appear in the
role's securable objects and does not participate in grant listing. And a role that is never
assigned to anyone confers nothing, exactly as an unassigned role does today.

### The tag bind

The policy is attached to the tag through the existing policy-to-tag relation and its selector,
exactly as governance policies are. `ALL_VALUES` in the example above matches the tag regardless of
assignment value; value-sensitive selectors work as they do for governance policies, and nothing in
this design is specific to `ALL_VALUES`.

---

## Evaluation

An authorization decision needs to know, for the object being accessed and the caller's roles,
whether any access rule is satisfied. That requires three things:

1. the tags effective at the object after nearest-wins resolution
   ([tag-assignment-values.md](tag-assignment-values.md)), including those inherited from ancestors;
2. the `system_access_control` policies bound to those tags;
3. for each, whether `role` is among the caller's expanded roles.

The question is *when* steps 1 and 2 happen. Two options, presented without preference.

### Option 1 — expand when roles load

When a role's policies are loaded into the authorizer, walk the tags reachable for that role and
materialise a policy row per (role, object), alongside the rows already produced from RBAC grants.

- Tag-derived permissions become indistinguishable from RBAC ones at decision time. The existing
  composition — including the deny path, which narrows across all of a caller's roles rather than
  only the active ones — applies unchanged, with no new expression.
- The decision itself costs nothing extra; the work moves to load time.
- Expansion runs over *inherited* tags, so the row set for one role depends on the tag state of
  every ancestor of every object it can reach. That set changes when a tag is applied anywhere in
  the hierarchy, not only when the role changes.
- A missed invalidation leaves rows in place that should have been withdrawn. The failure direction
  is permissive.

### Option 2 — evaluate as a second stage at request time

Leave the loaded rows as they are. When the RBAC decision does not already allow the request,
resolve the object's effective tags, load the access policies bound to them, and test them against
the caller's roles.

- Two additional lookups on the request path, both cacheable, for requests RBAC did not already
  allow.
- Composition must be written explicitly rather than inherited:

  ```
  (rbacAllow || tagAllow) && !rbacDeny
  ```

  which makes the interaction with deny a stated rule rather than a property of the row set. See
  [OQ-2](#oq-2--composition-when-a-tag-allows-and-rbac-denies).
- A stale cache yields a decision computed from older tag state. The failure direction depends on
  the cache design rather than on invalidation coverage.

### Freshness

Either option has to answer how a tag change becomes visible on a node that has already loaded the
affected roles.

Today the authorizer keeps role policies fresh by version-checking on read: `loadedRoles` maps role
id to `updated_at`, and a newer `role_meta.updated_at` in the database evicts and reloads that
role's policies. `groupRoleCache` is validated the same way against `group_meta.updated_at`. Write
paths additionally call `handleRolePrivilegeChange`, `handleUserRoleRelChange` and
`handleGroupRoleRelChange` in-process on the node that performed the write, and TTL bounds the rest.

`JcasbinChangeListener` covers two other surfaces: it receives entity changes through
`onEntityChange` and invalidates the metadata name-to-id cache, and it polls `owner_meta` for
ownership changes.

Neither a tag assignment nor a policy-to-tag bind touches `role_meta.updated_at`, and neither
appears on the two surfaces the listener covers. Whichever evaluation option is chosen, tag-derived
state needs a freshness signal of its own — a version column consulted on read, a new poll, or an
explicit TTL accepted as the bound.

See [OQ-1](#oq-1--where-tags-are-evaluated).

### List filtering

List endpoints filter their results through the same authorizer, so tag-derived permissions must be
visible to filtering as well as to point decisions. Resolving tags per candidate object would turn
one listing into N lookups. Both options need a batch preload of tag and policy state for the
candidate set, alongside the existing `preloadToCache` and `preloadOwner` paths.

---

## Composition with RBAC

### Traversal stays RBAC

`USE_CATALOG` and `USE_SCHEMA` are not conferrable by a tag. Reaching
`lakehouse.finance.orders` requires:

```
USE_CATALOG   on lakehouse            from RBAC
USE_SCHEMA    on lakehouse.finance    from RBAC
SELECT_TABLE  on the table            from RBAC or from a tag rule
```

Two reasons. Tagging a leaf would otherwise widen access to its containers as a side effect, which
is a larger grant than the tagging operation appears to be. And the owner of a schema would lose
the ability to decide who may enter it, because anyone able to apply a tag could confer entry.

**Tag-based access is additive within territory a role already has, not a way to hand out new
territory.** For `analyst` to read `lakehouse.finance.orders` through `certified`, the role must
already hold `USE_CATALOG` on `lakehouse` and `USE_SCHEMA` on `finance`. A tag applied to a table in
a schema the role cannot enter has no effect. `validate()` rejects both names in `action`.

### Allow and deny

With `ALLOW` only, two access rules cannot conflict — they union. The interaction that remains is
between a tag rule that allows and an RBAC grant that denies. See
[OQ-2](#oq-2--composition-when-a-tag-allows-and-rbac-denies).

---

## Administration

Four write paths can change who has access. Their current authority requirements:

| Operation | Expression today | Scoping available |
|---|---|---|
| Create a policy | `METALAKE::OWNER \|\| METALAKE::CREATE_POLICY` | metalake only |
| Create a tag | `METALAKE::OWNER \|\| METALAKE::CREATE_TAG` | metalake only |
| Bind a policy to a tag | tag-scoped | metalake, or the specific tag |
| Apply a tag to an object | `METALAKE::OWNER \|\| ((TAG::OWNER \|\| ANY_APPLY_TAG) && CAN_ACCESS_METADATA)` | the specific tag, and only objects the caller can already access |

Creating access policies and creating tags are metalake-wide capabilities with no object scoping,
so authoring access rules is inherently a central function. It cannot be delegated per-catalog
without new privileges.

`ANY_APPLY_TAG` above expands to
`(METALAKE::APPLY_TAG || TAG::APPLY_TAG) && !(METALAKE::DENY_APPLY_TAG || TAG::DENY_APPLY_TAG)`.

Applying a tag is the one operation that is already bounded on the object side.
`CAN_ACCESS_METADATA` resolves per entity type to that type's load expression; for a table:

```
ANY(OWNER, METALAKE, CATALOG) ||
SCHEMA_OWNER_WITH_USE_CATALOG ||
ANY_USE_CATALOG && ANY_USE_SCHEMA && (TABLE::OWNER || ANY_SELECT_TABLE || ANY_MODIFY_TABLE)
```

So an applier must already own the object or an ancestor, or hold traversal plus read or write on
the object itself. They are inside the data's trust boundary before they can tag it.

### What binding a policy to a tag delegates

Binding an access policy to a tag is a deliberate delegation: it says that whoever can apply this
tag may confer this access on the named role. That is the feature, not a defect.

Two properties of that delegation are worth recording:

- `CAN_ACCESS_METADATA` establishes that the applier can *access* the object. It does not establish
  that they may *confer* access on a role they do not control. These are different authorities.
- `ApplyTag.canBindTo` accepts only `METALAKE` and `TAG`, so the delegation cannot be scoped to a
  subtree — "may apply `certified` within `lakehouse.finance`" is not expressible.

See [OQ-4](#oq-4--object-scoped-apply_tag).

---

## Lifecycle

### Finding the policies that reference a role

The role name lives inside `content`, a JSON column. Answering "which policies reference `analyst`"
by scanning and parsing every policy in the metalake does not scale, and role deletion needs that
answer.

The server therefore also writes an indexed join row linking policy to role, in the same shape as
the existing `tag_relation_meta` and `policy_relation_meta` tables. The record is **derived** — the
server computes it from `content` — and **not user-writable**: no endpoint touches it. If it ever
disagrees with `content`, `content` is authoritative and the record is rebuilt.

Non-writability is the substantive part. If the record could be written directly there would be two
sources of truth for which role a policy conditions on, and the evaluator would have to choose
between them.

The record carries the role name, since that is what `content` holds.

### Deleting a role

The derived record makes the affected policies findable. What should happen to them is
[OQ-3](#oq-3--deleting-a-referenced-role).

### Deleting a policy or unbinding it from a tag

The rule stops applying immediately, subject to the freshness question above. Both are existing
policy operations; nothing is added.

### Events

Policy creation, update and deletion already emit events. Access policies emit the same events with
no additional payload. The bind and unbind operations emit the existing policy-to-tag events.

---

## Open questions

None of these are decided in this revision.

| | Question | Discussed in |
|---|---|---|
| OQ-1 | Where tags are evaluated | [Evaluation](#evaluation) |
| OQ-2 | Composition when a tag allows and RBAC denies | [below](#oq-2--composition-when-a-tag-allows-and-rbac-denies) |
| OQ-3 | What happens when a referenced role is deleted | [below](#oq-3--deleting-a-referenced-role) |
| OQ-4 | Whether object-scoped `APPLY_TAG` ships in v1 | [below](#oq-4--object-scoped-apply_tag) |

### OQ-1 — where tags are evaluated

Options and their consequences are set out in [Evaluation](#evaluation). The trade is between
inheriting the existing composition at the cost of an invalidation surface that fails permissive,
and stating the composition explicitly at the cost of request-path lookups.

### OQ-2 — composition when a tag allows and RBAC denies

| Option | Behaviour |
|---|---|
| Deny wins | An RBAC deny suppresses a tag allow unconditionally. Predictable, matches the existing deny semantics, and a tag can never be used to escape an explicit deny. Conflicts are invisible unless surfaced separately as diagnostics. |
| Refuse the overlap | Treat allow-from-tag over deny-from-RBAC as ambiguous and fail closed. Surfaces the conflict at the point it occurs, at the cost of denying requests that either rule alone would have resolved. |

Under Option 1 of [Evaluation](#evaluation) this is largely settled by the row set rather than
stated; under Option 2 it must be written into the composition expression.

### OQ-3 — deleting a referenced role

Only the policy is affected; the tag, the bind and the tagged objects are not. This also settles
whether `validate()` requires the role to exist at creation — refusing a dangling reference at one
end while allowing it at the other leaves the same state reachable.

| Option | Behaviour |
|---|---|
| Leave it dangling | Inert until a role of the same name is created, which silently reactivates the rule against a different population. |
| Refuse the deletion | Consistent with how other in-use references behave; requires the operator to clean up first. |
| Delete the referencing policies | No dangling state; silently removes rules the operator may not have known existed. |
| Disable them | Recoverable and visible; reactivation becomes an explicit act. |

### OQ-4 — object-scoped `APPLY_TAG`

`ApplyTag.canBindTo` accepts only `METALAKE` and `TAG`.

| Option | Behaviour |
|---|---|
| Leave as is | Delegation is per-tag, bounded on the object side by `CAN_ACCESS_METADATA`. A tag carrying access policies must be treated as a privileged object and granted narrowly. |
| Widen `canBindTo` to `CATALOG` and `SCHEMA` | Allows "may apply tags within `lakehouse.finance`". Introduces two grant dimensions — *which tag* and *where* — whose composition must then be defined, and changes the meaning of existing `APPLY_TAG` grants. |
| Restrict blanket `APPLY_TAG` for access-carrying tags | A tag with an access policy bound requires an explicit `TAG::APPLY_TAG`; metalake-wide `APPLY_TAG` does not reach it. Prevents grants issued when tags were purely descriptive from widening once tags carry access. |

This is the only open item that is a code change outside this feature rather than a decision
internal to it.

---

## Implementation phases

1. `AccessControlContent` and its `validate()`, registered in `PolicyContents` and the content DTO.
2. The derived policy-to-role record, written on policy create and update.
3. Evaluation, per the resolution of [OQ-1](#oq-1--where-tags-are-evaluated), including the
   freshness signal.
4. List filtering with batch preload.
5. Role deletion behaviour, per [OQ-3](#oq-3--deleting-a-referenced-role).
6. Documentation, including the traversal requirement described in
   [Composition with RBAC](#composition-with-rbac).

A task breakdown is deferred until the open questions are resolved, since the shape of steps 3 and 4
depends on OQ-1.
