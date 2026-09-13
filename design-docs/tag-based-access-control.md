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

An access rule is a `Policy` of type `system_access_control` whose `content` carries a set of
privileges and a role condition. The policy is bound to a tag. Any object carrying that tag
becomes subject to the rule.

```json
POST /api/metalakes/prod/policies
{
  "name": "certified_access",
  "policyType": "system_access_control",
  "enabled": true,
  "content": {
    "privileges": ["SELECT_TABLE", "MODIFY_TABLE"],
    "applicable_roles": ["analyst", "data_engineer"]
  }
}
```

```
PUT  /api/metalakes/prod/tags/certified/policies/certified_access
     { "selector": { "type": "ALL_VALUES" } }

POST /api/metalakes/prod/objects/TABLE/lakehouse.finance.orders/tags
     { "tagsToAdd": [{ "name": "certified" }] }
```

Read together: *a caller holding either `analyst` or `data_engineer` may select from and modify
any table that carries the tag `certified`.* Any listed role satisfies the condition, and every
listed privilege is conferred to it.

Only one thing is attached: the policy to the tag. The roles are values inside `content`, not a
second link. No new user-facing entity, REST resource or client API is introduced.

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

So a label cannot drive access. An organization that already tags tables `certified`, `pii` or
`data_domain=finance` must still issue grants object by object to act on those tags. New objects
need new grants, dropped objects leave stale ones, and the rule itself is written down nowhere — it
exists only as the pile of grants someone remembered to issue.

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
| `DENY` | A deny that a descendant tag cannot undo is a separate problem. Allow-only means every rule set has an answer and the order rules are applied in never matters. |
| Users and groups as the matched condition | The condition schema can gain them later without changing the model. |
| Row filtering and column masking | Distinct policy types; this design governs whole-object decisions. |
| Cross-tag conditions | A rule matches one tag. Conditions spanning several tags await the `EXPRESSION` selector type in [policy-on-tag.md](policy-on-tag.md). |
| Column-level decisions | A tag on a column does not affect decisions about its table. |
| A `scope` field in `content`, restricting a rule to a subtree | Not a security boundary: creating a policy already needs metalake-wide `CREATE_POLICY`, so whoever writes the rule chooses its reach anyway. It is also not checked when a tag is applied, so it limits where a rule takes effect rather than stopping a wrong tag. One tag meaning different things in different subtrees is already covered by tag assignment values with a value-sensitive selector. Can be added later, since an absent `scope` has always meant metalake-wide. |
| Replacing RBAC | Baseline privileges, ownership and traversal are unchanged. See [Composition with RBAC](#composition-with-rbac). |

---

## Alternatives considered

| Option | Pros | Cons | Status |
|---|---|---|---|
| **A `system_access_control` policy type bound to a tag** | Reuses the entity, relation, selector and resolver; no new REST or client surface; one governance model to learn | The role condition lives in `content` JSON, so lookup by role needs a derived index rather than a foreign key | **Proposed** |
| A dedicated `tag_access_policy` entity with action and role as columns | Foreign key on role; indexed lookup; cascade on role deletion falls out of the schema | New table across three dialects, new REST resource, new client and CLI surface, a second governance model alongside policies | Rejected |
| Extend RBAC grants with a tag predicate | No new concepts | The grant table is object-identified; a predicate has no object, and every grant read path would change | Rejected |
| Evaluate tags in an external engine (OPA and similar) | Arbitrary policy language | Moves the decision out of Gravitino, duplicates the tag hierarchy, and cannot use the existing expression path | Rejected |

That one con means the server keeps the role reference consistent, rather than the database schema
doing it. [Lifecycle](#lifecycle) covers how.

---

## Model

### Content

`PolicyContent` is an interface, and each built-in policy type has a concrete implementation with
typed fields and a `validate()` that runs at write time. `IcebergDataCompactionContent` is the
existing example. `system_access_control` follows the same pattern with a new
`AccessControlContent`:

| Field | Type | Meaning |
|---|---|---|
| `privileges` | list of `Privilege.Name` | The privileges the rule confers. Each must be a permitted name — see [Permitted privileges](#permitted-privileges). |
| `applicable_roles` | list of role names | The **condition**. Satisfied when any listed role is among the caller's expanded roles. |

`validate()` rejects at creation rather than at evaluation:

- `privileges` is non-empty and every entry parses to a permitted `Privilege.Name`.
- `applicable_roles` is non-empty and every name is non-blank.

Rejecting at write time matters because the alternative failure is silent: a policy naming a
privilege that does not parse simply grants nothing, and nothing surfaces until someone notices
the access they expected is missing.

Whether `validate()` also requires the named role to *exist* is part of
[OQ-3](#oq-3--deleting-a-referenced-role), not a separate decision.

### Permitted privileges

Parsing to a `Privilege.Name` is a syntax check, not a safety one. A rule may confer only
privileges that grant access to the tagged object itself; `validate()` rejects two classes:

- **Authority over the authorization system** — `MANAGE_USERS`, `MANAGE_GROUPS`, `MANAGE_GRANTS`,
  `CREATE_ROLE`, `CREATE_TAG`, `APPLY_TAG`, `CREATE_POLICY`, `APPLY_POLICY`. These turn one tagging
  operation into a standing ability to widen access. `MANAGE_GRANTS` binds to every taggable type
  and covers all children of whatever it binds to, so a tag carrying it on one catalog would let
  every role in `applicable_roles` grant anything beneath that catalog — and would keep doing so
  after the applier's own authority was revoked. `APPLY_TAG` and `APPLY_POLICY` close the loop
  further, letting a conferred role extend the tag system's own reach.
- **Traversal** — `USE_CATALOG` and `USE_SCHEMA`, for the reasons in
  [Traversal stays RBAC](#traversal-stays-rbac).

Both exclusions are the same argument: a tag confers access to data, never the ability to hand out
access or to reach new territory. [OQ-4](#oq-4--authority-to-confer-access-through-a-tag) governs
who may apply a rule and does not substitute for this, because the applier holds the authority
privilege by construction — the check they pass is exactly the one the rule would make permanent.

### `applicable_roles` is a condition, not a principal

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
3. for each, whether any of `applicable_roles` is among the caller's expanded roles.

Access rules only allow — `content` carries no condition field — so neither option below lets a tag
restrict or deny. An RBAC `DENY` is unaffected; see [Allow and deny](#allow-and-deny).

The question is *where* steps 1 and 2 happen.

### Proposed: check tags at the privilege leaf

Every privilege check bottoms out in `GravitinoAuthorizer.authorize`. The expression converter
expands each `ANY_*` macro mechanically —

```
ANY_USE_CATALOG → ANY(USE_CATALOG, METALAKE, CATALOG) && !ANY(DENY_USE_CATALOG, METALAKE, CATALOG)
```

— and `hasAuthorizeWithoutDeny` walks the object's ancestor chain calling `authorize` and `deny` at
each level. Tag evaluation goes inside `authorize`: when the RBAC rows do not allow, resolve the
effective tags of the object being decided, load the access policies bound to them, and test those
against the caller's roles.

Three properties follow from the surrounding code rather than from a rule this design has to write:

- **RBAC deny still wins.** The `!ANY(DENY_…)` conjunct is built from `deny`, which the tag path
  never touches, so an allow-only tag cannot reach it. See
  [OQ-2](#oq-2--composition-when-a-tag-allows-and-rbac-denies).
- **Traversal stays RBAC.** Each conjunct consults tags independently, so a tag granting
  `SELECT_TABLE` still cannot bypass `USE_CATALOG`.
- **A denial stays attributable.** The tag check is a distinct step, so the information needed to
  explain a decision stays separable from the grant that would otherwise have produced it.

One rule does not come for free. Role assumption narrows a request to the roles the caller
activated, but that narrowing lives inside `enforceNarrowed`, on the jCasbin path the tag check
does not take. The tag check therefore applies it itself: `applicable_roles` is tested against the
caller's *active* roles, and an `ActiveRoles.none()` request grants nothing. Otherwise a caller who
narrowed would silently keep tag-derived access they had asked to drop.

Inheritance is the one thing the walk does not supply. `authorize` resolves the object's effective
tags once ([tag-assignment-values.md](tag-assignment-values.md)) rather than asking each level in
turn. Different tag names still union down the chain; nearest-wins settles only the same name
assigned at two levels, where the nearer assignment wins and the farther one is dropped:

```
catalog lakehouse        certified = gold      pii = true
table   finance.orders   certified = bronze

effective on the table   certified = bronze    pii = true
```

`pii` is inherited; `certified=gold` is gone because the table overrode it. So a rule bound with
`TAG_VALUE("gold")` does not match, while asking level by level would still find `gold` on the
catalog and grant. The two readings agree under `ALL_VALUES`, where only the presence of the name
matters, and diverge as soon as a rule reads the value.

One constraint on where the check hooks in. `hasAuthorizeWithoutDeny` walks the ancestor chain
calling `authorize` at each level, so a check placed inside that per-level call would resolve
effective tags once per level, each resolution walking its own chain — quadratic in chain depth,
and for nothing, since the leaf's effective tags already subsume every ancestor's. The check runs
once for the object under decision, not once per level of the RBAC walk.

The cost lands on the request path, and is set out in [Cost](#cost).

### Rejected: expand tag rows when roles load

The alternative writes one permission row per (role, object) when a role's policies load, so tag
permissions are indistinguishable from RBAC ones at decision time.

It fails on cardinality. The jCasbin matcher compares `metadataId` for equality with no prefix
form, so a tag on a catalog grants on a table only if a row exists for that table. Tagging one
catalog materialises a row per descendant per affected role, and every later `CREATE TABLE` beneath
it has to add rows — a write-path dependency on the authorizer that does not exist today. Miss one
and stale rows keep granting, which errs towards more access rather than less.

### Freshness

A node that has already loaded the affected roles still has to learn that tag state changed.

Today the authorizer keeps role policies fresh by version-checking on read: `loadedRoles` maps role
id to `updated_at`, and a newer `role_meta.updated_at` in the database evicts and reloads that
role's policies. `groupRoleCache` is validated the same way against `group_meta.updated_at`. Write
paths additionally call `handleRolePrivilegeChange`, `handleUserRoleRelChange` and
`handleGroupRoleRelChange` in-process on the node that performed the write, and TTL bounds the rest.
`JcasbinChangeListener` covers two further surfaces: entity changes through `onEntityChange`, and a
poll of `owner_meta`.

Tag state reaches none of that, and the transport differs by what changed:

| Change | Reaches other nodes today |
|---|---|
| Tag or policy entity created, altered, dropped | Yes — `entity_change_log` carries `TAG` and `POLICY`, but `JcasbinChangeListener` discards both as virtual-namespace types |
| Tag applied to or removed from an object | No — relation changes emit no change-log rows |
| Policy bound to or unbound from a tag | No — same |

The first needs the existing filter relaxed. The second and third need a transport that does not
exist yet: relation changes emitted into `entity_change_log`, a poll of the relation tables, or a
TTL accepted as the bound. The rejected option needs the same three signals, and reacts to each by
rewriting rows rather than by dropping a cache entry.

See [OQ-1](#oq-1--where-tags-are-evaluated).

### List filtering

List endpoints filter their results through the same authorizer, so tag-derived permissions must be
visible to filtering as well as to point decisions.

Filtering has a fast path today: `allVisibleViaParentScope` skips the per-object loop entirely when
a parent-scope grant makes every candidate visible and no object-level deny exists. Tags only add
access, so that short-circuit stays correct untouched — if RBAC already shows everything, no tag
can change the answer.

When it misses, the per-object loop runs, and that loop is deliberately free of per-object queries:
`preloadToCache` batch-gets the entities and `preloadOwner` batch-gets their owners, leaving the
loop as in-memory evaluation. Resolving tags per candidate breaks that property and turns one
listing into N walks of the ancestor chain. Filtering needs a batch preload of tag and policy state
for the candidate set, alongside those two paths — not as an optimisation, but to keep an invariant
the loop already has.

### Cost

The feature is off by default, and the gate sits on evaluation, so a deployment that leaves it off
pays nothing and the rest of this section does not apply to it. See
[Enabling the feature](#enabling-the-feature).

With it on, tags are consulted only when the RBAC rows do not already allow, so a check RBAC grants
costs nothing. The cost falls on the not-allowed branch — which is the common branch for list
filtering, where most candidates are objects the caller cannot see. The feature therefore makes
"no" more expensive than "yes", inverting the shape RBAC has today.

On that branch, one check resolves the object's effective tags, then loads the policies bound to
them:

| Step | Cost today |
|---|---|
| Resolve the object's effective tags | One relation query per level of the ancestor chain. Nothing caches the result — `RelationalEntityStore` caches entities, not relation queries. |
| Load the policies bound to each tag | One `POLICY_METADATA_OBJECT_REL` query per distinct effective tag. |
| Test `applicable_roles` | In memory, against roles the request has already loaded. |

The chain is not bounded by a constant. `getParentMetadataObjects` expands a hierarchical schema
one level at a time, so a column under `catalog.a:b.table` walks five levels, and a deeper schema
walks more:

```
COLUMN:catalog.a:b.table.col -> TABLE:catalog.a:b.table -> SCHEMA:catalog.a:b
                             -> SCHEMA:catalog.a -> CATALOG:catalog
```

Two costs the query count hides. `TagManager.listTagsInfoForMetadataObject` and
`PolicyManager.listPolicyInfosForMetadataObject` each take a tree read lock and call
`checkMetadataObject`, whose existence check can reach the underlying catalog — so the evaluator
has to query the relation directly rather than reuse them. And `batchListEntitiesByRelation`
implements only `OWNER_REL` today, so the batch preload below is a new query, not a reuse.

There is also a write-side cost that predates this design: applying or removing a tag invalidates
the entity cache for both endpoints, and that invalidation cascades down the identifier hierarchy,
so tagging a catalog drops every cached schema and table beneath it.

Three things get worse when the feature is on. The second is a trade-off the feature asks for
rather than a defect, but it should be a deliberate choice rather than a surprise:

- **Listings that miss the parent-scope short-circuit.** The per-object loop issues no per-object
  queries today, provided the preload runs — `preloadToCache` returns early when the entity cache
  is disabled or the type is not preloadable. Each not-allowed candidate adds a chain walk plus a
  policy query per tag, so a listing of N objects goes from a batch preload and in-memory
  evaluation to O(N × d) queries against the relational store. Latency on a single call is not the
  concern; connection-pool pressure under concurrency is.
- **Granting through tags instead of coarse RBAC makes that miss more often.** The short-circuit
  fires on a parent-scope RBAC grant. A deployment that replaces those grants with tag-derived
  access removes the condition the short-circuit tests, so listings that are free today take the
  slow path.
- **Tag churn degrades RBAC.** The invalidation above is not scoped to the tag path — the entity
  cache is shared, so retagging a catalog costs every request that reads entities beneath it,
  including requests that never touch a tag.

Two guards keep the not-allowed branch cheap where no tag could grant anyway: no enabled
`system_access_control` policy in the metalake, resolved once per request; and no active roles on
the request, since `applicable_roles` is tested against active roles and an empty set matches
nothing. The first has no list-by-type query today, so it lists the metalake's policies.

Three mitigations, in increasing order of what they cost to build:

- **Per-request memoisation.** `AuthorizationRequestContext` already memoises the final decision,
  but keys it on principal, metalake, object and privilege, while the expensive part — effective
  tags and the policies bound to them — depends on neither the principal nor the privilege.
  Memoising that per object keeps a request checking several privileges on one object to a single
  walk.
- **Batch preload for lists.** Resolve the candidate set's tag and policy state in one round trip
  rather than N walks, as above.
- **A cache across requests.** Not designed here, and not yet designable: caching tag state beyond
  one request needs an invalidation signal, and two of the three signals in
  [Freshness](#freshness) have no carrier today. Performance and freshness are the same problem.

The first two land in M3 and M5. The third becomes possible once M4 does, and until then the
uncached cost above is what the feature costs.

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
a schema the role cannot enter has no effect. `validate()` rejects both names in `privileges`.

### Allow and deny

With `ALLOW` only, two access rules cannot conflict — they union. The interaction that remains is
between a tag rule that allows and an RBAC grant that denies; the proposal is that the deny wins. See
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

Creating a policy and creating a tag are both metalake-wide, with no way to scope either to a
catalog. Authoring access rules is therefore a central function today; delegating it per-catalog
would need new privileges.

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
the object itself. That bounds which objects they can tag, not what the tag may confer on them.

### What binding a policy to a tag delegates

Binding an access policy to a tag is a deliberate delegation: it says that whoever can apply this
tag may confer this access on the named role. That is the feature, not a defect.

Two properties of that delegation are worth recording:

- `CAN_ACCESS_METADATA` establishes that the applier can *access* the object. It does not establish
  that they may *confer* access on a role they do not control. These are different authorities.
- `ApplyTag.canBindTo` accepts only `METALAKE` and `TAG`, so the delegation cannot be scoped to a
  subtree — "may apply `certified` within `lakehouse.finance`" is not expressible.

See [OQ-4](#oq-4--authority-to-confer-access-through-a-tag).

### Enabling the feature

A server-level configuration gates the feature, and it defaults to off.

The gate sits on evaluation, not on storage. With it off, policies can still be created, validated
and bound to tags, and tags can still be applied — every write path in this section works, and
every authority check on it still applies. The evaluator never consults the result, so no tag
confers access. That is not a dry run: nothing is evaluated, so nothing is reported. The
configuration is inert rather than observed.

Two consequences are worth stating plainly:

- **Turning it off revokes.** Access held only through a tag disappears at once. The direction is
  safe — nothing gains access — but callers experience a revocation, not a pause.
- **Turning it on confers everything authored while it was off.** That access was not granted
  unchecked: the write-path authority checks run at bind time, so whoever bound a policy to a tag
  held the authority to confer it. The flag decides when a delegation takes effect, not whether it
  was authorised.

On means fully on. Enforcing at point checks but not in list filtering would leave a caller holding
access to objects that never appear in their listings; the reverse lists objects that are then
denied. Neither leaks data, since tags only add access, but neither is worth building.

A dry run — evaluate, log what the tag path would have granted, return the RBAC answer regardless —
is a third mode rather than this one. Because tags only add access, its output can only ever be
grants that would newly apply, which makes it a useful way to review a configuration before
enabling it. It pays the full request-path cost for no functional benefit, so it is a diagnostic
rather than a resting state, and nothing below depends on it.

---

## Lifecycle

### Finding the policies that reference a role

Role names live inside `content`, a JSON column. Answering "which policies reference `analyst`"
by scanning and parsing every policy in the metalake does not scale, and role deletion needs that
answer.

The server therefore also writes an indexed join row per referenced role, in the same shape as
the existing `tag_relation_meta` and `policy_relation_meta` tables. The record is **derived** — the
server computes it from `content` — and **not user-writable**: no endpoint touches it. If it ever
disagrees with `content`, `content` is authoritative and the record is rebuilt.

The "not user-writable" part is what matters. If the record could be written directly there would
be two answers to which roles a policy names, and the evaluator would have to pick one.

The record carries role names, since that is what `content` holds.

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

## Prior art

Three systems solve this problem, and the shape proposed here matches them. The table records only
what current vendor documentation states; `—` means not verified rather than absent.

| | Apache Ranger | AWS Lake Formation | Databricks Unity Catalog |
|---|---|---|---|
| Feature | Tag-based policies | LF-TBAC | ABAC `GRANT` policies |
| Rule attaches to | A tag, within a tag service | An LF-Tag expression | A catalog or schema, with a tag condition |
| Evaluated | At request time; `RangerTagEnricher` adds the resource's tags to the request context | At request time, against the resource's tags | At request time, on each access attempt |
| Inheritance | From the tag source | Table from database, column from table; override allowed | From parent catalog or schema; override allowed |
| Can a tag rule deny? | Yes | No — grant only | No — adds access only |
| Authority to apply a tag | — | A distinct grant to assign LF-Tags | `ASSIGN` on the tag **and** `APPLY TAG` on the object |

Four points of agreement, each corresponding to a decision made above:

- **Rules attach to a tag, not to the object.** The premise of the feature.
- **Tags are resolved on the request path**, not pre-expanded into a grant per object. No system
  surveyed does the expansion, which is the option [Evaluation](#evaluation) rejects.
- **Tags inherit down the hierarchy**, and both Lake Formation and Unity Catalog let a nearer
  assignment override an inherited one — the nearest-wins rule this design already assumes
  ([tag-assignment-values.md](tag-assignment-values.md)).
- **Applying a tag is authority in its own right.** Unity Catalog requires a tag permission *and*
  an object permission, which is the two-clause structure proposed in
  [OQ-4](#oq-4--authority-to-confer-access-through-a-tag). Databricks gives the reason directly:
  if a user can change tags on an asset, they can change which policies apply to it.

Two divergences are worth naming.

Ranger tag policies can deny; the policies here cannot. Lake Formation and Unity Catalog are both
allow-only, and Databricks states that GRANT policies cannot revoke access granted directly, so the
restriction in [Allow and deny](#allow-and-deny) is the majority position rather than an unusual
one.

Ranger is also the only one of the three that documents an answer to [Freshness](#freshness): the
plugin caches tags locally, polls the tag store for changes, and falls back to the cache file when
the store is unreachable. It accepts a staleness window rather than eliminating one — which is the
shape of answer OQ-1 is likely to need.

---

## Open questions

None of these are settled. Where this revision has a preference, the option is marked **Proposed**.

| | Question | Discussed in | Proposal |
|---|---|---|---|
| OQ-1 | Where tags are evaluated | [Evaluation](#evaluation) | Inside `authorize`, at the privilege leaf |
| OQ-2 | Composition when a tag allows and RBAC denies | [below](#oq-2--composition-when-a-tag-allows-and-rbac-denies) | Deny wins |
| OQ-3 | What happens when a referenced role is deleted | [below](#oq-3--deleting-a-referenced-role) | Refuse the deletion |
| OQ-4 | What authority conferring access through a tag requires | [below](#oq-4--authority-to-confer-access-through-a-tag) | Grant authority on the object, and an explicit tag grant |

### OQ-1 — where tags are evaluated

Both placements and the reasoning are set out in [Evaluation](#evaluation). Checking at the leaf
inherits deny and traversal from the surrounding code, keeps a denial attributable, and reaches
inherited tags through one nearest-wins resolution per object; expanding rows at load time reuses
the permission engine but, because the matcher compares ids for equality, needs a row per
descendant object and a write-path dependency to maintain them.

What stays open is not the placement but the freshness transport: two of the three signals in
[Freshness](#freshness) have no carrier today, and both placements need all three.

### OQ-2 — composition when a tag allows and RBAC denies

The overlap in question is an explicit RBAC `DENY_*` row against a tag allow, not the absence of an
RBAC grant — absence is what sends the decision to tags in the first place.

| | Option | Behaviour |
|---|---|---|
| **Proposed** | Deny wins | An RBAC deny suppresses a tag allow unconditionally. Predictable, matches the existing deny semantics, and a tag can never be used to escape an explicit deny. Conflicts are invisible unless surfaced separately as diagnostics. |
| | Refuse the overlap | Treat allow-from-tag over deny-from-RBAC as ambiguous and fail closed. Surfaces the conflict at the point it occurs, at the cost of a third decision outcome the authorizer does not have today. |

Justification: deny wins for free under the placement proposed in [Evaluation](#evaluation).
`AuthorizationExpressionConverter` already expands every `ANY_*` macro to `ANY(X) && !ANY(DENY_X)`,
and a tag allow only feeds the allow side, so the RBAC deny still applies. Refusing would deny the
request too, so it changes no outcome — it only makes the conflict visible, and the M3 diagnostic,
which names the tag and policy whose allow the deny suppressed, does that without adding a third
decision state.

This relies on tag-conferrable privileges being reached through `ANY_*` macros; a bare
`TYPE::PRIVILEGE` has no deny conjunct. M3 carries a test to keep it that way.

### OQ-3 — deleting a referenced role

Only the policy is affected; the tag, the bind and the tagged objects are not. This also settles
whether `validate()` requires the role to exist at creation, since rejecting a dangling reference
at one end while allowing it at the other reaches the same state either way.

| | Option | Behaviour |
|---|---|---|
| **Proposed** | Refuse the deletion | The role cannot be deleted while a policy still references it; the operator clears those references first. No delete in Gravitino is blocked by a reference today, so this would be the first. |
| | Delete the referencing policies | No dangling state, but it removes rules the operator may not have known existed. Acceptable only as an explicit, confirmed cascade, which is out of scope here. |
| | Leave it dangling | Inert until a role of the same name is created, which silently reactivates the rule against a different population. |
| | Disable them | Deleting a role would then also change policy state, conflating two operations that should stay separate. |

Justification: the operator deleting a role and the operator owning the policies that reference
it are often not the same person, so the deletion should stop rather than silently change rules
its author cannot see. Cascade stays available later as an explicit, confirmed act.

### OQ-4 — authority to confer access through a tag

Two write paths confer access without going through a grant: applying a tag that carries an access
policy, and binding an access policy to a tag that is already applied. Neither checks grant
authority. A user holding only `SELECT_TABLE` can apply a tag that gives another role
`MODIFY_TABLE` — read access on an object becomes authority over who else can reach it.

| | Option | Behaviour |
|---|---|---|
| **Proposed** | Require grant authority on the object, and an explicit `TAG::APPLY_TAG` | Applying an access-carrying tag also requires `MANAGE_GRANTS` on the object or an ancestor, or ownership of it — the same check `grantPrivilegeToRole` makes today. And a metalake-wide `APPLY_TAG` stops reaching a tag once it confers access, so grants issued when tags were descriptive do not silently widen. |
| | Leave as is | Reading an object is enough to change who else can reach it. |
| | Require the applier to hold the privilege the tag confers | SQL `GRANT` semantics. Gravitino's own grant path does not work this way — `MANAGE_GRANTS` lets an operator hand out privileges they do not hold — so tags would become stricter than roles. |
| | Require policy authority to bind an access policy to a tag | Covers the other write path. Today a tag owner holding no policy privileges can bind one, and everything already carrying the tag gains the access at once. |

Justification: the two requirements cover different halves — one governs who may change access
on an object, the other who may wield a tag that confers it. Neither adds a new privilege, and
scoping needs no new mechanism, because grant authority can already be given on a catalog or a
schema.

---

## Implementation milestones

Each milestone names the open questions it rests on. Those are proposals, not decisions — if one
resolves differently, the milestones marked against it change shape.

| Milestone | What lands | Rests on |
|---|---|---|
| M1 — model and storage | `AccessControlContent` and its `validate()`, registered in `PolicyContents` and the content DTO, and the derived policy-to-role record written on policy create and update. Policies can be created, validated and bound to tags; nothing evaluates them yet. | [OQ-3](#oq-3--deleting-a-referenced-role), for whether `validate()` rejects a reference to a role that does not exist. |
| M2 — authority on the write paths | The checks that applying an access-carrying tag, and binding an access policy to a tag already applied, have to make. Lands before M3 is switched on, or both paths confer access unchecked. | [OQ-4](#oq-4--authority-to-confer-access-through-a-tag). Independent of where tags are evaluated. |
| M3 — enforcement, single node | The check at the privilege leaf described in [Evaluation](#evaluation), with per-request caching, behind the configuration in [Enabling the feature](#enabling-the-feature) — which lands here, or there is no way to back the feature out. Tags now grant access, correctly on one node: an edit to a tag or a policy takes effect once the existing caches turn over. Because list endpoints filter through the same authorizer, filtering starts consulting tags here too, at the unbatched cost in [Cost](#cost). Two smaller pieces land with it: the diagnostic naming the tag and policy whose allow an RBAC deny suppressed, and the test asserting that no tag-conferrable privilege appears in bare `TYPE::PRIVILEGE` form in an authorization expression. | [OQ-1](#oq-1--where-tags-are-evaluated) — expanding rows at load time would make this a write-path milestone instead. [OQ-2](#oq-2--composition-when-a-tag-allows-and-rbac-denies) needs no combining rule under the proposed placement, only those two pieces; the other option needs one. |
| M4 — freshness | A transport for the three signals in [Freshness](#freshness). Makes M3 correct across a cluster; until it lands, the feature is only safe to rely on in a single-node deployment. | The transport is the second half of [OQ-1](#oq-1--where-tags-are-evaluated). Both placements need all three signals, so the milestone itself stands either way. |
| M5 — affordable list filtering | The batch preload in [List filtering](#list-filtering). Filtering already consults tags from M3; this is what stops it costing an ancestor walk per candidate, so enabling the feature on a large metalake before M5 is correct but expensive. | [OQ-1](#oq-1--where-tags-are-evaluated), for the same reason as M3. |
| M6 — lifecycle and documentation | Role deletion, the events in [Events](#events), and the traversal requirement in [Composition with RBAC](#composition-with-rbac). | [OQ-3](#oq-3--deleting-a-referenced-role), for the deletion behaviour. |

M1, M2 and M4 hold whichever way OQ-1 is answered. M3 and M5 are the two that change with it.
