---
title: "Migration Guide"
slug: "/migration-guide"
keyword: "migration, upgrade, compatibility, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

This guide lists user-visible behavior changes by upgrade version and the action needed for
existing deployments. For database backup, schema scripts, and rollback commands, see
[Upgrade Gravitino](./how-to-upgrade.md).

## Upgrading from Gravitino 1.3 to 2.0

### Policies are selected through tags

- Direct policy associations with metadata objects are no longer read or written. The upgrade
  does not convert existing direct associations. Before upgrading, export each policy's directly
  associated objects from `GET /api/metalakes/{metalake}/policies/{policy}/objects`. Save the
  policy name, object type, and full name, including direct associations on ancestor objects.
  Back up the metadata database before the schema upgrade.
- To preserve the old scope, create one dedicated tag per policy, associate the policy with that
  tag using the `ALL_VALUES` selector, and assign the tag to every object that had a direct
  policy association. A tag on a catalog or schema reaches its descendants. If a policy was
  directly associated with both a parent and a child, assign the tag to both.
- Object policy lookup at `GET /api/metalakes/{metalake}/objects/{type}/{fullName}/policies`
  remains available, but now returns enabled policies matching the object's effective tags.
  A policy matching through multiple tags appears once. With `details=true`, `inherited` is
  true only if the matching tag assignments are inherited. Disabled policies remain associated
  with tags but do not appear in object policy results.
- The legacy `supportedObjectTypes` field no longer filters object policy lookup. Consumers
  must decide which policy types they can use. For example, TMS should consume the built-in
  compaction policy type from each table's resolved policies.

### REST and client API changes

| Previous API or call | Migration |
|----------------------|-----------|
| `POST /objects/{type}/{fullName}/policies` | Associate the policy with a tag using `POST /tags/{tag}/policies/{policy}`, then assign the tag using `POST /objects/{type}/{fullName}/tags`. |
| `GET /objects/{type}/{fullName}/policies/{policy}` | List resolved policies with `GET /objects/{type}/{fullName}/policies?details=true` and select the policy by name. |
| `GET /policies/{policy}/objects` | Use `GET /policies/{policy}/tags` to inspect tag associations. It does not list every object reached through those tags. |
| Java `supportsPolicies().associatePolicies(...)` and `getPolicy(...)` | Use `GravitinoClient.addPolicyForTag(...)` and tag assignment APIs for writes; use `supportsPolicies().listPolicies()` or `listPolicyInfos()` for reads. |
| Java `Policy.associatedObjects()` | List tag associations with `GravitinoClient.listTagAssociationsForPolicy(...)`, then inspect affected objects separately. |

Paths in this table are relative to `/api/metalakes/{metalake}`. A policy-to-tag association
requires a selector. `ALL_VALUES` matches tag presence, including an assignment without a
value; `TAG_VALUE` matches one exact assignment value. To change an existing selector, remove
the association and add it again. See [Manage Policies](./manage-policies-in-gravitino.md#policy-to-tag-associations)
for REST and Java examples.

The Python client can manage tag assignments but does not provide policy APIs. Use REST for
policy-to-tag association changes.

### Tag assignment values and inheritance

- Existing string-based tag assignments remain supported. Use the v2
  `application/vnd.gravitino.v2+json` request when adding or removing a tag-value pair.
  A tag can have no value, one value, or multiple values.
- Allowed values are chosen when a tag is created and cannot be changed later. Plan the
  constraint before using a `TAG_VALUE` selector.
- A direct assignment on a child replaces inherited values for the same tag name. For example,
  `data_domain=risk` on a table overrides `data_domain=finance` from its catalog, so a
  `TAG_VALUE("finance")` association no longer matches that table. A dedicated migration tag
  with `ALL_VALUES` avoids this interaction.

See [Manage Tags](./manage-tags-in-gravitino.md#create-a-tag-with-a-value-constraint)
for tag creation and value assignment examples.

### Authorization, UI, and consumers

- Creating a migration tag requires `CREATE_TAG` on the metalake or ownership. Associating
  a policy with a tag requires access to both: `APPLY_POLICY` on the policy and `APPLY_TAG`
  on the tag, or the corresponding ownership. Assigning a tag also requires access to the
  target metadata object.
- Read-only inspection requires `VIEW_TAG` or `APPLY_TAG` for tags and `VIEW_POLICY` or
  `APPLY_POLICY` for policies. Object policy results include only policies visible to the caller;
  use an account with the same policy visibility when comparing results before and after migration.
- The current web UI still displays direct policy controls that call the removed
  object-policy write API. Use REST or Java for policy-to-tag associations during migration.
- Update TMS and other policy consumers to read the resolved object policy list. Confirm the
  expected policy appears on representative tables before resuming maintenance jobs.

### Migration checklist

1. On the old runtime, back up the database. Export direct policy associations for **each
   metalake** and record policy results on representative descendants. The old
   `/policies/{policy}/objects` endpoint is unavailable after the upgrade.
2. Map each direct relation to a tag assignment. A dedicated tag per policy with
   `ALL_VALUES` preserves the original scope; review existing tag values and child overrides
   before choosing `TAG_VALUE`.
3. Stop direct policy writes and pause consumers that require complete policy results. Upgrade
   the server and schema using [Upgrade Gravitino](./how-to-upgrade.md).
4. Create the tags, add policy-to-tag associations, and assign tags to every object from the
   exported direct-relation inventory.
5. Compare resolved policy names and `inherited` values for every inventoried object and
   representative descendants. Check enabled and disabled policies, direct child assignments,
   and value selectors. Use the same policy visibility for before and after comparisons.
6. Resume consumers after verification. Keep the inventory and database backup until the
   deployment is stable. If rollback is needed, stop the new server and follow the
   [database rollback procedure](./how-to-upgrade.md#rollback-if-upgrade-fails).

There is no automatic converter or verification command for direct policy relations. Do not
edit relation tables directly as part of this migration.
