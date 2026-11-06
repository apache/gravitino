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

### Audit client address behind a reverse proxy

- Gravitino 1.3 used the first `X-Forwarded-For` entry as the audit client address whenever
  the header was present. The server now ignores `X-Forwarded-For` unless the request comes from
  a proxy listed in `trustedProxies`.
- If the Gravitino server, the Iceberg REST service, or the Lance REST service runs behind a load
  balancer, ingress controller, or other reverse proxy, set
  `gravitino.server.webserver.trustedProxies`, `gravitino.iceberg-rest.trustedProxies`, or
  `gravitino.lance-rest.trustedProxies` to the proxy addresses. Without it, the audit log records
  the proxy address instead of the client address.
- IPv6 client addresses in audit events are now recorded in compressed form without brackets,
  for example `::1` instead of `[0:0:0:0:0:0:0:1]`. Update log queries or parsers that match the
  old form.
- List only the proxies. Trusting a range that untrusted clients can also connect from, such as a
  whole Kubernetes pod CIDR, lets those clients choose the recorded address again. See
  [HTTP Server](./gravitino-server-config.md#http-server).

### Policies are selected through tags

- Direct policy associations with metadata objects are no longer read or written at runtime.
  Before starting 2.0, use the offline migration tool below to convert historical bindings to
  `object → hidden tag → policy`. Startup is refused while active legacy bindings remain.
- The tool creates one server-managed hidden tag per policy and assigns it only to the objects
  with historical direct bindings. It does not expand assignments onto descendants. Policy IDs
  determine tag identities, so policy renames do not change scope. A policy attached to both a
  parent and a child retains both assignments and the child's direct-match flag.
- Each newly created policy also receives an unassigned hidden tag and its policy-to-tag
  relation in the same metadata transaction.
- The `tag_meta.hidden` column defaults to `false` for existing and ordinary tags. Policy-owned
  hidden tags store `hidden=true`; the server preserves this flag when loading and copying tags.
- Hidden tags have no assignment values and select the policy by tag presence. They are excluded
  from ordinary tag and policy-to-tag listings. Tag APIs cannot create, rename, change, assign,
  detach, or delete them. The `reserved_` name prefix is reserved, including alternate
  casing. New scope changes use ordinary policy-to-tag associations.
- Object policy lookup at `GET /api/metalakes/{metalake}/objects/{type}/{fullName}/policies`
  remains available, but now returns enabled policies matching the object's effective tags.
  A policy matching through multiple tags appears once. With `details=true`, `inherited` is
  true only if the matching tag assignments are inherited. Disabled policies remain associated
  with tags but do not appear in object policy results.
- In Gravitino 1.3, disabling a policy was advisory and did not change what consumers received.
  Object policy lookup now excludes disabled policies. Before upgrading, inventory disabled
  policies and re-enable any that consumers must continue to receive.
- The legacy `supportedObjectTypes` field no longer filters object policy lookup. Consumers
  must decide which policy types they can use. For example, TMS should consume the built-in
  compaction policy type from each table's resolved policies.

### REST and client API changes

| Previous API or call                                                  | Migration                                                                                                                                                |
|-----------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `POST /objects/{type}/{fullName}/policies`                            | Associate the policy with a tag using `POST /tags/{tag}/policies/{policy}`, then assign the tag using `POST /objects/{type}/{fullName}/tags`.            |
| `GET /objects/{type}/{fullName}/policies/{policy}`                    | List resolved policies with `GET /objects/{type}/{fullName}/policies?details=true` and select the policy by name.                                        |
| `GET /policies/{policy}/objects`                                      | Use `GET /policies/{policy}/tags` to inspect tag associations. It does not list every object reached through those tags.                                 |
| Java `supportsPolicies().associatePolicies(...)` and `getPolicy(...)` | Use `GravitinoClient.addPolicyForTag(...)` and tag assignment APIs for writes; use `supportsPolicies().listPolicies()` or `listPolicyInfos()` for reads. |
| Java `Policy.associatedObjects()`                                     | List tag associations with `GravitinoClient.listTagAssociationsForPolicy(...)`, then inspect affected objects separately.                                |

Paths in this table are relative to `/api/metalakes/{metalake}`. A policy-to-tag association
requires a selector. `ALL_VALUES` matches tag presence, including an assignment without a
value; `TAG_VALUE` matches when one effective tag assignment value equals the selector value.
To change an existing selector, remove the association and add it again. See
[Manage Policies](./manage-policies-in-gravitino.md#policy-to-tag-associations) for REST and Java
examples.

The Python client can manage tag assignments but does not provide policy APIs. Use REST for
policy-to-tag association changes.

### Tag assignment values and inheritance

- Existing string-based tag assignments remain supported. Use the v2
  `application/vnd.gravitino.v2+json` request when adding or removing a tag-value pair.
  A tag can have no value, one value, or multiple values.
- Allowed values are chosen when a tag is created and cannot be changed later. Plan the
  constraint before using a `TAG_VALUE` selector.
- Resolution uses the nearest assignment for a tag name. A direct assignment replaces all
  inherited values, and a nearer ancestor overrides a farther one. For example,
  `data_domain=risk` on a schema overrides `data_domain=finance` on its catalog for every table
  in that schema, so a `TAG_VALUE("finance")` association no longer matches those tables. A
  hidden migration tag matching presence avoids this interaction.

See [Manage Tags](./manage-tags-in-gravitino.md#create-a-tag-with-a-value-constraint)
for tag creation and value assignment examples.

### Authorization, UI, and consumers

- The offline tool uses the metadata database credentials from the server configuration.
  Run it only while every server using that database is stopped. It does not start the server,
  run schema upgrades, or expose a direct-binding REST or client API. Normal policy-to-tag
  operations retain their existing authorization requirements.
- Read-only inspection requires `VIEW_TAG` or `APPLY_TAG` for tags and `VIEW_POLICY` or
  `APPLY_POLICY` for policies. Object policy results include only policies visible to the caller;
  use an account with the same policy visibility when comparing results before and after migration.
- The current web UI still displays direct policy controls that call the removed
  object-policy write API. Use REST or Java for policy-to-tag associations during migration.
- Update TMS and other policy consumers to read the resolved object policy list. Confirm the
  expected policy appears on representative tables before resuming maintenance jobs.

### Offline policy binding migration

The server distribution includes `bin/migrate-policy-bindings.sh`. It reads `gravitino.conf`
from `GRAVITINO_CONF_DIR` (default: `$GRAVITINO_HOME/conf`) and uses the configured JDBC store.
H2, MySQL, and PostgreSQL are supported. For H2, the tool resolves the same storage path as the
server and requires the database file to exist; it never creates an empty replacement database.
Install the same JDBC driver in `libs` as for the server.

1. Inventory policies, disabled states, and representative descendant results on the old runtime.
   Stop all servers sharing the metadata database and pause policy consumers. Back up the database.
2. Apply the required SQL schema upgrades through 2.0 using
   [Upgrade Gravitino](./how-to-upgrade.md). Keep the preserved `policy_relation_meta` table
   until migration finishes.
3. From the upgraded server distribution, run the precheck:

   ```shell
   bin/migrate-policy-bindings.sh --dry-run
   ```

   This command is read-only. It reports the number of active legacy bindings and rejects
   unsupported object types, missing or foreign objects, missing policies or metalakes, and
   reserved tag name collisions. Resolve reported problems before proceeding.
4. Convert the historical bindings:

   ```shell
   bin/migrate-policy-bindings.sh
   ```

   Each policy is migrated in one database transaction. The tool calls the internal
   `PolicyManager.addPolicyToObject` method to create or reuse its hidden tag and object association,
   then checks every replacement, then soft-deletes its old relations.
   Relation audit information is preserved. Progress is logged for verification and committed cleanup.
   If a policy fails, its transaction rolls back; already committed policies remain migrated. Rerun the same command
   after correcting the failure. It resumes from the remaining active legacy rows.
5. Verify before restarting servers:

   ```shell
   bin/migrate-policy-bindings.sh --verify
   ```

   This command is read-only and fails if active legacy bindings remain, hidden tags or their
   policy relations are invalid, or a hidden tag has assignment values. Each replacement object
   association is verified inside its migration transaction before the old binding is retired.
   Subsequent `--verify` runs check the current hidden-tag structure and migration completion.
   A fresh installation without the legacy table needs no migration. Repeat execution creates no
   duplicate tags or associations. The tool accepts no object or policy arguments to create
   arbitrary bindings.
6. Start the new server and compare resolved policy names and `inherited` values on representative
   objects and descendants, using the same policy visibility as before. Verify disabled policies
   and existing business tag associations, then resume consumers.

Migrated policies remain visible through object policy lookup and normal policy lifecycle APIs.
Disabling a policy suspends its effect; deleting it removes its hidden tag and assignments in the
same storage transaction. Ordinary tag APIs do not offer per-object editing of migrated bindings.
For ongoing scope management, use visible business tags and policy-to-tag associations. Run
`--verify` as an upgrade check before normal object deletion or other metadata changes resume.

Keep the database backup and migration logs until the deployment is stable. If rollback is
needed, stop all new servers and restore the complete pre-upgrade backup using the
[database rollback procedure](./how-to-upgrade.md#rollback-if-upgrade-fails). Do not restart the
old runtime against a partially migrated database or manually edit relation tables.
