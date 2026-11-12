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

### Domains

- Domains add business groupings within a metalake, backed by explicit tag bindings. Existing tags
  and assignments are preserved; they are not automatically converted into domains.
- Complete the standard 1.3-to-2.0 database schema upgrade before using Domain APIs. The H2, MySQL
  and PostgreSQL upgrade scripts include the Domain metadata and hierarchy tables. Use matching
  server and client versions; a client upgrade alone does not add server-side Domain support.
- With authorization enabled, grant `MANAGE_DOMAIN` or `USE_DOMAIN` separately from tag and data
  permissions. Existing tag/data privileges do not grant Domain management or discovery.
- See [Manage domains](./manage-domains-in-gravitino.md) for the workflow and
  [Upgrade Gravitino](./how-to-upgrade.md) for the database upgrade procedure.

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
  Use the offline migration command below to convert existing bindings to `object → hidden tag → policy`.
  It uses the existing tables and requires no additional schema changes.
- The tool assigns each policy's hidden tag to the objects with historical direct bindings.
  It preserves assignments on both parents and children, so descendant inheritance and direct-match
  flags remain unchanged. Hidden tag names use `gravitino_hidden_{policyId}` and survive policy renames.
- Hidden tags have no assignment values and select the policy by tag presence. REST APIs hide
  these tags and prevent users from modifying them. Use visible business tags for future scope changes.
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

The distribution includes `bin/migrate-policy-bindings.sh`. It reads `gravitino.conf` from
`GRAVITINO_CONF_DIR` (default: `$GRAVITINO_HOME/conf`) and connects to the configured metadata
store. H2, MySQL, and PostgreSQL are supported; use the same JDBC driver as the server.

1. Record representative policy results, stop all servers sharing the metadata database, pause
   policy consumers, and back up the database.
2. Run the command from the new distribution:

   ```shell
   bin/migrate-policy-bindings.sh
   ```

   Each policy migrates in one transaction: create or reuse its hidden tag, copy its direct object
   assignments with their audit information, verify the replacements, then soft-delete the legacy
   rows. A missing or incorrect policy-to-tag relation fails verification and leaves the legacy
   bindings active. No tables, columns, or indexes are added.
   Bindings whose policy, metalake, or target object is missing or deleted are logged and left
   active. Other valid bindings still migrate. The final summary reports migrated bindings,
   skipped owners and bindings, missing-object bindings, failed policies, and unprocessed policies.
   Skipped bindings alone do not cause a nonzero exit code. A final WARN reports their counts:
   the new server reads tag-based associations and does not read these retained legacy rows.
   Review the logged IDs before completing the upgrade; a successful exit does not mean all
   legacy bindings were migrated.
3. If a policy fails, its changes roll back and other policies are still attempted. Any failed
   policy makes the command exit unsuccessfully after printing the summary. If rollback itself
   fails, processing stops and the summary reports how many policies were not attempted.
   Keep all servers and policy consumers stopped on failure or interruption. Fix the reported cause
   and rerun the same command: completed policies remain committed, while unfinished policies are
   retried from their active legacy rows. A partially migrated
   database is only an offline intermediate state, not a deployment to start serving requests.
   Restore the backup only when abandoning or rolling back the upgrade.
   A `Hidden tag name collision` error identifies the policy and tag and rolls back that policy. If an
   existing user tag occupies `gravitino_hidden_{policyId}`, restore the pre-upgrade backup, run
   the old server version, and rename that user tag to a name outside `gravitino_hidden_` through
   the tag API. Stop the old servers, take a fresh backup, and repeat the upgrade and migration.
   Renaming preserves the tag's assignments. If the conflicting tag is a damaged server-managed
   tag, restore a known-good backup and investigate the corruption before retrying. Do not delete
   legacy bindings or overwrite the conflicting tag to force migration to finish.
4. Start the server and compare resolved policy names and `inherited` values on representative
   objects and descendants. Check disabled policies and existing business tag associations before
   resuming consumers.

Migrated policies retain their normal lifecycle: disabling a policy suspends its effect, and
removing it cleans up its hidden tag and assignments. Keep the database backup until the deployment
is stable. For rollback, stop the new server and restore the complete pre-upgrade backup using
[Upgrade Gravitino](./how-to-upgrade.md#rollback-if-upgrade-fails).
