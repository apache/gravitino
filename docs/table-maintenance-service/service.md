---
title: "Service"
slug: "/table-maintenance-service/service"
keywords:
  - table maintenance
  - time trigger
  - cron
license: "This software is licensed under the Apache License version 2."
---

## Overview

The table maintenance service runs inside the Gravitino server. Associate a policy with a tag and assign that tag to a catalog, schema, or table. The service keeps the nearest policy of each built-in type and submits a Spark job. A table tag beats a schema tag, and a schema tag beats a catalog tag. Two policies of the same type on the same object are a conflict, and that table is skipped.

`timeTrigger` is a six-field Quartz cron (`second minute hour day-of-month month day-of-week`) on the policy. When it is due, the expand pool lists matching tables and inserts one cron-job per table. The cron pool then submits Spark. Operators do not create cron-job rows.

`commitTrigger=true` enables commit-triggered maintenance. A successful Iceberg REST table update enqueues one chained job and does not pass through the expand pool. See [Commit Trigger](#commit-trigger).

A policy may set **at most one** of a non-blank `timeTrigger` or `commitTrigger=true`. Create and alter reject both together. `minIntervalMs` is allowed only with `commitTrigger=true`.

Passwords, OAuth secrets, and keytab paths belong in `gravitino.conf`, not in policy content.

## Quick Start

The steps below turn on a compaction schedule for one Iceberg table. They assume a metalake `test`, an Iceberg catalog `rest_catalog`, and a table `rest_catalog.db.t1` that already exists. Replace those names, and send `Accept: application/vnd.gravitino.v1+json` on every request.

**1. Enable the service.** Register the maintenance REST callbacks in `gravitino.conf` and restart Gravitino. Client authentication and the job executor are in [Configuration](./optimizer-configuration.md#tms-cluster-static-client-auth).

```properties
gravitino.server.rest.extensionPackages = org.apache.gravitino.maintenance.web.rest.feature
```

**2. Create a compaction policy with a time trigger.**

```shell
curl -s -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "nightly_compaction",
    "policyType": "system_iceberg_compaction",
    "comment": "Compact small Iceberg files",
    "enabled": true,
    "content": {
      "timeTrigger": "0 */5 * * * ?"
    }
  }' \
  http://localhost:8090/api/metalakes/test/policies
```

**3. Associate the policy with a tag, then assign the tag to the table.**

```shell
curl -s -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{"name": "maintained", "comment": "Tables enrolled in TMS"}' \
  http://localhost:8090/api/metalakes/test/tags

curl -s -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{"selector": {"type": "ALL_VALUES"}}' \
  http://localhost:8090/api/metalakes/test/tags/maintained/policies/nightly_compaction

curl -s -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{"tagsToAdd": ["maintained"]}' \
  http://localhost:8090/api/metalakes/test/objects/table/rest_catalog.db.t1/tags
```

**4. Confirm the derived policy, then wait for the next time trigger.**

```shell
curl -s -H "Accept: application/vnd.gravitino.v1+json" \
  "http://localhost:8090/api/metalakes/test/objects/table/rest_catalog.db.t1/policies?details=true"
```

After the trigger is due, list job runs and look for template `builtin-iceberg-maintenance-task`. See [Manage jobs](../manage-jobs-in-gravitino.md#list-jobs-get-a-job-and-cancel).

## Configuration

Set the maintenance REST extension in `gravitino.conf`. Client authentication, Iceberg REST auth, and the job executor are in [Configuration](./optimizer-configuration.md#tms-cluster-static-client-auth).

| Configuration item                        | Description                                                                                          |
| ----------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `gravitino.server.rest.extensionPackages` | Register the maintenance REST callbacks. Set to `org.apache.gravitino.maintenance.web.rest.feature`. |

A policy `updaterOptions.gravitino_uri` overrides the cluster callback URI. Every other `gravitinoAuth` key from the cluster wins over the same key in the policy.

Scheduler pool sizes have these defaults:

| Configuration item                              | Default |
| ----------------------------------------------- | ------- |
| `gravitino.maintenance.scheduler.expandThreads` | `4`     |
| `gravitino.maintenance.scheduler.cronThreads`   | `8`     |
| `gravitino.maintenance.scheduler.commitThreads` | `4`     |

When `gravitino.authorization.enable=true`, set a TMS submit identity:

| Configuration item                   | Default | Description                                        |
| ------------------------------------ | ------- | -------------------------------------------------- |
| `gravitino.maintenance.jobSubmitter` | (none)  | USER principal TMS uses to submit maintenance jobs |

Create that user under every metalake that runs maintenance jobs. Grant the user metalake `RUN_JOB` and `USE_JOB_TEMPLATE` on the Iceberg maintenance job template so `runJob` can succeed; grant any further table or catalog privileges your maintenance Spark jobs need when they call back into Gravitino.

## Time Trigger

`timeTrigger` stores the cron string on the policy. `0 */5 * * * ?` runs at second 0 of every fifth minute. A five-field Unix cron is not accepted.

Three objects decide which tables run:

| Object          | What to set                                                                                              |
| --------------- | -------------------------------------------------------------------------------------------------------- |
| Policy          | `policyType`, `enabled`, and exactly one trigger: `timeTrigger` **or** `commitTrigger=true` (optional `minIntervalMs` only with commitTrigger=true). |
| Tag association | `POST /api/metalakes/{metalake}/tags/{tag}/policies/{policy}` with selector `ALL_VALUES` or `TAG_VALUE`. |
| Tag assignment  | Assign the tag to the table, or to a catalog or schema ancestor.                                         |

`minIntervalMs` is the cooldown after a finished run of that policy on that table. It is valid only when `commitTrigger` is `true`. When a `commitTrigger=true` policy omits it, the server uses the per-type default in [Table maintenance cooldowns](../gravitino-server-config.md#table-maintenance-cooldowns). Scheduled (`timeTrigger`) runs do not use `minIntervalMs`; overlap is blocked only by the in-flight check.

Built-in content types:

| `policyType`                         | Time trigger | Commit trigger |
| ------------------------------------ | ------------ | -------------- |
| `system_iceberg_compaction`          | Yes          | Yes            |
| `system_iceberg_rewrite_manifests`   | Yes          | Yes            |
| `system_iceberg_snapshot_expiration` | Yes          | Yes            |
| `system_iceberg_orphan_file_removal` | Yes          | No             |

Field lists for each type are in [Iceberg compaction policy](../iceberg-compaction-policy.md),
[Iceberg rewrite manifests policy](../iceberg-rewrite-manifests-policy.md),
[Iceberg snapshot expiration policy](../iceberg-snapshot-expiration-policy.md),
[Iceberg orphan file removal policy](../iceberg-orphan-file-removal-policy.md), and
[Manage policies](../manage-policies-in-gravitino.md).

## Cron Path

The cron path is what `timeTrigger` does after it is due.

1. The expand pool wakes on the policy cron.
2. It lists schemas and tables from the catalog. A missing Gravitino schema or table is imported by `loadSchema` or `loadTable`.
3. It drops tables that lost the nearest-policy check or that are already in flight.
4. It inserts one cron-job per remaining table. The cron pool submits one `builtin-iceberg-maintenance-task` Spark job for that row.
5. The job calls the maintenance REST callbacks below.

TMS fills `action_ids` when it builds the job. Do not put `action_ids` in policy `jobOptions`.

The job reads two bags that were snapshotted at enqueue time:

- `gravitinoAuth` becomes `updater_options`, including `gravitino_uri` and `metalake`. `metalake` always comes from the policy namespace.
- `ircAuth` becomes Iceberg REST catalog properties under `spark.sql.catalog.<catalog>.*`.

On Kubernetes, leave `spark_master` empty. See [Built-in jobs](../k8s-job-executor.md#built-in-jobs).

These endpoints are for the Spark job, not for operators. Schemas are in the [maintenance OpenAPI definition](../open-api/maintenance/maintenance.yaml). The commit path uses the same callbacks.

| Callback                    | Method and path                                                                |
| --------------------------- | ------------------------------------------------------------------------------ |
| Before-maintenance metrics  | `POST /api/metalakes/{metalake}/maintenance/actions/{actionId}/metrics/before` |
| After-maintenance metrics   | `POST /api/metalakes/{metalake}/maintenance/actions/{actionId}/metrics/after`  |
| Finish and release the slot | `POST /api/metalakes/{metalake}/maintenance/actions/{actionId}/finish`         |

### Validation before and after metrics

Cron and commit paths submit `builtin-iceberg-maintenance-task`, which posts before and after
metrics through the callbacks above. The UI can show Before, After, and Change (After minus
Before when both exist). Standalone procedure templates used by the CLI or optimizer
(`builtin-iceberg-rewrite-data-files`, `builtin-iceberg-rewrite-manifests`,
`builtin-iceberg-expire-snapshots`, `builtin-iceberg-remove-orphan-files`) do not report these
metrics.

#### Compaction (`system_iceberg_compaction`)

Before and after inventory comes from Spark SQL on the Iceberg `files` metadata table.
`custom-data-mse-ratio` is computed in the job as
`custom-data-file-mse / minDataFileMse` (policy field). It is stored as a JSON number
(`double`) for display; `minDataFileMse` itself is not stored as a metric.

| Phase            | Key                         | Source                                      |
| ---------------- | --------------------------- | ------------------------------------------- |
| Before and After | `custom-data-file-mse`      | File inventory (also drives `trigger-expr`) |
| Before and After | `custom-delete-file-number` | File inventory                              |
| Before and After | `custom-avg-file-size`      | File inventory                              |
| Before and After | `custom-total-file-size`    | File inventory                              |
| Before and After | `custom-file-number`        | File inventory                              |
| Before and After | `custom-small-file-number`  | File inventory (files smaller than 32 MiB)  |
| Before and After | `custom-data-mse-ratio`     | `custom-data-file-mse / minDataFileMse`     |

#### Manifest rewrite (`system_iceberg_rewrite_manifests`)

Before and after inventory comes from the Iceberg Java API (`snapshot.allManifests`) for the
resolved partition spec, not from Spark SQL.

| Phase  | Key                                | Source                                                                          |
| ------ | ---------------------------------- | ------------------------------------------------------------------------------- |
| Before | `custom-manifest-number`           | Manifest inventory for the resolved partition spec (also drives `trigger-expr`) |
| Before | `custom-avg-manifest-size`         | Manifest inventory for the resolved partition spec                              |
| Before | `custom-manifest-spec-id`          | Spec id when the table is partitioned (diagnostic only)                         |
| Before | `custom-manifest-partition-spec`   | Partition fields when the table is partitioned (diagnostic)                     |
| After  | `custom-manifest-number`           | Manifest inventory re-collected after rewrite                                   |
| After  | `custom-avg-manifest-size`         | Manifest inventory re-collected after rewrite                                   |
| After  | `custom-rewritten-manifest-number` | Count returned by the Iceberg `rewrite_manifests` procedure                     |
| After  | `custom-manifest-spec-id`          | Spec id when the table is partitioned (diagnostic only)                         |
| After  | `custom-manifest-partition-spec`   | Partition fields when the table is partitioned (diagnostic)                     |

#### Snapshot expiration (`system_iceberg_snapshot_expiration`)

| Phase  | Key                              | Source                                                                   |
| ------ | -------------------------------- | ------------------------------------------------------------------------ |
| Before | `custom-snapshot-number`         | Snapshot count before expire                                             |
| After  | `custom-snapshot-number`         | Snapshot count after expire (re-counted, not before minus expired)       |
| After  | `custom-expired-snapshot-number` | Snapshots that existed before expire and are gone afterward              |
| After  | `custom-expired-file-size`       | Bytes of content files that existed before expire and are gone afterward |

#### Orphan file removal (`system_iceberg_orphan_file_removal`)

| Phase  | Key                          | Source                                       |
| ------ | ---------------------------- | -------------------------------------------- |
| Before | —                            | Not collected                                |
| After  | `custom-removed-file-number` | Removed-file count from the procedure result |
| After  | `custom-removed-file-size`   | Removed-file bytes measured with FileIO      |

## Commit Trigger

Set `commitTrigger` to `true` on a policy that should run after a successful Iceberg REST table update (and omit `timeTrigger`). Orphan file removal cannot set `commitTrigger`. Optional `minIntervalMs` applies only on the commitTrigger path.

Commit-triggered maintenance needs the Iceberg REST auxiliary service and the listener:

```properties
gravitino.auxService.names = iceberg-rest
gravitino.eventListener.names = post-commit-maintenance-listener
gravitino.eventListener.post-commit-maintenance-listener.class = org.apache.gravitino.maintenance.listener.PostCommitMaintenanceListener
```

Without the listener, time triggers still run. Commit-triggered maintenance does not.

1. An Iceberg REST table update reaches `PostCommitMaintenanceListener`.
2. TMS loads the nearest policies with `commitTrigger` set to `true`, in order: compaction, manifest rewrite, then snapshot expiry.
3. A policy still inside `minIntervalMs` is dropped from the chain.
4. If the effective `gravitinoAuth` or `ircAuth` bags disagree across the remaining policies, the chain is not submitted.
5. Otherwise one chained `builtin-iceberg-maintenance-task` job runs every remaining policy.

## Related

- [Configuration](./optimizer-configuration.md) for client authentication, Iceberg REST auth, and the job executor
- [Table Maintenance Service](./optimizer.md) for the manual CLI workflow
- [CLI Reference](./optimizer-cli-reference.md) for every command and the built-in job templates
- [Iceberg Compaction Policy](../iceberg-compaction-policy.md)
- [Iceberg Manifest Rewrite Policy](../iceberg-rewrite-manifests-policy.md)
- [Iceberg Snapshot Expiration Policy](../iceberg-snapshot-expiration-policy.md)
- [Iceberg Orphan File Removal Policy](../iceberg-orphan-file-removal-policy.md)
