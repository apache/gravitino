---
title: "Using the Table Maintenance Service"
slug: "/table-maintenance-service/using-table-maintenance"
keywords:
  - table maintenance
  - scheduler
  - policy
  - iceberg
license: "This software is licensed under the Apache License version 2."
---

## Overview

The table maintenance service (TMS) runs inside the Gravitino server. A maintenance policy is associated with a tag, and that tag is assigned to a catalog, schema, or table. The scheduler selects the nearest policy of each built-in type and submits a Spark job.

Two paths enqueue work:

- **Cron.** A policy `timeTrigger` cron expression becomes due. The expand pool lists matching tables, imports a schema or table that is not yet in the Gravitino store, and submits a cron job.
- **Commit.** With the Iceberg REST auxiliary service and the post-commit listener enabled, a successful table update enqueues one chained job for the nearest policies that set `commitTrigger` to `true`.

TMS does not replace the optimizer CLI. Use this page to turn the scheduler on. Use [Table Maintenance Service](./optimizer.md) to run the same built-in jobs by hand.

Passwords, OAuth secrets, and keytab paths belong in `gravitino.conf`, not in policy content.

## Quick Start

The steps below turn on a compaction schedule for one Iceberg table. `timeTrigger` is a six-field Quartz cron (`second minute hour day-of-month month day-of-week`), not a five-field Unix cron. They assume a metalake `test`, an Iceberg catalog `rest_catalog`, and a table `rest_catalog.db.t1` that already exists. Replace those names, and send `Accept: application/vnd.gravitino.v1+json` on every request.

**1. Configure the server.** Add the following to `gravitino.conf` and restart Gravitino. `gravitino.maintenance.gravitinoAuth.uri` is the base URI Spark jobs use to call back into Gravitino. Set `authType` to match the server authenticator. The `basic` example below is the local user store.

```properties
gravitino.server.rest.extensionPackages = org.apache.gravitino.maintenance.web.rest.feature
gravitino.job.executor = local
gravitino.jobExecutor.local.sparkHome = /path/to/spark
gravitino.maintenance.gravitinoAuth.uri = http://localhost:8090
gravitino.maintenance.gravitinoAuth.authType = basic
gravitino.maintenance.gravitinoAuth.username = {username}
gravitino.maintenance.gravitinoAuth.password = {password}
```

**2. Create a compaction policy with a cron trigger.**

```shell
curl -s -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "nightly_compaction",
    "policyType": "system_iceberg_compaction",
    "comment": "Compact small Iceberg files",
    "enabled": true,
    "content": {
      "timeTrigger": "0 */5 * * * ?",
      "commitTrigger": false,
      "minIntervalMs": 3600000
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
  -d '{"tagsToAdd": [{"name": "maintained"}]}' \
  http://localhost:8090/api/metalakes/test/objects/table/rest_catalog.db.t1/tags
```

**4. Confirm the derived policy, then wait for the next cron tick.**

```shell
curl -s -H "Accept: application/vnd.gravitino.v1+json" \
  "http://localhost:8090/api/metalakes/test/objects/table/rest_catalog.db.t1/policies?details=true"
```

After the cron is due, list job runs and look for template `builtin-iceberg-maintenance-task`. See [Manage jobs](../manage-jobs-in-gravitino.md#list-job-runs).

## Configuration

Set these in `gravitino.conf`. The full key list, including OAuth and Iceberg REST auth, is in [Configuration](./optimizer-configuration.md#tms-cluster-static-client-auth).

| Configuration item | Description |
| --- | --- |
| `gravitino.server.rest.extensionPackages` | Register the maintenance REST callbacks. Set to `org.apache.gravitino.maintenance.web.rest.feature`. |
| `gravitino.maintenance.gravitinoAuth.uri` | Base URI for Spark callbacks. Required unless the policy sets `updaterOptions.gravitino_uri`. |
| `gravitino.maintenance.gravitinoAuth.authType` | `none`, `simple`, `basic`, or `oauth`. Required when any other `gravitinoAuth` key besides `uri` is set. |
| `gravitino.maintenance.gravitinoAuth.username` | User for `simple` or `basic`. |
| `gravitino.maintenance.gravitinoAuth.password` | Password for `basic`. |
| `gravitino.maintenance.ircAuth.*` | Iceberg REST properties copied into the Spark job. See [Iceberg REST auth](./optimizer-configuration.md#iceberg-rest--spark-auth-gravitinomaintenanceircauth--arbitrary-passthrough). |
| `gravitino.job.executor` | `local` or `k8s`. See [Local job executor](../local-job-executor.md) and [Kubernetes job executor](../k8s-job-executor.md). |

`uri` may be set without `authType`. A policy `updaterOptions.gravitino_uri` overrides `gravitino.maintenance.gravitinoAuth.uri`. Every other `gravitinoAuth` key wins over the same key in the policy.

Commit-triggered maintenance also needs the Iceberg REST auxiliary service and the listener:

```properties
gravitino.auxService.names = iceberg-rest
gravitino.eventListener.names = post-commit-maintenance-listener
gravitino.eventListener.post-commit-maintenance-listener.class = org.apache.gravitino.maintenance.listener.PostCommitMaintenanceListener
```

Without the listener, cron scheduling still runs. Commit-triggered maintenance does not.

Scheduler pool sizes have these defaults:

| Configuration item | Default |
| --- | --- |
| `gravitino.maintenance.scheduler.expandThreads` | `4` |
| `gravitino.maintenance.scheduler.cronThreads` | `8` |
| `gravitino.maintenance.scheduler.commitThreads` | `4` |

## Defining a Maintenance Run

Three objects decide what runs.

| Object | What to set |
| --- | --- |
| Policy | `policyType`, `enabled`, and content fields `timeTrigger` (six-field Quartz cron), `commitTrigger`, and `minIntervalMs`. |
| Tag association | `POST /api/metalakes/{metalake}/tags/{tag}/policies/{policy}` with selector `ALL_VALUES` or `TAG_VALUE`. |
| Tag assignment | Assign the tag to the table, or to a catalog or schema ancestor. |

The scheduler keeps a table only when this policy is the nearest one of its type. A table tag beats a schema tag, and a schema tag beats a catalog tag. Two policies of the same type on the same object are a conflict, and that table is skipped.

`minIntervalMs` is the cooldown after a finished run. When the policy omits it, the server uses the per-type default in [Table maintenance cooldowns](../gravitino-server-config.md#table-maintenance-cooldowns).

Built-in content types:

| `policyType` | Cron (`timeTrigger`) | Commit (`commitTrigger`) |
| --- | --- | --- |
| `system_iceberg_compaction` | Yes | Yes |
| `system_iceberg_rewrite_manifests` | Yes | Yes |
| `system_iceberg_snapshot_expiration` | Yes | Yes |
| `system_iceberg_orphan_file_removal` | Yes | No |

Field lists for each type are in [Iceberg compaction policy](../iceberg-compaction-policy.md), [Iceberg rewrite manifests policy](../iceberg-rewrite-manifests-policy.md), and [Manage policies](../manage-policies-in-gravitino.md).

The submitted template is `builtin-iceberg-maintenance-task`. TMS fills `action_ids` when it builds the job. Do not put `action_ids` in policy `jobOptions`.

## Running Paths

### Cron path

1. The expand pool wakes when `timeTrigger` is due.
2. It lists schemas and tables from the catalog. A missing Gravitino schema or table is imported by `loadSchema` or `loadTable`.
3. It drops tables that lost the nearest-policy check, that are already in flight, or that are still inside `minIntervalMs`.
4. The cron pool submits one `builtin-iceberg-maintenance-task` Spark job.
5. The job calls the maintenance REST callbacks below.

### Commit path

1. An Iceberg REST table update reaches `PostCommitMaintenanceListener`.
2. TMS loads the nearest policies with `commitTrigger` set to `true`, in order: compaction, manifest rewrite, then snapshot expiry.
3. A policy still inside `minIntervalMs` is dropped from the chain.
4. If the effective `gravitinoAuth` or `ircAuth` bags disagree across the remaining policies, the chain is not submitted.
5. Otherwise one chained `builtin-iceberg-maintenance-task` job runs every remaining policy.

### Job callbacks

These endpoints are for the Spark job, not for operators. Schemas are in the [maintenance OpenAPI definition](../open-api/maintenance/maintenance.yaml).

| Callback | Method and path |
| --- | --- |
| Before-maintenance metrics | `POST /api/metalakes/{metalake}/maintenance/actions/{jobId}/metrics/before` |
| Finish and release the slot | `POST /api/metalakes/{metalake}/maintenance/actions/{jobId}/finish` |
| After-maintenance metrics | `POST /api/metalakes/{metalake}/maintenance/actions/{jobId}/metrics/after` |

## Connecting Spark

TMS submits jobs through the Gravitino job executor. The job reads two bags that were snapshotted at enqueue time:

- `gravitinoAuth` becomes `updater_options`, including `gravitino_uri` and `metalake`. `metalake` always comes from the policy namespace.
- `ircAuth` becomes Iceberg REST catalog properties under `spark.sql.catalog.<catalog>.*`.

Point `gravitino.maintenance.gravitinoAuth.uri` at the Gravitino HTTP endpoint the executors can reach. Point `gravitino.maintenance.ircAuth` properties at the Iceberg REST endpoint, usually `http://{host}:9001/iceberg` when IRC runs in the same process.

For a local Spark install, set `gravitino.job.executor=local` and `gravitino.jobExecutor.local.sparkHome`. On Kubernetes, set `gravitino.job.executor=k8s` and leave `spark_master` empty. See [Built-in jobs](../k8s-job-executor.md#built-in-jobs).

## Further Reading

- [Table Maintenance Service](./optimizer.md) for the manual CLI workflow
- [Configuration](./optimizer-configuration.md) for every TMS and CLI key
- [CLI reference](./optimizer-cli-reference.md)
- [Troubleshooting](./optimizer-troubleshooting.md)
- [Extension guide](./optimizer-extension-guide.md)
- [Maintenance OpenAPI definition](../open-api/maintenance/maintenance.yaml)
