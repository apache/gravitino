---
title: "Configuration"
slug: "/table-maintenance-service/optimizer-configuration"
keywords:
  - table maintenance
  - configuration
license: "This software is licensed under the Apache License version 2."
---

Three layers of configuration apply, and they are set in different places for different lifetimes. Server configuration governs how jobs run at all, CLI configuration governs how the commands reach Gravitino, and `jobConf` governs a single job submission.

| Layer          | Where it lives                  | Lifetime             |
| -------------- | ------------------------------- | -------------------- |
| Server         | `gravitino.conf`                | Until server restart |
| CLI            | `conf/gravitino-optimizer.conf` | Per command          |
| Job submission | `jobConf` in the request body   | One job run          |

## Server Configuration

Set these in `gravitino.conf`. They control the job executor rather than maintenance itself, so they apply to every job Gravitino runs.

```properties
gravitino.job.executor=local
gravitino.job.statusPullIntervalInMs=300000
gravitino.jobExecutor.local.sparkHome=/path/to/spark
```

These settings run the jobs with the [local job executor](../local-job-executor.md). To run them on
Kubernetes, set `gravitino.job.executor=k8s` and configure the
[Kubernetes job executor](../k8s-job-executor.md) instead. The examples on this page are for the
local job executor: on Kubernetes, leave `spark_master` empty, as the Kubernetes job executor
rejects `local[2]`. See
[Built-In Jobs](../k8s-job-executor.md#built-in-jobs).

`gravitino.job.statusPullIntervalInMs` defaults to five minutes. Job status is polled rather than pushed, so REST status can lag the real Spark process by a full interval, which makes a working job look hung. Lower it to `10000` for local work and restart the server.

### TMS cluster-static client auth

For in-process Table Maintenance Service (TMS), client credentials are **cluster-static**: set them under
the prefixes below in `gravitino.conf`. Do not store passwords, OAuth secrets, or keytab paths in policy
content. Values may be plaintext or secret-manager references when the deployment supports them.

When building the schedule-time job snapshot, `gravitino.maintenance.gravitinoAuth.*` overlays
policy `updaterOptions` (serialized into `updater_options` for Spark → Gravitino callbacks), and
`gravitino.maintenance.ircAuth.*` overlays policy `jobOptions`. Auth wins on duplicate keys.
Non-template IRC keys are then packed into `spark_conf` as `spark.sql.catalog.<catalog>.*` so
Spark Iceberg REST catalogs can authenticate.

Cron also injects callback identity into `updater_options`:

- `metalake` — always from the policy namespace (policy `updaterOptions.metalake` cannot redirect
  sample/finish to another metalake)
- `gravitino_uri` — from `gravitino.maintenance.gravitinoAuth.uri` when the policy does not set
  `updaterOptions.gravitino_uri`. A policy `updaterOptions.gravitino_uri` overrides the cluster
  value. Submit fails closed if neither source provides a URI.

```properties
gravitino.maintenance.gravitinoAuth.uri = http://localhost:8090
```

### TMS commit-job (IRC update → chained Spark)

When Iceberg REST runs as an auxiliary service in the same JVM, register the TMS listener so a
successful IRC table update upserts a one-shot `commit-job` / `{table_id}` (bursts merge and
keep the first snapshot). **At schedule**, TMS collects nearest policies with
`commitTrigger=true` in order compaction → manifest-rewrite → snapshot-expiry, drops members
still inside `minIntervalMs`, **fails closed when effective `gravitinoAuth` / `ircAuth` bags
disagree across the chain**, and writes a schedule-time job snapshot (`task_data`: tables,
operations, merged `jobConf`). The commit pool **consumes that snapshot**: if any snapshot
type is still in-flight it deletes the row and stops; otherwise it claims a soft slot for
**every** chain policy and submits one chained `builtin-iceberg-maintenance-task` Spark job.

```properties
gravitino.server.rest.extensionPackages = org.apache.gravitino.maintenance.web.rest.feature
gravitino.auxService.names = iceberg-rest
gravitino.eventListener.names = post-commit-maintenance-listener
gravitino.eventListener.post-commit-maintenance-listener.class = org.apache.gravitino.maintenance.listener.PostCommitMaintenanceListener
```

Without the listener, the crontab (`timeTrigger` → expand → cron-job) path still works; commit-triggered
maintenance does not.

#### Gravitino client auth (`gravitino.maintenance.gravitinoAuth.`) — fixed schema

`gravitino.conf` uses **camelCase** suffixes. At load time they are remapped to built-in
`--updater-options` / `GravitinoAuthSettings` short names (`auth_type`, `oauth_server_uri`, …)
before overlay onto policy `updaterOptions`. Only the suffixes in the table below are accepted;
unknown or blank suffixes fail fast when the server loads config. Supported `authType` values are
exactly four: **`none`**, **`simple`**, **`basic`**, **`oauth`**. When any Gravitino auth key is
set, `authType` is required, and `basic` / `oauth` also require their mode-specific fields below.

| Conf suffix       | `updaterOptions` key | Used when `authType` is | Meaning                                                |
| ----------------- | -------------------- | ----------------------- | ------------------------------------------------------ |
| `uri`             | `gravitino_uri`      | any, including unset    | Gravitino callback base URI. Optional on its own.      |
| `authType`        | `auth_type`          | all modes               | `none`, `simple`, `basic`, or `oauth`                  |
| `username`        | `username`           | `simple` / `basic`      | User principal (`simple` optional; `basic` required)   |
| `password`        | `password`           | `basic`                 | Password or secret reference (required)                |
| `oauthServerUri`  | `oauth_server_uri`   | `oauth`                 | OAuth2 token server base URI (required)                |
| `oauthPath`       | `oauth_path`         | `oauth`                 | Token path (for example `oauth2/token`) (required)     |
| `oauthCredential` | `oauth_credential`   | `oauth`                 | Client credential, often `client_id:secret` (required) |
| `oauthScope`      | `oauth_scope`        | `oauth`                 | Token scope (IdP-specific) (required)                  |

**1. `none`** — Gravitino authorization disabled; omit the prefix or set:

```properties
gravitino.maintenance.gravitinoAuth.authType = none
```

**2. `simple`**:

```properties
gravitino.maintenance.gravitinoAuth.authType = simple
gravitino.maintenance.gravitinoAuth.username = {username}
```

**3. `basic`**:

```properties
gravitino.maintenance.gravitinoAuth.authType = basic
gravitino.maintenance.gravitinoAuth.username = {username}
gravitino.maintenance.gravitinoAuth.password = {password}
```

**4. `oauth`**:

```properties
gravitino.maintenance.gravitinoAuth.authType = oauth
gravitino.maintenance.gravitinoAuth.oauthServerUri = https://idp.example.com/realms/demo
gravitino.maintenance.gravitinoAuth.oauthPath = protocol/openid-connect/token
gravitino.maintenance.gravitinoAuth.oauthCredential = {client_id}:{client_secret}
gravitino.maintenance.gravitinoAuth.oauthScope = openid profile email
```

#### Iceberg REST / Spark auth (`gravitino.maintenance.ircAuth.`) — arbitrary passthrough

Open prefix for Spark jobs that talk to Iceberg REST: any non-empty suffix under the prefix is
stripped and overlaid onto `jobOptions`. Prefer Iceberg catalog properties (`rest.auth.*`, and
related OAuth keys such as `credential` / `oauth2-server-uri` / `scope`).

Gravitino Iceberg REST (IRC) shares `gravitino.authenticators` with the Gravitino server
(`simple`, `basic`, `oauth`, `kerberos`), but **IRC does not support Kerberos** (see
[How to authenticate](../security/how-to-authenticate.md#kerberos-mode)). For Spark/Flink clients,
map the IRC authenticator to Iceberg `rest.auth.type` as below. Iceberg also defines `sigv4` and
`google`; those are **not** supported when the target is Gravitino IRC.

| Iceberg `rest.auth.type` | Gravitino IRC     | Notes                                                       |
| ------------------------ | ----------------- | ----------------------------------------------------------- |
| `none`                   | Supported         | IRC `simple` / auth disabled; omit the prefix or set `none` |
| `basic`                  | Supported         | IRC `basic` (local users)                                   |
| `oauth2`                 | Supported         | IRC `oauth`                                                 |
| `sigv4`                  | Not supported yet | Iceberg AWS signing; not a Gravitino IRC authenticator      |
| `google`                 | Not supported yet | Iceberg Google REST auth; not a Gravitino IRC authenticator |
| *(Kerberos)*             | Not supported yet | Documented: IRC does not support Kerberos                   |

**1. `none`** — IRC `simple` / no REST auth (default if the prefix is omitted):

```properties
gravitino.maintenance.ircAuth.rest.auth.type = none
```

**2. `basic`** — IRC `basic`:

```properties
gravitino.maintenance.ircAuth.rest.auth.type = basic
gravitino.maintenance.ircAuth.rest.auth.basic.username = {username}
gravitino.maintenance.ircAuth.rest.auth.basic.password = {password}
```

**3. `oauth2`** — IRC `oauth`, bearer token:

```properties
gravitino.maintenance.ircAuth.rest.auth.type = oauth2
gravitino.maintenance.ircAuth.rest.auth.oauth2.token = <your-token>
```

`oauth2` with client credentials (Iceberg catalog property names, not Gravitino updater short names):

```properties
gravitino.maintenance.ircAuth.rest.auth.type = oauth2
gravitino.maintenance.ircAuth.credential = {client_id}:{client_secret}
gravitino.maintenance.ircAuth.oauth2-server-uri = https://idp.example.com/realms/demo/protocol/openid-connect/token
gravitino.maintenance.ircAuth.scope = catalog
```

**4. `sigv4`** — **not supported yet** for Gravitino IRC.

**5. `google`** — **not supported yet** for Gravitino IRC.

## CLI Configuration

The CLI needs to know where Gravitino is and which components to use. This is a minimal working file for `submit-strategy-jobs`.

```properties
gravitino.optimizer.gravitinoUri = http://localhost:8090
gravitino.optimizer.gravitinoMetalake = test
gravitino.optimizer.gravitinoDefaultCatalog = rest_catalog
gravitino.optimizer.recommender.statisticsProvider = gravitino-statistics-provider
gravitino.optimizer.recommender.strategyProvider = gravitino-strategy-provider
gravitino.optimizer.recommender.tableMetaProvider = gravitino-table-metadata-provider
gravitino.optimizer.recommender.jobSubmitter = gravitino-job-submitter
gravitino.optimizer.strategyHandler.iceberg-data-compaction.className = org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionStrategyHandler
gravitino.optimizer.strategyHandler.iceberg-rewrite-manifests.className = org.apache.gravitino.maintenance.optimizer.recommender.handler.ManifestRewriteStrategyHandler
gravitino.optimizer.strategyHandler.iceberg-snapshot-expiration.className = org.apache.gravitino.maintenance.optimizer.recommender.handler.snapshot.SnapshotExpirationStrategyHandler
gravitino.optimizer.strategyHandler.iceberg-orphan-file-removal.className = org.apache.gravitino.maintenance.optimizer.recommender.handler.orphan.OrphanFileRemovalStrategyHandler
gravitino.optimizer.jobSubmitterConfig.catalog_name = rest_catalog
gravitino.optimizer.jobSubmitterConfig.spark_master = local[2]
gravitino.optimizer.jobSubmitterConfig.spark_executor_instances = 1
gravitino.optimizer.jobSubmitterConfig.spark_executor_cores = 1
gravitino.optimizer.jobSubmitterConfig.spark_executor_memory = 1g
gravitino.optimizer.jobSubmitterConfig.spark_driver_memory = 1g
gravitino.optimizer.jobSubmitterConfig.catalog_type = rest
gravitino.optimizer.jobSubmitterConfig.catalog_uri = http://localhost:9001/iceberg
# Local filesystem: leave empty. Iceberg REST: Gravitino catalog name (not s3://...).
gravitino.optimizer.jobSubmitterConfig.warehouse_location =
gravitino.optimizer.jobSubmitterConfig.spark_conf = {"spark.hadoop.fs.defaultFS":"file:///"}
```

### Gravitino client auth (CLI and update-stats)

One auth model backs both the CLI Gravitino client (`submit-strategy-jobs` and related providers)
and `builtin-iceberg-update-stats` / `submit-update-stats-job`. Supported types are **`none`**,
**`simple`**, **`basic`**, and **`oauth`** (client-credentials only). Put the same fields in either
place:

| Type             | CLI conf (`gravitino-optimizer.conf`)                                                                                                                                                                   | Short name in `updater_options` / `--updater-options`                            |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `none` (default) | omit keys, or `gravitino.optimizer.auth.type = none`                                                                                                                                                    | omit keys, or `auth_type=none`                                                   |
| `simple`         | `gravitino.optimizer.auth.type`, optional `gravitino.optimizer.auth.username`                                                                                                                           | `auth_type`, optional `username`                                                 |
| `basic`          | `gravitino.optimizer.auth.type`, `gravitino.optimizer.auth.username`, `gravitino.optimizer.auth.password`                                                                                               | `auth_type`, `username`, `password`                                              |
| `oauth`          | `gravitino.optimizer.auth.type`, `gravitino.optimizer.auth.oauth.serverUri`, `gravitino.optimizer.auth.oauth.path`, `gravitino.optimizer.auth.oauth.credential`, `gravitino.optimizer.auth.oauth.scope` | `auth_type`, `oauth_server_uri`, `oauth_path`, `oauth_credential`, `oauth_scope` |

Short names are promoted onto the `gravitino.optimizer.auth.*` keys before the client is built.
Use CLI conf when the optimizer process talks to a secured server; use short names in
`updater_options` when the update-stats Spark job (or `submit-update-stats-job` `runJob`) must
authenticate. They are two surfaces for the same fields, not two auth systems.

Iceberg REST catalog authentication is separate: set `rest.auth.*` in `spark-conf` for any built-in
job that talks to a secured IRC (including update-stats). Expire-snapshots and rewrite-data-files
do not read these Gravitino client auth fields. For Iceberg REST on object storage, set
`warehouse_location` to the catalog name and put vended credentials (plus IRC `rest.auth.*` when
required) in `spark_conf`. See
[Secured Gravitino and Iceberg REST](./optimizer.md#secured-gravitino-and-iceberg-rest-object-storage).

Passwords and OAuth credentials in `updater_options` (and secrets in `spark_conf`) travel with
the job command line and `jobConf`; avoid logging raw `jobConf` (the submit-update-stats CLI
redacts them in DRY-RUN / SUBMIT output).

Everything under `gravitino.optimizer.jobSubmitterConfig.` becomes the `jobConf` of jobs this CLI submits, so the two layers carry the same keys under different names.

The policy-driven path needs the same catalog Spark keys. Strategy adapters derive
task-specific fields (table identifier, where clause, retention options, and so on) from the
policy, so `catalog_name`, `catalog_type`, `catalog_uri`, and `warehouse_location` must come from
`gravitino.optimizer.jobSubmitterConfig.`. Missing ones are reported when the job is submitted,
instead of failing later inside Spark.

## Job Submission Configuration

A direct job submission carries its own `jobConf`. This is `builtin-iceberg-update-stats` with the keys it needs.

```json
{
  "catalog_name": "rest_catalog",
  "table_identifier": "db.t1",
  "update_mode": "all",
  "updater_options": "{\"gravitino_uri\":\"http://localhost:8090\",\"metalake\":\"test\",\"statistics_updater\":\"gravitino-statistics-updater\",\"metrics_updater\":\"gravitino-metrics-updater\",\"auth_type\":\"basic\",\"username\":\"admin\",\"password\":\"YourSecureGravitinoPassword\"}",
  "spark_conf": "{\"spark.hadoop.fs.defaultFS\":\"file:///\"}",
  "spark_master": "local[2]",
  "spark_executor_instances": "1",
  "spark_executor_cores": "1",
  "spark_executor_memory": "1g",
  "spark_driver_memory": "1g",
  "catalog_type": "rest",
  "catalog_uri": "http://localhost:9001/iceberg",
  "warehouse_location": ""
}
```

`updater_options` and `spark_conf` are JSON strings inside a JSON object, so their quotes are escaped. That nesting is the most common source of malformed submissions.

Only some of these keys are required. The built-in templates give the optional ones a default, so
`jobConf` can leave them out; a submission that misses a required key is rejected with an error
that lists the missing keys. See
[Built-in Job Templates](./optimizer-cli-reference.md#built-in-job-templates) for the required keys
and the defaults of each template.

Built-in Iceberg templates also need an Iceberg Spark runtime on the Spark classpath. They do not
ship that JAR or fill template `jars`, so include it yourself — for example by installing it into
`SPARK_HOME/jars`, or, with the k8s job executor, by
`"spark.jars":"https://.../iceberg-spark-runtime-....jar"` inside `spark_conf`, which replaces a
`spark.jars` default of the k8s job executor, so list every jar the job needs there. With the local job
executor, `spark.jars` inside `spark_conf` doesn't add it, as the driver applies `spark_conf` only
after it starts. Match the artifact to
your Spark, Scala, and Iceberg versions. Details are under
[Built-in Job Templates](./optimizer-cli-reference.md#built-in-job-templates).

`warehouse_location` may be empty for local filesystem testing. For Iceberg REST catalogs, set it
to the **Gravitino catalog name** (for example `rest_catalog` or `catalog_iceberg_s3`), not an
`s3://` or other object-store URI — that URI form yields `NoSuchWarehouse`. For non-REST backends
that expect a storage root, use the warehouse URI those backends document.

## Compaction (rewrite data files)

Submit `builtin-iceberg-rewrite-data-files` through `POST /api/metalakes/{metalake}/jobs/runs` or
through `submit-strategy-jobs`. Direct `runJob` must include `strategy`, `sort_order`, and
`where_clause` in `jobConf` even when empty (`""` is accepted). Omitting them returns HTTP 400.
Policy-driven submits fill these through the compaction adapter.

| Key                | Meaning                                           | Default   |
| ------------------ | ------------------------------------------------- | --------- |
| `catalog_name`     | Iceberg catalog registered in Spark               | Required  |
| `table_identifier` | Table identifier, for example `db.t1`             | Required  |
| `where_clause`     | Filter predicate; `""` rewrites the whole table   | Required  |
| `strategy`         | `binpack` or `sort`                               | `binpack` |
| `sort_order`       | Sort order when using `sort`; empty for `binpack` | empty     |
| `options`          | JSON map of Iceberg rewrite options               | empty     |
| `spark_conf`       | JSON string containing additional Spark settings  | None      |

Include the shared Spark and catalog template settings. Register the handler (optional when using
the Recommender default):

```properties
gravitino.optimizer.strategyHandler.iceberg-data-compaction.className = org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionStrategyHandler
```

Create a policy through `POST /api/metalakes/{metalake}/policies`:

```json
{
  "name": "iceberg_compaction_default",
  "policyType": "system_iceberg_compaction",
  "enabled": true,
  "content": {}
}
```

Associate the policy with a tag and attach that tag to the target table, schema, or catalog.
Threshold fields (`minDataFileMse`, `minDeleteFileNumber`, `rewriteStrategy`, and related options)
are documented in [Iceberg Compaction Policy](../iceberg-compaction-policy.md). Use
`submit-strategy-jobs --strategy-name iceberg_compaction_default`. Submission and verification
examples are under [Rewrite Data Files](./optimizer-cli-reference.md#rewrite-data-files).

## Rewrite Manifests

Submit `builtin-iceberg-rewrite-manifests` through `POST /api/metalakes/{metalake}/jobs/runs`.

| Key                | Meaning                                               | Default                                       |
| ------------------ | ----------------------------------------------------- | --------------------------------------------- |
| `catalog_name`     | Iceberg catalog registered in Spark                   | Required                                      |
| `table_identifier` | Table identifier, for example `db.t1`                 | Required                                      |
| `spec_id`          | Existing partition spec whose manifests to rewrite    | Current table spec                            |
| `use_caching`      | `true` or `false` to control caching during rewriting | Installed Iceberg default (`false` in 1.11.0) |
| `spark_conf`       | JSON string containing additional Spark settings      | None                                          |

`spec_id` must be a non-negative integer identifying an existing spec; it does not repartition data
or migrate manifests between specs. Omitted, blank, or unresolved optional placeholders are treated
as absent. Include the shared Spark and catalog template settings.

Register the handler (optional; the Recommender defaults this class when unset):

```properties
gravitino.optimizer.strategyHandler.iceberg-rewrite-manifests.className = org.apache.gravitino.maintenance.optimizer.recommender.handler.ManifestRewriteStrategyHandler
```

Policy content and per-spec thresholds are documented in
[Iceberg manifest rewrite policy](../iceberg-rewrite-manifests-policy.md). Policy -> Strategy ->
Adapter automatic selection is a follow-up; `submit-strategy-jobs` does not select this job
automatically yet. Direct submission and verification examples are under
[Rewrite Manifests](./optimizer-cli-reference.md#rewrite-manifests).

## Snapshot Expiration

Submit `builtin-iceberg-expire-snapshots` with the shared Spark and catalog keys plus:

| Key                | Meaning                                          | Default       |
| ------------------ | ------------------------------------------------ | ------------- |
| `catalog_name`     | Iceberg catalog registered in Spark              | Required      |
| `table_identifier` | Table identifier, for example `db.t1`            | Required      |
| `older_than`       | Expire snapshots older than this timestamp       | Five days ago |
| `retain_last`      | Minimum number of recent snapshots to keep       | `1`           |
| `stream_results`   | Stream intermediate delete results when present  | Disabled      |
| `spark_conf`       | JSON string containing additional Spark settings | None          |

Register the handler (optional; the Recommender defaults this class when unset):

```properties
gravitino.optimizer.strategyHandler.iceberg-snapshot-expiration.className = org.apache.gravitino.maintenance.optimizer.recommender.handler.snapshot.SnapshotExpirationStrategyHandler
```

The built-in job adapter is registered automatically. It supplies `table_identifier` and
`stream_results`. For `older_than` / `retain_last`, values equal to Iceberg's procedure defaults
(`olderThanDays=5`, `retainLast=1`) are omitted so template placeholders stay empty and Iceberg
table `history.expire.*` properties remain authoritative; non-default values are emitted explicitly.

Create a policy through `POST /api/metalakes/{metalake}/policies`:

```json
{
  "name": "expire_snapshots",
  "policyType": "system_iceberg_snapshot_expiration",
  "enabled": true,
  "content": {
    "olderThanDays": 5,
    "retainLast": 1,
    "streamResults": false
  }
}
```

Associate the policy with a tag and attach that tag to the target table, schema, or catalog.
`olderThanDays` defaults to 5 and must be between 1 and 36500. `retainLast` defaults to 1.
`streamResults` defaults to false. When `olderThanDays` is not 5, the adapter subtracts it from the
current instant and emits `older_than` as an explicit UTC timestamp ending in `Z`. Each optimizer
invocation may submit expire when the policy is attached; this does not install a periodic
scheduler. See [Expire Snapshots](./optimizer-cli-reference.md#expire-snapshots).

## Orphan Cleanup

Submit `builtin-iceberg-remove-orphan-files` with the shared Spark and catalog keys plus:

| Key                | Meaning                                                                               | Default                          |
| ------------------ | ------------------------------------------------------------------------------------- | -------------------------------- |
| `catalog_name`     | Iceberg catalog registered in Spark                                                   | Required                         |
| `table_identifier` | Table identifier within the catalog, such as `db.sample`                              | Required                         |
| `older_than`       | Timestamp; explicit values must be at least 24 hours old                              | Three days ago (Iceberg default) |
| `location`         | Scan only this directory within the table's storage location                          | Table location                   |
| `dry_run`          | `true` logs candidate paths without deleting; `false` deletes                         | `false`                          |
| `spark_conf`       | JSON string containing custom Spark settings, including the Iceberg runtime if needed | None                             |

For direct template submission, supply `older_than: ""` and `location: ""` to use the defaults,
`dry_run: "false"` (or `"true"` to preview), and `spark_conf: "{}"` when no overrides are needed.
Keep the three-day default unless your workload needs a longer retention window. The 24-hour
minimum also applies to dry runs.

Register the handler (optional; the Recommender defaults this class when unset):

```properties
gravitino.optimizer.strategyHandler.iceberg-orphan-file-removal.className = org.apache.gravitino.maintenance.optimizer.recommender.handler.orphan.OrphanFileRemovalStrategyHandler
```

The adapter supplies `table_identifier`, `older_than`, `location`, and `dry_run`. Create a policy:

```json
{
  "name": "orphan_cleanup",
  "policyType": "system_iceberg_orphan_file_removal",
  "enabled": true,
  "content": {
    "olderThanDays": 3,
    "dryRun": true,
    "timeTrigger": "0 0 3 * * ?",
    "minIntervalMs": 86400000
  }
}
```

Optional TMS peer fields on content (`timeTrigger`, `commitTrigger`, `minIntervalMs`, `jobOptions`,
`updaterOptions`) are shared with other maintenance policy types. `commitTrigger` is not allowed for
orphan cleanup. See [Iceberg compaction policy](../iceberg-compaction-policy.md) for peer-field
semantics. `olderThanDays` defaults to 3 (1–36500). The adapter always emits `older_than` as an
explicit UTC timestamp ending in `Z`.

The strategy operates on the whole table and does not need table or partition statistics. Every
explicit `submit-strategy-jobs` invocation for an enabled, selected orphan policy submits a Spark
job (no metric gate). Start with `dryRun: true`. Use
`submit-strategy-jobs --strategy-name orphan_cleanup`. See
[Remove Orphan Files](./optimizer-cli-reference.md#remove-orphan-files).

## Running Against a Local Filesystem

On a machine with no HDFS, Spark still defaults to `hdfs://localhost:9000` and fails. Set the default filesystem explicitly, in `spark_conf` for job submissions and in the CLI `spark_conf` value:

```properties
spark.hadoop.fs.defaultFS=file:///
```

## Checking Your Configuration

Confirm these before assuming a configuration problem is a code problem.

- The built-in templates you need appear in the job template list (`update-stats`, rewrite data
  files, rewrite manifests, expire snapshots, remove orphan files).
- For policy-driven runs, the policy is associated with a tag assigned to the target table or one of
  its ancestors.
- `submit-strategy-jobs` prints `SUBMIT` lines rather than nothing when the policy should fire.
- Compaction: rewrite log shows `Rewritten data files: N` with `N > 0` when enough small files exist
  (`min-input-files`, default 5). `N = 0` is usually a data-shape issue.
- Manifest rewrite: log shows rewritten/added manifest counts (both zero can be a successful no-op).
- Snapshot expiry: log shows deleted data/manifest/manifest-list counts.
- Orphan cleanup: dry-run log lists candidate paths before you switch `dry_run` to `false`.

## Related

- [Service](./service.md) for the in-process service, `timeTrigger`, and the cron path
- [Table Maintenance Service](./optimizer.md) for the CLI walkthrough
- [CLI Reference](./optimizer-cli-reference.md) for every command and the built-in job templates
- [Troubleshooting](./optimizer-troubleshooting.md) when a command or job fails
- [Extension Guide](./optimizer-extension-guide.md) for custom strategies and providers
