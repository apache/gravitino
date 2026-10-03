# Support database schema migration via Kubernetes Job in Helm Chart

## Status

Proposed design for issue #11099. Implementation is planned separately.

## Summary

When Gravitino is deployed or upgraded via the Helm chart, there is currently no
safe built-in mechanism for applying database schema migrations. Today, migrations
run as init containers inside the Gravitino Deployment. During a rolling update with
multiple replicas, several pods can start at once and run the migration concurrently,
which can race and corrupt data or fail the migration.

This design introduces a Kubernetes `Job` in the Helm chart that applies database
schema upgrades. The Job is annotated with Helm lifecycle hooks so it runs before
application pods are updated, guaranteeing the migration runs exactly once per
install/upgrade and that the rolling update only begins after the migration has
completed successfully.

## Current behavior

Schema migration is performed by init containers in `templates/deployment.yaml`:

1. `sqlfile` — copies `/opt/gravitino/scripts/*` (schema/upgrade SQL files) from the
   Gravitino image into a shared `emptyDir`, and writes the server version derived from
   `gravitino-server-*.jar` into `version.txt`.
2. `init-mysql` (rendered only when `mysql.enabled`) — waits for the bundled MySQL to be
   reachable, then applies the newest applicable `upgrade-*-to-*-mysql.sql` (best-effort,
   logs a warning on failure) followed by the newest applicable `schema-*-mysql.sql`.
3. `init-postgresql` (rendered only when `postgresql.enabled`) — the PostgreSQL analogue.

The upgrade file is applied best-effort because the upgrade scripts are not idempotent
(`ALTER TABLE` etc.), while the schema files use `CREATE TABLE IF NOT EXISTS` and are
idempotent. The `init-mysql`/`init-postgresql` containers connect to the bundled
database using its subchart service DNS name (`<release>-mysql`, `<release>-postgresql`)
and its root/admin credentials from the subchart secret.

External (pre-existing) MySQL/PostgreSQL databases are not migrated by the chart today;
`docs/chart.md` instructs users to run `schema-0.*.0-*.sql` manually before deploying.

## Key constraint: Helm hook ordering vs. bundled databases

Helm `pre-install` hooks execute *after* templates are rendered but *before any
resources are created in Kubernetes*. All resources — including resources of subcharts
such as the bundled MySQL/PostgreSQL — are created only after the `pre-install` hook
Job has completed successfully.

Therefore a `pre-install` hook Job that waits for a bundled subchart database to be
reachable would **deadlock on a fresh install**: the Job would wait for a Service that
Helm has not yet created. This is a well-known Helm limitation (also encountered by
OpenFGA, Airflow, and others).

## Design

Hybrid approach:

- **Bundled MySQL/PostgreSQL** (`mysql.enabled` or `postgresql.enabled`): keep the
  existing init containers unchanged. They run inside the Deployment, wait for the
  database, and apply the same migration logic. No hook Job is rendered. This avoids
  the pre-install deadlock because the bundled database is deployed by the same Helm
  release as the application pods.
- **External/existing MySQL or PostgreSQL** (neither bundled database enabled, and
  `entity.jdbcUrl` starts with `jdbc:mysql://` or `jdbc:postgresql://`): render a
  migration `Job` annotated `pre-install,pre-upgrade`. The database already exists and
  is reachable independently of the Helm release, so a pre-install/pre-upgrade hook Job
  is safe and migrates the schema before the Deployment is created/updated.

### 1. New values (`values.yaml`)

```yaml
## Schema migration Job configuration.
## Only rendered for external (pre-existing) MySQL/PostgreSQL databases configured via
## entity.jdbcUrl. Bundled in-chart databases keep using init containers for migration.
## ref: https://kubernetes.io/docs/concepts/workloads/controllers/job/
schemaMigration:
  ## @param schemaMigration.enabled Whether to run schema migration as a Kubernetes Job
  ## before install/upgrade for external MySQL/PostgreSQL databases
  ##
  enabled: true
  ## @param schemaMigration.backoffLimit Number of retries before the migration Job is
  ## considered failed
  ##
  backoffLimit: 4
  ## @param schemaMigration.activeDeadlineSeconds Hard cap on the total Job duration in
  ## seconds. Leave empty to disable.
  ##
  activeDeadlineSeconds: ""
  ## @param schemaMigration.ttlSecondsAfterFinished TTL in seconds before a finished
  ## migration Job is cleaned up. Leave empty to keep the Job after completion.
  ##
  ttlSecondsAfterFinished: ""
```

The Job reuses existing values for its pods/containers:

- `initContainerSecurityContext` for both the `sqlfile` init container and the
  `migrate` main container.
- `initResources` for both containers.
- `global.imagePullSecrets`, `serviceAccountName`, `image` (Gravitino image for the
  `sqlfile` init container), and `mysql.image` / `postgresql.image` for the DB client
  main container.

### 2. New template `templates/schema-migration-job.yaml`

Rendered only when all of these hold:

- `schemaMigration.enabled` is true.
- `mysql.enabled` is false and `postgresql.enabled` is false.
- `entity.jdbcUrl` has prefix `jdbc:mysql://` (MySQL) or `jdbc:postgresql://`
  (PostgreSQL).

Resource:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ include "gravitino.fullname" . }}-schema-migration
  namespace: {{ include "gravitino.namespace" . }}
  labels:
    {{- include "gravitino.labels" . | nindent 4 }}
  annotations:
    "helm.sh/hook": pre-install,pre-upgrade
    "helm.sh/hook-weight": "1"
    "helm.sh/hook-delete-policy": before-hook-creation
spec:
  backoffLimit: {{ .Values.schemaMigration.backoffLimit }}
  {{- if .Values.schemaMigration.activeDeadlineSeconds }}
  activeDeadlineSeconds: {{ .Values.schemaMigration.activeDeadlineSeconds }}
  {{- end }}
  {{- if .Values.schemaMigration.ttlSecondsAfterFinished }}
  ttlSecondsAfterFinished: {{ .Values.schemaMigration.ttlSecondsAfterFinished }}
  {{- end }}
  template:
    spec:
      restartPolicy: Never
      serviceAccountName: {{ .Values.serviceAccountName }}
      initContainers:
        - name: sqlfile
          image: {{ include "gravitino.image" . }}
          # copies /opt/gravitino/scripts/* and writes version.txt (same as Deployment)
      containers:
        - name: migrate
          # mysql or postgresql client image selected by jdbcUrl prefix
          # env: JDBC_URL / JDBC_USER / JDBC_PASSWORD from entity.*
          # shell: parse host/port/db from JDBC_URL, wait for DB, apply upgrade + schema
      volumes:
        - name: scripts-emptydir
          emptyDir: {}
```

Details:

- `restartPolicy: Never` is required for Jobs.
- The `sqlfile` init container copies the schema SQL files and writes `version.txt`
  into a shared `emptyDir`, mirroring the Deployment's `sqlfile` init container exactly.
- The `migrate` main container uses the MySQL client image (`mysql.image`) or the
  PostgreSQL client image (`postgresql.image`) based on the `entity.jdbcUrl` prefix.
  It receives `JDBC_URL`, `JDBC_USER`, and `JDBC_PASSWORD` from the `entity.*` values.
  Its shell script parses the host, port, and database name from the JDBC URL, waits
  for the database to be reachable, then applies the newest applicable upgrade file
  (best-effort, warning on failure) followed by the newest applicable schema file —
  identical selection and ordering semantics to the current `init-mysql` /
  `init-postgresql` containers.
- Hook annotations follow the issue's proposal: `pre-install,pre-upgrade`, weight `1`,
  and `before-hook-creation` delete policy. `before-hook-creation` removes any prior
  hook Job before creating a new one, so the hook Job is (re)created and run on every
  install/upgrade, and a failed Job remains for debugging until the next run.

### 3. `templates/deployment.yaml`

Unchanged. The existing `sqlfile`, `init-mysql`, and `init-postgresql` init containers
and the `scripts-emptydir` volume stay exactly as they are for bundled databases.

### 4. Tests (`tests/schema-migration-job_test.yaml`)

helm-unittest suite covering:

- Job is rendered for external MySQL (`entity.jdbcUrl: jdbc:mysql://...`) with the
  expected hook annotations (`helm.sh/hook`, `helm.sh/hook-weight`,
  `helm.sh/hook-delete-policy`), `kind: Job`, `spec.backoffLimit`, and
  `restartPolicy: Never`.
- Job is rendered for external PostgreSQL (`entity.jdbcUrl: jdbc:postgresql://...`).
- `sqlfile` init container uses the Gravitino image; `migrate` container uses the
  mysql/postgresql client image; both carry the reused security context and resources.
- Job is not rendered when a bundled database is enabled (`mysql.enabled: true`).
- Job is not rendered for the default embedded H2 configuration.
- Job is not rendered when `schemaMigration.enabled: false`.

Existing tests in `tests/deployment_test.yaml` remain valid because the Deployment is
unchanged.

### 5. Docs

Update `docs/chart.md`:

- New subsection under "Deploy Gravitino Using an Existed MySQL Database" describing
  automatic schema migration via the Kubernetes Job for external MySQL/PostgreSQL,
  including the `schemaMigration.*` values and the migration ordering guarantee.
- Note that bundled in-chart databases keep using init containers.

## Error handling

Matches the current init-container semantics:

- If the database is unreachable after the wait loop, the Job fails and Helm aborts the
  install/upgrade.
- If the upgrade SQL fails, a warning is logged and the Job continues with the schema
  SQL (non-idempotent upgrade scripts are best-effort).
- If the schema SQL fails, the Job fails and Helm aborts the install/upgrade. The failed
  Job is kept for debugging (`before-hook-creation` deletes it only on the next run).

## Scope / non-goals

- Bundled MySQL/PostgreSQL keep using init containers; no behavior change.
- External databases other than MySQL/PostgreSQL (e.g., H2) are not migrated by the Job.
- No changes to the schema SQL files themselves.

## Files changed

- `dev/charts/gravitino/values.yaml` — add `schemaMigration` block.
- `dev/charts/gravitino/templates/schema-migration-job.yaml` — new template.
- `dev/charts/gravitino/tests/schema-migration-job_test.yaml` — new test suite.
- `docs/chart.md` — document the feature.

## Verification

- `helm lint dev/charts/gravitino`
- `helm unittest --with-subchart=false dev/charts/gravitino`
- chart-testing install on a kind cluster for the existing scenarios
  (`ct install --charts dev/charts/gravitino`), which already cover default, MySQL, and
  PostgreSQL configurations.