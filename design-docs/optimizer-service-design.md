<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied. See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Design: Optimizer Service for Apache Gravitino

---

## Background

The Table Maintenance Service (Optimizer) is currently an alpha feature in Apache Gravitino. The
optimizer package provides CLI commands that connect statistics and metrics collection, rule
evaluation, strategy recommendation, and job submission.

The current optimizer execution model is local-process oriented:

| Command                   | Current execution style                                       | Main responsibility                                     |
| ------------------------- | ------------------------------------------------------------- | ------------------------------------------------------- |
| `update-statistics`       | CLI loads optimizer config and runs updater logic locally     | Calculate and persist table or partition statistics     |
| `append-metrics`          | CLI loads optimizer config and runs updater logic locally     | Calculate and append table, partition, or job metrics   |
| `submit-strategy-jobs`    | CLI loads optimizer config and runs recommender logic locally | Evaluate policies and optionally submit jobs            |
| `monitor-metrics`         | CLI loads optimizer config and runs monitor logic locally     | Evaluate before/after metrics around an action time     |
| `list-table-metrics`      | CLI queries metrics storage locally                           | Query stored table or partition metrics                 |
| `list-job-metrics`        | CLI queries metrics storage locally                           | Query stored job metrics                                |
| `submit-update-stats-job` | CLI submits Gravitino jobs                                    | Submit built-in Iceberg update stats/metrics Spark jobs |

This model is useful for local validation and batch scripts, but it has operational limitations:

1. The CLI is both user interface and execution runtime. This makes it hard to centralize service
   configuration, request validation, audit logging, and service-level metrics.
2. Automation systems must shell out to the CLI instead of calling a service API with structured
   request and response payloads.
3. Every CLI invocation creates its own runtime and provider instances.
4. The current CLI has no optimizer-owned background execution model. Commands run synchronously in
   the calling process. When Spark maintenance work is needed, the CLI submits a Gravitino job and
   returns the Gravitino `jobId`; job status remains owned by the Gravitino job framework.

The current architecture is:

```text
User or automation
      |
      v
gravitino-optimizer.sh
      |
      +--> OptimizerCmd
             |
             +--> Updater / Recommender / Monitor / Metrics query
             |
             +--> Gravitino server REST APIs and optional metrics storage
```

This design introduces a long-running Optimizer Service and a separate service client CLI while
preserving the existing CLI as the local execution tool.

---

## Goals

1. **Service Mode for Optimizer Workloads**: Users can run a long-running optimizer service and call
   optimizer operations through REST APIs.
2. **CLI Compatibility**: Existing optimizer CLI commands and options continue to work for local
   execution.
3. **Separate Service Client CLI**: A new client CLI calls the Optimizer Service with command options
   aligned to the existing local CLI.
4. **Synchronous API Semantics**: The MVP service follows current CLI behavior. Each request executes
   in the HTTP request path and returns the same kind of result the CLI prints today.
5. **Structured Responses**: REST responses return typed summaries, recommendations, metrics, monitor
   evaluations, submitted Gravitino job IDs, or sanitized errors.
6. **Job Framework Compatibility**: Spark maintenance work continues to use the existing Gravitino job
   framework. The Optimizer Service returns submitted `jobId` values but does not track job status.
7. **Incremental Migration**: The design can be implemented in phases without requiring all optimizer
   commands to be added to the service client in one release.

---

## Non-Goals

1. **Provider SPI Rewrite**: This design does not replace `StatisticsUpdater`, `MetricsUpdater`,
   `StatisticsCalculator`, `StatisticsProvider`, `StrategyProvider`, `TableMetadataProvider`,
   `JobSubmitter`, `MetricsProvider`, `MetricsEvaluator`, or `MonitorCallback`. The service calls
   the existing provider contracts.
2. **Strategy Algorithm Changes**: This design does not change how policies are evaluated or how
   recommendations are ranked. It only changes the execution boundary.
3. **Statistics and Metrics Model Changes**: This design does not change Gravitino statistics APIs,
   metrics storage schemas, metric names, or JSON Lines input semantics.
4. **Optimizer Task Model in MVP**: This design does not introduce optimizer task IDs, task status,
   task cancellation, task listing, task retention, or persistent optimizer task storage.
5. **Gravitino Job Manager Replacement**: This design does not create a second job management system.
   Built-in maintenance jobs continue to be submitted to the Gravitino job framework.
6. **Immediate Removal of Local CLI Mode**: Local CLI execution remains supported for compatibility,
   local debugging, and environments that do not deploy the service.
7. **Request-Scoped Tenant Routing in MVP**: The MVP does not accept tenant or metalake information
   in request paths. Each Optimizer Service instance uses its configured Gravitino context.

---

## Solution Investigations

### Option A: Keep CLI-only execution

Continue running all optimizer work inside the CLI process, with no service process.

**Pros:** No new server process. Minimal implementation work. Preserves current behavior.

**Cons:** Does not provide structured REST APIs, centralized service configuration, shared
observability, or a stable endpoint for automation.

**Decision:** Rejected because it does not address the automation and operational problems.

### Option B: Embed optimizer execution in the Gravitino server

Run the optimizer execution logic inside the existing Gravitino metadata server process.

**Pros:** Reuses the existing server process, REST stack, auth, and deployment path. Avoids another
daemon.

**Cons:** Couples maintenance workloads to the metadata server. Optimizer providers, metrics storage
drivers, and job-submission dependencies can affect metadata API latency and server stability. The
optimizer package already has a separate distribution and configuration model.

**Decision:** Rejected for MVP because optimizer workloads should be isolated from the metadata
control plane.

### Option C: Use only the existing Gravitino job framework for every optimizer action

Model every optimizer command as a Gravitino job.

**Pros:** Provides persisted job status and existing server APIs for job execution.

**Cons:** Most optimizer commands are not jobs. `update-statistics`, `append-metrics`,
`monitor-metrics`, and metrics queries need immediate result payloads. Forcing all optimizer actions
into jobs would blur responsibilities.

**Decision:** Rejected because the job framework should remain the execution backend for submitted
jobs, not the universal optimizer control API.

### Option D: Add an independent synchronous Optimizer Service (Chosen)

Introduce a standalone Optimizer Service in the optimizer distribution, with the CLI as one client.
The service exposes synchronous REST endpoints that map to existing CLI commands.

**Pros:** Keeps optimizer workloads isolated, preserves the optimizer distribution boundary, supports
structured automation, and avoids adding a second task or job status model.

**Cons:** Adds a daemon, service configuration, REST resources, and request timeout/concurrency
handling.

**Decision:** **Chosen** because it solves the target automation problem while matching current CLI
semantics and respecting existing Gravitino boundaries.

---

## Proposal

### Architecture

Introduce an independent Optimizer Service in the optimizer distribution:

```text
User or automation
      |
      +--------------------------+-----------------------------+
      |                          |                             |
      v                          v                             v
gravitino-optimizer.sh  gravitino-optimizer-client.sh     REST client
      |                          |                             |
      v                          +--------------+--------------+
 Existing optimizer classes                     |
                                                v
                                      Optimizer Service
                                                |
        +-------------------+-------------------+-------------------+-------------------+-------------------+
        |                   |                   |                   |                   |                   |
        v                   v                   v                   v                   v
 UpdateStatisticsHandler  StrategyJobsHandler  MonitorHandler  MetricsQueryHandler  SubmitUpdateJobHandler
        |                   |                   |                   |                   |
        +-------------------+-------------------+-------------------+-------------------+
                                                |
                                                v
             Existing optimizer classes, provider SPIs, Gravitino jobs, and metrics storage
```

The service runs as a separate process from the Gravitino server. It uses optimizer configuration to
load existing providers and exposes REST APIs under one optimizer-specific prefix:

```text
/api/optimizer/v1
```

The prefix keeps optimizer APIs separate from Gravitino metadata APIs and leaves room for future
service-level authentication, routing, and documentation.

The MVP service uses its configured Gravitino context for every request. The REST path does not
expose tenant or metalake information.

### Internal Structure

| Part                        | Responsibility                                                                                                      |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `OptimizerServiceServer`    | Starts the HTTP server, registers REST resources, initializes handlers, exposes lifecycle hooks, and owns shutdown. |
| `UpdateStatisticsHandler`   | Converts update-statistics and append-metrics requests into calls to `Updater`.                                     |
| `StrategyJobsHandler`       | Converts submit-strategy-jobs requests into calls to `Recommender`.                                                |
| `MonitorMetricsHandler`     | Converts monitor-metrics requests into calls to `Monitor`.                                                         |
| `MetricsQueryHandler`       | Serves table, partition, and job metrics query APIs using existing metrics storage/provider logic.                  |
| `SubmitUpdateJobHandler`    | Converts submit-update-stats-job requests into Gravitino `runJob` calls and returns submitted job IDs.             |
| `OptimizerServiceClient`    | Client used by the service client CLI.                                                                            |

### REST API

The API follows the same conventions as other Gravitino REST APIs:

- All endpoints exchange JSON using the `application/vnd.gravitino.v1+json` media type.
- Every response body carries an integer `code` (`0` for success) wrapping the payload.
- Endpoints are grouped by optimizer domain (`statistics`, `metrics`, `recommendations`, `monitors`,
  and `jobs`) instead of exposing CLI command names directly in the path.
- `metrics/jobs` means metrics scoped by Gravitino job identifier. `jobs/update-stats` is reserved
  for submitting built-in Gravitino update-stats jobs.
- Endpoints are synchronous. The service returns only after the underlying optimizer operation
  finishes or fails.
- Errors use the Gravitino `ErrorModel` (`code`, `type`, `message`, `stack`). The `message` must be
  sanitized and must not expose secrets from config files, job options, or provider exceptions.

#### POST /api/optimizer/v1/statistics/update

Runs the same logic as CLI `update-statistics`.

**Request:**

| Field               | Type          | Required | Description                                                                           |
| ------------------- | ------------- | -------- | ------------------------------------------------------------------------------------- |
| `calculatorName`    | string        | yes      | Statistics calculator name, for example `local-stats-calculator`.                     |
| `identifiers`       | array[string] | no       | Table identifiers. Empty means the selected calculator/provider decides scope.        |
| `statisticsPayload` | string        | no       | Inline JSON Lines payload. Mutually exclusive with `filePath`.                        |
| `filePath`          | string        | no       | Server-local JSON Lines path allowed only under configured prefixes. Mutually exclusive with `statisticsPayload`. |

**Response:** `200 OK`

```json
{
  "code": 0,
  "summary": {
    "updateType": "STATISTICS",
    "totalRecords": 1,
    "tableRecords": 1,
    "partitionRecords": 0,
    "jobRecords": 0
  }
}
```

**Behavior:**

The service validates the same command rules as the CLI. For `local-stats-calculator`, either
`statisticsPayload` or `filePath` is required. `statisticsPayload` and `filePath` cannot both be set.
The service client should translate client-local `--file-path` into `statisticsPayload`; REST
`filePath` remains a server-side operator feature guarded by allowed path prefixes.

#### POST /api/optimizer/v1/metrics/append

Runs the same logic as CLI `append-metrics`.

**Request:**

| Field               | Type          | Required | Description                                                                           |
| ------------------- | ------------- | -------- | ------------------------------------------------------------------------------------- |
| `calculatorName`    | string        | yes      | Statistics calculator name, for example `local-stats-calculator`.                     |
| `identifiers`       | array[string] | no       | Table or job identifiers. Empty means the selected calculator/provider decides scope. |
| `statisticsPayload` | string        | no       | Inline JSON Lines payload. Mutually exclusive with `filePath`.                        |
| `filePath`          | string        | no       | Server-local JSON Lines path allowed only under configured prefixes. Mutually exclusive with `statisticsPayload`. |

**Response:** `200 OK`

```json
{
  "code": 0,
  "summary": {
    "updateType": "METRICS",
    "totalRecords": 3,
    "tableRecords": 1,
    "partitionRecords": 1,
    "jobRecords": 1
  }
}
```

#### POST /api/optimizer/v1/recommendations/submit

Runs the same logic as CLI `submit-strategy-jobs`.

**Request:**

| Field          | Type          | Required | Description                                                          |
| -------------- | ------------- | -------- | -------------------------------------------------------------------- |
| `strategyName` | string        | yes      | Policy name to evaluate, matching CLI `--strategy-name`.             |
| `identifiers`  | array[string] | yes      | Table identifiers.                                                   |
| `dryRun`       | boolean       | no       | Preview recommendations without submitting jobs. Default is `false`. |
| `limit`        | integer       | no       | Maximum number of recommendations or submissions to process.         |

**Response:** `200 OK`

```json
{
  "code": 0,
  "dryRun": false,
  "results": [
    {
      "strategyName": "iceberg_compaction_default",
      "identifier": "rest_catalog.db.t1",
      "score": 100,
      "jobTemplate": "builtin-iceberg-rewrite-data-files",
      "jobOptions": {
        "catalog_name": "rest_catalog",
        "table_identifier": "db.t1"
      },
      "jobId": "job-123"
    }
  ]
}
```

**Behavior:**

The service validates identifiers, strategy name, and positive `limit` values. Dry-run requests
return recommendation details with empty `jobId` values. Non-dry-run requests submit jobs through the
configured `JobSubmitter`; when that submitter creates Gravitino jobs, returned `jobId` values are
owned and tracked by the Gravitino job framework.

#### POST /api/optimizer/v1/monitors/metrics

Runs the same logic as CLI `monitor-metrics`.

**Request:**

| Field           | Type          | Required | Description                                                                 |
| --------------- | ------------- | -------- | --------------------------------------------------------------------------- |
| `identifiers`   | array[string] | yes      | Table identifiers to evaluate.                                              |
| `actionTime`    | integer       | yes      | Action timestamp in epoch seconds.                                          |
| `rangeSeconds`  | integer       | no       | Evaluation window. Default is the current CLI default, 86400 seconds.       |
| `partitionPath` | array[object] | no       | Partition path. Allowed only when exactly one table identifier is provided. |

**Response:** `200 OK`

```json
{
  "code": 0,
  "evaluations": [
    {
      "identifier": "rest_catalog.db.t1",
      "scope": "table",
      "partitionPath": null,
      "evaluator": "gravitino-metrics-evaluator",
      "passed": true
    }
  ]
}
```

#### GET /api/optimizer/v1/metrics/tables

Runs the same logic as CLI `list-table-metrics`.

**Request query parameters:**

| Field           | Type   | Required | Description                                                 |
| --------------- | ------ | -------- | ----------------------------------------------------------- |
| `identifiers`   | string | yes      | Comma-separated table identifiers.                          |
| `partitionPath` | string | no       | Partition path JSON array. Requires exactly one identifier. |

**Response:** `200 OK`

```json
{
  "code": 0,
  "metrics": [
    {
      "identifier": "rest_catalog.db.t1",
      "scope": "table",
      "partitionPath": null,
      "points": [
        {
          "name": "row_count",
          "value": 100,
          "timestamp": 1735689600
        }
      ]
    }
  ]
}
```

#### GET /api/optimizer/v1/metrics/jobs

Runs the same logic as CLI `list-job-metrics`.

**Request query parameters:**

| Field         | Type   | Required | Description                      |
| ------------- | ------ | -------- | -------------------------------- |
| `identifiers` | string | yes      | Comma-separated job identifiers. |

**Response:** `200 OK`

```json
{
  "code": 0,
  "metrics": [
    {
      "identifier": "job_1",
      "scope": "job",
      "points": [
        {
          "name": "duration_ms",
          "value": 12500,
          "timestamp": 1735689800
        }
      ]
    }
  ]
}
```

#### POST /api/optimizer/v1/jobs/update-stats

Runs the same logic as CLI `submit-update-stats-job`.

**Request:**

| Field            | Type          | Required | Description                                                                  |
| ---------------- | ------------- | -------- | ---------------------------------------------------------------------------- |
| `identifiers`    | array[string] | yes      | Table identifiers.                                                           |
| `dryRun`         | boolean       | no       | Preview job configs without submitting jobs. Default is `false`.             |
| `updateMode`     | string        | no       | `stats`, `metrics`, or `all`. Default is `all`.                              |
| `updaterOptions` | object        | no       | Flat option map passed to updater logic.                                     |
| `sparkConf`      | object        | no       | Flat Spark and Iceberg catalog config map used by the submitted job.         |

**Response:** `200 OK`

```json
{
  "code": 0,
  "dryRun": false,
  "submitted": [
    {
      "identifier": "rest_catalog.db.t1",
      "jobTemplate": "builtin-iceberg-update-stats",
      "jobId": "job-456",
      "jobConfig": {
        "catalog_name": "rest_catalog",
        "table_identifier": "db.t1",
        "update_mode": "all"
      }
    }
  ],
  "summary": {
    "total": 1,
    "submitted": 1
  }
}
```

**Behavior:**

The service only submits Gravitino jobs and returns job IDs. Job execution status, cancellation, and
history remain owned by the Gravitino job framework.

#### GET /api/optimizer/v1/health

Returns service health. This is an operational endpoint and is the one response that is not wrapped in
the `code` envelope, so external liveness probes can consume it directly.

**Response:** `200 OK`

```json
{
  "status": "UP"
}
```

### Service Configuration

The service reads the same `conf/gravitino-optimizer.conf` file as the CLI, plus service-side keys
that control the HTTP endpoint:

| Key                                                   | Default     | Description                                                         |
| ----------------------------------------------------- | ----------- | ------------------------------------------------------------------- |
| `gravitino.optimizer.server.host`                     | `127.0.0.1` | Bind address. Use `0.0.0.0` only for explicitly remote deployments. |
| `gravitino.optimizer.server.port`                     | `8091`      | Listen port for the Optimizer Service HTTP server.                  |
| `gravitino.optimizer.server.requestTimeoutMs`         | `600000`    | Maximum time for one synchronous service request.                   |
| `gravitino.optimizer.server.allowedInputPathPrefixes` | none        | Allowed server-local prefixes for REST `filePath`; empty disables `filePath`. |

### Service Client CLI

Keep `gravitino-optimizer.sh` as the local execution CLI. Add a separate service client script, for
example `gravitino-optimizer-client.sh`, for remote execution through the Optimizer Service. The
client reuses the current command names, option names, validation rules, and output style where
possible, but it does not instantiate local providers or run optimizer logic.

Add service client configuration keys:

| Key                                                   | Default | Description                                                             |
| ----------------------------------------------------- | ------- | ----------------------------------------------------------------------- |
| `gravitino.optimizer.client.serviceUrl`               | none    | Base URL of the Optimizer Service, for example `http://localhost:8091`. |
| `gravitino.optimizer.client.requestTimeoutMs`         | `30000` | HTTP request timeout for service calls.                                 |

The two CLIs have separate execution backends:

| CLI script                       | Execution backend                                      |
| -------------------------------- | ------------------------------------------------------ |
| `gravitino-optimizer.sh`         | Local optimizer execution using configured providers.  |
| `gravitino-optimizer-client.sh`  | Remote execution through Optimizer Service REST APIs.  |

The service client maps commands to REST endpoints:

| CLI command               | Service client behavior                                    |
| ------------------------- | ---------------------------------------------------------- |
| `update-statistics`       | Calls `POST /api/optimizer/v1/statistics/update`.          |
| `append-metrics`          | Calls `POST /api/optimizer/v1/metrics/append`.             |
| `submit-strategy-jobs`    | Calls `POST /api/optimizer/v1/recommendations/submit`.     |
| `monitor-metrics`         | Calls `POST /api/optimizer/v1/monitors/metrics`.           |
| `list-table-metrics`      | Calls `GET /api/optimizer/v1/metrics/tables`.              |
| `list-job-metrics`        | Calls `GET /api/optimizer/v1/metrics/jobs`.                |
| `submit-update-stats-job` | Calls `POST /api/optimizer/v1/jobs/update-stats`.          |

If the service client call fails, the client should fail fast by default. Automatic fallback to local
execution is intentionally not supported because it can hide partial service outages and cause
duplicated submissions.

### User Process

The service client user flow is:

1. The operator configures `conf/gravitino-optimizer.conf` with Gravitino connection settings,
   optimizer providers, and service settings.
2. The operator starts the Optimizer Service with the optimizer distribution.
3. A user runs the service client CLI with familiar optimizer command options, such as:

   ```bash
   ./bin/gravitino-optimizer-client.sh \
     --type submit-strategy-jobs \
     --identifiers rest_catalog.db.t1 \
     --strategy-name iceberg_compaction_default \
     --dry-run \
     --limit 10
   ```

4. The client validates arguments, sends a synchronous REST request, and waits for the HTTP response.
5. The client prints output compatible with the existing local CLI command.
6. Automation can call the REST API directly and consume structured response payloads.

### Implementation Process

The internal flow for service requests is:

```text
CLI or REST client
      |
      v
REST resource validates request
      |
      v
Handler invokes existing optimizer class
      |
      +--> Updater / Recommender / Monitor / MetricsProvider / GravitinoClient
      |
      v
REST resource returns structured result or sanitized error
```

Implementation should keep handler boundaries thin. Existing `Updater`, `Recommender`, `Monitor`,
metrics provider logic, and Gravitino job submission remain the execution core. REST resources
translate requests into handler calls, and handlers translate optimizer results into REST result DTOs.

### Backward Compatibility

This design is backward compatible for existing CLI users:

1. `gravitino-optimizer.sh` remains the local execution CLI.
2. Existing command names and options remain valid.
3. Existing local output remains unchanged in `gravitino-optimizer.sh`.
4. The new service client is opt-in and should print output compatible with the existing command
   output.
5. Submitted Gravitino job IDs remain visible in service responses and service client output.

New REST APIs are additive. They do not change existing Gravitino metadata APIs or job APIs.

### Security

The service must validate request parameters with the same rules as the CLI. REST `filePath` is
server-local, disabled by default, and must resolve under `allowedInputPathPrefixes` when enabled.

Authentication and authorization can be phased:

1. MVP binds to loopback by default; remote deployments should add a trusted network boundary,
   reverse proxy, or service token before exposing mutating APIs.
2. Audit creator is the authenticated service principal, or `anonymous` only when auth is disabled.
3. Later phases can add Gravitino auth integration and request-scoped authorization.
4. Direct REST clients receive sanitized errors that do not expose config, job-option, or provider
   secrets.

### Reliability and Observability

The service should provide:

| Area            | Requirement                                                                                     |
| --------------- | ----------------------------------------------------------------------------------------------- |
| Request control | HTTP request timeout and optional max concurrent request limit.                                  |
| Shutdown        | Graceful shutdown that stops accepting new requests and waits for in-flight requests to finish. |
| Logs            | Request lifecycle logs with command name, request ID, result status, and duration.              |
| Metrics         | Request counts by command/status, latency, active request count, and failure counts.            |
| Health          | Health endpoint with optimizer service status.                                                  |

### Rollout Plan

#### Phase 1: Service Shell and Common DTOs

Create the service entry point, REST server, health endpoint, common request/response DTOs, sanitized
error handling, and request timeout configuration.

#### Phase 2: Updater APIs and Service Client CLI

Implement `update-statistics` and `append-metrics` REST APIs and service client support. Validate
parity with local execution, including client `--file-path` conversion and REST `filePath` allowlist
validation.

#### Phase 3: Recommender APIs and Service Client CLI

Implement `submit-strategy-jobs` REST API and service client support. Cover dry-run and real submission
paths, including returned Gravitino job IDs.

#### Phase 4: Monitor and Metrics Query APIs

Implement `monitor-metrics`, `list-table-metrics`, and `list-job-metrics` REST APIs and service client
mode.

#### Phase 5: Built-In Update Stats Job Submission

Implement `submit-update-stats-job` REST API and service client support. The service returns submitted
Gravitino job IDs; actual job execution and status remain owned by the Gravitino job framework.

#### Phase 6: Hardening and Documentation

Add authentication integration, service metrics export, and stronger graceful shutdown semantics.

---

## Task Breakdown

### Phase 1: Service Shell and Common DTOs

- [ ] Add optimizer service configuration keys to `OptimizerConfig`.
- [ ] Add `OptimizerServiceServer` entry point and startup script in the optimizer distribution.
- [ ] Add common request, response, summary, result, and error DTOs.
- [ ] Add `GET /api/optimizer/v1/health`.
- [ ] Add sanitized error handling and request timeout handling.
- [ ] Add unit tests for DTO serialization, validation errors, timeout handling, and sanitized
      errors.

### Phase 2: Updater APIs and Service Client CLI

- [ ] Add REST resources for `update-statistics` and `append-metrics`.
- [ ] Add `UpdateStatisticsHandler` that adapts REST requests to existing `Updater` execution.
- [ ] Add `OptimizerServiceClient` support for updater APIs.
- [ ] Add service client commands for `update-statistics`.
- [ ] Add service client commands for `append-metrics`.
- [ ] Add tests for updater validation, client `--file-path` conversion, REST `filePath` allowlist,
      successful summaries, and failure responses.

### Phase 3: Recommender APIs and Service Client CLI

- [ ] Add REST resource for `submit-strategy-jobs`.
- [ ] Add `StrategyJobsHandler` that adapts REST requests to existing `Recommender` execution.
- [ ] Add `OptimizerServiceClient` support for strategy job APIs.
- [ ] Add service client command for `submit-strategy-jobs`.
- [ ] Add tests for dry-run recommendations, limit handling, submitted job IDs, and service client
      output.

### Phase 4: Monitor and Metrics Query APIs

- [ ] Add REST resource for `monitor-metrics`.
- [ ] Add `MonitorMetricsHandler` that adapts REST requests to existing `Monitor` execution.
- [ ] Add metrics query REST resources for table, partition, and job metric queries.
- [ ] Add `OptimizerServiceClient` support for monitor and metrics query APIs.
- [ ] Add service client command for `monitor-metrics`.
- [ ] Add service client commands for `list-table-metrics` and `list-job-metrics`.
- [ ] Add tests for monitor validation, partition path handling, evaluation results, and metrics query
      responses.

### Phase 5: Built-In Update Stats Job Submission

- [ ] Add REST resource for `submit-update-stats-job`.
- [ ] Keep actual job creation in the existing Gravitino job framework.
- [ ] Add `OptimizerServiceClient` support for update-stats job submission.
- [ ] Add service client command for `submit-update-stats-job`.
- [ ] Add tests that service responses include submitted Gravitino job IDs.

### Phase 6: Hardening and Documentation

- [ ] Add service authentication and sanitized error handling.
- [ ] Add service metrics for request counts, latency, active requests, and failures.
- [ ] Add graceful shutdown tests.
- [ ] Update user-facing TMS documentation in `docs/table-maintenance-service/`.
- [ ] Add OpenAPI documentation if optimizer service APIs are published under `docs/open-api/`.
- [ ] Validate OpenAPI documentation with `./gradlew :docs:build` if OpenAPI files are changed.
