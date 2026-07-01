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

1. Optimizer work is tied to the CLI process lifecycle. If the process exits, operators lose a
   consistent service-side task record for updater, recommender, and monitor work.
2. The CLI is both user interface and execution runtime. This makes it hard to centralize retries,
   concurrency control, audit logging, and service-level metrics.
3. Long-running work has no shared asynchronous task model. Users cannot submit a request, disconnect,
   and later query status through a stable API.
4. Horizontal scaling and resource isolation are difficult because each invocation creates its own
   runtime and provider instances.
5. Automation systems must shell out to the CLI instead of calling a service API with structured
   request and response payloads.

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

This design introduces a long-running Optimizer Service while preserving the existing CLI as a
client and local execution tool.

---

## Goals

1. **Service Mode for Optimizer Workloads**: Users can run a long-running optimizer service and
   submit updater, recommender, monitor, and metrics-query requests through REST APIs.
2. **Asynchronous Task Execution**: Mutating or long-running optimizer commands return a task ID
   immediately and expose status, result, and error details through query APIs.
3. **CLI Compatibility**: Existing optimizer CLI commands and options continue to work. A
   configuration switch controls whether supported commands execute locally or call the service.
4. **Shared Task Runtime**: Updater, recommender, and monitor modules use one task status model,
   task ID format, cancellation contract, and list/query behavior.
5. **Operational Controls**: The service exposes health checks, bounded queues, configurable worker
   pools, request validation, structured lifecycle logs, and metrics for task execution.
6. **Incremental Migration**: The design can be implemented in phases without requiring all optimizer
   commands to move to service mode in one release.
7. **Job Framework Compatibility**: `submit-update-stats-job` continues to use the existing Gravitino
   job framework for Spark job submission. The Optimizer Service does not replace the job manager.

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
4. **Gravitino Job Manager Replacement**: This design does not create a second job management system.
   Built-in maintenance jobs continue to be submitted to the Gravitino job framework.
5. **Immediate Removal of Local CLI Mode**: Local CLI execution remains supported for compatibility,
   local debugging, and environments that do not deploy the service.
6. **Full Multi-Node Coordination in MVP**: The first implementation may use an in-memory task store.
   Durable task storage and multi-node ownership are included as later hardening work.

---

## Solution Investigations

### Option A: Keep CLI-only execution

Continue running all optimizer work inside the CLI process, with no server component.

**Pros:** No new server process. Minimal implementation work. Preserves current behavior.

**Cons:** Does not solve async status tracking, centralized observability, retries, queueing, or
structured automation APIs. Each invocation remains an isolated runtime.

**Decision:** Rejected because it does not address the operational problems.

### Option B: Embed optimizer execution in the Gravitino server

Run the optimizer modules inside the existing Gravitino metadata server process.

**Pros:** Reuses the existing server process, REST stack, auth, and deployment path. Avoids another
daemon.

**Cons:** Couples maintenance workloads to the metadata server. Heavy optimizer tasks, provider
dependencies, and worker pools can affect metadata API latency and server stability. The optimizer
package already has a separate distribution and configuration model.

**Decision:** Rejected for MVP because optimizer workloads should be isolated from the metadata
control plane.

### Option C: Use only the existing Gravitino job framework for every optimizer action

Model every optimizer command (updater, recommender, monitor, metrics query) as a Gravitino job.

**Pros:** Provides persisted job status and existing server APIs for job execution.

**Cons:** Updater, recommender, monitor, and metrics-query commands are not all jobs. Some requests
need provider execution and immediate result payloads, while Spark job submission already has a
separate job manager. Forcing all optimizer actions into jobs would blur responsibilities.

**Decision:** Rejected because the job framework should remain the execution backend for submitted
jobs, not the universal optimizer control API.

### Option D: Add an independent Optimizer Service (Chosen)

Introduce a standalone Optimizer Service in the optimizer distribution, with the CLI as one client.

**Pros:** Keeps optimizer workloads isolated, preserves the optimizer distribution boundary, and
allows independent scaling and resource limits. The CLI can become a client without losing local mode.

**Cons:** Adds a daemon, service configuration, REST resources, and task runtime that must be operated
and tested.

**Decision:** **Chosen** because it solves the target operational issues while respecting existing
Gravitino boundaries.

---

## Proposal

### Architecture

Introduce an independent Optimizer Service in the optimizer distribution:

```text
User or automation
      |
      +-------------------------------+
      |                               |
      v                               v
gravitino-optimizer.sh          REST client
      |                               |
      +---------------+---------------+
                      |
                      v
             Optimizer Service
                      |
        +-------------+-------------+----------------+----------------+
        |                           |                |                |
        v                           v                v                v
  UpdaterModule              RecommenderModule  MonitorModule   MetricsQueryModule
        |                           |                |                |
        +-------------+-------------+----------------+----------------+
                      |
                      v
                 TaskRuntime
                      |
        +-------------+-------------+
        |                           |
        v                           v
 Existing provider SPIs      Gravitino server and metrics storage
```

The service runs as a separate process from the Gravitino server. It uses optimizer configuration to
load existing providers and exposes REST APIs under one optimizer-specific prefix:

```text
/api/optimizer/v1
```

The prefix keeps optimizer APIs separate from Gravitino metadata APIs and leaves room for future
service-level authentication, routing, and documentation.

### Components

| Component                | Responsibility                                                                                                                                  |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `OptimizerServiceServer` | Starts the HTTP server, registers REST resources, initializes modules, exposes lifecycle hooks, and owns graceful shutdown.                     |
| `TaskRuntime`            | Creates task IDs, stores task records, runs asynchronous tasks, handles status transitions, lists tasks, and performs best-effort cancellation. |
| `UpdaterModule`          | Converts updater REST requests into calls to `Updater` for statistics and metrics updates.                                                      |
| `RecommenderModule`      | Converts recommender REST requests into calls to `Recommender` for dry-run recommendation or job submission.                                    |
| `MonitorModule`          | Converts monitor REST requests into calls to `Monitor` for rule evaluation and callback execution.                                              |
| `MetricsQueryModule`     | Serves structured table, partition, and job metrics query APIs using existing metrics storage/provider logic.                                   |
| `OptimizerServiceClient` | Client used by the CLI when service mode is enabled.                                                                                            |

### Task Status Model

The task runtime uses one status model across updater, recommender, and monitor tasks. The status
vocabulary follows the Gravitino job model (`queued`, `started`, `succeeded`, `failed`, `cancelling`,
`canceled`) so that optimizer tasks read the same way as job runs:

| Status       | Meaning                                                                 |
| ------------ | ----------------------------------------------------------------------- |
| `queued`     | The service accepted the request and queued it for execution.           |
| `started`    | A worker is executing the request.                                      |
| `succeeded`  | Execution finished and the task record contains a result payload.       |
| `failed`     | Execution failed and the task record contains a sanitized error object. |
| `cancelling` | Cancellation was requested for a started task and is being applied.     |
| `canceled`   | The request was canceled before or during execution.                    |

`succeeded`, `failed`, and `canceled` are terminal statuses. The allowed transitions are:

```text
queued --> started --> succeeded
  |          |
  |          +--------> failed
  |          |
  |          +--------> cancelling --> canceled
  |
  +-------------------------------> canceled
```

A task in `queued` moves directly to `canceled` if it is canceled before a worker picks it up. A
`started` task first enters `cancelling` and reaches `canceled` only when the worker or provider
observes cancellation.

Each task record follows the job record shape (identifier, status, `audit`) plus optimizer-specific
timing and payload fields:

| Field         | Type   | Description                                                                 |
| ------------- | ------ | --------------------------------------------------------------------------- |
| `taskId`      | string | Unique optimizer task ID, for example `updater-018f2f4f`.                   |
| `module`      | string | `updater`, `recommender`, or `monitor`.                                     |
| `status`      | string | Current task status.                                                        |
| `startedAt`   | string | ISO-8601 start time, if started.                                            |
| `finishedAt`  | string | ISO-8601 completion time, if in a terminal status.                          |
| `requestHash` | string | Hash of the normalized request payload for audit and troubleshooting.       |
| `result`      | object | Module-specific result payload for successful tasks.                        |
| `error`       | object | Sanitized error object for failed tasks.                                    |
| `audit`       | object | Audit info (`createTime`, `creator`), matching the Gravitino `Audit` model. |

For a `failed` task, the `error` object mirrors the Gravitino `ErrorModel` (`code`, `type`, `message`)
so clients and automation branch on the same shape used by the metadata APIs:

```json
{
  "code": 1001,
  "type": "IllegalArgumentException",
  "message": "statisticsPayload and filePath cannot both be set"
}
```

`message` is always sanitized and never exposes secrets from config files, job options, or provider
exceptions.

The MVP may use an in-memory task store. The storage API should be pluggable so a later
implementation can add a DB-backed store without changing REST semantics.

### REST API

The API follows the same conventions as the Gravitino job run APIs (`/metalakes/{metalake}/jobs/runs`):

- All endpoints exchange JSON using the `application/vnd.gravitino.v1+json` media type.
- Every response body carries an integer `code` (`0` for success) wrapping the payload.
- Each asynchronous module exposes the same four operations as job runs: submit (`POST .../tasks`),
  get (`GET .../tasks/{taskId}`), list (`GET .../tasks`), and cancel (`POST .../tasks/{taskId}`).
- Errors use the Gravitino `ErrorModel` (`code`, `type`, `message`, `stack`), so a missing task
  returns a `NoSuchTaskException` and an invalid payload returns an `IllegalArgumentException`.
- List APIs support pagination in the service model even if the MVP initially returns an in-memory
  bounded list.

#### POST /api/optimizer/v1/updater/tasks

Submits an asynchronous updater task.

**Request:**

| Field               | Type          | Required | Description                                                                           |
| ------------------- | ------------- | -------- | ------------------------------------------------------------------------------------- |
| `updateType`        | string        | yes      | `STATISTICS` for `update-statistics` or `METRICS` for `append-metrics`.               |
| `calculatorName`    | string        | yes      | Statistics calculator name, for example `local-stats-calculator`.                     |
| `identifiers`       | array[string] | no       | Table or job identifiers. Empty means the selected calculator/provider decides scope. |
| `statisticsPayload` | string        | no       | Inline JSON Lines payload. Mutually exclusive with `filePath`.                        |
| `filePath`          | string        | no       | Server-local JSON Lines input file path. Mutually exclusive with `statisticsPayload`. |

**Response:** `200 OK`

```json
{
  "code": 0,
  "task": {
    "taskId": "updater-018f2f4f",
    "module": "updater",
    "status": "queued"
  }
}
```

**Behavior:**

The service validates the same command rules as the CLI. For `local-stats-calculator`, either
`statisticsPayload` or `filePath` is required. `statisticsPayload` and `filePath` cannot both be set.
The task result includes the updater summary currently printed by the CLI.

#### GET /api/optimizer/v1/updater/tasks/{taskId}

Returns one updater task.

**Response:** `200 OK`

```json
{
  "code": 0,
  "task": {
    "taskId": "updater-018f2f4f",
    "module": "updater",
    "status": "succeeded",
    "startedAt": "2026-06-30T10:00:01Z",
    "finishedAt": "2026-06-30T10:00:03Z",
    "result": {
      "updateType": "STATISTICS",
      "summary": {
        "updatedTables": 1,
        "updatedPartitions": 0,
        "updatedJobs": 0
      }
    },
    "audit": {
      "createTime": "2026-06-30T10:00:00Z",
      "creator": "anonymous"
    }
  }
}
```

**Behavior:**

Returns `404 Not Found` with a `NoSuchTaskException` `ErrorModel` if the task ID does not exist in the
task store.

#### GET /api/optimizer/v1/updater/tasks

Lists updater tasks.

**Request query parameters:**

| Field       | Type    | Required | Description                                            |
| ----------- | ------- | -------- | ------------------------------------------------------ |
| `status`    | string  | no       | Filter by task status.                                 |
| `fromTime`  | string  | no       | Include tasks created at or after this ISO-8601 time.  |
| `toTime`    | string  | no       | Include tasks created before or at this ISO-8601 time. |
| `limit`     | integer | no       | Maximum records to return.                             |
| `pageToken` | string  | no       | Pagination token for later persistent stores.          |

**Response:** `200 OK`

```json
{
  "code": 0,
  "tasks": [
    {
      "taskId": "updater-018f2f4f",
      "module": "updater",
      "status": "succeeded",
      "audit": {
        "createTime": "2026-06-30T10:00:00Z",
        "creator": "anonymous"
      }
    }
  ],
  "nextPageToken": ""
}
```

#### POST /api/optimizer/v1/updater/tasks/{taskId}

Requests best-effort cancellation for one updater task, mirroring `POST /jobs/runs/{jobId}`.

**Response:** `200 OK`

```json
{
  "code": 0,
  "task": {
    "taskId": "updater-018f2f4f",
    "module": "updater",
    "status": "cancelling"
  }
}
```

**Behavior:**

A `queued` task moves directly to `canceled`. A `started` task moves to `cancelling` and reaches
`canceled` only when the provider or worker observes cancellation. Completed tasks remain in their
terminal status.

#### POST /api/optimizer/v1/recommender/tasks

Submits an asynchronous recommender task.

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
  "task": {
    "taskId": "recommender-018f2f50",
    "module": "recommender",
    "status": "queued"
  }
}
```

**Behavior:**

The service validates identifiers, strategy name, and positive `limit` values. Dry-run tasks return
recommendation details. Non-dry-run tasks submit jobs through the configured `JobSubmitter`, which may
call Gravitino job APIs.

#### GET /api/optimizer/v1/recommender/tasks/{taskId}

Returns one recommender task.

**Response:** `200 OK`

```json
{
  "code": 0,
  "task": {
    "taskId": "recommender-018f2f50",
    "module": "recommender",
    "status": "succeeded",
    "result": {
      "dryRun": true,
      "recommendations": [
        {
          "strategyName": "iceberg_compaction_default",
          "identifier": "rest_catalog.db.t1",
          "score": 100,
          "jobTemplate": "builtin-iceberg-rewrite-data-files",
          "jobOptions": {
            "catalog_name": "rest_catalog",
            "table_identifier": "db.t1"
          },
          "jobId": ""
        }
      ]
    },
    "audit": {
      "createTime": "2026-06-30T10:10:00Z",
      "creator": "anonymous"
    }
  }
}
```

**Behavior:**

The `jobId` field carries the submitted Gravitino job ID when the configured submitter creates a job,
so the optimizer task result links directly to the job run tracked by the job framework.

#### GET /api/optimizer/v1/recommender/tasks

Lists recommender tasks.

**Request query parameters:**

| Field       | Type    | Required | Description                                            |
| ----------- | ------- | -------- | ------------------------------------------------------ |
| `status`    | string  | no       | Filter by task status.                                 |
| `fromTime`  | string  | no       | Include tasks created at or after this ISO-8601 time.  |
| `toTime`    | string  | no       | Include tasks created before or at this ISO-8601 time. |
| `limit`     | integer | no       | Maximum records to return.                             |
| `pageToken` | string  | no       | Pagination token for later persistent stores.          |

**Response:** `200 OK`

```json
{
  "code": 0,
  "tasks": [
    {
      "taskId": "recommender-018f2f50",
      "module": "recommender",
      "status": "succeeded",
      "audit": {
        "createTime": "2026-06-30T10:10:00Z",
        "creator": "anonymous"
      }
    }
  ],
  "nextPageToken": ""
}
```

**Behavior:**

The service returns recommender task summaries sorted by creation time. The MVP may only return tasks
that are still retained by the in-memory task store.

#### POST /api/optimizer/v1/recommender/tasks/{taskId}

Requests best-effort cancellation for one recommender task.

**Response:** `200 OK`

```json
{
  "code": 0,
  "task": {
    "taskId": "recommender-018f2f50",
    "module": "recommender",
    "status": "cancelling"
  }
}
```

**Behavior:**

A `queued` task moves directly to `canceled`; a `started` task moves through `cancelling` when the
worker or provider observes cancellation. Jobs already submitted through Gravitino are not
automatically canceled by canceling the optimizer task.

#### POST /api/optimizer/v1/monitor/tasks

Submits an asynchronous monitor task.

**Request:**

| Field           | Type          | Required | Description                                                                 |
| --------------- | ------------- | -------- | --------------------------------------------------------------------------- |
| `identifiers`   | array[string] | yes      | Table or job identifiers to evaluate.                                       |
| `actionTime`    | integer       | yes      | Action timestamp in epoch seconds.                                          |
| `rangeSeconds`  | integer       | no       | Evaluation window. Default is the current CLI default, 86400 seconds.       |
| `partitionPath` | array[object] | no       | Partition path. Allowed only when exactly one table identifier is provided. |

**Response:** `200 OK`

```json
{
  "code": 0,
  "task": {
    "taskId": "monitor-018f2f51",
    "module": "monitor",
    "status": "queued"
  }
}
```

**Behavior:**

The service evaluates monitor rules with the configured metrics provider, evaluator, table-job
relation provider, and callbacks. The task result includes the evaluation records currently printed by
the CLI.

#### GET /api/optimizer/v1/monitor/tasks/{taskId}

Returns one monitor task.

**Response:** `200 OK`

```json
{
  "code": 0,
  "task": {
    "taskId": "monitor-018f2f51",
    "module": "monitor",
    "status": "succeeded",
    "result": {
      "evaluations": [
        {
          "identifier": "rest_catalog.db.t1",
          "scope": "table",
          "evaluator": "gravitino-metrics-evaluator",
          "passed": true
        }
      ]
    },
    "audit": {
      "createTime": "2026-06-30T10:20:00Z",
      "creator": "anonymous"
    }
  }
}
```

#### GET /api/optimizer/v1/monitor/tasks

Lists monitor tasks.

**Request query parameters:**

| Field       | Type    | Required | Description                                            |
| ----------- | ------- | -------- | ------------------------------------------------------ |
| `status`    | string  | no       | Filter by task status.                                 |
| `fromTime`  | string  | no       | Include tasks created at or after this ISO-8601 time.  |
| `toTime`    | string  | no       | Include tasks created before or at this ISO-8601 time. |
| `limit`     | integer | no       | Maximum records to return.                             |
| `pageToken` | string  | no       | Pagination token for later persistent stores.          |

**Response:** `200 OK`

```json
{
  "code": 0,
  "tasks": [
    {
      "taskId": "monitor-018f2f51",
      "module": "monitor",
      "status": "succeeded",
      "audit": {
        "createTime": "2026-06-30T10:20:00Z",
        "creator": "anonymous"
      }
    }
  ],
  "nextPageToken": ""
}
```

**Behavior:**

The service returns monitor task summaries sorted by creation time. The MVP may only return tasks
that are still retained by the in-memory task store.

#### POST /api/optimizer/v1/monitor/tasks/{taskId}

Requests best-effort cancellation for one monitor task.

**Response:** `200 OK`

```json
{
  "code": 0,
  "task": {
    "taskId": "monitor-018f2f51",
    "module": "monitor",
    "status": "cancelling"
  }
}
```

**Behavior:**

A `queued` task moves directly to `canceled`; a `started` task moves through `cancelling` when the
worker or provider observes cancellation. Monitor callbacks that have already been invoked are not
rolled back.

#### GET /api/optimizer/v1/metrics/tables

Queries table or partition metrics. Unlike the module tasks, metrics queries are synchronous because
they read existing metrics and are expected to be short, so they return the result directly instead of
a task record.

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

**Behavior:**

Large result pagination can be added with the same `limit` and `pageToken` pattern used by the task
list APIs.

#### GET /api/optimizer/v1/metrics/jobs

Queries job metrics.

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

#### GET /api/optimizer/v1/health

Returns service health. This is an operational endpoint and is the one response that is not wrapped in
the `code` envelope, so external liveness probes can consume it directly.

**Response:** `200 OK`

```json
{
  "status": "UP",
  "modules": {
    "updater": "UP",
    "recommender": "UP",
    "monitor": "UP",
    "metricsQuery": "UP"
  }
}
```

### Service Configuration

The service reads the same `conf/gravitino-optimizer.conf` file as the CLI, plus service-side keys
that control the HTTP endpoint and the task runtime described in the Reliability section:

| Key                                          | Default   | Description                                                          |
| -------------------------------------------- | --------- | -------------------------------------------------------------------- |
| `gravitino.optimizer.server.host`            | `0.0.0.0` | Bind address for the Optimizer Service HTTP server.                  |
| `gravitino.optimizer.server.port`            | `8091`    | Listen port for the Optimizer Service HTTP server.                   |
| `gravitino.optimizer.server.workerPoolSize`  | `4`       | Worker threads per module for asynchronous task execution.           |
| `gravitino.optimizer.server.queueCapacity`   | `100`     | Maximum queued tasks per module before new submissions are rejected. |
| `gravitino.optimizer.server.taskTimeoutMs`   | `600000`  | Per-task execution timeout after which the task is marked `failed`.  |
| `gravitino.optimizer.server.taskRetentionMs` | `3600000` | How long terminal task records are retained by the in-memory store.  |

### CLI Service Mode

Add optimizer CLI service configuration keys:

| Key                                             | Default | Description                                                             |
| ----------------------------------------------- | ------- | ----------------------------------------------------------------------- |
| `gravitino.optimizer.service.enabled`           | `false` | Enables CLI remote execution for supported commands.                    |
| `gravitino.optimizer.service.url`               | none    | Base URL of the Optimizer Service, for example `http://localhost:8091`. |
| `gravitino.optimizer.service.requestTimeoutMs`  | `30000` | HTTP request timeout for CLI service calls.                             |
| `gravitino.optimizer.service.pollIntervalMs`    | `1000`  | Poll interval when the CLI waits for async task completion.             |
| `gravitino.optimizer.service.waitForCompletion` | `true`  | Whether CLI commands wait and print final results by default.           |

The existing CLI commands remain the primary user interface:

| CLI command               | Service mode behavior                                                                                                   |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `update-statistics`       | Calls `POST /api/optimizer/v1/updater/tasks` with `updateType=STATISTICS`.                                              |
| `append-metrics`          | Calls `POST /api/optimizer/v1/updater/tasks` with `updateType=METRICS`.                                                 |
| `submit-strategy-jobs`    | Calls `POST /api/optimizer/v1/recommender/tasks`.                                                                       |
| `monitor-metrics`         | Calls `POST /api/optimizer/v1/monitor/tasks`.                                                                           |
| `list-table-metrics`      | Calls `GET /api/optimizer/v1/metrics/tables`.                                                                           |
| `list-job-metrics`        | Calls `GET /api/optimizer/v1/metrics/jobs`.                                                                             |
| `submit-update-stats-job` | Initially remains local CLI submission to the Gravitino job framework. A later phase may add an optimizer task wrapper. |

If service mode is disabled, commands use the current local execution path. If service mode is
enabled and the service call fails, the CLI should fail fast by default. Automatic local fallback can
hide partial service outages and cause duplicated submissions, so fallback should require an explicit
future configuration if it is added.

### User Process

The service mode user flow is:

1. The operator configures `conf/gravitino-optimizer.conf` with Gravitino connection settings,
   optimizer providers, and service settings.
2. The operator starts the Optimizer Service with the optimizer distribution.
3. A user runs an existing CLI command such as:

   ```bash
   ./bin/gravitino-optimizer.sh \
     --type submit-strategy-jobs \
     --identifiers rest_catalog.db.t1 \
     --strategy-name iceberg_compaction_default \
     --dry-run \
     --limit 10
   ```

4. The CLI validates arguments, detects service mode, submits a REST request, and prints the task ID.
5. If `waitForCompletion` is enabled, the CLI polls the task status endpoint and prints the final
   result using output compatible with the existing command.
6. Automation can call the REST API directly, store the task ID, and query the result later.

### Implementation Process

The internal flow for asynchronous tasks is:

```text
CLI or REST client
      |
      v
REST resource validates request
      |
      v
TaskRuntime creates task record in queued status
      |
      v
Worker picks task and marks it started
      |
      v
Module invokes existing optimizer class
      |
      +--> Updater / Recommender / Monitor
      |
      v
TaskRuntime stores succeeded, failed, or canceled result
      |
      v
Client queries task result
```

Implementation should keep module boundaries thin. Existing `Updater`, `Recommender`, and `Monitor`
classes remain the execution core. REST resources translate requests into module calls, and modules
translate module results into REST result DTOs.

### Backward Compatibility

This design is backward compatible for existing CLI users:

1. Service mode is disabled by default.
2. Existing command names and options remain valid.
3. Existing local output should remain the default when service mode is disabled.
4. Service mode may print the task ID in addition to existing summary lines. This is additive.
5. `submit-update-stats-job` keeps the existing local submission path until a service wrapper is
   implemented.

New REST APIs are additive. They do not change existing Gravitino metadata APIs or job APIs.

### Security

The service must validate all request parameters at the REST boundary using the same rules as the
CLI. File-based input such as `filePath` is server-local in service mode and should be documented as
an operator-controlled path, not a client upload path.

Authentication and authorization can be phased:

1. MVP supports deployments behind trusted internal networks or reverse proxies.
2. A later phase can integrate with Gravitino authentication mechanisms or service-specific tokens.
3. Direct REST clients should receive sanitized error messages that do not expose secrets from config
   files, job options, or provider exceptions.

### Reliability and Observability

The service should provide:

| Area          | Requirement                                                                                 |
| ------------- | ------------------------------------------------------------------------------------------- |
| Queue control | Per-module queue size and worker count.                                                     |
| Timeout       | Per-task execution timeout and HTTP client timeout.                                         |
| Cancellation  | Best-effort cancellation for queued and cooperative running tasks.                          |
| Shutdown      | Graceful shutdown that stops accepting requests and drains or cancels queued work.          |
| Logs          | Task lifecycle logs with task ID, module, status transition, and duration.                  |
| Metrics       | Task counts by module/status, latency, queue depth, worker utilization, and failure counts. |
| Health        | Health endpoint with module initialization status.                                          |

### Rollout Plan

#### Phase 1: Service Shell and Shared Runtime

Create the service entry point, REST server, health endpoint, task runtime, in-memory task store, and
common DTOs. Add unit tests for state transitions, cancellation, serialization, and request listing.

#### Phase 2: Updater APIs and CLI Service Mode

Implement updater REST APIs and CLI service mode for `update-statistics` and `append-metrics`.
Validate parity with local execution using existing updater tests and new service-mode CLI tests.

#### Phase 3: Recommender APIs and CLI Service Mode

Implement recommender REST APIs and CLI service mode for `submit-strategy-jobs`. Cover dry-run and
real submission paths, including returned job IDs from the configured submitter.

#### Phase 4: Monitor and Metrics Query APIs

Implement monitor task APIs and synchronous metrics query APIs. Add CLI service mode for
`monitor-metrics`, `list-table-metrics`, and `list-job-metrics`.

#### Phase 5: Built-In Update Stats Job Wrapper

Optionally add an asynchronous optimizer task wrapper for `submit-update-stats-job`. The actual Spark
job submission and status remain owned by the Gravitino job framework.

#### Phase 6: Hardening and Documentation

Add persistent task storage, authentication integration, service metrics export, stronger graceful
shutdown semantics, and optional multi-node deployment support.

---

## Task Breakdown

### Phase 1: Service Shell and Shared Runtime

- [ ] Add optimizer service configuration keys to `OptimizerConfig`.
- [ ] Add `OptimizerServiceServer` entry point and startup script in the optimizer distribution.
- [ ] Add common request, response, task record, task state, and error DTOs.
- [ ] Implement `TaskRuntime` with in-memory task storage, bounded queues, worker pools, state
      transitions, list/query support, and best-effort cancellation.
- [ ] Add `GET /api/optimizer/v1/health`.
- [ ] Add unit tests for task state transitions, task listing filters, cancellation, and DTO
      serialization.

### Phase 2: Updater APIs and CLI Service Mode

- [ ] Add updater REST resource for submit, get, list, and cancel APIs.
- [ ] Add `UpdaterModule` that adapts REST requests to existing `Updater` execution.
- [ ] Add `OptimizerServiceClient` support for updater APIs.
- [ ] Add CLI service-mode routing for `update-statistics`.
- [ ] Add CLI service-mode routing for `append-metrics`.
- [ ] Add tests for updater request validation, local calculator input rules, successful results, and
      failure results.

### Phase 3: Recommender APIs and CLI Service Mode

- [ ] Add recommender REST resource for submit, get, list, and cancel APIs.
- [ ] Add `RecommenderModule` that adapts REST requests to existing `Recommender` execution.
- [ ] Add `OptimizerServiceClient` support for recommender APIs.
- [ ] Add CLI service-mode routing for `submit-strategy-jobs`.
- [ ] Add tests for dry-run recommendations, limit handling, job submission results, and service-mode
      CLI output.

### Phase 4: Monitor and Metrics Query APIs

- [ ] Add monitor REST resource for submit, get, list, and cancel APIs.
- [ ] Add `MonitorModule` that adapts REST requests to existing `Monitor` execution.
- [ ] Add metrics query REST resource for table, partition, and job metric queries.
- [ ] Add `OptimizerServiceClient` support for monitor and metrics query APIs.
- [ ] Add CLI service-mode routing for `monitor-metrics`.
- [ ] Add CLI service-mode routing for `list-table-metrics` and `list-job-metrics`.
- [ ] Add tests for monitor validation, partition path handling, evaluation results, and metrics query
      responses.

### Phase 5: Built-In Update Stats Job Wrapper

- [ ] Add optional service task API for `submit-update-stats-job` request wrapping.
- [ ] Keep actual job creation in the existing Gravitino job framework.
- [ ] Add CLI service-mode routing for `submit-update-stats-job` only after the wrapper is available.
- [ ] Add tests that returned optimizer task results include submitted Gravitino job IDs.

### Phase 6: Hardening and Documentation

- [ ] Add persistent task store abstraction and DB-backed implementation.
- [ ] Add service authentication and sanitized error handling.
- [ ] Add service metrics for task counts, latency, queue depth, and worker utilization.
- [ ] Add graceful shutdown tests.
- [ ] Update user-facing TMS documentation in `docs/table-maintenance-service/`.
- [ ] Add OpenAPI documentation if optimizer service APIs are published under `docs/open-api/`.
- [ ] Validate OpenAPI documentation with `./gradlew :docs:build` if OpenAPI files are changed.
