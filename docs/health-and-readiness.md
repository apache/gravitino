---
title: "Health and readiness"
slug: /health-and-readiness
keywords:
  - health
  - readiness
  - liveness
  - monitoring
license: "This software is licensed under the Apache License version 2."
---

Gravitino exposes separate liveness and readiness endpoints so that a caller can tell "restart this
process" apart from "send traffic somewhere else." Liveness checks whether the server can respond
and has not observed an out-of-memory error. Readiness also checks whether it can reach the entity
store.

The endpoints follow MicroProfile Health semantics. A healthy check returns 200 and an unhealthy one
returns 503, both with a JSON body naming the individual checks that ran.

## Quick Start

**1. Check liveness.** This returns 200 when an HTTP thread can answer and no out-of-memory error
has been observed.

```shell
GRAVITINO_URL=http://localhost:8090

curl -i "${GRAVITINO_URL}/api/health/live"
```

**2. Check readiness.** This returns 200 only when the entity store responds and no out-of-memory
error has been observed.

```shell
curl -i "${GRAVITINO_URL}/api/health/ready"
```

**3. Check both at once.** The aggregate endpoint runs the liveness and readiness checks together
and reports 503 if either fails.

```shell
curl -i "${GRAVITINO_URL}/api/health"
```

## Endpoints

| Path                | Checks                                  | Returns 503 when                                                    |
|---------------------|-----------------------------------------|---------------------------------------------------------------------|
| `/api/health/live`  | HTTP server and OOM state               | An out-of-memory error was observed                                 |
| `/api/health/ready` | Entity store and OOM state              | An out-of-memory error was observed or the entity store check fails |
| `/api/health`       | HTTP server, entity store and OOM state | Any check fails                                                     |

Each path is also served at the root of the server, without the `/api` prefix, for load balancers
and traffic managers that require probes at well-known locations. The root aliases are `/health`,
`/health/live`, `/health/ready`, and `/health.html`, and the last of these maps to the aggregate
endpoint rather than to a check of its own.

The response body carries an overall status and a list of individual checks. Each check has a name,
a status of `up` or `down`, and a details map that explains a failure. On the Gravitino server the two
normal check names are `httpServer` and `entityStore`. After an observed out-of-memory error, all
three endpoints instead report the `jvm` failure described below.

## Out-of-memory Failures

A Metaspace or heap `OutOfMemoryError` can leave already-loaded endpoints responding successfully
while other operations fail. A successful HTTP response or entity-store lookup therefore does not
prove recovery after OOM.

The Gravitino, Iceberg REST, and Lance REST servers record OOM observed by their Jersey exception
listeners, error mappers, and a servlet filter installed before other filters and servlets.
Authentication error handling and request execution/error-response helpers (including the built-in
IdP helpers) also record errors they consume. The main server also records failures in health-probe
tasks. The Jetty worker uncaught-exception handler is an additional fallback, not the request
exception boundary.
Wrapped causes are checked too. Once recorded, the affected service’s health endpoints and root
aliases on Gravitino and Iceberg REST return HTTP 503. Gravitino serializes status values as
`up`/`down`; Iceberg REST uses `UP`/`DOWN`. The following body shows the Gravitino format
(the main server uses the `/api/health` prefix); Iceberg REST uses `"DOWN"` for both status fields:

```json
{
  "code": 0,
  "status": "down",
  "checks": [
    {
      "name": "jvm",
      "status": "down",
      "details": { "reason": "OutOfMemoryError; restart required" }
    }
  ]
}
```

This state lasts until process restart, even if subsequent ordinary API requests succeed. Health
checks skip the entity-store probe once OOM is recorded. A database outage, ordinary HTTP 500,
`StackOverflowError`, or missing connector class alone does not set this state.

This policy also applies to an OOM caused by a single request, such as an oversized list response
or `Requested array size exceeds VM limit`. The server does not distinguish recoverable allocation
failures from persistent memory exhaustion: even if memory becomes available again, the health
state remains unhealthy until restart. If liveness probes trigger automatic restarts, repeatedly
retrying the same oversized request against different replicas can cause those replicas to restart
in succession. Account for this behavior when configuring request limits and retry policies.

Detection covers errors reaching these server boundaries; it cannot detect an OOM swallowed
entirely by a connector or unrelated background executor. This is not a JVM-wide OOM trap. If the
JVM cannot allocate enough memory to answer a probe, the probe may fail without a JSON response.
Only the throwable itself and its cause chain are inspected. An OOM present only in suppressed
exceptions (for example, from resource cleanup) is not detected, avoiding defensive array copies
while examining failures.

When Iceberg REST and Lance REST run embedded in the main server, the default auxiliary
classloaders share the same `ServerHealth` marker. An OOM recorded by any of these services makes
the Gravitino and Iceberg REST health endpoints report unhealthy. Lance REST has no dedicated
health endpoints in version 1.3. Services running in separate JVM processes track
OOM independently.

## What Readiness Actually Tests

The entity store check issues an existence lookup for a metalake named `gravitino_health_probe`.
The name is a sentinel and is not expected to exist. What matters is that the store answers rather
than what it answers, so a reachable store reports UP even though the lookup finds nothing.

The lookup runs on a small dedicated thread pool rather than on the request thread, so a store that
has stopped responding cannot tie up HTTP threads. The pool holds one core thread, grows to four,
and queues at most twenty probes before rejecting further ones.

## Iceberg REST Endpoints

The Iceberg REST service runs its own HTTP server, including when embedded in the Gravitino
server process. Embedded services share the OOM marker, but HTTP availability and initialization
checks remain specific to each service. Probe the Iceberg REST port as well.

| Server               | Default Port | Health Path Prefix | Readiness Check         |
|----------------------|--------------|--------------------|-------------------------|
| Gravitino server     | `8090`       | `/api/health`      | `entityStore`           |
| Iceberg REST service | `9001`       | `/iceberg/health`  | `catalogWrapperManager` |

Each prefix serves `/live` and `/ready` beneath it, with the aggregate check at the prefix itself.
Both servers also serve `/health`, `/health/live`, `/health/ready`, and `/health.html` root aliases.
All these endpoints return 503 after an observed OOM until restart.

Iceberg REST readiness reports UP once its catalog wrapper manager exists during startup. It does
not test whether the catalog backend is reachable. The entity store probe timeout setting applies
only to the Gravitino server; Iceberg REST has no backend probe timeout to tune.

Lance REST does not expose dedicated health endpoints or root health aliases in version 1.3.

## Configuration

| Property                                             | Description                                  | Default |
|------------------------------------------------------|----------------------------------------------|---------|
| `gravitino.server.health.entityStore.probeTimeoutMs` | Timeout for the entity store readiness probe | `2000`  |

Set this above the worst-case latency of the store rather than at its typical latency. A probe that
exceeds the timeout is cancelled and reported as DOWN, which takes the server out of rotation, so a
value tuned too tightly turns a slow backend into an outage.

## Failure Reasons

A DOWN entity store check names the reason in its details.

| Reason                         | Meaning                                                         |
|--------------------------------|-----------------------------------------------------------------|
| `entity store not initialized` | The server is still starting and the store is not available yet |
| `timeout`                      | The probe exceeded the configured timeout and was cancelled     |
| `probe-rejected`               | The probe queue was full and the probe was never run            |
| `interrupted`                  | The probe thread was interrupted                                |
| An exception class name        | The store raised that exception                                 |

A steady stream of `probe-rejected` means probe traffic is outpacing the store rather than that any
single probe failed, so it usually points at an aggressive probe interval or a degraded backend
rather than at a configuration error.

## Authentication and Auditing

Health paths bypass authentication, so a probe does not need credentials and does not break when
authentication is enabled on the server. They are also excluded from audit logging, so probe traffic
does not fill the audit log.

Both behaviors cover the root aliases as well as the canonical paths, because a forwarded request
still reports its original URI. Both behaviors also hold for Iceberg REST health paths on its
own port.

## Kubernetes Probes

The Gravitino chart's default liveness and readiness probes both target `/` rather than the health
endpoints, so an out-of-the-box install does not use the checks described on this page. A probe
against `/` confirms only that the HTTP listener is accepting connections, which means a pod whose
entity store has failed still reports ready and still receives traffic.

Point the probes at the health endpoints in your values file.

```yaml
livenessProbe:
  httpGet:
    path: /api/health/live
    port: http
  initialDelaySeconds: 20
  timeoutSeconds: 5

readinessProbe:
  httpGet:
    path: /api/health/ready
    port: http
  initialDelaySeconds: 20
  timeoutSeconds: 5
```

Keep the readiness timeout above the entity store probe timeout so that Kubernetes waits for the
server's own answer instead of timing out first and losing the reason for the failure.

Liveness should stay on the liveness endpoint rather than the readiness or aggregate one. Pointing
liveness at a check that includes the entity store means a database outage restarts every pod, which
removes the servers that would otherwise recover when the store returns.

The Iceberg REST and Lance REST charts default their probes to request paths rather than to `/`, so
those probes carry no credentials and fail once authentication is enabled. Point the Iceberg REST
probes at `/iceberg/health/live` and `/iceberg/health/ready`, which are exempt from authentication.
Lance REST has no dedicated health endpoints in version 1.3; its request-path probes require
credentials when authentication is enabled.
