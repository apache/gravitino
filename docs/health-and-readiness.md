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
process" apart from "send traffic somewhere else." Liveness answers whether the server can respond
at all. Readiness answers whether it can reach the entity store and therefore do useful work.

The endpoints follow MicroProfile Health semantics. A healthy check returns 200 and an unhealthy one
returns 503, both with a JSON body naming the individual checks that ran.

## Quick Start

**1. Check liveness.** This returns 200 whenever an HTTP thread is able to answer.

```shell
GRAVITINO_URL=http://localhost:8090

curl -i "${GRAVITINO_URL}/api/health/live"
```

**2. Check readiness.** This returns 200 only when the entity store responds.

```shell
curl -i "${GRAVITINO_URL}/api/health/ready"
```

**3. Check both at once.** The aggregate endpoint runs the liveness and readiness checks together
and reports 503 if either fails.

```shell
curl -i "${GRAVITINO_URL}/api/health"
```

## Endpoints

| Path                | Checks                        | Returns 503 when                       |
|---------------------|-------------------------------|----------------------------------------|
| `/api/health/live`  | HTTP server                   | Never, if the request is answered      |
| `/api/health/ready` | Entity store                  | The entity store check fails           |
| `/api/health`       | HTTP server and entity store  | Either check fails                     |

Each path is also served at the root of the server, without the `/api` prefix, for load balancers
and traffic managers that require probes at well-known locations. The root aliases are `/health`,
`/health/live`, `/health/ready`, and `/health.html`, and the last of these maps to the aggregate
endpoint rather than to a check of its own.

The response body carries an overall status and a list of individual checks. Each check has a name,
a status of UP or DOWN, and a details map that explains a failure. The two check names are
`httpServer` and `entityStore`.

## What Readiness Actually Tests

The entity store check issues an existence lookup for a metalake named `gravitino_health_probe`.
The name is a sentinel and is not expected to exist. What matters is that the store answers rather
than what it answers, so a reachable store reports UP even though the lookup finds nothing.

The lookup runs on a small dedicated thread pool rather than on the request thread, so a store that
has stopped responding cannot tie up HTTP threads. The pool holds one core thread, grows to four,
and queues at most twenty probes before rejecting further ones.

## Configuration

| Property                                              | Description                                    | Default |
|-------------------------------------------------------|------------------------------------------------|---------|
| `gravitino.server.health.entityStore.probeTimeoutMs`   | Timeout for the entity store readiness probe   | `2000`  |

Set this above the worst-case latency of the store rather than at its typical latency. A probe that
exceeds the timeout is cancelled and reported as DOWN, which takes the server out of rotation, so a
value tuned too tightly turns a slow backend into an outage.

## Failure Reasons

A DOWN entity store check names the reason in its details.

| Reason                        | Meaning                                                          |
|-------------------------------|------------------------------------------------------------------|
| `entity store not initialized` | The server is still starting and the store is not available yet  |
| `timeout`                     | The probe exceeded the configured timeout and was cancelled       |
| `probe-rejected`              | The probe queue was full, meaning probes are arriving faster than the store answers |
| `interrupted`                 | The probe thread was interrupted                                  |
| An exception class name       | The store raised that exception                                   |

A steady stream of `probe-rejected` means probe traffic is outpacing the store rather than that any
single probe failed, so it usually points at an aggressive probe interval or a degraded backend
rather than at a configuration error.

## Authentication and Auditing

Health paths bypass authentication, so a probe does not need credentials and does not break when
authentication is enabled on the server. They are also excluded from audit logging, so probe traffic
does not fill the audit log.

Both behaviors cover the root aliases as well as the canonical paths, because a forwarded request
still reports its original URI.

## Kubernetes Probes

The Helm chart's default liveness and readiness probes both target `/` rather than the health
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
