---
title: Gravition metrics
slug: /metrics
keywords:
  - metrics
---

## Introduction
Gravitino Metrics is built upon the [Dropwizard Metrics](https://metrics.dropwizard.io/). These metrics are exported through both JMX and an HTTP server, supporting JSON and Prometheus formats. You can retrieve them via HTTP requests, as illustrated below:
```
// Use Gravitino Server address or Iceberg REST server address to replace 127.0.0.1:8090
// Get metrics in json format
curl http://127.0.0.1:8090/metrics
// Get metrics in Promethus format
curl http://127.0.0.1:8090/prometheus/metrics
```

### Metrics source
#### HTTP server metrics
HTTP server metrics encompass the histogram of HTTP request processing time and the number of HTTP response codes, categorized by different HTTP interfaces such as `create-table` and `load-table`.

For instance, considering the `create-table` operation in the Gravitino server, Prometheus metrics are obtained as follows:
```
gravitino_server_1xx_responses_total{operation="create-table",} 0.0
gravitino_server_4xx_responses_total{operation="create-table",} 0.0
gravitino_server_5xx_responses_total{operation="create-table",} 0.0
gravitino_server_2xx_responses_total{operation="create-table",} 0.0
gravitino_server_3xx_responses_total{operation="create-table",} 0.0
gravitino_server_http_request_duration_seconds_count{operation="create-table",} 0.0
gravitino_server_http_request_duration_seconds{operation="create-table",quantile="0.5",} 0.0
gravitino_server_http_request_duration_seconds{operation="create-table",quantile="0.75",} 0.0
gravitino_server_http_request_duration_seconds{operation="create-table",quantile="0.95",} 0.0
gravitino_server_http_request_duration_seconds{operation="create-table",quantile="0.98",} 0.0
gravitino_server_http_request_duration_seconds{operation="create-table",quantile="0.99",} 0.0
gravitino_server_http_request_duration_seconds{operation="create-table",quantile="0.999",} 0.0
```

Please note that metrics with the `gravitino-server` prefix pertain to the Gravitino server, while those with the `iceberg-rest-server` prefix are for the Gravitino Iceberg REST server.

#### JVM metrics
JVM metrics source using [JVM instrumentation](https://metrics.dropwizard.io/4.2.0/manual/jvm.html) with BufferPoolMetricSet, GarbageCollectorMetricSet and MemoryUsageGaugeSet.
These metrics start with the `jvm` prefix, like `jvm.heap.used` in json format, `jvm_head_used` in Prometheus format.
