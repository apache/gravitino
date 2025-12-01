---
title: Apache Gravitino metrics
slug: /metrics
keywords:
  - metrics
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Apache Gravitino Metrics builds upon the [Dropwizard Metrics](https://metrics.dropwizard.io/). It exports these metrics through both JMX and an HTTP server, supporting JSON and Prometheus formats. You can retrieve them via HTTP requests, as illustrated below:

```shell
// Use Gravitino Server address or Iceberg REST server address to replace 127.0.0.1:8090
// Get metrics in JSON format
curl http://127.0.0.1:8090/metrics
// Get metrics in Prometheus format
curl http://127.0.0.1:8090/prometheus/metrics
```

### Metrics source

#### HTTP server metrics

HTTP server metrics encompass the histogram of HTTP request processing time and the number of HTTP response codes, categorized by different HTTP interfaces such as `create-table` and `load-table`.

For instance, you can get Prometheus metrics for `create-table` operation in the Gravitino server as follows:

```text
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

:::info
Metrics with the `gravitino-server` prefix pertain to the Gravitino server, while those with the `iceberg-rest-server` prefix are for the Gravitino Iceberg REST server.
:::

#### JVM metrics

JVM metrics source uses [JVM instrumentation](https://metrics.dropwizard.io/4.2.0/manual/jvm.html) with BufferPoolMetricSet, GarbageCollectorMetricSet, and MemoryUsageGaugeSet.
These metrics start with the `jvm` prefix, like `jvm.heap.used` in JSON format, `jvm_heap_used` in Prometheus format.

#### Catalog metrics

Catalog metrics provide the metrics from different catalog instances.
All the catalog metrics start with the `gravitino-catalog` prefix in Prometheus format and with labels `provider`, `metalake`, and `catalog` to distinguish different catalog instances.

Currently, Catalog metrics only support Fileset catalog and JDBC catalog. 

You can get Prometheus metrics for a Fileset catalog named `test_catalog` under a metalake named `test_metalake` in the Gravitino server as follows:

```text
gravitino_catalog_filesystem_cache_hits{provider="fileset",metalake="test_metalake",catalog="test_catalog",} 0.0
gravitino_catalog_filesystem_cache_misses{provider="fileset",metalake="test_metalake",catalog="test_catalog",} 0.0
```

You can get Prometheus metrics for a JDBC catalog named `test_catalog` under a metalake named `test_metalake` in the Gravitino server as follows:

```text
gravitino_catalog_datasource_idle_connections{provider="jdbc",metalake="test_metalake",catalog="test_catalog",} 1.0
gravitino_catalog_datasource_active_connections{provider="jdbc",metalake="test_metalake",catalog="test_catalog",} 0.0
gravitino_catalog_datasource_max_connections{provider="jdbc",metalake="test_metalake",catalog="test_catalog",} 10.0
```
