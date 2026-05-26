---
title: "Metrics"
slug: "/metrics"
keywords:
  - metrics
license: "This software is licensed under the Apache License version 2."
---

Apache Gravitino exposes runtime metrics via the [Dropwizard Metrics](https://metrics.dropwizard.io/) library. Metrics are available through JMX and an HTTP endpoint, in both JSON and Prometheus formats. Metrics behavior is configured in `gravitino.conf`; see [Server Configuration > Metrics](gravitino-server-config.md#metrics) for the property reference.

Retrieve metrics from a running server (substitute the Gravitino server or Iceberg REST server address for `127.0.0.1:8090`):

```shell
# JSON format
curl http://127.0.0.1:8090/metrics

# Prometheus format
curl http://127.0.0.1:8090/prometheus/metrics
```

Metrics with the `gravitino-server` prefix are emitted by the Gravitino server; those with the `iceberg-rest-server` prefix are emitted by the embedded Iceberg REST server.

## HTTP Server Metrics

HTTP server metrics include a histogram of HTTP request processing time and counts of HTTP response codes, partitioned by endpoint (for example `create-table` and `load-table`).

Example Prometheus output for the `create-table` operation:

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

## JVM Metrics

JVM metrics use [JVM instrumentation](https://metrics.dropwizard.io/4.2.0/manual/jvm.html) from the Dropwizard library, including `BufferPoolMetricSet`, `GarbageCollectorMetricSet`, and `MemoryUsageGaugeSet`. JVM metric names start with the `jvm` prefix (for example, `jvm.heap.used` in JSON, `jvm_heap_used` in Prometheus).

## Catalog Metrics

Catalog metrics report runtime statistics from individual catalog instances. All catalog metric names start with the `gravitino-catalog` prefix and carry the labels `provider`, `metalake`, and `catalog` to distinguish instances.

Catalog metrics are currently supported for Fileset and JDBC catalogs only.

Example Prometheus output for a Fileset catalog named `test_catalog` in a metalake named `test_metalake`:

```text
gravitino_catalog_filesystem_cache_hits{provider="fileset",metalake="test_metalake",catalog="test_catalog",} 0.0
gravitino_catalog_filesystem_cache_misses{provider="fileset",metalake="test_metalake",catalog="test_catalog",} 0.0
```

Example Prometheus output for a JDBC catalog named `test_catalog` in a metalake named `test_metalake`:

```text
gravitino_catalog_datasource_idle_connections{provider="jdbc",metalake="test_metalake",catalog="test_catalog",} 1.0
gravitino_catalog_datasource_active_connections{provider="jdbc",metalake="test_metalake",catalog="test_catalog",} 0.0
gravitino_catalog_datasource_max_connections{provider="jdbc",metalake="test_metalake",catalog="test_catalog",} 10.0
```

## Configuration

Metrics behavior is configured in `gravitino.conf`. See [Server Configuration > Metrics](gravitino-server-config.md#metrics) for the property reference.

## Related

- [Server Configuration](gravitino-server-config.md)
- [Iceberg REST Server](iceberg-rest-service.md)
