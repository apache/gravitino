---
title: "Metrics"
slug: "/metrics"
keywords:
  - metrics
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Apache Gravitino Metrics builds upon the [Dropwizard Metrics](https://metrics.dropwizard.io/). It exports these metrics through both JMX and an HTTP server, supporting JSON and Prometheus formats. Retrieve them via HTTP requests, as illustrated below:

```shell
// Use Gravitino Server address or Iceberg REST server address to replace 127.0.0.1:8090
// Get metrics in JSON format
curl http://127.0.0.1:8090/metrics
// Get metrics in Prometheus format
curl http://127.0.0.1:8090/prometheus/metrics
```

### Metrics Source

#### HTTP Server Metrics

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

#### JVM Metrics

JVM metrics source uses [JVM instrumentation](https://metrics.dropwizard.io/4.2.0/manual/jvm.html) with BufferPoolMetricSet, GarbageCollectorMetricSet, and MemoryUsageGaugeSet.
These metrics start with the `jvm` prefix, like `jvm.heap.used` in JSON format, `jvm_heap_used` in Prometheus format.

#### Catalog Metrics

Catalog metrics provide the metrics from different catalog instances.
All the catalog metrics start with the `gravitino-catalog` prefix in Prometheus format and with labels `provider`, `metalake`, and `catalog` to distinguish different catalog instances.

Catalog metrics only support Fileset catalog and JDBC catalog. 

Get Prometheus metrics for a Fileset catalog named `test_catalog` under a metalake named `test_metalake` in the Gravitino server as follows:

```text
gravitino_catalog_filesystem_cache_hits{provider="fileset",metalake="test_metalake",catalog="test_catalog",} 0.0
gravitino_catalog_filesystem_cache_misses{provider="fileset",metalake="test_metalake",catalog="test_catalog",} 0.0
```

Get Prometheus metrics for a JDBC catalog named `test_catalog` under a metalake named `test_metalake` in the Gravitino server as follows:

```text
gravitino_catalog_datasource_idle_connections{provider="jdbc",metalake="test_metalake",catalog="test_catalog",} 1.0
gravitino_catalog_datasource_active_connections{provider="jdbc",metalake="test_metalake",catalog="test_catalog",} 0.0
gravitino_catalog_datasource_max_connections{provider="jdbc",metalake="test_metalake",catalog="test_catalog",} 10.0
```

#### Entity Change Log Metrics

The `entity-change-log` source exposes each server's change-log processing state through JMX and
`/prometheus/metrics`. For example, `entity-change-log.record-lag` in the metrics registry becomes
`entity_change_log_record_lag` in Prometheus. Gauges read only in-memory values; the poller samples
the database tail once per cycle. If only the tail sample fails, delivery continues and the tail
value remains at its last successful sample.

| Metric suffix                                                | Type and unit            | Meaning                                                                                                                                                     |
| ------------------------------------------------------------ | ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `db-tail-id`, `cursor-id`                                    | gauge, change ID         | Latest sampled database ID and last delivered ID on this server.                                                                                            |
| `record-lag`                                                 | gauge, records           | Sampled tail minus cursor, clamped at zero. Interpret only while tail sampling succeeds.                                                                    |
| `seconds-since-last-successful-poll`                         | gauge, seconds           | Time since a successful database poll, including an empty result; `-1` before the first poll.                                                               |
| `seconds-since-last-successful-tail-sample`                  | gauge, seconds           | Time since `db-tail-id` was last refreshed; `-1` before the first sample. Trust `db-tail-id` and `record-lag` only while this stays near the poll interval. |
| `poll-failures-total`                                        | counter, failures        | Failed poll cycles.                                                                                                                                         |
| `tail-sample-failures-total`                                 | counter, failures        | Failed database-tail samples; fetched batches can still be delivered.                                                                                       |
| `listener-failures-total`, `listener-failures.<class>-total` | counter, failures        | Total failures and failures by registered listener class.                                                                                                   |
| `records-fetched-total`, `records-delivered-total`           | counter, records         | Rows fetched and rows delivered successfully to listeners; one row delivered to two listeners counts twice as delivered.                                    |
| `records-delivered.<class>-total`                            | counter, records         | Successful deliveries by listener class. Lambda and anonymous listeners share the `anonymous` bucket.                                                       |
| `records-applied-total`                                      | counter, invalidations   | Targeted entity-cache invalidations completed successfully; malformed rows and fallback clears do not count.                                                |
| `batch-size-records`                                         | histogram, records       | Number of rows fetched per successful poll, including empty polls.                                                                                          |
| `poll-duration`                                              | timer, duration          | End-to-end poll-cycle duration.                                                                                                                             |
| `invalidation-failures-total`, `fallback-clears-total`       | counter, failures/clears | Failed targeted entity-cache invalidations and successful full-cache recovery clears.                                                                       |

The poller delivers each batch once and has no pending or retry state. A failed listener must
recover locally; its failure counter and log identify the affected listener. The debug logs use
`entityChangeLog` fields to trace an append, poll, delivery, and invalidation. Append logs mean the
row was added to the current transaction, not that the transaction committed.

For an incident, check `seconds-since-last-successful-tail-sample` before comparing `db-tail-id`
with `cursor-id` on the affected server. If it exceeds the poll interval, the tail sample itself is
failing: the retained tail can fall below an advancing cursor and `record-lag` can read zero despite
an unknown database tail. `tail-sample-failures-total` counts those failures for alerting. With a
fresh tail sample, a growing `record-lag` together with an increasing poll age or
`poll-failures-total` points to polling trouble.
If the cursor advances but data remains stale, inspect `listener-failures-total`, `records-delivered.<class>-total`,
`invalidation-failures-total`, and `fallback-clears-total`, then correlate the debug logs by
encoded `fullName` and change ID. The sampled tail and cursor are process-local; each server has
its own values.
