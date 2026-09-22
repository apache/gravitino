---
title: "Flink Lineage"
slug: "/lineage/gravitino-flink-lineage"
keyword: "Gravitino Flink OpenLineage"
license: "This software is licensed under the Apache License version 2."
---

## Overview

By leveraging OpenLineage Flink plugin, Gravitino provides separate Flink plugins to extract data
lineage and transform the dataset identifier to Gravitino identifier.

Gravitino offers **two plugin variants** to cover different Flink versions:

| Plugin            | Flink Version                                    | SQL Lineage | DataStream Lineage | User Code Change      |
|-------------------|--------------------------------------------------|-------------|--------------------|-----------------------|
| **Flink2 plugin** | Flink 2.x, or Flink 1.20+ with FLIP-314 backport | Yes         | Yes                | None (config only)    |
| **Flink1 plugin** | Flink 1.18 / 1.19 / 1.20 (native, no backport)   | No          | Yes                | `registerJobListener` |

## Capabilities

- Supports table-level lineage for Flink SQL jobs (Flink2 plugin only).
- Supports DataStream API lineage for Kafka, JDBC, Iceberg, and Cassandra sources/sinks, plus any
  connector implementing the OpenLineage `LineageVertexProvider` interface.
- Supports lineage across different catalogs like Paimon, Kafka, Hive, JDBC, Iceberg, etc. (via SQL
  lineage).
- Supports Gravitino Flink Connector and non-Gravitino Flink catalogs.
- Supports physical address resolution via DDL connector options and the Gravitino REST API
  (populates the `symlinks` facet).
- Supports job lifecycle events: START, COMPLETE, FAIL, ABORT.
- Supports disabling checkpoint tracking to reduce excessive RUNNING events.
- Supports the standard OpenLineage transports (`kafka`, `http`, `console`) for event delivery,
  including sending events to the Gravitino server's lineage endpoint via the `http` transport.

## Gravitino Dataset

The Gravitino OpenLineage Flink plugin transforms the Gravitino metalake name into the dataset
namespace. The dataset name follows the format `${catalogName}.${databaseName}.${tableName}`.

When using the [Gravitino Flink Connector](../flink-connector/flink-connector.md) to access tables
managed by Gravitino, the dataset name follows this format:

| Dataset Type    | Dataset name                                         | Example                          | Since Version |
|-----------------|------------------------------------------------------|----------------------------------|---------------|
| Paimon catalog  | `${GravitinoCatalogName}.${schemaName}.${tableName}` | `paimon_catalog.ods.user_events` | 2.0.0         |
| Kafka catalog   | `${GravitinoCatalogName}.${schemaName}.${tableName}` | `kafka_catalog.default.events`   | 2.0.0         |
| Hive catalog    | `${GravitinoCatalogName}.${schemaName}.${tableName}` | `hive_catalog.warehouse.orders`  | 2.0.0         |
| JDBC catalog    | `${GravitinoCatalogName}.${schemaName}.${tableName}` | `jdbc_catalog.public.users`      | 2.0.0         |
| Iceberg catalog | `${GravitinoCatalogName}.${schemaName}.${tableName}` | `iceberg_catalog.db.table1`      | 2.0.0         |

For datasets not managed by Gravitino (e.g., `default_catalog`), the dataset name is:

| Dataset Type    | Dataset name                                | Example                            | Since Version |
|-----------------|---------------------------------------------|------------------------------------|---------------|
| Default catalog | `${catalogName}.${schemaName}.${tableName}` | `default_catalog.default.my_table` | 2.0.0         |

The dataset `name` field uses the **Flink logical table name** (as registered in CatalogManager),
not the underlying physical resource name. For example:

```sql
CREATE TABLE kafka_catalog.`default`.user_events (...)
  WITH ('connector' = 'kafka', 'topic' = 'raw_user_events_v2');
```

The lineage output will be `name = "kafka_catalog.default.user_events"` (logical name), not `raw_user_events_v2` (physical topic name).

### Physical address in symlinks

Besides the logical Gravitino identifier, the plugin also populates a `symlinks` facet with the
underlying physical address (Kafka brokers, JDBC URL, Hive metastore URI, warehouse path, etc.).
This allows downstream consumers to correlate logical Gravitino identifiers with physical cluster
addresses for quick troubleshooting.

The physical address is resolved with a fixed priority: **table-level** (from the DDL `WITH`
options, no Gravitino server needed) > **catalog-level** (from the Gravitino REST API, reusing the
`table.catalog-store.gravitino.*` connector configuration) > empty.

```json
"symlinks": {
  "identifiers": [
    {
      "namespace": "kafka://broker1:9092",
      "name": "raw_user_events_v2",
      "type": "TABLE"
    }
  ]
}
```

**Table-level resolution** (no Gravitino Server needed):

| Connector                | Physical namespace            | Physical name | Source            |
|--------------------------|-------------------------------|---------------|-------------------|
| `kafka` / `upsert-kafka` | `kafka://<bootstrap.servers>` | topic name    | DDL `WITH` clause |
| `jdbc`                   | JDBC URL                      | table-name    | DDL `WITH` clause |

**Catalog-level resolution** (requires Gravitino Server):

| Provider                         | Physical address source | Example                        |
|----------------------------------|-------------------------|--------------------------------|
| `hive`                           | `metastore.uris`        | `thrift://metastore:9083`      |
| `jdbc-mysql` / `jdbc-postgresql` | `jdbc-url`              | `jdbc:mysql://host:3306`       |
| `lakehouse-paimon`               | `warehouse`             | `file:///tmp/paimon-warehouse` |
| `lakehouse-iceberg`              | `uri` or `warehouse`    | `thrift://metastore:9083`      |

:::note
Catalog-level resolution is skipped for Flink's built-in `default_catalog` (it never exists in a
Gravitino metalake). When the Gravitino server has authentication enabled, the resolver reuses the
Gravitino Flink Connector client config (`table.catalog-store.gravitino.gravitino.client.auth.type`
and related keys); with no `auth.type` configured it falls back to simple auth using the current
Hadoop/JVM user, matching the connector's behavior.
:::

### Connector support matrix

| Connector            | SQL lineage (Flink2) | DataStream lineage | Notes                                                                                                                                                                                      |
|----------------------|----------------------|--------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Paimon               | Yes                  | No                 | Paimon has not implemented `LineageVertexProvider`                                                                                                                                         |
| Kafka / upsert-kafka | Yes                  | Yes                | DataStream visitor requires `flink-connector-kafka` on the classpath                                                                                                                       |
| Hive                 | Yes                  | No                 | SQL lineage carries the table identifier; the Hive source exposes a `DataStreamScanProvider`, which the Flink Planner does not extract a lineage vertex from, so no rich DataStream facets |
| JDBC                 | Yes                  | Yes (Flink1)       | DataStream via the community `JdbcSourceVisitor`                                                                                                                                           |
| Iceberg              | Yes                  | Yes (Flink1)       | DataStream via the community `IcebergSourceVisitor` / `IcebergSinkVisitor`                                                                                                                 |
| Cassandra            | N/A                  | Yes (Flink1)       | DataStream via the community `CassandraSourceVisitor` / `CassandraSinkVisitor`                                                                                                             |
| Other                | Yes                  | Depends            | SQL lineage works for any connector registered via CatalogManager                                                                                                                          |


## Getting Started (Flink2 plugin)

1. Download [Gravitino OpenLineage Flink2 plugin jar](https://github.com/datastrato/gravitino-openlineage-plugins/tree/main/flink-plugin/) (`openlineage-flink2-<version>.jar`) and place it in the Flink `lib/` directory.
2. For Kafka transport, ensure `kafka-clients` jar is in Flink `lib/`.
3. Add configuration to Flink to enable lineage collection.

Configuration example for Flink `config.yaml`:

```yaml
# Register the listener
execution.job-status-changed-listeners: io.openlineage.flink.listener.GravitinoOpenLineageListenerFactory

# Gravitino lineage configuration
openlineage.gravitino.metalake: ${metalakeName}
openlineage.gravitino.useGravitinoIdentifier: true
openlineage.job.namespace: ${metalakeName}

# Kafka transport
openlineage.transport.type: kafka
openlineage.transport.topicName: lineage-events
openlineage.transport.properties.bootstrap.servers: ${bootstrap-servers}
openlineage.transport.properties.key.serializer: org.apache.kafka.common.serialization.StringSerializer
openlineage.transport.properties.value.serializer: org.apache.kafka.common.serialization.StringSerializer
openlineage.transport.properties.acks: all
openlineage.transport.properties.retries: 5

# Disable checkpoint tracking (optional, reduces excessive RUNNING events)
# openlineage.flink.disableCheckpointTracking: true
```

:::note
The `key.serializer` and `value.serializer` properties are **required** for Kafka transport. The
plugin passes properties directly to `KafkaProducer` without setting defaults.
:::

To send lineage events directly to the Gravitino server instead of Kafka, use the `http` transport
pointing at the Gravitino lineage endpoint (`/api/lineage`):

```yaml
openlineage.transport.type: http
openlineage.transport.url: http://${gravitino-host}:${gravitino-port}
openlineage.transport.endpoint: /api/lineage
```

This requires the Gravitino server to have its lineage HTTP source enabled (it is the default
`gravitino.lineage.source`). See [Server Lineage](./gravitino-server-lineage.md) for the
server-side configuration.

Alternatively, transport can be configured via an external `openlineage.yml` file:

```bash
export OPENLINEAGE_CONFIG=/path/to/openlineage.yml
```

```yaml
transport:
  type: kafka
  topicName: lineage-events
  properties:
    bootstrap.servers: broker1:9092,broker2:9092
    key.serializer: org.apache.kafka.common.serialization.StringSerializer
    value.serializer: org.apache.kafka.common.serialization.StringSerializer
    acks: all
```

Refer to [OpenLineage Flink guides](https://openlineage.io/docs/integrations/flink/) and
[Gravitino Flink Connector](../flink-connector/flink-connector.md) for more details.

## Getting Started (Flink1 plugin)

1. Download [Gravitino OpenLineage Flink1 plugin jar](https://github.com/datastrato/gravitino-openlineage-plugins/tree/main/flink-plugin/) (`openlineage-flink1-<version>.jar`) and place it in the Flink `lib/` directory.
2. Register the listener in your application code:

```java
import io.openlineage.flink.GravitinoOpenLineageFlinkJobListener;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.registerJobListener(
    GravitinoOpenLineageFlinkJobListener.builder()
        .executionEnvironment(env)
        .build());

// Your DataStream job logic...
env.execute("My Flink Job");
```

3. Add configuration to Flink:

```yaml
execution.job-listener.openlineage.namespace: ${metalakeName}
execution.job-listener.openlineage.job-name: ${jobName}
```

4. Set `OPENLINEAGE_CONFIG` environment variable pointing to `openlineage.yml` (same format as Flink2).

### Flink1 plugin limitations

- **No SQL lineage**: Only DataStream API jobs produce lineage events.
- **Requires code change**: Must call `env.registerJobListener(...)`.
- **No automatic connector detection**: Only connectors with explicit Visitor implementations.

## Configuration Reference

The configurations are grouped by purpose. Unless noted otherwise, they apply to both plugins.

### Gravitino identifier configuration

| Configuration item                             | Description                                                                                                                                                                                                                                                                                                                                                    | Default value | Required | Since Version |
|------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `openlineage.gravitino.metalake`               | The Gravitino metalake name used as the dataset namespace. If not set, falls back to `table.catalog-store.gravitino.gravitino.metalake`.                                                                                                                                                                                                                       | None          | Yes      | 2.0.0         |
| `openlineage.gravitino.useGravitinoIdentifier` | Whether to transform the dataset identifier to the Gravitino format. When `false`, the original OpenLineage identifier is used (e.g. `kafka://broker:9092` as the namespace for Kafka tables); note that for the Paimon/Hive/JDBC SQL path the original namespace is empty, since the Flink Planner does not populate physical addresses for these connectors. | `true`        | No       | 2.0.0         |
| `openlineage.gravitino.catalogMappings`        | Catalog name mapping rules when the Flink catalog registration name differs from the Gravitino catalog name. Format: `origin1:gravitino1,origin2:gravitino2`.                                                                                                                                                                                                  | None          | No       | 2.0.0         |

### Flink2 plugin configuration

| Configuration item                            | Description                                                                                                                             | Default value | Required | Since Version |
|-----------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `execution.job-status-changed-listeners`      | Flink listener factory class. Set to `io.openlineage.flink.listener.GravitinoOpenLineageListenerFactory`.                               | None          | Yes      | 2.0.0         |
| `openlineage.job.namespace`                   | The OpenLineage job namespace. Typically set to the metalake name.                                                                      | `flink-jobs`  | No       | 2.0.0         |
| `openlineage.flink.disableCheckpointTracking` | When `true`, disables checkpoint tracking to reduce excessive RUNNING events. Only START, COMPLETE, FAIL, and ABORT events are emitted. | `false`       | No       | 2.0.0         |

### Flink1 plugin configuration

| Configuration item                             | Description                    | Default value         | Required | Since Version |
|------------------------------------------------|--------------------------------|-----------------------|----------|---------------|
| `execution.job-listener.openlineage.namespace` | The OpenLineage job namespace. | `flink_jobs`          | No       | 2.0.0         |
| `execution.job-listener.openlineage.job-name`  | The OpenLineage job name.      | `Flink Streaming Job` | No       | 2.0.0         |

### Transport configuration

These are standard OpenLineage transport options; the Gravitino plugin passes them through unchanged.

| Configuration item                   | Description                                                                                                                               | Default value | Required    | Since Version |
|--------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|---------------|-------------|---------------|
| `openlineage.transport.type`         | Transport type: `kafka`, `http`, or `console`. Use `http` to send events to the Gravitino server.                                         | None          | Yes         | 2.0.0         |
| `openlineage.transport.topicName`    | Kafka topic name.                                                                                                                         | None          | Yes (Kafka) | 2.0.0         |
| `openlineage.transport.properties.*` | Kafka producer properties passed directly to `KafkaProducer`. Must include `bootstrap.servers`, `key.serializer`, and `value.serializer`. | None          | Yes (Kafka) | 2.0.0         |
| `openlineage.transport.url`          | Target server URL.                                                                                                                        | None          | Yes (HTTP)  | 2.0.0         |
| `openlineage.transport.endpoint`     | Endpoint path appended to the URL. For the Gravitino server, use `/api/lineage`.                                                          | None          | Yes (HTTP)  | 2.0.0         |

## OpenLineage Event Example

When executing `INSERT INTO paimon_catalog.ods.target SELECT * FROM paimon_catalog.ods.source`
(Flink2 plugin with `GravitinoCatalogStore`), the plugin produces:

```json
{
  "eventType": "START",
  "eventTime": "2026-08-11T14:01:30.776Z",
  "producer": "https://github.com/OpenLineage/OpenLineage/tree/1.45.0-SNAPSHOT/integration/flink",
  "job": {
    "namespace": "dip_metalake",
    "name": "insert-into_paimon_catalog.ods.target",
    "facets": {
      "jobType": {
        "processingType": "BATCH",
        "integration": "FLINK",
        "jobType": "JOB"
      }
    }
  },
  "run": {
    "runId": "ae8a14e8-513c-fe9a-fde9-91fe1f35d2b3",
    "facets": {
      "processing_engine": {
        "version": "1.20.3",
        "name": "flink"
      },
      "flink_job": {
        "jobId": "fde991fe1f35d2b3ae8a14e8513cfe9a"
      }
    }
  },
  "inputs": [
    {
      "namespace": "dip_metalake",
      "name": "paimon_catalog.ods.source",
      "facets": {
        "schema": {
          "fields": [
            {"name": "user_id", "type": "BIGINT"},
            {"name": "event_type", "type": "STRING"}
          ]
        },
        "symlinks": {
          "identifiers": [
            {
              "namespace": "file:///tmp/paimon-warehouse",
              "name": "paimon_catalog.ods.source",
              "type": "TABLE"
            }
          ]
        }
      }
    }
  ],
  "outputs": [
    {
      "namespace": "dip_metalake",
      "name": "paimon_catalog.ods.target",
      "facets": {
        "schema": {
          "fields": [
            {"name": "user_id", "type": "BIGINT"},
            {"name": "event_type", "type": "STRING"}
          ]
        },
        "symlinks": {
          "identifiers": [
            {
              "namespace": "file:///tmp/paimon-warehouse",
              "name": "paimon_catalog.ods.target",
              "type": "TABLE"
            }
          ]
        }
      }
    }
  ]
}
```

The `flink_job.jobId` field is the same ID visible in the Flink Web UI and REST API, enabling
correlation between lineage events and Flink job monitoring.

## Job Lifecycle Events

| Flink event                         | OpenLineage event type | Plugin | Notes                                   |
|-------------------------------------|------------------------|--------|-----------------------------------------|
| `JobCreatedEvent`                   | `START`                | Flink2 | Emitted when the job graph is submitted |
| `JobExecutionStatusEvent(FINISHED)` | `COMPLETE`             | Flink2 | Job completed successfully              |
| `JobExecutionStatusEvent(FAILED)`   | `FAIL`                 | Flink2 | Job failed with an exception            |
| `JobExecutionStatusEvent(CANCELED)` | `ABORT`                | Flink2 | Job was canceled                        |
| `onJobSubmitted`                    | `START`                | Flink1 |                                         |
| `onJobExecuted` (success)           | `COMPLETE`             | Flink1 |                                         |
| `onJobExecuted` (failure)           | `FAIL`                 | Flink1 |                                         |

The run ID (`run.runId`) is derived deterministically from the Flink Job ID. This ensures that the
`START` event and the terminal event (`COMPLETE`/`FAIL`) of the same job always share the same
`runId`, allowing downstream consumers to correlate events into a single run.

## Fault Tolerance

Lineage collection is best-effort and is designed to **never break the user's Flink job**:

- If the listener fails to initialize (for example, an invalid transport configuration), it
  degrades to a no-op listener and the job continues; a `LINEAGE DISABLED` error is logged.
- Physical address resolution failures (missing DDL properties, connector not configured, Gravitino
  server unreachable, authentication errors) are caught and result in an empty `symlinks` facet,
  never an exception.
- Failed Gravitino catalog lookups are cached, so they are not retried per dataset.

If lineage does not appear as expected, check the JobManager logs (the listener runs on the
JobManager) with the `io.openlineage.flink` logger set to `DEBUG`.

## Known Limitations

- **Anonymous sources (temporary tables, views, inline/DataStream-derived tables)**: The Flink
  Planner reports an anonymous `ContextResolvedTable` for these, so its object path is `null` and
  the dataset identifier cannot be resolved to a real `catalog.database.table` name. This affects
  sources declared with `CREATE TEMPORARY TABLE`, `CREATE VIEW`, or in-memory tables. Sinks are not
  affected. This is a Flink Planner behavior, not a
  plugin issue. Reference real catalog tables directly to get resolvable lineage.
- **Column lineage**: Flink Planner has not yet populated `TableColumnLineageEdge` field mappings.
  Column lineage will be automatically available once the Flink community fills this in.
- **Paimon DataStream**: Paimon has not implemented `LineageVertexProvider`. Paimon SQL lineage
  works normally.
- **Hive DataStream facets**: The Hive source exposes a `DataStreamScanProvider`, from which the
  Flink Planner does not extract a lineage vertex. Hive SQL lineage still carries the table
  identifier, but no connector-specific DataStream facets are produced.
- **Flink1 plugin - no SQL lineage**: The Flink1 plugin only supports DataStream API.
- **Forceful termination**: If the JobManager process is terminated abruptly (for example
  `SIGKILL`, an OOM kill, or a pod eviction), the terminal `COMPLETE`/`FAIL` event may not be
  emitted. Downstream consumers that need to detect such cases can rely on a timeout since the last
  `START` event.
- **Physical address (symlinks)**: Catalog-level resolution requires `GravitinoCatalogStore`
  configuration. If the Gravitino server is unreachable or the catalog does not exist, symlinks will
  be empty but lineage collection continues.
