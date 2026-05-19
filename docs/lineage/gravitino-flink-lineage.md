---
title: "Gravitino Flink Lineage support"
slug: /lineage/gravitino-flink-lineage
keyword: Gravitino Flink OpenLineage
license: "This software is licensed under the Apache License version 2."
---

## Overview

By leveraging OpenLineage Flink plugin, Gravitino provides a separate Flink plugin to extract data lineage and transform the dataset identifier to Gravitino identifier. The plugin works with Flink 1.20.3+ (with FLIP-314 backport) and Flink 2.x, supporting both Flink SQL and DataStream API lineage collection.

## Capabilities

- Supports table-level lineage for Flink SQL jobs (automatic, no code changes required).
- Supports DataStream API lineage for connectors implementing `LineageVertexProvider`.
- Supports lineage across different connectors: Paimon, Kafka, upsert-kafka, Hive.
- Supports Gravitino Flink Connector (`GravitinoCatalogStore` mode) and non-Gravitino Flink catalogs.
- Supports job lifecycle events: START, RUNNING, COMPLETE, FAIL, ABORT.
- Supports Kafka Transport for high-throughput lineage event delivery.

## Prerequisites

- Flink 1.20.3 with FLIP-314 backport (provides `JobStatusChangedListener` + `LineageGraph` runtime), or Flink 2.x.
- Gravitino Flink Connector (optional, for `GravitinoCatalogStore` mode).

## Gravitino dataset

The Gravitino OpenLineage Flink plugin transforms the Gravitino metalake name into the dataset namespace. The dataset name follows the format `${catalogName}.${databaseName}.${tableName}`.

### Flink SQL path (TableLineageDataset)

For Flink SQL jobs, the Planner automatically populates `TableLineageDataset` with catalog context and object path. The plugin extracts the catalog name from `CatalogContext` and resolves it via `catalogMappings` if configured.

| Dataset Type    | Dataset name                                  | Example                          | Since Version |
|-----------------|-----------------------------------------------|----------------------------------|---------------|
| Paimon catalog  | `${catalogName}.${databaseName}.${tableName}` | `paimon_catalog.ods.user_events` | 1.3.0         |
| Kafka catalog   | `${catalogName}.${databaseName}.${tableName}` | `kafka_catalog.default.events`   | 1.3.0         |
| Hive catalog    | `${catalogName}.${databaseName}.${tableName}` | `hive_catalog.warehouse.orders`  | 1.3.0         |
| JDBC catalog    | `${catalogName}.${databaseName}.${tableName}` | `jdbc_catalog.public.users`      | 1.3.0         |
| Default catalog | `${catalogName}.${databaseName}.${tableName}` | `default_catalog.default.table1` | 1.3.0         |

### DataStream path (LineageVertexProvider)

For DataStream API jobs, connectors that implement `LineageVertexProvider` produce `LineageDataset` instances. The plugin infers the connector type from the dataset namespace scheme and resolves the Gravitino catalog name.

| Connector namespace | Inferred type | Example dataset name                  | Since Version |
|---------------------|---------------|---------------------------------------|---------------|
| `kafka://...`       | kafka         | `kafka.default.topic_name`            | 1.3.0         |
| `hive://...`        | hive          | `hive.warehouse.orders`               | 1.3.0         |
| `thrift://...`      | hive          | `hive.db.table1`                      | 1.3.0         |
| `jdbc:...`          | jdbc          | `jdbc.public.users`                   | 1.3.0         |
| `paimon://...`      | paimon        | `paimon.ods.user_events`              | 1.3.0         |
| Other               | unknown       | `unknown.default.dataset_name`        | 1.3.0         |

### Connector support matrix

| Connector                      | SQL lineage | DataStream lineage | Notes                                                             |
|--------------------------------|-------------|--------------------|-------------------------------------------------------------------|
| Paimon                         | Yes         | No                 | DataStream: community has not implemented `LineageVertexProvider` |
| Kafka                          | Yes         | Yes                | DataStream requires `flink-connector-kafka:3.4.0+`                |
| upsert-kafka                   | Yes         | Yes                | Same as Kafka                                                     |
| Hive                           | Yes         | Yes                | DataStream requires FLINK-35326 backport                          |
| JDBC (MySQL, PostgreSQL, etc.) | Yes         | Yes                | DataStream requires `flink-connector-jdbc:3.3.0+`                 |
| Iceberg                        | Yes         | No                 | DataStream: not yet implemented                                   |
| Elasticsearch                  | Yes         | Partial            | Community has partial `LineageVertexProvider` support             |
| StarRocks                      | Yes         | No                 | Third-party connector, not yet implemented                        |
| Doris                          | Yes         | No                 | Third-party connector, not yet implemented                        |
| Other                          | Yes         | Depends            | SQL lineage works for any connector registered via CatalogManager |

## How to use

### Step 1: Download and place the plugin jar

Download [Gravitino OpenLineage Flink plugin jar](https://github.com/datastrato/gravitino-openlineage-plugins/tree/main/flink-plugin/) and place it in the Flink `lib/` directory.

For Kafka Transport, also add `kafka-clients` jar to the Flink `lib/` directory.

### Step 2: Configure Flink

Add the following to `flink-conf.yaml` (Flink 1.20) or `config.yaml` (Flink 1.20.3+):

```yaml
# Register the Gravitino OpenLineage Listener Factory
execution.job-status-changed-listeners: io.openlineage.flink.listener.GravitinoOpenLineageListenerFactory

# Gravitino lineage configuration
openlineage.gravitino.metalake: ${metalakeName}
openlineage.gravitino.useGravitinoIdentifier: true
openlineage.job.namespace: ${metalakeName}
```

### Step 3: Configure OpenLineage Transport

Create an `openlineage.yml` file and set the `OPENLINEAGE_CONFIG` environment variable to point to it.

#### Kafka Transport (recommended for production)

```yaml
# openlineage.yml
transport:
  type: kafka
  topicName: lineage-events
  properties:
    bootstrap.servers: ${kafka-bootstrap-servers}
    key.serializer: org.apache.kafka.common.serialization.StringSerializer
    value.serializer: org.apache.kafka.common.serialization.StringSerializer
```

Set the environment variable before starting Flink:

```bash
export OPENLINEAGE_CONFIG=/path/to/openlineage.yml
```

#### Console Transport (for debugging)

```yaml
# openlineage.yml
transport:
  type: console
```

#### HTTP Transport

```yaml
# openlineage.yml
transport:
  type: http
  url: http://your-lineage-server:8090
  endpoint: /api/lineage
```

### Step 4: Start Flink and submit jobs

No code changes are required. Any Flink SQL job will automatically produce lineage events:

```sql
INSERT INTO paimon_catalog.ods.target SELECT * FROM kafka_catalog.default.input;
```

This produces an OpenLineage RunEvent with:
- `inputs[0].namespace = "${metalakeName}"`, `inputs[0].name = "kafka_catalog.default.input"`
- `outputs[0].namespace = "${metalakeName}"`, `outputs[0].name = "paimon_catalog.ods.target"`

## Configuration reference

<table>
  <thead>
    <tr>
      <th>Configuration item</th>
      <th>Description</th>
      <th>Default value</th>
      <th>Required</th>
      <th>Since Version</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>openlineage.gravitino.metalake</code></td>
      <td>The Gravitino metalake name used as the dataset namespace. If not set, falls back to <code>table.catalog-store.gravitino.gravitino.metalake</code> (Gravitino Flink Connector config).</td>
      <td>None</td>
      <td>Yes</td>
      <td>1.3.0</td>
    </tr>
    <tr>
      <td><code>openlineage.gravitino.useGravitinoIdentifier</code></td>
      <td>Whether to convert dataset identifiers to Gravitino format. If set to false, the original OpenLineage identifiers (physical addresses) are used.</td>
      <td>true</td>
      <td>No</td>
      <td>1.3.0</td>
    </tr>
    <tr>
      <td><code>openlineage.gravitino.catalogMappings</code></td>
      <td>Catalog name mapping rules when the Flink catalog registration name differs from the Gravitino catalog name. Format: <code>origin1:gravitino1,origin2:gravitino2</code>. For example, <code>kafka_raw:kafka_catalog</code> maps <code>kafka_raw</code> to <code>kafka_catalog</code>.</td>
      <td>None</td>
      <td>No</td>
      <td>1.3.0</td>
    </tr>
    <tr>
      <td><code>openlineage.job.namespace</code></td>
      <td>The OpenLineage job namespace. Typically set to the metalake name.</td>
      <td>flink-jobs</td>
      <td>No</td>
      <td>1.3.0</td>
    </tr>
    <tr>
      <td><code>execution.job-status-changed-listeners</code></td>
      <td>The Flink listener factory class. Must be set to <code>io.openlineage.flink.listener.GravitinoOpenLineageListenerFactory</code>.</td>
      <td>None</td>
      <td>Yes</td>
      <td>1.3.0</td>
    </tr>
  </tbody>
</table>

## OpenLineage event example

When executing `INSERT INTO blackhole_sink SELECT * FROM datagen_source`, the plugin produces:

```json
{
  "eventType": "START",
  "eventTime": "2026-05-19T14:46:46.199Z",
  "producer": "https://github.com/OpenLineage/OpenLineage/tree/1.45.0-SNAPSHOT/integration/flink",
  "job": {
    "namespace": "test_metalake",
    "name": "insert-into_default_catalog.default_database.blackhole_sink",
    "facets": {
      "jobType": {
        "processingType": "BATCH",
        "integration": "FLINK",
        "jobType": "JOB"
      }
    }
  },
  "run": {
    "runId": "019e3efc-a634-7815-bd97-87cfe5776d0f",
    "facets": {
      "processing_engine": {
        "version": "1.20.3",
        "name": "flink"
      },
      "flink_job": {
        "jobId": "f5abfe8e13506475f8eb907c0b643475"
      }
    }
  },
  "inputs": [
    {
      "namespace": "test_metalake",
      "name": "default_catalog.default_database.datagen_source",
      "facets": {
        "schema": {
          "fields": [
            {"name": "id", "type": "BIGINT"},
            {"name": "name", "type": "STRING"}
          ]
        }
      }
    }
  ],
  "outputs": [
    {
      "namespace": "test_metalake",
      "name": "default_catalog.default_database.blackhole_sink",
      "facets": {
        "schema": {
          "fields": [
            {"name": "id", "type": "BIGINT"},
            {"name": "name", "type": "STRING"}
          ]
        }
      }
    }
  ]
}
```

## Job lifecycle events

| Flink event                         | OpenLineage event type |
|-------------------------------------|------------------------|
| `JobCreatedEvent`                   | `START`                |
| `JobExecutionStatusEvent(RUNNING)`  | `RUNNING`              |
| `JobExecutionStatusEvent(FINISHED)` | `COMPLETE`             |
| `JobExecutionStatusEvent(FAILED)`   | `FAIL`                 |
| `JobExecutionStatusEvent(CANCELED)` | `ABORT`                |

## Known limitations

- **Column lineage**: Flink Planner has not yet populated `TableColumnLineageEdge` field mappings. The interface is defined but data is empty. Column lineage will be automatically available once the Flink community fills this in.
- **Paimon DataStream**: Paimon community has not implemented `LineageVertexProvider`. Paimon SQL lineage works normally.
- **Application Mode kill -9**: `COMPLETE`/`FAIL` events may be lost if the JVM is forcefully killed. Downstream consumers should implement timeout detection.

## Architecture

```
Flink SQL / DataStream API
        │
        ▼
  StreamGraph (LineageGraph, populated by FLIP-314)
        │
        ▼
  GravitinoOpenLineageListenerFactory
    └─ GravitinoInfoLoader.load(config) → GravitinoInfo (immutable, thread-safe)
    └─ GravitinoVisitorFactory (injects GravitinoInfo into Visitors)
        │
        ▼
  OpenLineageJobStatusChangedListener
    └─ GravitinoTableDatasetIdentifierVisitor (SQL path)
    └─ GravitinoStreamDatasetIdentifierVisitor (DataStream path)
        │
        ▼
  OpenLineage KafkaTransport / ConsoleTransport / HttpTransport
        │
        ▼
  Kafka Topic / Console / HTTP Endpoint
```

The plugin uses constructor injection (no ThreadLocal) to pass `GravitinoInfo` from the Listener creation thread to the event processing thread, ensuring thread safety in all deployment modes.
