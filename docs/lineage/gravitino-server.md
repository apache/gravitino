---
title: Gravitino server Lineage support
slug: /lineage/gravitino-server-lineage
keyword: Gravitino OpenLineage
license: This software is licensed under the Apache License version 2.
---

## Overview

Gravitino server provides a pluggable lineage framework to receive, process, and sink OpenLineage events.
By leveraging this, you could do custom processing for the lineage events and sink to your dedicated systems.

## Lineage Configuration

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.lineage.source</tt></td>
  <td>The name of lineage event source.</td>
  <td><tt>http</tt></td>
  <td>No</td>
  <td>`0.9.0-incubating`</td>
</tr>

<tr>
  <td><tt>gravitino.lineage.\$\{sourceName}.sourceClass</tt></td>
  <td>
    The name of the lineage source class that implements the `org.apache.gravitino.lineage.source.LineageSource` interface.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.lineage.processorClass</tt></td>
  <td>
    The name of the lineage processor class that implements the `org.apache.gravitino.lineage.processor.LineageProcessor` interface.
    The default `NoopProcessor` processor do nothing about the run event.
  </td>
  <td><tt>org.apache.gravitino.lineage.processor.NoopProcessor</tt></td>
  <td>No</td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.lineage.sinks</tt></td>
  <td>
    The Lineage event sink names.
    Multiple sinks can be specified using comma-separated strings.
  </td>
  <td><tt>log</tt></td>
  <td>No</td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.lineage.\$\{sinkName}.sinkClass</tt></td>
  <td>
    The name of the lineage sink class that implements the `org.apache.gravitino.lineage.sink.LineageSink` interface.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.lineage.queueCapacity</tt></td>
  <td>
    The total capacity of lineage event queues.
    When there are more than one lineage sinks, each sink utilizes an isolated event queue.
    The capacity of each queue is calculated by dividing the value of `gravitino.lineage.queueCapacity` by the number of sinks.
  </td>
  <td><tt>10000</tt></td>
  <td>No</td>
  <td>`0.9.0-incubating`</td>
</tr>
</tbody>
</table>

## Lineage HTTP source 

A HTTP source provides an endpoint that implements the [OpenLineage API](https://openlineage.io/apidocs/openapi/).
The HTTP source can receive OpenLineage run event.

The following is an example:

```shell
cat <<EOF >source.json
{
  "eventType": "START",
  "eventTime": "2023-10-28T19:52:00.001+10:00",
  "run": {
    "runId": "0176a8c2-fe01-7439-87e6-56a1a1b4029f"
  },
  "job": {
    "namespace": "gravitino-namespace",
    "name": "gravitino-job1"
  },
  "inputs": [{
    "namespace": "gravitino-namespace",
    "name": "gravitino-table-identifier"
  }],
  "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
  "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent"
}
EOF

curl -X POST \
  -i -H 'Content-Type: application/json' \
  -d '@source.json' \
  http://localhost:8090/api/lineage
```

## Lineage log sink

Log sink prints the log in a separate log file `gravitino_lineage.log`.
The default logging behavior can be customized using the `conf/log4j2.properties` file.

## High watermark status

When the lineage sink operates slowly, lineage events may get accumulated in the async queue.
Once the queue size exceeds 90% of its capacity (high watermark threshold),
the lineage system enters a _high watermark_ status.
In this state, the lineage source must implement retry and logging mechanisms for rejected events to prevent system overload.
For the HTTP source, it returns the `429 Too Many Requests` status code to the client.

