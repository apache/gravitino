---
title: "Gravitino server Lineage support"
slug: /lineage/gravitino-server-lineage
keyword: Gravitino OpenLineage
license: "This software is licensed under the Apache License version 2."
---

## Overview

Gravitino server provides a pluggable lineage framework to receive, process, and sink OpenLineage events. By leveraging this, you could do custom process for the lineage event and sink to your dedicated systems.

## Lineage Configuration

| Configuration item                            | Description                                                                                                                                                                                                                                                | Default value                                          | Required | Since Version    |
|-----------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------|----------|------------------|
| `gravitino.lineage.source`                    | The name of lineage event source.                                                                                                                                                                                                                          | http                                                   | No       | 0.9.0-incubating |
| `gravitino.lineage.${sourceName}.sourceClass` | The name of the lineage source class which should implement `org.apache.gravitino.lineage.source.LineageSource` interface.                                                                                                                                 | (none)                                                 | No       | 0.9.0-incubating |
| `gravitino.lineage.processorClass`            | The name of the lineage processor class which should implement `org.apache.gravitino.lineage.processor.LineageProcessor` interface. The default noop processor do nothing about the run event.                                                             | `org.apache.gravitino.lineage.processor.NoopProcessor` | No       | 0.9.0-incubating |
| `gravitino.lineage.sinks`                     | The Lineage event sink names (support multiple sinks separated by commas).                                                                                                                                                                                 | log                                                    | No       | 0.9.0-incubating |
| `gravitino.lineage.${sinkName}.sinkClass`     | The name of the lineage sink class which should implement `org.apache.gravitino.lineage.sink.LineageSink` interface.                                                                                                                                       | (none)                                                 | No       | 0.9.0-incubating |
| `gravitino.lineage.queueCapacity`             | The total capacity of lineage event queues. When there are multiple lineage sinks, each sink utilizes an isolated event queue. The capacity of each queue is calculated by dividing the value of `gravitino.lineage.queueCapacity` by the number of sinks. | 10000                                                  | No       | 0.9.0-incubating |

## Lineage http source 

Http source provides an endpoint which follows [OpenLineage API spec](https://openlineage.io/apidocs/openapi/) to receive OpenLineage run event. The following use example:

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

Log sink prints the log in a separate log file `gravitino_lineage.log`, you could change the default behavior in `conf/log4j2.properties`.

## Lineage HTTP sink

The HTTP sink supports sending the lineage event to an HTTP server that follows the OpenLineage REST specification, like marquez
| Property Name                     | Description                                                                                                                            | Default Value                                      | Required | Since Version |
|-----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------|----------|---------------|
| gravitino.lineage.sinks           | Specifies the lineage sink implementation to use. For http sink `http`.                                                                | `log`                                              | Yes      | 0.9.0         |
| gravitino.lineage.http.sinkClass  | Fully qualified class name of the http sink lineage sink implementation  `org.apache.gravitino.lineage.sink.LineageHttpSink`)          | `org.apache.gravitino.lineage.sink.LineageLogSink` | Yes      | 0.9.0         |
| gravitino.lineage.http.url        | URL of the http sink server endpoint for lineage collection(e.g., `http://localhost:5000`)                                             | none                                               | Yes      | 1.0.0         |
| gravitino.lineage.http.authType   | Authentication type for http sink (options: `apiKey` or `none`)                                                                        | none                                               | Yes      | 1.0.0         |
| gravitino.lineage.http.apiKey     | API key for authenticating with http sink (required if authType=`apiKey`)                                                              | none                                               | No       | 1.0.0         |

## High watermark status

When the lineage sink operates slowly, lineage events accumulate in the async queue. Once the queue size exceeds 90% of its capacity (high watermark threshold), the lineage system enters a high watermark status. In this state, the lineage source must implement retry and logging mechanisms for rejected events to prevent system overload. For the HTTP source, it returns the `429 Too Many Requests` status code to the client.