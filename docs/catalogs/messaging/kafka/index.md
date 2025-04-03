---
title: "Kafka catalog"
slug: /kafka-catalog
date: 2024-4-22
keyword: kafka catalog
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Kafka catalog is a messaging catalog for managing the metadata
for Apache Kafka topics.
One Kafka catalog corresponds to one Kafka cluster.

## Catalog

### Catalog properties

Besides the [common catalog properties](../../../admin/server-config.md#apache-gravitino-catalog-properties-configuration),
the Kafka catalog has the following properties:

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
  <td><tt>bootstrap.servers</tt></td>
  <td>
    The Kafka broker(s) to connect to.
    Multiple brokers can be specified using comma-separated list.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.5.0`</td>
</tr>
<tr>
  <td><tt>gravitino.bypass.&#42;</tt></td>
  <td>
    Property name with this prefix passed down to the underlying Kafka Admin client for use.
    Refer to [Kafka Admin Configs](https://kafka.apache.org/34/documentation.html#adminclientconfigs)
    for more details.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.5.0`</td>
</tr>
</tbody>
</table>

### Catalog operations

For more details, refer to [messaging catalog operations](../../../metadata/messaging.md#catalog-operations).

## Schema

A "default" schema, which includes all the topics in the Kafka cluster,
will be automatically created when catalog is created.

### Schema capabilities

- Since the "default" schema is read-only, it only supports loading and listing schema.

### Schema properties

None.

### Schema operations

For more details, refer to [messaging schema operation](../../../metadata/messaging.md#schema-operations).

## Topic

### Topic capabilities

- The Kafka catalog supports creating, updating, deleting, and listing topics.

### Topic properties

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
  <td><tt>partition-count</tt></td>
  <td>
    The number of partitions for the topic.
    If not specified, defaults to the `num.partition` property in the broker.
  </td>
  <td>`num.partition`</td>
  <td>No</td>
  <td>`0.5.0`</td>
</tr>
<tr>
  <td><tt>replication-factor</tt></td>
  <td>
    The number of replications for the topics.

    If not specified, defaults to the `default.replication.factor` property in the broker.
  </td>
  <td>`default.replication.factor`</td>
  <td>No</td>
  <td>`0.5.0`</td>
</tr>
</tbody>
</table>

You can pass other topic configurations to the topic properties.
Refer to [Topic Configs](https://kafka.apache.org/34/documentation.html#topicconfigs)
for more details.

### Topic operations

Refer to [messaging topic operation](../../../metadata/messaging.md#topic-operations).

