---
title: "Manage Statistics"
slug: "/manage-statistics-in-gravitino"
keyword: "statistics management, statistics, partition statistics, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

This page covers the Gravitino API for statistics. For what a statistic is, the difference between
reserved and custom, and how partition statistics relate to partitions, see
[Statistics](./statistics.md).

Statistics attach to tables. Custom names must begin with `custom.` to stay clear of names Gravitino
may reserve later.

## Table Statistics

### Update Statistics

Updating creates a statistic that does not exist and overwrites one that does. Reserved statistics
maintained by the system are not modifiable and the request is rejected.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "updates": {
    "custom.last_reviewed": "2026-08-02",
    "custom.owner_team": "risk"
  }
}' http://localhost:8090/api/metalakes/example/objects/table/sales.public.orders/statistics
```

</TabItem>
<TabItem value="java" label="Java">

```java
Table orders = ...
Map<String, StatisticValue<?>> updates = Maps.newHashMap();
updates.put("custom.last_reviewed", StatisticValues.stringValue("2026-08-02"));
updates.put("custom.owner_team", StatisticValues.stringValue("risk"));

orders.supportsStatistics().updateStatistics(updates);
```

</TabItem>
</Tabs>

### List Statistics

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/objects/table/sales.public.orders/statistics
```

</TabItem>
<TabItem value="java" label="Java">

```java
List<Statistic> statistics = orders.supportsStatistics().listStatistics();
```

</TabItem>
</Tabs>

### Drop Statistics

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "names": ["custom.owner_team"]
}' http://localhost:8090/api/metalakes/example/objects/table/sales.public.orders/statistics
```

</TabItem>
<TabItem value="java" label="Java">

```java
orders.supportsStatistics().dropStatistics(ImmutableList.of("custom.owner_team"));
```

</TabItem>
</Tabs>

## Partition Statistics

Partition statistics are held by Gravitino rather than by the catalog, so they work on any table
including catalogs that expose no partition objects. Partition names are supplied by the caller, and
several partitions are read or written in one request.

### Update Partition Statistics

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "updates": [
    {
      "partitionName": "dt=2026-08-02",
      "statistics": {"custom.row_estimate": "18000"}
    }
  ]
}' http://localhost:8090/api/metalakes/example/objects/table/sales.public.orders/statistics/partitions
```

</TabItem>
<TabItem value="java" label="Java">

```java
Map<String, StatisticValue<?>> stats = Maps.newHashMap();
stats.put("custom.row_estimate", StatisticValues.longValue(18000L));

orders.supportsPartitionStatistics().updatePartitionStatistics(
    ImmutableList.of(PartitionStatisticsModification.update("dt=2026-08-02", stats)));
```

</TabItem>
</Tabs>

### List Partition Statistics

Listing takes a partition range rather than a single name.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  "http://localhost:8090/api/metalakes/example/objects/table/sales.public.orders/statistics/partitions?from=dt=2026-08-01&to=dt=2026-08-31"
```

</TabItem>
<TabItem value="java" label="Java">

```java
List<PartitionStatistics> statistics = orders.supportsPartitionStatistics().listPartitionStatistics(
    PartitionRange.between(
        "dt=2026-08-01", PartitionRange.BoundType.CLOSED,
        "dt=2026-08-31", PartitionRange.BoundType.CLOSED));
```

</TabItem>
</Tabs>

### Drop Partition Statistics

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "drops": [
    {
      "partitionName": "dt=2026-08-02",
      "statisticNames": ["custom.row_estimate"]
    }
  ]
}' http://localhost:8090/api/metalakes/example/objects/table/sales.public.orders/statistics/partitions
```

</TabItem>
<TabItem value="java" label="Java">

```java
orders.supportsPartitionStatistics().dropPartitionStatistics(
    ImmutableList.of(
        PartitionStatisticsModification.drop(
            "dt=2026-08-02", ImmutableList.of("custom.row_estimate"))));
```

</TabItem>
</Tabs>

## Storage Configuration

Partition statistics use a pluggable storage backend, configured on the server. See
[Server configuration](./gravitino-server-config.md). Writing a custom backend is covered in
[Custom partition storage](./development/custom-partition-storage.md).
