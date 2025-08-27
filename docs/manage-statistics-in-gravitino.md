---
title: "Manage statistics in Gravitino"
slug: /manage-statistics-in-gravitino
date: 2025-08-21
keyword: statistics management, statistics, statistic, Gravitino
license: This software is licensed under the Apache License version 2.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

Starting from 1.0.0, Gravitino introduces statistics of tables and partitions.

This document provides a brief introduction by using both Gravitino Java client and
REST APIs. If you want to know more about the statistics system in Gravitino, please refer to the
Javadoc and REST API documentation.

Statistics only support the custom statistics, which names must start with `custom-`.
Gravitino will support build-in statistics in the future.


## Metadata object Statistic operations

### Update metadata object statistics

You can update the statistics of a metadata object by providing the statistics key and value.
Now only table statistics can be updated.

The request path for REST API is `/api/metalakes/{metalake}/objects/{metadataObjectType}/{metadataObjectName}/statistics`.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "updates" : {
      "custom-k1":"v1"
  }
}' http://localhost:8090/api/metalakes/metalake/objects/table/catalog.schema.table/statistics
```

</TabItem>
<TabItem value="java" label="Java">

```java
Table table = ...
Map<String, StatisticValue<?>> updateStatistics = Maps.newHashMap();
updateStatistics.put("custom-k1", StatisticValues.stringValue("v1"));
updateStatistics.put("custom-k2", StatisticValues.stringValue("v2"));
table.updateStatistics(updateStatistics);
```

</TabItem>
</Tabs>

### List statistics of metadata objects

You can list all the statistics of a metadata object.
Now only table statistics can be listed.

The request path for REST API is `/api/metalakes/{metalake}/objects/{metadataObjectType}/{metadataObjectName}/statistics`.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
 -H "Content-Type: application/json" \
 http://localhost:8090/api/metalakes/metalake/objects/table/catalog.schema.table/statistics
```

</TabItem>
<TabItem value="java" label="Java">

```java
Table table = ...
table.listStatistics();
```

</TabItem>
</Tabs>

### Drop statistics of metadata object

You can drop the statistics of a metadata object by providing the statistics keys.
Now only table statistics can be dropped.

The request path for REST API is `/api/metalakes/{metalake}/objects/{metadataObjectType}/{metadataObjectName}/statistics`.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
 -H "Content-Type: application/json" -d '{
 "names":["custom-k1"]
}' http://localhost:8090/api/metalakes/metalake/objects/table/catalog.schema.table/statistics
```

</TabItem>
<TabItem value="java" label="Java">

```java
Table table = ...
List<String> statisticsToDrop = Lists.newArrayList("custom-k1");
table.dropStatistics(statisticsToDrop);
```

</TabItem>
</Tabs>

### Partition statistics operations

### Update statistics of partitions

You can update the statistics of a partition by providing the statistics key and value.

The request path for REST API is `/api/metalakes/{metalake}/objects/table/{metadataObjectName}/statistics/partitions`.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "updates":[{
  "partitionName" : "p0" ,
  "statistics" : {
    "custom-k1" : "v1"
  }
  }]
}' http://localhost:8090/api/metalakes/metalake/objects/table/catalog.schema.table/statistics/partitions
```

</TabItem>
<TabItem value="java" label="Java">

```java
Table table = ...
List<PartitionStatisticsUpdate> statisticsToUpdate = Lists.newArrayList();
Map<String, StatisticValue<?>> stats = Maps.newHashMap();
stats.put("custom-k1", StatisticValues.stringValue("v1"));
stats.put("custom-k2", StatisticValues.stringValue("v2"));
statisticsToUpdate.add(PartitionStatisticsModification.update("p1", stats));
table.updatePartitionStatistics(statisticsToUpdate);
```

</TabItem>
</Tabs>


### List statistics of partitions

You can list the statistics of specified partitions.
You can specify a range of partitions by providing the `from` and `to` parameters,
and whether the range is inclusive or not using `fromInclusive` and `toInclusive` parameters.

The request path for REST API is `/api/metalakes/{metalake}/objects/table/{metadataObjectName}/statistics/partitions`.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
 -H "Content-Type: application/json" \
 'http://localhost:8090/api/metalakes/metalake/objects/table/catalog.schema.table/statistics/partitions?from=p0&to=p1&fromInclusive=true&toInclusive=false'
```

</TabItem>
<TabItem value="java" label="Java">

```java
Table table = ...
PartitionRange range = PartitionRange.downTo("p0", PartitionRange.BoundType.CLOSED);
table.listPartitionStatistics(range);
```

</TabItem>
</Tabs>


### Drop statistics of partitions

You can drop the statistics of specified partitions by providing the statistics keys.

The request path for REST API is `/api/metalakes/{metalake}/objects/table/{metadataObjectName}/statistics/partitions`.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "drops" : [{
  "partitionName" : "p0",
  "statisticNames": ["custom-k1"]
  }]
}' http://localhost:8090/api/metalakes/metalake/objects/table/catalog.schema.table/statistics/partitions 
```

</TabItem>
<TabItem value="java" label="Java">

```java

List<PartitionStatisticsDrop> statisticsToDrop = Lists.newArrayList();
statisticsToDrop.add(
    PartitionStatisticsModification.drop("p0", Lists.newArrayList("custom-k1")));

table.dropPartitionStatistics(statisticsToDrop);

```

</TabItem>
</Tabs>
