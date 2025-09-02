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

This document provides a brief introduction using both Gravitino Java client and
REST APIs. If you want to know more about the statistics system in Gravitino, please refer to the
Javadoc and REST API documentation.

Statistics only support the custom statistics, which names must start with `custom-`.
Gravitino will support built-in statistics in the future.

The query engine uses statistics for cost-based optimization (CBO). Meanwhile, statistics can also
be used for metadata action systems to trigger some jobs, such as compaction, data archiving, etc.

You can create statistics. And then you can create policies based on statistics. Users can analyze the statistics
and policies to decide the next action. For example,
you can create a statistic named `custom-tableLastModifiedTime` to record the last modified time of a table.
Then you can create a policy to check if the table hasn't been modified for a long time, and archive the table data to
cold storage.

Currently, Gravitino doesn't handle the computation of the statistics, you need to compute the statistics
and update them to Gravitino. Gravitino can't judge the expiration of the statistics, 
You need to ensure the statistics are up-to-date.


## Metadata object statistic operations

### Update statistics of metadata objects

You can update the statistics of a metadata object by providing the statistics key and value.
Now only table statistics can be updated.

The request path for REST API is `/api/metalakes/{metalake}/objects/{metadataObjectType}/{metadataObjectName}/statistics`.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "updates" : {
      "custom-tableLastModifiedTime": "20250128",
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

### Drop statistics of metadata objects

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

You can update the statistics of a partition by providing the statistics key and value. If the statistics
already exist, it will be updated; otherwise, a new statistic will be created.

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
and whether the range is inclusive using `fromInclusive` and `toInclusive` parameters.

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


### Server configuration

| Configuration item                                  | Description                                                                                                                                                                                                                          | Default value                                                              | Required                                        | Since version |
|-----------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------|-------------------------------------------------|---------------|
| `gravitino.stats.partition.storageFactoryClass`     | The storage factory class for partition statistics, which is used to store partition statistics in the different storage. The `org.apache.gravitino.stats.storage.MemoryPartitionStatsStorageFactory`  can only be used for testing. | `org.apache.gravitino.stats.storage.LancePartitionStatisticStorageFactory` |  No                                             | 1.0.0         |


If you use [Lance](https://lancedb.github.io/lance/) as the partition statistics storage, you can set the options below, if you have other lance storage options, you can pass it by adding prefix `gravitino.stats.partition.storageOption.`.
For example, if you set an extra property `foo` to `bar` for Lance storage option, you can add a configuration item `gravitino.stats.partition.storageOption.foo` with value `bar`.

For Lance remote storage, you can refer to the document [here](https://lancedb.github.io/lance/usage/storage/).


| Configuration item                                        | Description                          | Default value                  | Required                                        | Since version |
|-----------------------------------------------------------|--------------------------------------|--------------------------------|-------------------------------------------------|---------------|
| `gravitino.stats.partition.storageOption.location`        | The location of Lance files          | `${GRAVITINO_HOME}/data/lance` | No                                              | 1.0.0         |
| `gravitino.stats.partition.storageOption.maxRowsPerFile`  | The maximum rows per file            | `1000000`                      | No                                              | 1.0.0         |
| `gravitino.stats.partition.storageOption.maxBytesPerFile` | The maximum bytes per file           | `104857600`                    | No                                              | 1.0.0         |
| `gravitino.stats.partition.storageOption.maxRowsPerGroup` | The maximum rows per group           | `1000000`                      | No                                              | 1.0.0         |
| `gravitino.stats.partition.storageOption.readBatchSize`   | The batch record number when reading | `10000`                        | No                                              | 1.0.0         |

### Implementation a custom partition storage

You can implement a custom partition storage by implementing the interface `org.apache.gravitino.stats.storage.PartitionStatisticStorageFactory` and
setting the configuration item `gravitino.stats.partition.storageFactoryClass` to your class name.

For example:

```java
public class MyPartitionStatsStorageFactory implements PartitionStatisticStorageFactory {
    @Override
    public PartitionStatisticStorage create(Map<String, String> options) {
        // Create your custom PartitionStatsStorage here
        return new MyPartitionStatsStorage(...);
    }
}
```

```java
public class MyPartitionStatsStorage implements PartitionStatisticStorage {


    @Override
    public void close() {
        // Close your storage here
    }

    @Override
    public void updateStatistics(String metalake, List<MetadataObjectStatisticsUpdate> updates) {
        // Update partition statistics in your storage here
    }

    @Override
    public List<PersistedPartitionStatistics> listStatistics(
            String metalake, MetadataObject metadataObject, PartitionRange range) {
        // List partition statistics from your storage here
        return Maps.newHashMap();
    }

    @Override
    public int dropStatistics(String metalake, List<MetadataObjectStatisticsDrop> drops) {    
        // Drop partition statistics from your storage here
    }
}
```
