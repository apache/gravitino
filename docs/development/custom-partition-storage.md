---
title: "Custom Partition Storage"
slug: "/development/custom-partition-storage"
keyword: "partition statistics, storage, extension, PartitionStatisticStorageFactory, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Partition statistics use a pluggable storage backend, set with
`gravitino.stats.partition.storageFactoryClass`. Gravitino ships JDBC and Lance backends; writing
your own backend means implementing the storage factory interface.

## Implement a Custom Partition Storage

Implement a custom partition storage by implementing the interface `org.apache.gravitino.stats.storage.PartitionStatisticStorageFactory` and
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
    public void close() throws IOException {
        // Close your storage here
    }

    @Override
    public void updateStatistics(String metalake, List<MetadataObjectStatisticsUpdate> updates)
            throws IOException {
        // Update partition statistics in your storage here
    }

    @Override
    public List<PersistedPartitionStatistics> listStatistics(
            String metalake, MetadataObject metadataObject, PartitionRange range)
            throws IOException {
        // List partition statistics from your storage here
        return Lists.newArrayList();
    }

    @Override
    public int dropStatistics(String metalake, List<MetadataObjectStatisticsDrop> drops)
            throws IOException {
        // Drop partition statistics from your storage here, returning the number actually dropped
        return drops.size();
    }
}
```
