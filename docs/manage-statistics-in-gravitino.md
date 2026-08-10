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
[Server Configuration](#server-configuration) below. Writing a custom backend is covered in
[Custom partition storage](./development/custom-partition-storage.md).

### Server Configuration

| Configuration item                              | Description                                                                                                                                                                                                                          | Default value                                                             | Required |
|-------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------|----------|
| `gravitino.stats.partition.storageFactoryClass` | The storage factory class for partition statistics, which is used to store partition statistics in the different storage. The `org.apache.gravitino.stats.storage.MemoryPartitionStatsStorageFactory`  can only be used for testing. | `org.apache.gravitino.stats.storage.JdbcPartitionStatisticStorageFactory` | No       |


#### JDBC Storage (Default)

Starting from version 1.2.0, Gravitino uses JDBC-based storage as the default partition statistics storage backend.
This provides a reliable, production-ready solution that supports multiple database backends:

- **MySQL** (recommended for production)
- **PostgreSQL**
- **H2** (suitable for testing and development)

To use JDBC storage, configure the following options by adding the prefix `gravitino.stats.partition.storageOption.`:

| Configuration item                                            | Description                                                       | Default value              | Required |
|---------------------------------------------------------------|-------------------------------------------------------------------|----------------------------|----------|
| `gravitino.stats.partition.storageOption.jdbcUrl`             | JDBC connection URL (e.g., jdbc:mysql://localhost:3306/gravitino) | None                       | Yes      |
| `gravitino.stats.partition.storageOption.jdbcUser`            | Database username                                                 | None                       | Yes      |
| `gravitino.stats.partition.storageOption.jdbcPassword`        | Database password                                                 | None                       | Yes      |
| `gravitino.stats.partition.storageOption.jdbcDriver`          | JDBC driver class name                                            | `com.mysql.cj.jdbc.Driver` | No       |
| `gravitino.stats.partition.storageOption.poolMaxSize`         | Maximum connection pool size                                      | `10`                       | No       |
| `gravitino.stats.partition.storageOption.poolMinIdle`         | Minimum idle connections in pool                                  | `2`                        | No       |
| `gravitino.stats.partition.storageOption.connectionTimeoutMs` | Connection timeout in milliseconds                                | `30000`                    | No       |
| `gravitino.stats.partition.storageOption.testOnBorrow`        | Test connections before use                                       | `true`                     | No       |

**Example MySQL Configuration:**

```properties
gravitino.stats.partition.storageFactoryClass = org.apache.gravitino.stats.storage.JdbcPartitionStatisticStorageFactory
gravitino.stats.partition.storageOption.jdbcUrl = jdbc:mysql://localhost:3306/gravitino
gravitino.stats.partition.storageOption.jdbcUser = gravitino
gravitino.stats.partition.storageOption.jdbcPassword = gravitino123
gravitino.stats.partition.storageOption.poolMaxSize = 20
```

**Example PostgreSQL Configuration:**

```properties
gravitino.stats.partition.storageFactoryClass = org.apache.gravitino.stats.storage.JdbcPartitionStatisticStorageFactory
gravitino.stats.partition.storageOption.jdbcUrl = jdbc:postgresql://localhost:5432/gravitino
gravitino.stats.partition.storageOption.jdbcUser = gravitino
gravitino.stats.partition.storageOption.jdbcPassword = gravitino123
gravitino.stats.partition.storageOption.jdbcDriver = org.postgresql.Driver
```

**Database Schema Setup:**

Before using JDBC storage, you need to create the database schema. Schema files are provided for all supported databases:

- MySQL: `scripts/mysql/schema-${GRAVITINO_VERSION}-mysql.sql`
- PostgreSQL: `scripts/postgresql/schema-${GRAVITINO_VERSION}-postgresql.sql`
- H2: `scripts/h2/schema-${GRAVITINO_VERSION}-h2.sql`

For MySQL:
```bash
mysql -u root -p < scripts/mysql/schema-${GRAVITINO_VERSION}-mysql.sql
```

For PostgreSQL:
```bash
psql -U postgres -d gravitino -f scripts/postgresql/schema-${GRAVITINO_VERSION}-postgresql.sql
```

#### Lance Storage (Alternative)

If you use [Lance](https://lancedb.github.io/lance/) as the partition statistics storage, you can set the options below, if you have other lance storage options, you can pass it by adding prefix `gravitino.stats.partition.storageOption.`.
For example, if you set an extra property `foo` to `bar` for Lance storage option, you can add a configuration item `gravitino.stats.partition.storageOption.foo` with value `bar`.

For Lance remote storage, you can refer to the document [here](https://lancedb.github.io/lance/usage/storage/).


| Configuration item                                                   | Description                                               | Default value                        | Required |
|----------------------------------------------------------------------|-----------------------------------------------------------|--------------------------------------|----------|
| `gravitino.stats.partition.storageOption.location`                   | The location of Lance files                               | `${GRAVITINO_HOME}/data/lance`       | No       |
| `gravitino.stats.partition.storageOption.maxRowsPerFile`             | The maximum rows per file                                 | `1000000`                            | No       |
| `gravitino.stats.partition.storageOption.maxBytesPerFile`            | The maximum bytes per file                                | `104857600`                          | No       |
| `gravitino.stats.partition.storageOption.maxRowsPerGroup`            | The maximum rows per group                                | `1000000`                            | No       |
| `gravitino.stats.partition.storageOption.readBatchSize`              | The batch record number when reading                      | `10000`                              | No       |
| `gravitino.stats.partition.storageOption.datasetCacheSize`           | size of dataset cache for Lance                           | `0`, It means we don't use the cache | No       |
| `gravitino.stats.partition.storageOption.metadataFileCacheSizeBytes` | The Lance's metadata file cache size                      | `102400`                             | No       |
| `gravitino.stats.partition.storageOption.indexCacheSizeBytes`        | The Lance's index cache size                              | `102400`                             | No       |
| `gravitino.stats.partition.storageOption.maxStatisticsPerUpdate`     | Maximum number of statistics allowed per update operation | `100`                                | No       |

If you have many tables with a small number of partitions, you should set a smaller metadataFileCacheSizeBytes and indexCacheSizeBytes.

**To use Lance storage, configure:**

```properties
gravitino.stats.partition.storageFactoryClass = org.apache.gravitino.stats.storage.LancePartitionStatisticStorageFactory
gravitino.stats.partition.storageOption.location = /data/lance
```
