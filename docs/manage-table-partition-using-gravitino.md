---
title: "Manage Table Partitions"
slug: "/manage-table-partition-using-gravitino"
keyword: "partition management, partition, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

This page covers the Gravitino API for partitions. For what a partition is, the partition types, and
which catalogs support managing them, see [Partitions](./partitions.md).

Partition management works on Hive and Doris catalogs. Iceberg, MySQL, and PostgreSQL catalogs
manage partitions themselves and do not expose them here.

## Partition Operations

### Add a Partition

The partition type must match the table's partitioning strategy. The three shapes are below.

<Tabs groupId="partitions">
<TabItem value="identity" label="Identity">

```json
{
  "type": "identity",
  "name": "dt=2026-08-02/country=us",
  "fieldNames": [["dt"], ["country"]],
  "values": [
    {"type": "literal", "dataType": "date", "value": "2026-08-02"},
    {"type": "literal", "dataType": "string", "value": "us"}
  ]
}
```

`values` must be in the same order as `fieldNames`. On a Hive table the partition name is ignored,
since Hive derives it from the field names and values.

</TabItem>
<TabItem value="range" label="Range">

```json
{
  "type": "range",
  "name": "p20260802",
  "upper": {"type": "literal", "dataType": "date", "value": "2026-08-02"},
  "lower": {"type": "literal", "dataType": "date", "value": "2026-08-01"}
}
```

</TabItem>
<TabItem value="list" label="List">

```json
{
  "type": "list",
  "name": "p_north_america",
  "lists": [
    [{"type": "literal", "dataType": "string", "value": "us"}],
    [{"type": "literal", "dataType": "string", "value": "ca"}]
  ]
}
```

</TabItem>
</Tabs>

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "partitions": [
    {
      "type": "identity",
      "name": "dt=2026-08-02/country=us",
      "fieldNames": [["dt"], ["country"]],
      "values": [
        {"type": "literal", "dataType": "date", "value": "2026-08-02"},
        {"type": "literal", "dataType": "string", "value": "us"}
      ]
    }
  ]
}' http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/tables/orders/partitions
```

</TabItem>
<TabItem value="java" label="Java">

```java
Table orders = catalog.asTableCatalog().loadTable(
    NameIdentifier.of("public", "orders"));

Partition partition = Partitions.identity(
    new String[][] {{"dt"}, {"country"}},
    new Literal[] {
        Literals.dateLiteral(LocalDate.of(2026, 8, 2)),
        Literals.stringLiteral("us")
    });

orders.supportPartitions().addPartition(partition);
```

</TabItem>
</Tabs>

### Get a Partition

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  "http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/tables/orders/partitions/dt=2026-08-02%2Fcountry=us"
```

</TabItem>
<TabItem value="java" label="Java">

```java
Partition partition = orders.supportPartitions().getPartition(
    "dt=2026-08-02/country=us");
```

</TabItem>
</Tabs>

### List Partitions

Listing returns names, or full partition objects when `details=true` is set.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/tables/orders/partitions

curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  "http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/tables/orders/partitions?details=true"
```

</TabItem>
<TabItem value="java" label="Java">

```java
String[] names = orders.supportPartitions().listPartitionNames();
Partition[] partitions = orders.supportPartitions().listPartitions();
```

</TabItem>
</Tabs>

### Drop a Partition

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  "http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/tables/orders/partitions/dt=2026-08-02%2Fcountry=us"
```

</TabItem>
<TabItem value="java" label="Java">

```java
boolean dropped = orders.supportPartitions().dropPartition(
    "dt=2026-08-02/country=us");

boolean purged = orders.supportPartitions().purgePartition(
    "dt=2026-08-02/country=us");
```

</TabItem>
</Tabs>

Dropping removes the partition metadata. Purging removes its data as well, and is not supported by
every catalog.
