---
title: "Partitions"
slug: "/partitions"
keyword: "partition, partition management, partition pruning, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

A partition is a named slice of a table's data, and in Gravitino it is a metadata object you can
list, inspect, add, and remove.

Most of the time partitions look after themselves. A catalog creates them as data arrives and an
engine prunes them at query time without anyone asking. Managing them by hand matters in a few
specific cases: expiring data by partition rather than by row, recording statistics per partition so
a planner can prune on them, and adding a partition ahead of the data that will fill it.

Partitions belong to partitioned tables. A table becomes partitioned when it is created with a
partitioning strategy, which is a separate thing from the partitions themselves. See
[Table partitioning, distribution, sort order, and indexes](./table-partitioning-distribution-sort-order-indexes.md).

## Quick Start

**1. Start with a partitioned table.** A table with no partitioning strategy has no partitions to
manage. See [Tables and Views](./tables-and-views.md).

**2. Pick the path your catalog uses.** On Hive, Glue, and Doris, partitions are objects you manage
through the Gravitino API. On Iceberg, partitioning is changed through an Iceberg-compatible engine
pointed at the Iceberg REST catalog service, which listens on port 9001 by default.

**3. List, add, or drop.** Either path goes through an API. The UI shows a table's partitioning but
does not list, add, or drop individual partitions.

## The Partition Model

### Partition Types

| Type       | Describes                                                        |
|------------|------------------------------------------------------------------|
| `IDENTITY` | One value per partitioning column, such as `dt=2026-08-02`       |
| `RANGE`    | An upper and lower bound, such as a date range                   |
| `LIST`     | An explicit set of value combinations                            |

The type follows from how the table was partitioned rather than being chosen per partition.

### Where Partition Management Works

Partition management through the Gravitino API works on Hive, Glue, and Doris catalogs. Every
operation below is available on those three and on no others.

Iceberg, Paimon, and Hudi manage partitions themselves, and the JDBC catalogs other than Doris have
no partition operations, so a call against one of them fails rather than returning an empty list.
Nothing is missing from Gravitino for those catalogs, and the sections below say what to do instead.

### Iceberg Tables

Iceberg partitions differently, and the difference is deliberate rather than a gap.

An Iceberg table declares a partition spec, a set of transforms over its columns such as
`day(event_time)` or `bucket(16, customer_id)`. Partitions are then derived from the data as it
arrives, which Iceberg calls hidden partitioning. There is no partition object to add, get, or drop,
which is why Iceberg is not in the list above.

Gravitino sets the spec when it creates the table. Identity, bucket, truncate, year, month, day, and
hour transforms all convert to an Iceberg spec, with bucket limited to one field. List and range
partitioning have no Iceberg equivalent and are rejected.

After creation, partitioning is changed through the engine rather than through the Gravitino API.
Point Spark, Trino, or another Iceberg client at the Iceberg REST catalog service, which listens on
port 9001 by default, and use the engine's own syntax:

```sql
ALTER TABLE sales.public.orders ADD PARTITION FIELD day(event_time);
ALTER TABLE sales.public.orders DROP PARTITION FIELD country;
```

The Iceberg REST catalog service handles the resulting `AddPartitionSpec` and
`SetDefaultPartitionSpec` updates, so partition evolution behaves exactly as it does against any
Iceberg catalog. Existing data is not rewritten, and the new spec applies to what is written
afterward.

See [Iceberg REST catalog service](./iceberg-rest-service.md).

### Partitions and Statistics

Statistics attach to partitions as well as to whole tables, which is what makes per-partition
measurements available to a planner. See [Statistics](./statistics.md).

## Working With Partitions in the UI

A table page shows the table's partitioning strategy, the transforms it was created with, and a
count of how many partition fields it has. A table partitioned by `dt` and `country` shows two.
Individual partitions are not listed, and adding or dropping one goes through the API.

The display comes from the table itself, so it works for every relational catalog including Iceberg.
An Iceberg table partitioned by `day(event_time)` shows that transform like any other table, because
the partition spec is read back and presented in the same form. A count of zero means the table
declares no partitioning rather than that partitions could not be read.

## Permissions

Partitions follow the table they belong to. Listing them requires the privilege to read the table,
and adding or dropping requires the privilege to modify it. See
[Tables and Views](./tables-and-views.md).

## Using the API

On Hive, Glue, and Doris, partitions are listed, added, inspected, and dropped over REST and through
the Java and Python clients. Endpoints, payload shapes, and worked examples are in
[Manage Table Partitions](./manage-table-partition-using-gravitino.md).

On Iceberg, partitioning is changed through an Iceberg-compatible engine pointed at the
[Iceberg REST catalog service](./iceberg-rest-service.md) rather than through these endpoints.

Partition statistics are a separate path and work on any table, including Iceberg. They are held by
Gravitino rather than by the catalog, so a partition can carry statistics even where the catalog
exposes no partition objects to list. Partition names are supplied by the caller rather than
discovered, and statistics are read and written over a range rather than one at a time. See
[Statistics](./statistics.md) and [Manage Statistics](./manage-statistics-in-gravitino.md).
