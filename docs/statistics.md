---
title: "Statistics"
slug: "/statistics"
keyword: "statistics, row count, partition statistics, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

A statistic is a named measurement attached to a table or one of its partitions. Row counts, sizes,
and file counts are the obvious ones, and anything else you want to record about a dataset can sit
beside them.

Statistics are how the catalog answers questions about size and shape without touching the data.
That matters most on a federated estate, where asking each source system directly means a different
mechanism per system.

## Quick Start

**1. Open a table.** Statistics attach to tables and to partitions of tables. See
[Tables and Views](./tables-and-views.md).

**2. Read what is already there.** Some statistics are maintained by Gravitino or the source system,
and are visible without doing anything.

**3. Record your own.** A custom statistic carries whatever measurement matters to you, under a name
you choose.

## The Statistics Model

### Reserved and Custom

A reserved statistic is one Gravitino defines, such as `row_count`. The set can grow in future
releases, so reserved names are not available for your own use.

A custom statistic is yours, and its name must begin with `custom.` to keep it clear of anything
Gravitino may reserve later. The prefix is the only naming rule.

### Modifiable and Not

A statistic is marked modifiable or not. One derived by the system, such as a count maintained by
the source, is not something to overwrite by hand, and Gravitino rejects the attempt rather than
accepting a value that the next refresh discards.

Custom statistics are modifiable, since nothing else maintains them.

### Values

A value is typed rather than free text, so a row count is a number and a boolean is a boolean.
A statistic can also exist with no value at all, which means it is known but not currently set.

### Partition Statistics

Statistics attach to partitions as well as to whole tables, and partition statistics are read and
written over a range rather than one at a time. Where a table is large and partitioned, per-partition
measurements are what make pruning and planning decisions possible.

## Working With Statistics in the UI

Statistics are not surfaced in the UI today. Reading and writing them goes through the API.

## Permissions

Statistics follow the table they belong to. Reading them requires the privilege to read the table,
and writing them requires the privilege to modify it. See
[Tables and Views](./tables-and-views.md).

## Using the API

Statistics can be listed, set, and dropped over REST and through the Java client, for tables and for
partitions. Endpoints, payload shapes, and worked examples are in
[Manage Statistics](./manage-statistics-in-gravitino.md).
