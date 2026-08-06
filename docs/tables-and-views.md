---
title: "Tables and Views"
slug: "/tables-and-views"
keyword: "table, view, column, relational metadata, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

A table is the object a relational catalog holds, and a view is a stored query over one or more
tables. Both live in a schema, and both are reached the same way no matter which system stores them.

Gravitino does not hold a copy of either. A table registered through a Hive, Iceberg, MySQL, or
Paimon catalog stays in that system, and Gravitino keeps the reference plus anything attached to it.
Creating a table through Gravitino creates it in the source system, and listing tables asks the
source at request time, so a table created directly in Hive appears the next time Gravitino is
asked.

What Gravitino adds is a single shape across all of them. The same call describes a Hive table and
an Iceberg table, columns carry the same type system, and tags, policies, ownership, and statistics
attach the same way regardless of the system underneath.

## Quick Start

**1. Open a catalog.** Tables live in a schema inside a relational catalog. See
[Catalogs and Schemas](./catalogs-and-schemas.md) for connecting one.

**2. Browse or create.** A connected catalog surfaces the tables already there. Creating a table
through Gravitino creates it in the source system, with the columns, partitioning, and properties
you specify.

**3. Classify what matters.** Tables and their columns both carry tags and policies, which is how a
classification set once reaches every engine that reads through Gravitino.

## The Table Model

### Columns

A column has a name and a type, and can carry a comment, nullability, an auto-increment flag, and a
default value. Types are Gravitino types rather than any one system's, so the same definition works
across catalogs and each provider maps them to its own.

Where a provider cannot represent a type, the provider's own page says so. Type mapping is the most
common place two catalogs of different providers differ.

### Table Properties

Properties are provider-specific and carry what the source system needs, such as the file format for
a Hive table or the write mode for an Iceberg table. Each catalog type's page documents its own set.

### Partitioning, Distribution, and Sort Order

A table can be created with a partitioning strategy, a distribution, a sort order, and indexes.
Which of those a catalog supports depends on the provider. See
[Table partitioning, distribution, sort order, and indexes](./table-partitioning-distribution-sort-order-indexes.md).

Once a table is partitioned, its partitions are metadata objects in their own right. See
[Manage table partitions](./manage-table-partition-using-gravitino.md).

### Dropping Versus Purging

Dropping a table removes the metadata, and for a managed table the underlying directory as well. For
an external table, only the metadata goes and the data stays where it is.

Purging removes the data completely and skips any trash the system would otherwise use. Not every
catalog supports it, and purging an external table is rejected rather than silently ignored.

The distinction matters most on Hive, where external tables are common and dropping one leaves the
files in place.

## Views

A view is a stored query, and Gravitino treats it as its own object type rather than a kind of
table. Views are supported by the Hive, Iceberg, and Paimon catalogs, and a relational catalog with
a provider that has no view concept simply has none.

A view carries the query text, the dialect the query is written in, and its own comment and
properties. Gravitino stores the definition and does not execute it, so whether a view resolves is a
question for the engine reading it.

Views can carry tags, and appear in listings alongside tables.

## Working With Tables and Views in the UI

Opening a schema lists its tables and views. Selecting a table shows its columns with their types,
its properties, and its tags.

Tags attach from the table row and from individual column rows, which is the fastest way to classify
a specific field rather than a whole table. Policies attach at the table level.

## Permissions

| Privilege      | Grantable on                        | What it allows                  |
|----------------|-------------------------------------|---------------------------------|
| `CREATE_TABLE` | Metalake, catalog, or schema        | Creating tables and views       |
| `SELECT_TABLE` | Metalake, catalog, schema, or table | Reading a table or view         |
| `MODIFY_TABLE` | Metalake, catalog, schema, or table | Writing to and altering a table |

Granting at a wider scope covers everything beneath it. Dropping a table is reserved for the
metalake owner and the object owner, and ownership resolves down the hierarchy, so the owner of a
catalog has the owner path to every table in it.

## Using the API

Tables and views can be created, listed, altered, and dropped over REST and through the Java and
Python clients. Endpoints, payload shapes, and worked examples are in
[Manage Relational Metadata](./manage-relational-metadata-using-gravitino.md) and
[Manage View Metadata](./manage-view-metadata-using-gravitino.md).
