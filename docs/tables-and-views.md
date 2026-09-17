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

#### Table Column Type

Gravitino supports the following column types. A catalog may support only a subset; see the
provider's page for its type mapping.

| Type                      | Java                                                                    | JSON                                                                                                                               |
|---------------------------|-------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| Boolean                   | `Types.BooleanType.get()`                                               | `"boolean"`                                                                                                                        |
| Byte                      | `Types.ByteType.get()`                                                  | `"byte"`                                                                                                                           |
| Unsigned Byte             | `Types.ByteType.unsigned()`                                             | `"byte unsigned"`                                                                                                                  |
| Short                     | `Types.ShortType.get()`                                                 | `"short"`                                                                                                                          |
| Unsigned Short            | `Types.ShortType.unsigned()`                                            | `"short unsigned"`                                                                                                                 |
| Integer                   | `Types.IntegerType.get()`                                               | `"integer"`                                                                                                                        |
| Unsigned Integer          | `Types.IntegerType.unsigned()`                                          | `"integer unsigned"`                                                                                                               |
| Long                      | `Types.LongType.get()`                                                  | `"long"`                                                                                                                           |
| Unsigned Long             | `Types.LongType.unsigned()`                                             | `"long unsigned"`                                                                                                                  |
| Float                     | `Types.FloatType.get()`                                                 | `"float"`                                                                                                                          |
| Double                    | `Types.DoubleType.get()`                                                | `"double"`                                                                                                                         |
| Decimal(precision, scale) | `Types.DecimalType.of(precision, scale)`                                | `"decimal(p,s)"`                                                                                                                   |
| String                    | `Types.StringType.get()`                                                | `"string"`                                                                                                                         |
| FixedChar(length)         | `Types.FixedCharType.of(length)`                                        | `"char(l)"`                                                                                                                        |
| VarChar(length)           | `Types.VarCharType.of(length)`                                          | `"varchar(l)"`                                                                                                                     |
| Timestamp                 | `Types.TimestampType.withoutTimeZone()`                                 | `"timestamp"`                                                                                                                      |
| Timestamp(p)              | `Types.TimestampType.withoutTimeZone(p)`                                | `"timestamp(p)"`                                                                                                                   |
| TimestampWithTimezone     | `Types.TimestampType.withTimeZone()`                                    | `"timestamp_tz"`                                                                                                                   |
| TimestampWithTimezone(p)  | `Types.TimestampType.withTimeZone(p)`                                   | `"timestamp_tz(p)"`                                                                                                                |
| Date                      | `Types.DateType.get()`                                                  | `"date"`                                                                                                                           |
| Time                      | `Types.TimeType.get()`                                                  | `"time"`                                                                                                                           |
| Time(p)                   | `Types.TimeType.of(p)`                                                  | `"time(p)"`                                                                                                                        |
| IntervalToYearMonth       | `Types.IntervalYearType.get()`                                          | `"interval_year"`                                                                                                                  |
| IntervalToDayTime         | `Types.IntervalDayType.get()`                                           | `"interval_day"`                                                                                                                   |
| Fixed(length)             | `Types.FixedType.of(length)`                                            | `"fixed(l)"`                                                                                                                       |
| Binary                    | `Types.BinaryType.get()`                                                | `"binary"`                                                                                                                         |
| List                      | `Types.ListType.of(Types.IntegerType.get(), true)`                       | `{"type":"list","containsNull":true,"elementType":"integer"}`                                                                |
| Map                       | `Types.MapType.of(Types.StringType.get(), Types.IntegerType.get(), true)` | `{"type":"map","keyType":"string","valueType":"integer","valueContainsNull":true}`                                       |
| Struct                    | `Types.StructType.of(Types.StructType.Field.of("id", Types.IntegerType.get(), false, null))` | `{"type":"struct","fields":[{"name":"id","type":"integer","nullable":false}]}`                            |
| Union                     | `Types.UnionType.of(Types.IntegerType.get(), Types.StringType.get())`    | `{"type":"union","types":["integer","string"]}`                                                                              |
| UUID                      | `Types.UUIDType.get()`                                                  | `"uuid"`                                                                                                                           |
| Variant                   | `Types.VariantType.get()`                                               | `"variant"`                                                                                                                        |
| Null                      | `Types.NullType.get()`                                                  | `"null"`                                                                                                                           |
| Geometry                  | `Types.GeometryType.crs84()`                                            | `"geometry"`                                                                                                                       |
| Geography                 | `Types.GeographyType.crs84()`                                           | `"geography"`                                                                                                                      |

Decimal precision is in the range 1-38, and scale is in the range 0-precision. The optional
precision for time and timestamp types is in the range 0-12.

##### Null type

The null type represents a column that holds only null values and whose concrete type is not yet
known. It is intended to be promoted to a concrete type through schema evolution before data is
written. Support is connector-specific.

##### External type

An external type represents a catalog type that is not part of the Gravitino type system. It keeps
the external catalog's type string so clients can inspect it without losing information.

```json
{
  "type": "external",
  "catalogString": "user-defined"
}
```

```java
String typeString = ((ExternalType) type).catalogString();
```

##### Unparsed type

An unparsed type preserves forward compatibility when a client does not recognize a type returned
by the server. The client retains the serialized value instead of failing deserialization.

```json
{
  "type": "unparsed",
  "unparsedType": "unknown-type"
}
```

```java
String unparsedValue = ((UnparsedType) type).unparsedType();
```

#### Table Column Default Value

A column default can be a [literal](./expression.md#literal) or an
[expression](./expression.md). The underlying catalog applies it to new rows, and support depends
on the catalog provider.

#### Table Column Auto-increment

An auto-increment column asks the underlying catalog to generate values for new rows. Support and
restrictions are provider-specific, so check the provider's table capabilities before enabling it.

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
| `CREATE_TABLE` | Metalake, catalog, or schema        | Creating tables                 |
| `SELECT_TABLE` | Metalake, catalog, schema, or table | Reading table metadata          |
| `MODIFY_TABLE` | Metalake, catalog, schema, or table | Writing to and altering a table |
| `CREATE_VIEW`  | Metalake, catalog, or schema        | Creating views                  |
| `SELECT_VIEW`  | Metalake, catalog, schema, or view  | Reading view metadata           |

Granting at a wider scope covers everything beneath it. Dropping a table is reserved for the
metalake owner and the object owner, and ownership resolves down the hierarchy, so the owner of a
catalog has the owner path to every table in it.

When metadata authorization is enabled, listing views first requires access to the schema and then
returns only views the caller owns or can read with `SELECT_VIEW`. Creating a view requires
`CREATE_VIEW` in scope and makes the caller its owner; altering and dropping a view are owner-only.

View permissions cover metadata operations only. The API does not yet define an `INVOKER` or
`DEFINER` execution mode, so access to referenced data remains subject to the engine's
authorization.

## Using the API

Tables and views can be created, listed, altered, and dropped over REST and through the Java and
Python clients. Endpoints, payload shapes, and worked examples are in
[Manage Relational Metadata](./manage-relational-metadata-using-gravitino.md) and
[Manage View Metadata](./manage-view-metadata-using-gravitino.md).
