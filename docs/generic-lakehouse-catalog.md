---
title: "Lakehouse catalog"
slug: /lakehouse-catalog
keywords:
  - lakehouse
  - lance
  - metadata
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

TBD.

### Requirements and limitations

TBD.

## Catalog

### Catalog capabilities

TBD.

### Catalog properties

TBD.

### Catalog operations

TBD.

## Schema

### Schema capabilities

TBD.

### Schema properties

TBD.

### Schema operations

Please refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more details.

## Table

### Table capabilities

TBD.

### Table partitions

TBD.

### Table sort orders

TBD.

### Table distributions

TBD.

### Table column types

Since Lance uses Apache Arrow as the table schema, the following table shows the mapping between Gravitino types and Arrow types:

| Gravitino Type                   | Arrow Type                              |
|----------------------------------|-----------------------------------------|
| `Struct`                         | `Struct`                                |
| `Map`                            | `Map`                                   |
| `List`                           | `Array`                                 |
| `Boolean`                        | `Boolean`                               |
| `Byte`                           | `Int8`                                  |
| `Short`                          | `Int16`                                 |
| `Integer`                        | `Int32`                                 |
| `Long`                           | `Int64`                                 |
| `Float`                          | `Float`                                 |
| `Double`                         | `Double`                                |
| `String`                         | `Utf8`                                  |
| `Binary`                         | `Binary`                                |
| `Decimal(p, s)`                  | `Decimal(p, s)` (128-bit)               |
| `Date`                           | `Date`                                  |
| `Timestamp`/`Timestamp(6)`       | `TimestampType withoutZone`             |
| `Timestamp(0)`                   | `TimestampType Second withoutZone`      |
| `Timestamp(3)`                   | `TimestampType Millisecond withoutZone` |
| `Timestamp(9)`                   | `TimestampType Nanosecond withoutZone`  |
| `Timestamp_tz`/`Timestamp_tz(6)` | `TimestampType Microsecond withUtc`     |
| `Timestamp_tz(0)`                | `TimestampType Second withUtc`          |
| `Timestamp_tz(3)`                | `TimestampType Millisecond withUtc`     |
| `Timestamp_tz(9)`                | `TimestampType Nanosecond withUtc`      |
| `Time`/`Time(9)`                 | `Time Nanosecond`                       |
| `Null`                           | `Null`                                  |
| `Fixed(n)`                       | `Fixed-Size Binary(n)`                  |
| `Interval_year`                  | `Interval(YearMonth)`                   |
| `Interval_day`                   | `Duration(Microsecond)`                 |
| `External(arrow_field_json_str)` | Any Arrow Field (see note below)        |

`External(arrow_field_json_str)`:

As the table above shows, Gravitino provides mappings for most common data types. However, 
in some cases, you may need to use an Arrow data type that is not directly supported by Gravitino.

To address this, Gravitino introduces the `External(arrow_field_json_str)` type, 
which allows you to define any Arrow data type by providing the JSON string of an Arrow `Field`.

The JSON string must conform to the Apache Arrow `Field` [specification](https://github.com/apache/arrow-java/blob/ed81e5981a2bee40584b3a411ed755cb4cc5b91f/vector/src/main/java/org/apache/arrow/vector/types/pojo/Field.java#L80C1-L86C68), 
including details such as the field name, data type, and nullability. For example, you can define a `LargeUtf8` type field using its JSON representation.
```json
{
  "name": "col_name",
  "nullable": true,
  "type": {
    "name": "largeutf8"
  },
  "children": []
}
```

**Important considerations:**
- The `name` attribute and `nullable` attribute in the JSON string must exactly match the corresponding column name and nullable in the Gravitino table.
- The `children` array should be empty for primitive types. For complex types like `Struct` or `List`, it must contain the definitions of the child fields.

### Table properties

TBD.

### Table indexes

TBD.

### Table operations

Please refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for more details.

## Object store configuration

TBD.
