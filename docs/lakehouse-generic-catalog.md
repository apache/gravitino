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

Generic Lakehouse Catalog is a Gravitino catalog implementation that enables Gravitino to interact with Lakehouse storage systems that use file system for storing tabular data. Such lakehouse system could be built on top of object stores like Amazon S3, Azure Blob Storage, Google Cloud Storage, or HDFS.
Theoretically, it can work with any lakehouse storage system that supports standard file system operations such as Apache Iceberg, Lance, Delta Lake, and Apache Hudi. However, currently Gravitino only provides native support for Lance-based lakehouse storage systems.

### Requirements and limitations

- The lakehouse storage system must support standard file system operations such as listing directories, reading files, and writing files.

## Catalog

### Catalog capabilities

All capabilities are the same as relational catalog, please refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md) for more details.

### Catalog properties

The only property that need to be noted for a generic lakehouse catalog is `location`. This property specifies the root location of the lakehouse storage system. All schemas and tables will be stored under this location if not
specified otherwise in schema or table properties.


### Catalog operations

All operations are the same as relational catalog, please refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md) for more details.

One thing needs to be noted is that the provider will be `generic_lakehouse` when creating a generic lakehouse catalog.
That is:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "generic_lakehouse_catalog",
  "type": "RELATIONAL",
  "comment": "comment",
  "provider": "generic-lakehouse", 
  "properties": {
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs
```

</TabItem>
<TabItem value="java" label="Java">

```java

// Assuming you have just created a metalake named `metalake`
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://127.0.0.1:8090")
    .withMetalake("metalake")
    .build();

Map<String, String> genericCatalogProperties = ImmutableMap.<String, String>builder()
    .put("location", "hdfs://localhost:9000/user/lakehouse") // The root location of the lakehouse storage system
    .build();

Catalog catalog = gravitinoClient.createCatalog("generic_lakehouse_catalog",
    Type.RELATIONAL,
    "generic_lakehouse", 
    "This is a generic lakehouse catalog",
    genericCatalogProperties); // Please change the properties according to the value of the provider.
// ...
```

</TabItem>
</Tabs>


## Schema

### Schema capabilities

All capabilities are the same as relational catalog, please refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md) for more details.

### Schema properties

The same as catalog properties, please refer to [Catalog properties](#catalog-properties) section for more details. Schema `location` property can be used to specify the location to store all tables under this schema.

### Schema operations

Please refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#schema-operations) for more details.

## Table

### Table capabilities

Currently, for a lance table, Gravitino supports the following capabilities:
- List 
- Load
- Alter (partial supported)
- Create/register
- Drop and truncate

### Table partitions

Not supported now

### Table sort orders

Not supported now.

### Table distributions

Not supported now.

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
including details such as the field name, data type, and nullability.
Here are some examples of how to use `External` type for various Arrow types that are not natively supported by Gravitino:

| Arrow Type        | External type                                                                                                                                                                                                                                           | 
|-------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Large Utf8`      | `External("{\"name\":\"col_name\",\"nullable\":true,\"type\":{\"name\":\"largeutf8\"},\"children\":[]}")`                                                                                                                                               |
| `Large Binary`    | `External("{\"name\":\"col_name\",\"nullable\":true,\"type\":{\"name\":\"largebinary\"},\"children\":[]}")`                                                                                                                                             |         
| `Large List`      | `External("{\"name\":\"col_name\",\"nullable\":true,\"type\":{\"name\":\"largelist\"},\"children\":[{\"name\":\"element\",\"nullable\":true,\"type\":{\"name\":\"int\", \"bitWidth\":32, \"isSigned\": true},\"children\":[]}]}")`                      |
| `Fixed-Size List` | `External("{\"name\":\"col_name\",\"nullable\":true,\"type\":{\"name\":\"fixedsizelist\", \"listSize\":10},\"children\":[{\"name\":\"element\",\"nullable\":true,\"type\":{\"name\":\"int\", \"bitWidth\":32, \"isSigned\": true},\"children\":[]}]}")` |

**Important considerations:**
- The `name` attribute and `nullable` attribute in the JSON string must exactly match the corresponding column name and nullability in the Gravitino table.
- The `children` array should be empty for primitive types. For complex types like `Struct` or `List`, it must contain the definitions of the child fields.

### Table properties

Currently, the following properties are required for a table in a generic lakehouse catalog

| Configuration item | Description                                                                                   | Default value | Required                                                                    | Since version |
|--------------------|-----------------------------------------------------------------------------------------------|---------------|-----------------------------------------------------------------------------|---------------|
| `format`           | The format for a table, it can be `lance`, `iceberg`,..., currently, it only supports `lance` | (none)        | Yes                                                                         | 1.1.0         |
| `location`         | The location to store the table meta and data.                                                | (none)        | No, but if this is not set in catalog or schema, then it's a required value | 1.1.0         |

Of course, apart from the above-required properties, you can also set other table properties supported by the underlying lakehouse storage system or your custom properties.

### Table indexes

This part is almost the same as relational catalog, please refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-partitioning-distribution-sort-ordering-and-indexes) for more details.
However, different lakehouse storage systems may have different supports for indexes, and the following tables show the support for indexes in a Lance-based lakehouse storage system.

| Index type | Description                                                                                                                                                                                                                                                                                                                        | Lance |
|------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------|         
| SCALAR     | SCALAR index is used to optimize searches on scalar data types such as integers, floats, and so on.                                                                                                                                                                                                                                |  Y    |
| VECTOR     | VECTOR index is used to optimize similarity searches in high-dimensional vector spaces.                                                                                                                                                                                                                                            |  Y    |
| BTREE      | BTREE index is a balanced tree data structure that maintains sorted data and allows for logarithmic time complexity for search, insert, and delete operations.                                                                                                                                                                     |  Y    |
| INVERTED   | INVERTED index is a data structure used to optimize full-text searches by mapping terms to  their locations within a dataset, allowing for quick retrieval of documents containing  specific words or phrases.                                                                                                                     |  Y    |
| IVF_FLAT   | IVF_FLAT (Inverted File with Flat quantization) index is used for efficient similarity searches in high-dimensional vector spaces by partitioning the vector space into clusters and storing vectors in a flat structure within each cluster.                                                                                      |  Y    |
| IVF_SQ     | IVF_SQ (Inverted File with Scalar Quantization) index is used for efficient similarity searches in high-dimensional vector spaces by partitioning the vector space into clusters and storing quantized representations of vectors within each cluster to reduce memory usage.                                                      |  Y    |
| IVF_PQ     | IVF_PQ (Inverted File with Product Quantization) index is used for efficient similarity searches in high-dimensional vector spaces by partitioning the vector space into clusters and storing product-quantized representations of vectors within each cluster to achieve a balance between search accuracy and memory efficiency. |  Y    |

Another point is that **NOT all lakehouse tables support creating indexes when creating a table**, and Lance is one of them. So when creating a lance table, you cannot specify indexes at the same time. You need to create the table first, then create indexes on the table.

### Table operations

Please refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for more details. 

The only difference is when creating/registering a table, you need to specify the `format` property to indicate the underlying lakehouse storage system format, e.g. `lance`.

The following is an example of creating a lance table in a generic lakehouse catalog:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "lance_table",
  "comment": "This is an example lance table",
  "columns": [
    {
      "name": "id",
      "type": "integer",
      "comment": "id column comment",
      "nullable": false,
      "autoIncrement": true,
      "defaultValue": {
        "type": "literal",
        "dataType": "integer",
        "value": "-1"
      }
    }
  ],
  "properties": {
    "format": "lance",
    "location": "/tmp/lance_catalog/schema/lance_table1"
  }
}' http://localhost:8090/api/metalakes/test/catalogs/generic_lakehouse_lance_catalog/schemas/schema/tables
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("generic_lakehouse_lance_catalog");

TableCatalog tableCatalog = catalog.asTableCatalog();

Map<String, String> tablePropertiesMap = ImmutableMap.<String, String>builder()
        .put("format", "lance")
        .put("location", "/tmp/lance_catalog/schema/example_table") 
        .build();

tableCatalog.createTable(
  NameIdentifier.of("schema", "lance_table"),
  new Column[] {
    Column.of("id", Types.IntegerType.get(), "id column comment", false, true, Literals.integerLiteral(-1)),
  },
  "This is an example lance table",
  tablePropertiesMap,
  null,
  null,
  null,
  null
);
```

</TabItem>
</Tabs>

About the properties `location`, if it's not set in table properties, Gravitino will use the following hierarchy to determine the location to store the table metadata and data:
1. If `location` is set in table properties, use it.
2. Else if `location` is set in schema properties, use it and the final location will be `schema_location/table_name`.
3. Else if `location` is set in catalog properties, use it and the final location will be `catalog_location/schema_name/table_name`.
4. If both catalog and schema have `location` set, schema `location` takes precedence over catalog `location`. 
5. Else, throw an error indicating that the location is not specified.

For other operations such as load, alter, drop, and truncate table, they are the same as relational catalog, please refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for more details.

One thing to address is that lakehouse generic catalog supports registering and deregistering external tables. This is useful when you have existing tables in the lakehouse storage system and want to manage them using Gravitino without moving or copying the data.
The following is an example of registering an existing lance table in a generic lakehouse catalog:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "register_lance_table",
  "comment": "This is an example to register lance table",
  "columns": [
  ],
  "properties": {
    "format": "lance",
    "lance.register": "true",
    "location": "/tmp/lance_catalog/schema/lance_table1"
  }
}' http://localhost:8090/api/metalakes/test/catalogs/generic_lakehouse_lance_catalog/schemas/schema/tables
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("generic_lakehouse_lance_catalog");

TableCatalog tableCatalog = catalog.asTableCatalog();

Map<String, String> tablePropertiesMap = ImmutableMap.<String, String>builder()
        .put("format", "lance")
        .put("lance.register", "true")
        .put("location", "/tmp/lance_catalog/schema/example_table") 
        .build();

tableCatalog.createTable(
  NameIdentifier.of("schema", "lance_table"),
  new Column[] {
  },
  "This is an example to register lance table",
  tablePropertiesMap,
  null,
  null,
  null,
  null
);
```