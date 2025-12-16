---
title: "Lance table support"
slug: /lance-table-support
keywords:
- lakehouse
- lance
- metadata
- generic catalog
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


## Overview

This document describes how to use Apache Gravitino to manage a generic lakehouse catalog using Lance as the underlying table format. 


## Table Management

### Supported Operations

For Lance tables in a Generic Lakehouse Catalog, the following table summarizes supported operations:

| Operation | Support Status  |
|-----------|-----------------|
| List      | ✅ Full          |
| Load      | ✅ Full          |
| Alter     | Not support now |
| Create    | ✅ Full          |
| Register  | ✅ Full          |
| Drop      | ✅ Full          |
| Purge     | ✅ Full          |

:::note Feature Limitations
- **Partitioning:** Not currently supported
- **Sort Orders:** Not currently supported
- **Distributions:** Not currently supported
- **Indexes:** Not currently supported
:::

### Data Type Mappings

Lance uses Apache Arrow for table schemas. The following table shows type mappings between Gravitino and Arrow:

| Gravitino Type                   | Arrow Type                              |
|----------------------------------|-----------------------------------------|
| `Struct`                         | `Struct`                                |
| `Map`                            | Not supported by Lance                  |
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
| `Interval_year`                  | Not supported by Lance                  |
| `Interval_day`                   | `Duration(Microsecond)`                 |
| `External(arrow_field_json_str)` | Any Arrow Field                         |

### External Type Support

For Arrow types not natively mapped in Gravitino, use the `External(arrow_field_json_str)` type, which accepts a JSON string representation of an Arrow `Field`.

**Requirements:**
- JSON must conform to Apache Arrow [Field specification](https://github.com/apache/arrow-java/blob/ed81e5981a2bee40584b3a411ed755cb4cc5b91f/vector/src/main/java/org/apache/arrow/vector/types/pojo/Field.java#L80C1-L86C68)
- `name` attribute must match column name exactly
- `nullable` attribute must match column nullability
- `children` array:
  - Empty for primitive types
  - Contains child field definitions for complex types (Struct, List)

**Examples:**

| Arrow Type        | External Type Definition                                                                                                                                                                                                                |
|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Large Utf8`      | `External("{\"name\":\"col_name\",\"nullable\":true,\"type\":{\"name\":\"largeutf8\"},\"children\":[]}")`                                                                                                                               |
| `Large Binary`    | `External("{\"name\":\"col_name\",\"nullable\":true,\"type\":{\"name\":\"largebinary\"},\"children\":[]}")`                                                                                                                             |
| `Large List`      | `External("{\"name\":\"col_name\",\"nullable\":true,\"type\":{\"name\":\"largelist\"},\"children\":[{\"name\":\"element\",\"nullable\":true,\"type\":{\"name\":\"int\",\"bitWidth\":32,\"isSigned\":true},\"children\":[]}]}")`         |
| `Fixed-Size List` | `External("{\"name\":\"col_name\",\"nullable\":true,\"type\":{\"name\":\"fixedsizelist\",\"listSize\":10},\"children\":[{\"name\":\"element\",\"nullable\":true,\"type\":{\"name\":\"int\",\"bitWidth\":32,\"isSigned\":true},\"children\":[]}]}")` |

### Table Properties

Required and optional properties for tables in a Generic Lakehouse Catalog:

| Property              | Description                                                                                                                                                                                                                                                                                                                                     | Default  | Required     | Since Version |
|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|--------------|---------------|
| `format`              | Table format: `lance`, currently only `lance` is fully supported.                                                                                                                                                                                                                                                                               | (none)   | Yes          | 1.1.0         |
| `location`            | Storage path for table metadata and data, Lance currently supports: S3, GCS, OSS, AZ, File, Memory and file-object-store.                                                                                                                                                                                                                       | (none)   | Conditional* | 1.1.0         |
| `external`            | Whether the data directory is an external location. If it's `true`, dropping a table will only remove metadata in Gravitino and will not delete the data directory, and purge table will delete both. For a non-external table, dropping will drop both.                                                                                        | false    | No           | 1.1.0         |
| `lance.creation-mode` | Create mode: for create table, it can be `CREATE`, `EXIST_OK` or `OVERWRITE`. and it should be `CREATE` or `OVERWRITE` for registering tables                                                                                                                                                                                                   | `CREATE` | No           | 1.1.0         |
| `lance.register`      | Whether it is a register table operation. If it's `true`, This API will not create data directory actually and it's the user's responsibility to create and manage the data directory. `false` it will actually create a table.                                                                                                                 | false    | No           | 1.1.0         |
| `lance.storage.xxxx`  | Any additional storage-specific properties required by Lance format (e.g., S3 credentials, HDFS configs). Replace `xxxx` with actual property names. For example, we can use `lance.storage.aws_access_key_id` to set S3 aws_access_key_id when using a S3 location, for detail, please refer to https://lancedb.com/docs/storage/integrations/ | (none)   | No           | 1.1.0         |

- `CREATE`: Create a new table, fail if the table already exists.
- `EXIST_OK`: Create a new table if it does not exist, otherwise do nothing.
- `OVERWRITE`: Create a new table, overwrite if the table already exists, it will delete the existing data directory first if the table is not a registered table and then create a new one.

**Location Requirement:** Must be specified at catalog, schema, or table level. See [Location Resolution](./lakehouse-generic-catalog.md#key-property-location).

You may also set additional properties specific to your lakehouse format or custom requirements.

### Table Operations

Table operations follow standard relational catalog patterns. See [Table Operations](./manage-relational-metadata-using-gravitino.md#table-operations) for comprehensive documentation.

The following sections provide examples and important details for working with Lance tables. 

#### Creating a Lance Table

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "lance_table",
  "comment": "Example Lance table",
  "columns": [
    {
      "name": "id",
      "type": "integer",
      "comment": "Primary identifier",
      "nullable": false
    }
  ],
  "properties": {
    "format": "lance",
    "location": "/tmp/lance_catalog/schema/lance_table"
  }
}' http://localhost:8090/api/metalakes/test/catalogs/generic_lakehouse_lance_catalog/schemas/schema/tables
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("generic_lakehouse_lance_catalog");
TableCatalog tableCatalog = catalog.asTableCatalog();

Map<String, String> tableProperties = ImmutableMap.<String, String>builder()
    .put("format", "lance")
    .put("location", "/tmp/lance_catalog/schema/example_table")
    .build();

tableCatalog.createTable(
    NameIdentifier.of("schema", "lance_table"),
    new Column[] {
        Column.of("id", Types.IntegerType.get(), "Primary identifier", 
                  true, false, null)
    },
    "Example Lance table",
    tableProperties,
    null,  // partitions
    null,  // distributions
    null,  // sortOrders
    null   // indexes
);
```

</TabItem>
</Tabs>

#### Registering External Tables

Register existing Lance tables without moving or copying data:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "register_lance_table",
  "comment": "Registered existing Lance table",
  "columns": [],
  "properties": {
    "format": "lance",
    "lance.register": "true",
    "location": "/tmp/lance_catalog/schema/existing_lance_table"
  }
}' http://localhost:8090/api/metalakes/test/catalogs/generic_lakehouse_lance_catalog/schemas/schema/tables
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("generic_lakehouse_lance_catalog");
TableCatalog tableCatalog = catalog.asTableCatalog();

Map<String, String> registerProperties = ImmutableMap.<String, String>builder()
    .put("format", "lance")
    .put("lance.register", "true")
    .put("location", "/tmp/lance_catalog/schema/existing_lance_table")
    .build();

tableCatalog.createTable(
    NameIdentifier.of("schema", "register_lance_table"),
    new Column[] {},  // Schema auto-detected from existing table
    "Registered existing Lance table",
    registerProperties,
    null, null, null, null
);
```

</TabItem>
</Tabs>

:::tip Registration vs Creation
- **Registration** (`lance.register: true`):
  - Links to existing Lance dataset or a path placeholder
  - Schema automatically detected from Lance metadata
  - Useful for importing existing datasets

- **Creation** (default):
  - Creates new Lance table from scratch
  - Requires column schema definition
  - Initializes new Lance dataset files
:::

## Advanced Topics

### Troubleshooting

#### Common Issues

**Issue: "Location not specified" error**
```
Solution: Ensure at least one level (catalog/schema/table) specifies the location property
```

**Issue: Permission denied errors**
```
Solution: Check file system permissions and credentials for the storage backend
```

**Issue: Table not found after registration**
```
Solution: Verify the location path points to a valid Lance dataset directory
```

### Migration Guide

#### Migrating Existing Lance Tables

1. **Inventory**: List all existing Lance table locations
2. **Create Catalog**: Create Generic Lakehouse catalog pointing to root location
3. **Register Tables**: Use register operation for each table
4. **Verify**: Confirm all tables are accessible through Gravitino
5. **Update Clients**: Point applications to Gravitino metadata instead of direct Lance access

**Example Migration Script:**

```shell
# List of existing Lance tables to register
tables_to_migrate=(
    "sales orders /data/sales/orders"
    "sales customers /data/sales/customers"
    "inventory products /data/inventory/products"
)

# Register each table
for entry in "${tables_to_migrate[@]}"; do
    read -r schema table location <<< "$entry"
    echo ${schema}
    echo ${table}

    curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
      -H "Content-Type: application/json" -d "{
      \"name\": \"${table}\",
      \"comment\": \"Registered existing Lance table\",
      \"columns\": [],
      \"properties\": {
        \"format\": \"lance\",
        \"lance.register\": \"true\",
        \"location\": \"${location}\"
      }
    }" http://localhost:8090/api/metalakes/test/catalogs/generic_lakehouse_lance_catalog/schemas/$schema/tables

    echo "Registered ${schema}.${table}"
done
```

Other table operations (load, alter, drop, truncate) follow standard relational catalog patterns. See [Table Operations](./manage-relational-metadata-using-gravitino.md#table-operations) for details.

