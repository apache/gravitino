---
title: "Generic Lakehouse Catalog"
slug: /lakehouse-generic-catalog
keywords:
  - lakehouse
  - lance
  - metadata
  - generic catalog
  - file system
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Overview

The Generic Lakehouse Catalog is a Gravitino catalog implementation designed to seamlessly integrate with lakehouse storage systems built on file system-based architectures. This catalog enables unified metadata management for lakehouse tables stored on various storage backends, providing a consistent interface for data discovery, governance, and access control.

### What is a Lakehouse?

A lakehouse combines the best features of data lakes and data warehouses:

- **Data Lake Benefits**: 
  - Low-cost storage for massive volumes of raw data
  - Support for diverse data formats (structured, semi-structured, unstructured)
  - Decoupled storage and compute for flexible scaling

- **Data Warehouse Benefits**:
  - ACID transactions for data consistency
  - Schema enforcement and evolution
  - High-performance analytical queries
  - Time travel and versioning

### Supported Storage Systems

The catalog works with lakehouse systems built on top of:

**Storage Backends:**
- **Object Stores:** Amazon S3, Azure Blob Storage, Google Cloud Storage, MinIO
- **Distributed File Systems:** HDFS, Apache Ozone
- **Local File Systems:** For development and testing

**Lakehouse Formats:**
- **Lance** ✅ (Production ready - Full support)
- **Apache Iceberg** ⚠️ (Theoretical support)
- **Delta Lake** ⚠️ (Theoretical support)  
- **Apache Hudi** ⚠️ (Theoretical support)

:::info Current Support Status
While the architecture is designed to support various lakehouse formats, Gravitino currently provides **native production support only for Lance-based lakehouse systems** with comprehensive testing and optimization.
:::

### Why Use Generic Lakehouse Catalog?

1. **Unified Metadata Management**: Single source of truth for table metadata across multiple storage backends
2. **Multi-Format Support**: Extensible architecture to support various lakehouse table formats
3. **Storage Flexibility**: Work with any file system - local, or cloud object stores
4. **Gravitino Integration**: Leverage Gravitino's access control, lineage tracking, and data discovery
5. **Easy Migration**: Register existing lakehouse tables without data movement

### System Requirements

**Storage Requirements:**
- Lakehouse storage system must support standard file system operations:
  - Directory listing and navigation
  - File reading and writing with atomic operations
  - File deletion and renaming
  - Path-based access control (optional but recommended)

**Gravitino Requirements:**
- Gravitino server version 1.1.0 or later
- Configured metalake for catalog creation
- Appropriate permissions for catalog management

**Network Requirements:**
- Network connectivity between Gravitino server and storage backend
- For cloud storage: Internet access and valid credentials
- For HDFS: Proper Hadoop configuration and network access

## Catalog Management

### Capabilities

The Generic Lakehouse Catalog provides comprehensive relational metadata management capabilities equivalent to standard relational catalogs:

**Supported Operations:**
- ✅ Create, read, update, and delete catalogs
- ✅ List all catalogs in a metalake
- ✅ Manage catalog properties and metadata
- ✅ Set and modify catalog locations
- ✅ Configure storage backend credentials

For detailed information on available operations, see [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md).

### Properties

#### Required Properties

| Property   | Description                                          | Example                          | Required |
|------------|------------------------------------------------------|----------------------------------|----------|
| `provider` | Catalog provider type                                | `lakehouse-generic`              | Yes      |
| `location` | Root storage path for all schemas and tables         | `hdfs://namenode:9000/lakehouse` | False    |

#### Key Property: `location`

The `location` property specifies the root directory for the lakehouse storage system. All schemas and tables are stored under this location unless explicitly overridden at the schema or table level.

**Location Resolution Hierarchy:**
1. Table-level `location` (highest priority)
2. Schema-level `location`
3. Catalog-level `location` (fallback)

**Example Location Hierarchy:**
```
Catalog location: hdfs://namenode:9000/lakehouse
└── Schema: sales (hdfs://namenode:9000/lakehouse/sales)
    ├── Table: orders (hdfs://namenode:9000/lakehouse/sales/orders)
    └── Table: customers (custom: s3://analytics-bucket/customers)
```

### Creating a Catalog

Use `provider: "lakehouse-generic"` when creating a generic lakehouse catalog.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "generic_lakehouse_catalog",
  "type": "RELATIONAL",
  "comment": "Generic lakehouse catalog for Lance datasets",
  "provider": "lakehouse-generic",
  "properties": {
    "location": "hdfs://localhost:9000/user/lakehouse"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://127.0.0.1:8090")
    .withMetalake("metalake")
    .build();

Map<String, String> catalogProperties = ImmutableMap.<String, String>builder()
    .put("location", "hdfs://localhost:9000/user/lakehouse")
    .build();

Catalog catalog = gravitinoClient.createCatalog(
    "generic_lakehouse_catalog",
    Type.RELATIONAL,
    "lakehouse-generic",
    "Generic lakehouse catalog for Lance datasets",
    catalogProperties
);
```

</TabItem>
</Tabs>


## Schema Management

### Capabilities

Schema operations follow the same patterns as relational catalogs:

**Supported Operations:**
- ✅ Create schemas with custom properties
- ✅ List all schemas in a catalog
- ✅ Load schema metadata and properties
- ✅ Update schema properties
- ✅ Delete schemas
- ✅ Check schema existence

See [Schema Operations](./manage-relational-metadata-using-gravitino.md#schema-operations) for detailed documentation.

### Properties

Schemas inherit catalog properties and can override specific settings:

| Property   | Description                                          | Inherited from Catalog | Required |
|------------|------------------------------------------------------|------------------------|----------|
| `location` | Custom storage path for schema tables                | Yes                    | No       |

#### Location Inheritance

When a schema doesn't specify a `location`, it inherits from the catalog:

**Without Schema Location:**
```
Catalog: hdfs://namenode:9000/lakehouse
Schema: sales
→ Schema location: hdfs://namenode:9000/lakehouse/sales
→ Table location:  hdfs://namenode:9000/lakehouse/sales/orders
```

**With Schema Location:**
```
Catalog: hdfs://namenode:9000/lakehouse
Schema: sales (location: s3://sales-data/prod)
→ Schema location: s3://sales-data/prod
→ Table location:  s3://sales-data/prod/orders
```

### Schema Operations

**Creating a Schema:**

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "sales",
  "comment": "Sales department data",
  "properties": {
    "location": "s3://sales-bucket/data",
    "owner": "sales-team"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/lakehouse_catalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    .put("location", "s3://sales-bucket/data")
    .put("owner", "sales-team")
    .build();

catalog.asSchemas().createSchema(
    "sales",
    "Sales department data",
    schemaProperties
);
```

</TabItem>
</Tabs>

For additional operations, refer to [Schema Operations documentation](./manage-relational-metadata-using-gravitino.md#schema-operations).

## Table Management

### Supported Operations

For Lance tables in a Generic Lakehouse Catalog:

| Operation | Support Status |
|-----------|---------------|
| List      | ✅ Full       |
| Load      | ✅ Full       |
| Alter     | ⚠️ Partial    |
| Create    | ✅ Full       |
| Register  | ✅ Full       |
| Drop      | ✅ Full       |
| Truncate  | ✅ Full       |

:::note Feature Limitations
- **Partitioning:** Not currently supported
- **Sort Orders:** Not currently supported
- **Distributions:** Not currently supported
:::

### Data Type Mappings

Lance uses Apache Arrow for table schemas. The following table shows type mappings between Gravitino and Arrow:

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

| Property              | Description                                                                                                                                                                                                                                              | Default  | Required     | Since Version |
|-----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|--------------|---------------|
| `format`              | Table format: `lance`, `iceberg`, etc. (currently only `lance` is fully supported)                                                                                                                                                                       | (none)   | Yes          | 1.1.0         |
| `location`            | Storage path for table metadata and data                                                                                                                                                                                                                 | (none)   | Conditional* | 1.1.0         |
| `external`            | Whether the data directory is an external location. If it's `true`, dropping a table will only remove metadata in Gravitino and will not delete the data directory, and purge table will delete both. For a non-external table, dropping will drop both. | false    | No           | 1.1.0         |
| `lance.creation-mode` | Create mode: for create table, it can be `CREATE`, `EXIST_OK` or `OVERWRITE`. and it should be `CREATE` and `OVERWRITE` for registering tables                                                                                                           | `CREATE` | No           | 1.1.0         |
| `lance.register`      | Whether it is a register table operation. This API will not create data directory acutally and it's the user responsibility to create and manage the data directory.                                                                                     | false    | No           | 1.1.0         |


\* **Location Requirement:** Must be specified at catalog, schema, or table level. See [Location Resolution](#location-resolution) below.

You may also set additional properties specific to your lakehouse format or custom requirements.

### Index Support

Index capabilities vary by lakehouse format. The following table shows Lance format support:

| Index Type | Description                                                                                  | Lance Support |
|------------|----------------------------------------------------------------------------------------------|---------------|
| SCALAR     | Optimizes searches on scalar data types (integers, floats, etc.)                             | ✅            |
| VECTOR     | Optimizes similarity searches in high-dimensional vector spaces                              | ✅            |
| BTREE      | Balanced tree for sorted data with logarithmic search/insert/delete complexity               | ✅            |
| INVERTED   | Full-text search optimization through term-to-location mapping                               | ✅            |
| IVF_FLAT   | Vector search with inverted file and flat quantization                                       | ✅            |
| IVF_SQ     | Vector search with scalar quantization for memory efficiency                                 | ✅            |
| IVF_PQ     | Vector search with product quantization balancing accuracy and memory                        | ✅            |

:::caution Index Creation Limitation
**Lance tables do not support index creation during table creation.** You must:
1. Create the table first
2. Then create indexes on the created table

This is a fundamental limitation of Lance format, not Gravitino.
:::

For more details on table indexes, see [Table Partitioning, Distribution, Sort Ordering, and Indexes](./manage-relational-metadata-using-gravitino.md#table-partitioning-distribution-sort-ordering-and-indexes).

### Table Operations

Table operations follow standard relational catalog patterns. See [Table Operations](./manage-relational-metadata-using-gravitino.md#table-operations) for comprehensive documentation.

**Key Requirement:** Specify the `format` property (e.g., `"format": "lance"`) when creating or registering tables.

The following sections provide examples and important details for working with Lance tables. Please note that: Different lakehouse formats may have unique requirements though the overall patterns remain similar.
For example, Lance does not support HDFS storage for table data, while other formats may, please be aware of such differences when working with various formats.

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

Map<String, String> tableProperties = ImmutableMap.<String, String>builder()
    .put("format", "lance")
    .put("location", "/tmp/lance_catalog/schema/example_table")
    .build();

tableCatalog.createTable(
    NameIdentifier.of("schema", "lance_table"),
    new Column[] {
        Column.of("id", Types.IntegerType.get(), "Primary identifier", 
                  false, true, Literals.integerLiteral(-1))
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

#### Location Resolution

If `location` is not specified in table properties, Gravitino follows this resolution hierarchy:

1. **Table property** (highest priority) → Use directly
2. **Schema property** → `{schema_location}/{table_name}`
3. **Catalog property** → `{catalog_location}/{schema_name}/{table_name}`
4. **Not specified** → Error thrown

**Priority Rules:**
- Table location always takes precedence
- Schema location overrides catalog location
- At least one level must specify `location`

**Example:**
```
Catalog location: /data/lakehouse
Schema location:  /custom/schema/path

Table without location:
→ Resolves to: /custom/schema/path/my_table

Table with location: /specific/table/path
→ Resolves to: /specific/table/path
```

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
  - Links to existing Lance dataset
  - No data movement or copying
  - Schema automatically detected from Lance metadata
  - Useful for importing existing datasets
  
- **Creation** (default): 
  - Creates new Lance table from scratch
  - Requires column schema definition
  - Initializes new Lance dataset files
  - Data written during table creation or subsequent inserts
  
- **Empty columns array**: When registering, leave columns empty for auto-detection from Lance table
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

**Issue: Metadata inconsistency**
```
Solution: Ensure Gravitino server has stable connection to metadata backend
```

### Migration Guide

#### Migrating Existing Lance Tables

1. **Inventory**: List all existing Lance table locations
2. **Create Catalog**: Create Generic Lakehouse catalog pointing to root location
3. **Register Tables**: Use register operation for each table
4. **Verify**: Confirm all tables are accessible through Gravitino
5. **Update Clients**: Point applications to Gravitino metadata instead of direct Lance access

**Example Migration Script:**

```python
import lance_namespace as ln

# Connect to Lance REST service
ns = ln.connect("rest", {"uri": "http://localhost:9101/lance"})

# List of existing Lance tables to register
tables_to_migrate = [
    ("sales", "orders", "/data/sales/orders"),
    ("sales", "customers", "/data/sales/customers"),
    ("inventory", "products", "/data/inventory/products")
]

# Register each table
for schema, table, location in tables_to_migrate:
    register_req = ln.RegisterTableRequest(
        id=[f'lakehouse_catalog', schema, table],
        location=location
    )
    ns.register_table(register_req)
    print(f"Registered {schema}.{table}")
```

Other table operations (load, alter, drop, truncate) follow standard relational catalog patterns. See [Table Operations](./manage-relational-metadata-using-gravitino.md#table-operations) for details.
