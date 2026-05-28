---
title: "Lance Tables"
slug: "/lakehouse-generic-lance-table"
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

[Apache Lance](https://lancedb.com) is an open-source columnar table format optimized for AI workloads, with strong support for vector search, fast point lookups, and large multi-modal datasets. Use the Lance format in a Gravitino Generic Lakehouse catalog when you want a single Gravitino-managed access surface that covers Lance datasets alongside relational, dedicated-lakehouse, and fileset catalogs. Table reads and writes continue to go through Lance's own engine integrations (for example, `lance-spark`).

The guide assumes a Generic Lakehouse catalog already exists. See the [Generic Lakehouse catalog doc](./lakehouse-generic-catalog.md) for catalog creation, the catalog/schema/table location resolution model, and the corpus-level Requirements and Limitations.


## Requirements and Limitations

- **Catalog prerequisite.** Lance tables live inside a Generic Lakehouse catalog. See the [Generic Lakehouse catalog doc](./lakehouse-generic-catalog.md) for catalog creation and the catalog/schema/table location resolution rules.
- **Schema model.** Lance uses Apache Arrow for table schemas. Gravitino maps its types to Arrow types as described in [Data Type Mappings](#data-type-mappings); use `External(arrow_field_json_str)` for any Arrow type that has no direct Gravitino mapping (see [External Types](#external-types)).
- **Supported storage backings.** Lance supports S3, GCS, OSS, Azure Blob Storage, the local filesystem, in-memory storage, and other file-object stores. Pass storage-specific credentials and endpoints through `lance.storage.*` properties at the catalog level (preferred, so they apply to every table) or the table level. See [Lance Table with MinIO](#lance-table-with-minio) below for a worked S3-compatible example.
- **Unsupported features.** Lance tables do not support partitioning, sort orders, distributions, or indexes through the Gravitino catalog interface. Vector index management is performed through Lance's own APIs.
- **No `alterTable`.** The Lance per-format catalog does not currently support Gravitino's `alterTable` operation. Use Lance's own APIs through `lance-spark` or the Lance Python client for schema changes.
- **External tables and `external=true`.** Setting `external=true` at the table level changes the drop-semantics: `dropTable` removes Gravitino metadata only, while `purgeTable` removes metadata and the underlying data directory. For non-external tables, both `dropTable` and `purgeTable` remove the data directory.

## Quick Start

Create a Lance table inside an existing Generic Lakehouse catalog and confirm it is reachable. The walkthrough assumes a Gravitino server at `http://localhost:8090`, a metalake named `test`, a Generic Lakehouse catalog named `lakehouse_catalog` (see the [Generic Lakehouse catalog doc](./lakehouse-generic-catalog.md) for catalog creation), and a schema named `schema` under that catalog.

### Create a Schema (if not yet created)

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "schema",
    "comment": "Lance schema"
  }' \
  http://localhost:8090/api/metalakes/test/catalogs/lakehouse_catalog/schemas
```

### Create a Lance Table

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
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
  }' \
  http://localhost:8090/api/metalakes/test/catalogs/lakehouse_catalog/schemas/schema/tables
```

### Verify the Table

```bash
# List tables in the schema. lance_table should appear.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/lakehouse_catalog/schemas/schema/tables" | jq

# Load the table directly and inspect its properties and columns.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/lakehouse_catalog/schemas/schema/tables/lance_table" | jq
```

**Success check:** the table-list response includes `lance_table`, and the load-table response shows `"format":"lance"` in its properties, the configured location, and the one `id` column. The Lance dataset files are created in the configured location directory on the Gravitino server's filesystem. For a fuller create-table example with both shell and Java tabs, see [Create a Lance Table](#create-a-lance-table) below.

## Table Management

### Supported Operations

For Lance tables in a Generic Lakehouse catalog, the following table operations are supported:

| Operation | Support       |
|-----------|---------------|
| List      | Supported     |
| Load      | Supported     |
| Create    | Supported     |
| Register  | Supported     |
| Drop      | Supported     |
| Purge     | Supported     |
| Alter     | Not supported |

For schema changes on a Lance table, use Lance's own APIs through `lance-spark` or the Lance Python client. Lance tables do not support partitioning, sort orders, distributions, or indexes through the Gravitino catalog interface; vector index management is performed through Lance's own APIs.

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

### External Types

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

Required and optional properties for tables in a generic lakehouse catalog:

| Property              | Description                                                                                                                                                                                                                                                                                                                                     | Default  | Required     | Since Version |
|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|--------------|---------------|
| `format`              | Table format: `lance`, only `lance` is fully supported.                                                                                                                                                                                                                                                                               | (none)   | Yes          | 1.1.0         |
| `location`            | Storage path for table metadata and data, Lance supports: S3, GCS, OSS, AZ, File, Memory and file-object-store.                                                                                                                                                                                                                       | (none)   | Conditional* | 1.1.0         |
| `external`            | Whether the data directory is an external location. If it's `true`, dropping a table will only remove metadata in Gravitino and will not delete the data directory, and purge table will delete both. For a non-external table, dropping will drop both.                                                                                        | false    | No           | 1.1.0         |
| `lance.creation-mode` | Create mode for Lance tables. Accepts `CREATE`, `EXIST_OK`, or `OVERWRITE` for table creation, and `CREATE` or `OVERWRITE` for table registration. See [Lance creation modes](#lance-creation-modes) below. | `CREATE` | No           | 1.1.0         |
| `lance.register`      | Whether to register an existing Lance dataset rather than create a new one. When `true`, Gravitino does not create the data directory; the user is responsible for the existence and management of the directory. When `false` (default), Gravitino creates a new Lance table. | false    | No           | 1.1.0         |
| `lance.storage.xxxx`  | Any additional storage-specific properties required by the Lance format (for example, S3 credentials or HDFS configs). Replace `xxxx` with the actual property name; for example, `lance.storage.aws_access_key_id` sets the S3 access key when using an S3 location. See the [Lance storage integrations documentation](https://lancedb.com/docs/storage/integrations/) for the full list. | (none)   | No           | 1.1.0         |

A location must be specified at the catalog, schema, or table level. See [Location resolution](./lakehouse-generic-catalog.md#key-property-location).

Additional format-specific or custom properties can be added alongside the ones above.

#### Lance Creation Modes

The `lance.creation-mode` property accepts the following values:

- `CREATE`: Create a new table; fail if the table already exists.
- `EXIST_OK`: Create a new table if it does not already exist; otherwise do nothing.
- `OVERWRITE`: Create a new table, overwriting an existing one. For a non-registered table, the existing data directory is deleted first and a new one is created. For a registered table, only the Gravitino metadata is overwritten.

### Table Operations

Table operations follow standard relational catalog patterns. See [Table operations](./manage-relational-metadata-using-gravitino.md#table-operations) for comprehensive documentation.

The following sections cover examples and important details for working with Lance tables.

#### Create a Lance Table

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

#### Register External Tables

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

:::tip
Registration vs. creation:

- **Registration** (`lance.register: true`) — links to an existing Lance dataset or a path placeholder. The schema is detected from Lance metadata. Useful for importing existing datasets.
- **Creation** (default) — creates a new Lance table from scratch. Requires a column schema definition. Initializes new Lance dataset files.
:::

## Advanced Topics

### Troubleshooting

#### Common Issues

**`Location not specified` error.** Ensure at least one level (catalog, schema, or table) specifies the `location` property.

**Permission-denied errors.** Check the file-system permissions and credentials for the storage backend.

**Table not found after registration.** Verify that the `location` path points to a valid Lance dataset directory.

### Migration Guide

#### Migrate Existing Lance Tables

1. **Inventory** — list all existing Lance table locations.
2. **Create the catalog** — create a generic lakehouse catalog pointing at the root location.
3. **Register the tables** — use the register operation for each table.
4. **Verify** — confirm all tables are accessible through Gravitino.
5. **Update clients** — point applications at Gravitino metadata instead of direct Lance access.

Example migration script:

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

## Lance Table with MinIO

To use Lance tables stored in MinIO (or another S3-compatible object store) with Gravitino, configure the storage backend once on the Generic Lakehouse catalog. Gravitino returns those storage options to Lance clients, so Spark and other engines do not need to repeat them.

The first request below creates a catalog whose `lance.storage.*` properties cover all tables under the catalog; the second request creates a table on that catalog without restating any storage credentials.

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
  "name": "lance_catalog",
  "type": "RELATIONAL",
  "provider": "lakehouse-generic",
  "comment": "catalog for Lance tables on MinIO",
  "properties": {
    "location": "s3://bucket1/lance",
    "lance.storage.endpoint": "http://minio:9000",
    "lance.storage.access_key_id": "ak",
    "lance.storage.secret_access_key": "sk",
    "lance.storage.allow_http": "true",
    "lance.storage.region": "us-east-1"
  }
}' http://localhost:8090/api/metalakes/test/catalogs

curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "lance_orders",
  "comment": "Order table stored in MinIO",
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
    "location": "s3://bucket1/lance_orders"
  }
}' http://localhost:8090/api/metalakes/test/catalogs/lance_catalog/schemas/sales/tables

```

If you need to override storage on a single table, `lance.storage.*` table properties are still supported.
