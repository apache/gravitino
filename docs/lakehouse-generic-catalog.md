---
title: "Generic Lakehouse Catalog"
slug: "/lakehouse-generic-catalog"
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

The Generic Lakehouse catalog enables Apache Gravitino to manage lakehouse table metadata for formats that do not have a dedicated Gravitino catalog (Iceberg, Paimon, and Hudi each have their own catalog). It manages metadata for tables stored on a Hadoop Compatible File System and exposes a consistent interface for discovery, governance, and access control through Gravitino, with table I/O continuing to go through each format's own engine integration (for example, `lance-spark` for Lance). Use it when you want a single Gravitino-managed access surface that covers Lance datasets and other emerging lakehouse formats alongside relational, dedicated-lakehouse, and fileset catalogs.

Gravitino fully supports the **Lance** lakehouse format; support for additional formats is planned.

### Benefits

1. **Unified Metadata Management**: Single source of truth for table metadata across multiple storage backends
2. **Multi-Format Support**: Extensible architecture to support various lakehouse table formats such as Lance, Iceberg, Hudi, etc.
3. **Storage Flexibility**: Work with any file system, local, or cloud object stores
4. **Gravitino Integration**: Leverage Gravitino's metadata management, access control, lineage tracking, and data discovery
5. **Easy Migration**: Register existing lakehouse tables without data movement

### Requirements and Limitations

- **Supported lakehouse formats.** Apache Lance is fully supported. Delta is documented in a [per-format guide](./lakehouse-generic-delta-table.md); check that guide for current support level. Additional formats including Iceberg, Hudi, and others are planned.
- **Filesystem-based storage.** The catalog manages metadata for tables stored on a Hadoop Compatible File System (local, HDFS, S3, GCS, ADLS, OSS). The same Gravitino cloud bundle JARs used by the Fileset catalog (`gravitino-aws-bundle`, `gravitino-gcp-bundle`, `gravitino-aliyun-bundle`, `gravitino-azure-bundle`) provide the corresponding storage drivers when needed.
- **Location resolution.** Table storage paths resolve through table-level, schema-level, and catalog-level location precedence; see [Key Property: `location`](#key-property-location) for the full resolution rules.
- **Format-specific capabilities.** Table-level operations vary by format. See the per-format guides linked under [Table Management](#table-management) for what each format supports.
- **Metadata only.** The catalog manages metadata and the table-to-path mapping; it does not store or transfer data files. Use the appropriate engine integration (for example, `lance-spark`) for table reads and writes.

## Quick Start

Create a minimum-viable Generic Lakehouse catalog and confirm it is reachable. The example uses a local filesystem path so the walkthrough runs against a default Gravitino installation with no external metastore or cloud storage. For HDFS or cloud-backed catalogs, set `location` to the appropriate URI and ensure the corresponding Gravitino bundle JAR is on the Gravitino server. The walkthrough assumes a Gravitino server at `http://localhost:8090` and a metalake named `test`.

### Create the Catalog

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "lakehouse_catalog",
    "type": "RELATIONAL",
    "comment": "Generic lakehouse catalog",
    "provider": "lakehouse-generic",
    "properties": {
      "location": "file:///tmp/lakehouse"
    }
  }' \
  http://localhost:8090/api/metalakes/test/catalogs
```

The response is a JSON object describing the created catalog. A fuller create-catalog example with both shell and Java tabs is provided in [Create a Catalog](#create-a-catalog) under Catalog Management below.

### Verify the Catalog

```bash
# List catalogs in the metalake. lakehouse_catalog should appear.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs" | jq

# Load the catalog directly and inspect its properties.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/lakehouse_catalog" | jq

# List schemas. The response is an empty array on a freshly created catalog until a schema is added.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/lakehouse_catalog/schemas" | jq
```

**Success check:** the catalog-list response includes `lakehouse_catalog`, the load-catalog response shows `"provider":"lakehouse-generic"` with `location` set to `file:///tmp/lakehouse`, and the schema-list response is a JSON array (an empty array on a fresh catalog is expected). If load-catalog returns an error, confirm that the Gravitino server process has write access to the configured location. For cloud-backed catalogs, ensure the corresponding Gravitino bundle JAR is present on the Gravitino server.

## Catalog Management

### Capabilities

The generic lakehouse catalog provides the same relational metadata management capabilities as standard relational catalogs:

- ✅ Create, read, update, and delete catalogs.
- ✅ List all catalogs in a metalake.
- ✅ Manage catalog properties and metadata.
- ✅ Set and modify catalog locations.
- ✅ Configure storage-backend credentials.

For detailed information on available operations, see [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md).

### Catalog Properties

| Property   | Description                                  | Example                 | Required | Since |
|------------|----------------------------------------------|-------------------------|----------|---------------|
| `provider` | Catalog provider type                        | `lakehouse-generic`     | Yes      | 1.1.0         |
| `location` | Root storage path for all schemas and tables | `s3://bucket/lakehouse` | No       | 1.1.0         |

#### Key Property: `location`

The `location` property specifies the root directory for the lakehouse table. All schemas and tables are stored under this location unless explicitly overridden at the schema or table level.

**Location Resolution Hierarchy:**
1. Table-level `location` (highest priority)
2. Schema-level `location`, then the location of the table will be `{schema_location}/{table_name}`
3. Catalog-level `location` (fallback), then the location of the table will be `{catalog_location}/{schema_name}/{table_name}`

**Example Location Hierarchy:**
```
Case1: only catalog location is set
Catalog location: hdfs://namenode:9000/lakehouse
└── Schema: sales
    ├── Table: orders. Final location of table: hdfs://namenode:9000/lakehouse/sales/orders
    └── Table: customers. Final location of table: hdfs://namenode:9000/lakehouse/sales/customers
    
case2: schema location is set, overriding catalog location and table location is not set   
Catalog location: hdfs://namenode:9000/lakehouse
└── Schema: sales: s3://sales-bucket/data
    ├── Table: orders. Final location of table: s3://sales-bucket/data/orders
    └── Table: customers. Final location of table: s3://sales-bucket/data/customers

case3: table location is set, overriding both schema and catalog locations
Catalog location: hdfs://namenode:9000/lakehouse
└── Schema: sales: s3://sales-bucket/data
    ├── Table: orders.  Table location: s3://sales-bucket/my_orders, Final location of table: s3://sales-bucket/my_orders
    └── Table: customers. Table location: s3://sales-bucket/my_customers, Final location of table: s3://sales-bucket/my_customers
    
```

### Create a Catalog

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

Other catalog operations are general with relational catalogs. See [Catalog Operations](./manage-relational-metadata-using-gravitino.md#catalog-operations) for detailed documentation.

## Schema Management

### Capabilities

Schema operations follow the same pattern as in relational catalogs:

- ✅ Create schemas with custom properties.
- ✅ List all schemas in a catalog.
- ✅ Load schema metadata and properties.
- ✅ Update schema properties.
- ✅ Delete schemas.
- ✅ Check schema existence.

See [Schema Operations](./manage-relational-metadata-using-gravitino.md#schema-operations) for detailed documentation.

### Schema Properties

Schemas inherit catalog properties and can override specific settings:

| Property   | Description                                              | Example                      | Required | Since | 
|------------|----------------------------------------------------------|------------------------------|----------|---------------|
| `location` | Custom storage root path for all tables under the schema | 's3://bucket/path_to_schema' | No       | 1.1.0         |

For location resolution, see [Key property: `location`](#key-property-location) in the catalog-management section.

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

Different lakehouse table formats have different capabilities, so table-operation support varies by format. See the per-format documentation:

- [Lance format](./lakehouse-generic-lance-table.md)
- [Delta format](./lakehouse-generic-delta-table.md)