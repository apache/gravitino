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

Currently, Gravitino fully supports the **Lance** lakehouse format, with plans to extend support to additional formats in the future.

### Why Use Generic Lakehouse Catalog?

1. **Unified Metadata Management**: Single source of truth for table metadata across multiple storage backends
2. **Multi-Format Support**: Extensible architecture to support various lakehouse table formats such as Lance, Iceberg, Hudi, etc.
3. **Storage Flexibility**: Work with any file system - local, or cloud object stores
4. **Gravitino Integration**: Leverage Gravitino's metadata management, access control, lineage tracking, and data discovery
5. **Easy Migration**: Register existing lakehouse tables without data movement

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

### Catalog Properties

#### Required Properties

| Property   | Description                                  | Example                  | Required | Since Version |
|------------|----------------------------------------------|--------------------------|----------|---------------|
| `provider` | Catalog provider type                        | `lakehouse-generic`      | Yes      | 1.1.0         |
| `location` | Root storage path for all schemas and tables | `s3://bucket/lakehouse` | False    | 1.1.0         |


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

Other catalog operations are general with relational catalogs. See [Catalog Operations](./manage-relational-metadata-using-gravitino.md#catalog-operations) for detailed documentation.

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

### Schema Properties

Schemas inherit catalog properties and can override specific settings:

| Property   | Description                           | Inherited from Catalog | Required | Since version | 
|------------|---------------------------------------|------------------------|----------|---------------|
| `location` | Custom storage path for schema tables | Yes                    | No       | 1.1.0         |

About the details of schema `location`, please refer to Key Property: `location` in [Catalog Properties](#key-property-location) section.

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

### Supported Operations

Since different lakehouse formats have varying capabilities, table operation support may differ. The following are table operations for different lakehouse formats.

- [Lance Format Support](./lakehouse-generic-lance-table.md)