---
title: "Iceberg table support"
slug: /iceberg-table-support
keywords:
- lakehouse
- iceberg
- metadata
- generic catalog
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Overview

This document describes how to use Apache Gravitino to manage Iceberg tables in a Generic Lakehouse
Catalog. The current release supports **external Iceberg tables** (metadata-only registration).
Managed Iceberg tables will be supported in a future release.

## Table Management

### Supported Operations

For Iceberg tables in a Generic Lakehouse Catalog, the following table summarizes supported
operations:

| Operation | Support Status |
|-----------|----------------|
| List      | ✅ Full        |
| Load      | ✅ Full        |
| Alter     | ✅ Full        |
| Create    | ✅ External    |
| Register  | ✅ External    |
| Drop      | ✅ External    |
| Purge     | ❌ Not supported |

:::note Feature Limitations
- **Managed tables:** Not supported yet. A managed Iceberg table mode (with commit support) will be
  added in the next release.
- **Purge:** Not supported for external Iceberg tables because Gravitino does not manage the
  underlying data.
:::

### External vs Managed Tables

- **External table (current release)**:
  - Gravitino stores only metadata.
  - `drop` removes Gravitino metadata only.
  - `purge` is not supported.
  - Use this mode to register existing Iceberg tables.

- **Managed table (next release)**:
  - Gravitino will manage Iceberg table metadata updates and commit lifecycle.
  - `drop` and `purge` behavior will include data lifecycle management.

### Table Properties

Required and optional properties for Iceberg tables in a Generic Lakehouse Catalog:

| Property   | Description                                                                 | Default | Required | Since Version |
|------------|-----------------------------------------------------------------------------|---------|----------|---------------|
| `format`   | Table format. Must be `iceberg`.                                            | (none)  | Yes      | 1.1.0         |
| `location` | Storage path for the Iceberg table (warehouse or table root).               | (none)  | Conditional* | 1.1.0     |
| `external` | Must be `true` for external Iceberg tables.                                 | false   | Yes      | 1.1.0         |

**Location Requirement:** Must be specified at catalog, schema, or table level. See
[Location Resolution](./lakehouse-generic-catalog.md#key-property-location).

### Table Operations

Table operations follow standard relational catalog patterns. See
[Table Operations](./manage-relational-metadata-using-gravitino.md#table-operations).

The following sections provide examples for working with Iceberg tables.

#### Creating an External Iceberg Table

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "iceberg_table",
  "comment": "Example external Iceberg table",
  "columns": [
    {
      "name": "id",
      "type": "integer",
      "comment": "Primary identifier",
      "nullable": false
    }
  ],
  "properties": {
    "format": "iceberg",
    "external": "true",
    "location": "/tmp/iceberg_catalog/schema/iceberg_table"
  }
}' http://localhost:8090/api/metalakes/test/catalogs/generic_lakehouse_catalog/schemas/schema/tables
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("generic_lakehouse_catalog");
TableCatalog tableCatalog = catalog.asTableCatalog();

Map<String, String> tableProperties = ImmutableMap.<String, String>builder()
    .put("format", "iceberg")
    .put("external", "true")
    .put("location", "/tmp/iceberg_catalog/schema/iceberg_table")
    .build();

tableCatalog.createTable(
    NameIdentifier.of("schema", "iceberg_table"),
    new Column[] {
        Column.of("id", Types.IntegerType.get(), "Primary identifier",
                  true, false, null)
    },
    "Example external Iceberg table",
    tableProperties,
    null,  // partitions
    null,  // distributions
    null,  // sortOrders
    null   // indexes
);
```

</TabItem>
</Tabs>

#### Registering Existing Iceberg Tables

Register an existing Iceberg table without moving or copying data:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "register_iceberg_table",
  "comment": "Registered existing Iceberg table",
  "columns": [],
  "properties": {
    "format": "iceberg",
    "external": "true",
    "location": "/tmp/iceberg_catalog/schema/existing_iceberg_table"
  }
}' http://localhost:8090/api/metalakes/test/catalogs/generic_lakehouse_catalog/schemas/schema/tables
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("generic_lakehouse_catalog");
TableCatalog tableCatalog = catalog.asTableCatalog();

Map<String, String> registerProperties = ImmutableMap.<String, String>builder()
    .put("format", "iceberg")
    .put("external", "true")
    .put("location", "/tmp/iceberg_catalog/schema/existing_iceberg_table")
    .build();

tableCatalog.createTable(
    NameIdentifier.of("schema", "register_iceberg_table"),
    new Column[] {},
    "Registered existing Iceberg table",
    registerProperties,
    null, null, null, null
);
```

</TabItem>
</Tabs>
