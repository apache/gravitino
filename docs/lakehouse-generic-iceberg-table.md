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

| Operation | Support Status  |
|-----------|-----------------|
| List      | ✅ Supported     |
| Load      | ✅ Supported     |
| Alter     | ✅ Supported     |
| Create    | ✅ Supported     |
| Drop      | ✅ Supported     |
| Purge     | ❌ Not supported |

* Alter supports metadata updates; `format` and `external` are immutable.

:::note Feature Limitations
- **Managed tables:** Not supported yet. A managed Iceberg table mode (with commit support) will be
  added in the next release.
- **Purge:** Not supported for external Iceberg tables because Gravitino does not manage the
  underlying data.
- **Indexes:** Not supported for Iceberg tables in the Generic Lakehouse Catalog.
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

| Property   | Description                                 | Default | Required       | Since Version |
|------------|---------------------------------------------|---------|----------------|---------------|
| `format`   | Table format. Must be `iceberg`.            | (none)  | Yes            | 1.1.0         |
| `external` | Must be `true` for external Iceberg tables. | false   | Yes (enforced) | 1.2.0         |

**Immutability:** `format` and `external` are immutable after table creation.

### Table Operations

Table operations follow standard relational catalog patterns. See
[Table Operations](./manage-relational-metadata-using-gravitino.md#table-operations).

**Distribution defaulting:** If distribution is not specified (`none`), Gravitino will default to
`range` when sort orders are present, `hash` when partitions are present, and `none` otherwise.

**Location resolution:** For external Iceberg tables in the Generic Lakehouse Catalog, Gravitino
does not generate a `location`. If you provide `location` in table properties, Gravitino will store
it as-is. If `location` is omitted, the table is still created and Gravitino will not infer or
resolve a storage path. Use this mode when the table location is managed outside Gravitino (for
example by an existing Iceberg catalog), and set `location` explicitly if downstream tools require
it.

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
    "external": "true"
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
    .build();

tableCatalog.createTable(
    NameIdentifier.of("schema", "iceberg_table"),
    new Column[] {
        Column.of("id", Types.IntegerType.get(), "Primary identifier",
                  false, false, null)
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
