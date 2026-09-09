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

The Generic Lakehouse Catalog is a Gravitino catalog implementation designed to seamlessly integrate with lakehouse storage systems built on file system-based architectures. This catalog enables unified metadata management for lakehouse tables stored on various storage backends, providing a consistent interface for data discovery, governance, and access control. 

Gravitino fully supports the **Lance** lakehouse format, with plans to extend support to additional formats in the future.

### Benefits

1. **Unified Metadata Management**: Single source of truth for table metadata across multiple storage backends
2. **Multi-Format Support**: Extensible architecture to support various lakehouse table formats such as Lance, Iceberg, Hudi, etc.
3. **Storage Flexibility**: Work with any file system, local, or cloud object stores
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

| Property                    | Description                                                                                                                                                                                                    | Example                 | Required |
|-----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------|----------|
| `provider`                  | Catalog provider type                                                                                                                                                                                          | `lakehouse-generic`     | Yes      |
| `location`                  | Root storage path for all schemas and tables                                                                                                                                                                   | `s3://bucket/lakehouse` | No       |
| `table-location-provider`   | Name of the [table location provider](#pluggable-table-location-provider) that provisions and unprovisions the locations of this catalog's tables. Defaults to `default`, which resolves the location from the table, schema and catalog `location` properties as described below. Immutable once the catalog is created.                               | `default`               | No       |
| `lance.schema-refresh-mode` | Lance table schema refresh mode. `DECLARED_AND_EMPTY` (default) refreshes declared tables and tables with empty stored columns. `VERSION_CHECK` additionally refreshes when the Lance dataset version changes. | `DECLARED_AND_EMPTY`    | No       |

#### Pluggable table location provider

By default the catalog derives a new table's location from the `location` properties, following the
hierarchy described in [Key Property: `location`](#key-property-location). Deployments that allocate
storage through an external service can replace that logic with their own strategy.

Implement `org.apache.gravitino.catalog.lakehouse.generic.TableLocationProvider`, register it in
`META-INF/services/org.apache.gravitino.catalog.lakehouse.generic.TableLocationProvider`, drop the jar
into `catalogs/lakehouse-generic/libs`, and select it with the `table-location-provider` catalog
property (matched case-insensitively against `TableLocationProvider#name()`).

One provider instance is created per catalog. `initialize` is called once with the catalog properties
before the first provisioning, `provisionTableLocation` is called for every table creation and must
be thread-safe, and `close` is called when the catalog is closed. The property is immutable, so a catalog
keeps the provider it was created with.

Implementing `initialize` is optional: a provider that derives the location purely from the context it
is given has nothing to prepare. Override it when the provider holds a remote client, a connection or
any state that must outlive a single table creation, and release those resources in `close`.

Both methods receive a `TableLocationContext`, which carries `tableIdentifier()`, `tableProperties()`
and `schema()` -- the parent schema including its properties. Selection is per catalog, so a provider
that needs different behaviour for different schemas has to read a schema property and dispatch on it
itself. A provider doing that should reject an unrecognized value rather than falling back to another
strategy, and should treat the property as immutable: a table provisioned under one strategy has to be
unprovisioned under the same one.

A user-supplied table `location` reaches the provider through the context, but the location the
provider returns is what gets stored. Whether a user-supplied value is honoured is therefore up to the
selected provider; the built-in provider honours it.

`provisionTableLocation` runs on the server thread handling the table creation request. The catalog
applies no timeout, so a provider that calls a remote service must bound every call itself and fail
fast when the service is unavailable; a call that blocks holds that request thread until it returns.

`provisionTableLocation` must return a non-blank location; the catalog rejects the table creation
otherwise. That is the only check: the shape of the path belongs to the provider, nothing downstream
appends to it, and the value is stored verbatim so that a provider unprovisioning it later sees
exactly the string it returned.

When a table is dropped or purged, the catalog calls `unprovisionTableLocation` so that the provider
can hand the location back. It reads the location to reclaim from `context.tableProperties()` under
the `location` key, which the catalog fills in from the stored table properties read just before the
table was removed. Dropping a schema with cascade unprovisions the location of every table it
contains, one by one.

The method has no default implementation, so every provider has to answer for it. A provider that
composes the path from configuration and registers it nowhere -- like the built-in one -- writes an
empty body.

The unprovisioning happens *after* the table metadata, and the data of a managed table, have been
removed, so a provider that fails does not roll the drop back: the table is gone either way. The
failure is logged at WARN naming the table, the provider and the location that was not reclaimed,
and the drop still reports success, because reporting a drop that did happen as unsuccessful would
only invite a retry that cannot undo anything. For the same reason a failed unprovisioning does not
abort a cascading schema drop.

`unprovisionTableLocation` is called at most once per dropped table, but a server crash between the
removal and the call means it may not be called at all, so a provider reclaiming real storage needs
its own reconciliation to catch those, and must tolerate being called for a location that is already
released.

Two constraints follow from `ServiceLoader` discovery:

- The implementation needs a public no-argument constructor that is cheap and does not throw. Every
  provider on the classpath is instantiated before the one named by the catalog property is selected,
  so a heavy or failing constructor breaks catalog initialization for everyone, including catalogs
  using the built-in provider. Put clients, connection pools and other expensive setup in `initialize`.
- `name()` must be unique across the classpath and must not be `default`, which is reserved by the
  built-in provider. Two providers sharing a name make every catalog selecting that name fail to
  initialize.

#### Key Property: `location`

The `location` property specifies the root directory for the lakehouse table. All schemas and tables are stored under this location unless explicitly overridden at the schema or table level.

**Location Resolution Hierarchy** (applies to the built-in provider; a custom
`table-location-provider` defines its own):
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

Other catalog operations are general with relational catalogs. See [Catalog Operations](./manage-catalogs-and-schemas.md#catalog-operations) for detailed documentation.

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

See [Schema Operations](./manage-catalogs-and-schemas.md#schema-operations) for detailed documentation.

### Schema Properties

Schemas inherit catalog properties and can override specific settings:

| Property   | Description                                              | Example                      | Required |
|------------|----------------------------------------------------------|------------------------------|----------|
| `location` | Custom storage root path for all tables under the schema | 's3://bucket/path_to_schema' | No       |

For location resolution hierarchy, see [Key Property: `location`](#key-property-location) in the Catalog Management section for more details.

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

For additional operations, refer to [Schema Operations documentation](./manage-catalogs-and-schemas.md#schema-operations).

## Table Management

### Supported Operations

Since different lakehouse table formats have varying capabilities, table operation support may differ. The following are table operations for different lakehouse formats:

- [Lance Format Support](./lakehouse-generic-lance-table.md)
