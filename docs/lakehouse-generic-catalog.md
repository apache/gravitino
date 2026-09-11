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

All three callbacks receive a `TableLocationContext`, which carries `tableIdentifier()`,
`tableProperties()`, `schema()` -- the parent schema including its properties -- and
`isExternal()`, the `external` property read as a boolean. Selection is per catalog, so a provider
that needs different behaviour for different schemas has to read a schema property and dispatch on it
itself. A provider doing that should reject an unrecognized value rather than falling back to another
strategy, and should treat the property as immutable: a table provisioned under one strategy has to be
unprovisioned under the same one.

A **request that supplies its own `location` never reaches the provider**
-- the supplied value is stored, normalized only with a trailing slash. In this catalog a caller
supplies a location mostly because the data is already there: an external Delta table, or a Lance
registration, both of which point at a dataset that exists. Asking a provider for an address in
those cases would repoint the table at a freshly allocated empty path and orphan the caller's data,
while leaving behind an allocation that is never written to and never handed back.

The catalog cannot tell those requests apart from a caller merely overriding placement, so it keeps
the supplied location in both cases -- which is also exactly what it did before providers existed.
A deployment that wants allocation to be mandatory has to reject a caller-supplied `location`
before it reaches the catalog; a provider cannot enforce it, because it is not called. Everything
else does reach the provider, including an external table that carries no `location`; the table
format decides whether that combination is valid at all.

`provisionTableLocation` runs on the server thread handling the table creation request. The catalog
applies no timeout, so a provider that calls a remote service must bound every call itself and fail
fast when the service is unavailable; a call that blocks holds that request thread until it returns.

The catalog does not fence in-flight requests against `close()`, so a request that started before
the catalog was closed can reach `provisionTableLocation`, `unprovisionTableLocation` or
`releaseUnusedLocation` afterwards. A provider is not asked to keep working across a close, only to
fail cleanly rather than corrupt anything -- which a closed client throwing already does. On the
drop and release paths that failure is logged at WARN and nothing else happens; on the provisioning
path it fails the table creation, which is the right answer for a catalog that is shutting down.

`provisionTableLocation` must return a non-blank location; the catalog rejects the table creation
otherwise. That is the only check: the shape of the path belongs to the provider, nothing downstream
appends to it, and the value is stored verbatim so that a provider unprovisioning it later sees
exactly the string it returned.

When a table is dropped or purged, the catalog calls `unprovisionTableLocation` so that the provider
can hand the location back. It reads the location to reclaim from `context.tableProperties()` under
the `location` key, which the catalog fills in from the stored table properties read just before the
table was removed. Dropping a schema with cascade unprovisions the location of every table it
contains, one by one, resolving the shared parent schema once for the whole cascade.

The method has no default implementation, so every provider has to answer for it. A provider that
composes the path from configuration and registers it nowhere -- like the built-in one -- writes an
empty body.

The unprovisioning happens *after* the table metadata, and the data of a managed table, have been
removed, so a provider that fails does not roll the drop back: the table is gone either way. The
failure is logged at WARN naming the table, the provider and the location that was not reclaimed,
and the drop still reports success, because reporting a drop that did happen as unsuccessful would
only invite a retry that cannot undo anything. For the same reason a failed unprovisioning does not
abort a cascading schema drop.

`unprovisionTableLocation` is called once per dropped table, but it is not guaranteed to be called
at all. A crash between the removal and the call skips it, and so does a failure raised inside the
drop after the metadata is already gone, which takes no crash and leaves the server running
normally. A provider reclaiming real storage needs its own reconciliation to catch both, and must
tolerate being called for a location that is already released.

**External tables are skipped.** The catalog does not own their data — the table formats leave the
dataset in place on drop — so asking a provider to hand the location back would invite it to delete
exactly the data the catalog just promised not to touch. A leak is recoverable and a deletion is
not, so `unprovisionTableLocation` is not called for a table whose `external` property is true.
`context.isExternal()` reports the same flag on the paths where the provider *is* called.

The `external` flag is an approximation of the rule this callback wants, which is "hand back only
what was handed out", and two cases stay asymmetric under it:

- An external table created *without* a location does get one provisioned, because both table
  formats check the location only after the catalog has filled it in. Skipping leaks that
  allocation.
- A table that is not external, but whose creation carried its own `location`, was never
  provisioned, yet is still unprovisioned — so the provider is asked about a path it never issued.

Telling those apart exactly would need the catalog to record, per table, whether it provisioned the
location, which it does not do today. Both are why a provider reclaiming real storage needs its own
reconciliation, and why it must tolerate a location it does not recognize.

There is no separate purge flag in the context, because for the tables this catalog manages
`purgeTable` delegates to `dropTable` and the two paths remove exactly the same things.

##### Releasing a location the table format did not use

A format may decline the location it was given and still report success. Lance's `EXIST_OK`
creation mode returns the table that already exists, at the location it already had, and a client
retrying a create is the ordinary way to reach that. Without a callback the location provisioned for
that call would leak, once per retried create, with nothing in the logs to show it.

That callback is **`releaseUnusedLocation`, not `unprovisionTableLocation`**, and the difference
matters. The table here is alive: the creation succeeded and the caller is about to be handed the
table. Only the location went unused. A provider that derives paths from configuration does the same
nothing in both methods, but a provider that books allocations against `(schema, table)` would read
the drop callback literally and delete the registration of a table that exists. Two methods make
that difference hard to overlook in a way no runtime flag would. For the same reason the `external`
property is *not* a signal to skip here, though it is on the drop path: the location being released
was allocated by this provider moments ago, on request, so nobody else's data can be under it.

`releaseUnusedLocation` has a default implementation that does nothing, which leaks the unused
location. That is the deliberate default: not releasing is what this catalog did before the callback
existed and costs one stray allocation, whereas releasing something the provider has misidentified
costs live data. A provider that allocates real storage should implement it. A failure is logged at
WARN and does not fail the creation, which already succeeded.

This is deliberately the opposite call from `unprovisionTableLocation`, which has no default at all
so that no provider can stay silent about drops by accident. The cost of silence is what differs:
there it leaks a location on every drop, on the ordinary path, forever; here it leaks one location
in the uncommon case that a format declined the one it was given. A provider that cannot release by
path -- because the service behind it only deletes by table identity -- should leave the method
alone and reclaim through its own reconciliation, rather than write a body that would be wrong.

The catalog decides a location went unused by comparing the location it handed the format with the
one the created table reports, ignoring a trailing slash, since that is the one rewrite the catalog
performs itself. A format that rewrites the location further -- collapsing a duplicated separator,
or normalizing a URI scheme -- looks from here like a format that declined it, so a provider whose
paths may come back rewritten should verify before reclaiming.

**Known limitations.** Four of them, and they all point the same way: a provider that manages real
storage needs its own reconciliation against the catalog, and cannot treat these callbacks as a
complete record of the locations it handed out.

- `location` is a mutable table property, so `alterTable(setProperty("location", ...))` repoints a
  table without the provider being told. The old location is never unprovisioned and the new one
  never went through the provider.
- `provisionTableLocation` runs before the table is actually created, so a creation that fails
  afterwards -- a table that already exists, or a failure inside the table format itself -- leaves a
  location provisioned for a table that does not exist. There is no compensating unprovision, and
  that is deliberate: a table format that fails partway through creation may already have written to
  the location, and unprovisioning would then tell the provider it may reclaim a path that has data
  on it. Leaking an unused path is the safer of the two failures, and doing better would need the
  format to report whether it touched storage before failing, which the interface cannot express.
  The table format itself compensates where it can: Lance deletes the dataset it just created when
  the metadata write then fails. Everything the catalog can check on its own -- the table format is
  given, the format is supported -- is checked before the provider is consulted, so the cases that
  remain are the ones only the table format can detect.
- A table format that drops a table through its own internals rather than through the catalog does
  not trigger the release callback. Lance's `OVERWRITE` creation mode does this: it drops the
  existing table and creates a new one, so the old location is never handed back. This is the same
  shape as the cascading schema drop, which the catalog does route through its own `dropTable`, but
  a format-internal drop is not visible to the catalog at all.
- `alterTable(rename(...))` changes a table's identity without telling the provider, and without
  moving any data. A provider deriving the path from the table name is left with a path that no
  longer matches the name, which is cosmetic. A provider that books allocations against
  `(schema, table)` loses the table altogether: the drop that follows arrives under the new name,
  and the allocation booked under the old one is never handed back. Such a provider has to reconcile
  renames out of band, or the deployment has to forbid renaming tables in this catalog.

Two constraints follow from `ServiceLoader` discovery:

- The implementation needs a public no-argument constructor that is cheap and does not throw.
  Selecting a provider means asking each candidate its name, and `name()` is an instance method, so
  every provider on the classpath is constructed once before the named one is selected. That scan
  happens once per class loader and its result is remembered, so a heavy constructor costs the first
  catalog to start rather than every catalog, but it still costs that one -- including when the
  provider it is slowing down is not the one being selected. A constructor that throws costs that
  provider the ability to be selected at all. Put clients, connection pools and other expensive
  setup in `initialize`, which runs only on the selected provider.
- `name()` must be unique across the classpath and must not be `default`, which is reserved by the
  built-in provider. If two providers share a name, every catalog selecting that name fails to
  initialize. A candidate whose constructor or `name()` throws is logged and skipped, so one
  misbehaving provider does not stop a catalog that named a different one. A services file naming a
  class that cannot be loaded at all is not survivable in the same way: it fails the scan before any
  candidate is reached.

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
