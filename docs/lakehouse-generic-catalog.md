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

| Property                    | Description                                                                                                                                                                                                                                                                                                               | Example                 | Required |
|-----------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------|----------|
| `provider`                  | Catalog provider type                                                                                                                                                                                                                                                                                                     | `lakehouse-generic`     | Yes      |
| `location`                  | Root storage path for all schemas and tables                                                                                                                                                                                                                                                                              | `s3://bucket/lakehouse` | No       |
| `table-location-provider`   | Name of the [table location provider](#pluggable-table-location-provider) that provisions and unprovisions the locations of this catalog's tables. Defaults to `default`, which resolves the location from the table, schema and catalog `location` properties as described below. Immutable once the catalog is created. | `default`               | No       |
| `lance.schema-refresh-mode` | Lance table schema refresh mode. `DECLARED_AND_EMPTY` (default) refreshes declared tables and tables with empty stored columns. `VERSION_CHECK` additionally refreshes when the Lance dataset version changes.                                                                                                            | `DECLARED_AND_EMPTY`    | No       |

#### Pluggable table location provider

By default the catalog derives a new table's location from the `location` properties, following the
hierarchy described in [Key Property: `location`](#key-property-location). Deployments that allocate
storage through an external service can replace that logic with their own strategy.

Implement `org.apache.gravitino.catalog.lakehouse.generic.TableLocationProvider`, register it in
`META-INF/services/org.apache.gravitino.catalog.lakehouse.generic.TableLocationProvider`, drop the jar
into `catalogs/lakehouse-generic/libs`, and select it with the `table-location-provider` catalog
property (matched case-insensitively against `TableLocationProvider#name()`).

The interface is two operations wide -- `provisionTableLocation` and `unprovisionTableLocation` --
plus `name()`.

**The catalog reports; the provider decides.** The catalog's part is to hand over an accurate account
of what happened -- what the creation request asked for, where the table ended up, whether the data
under a location is gone -- and the provider's part is to decide what that account means for the
storage it manages. The catalog does not model what an implementation keeps, and an implementation is
never asked to reconstruct what the catalog or the table formats did. There is one exception, and it
is stated here as an exception rather than as the rule: an external table that is **dropped** does not
reach `unprovisionTableLocation` at all, for the reason given below.

**Responsibilities.** A provider owns the right to use a path; the table format owns the content at
the path. The dividing line is not physical against logical -- a provider may well create real
infrastructure -- but what a thing was created for: anything brought into being so that the path can
be used belongs to the provider, and anything written into the path belongs to the format. A provider
owns its reservation, its registry entry, whatever quota or grant it books, the name itself, and any
container it created to make the path usable; a table format owns the dataset, including deleting it;
the catalog owns neither, storing the location string verbatim and sequencing the two calls.

**Scope.** What this offers is allocation hooks plus best-effort release notification, with recovery
owned by the provider and the deployment. It is deliberately not a distributed transaction and not a
recovery system: the limitations below are real, and a provider that manages storage has to reconcile
against the catalog independently of these two calls.

One provider instance is created per catalog, and the property is immutable, so a catalog keeps the
provider it was created with. **There is no lifecycle**: the provider is never initialized and never
closed. A provider needing a remote client should create it lazily on first use and make it safe to
abandon, since a catalog is evicted from the server's catalog cache when idle and the provider it
held is simply discarded. Both callbacks are called concurrently and must be thread-safe.

Both callbacks receive a `TableLocationContext`, which carries `tableIdentifier()`,
`tableProperties()`, `schema()` -- the parent schema including its properties -- and
`catalogProperties()`. The last is how a provider receives configuration of its own, such as a
service endpoint or a quota group: the same map is handed to every call, so a provider that turns it
into something expensive should build that once and hold it rather than rebuilding it per table. The
`external` flag is available as the `external` entry of `tableProperties()`; read it with
`Boolean.parseBoolean`, which is how the table formats read it. Selection is per catalog, so a provider
that needs different behaviour for different schemas has to read a schema property and dispatch on it
itself. A provider doing that should reject an unrecognized value rather than falling back to another
strategy, and should treat the property as immutable: a table provisioned under one strategy has to be
unprovisioned under the same one.

A **request that supplies its own `location` reaches the provider too**, with the supplied value
visible as the `location` entry of `context.tableProperties()`. What happens to it is the provider's
decision: return it unchanged to honour it, return something else to place the table elsewhere, or
throw to refuse the creation. A provider enforcing a placement policy exists precisely for this
request, and deciding it in the catalog would leave that provider with nothing to enforce.

This is not a behaviour change for a catalog on the built-in provider, which returns a supplied
location verbatim as its first branch -- the same branch the catalog used to apply on its behalf.

An implementation that allocates storage has to handle the case deliberately. In this catalog a
caller usually supplies a location because the data is already there: an external Delta table, or a
Lance registration, both of which point at a dataset that exists. Allocating a fresh path for one of
those and returning it repoints the table at an empty directory and orphans the caller's data, while
the creation still reports success. Returning the supplied value unchanged is the safe default.
Everything else reaches the provider as well, including an external table that carries no
`location`; the table format decides whether that combination is valid at all.

`provisionTableLocation` runs on the server thread handling the table creation request. The catalog
applies no timeout, so a provider that calls a remote service must bound every call itself and fail
fast when the service is unavailable; a call that blocks holds that request thread until it returns.

`provisionTableLocation` must return a non-blank location; the catalog rejects the table creation
otherwise. That is the only check: the shape of the path belongs to the provider, nothing downstream
appends to it, and the value is stored verbatim so that a provider unprovisioning it later sees
exactly the string it returned.

**What the returned location promises, and what it does not.** It promises exactly one thing: that the
location is usable, meaning a table format may create its dataset there and will not be refused for
any reason under the provider's control. Whether an implementation had to reserve, register or create
anything to make that true is its own business, neither required nor forbidden. It does **not** promise
that a directory or prefix exists, that a dataset exists, or that the location is empty -- the last one
deliberately, because in this catalog a caller usually supplies a location precisely when the data is
already there.

When a table is dropped or purged, the catalog calls `unprovisionTableLocation` so that the provider
can hand the location back. It reads the location to reclaim from `context.tableProperties()` under
the `location` key, which the catalog fills in from the stored table properties read just before the
table was removed. Dropping a schema with cascade unprovisions the location of every table it
contains, one by one, resolving the shared parent schema once for the whole cascade.

The method has no default implementation, so every provider has to answer for it. A provider that
composes the path from configuration and registers it nowhere -- like the built-in one -- writes an
empty body.

**What the callback releases, and what it must not touch.** A provider releases what it issued: the
reservation, the registry entry, whatever quota or grant it booked, and the name itself so that it can
be handed out again. It may also remove a container it created itself -- a prefix or a bucket it
brought into being so the path could be used -- but only after establishing that the container holds
nothing except what the provider itself put there, and it must leave the container in place otherwise.
It must **not** delete content at the location: the table format owns the lifecycle of the data and has
already deleted it on every path that reaches the callback, so deleting anything further is at best
redundant and at worst destroys data this catalog promised not to touch.

Nothing about the location string establishes ownership on its own. The catalog supplies one fact, that
nothing anyone needs is under that location any more; whether the provider holds anything there is a
fact only the provider can establish, from its own records.

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

**Dropping an external table skips the callback; purging one does not.** On a drop the catalog does
not own the data — the table formats leave the dataset in place — so asking a provider to hand the
location back would invite it to delete exactly the data the catalog just promised not to touch. A
leak is recoverable and a deletion is not, so `unprovisionTableLocation` is not called when a table
whose `external` property is true is dropped.

A purge is the opposite request. `LanceTableOperations.purgeTable` deletes the external dataset that
its `dropTable` leaves alone, so by the time the callback would run the data is gone and the reason
to skip has gone with it; the location is handed back.

The callback is therefore reached in exactly three situations: a dropped managed table, a purged
managed table, and a purged external one. There is deliberately no flag distinguishing them, because
the invariant they share is about the location rather than the table: **nothing anyone needs is under
the location named in the context**. In all three the table format has already deleted the data under
it. The catalog makes the drop-versus-purge and external distinctions rather than passing them on,
because the catalog is where the knowledge lives about which formats leave data in place.

Even so, `external` is only an approximation of the rule this callback wants, which is "hand back
only what was handed out". An external table created *without* a location does get one provisioned,
because both table formats check the location only after the catalog has filled it in, so skipping
its drop leaks that allocation.

Telling those apart exactly would need the catalog to record, per table, whether it provisioned the
location, which it does not do today. Both are why a provider reclaiming real storage needs its own
reconciliation, and why it must tolerate a location it does not recognize.

`ManagedTableOperations.purgeTable` delegates to `dropTable`, so for a format that does not override
it the two paths remove the same things -- but Lance overrides both, and its purge deletes data its
drop does not. A provider must not assume the two are interchangeable.

##### A worked example: a provider that allocates

A provider backed by an external path-allocation service is the case this interface exists for, and it
makes the boundary concrete.

On `provisionTableLocation` the provider asks the service for a path the table is permitted to use.
The service validates the request, creates on demand whatever container the path needs -- shared by
many tables, coarser than one table, and never removed for one -- records an entry in a registry
saying which table holds the path, and returns the path. The provider returns that string and nothing
else happens: no dataset is created, and the catalog stores the value verbatim.

The table format then creates its dataset at that path, and later, when the table is dropped or
purged, deletes it. Neither step involves the provider.

On `unprovisionTableLocation` the provider deletes the registry entry, which is also what frees the
name so the path can be issued again. The only storage object it touches is the empty directory marker
it created itself, and only after establishing that nothing else is under the prefix; if anything else
is there it leaves the prefix alone. It never deletes the dataset -- by then the format already has.

The built-in provider is the same contract with the allocation set empty. It composes a path from
configuration, holds no reservation and no registry entry, and therefore has nothing to hand back,
which is why its `unprovisionTableLocation` body is empty. That is the correct implementation for a
provider of that shape, and it is also exactly what this catalog did before this interface existed.

**Known limitations.** Six of them, and they all point the same way: a provider that manages real
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
- A location that is provisioned but then not used is not handed back either. A creation mode such
  as Lance's `EXIST_OK` returns the table that already exists, at the location it already has, and
  the location provisioned for that call goes nowhere; a client retrying a create is the ordinary
  way to reach it. Detecting that from the catalog would mean comparing two location strings and
  concluding from the comparison what a provider did internally, which is exactly the inference this
  interface leaves to the provider, so an allocating provider reconciles these along with the
  failures above.
- A provider deriving a deterministic path from a table's identity hands out the same path again
  when a table of the same name is created after the old one is gone, and the built-in provider is
  one such provider. If anything survived under that path -- a drop whose data deletion failed, or a
  format-internal drop as below -- the next creation of that name fails inside the table format
  rather than in the provider, and keeps failing, because every retry derives the same path.
  Clearing what was left behind is outside what this interface can see or do.
- A table format that drops a table through its own internals rather than through the catalog does
  not trigger the unprovision callback. Lance's `OVERWRITE` creation mode does this: it drops the
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
  every provider on the classpath is constructed once, per catalog, before the named one is
  selected -- including when the provider being constructed is not the one selected. A constructor
  that throws costs that provider the ability to be selected at all. Put clients, connection pools
  and other expensive setup out of the constructor and create them lazily on first use, since the
  candidates that are not selected are discarded and there is no close callback to release anything
  they took.
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

### Dropping a table that does not exist

Dropping or purging a table this catalog does not have answers `200` with `{"dropped": false}`. It
previously answered `404 NoSuchTableException`.

This is a wire-level change for clients that treated a repeated `DELETE` as an error: such a request
now succeeds with `dropped: false`. The new response is what `TableCatalog.dropTable` documents
("False if the table does not exist") and what the other catalogs already return, so this catalog
was the one out of step.

:::note
Introduced together with the [pluggable table location
provider](#pluggable-table-location-provider), whose drop path reads the table before removing it
so that the location can be handed back.
:::
