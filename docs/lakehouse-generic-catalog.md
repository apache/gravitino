---
title: "Generic Lakehouse Catalog"
slug: "/lakehouse-generic-catalog"
keywords:
  - lakehouse
  - lance
  - delta
  - metadata
  - generic catalog
license: "This software is licensed under the Apache License version 2."
---

## Overview

Some table formats define a catalog of their own, and Apache Gravitino federates to it. Lance and Delta define no catalog. A table in either format is a set of files, so the metadata needs a home. The Generic Lakehouse Catalog provides one, holding the metadata directly rather than federating. That is what makes it generic: a single catalog serves any format that arrives without one of its own.
Each table names its format with the required `format` property, which is immutable once set, and Gravitino selects the matching implementation. Two are registered today.

- **Lance**, as `format=lance`. Fully supported, and documented in [Lance Tables](./lakehouse-generic-lance-table.md).
- **Delta**, as `format=delta`. Registration of existing external tables only, documented in [Delta Lake Tables](./lakehouse-generic-delta-table.md).

Because the metadata lives in Gravitino, these tables participate in the same discovery, access control, and lineage as the rest of a metalake. Registering an existing dataset moves no data.

The Gravitino REST API creates these catalogs, as described below. Lance has a second route: the [Lance REST Service](./lance-rest-service.md) creates one itself when a Lance client first connects, producing the same object. See [Choosing an API](./lance-rest-service.md#choosing-an-api). Delta has no equivalent, so Delta tables always arrive through the Gravitino REST API.

## Creating a Catalog

`provider` selects this implementation and is a field on the request, not a property. What goes in `properties` depends on which format the catalog will hold.

### For Lance Tables

Set the storage root and credentials on the catalog. Tables inherit the location, and Gravitino returns the `lance.storage.*` values to Lance clients so they can read the dataset files directly.

```shell
GRAVITINO_URL=http://localhost:8090
CATALOGS=${GRAVITINO_URL}/api/metalakes/{metalake_name}/catalogs

curl -X POST "${CATALOGS}" \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "{catalog_name}",
  "type": "RELATIONAL",
  "provider": "lakehouse-generic",
  "comment": "Catalog for Lance tables",
  "properties": {
    "location": "s3://{bucket}/{prefix}",
    "lance.storage.access_key_id": "{access_key}",
    "lance.storage.secret_access_key": "{secret_key}",
    "lance.storage.region": "us-east-1"
  }
}'
```

Then create tables as described in [Lance Tables](./lakehouse-generic-lance-table.md).

### For Delta Tables

No properties are needed. Delta tables normally carry their own `location`, because a registration has to land on a directory that already exists, and Gravitino hands nothing to Delta readers, so `lance.storage.*` has no effect. A catalog `location` is still honored if you set one.

```shell
curl -X POST "${CATALOGS}" \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "{catalog_name}",
  "type": "RELATIONAL",
  "provider": "lakehouse-generic",
  "comment": "Catalog for Delta tables"
}'
```

Then register tables as described in [Delta Lake Tables](./lakehouse-generic-delta-table.md).

A single catalog can hold both formats. Setting the Lance properties does no harm to Delta tables alongside them.

## Creating a Schema

Schema creation does not vary by format. A schema `location` narrows where Lance tables land, and is ignored by Delta tables, which always name their own.

```shell
curl -X POST "${CATALOGS}/{catalog_name}/schemas" \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H 'Content-Type: application/json' \
  -d '{"name": "{schema_name}", "comment": "Sales department data"}'
```

## Properties

### Catalog Properties

These go in the `properties` object of the catalog create request, `POST /api/metalakes/{metalake_name}/catalogs`, as shown in [Creating a Catalog](#creating-a-catalog). They can be changed afterward through the catalog alter request.

| Property   | Description                                                  | Example                 | Required |
|------------|--------------------------------------------------------------|-------------------------|----------|
| `location` | Root storage path for schemas and tables beneath the catalog | `s3://bucket/lakehouse` | No       |

### Schema Properties

These go in the `properties` object of the schema create request, `POST /api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas`.

| Property   | Description                                                      | Example                      | Required |
|------------|------------------------------------------------------------------|------------------------------|----------|
| `location` | Storage root for tables under the schema, overriding the catalog | `s3://bucket/path_to_schema` | No       |

Delta contributes no properties at either level. A Delta table names its own location, so it needs nothing from the catalog or schema to be reachable.

### Location Resolution

Every table needs a location, and it can come from any of three levels. The most specific one set wins.

| Level Set | Resulting Table Location                        |
|-----------|-------------------------------------------------|
| Table     | The table `location`, used as given             |
| Schema    | `{schema_location}/{table_name}`                |
| Catalog   | `{catalog_location}/{schema_name}/{table_name}` |

With only a catalog location of `s3://bucket/lakehouse`, a table `orders` in schema `sales` resolves to `s3://bucket/lakehouse/sales/orders`. Setting the schema `sales` to `s3://sales-bucket/data` moves it to `s3://sales-bucket/data/orders`. Setting the table location to `s3://sales-bucket/my_orders` overrides both and is used unchanged.

If no level sets a location, table creation fails.

### Lance Catalog Properties

Two further catalog properties apply only when the catalog holds Lance tables.

| Property                    | Description                                                                             | Example                            | Required |
|-----------------------------|-----------------------------------------------------------------------------------------|------------------------------------|----------|
| `lance.storage.{option}`    | Storage options for Lance tables, such as credentials and endpoint, returned to clients | `lance.storage.region = us-east-1` | No       |
| `lance.schema-refresh-mode` | When Gravitino re-reads Lance table columns from the dataset                            | `DECLARED_AND_EMPTY`               | No       |

#### Storage Options

Set `lance.storage.*` on the catalog rather than repeating it in each engine. Gravitino resolves the values and returns them to clients, so an engine reading the dataset files directly receives the credentials it needs. They can be overridden per table, as described in [Table Properties](./lakehouse-generic-lance-table.md#table-properties).

Gravitino does not define the `lance.storage.*` option names. It strips the prefix and passes whatever it finds to the client, so the names come from Lance rather than from Gravitino and no key is validated or rejected.

The options below cover S3 and S3-compatible stores such as MinIO, and are the ones Gravitino's own integration tests exercise.

| Property                          | Description                                              |
|-----------------------------------|----------------------------------------------------------|
| `lance.storage.access_key_id`     | Access key                                               |
| `lance.storage.secret_access_key` | Secret key                                               |
| `lance.storage.region`            | Bucket region                                            |
| `lance.storage.endpoint`          | Endpoint URL, needed for S3-compatible stores like MinIO |
| `lance.storage.allow_http`        | Set to `true` to permit a plain HTTP endpoint            |

For GCS, Azure, and the full option list per backend, see the [Lance storage documentation](https://lancedb.com/docs/storage/integrations/).

#### Schema Refresh

Gravitino stores table columns in its own metadata store, and some Lance writers update the dataset directly at its location. `lance.schema-refresh-mode` decides when Gravitino re-reads columns to stay in sync.

| Mode                 | Behavior                                                                                                                       |
|----------------------|--------------------------------------------------------------------------------------------------------------------------------|
| `DECLARED_AND_EMPTY` | Default. Refreshes declared tables whose schema has not yet been written, and tables whose Gravitino column list is empty      |
| `VERSION_CHECK`      | Opens the dataset on every load, compares its version with `lance.version`, and refreshes columns when the version has changed |

Use `VERSION_CHECK` only when tables are modified directly at their storage location outside Gravitino, since it adds a dataset open to every load.

A dataset that genuinely has no columns is handled once. Under `DECLARED_AND_EMPTY`, the first load records the checked version in `lance.version` and later loads skip opening the dataset while that version holds. Once columns are written, the next `VERSION_CHECK` load or an explicit alter repairs the schema.

## Related Pages

- [Lance Tables](./lakehouse-generic-lance-table.md) for Lance table properties, type mappings, and operations
- [Delta Lake Tables](./lakehouse-generic-delta-table.md) for registering external Delta tables
- [Lance REST Service](./lance-rest-service.md) for the Lance-native path that creates this catalog for you
- [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md) for the standard catalog, schema, and table operations
