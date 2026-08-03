---
title: "Delta Lake Tables"
slug: "/delta-table-support"
keywords:
- lakehouse
- delta
- delta lake
- metadata
- generic catalog
license: "This software is licensed under the Apache License version 2."
---

## Overview

Apache Gravitino registers existing Delta Lake tables in a `lakehouse-generic` catalog and holds their metadata: schema, location, properties, and identity partitions. It does not create, write, or modify Delta data.

Every Delta table is external. `external=true` is required on create, and a create call is a registration of a table that already exists at `location`. Dropping the table removes the Gravitino entry and leaves the Delta table where it is.

The practical consequence is that Gravitino and the Delta transaction log are two records of the same table, and Gravitino does not reconcile them. It stores the schema you supply without checking it against the `_delta_log`, so a schema that drifts stays wrong until you correct it.

Delta tables are reached through the Gravitino REST API only.

## Capabilities

| Capability                      | Gravitino REST API                                         |
|---------------------------------|------------------------------------------------------------|
| Adopt an existing table         | Create table with `external=true` and a `location`         |
| Create a new table              | Not available. Gravitino never writes Delta data           |
| List and describe               | List tables, load table                                    |
| Drop or rename a column         | Not available ¹                                            |
| Add an index                    | Not available                                              |
| Remove metadata, keep the table | Drop table                                                 |
| Remove metadata and the table   | Not available. Purge table is rejected for external tables |

¹ Alter is not supported at all. See [Changing a Table](#changing-a-table).

Create also rejects distributions, sort orders, indexes, and any partition transform other than identity.

## Table Properties

| Property   | Description                                                                                                    | Required          |
|------------|----------------------------------------------------------------------------------------------------------------|-------------------|
| `format`   | Selects the table implementation. Use `delta`                                                                  | Yes               |
| `external` | Must be `true`. Gravitino holds metadata only and never deletes the Delta data                                 | Yes               |
| `location` | Directory containing the Delta table, meaning the one holding `_delta_log`. Any Hadoop-compatible scheme works | Yes, or inherited |

`location` resolves through the table, schema, and catalog in that order, as described in [Location Resolution](./lakehouse-generic-catalog.md#location-resolution), so an inherited value satisfies the requirement. In practice set it on the table, since a registration has to land on the directory where the Delta table already is.

## Data Type Mappings

Gravitino performs no type conversion for Delta. It stores the columns you declare and never reads the Delta schema, so use this correspondence to declare metadata that matches the real table.

| Gravitino Type  | Spark Type                                           |
|-----------------|------------------------------------------------------|
| `Boolean`       | `BooleanType`                                        |
| `Byte`          | `ByteType`                                           |
| `Short`         | `ShortType`                                          |
| `Integer`       | `IntegerType`                                        |
| `Long`          | `LongType`                                           |
| `Float`         | `FloatType`                                          |
| `Double`        | `DoubleType`                                         |
| `Decimal(p, s)` | `DecimalType(p, s)`                                  |
| `String`        | `StringType`                                         |
| `Binary`        | `BinaryType`                                         |
| `Date`          | `DateType`                                           |
| `Timestamp`     | `TimestampNTZType`, no timezone, Spark 3.4 and later |
| `Timestamp_tz`  | `TimestampType`, with timezone                       |
| `List`          | `ArrayType`                                          |
| `Map`           | `MapType`                                            |
| `Struct`        | `StructType`                                         |

## Examples

### Registering a Table

Supply the columns yourself. Gravitino stores them as given and does not read the Delta schema to check them.

```shell
GRAVITINO_URL=http://localhost:8090
CATALOG=${GRAVITINO_URL}/api/metalakes/{metalake_name}/catalogs/{catalog_name}
TABLES=${CATALOG}/schemas/{schema_name}/tables

curl -X POST "${TABLES}" \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "{table_name}",
  "comment": "Customer orders Delta table",
  "columns": [
    {"name": "order_id", "type": "long", "nullable": false},
    {"name": "order_date", "type": "date", "nullable": false},
    {"name": "total_amount", "type": "decimal(10,2)"}
  ],
  "properties": {
    "format": "delta",
    "external": "true",
    "location": "s3://{bucket}/{prefix}/{table_name}"
  }
}'
```

Loading and dropping follow the standard patterns in [Table Operations](./manage-relational-metadata-using-gravitino.md#table-operations). A dropped table can be registered again from the same location.

## Partitioning

Gravitino stores identity partitions as metadata. Non-identity transforms, including bucket, truncate, year, and month, are rejected on create.

As with the schema, the partitions you declare are not validated against the Delta transaction log, where the real partitioning lives. Declaring partitions that do not match the table produces metadata that disagrees with the data.

## Changing a Table

Alter is not supported. Modify the table with Delta Lake tools such as Spark or delta-rs, then drop the Gravitino entry and register it again with the updated schema.

Time travel is likewise a Delta Lake feature reached through those tools rather than through Gravitino.

## Troubleshooting

| Error                                                                 | Cause                                                      |
|-----------------------------------------------------------------------|------------------------------------------------------------|
| `Gravitino only supports creating external Delta tables`              | `external=true` was not set                                |
| `'location' property is neither set in table properties`              | No location at the table, schema, or catalog level         |
| `Delta table only supports identity partitioning`                     | A bucket, truncate, year, or month transform was passed    |
| `ALTER TABLE operations are not supported`                            | Modify with Delta Lake tools, then drop and register again |
| `Purge operation is not supported for external Delta tables`          | Drop removes the metadata; remove the files yourself       |
| `Delta table doesn't support specifying distribution in CREATE TABLE` | Pass no distribution                                       |
| `Delta table doesn't support specifying sort orders in CREATE TABLE`  | Pass no sort orders                                        |
| `Delta table doesn't support specifying indexes in CREATE TABLE`      | Pass no indexes                                            |

## Related Pages

- [Lakehouse Generic Catalog](./lakehouse-generic-catalog.md) for the catalog, its properties, and location resolution
- [Lance Tables](./lakehouse-generic-lance-table.md) for the other format the same catalog holds
- [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#table-operations) for the standard load, list, and drop calls
- [Delta Lake documentation](https://docs.delta.io/) for writing and modifying the tables themselves
