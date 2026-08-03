---
title: "Lance Tables"
slug: "/lance-table-support"
keywords:
- lakehouse
- lance
- metadata
- generic catalog
license: "This software is licensed under the Apache License version 2."
---

## Overview

A Lance table in Apache Gravitino is a Lance dataset on storage plus its metadata in a `lakehouse-generic` catalog. Two APIs reach the same tables.

- **Lance REST API.** The [Lance REST Service](./lance-rest-service.md) speaks the Lance REST Catalog protocol on port 9101, for `lance-spark`, `lance-ray`, and any Lance SDK client.
- **Gravitino REST API.** Tables are ordinary Gravitino relational objects on port 8090, with `format` set to `lance`.

This page describes the table itself, which is the same either way. Where the two APIs differ, the difference is named.

## Capabilities

| Capability                        | Lance REST API                     | Gravitino REST API                      |
|-----------------------------------|------------------------------------|-----------------------------------------|
| Create a table                    | `CreateTable`                      | Create table with `format=lance`        |
| Record a table without a dataset  | `DeclareTable`                     | Create table with `lance.declared=true` |
| Adopt an existing dataset         | `RegisterTable`                    | Create table with `lance.register=true` |
| List and describe                 | `ListTables`, `DescribeTable`      | List tables, load table                 |
| Drop or rename a column           | `drop_columns`, `alter_columns`  ¹ | Alter table ¹                           |
| Add an index                      | Not available ²                    | Alter table ³                           |
| Remove metadata, keep the dataset | `DeregisterTable`                  | Drop table with `external=true`         |
| Remove metadata and the dataset   | `DropTable`  ⁴                     | Purge table                             |

¹ Changes are applied to the Lance dataset before the metadata is updated, and are not atomic across multiple changes, so a failure part way through can leave a subset applied.
² A workload that builds indexes needs the Gravitino REST API for that step.
³ Vector and scalar index types are supported. Vector index parameters are not currently configurable.
⁴ Removes the dataset files even though the service marks every table it creates `external`, so it behaves like Purge rather than like Drop.

Partitioning, sort orders, and distributions are not supported on either API. Column type changes are rejected.

## Table Properties

Every property below is a property of the table, whichever API created it. On the Gravitino REST API you set them directly. On the Lance REST API the service supplies them from the call you made, as the last column shows.

| Property                 | Description                                                                                                                                          | Default  | Supplied on the Lance Path By   |
|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------------------------|
| `format`                 | Selects the table implementation. Use `lance`                                                                                                        | (none)   | Always `lance`                  |
| `location`               | Storage path for the dataset. Lance supports S3, GCS, OSS, Azure, HDFS, local files, and memory                                                      | (none)   | `x-lance-table-location` header |
| `external`               | When `true`, dropping the table removes only the metadata and leaves the dataset. When `false`, dropping removes both                                | `false`  | Always `true`                   |
| `lance.creation-mode`    | `CREATE` fails if the table exists, `EXIST_OK` does nothing if it exists, `OVERWRITE` replaces it and deletes the existing dataset unless registered | `CREATE` | The `mode` query parameter      |
| `lance.register`         | When `true`, links an existing dataset instead of creating one. You manage the data directory                                                        | `false`  | `RegisterTable`                 |
| `lance.declared`         | When `true`, records the table in metadata without creating a dataset                                                                                | `false`  | `DeclareTable`                  |
| `lance.storage.{option}` | Storage options for this table, overriding the catalog values                                                                                        | (none)   | Inherited from the catalog      |
| `lance.version`          | Dataset version Gravitino last read columns from                                                                                                     | (none)   | The server, on both paths       |

`external` is the one row whose described behavior does not hold on the Lance REST API, because `DropTable` removes the dataset files regardless. It matters only if the same table is later dropped through the Gravitino REST API, where the files would be kept.

If `location` is set at neither the table nor the header, it resolves from the schema or catalog. See [Location Resolution](./lakehouse-generic-catalog.md#location-resolution).

## Data Type Mappings

Lance uses Apache Arrow for table schemas.

| Gravitino Type                   | Arrow Type                               |
|----------------------------------|------------------------------------------|
| `Boolean`                        | `Boolean`                                |
| `Byte`                           | `Int8`                                   |
| `Short`                          | `Int16`                                  |
| `Integer`                        | `Int32`                                  |
| `Long`                           | `Int64`                                  |
| `Float`                          | `Float`                                  |
| `Double`                         | `Double`                                 |
| `Decimal(p, s)`                  | `Decimal(p, s)` (128-bit)                |
| `String`                         | `Utf8`                                   |
| `Binary`                         | `Binary`                                 |
| `Fixed(n)`                       | `Fixed-Size Binary(n)`                   |
| `Date`                           | `Date`                                   |
| `Time`/`Time(9)`                 | `Time Nanosecond`                        |
| `Timestamp`/`Timestamp(6)`       | `TimestampType Microsecond withoutZone`  |
| `Timestamp(0)`                   | `TimestampType Second withoutZone`       |
| `Timestamp(3)`                   | `TimestampType Millisecond withoutZone`  |
| `Timestamp(9)`                   | `TimestampType Nanosecond withoutZone`   |
| `Timestamp_tz`/`Timestamp_tz(6)` | `TimestampType Microsecond withUtc`      |
| `Timestamp_tz(0)`                | `TimestampType Second withUtc`           |
| `Timestamp_tz(3)`                | `TimestampType Millisecond withUtc`      |
| `Timestamp_tz(9)`                | `TimestampType Nanosecond withUtc`       |
| `Interval_day`                   | `Duration(Microsecond)`                  |
| `List`                           | `List`                                   |
| `Struct`                         | `Struct`                                 |
| `Map`                            | `Map`                                    |
| `Union`                          | `Union(Sparse)`, rejected by Lance       |
| `Interval_year`                  | `Interval(YearMonth)`, rejected by Lance |
| `Null`                           | `Null`                                   |
| `External(arrow_field_json_str)` | Any Arrow field                          |

Two rows convert cleanly to Arrow but are rejected at the dataset, because Lance defines no interval and no union type. `Interval_day` is fine, since it maps to an Arrow `Duration`, which Lance does have.

Lance also has types with no Gravitino equivalent, including unsigned integers, `Float16`, `LargeUtf8`, `LargeBinary`, and dictionaries. Reach them with [External Types](#external-types).

Vector columns need care. Gravitino `List` produces an Arrow `List`, which is variable length, while Lance stores embeddings as a fixed-size list, written `fixed_size_list:float:128` in its own type system. Declare one with `External` and a `fixedsizelist` type whose `listSize` is the embedding dimension. See [External Types](#external-types) for the form.

## External Types

For Arrow types with no Gravitino equivalent, use `External(arrow_field_json_str)`, which takes a JSON string form of an Arrow [Field](https://github.com/apache/arrow-java/blob/main/vector/src/main/java/org/apache/arrow/vector/types/pojo/Field.java). The `name` and `nullable` attributes have to match the column, and `children` is empty for primitive types and holds child field definitions for complex ones.

| Arrow Type        | External Type Definition                                                                                                                                                                                                                       |
|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Large Utf8`      | `External("{\"name\":\"col\",\"nullable\":true,\"type\":{\"name\":\"largeutf8\"},\"children\":[]}")`                                                                                                                                           |
| `Large Binary`    | `External("{\"name\":\"col\",\"nullable\":true,\"type\":{\"name\":\"largebinary\"},\"children\":[]}")`                                                                                                                                         |
| `Large List`      | `External("{\"name\":\"col\",\"nullable\":true,\"type\":{\"name\":\"largelist\"},\"children\":[{\"name\":\"element\",\"nullable\":true,\"type\":{\"name\":\"int\",\"bitWidth\":32,\"isSigned\":true},\"children\":[]}]}")`                     |
| `Fixed-Size List` | `External("{\"name\":\"col\",\"nullable\":true,\"type\":{\"name\":\"fixedsizelist\",\"listSize\":10},\"children\":[{\"name\":\"element\",\"nullable\":true,\"type\":{\"name\":\"int\",\"bitWidth\":32,\"isSigned\":true},\"children\":[]}]}")` |

## Examples

### Lance REST API

Endpoints and protocol details are on the [Lance REST Service](./lance-rest-service.md#lance-rest-api) page.

**Creating a table.** The catalog and schema have to exist first, as the two levels of Lance namespace above the table. Creating them is covered in the [Lance REST Service](./lance-rest-service.md#quick-start) Quick Start.

The Arrow IPC body makes this the one call that is awkward to issue by hand. Write the schema to a file first:

```python
import pyarrow as pa

schema = pa.schema([("id", pa.int32()), ("score", pa.float32())])
with pa.ipc.new_stream("schema.arrows", schema):
    pass
```

Then post it, naming the table with all three levels:

```shell
LANCE_URL=http://localhost:9101/lance
TABLE_ID={catalog_name}%24{schema_name}%24{table_name}

curl -X POST "${LANCE_URL}/v1/table/${TABLE_ID}/create?mode=create" \
  -H 'Content-Type: application/vnd.apache.arrow.stream' \
  -H 'x-lance-table-location: s3://{bucket}/{schema_name}/{table_name}.lance' \
  --data-binary @schema.arrows
```

The response carries the resolved location and the `storageOptions` the client needs to read the dataset directly.

**Registering an existing table.** Registration takes JSON and points at a dataset that already exists. Gravitino sets `lance.register` for you.

```shell
curl -X POST "${LANCE_URL}/v1/table/${TABLE_ID}/register" \
  -H 'Content-Type: application/json' \
  -d '{"location": "s3://{bucket}/{schema_name}/{table_name}.lance",
       "mode": "create"}'
```

### Gravitino REST API

**Creating a table.** A `lakehouse-generic` catalog and a schema have to exist first. See [Creating a Catalog](./lakehouse-generic-catalog.md#for-lance-tables). `format` is required and selects the Lance implementation.

```shell
GRAVITINO_URL=http://localhost:8090
CATALOG=${GRAVITINO_URL}/api/metalakes/{metalake_name}/catalogs/{catalog_name}
TABLES=${CATALOG}/schemas/{schema_name}/tables

curl -X POST "${TABLES}" \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "{table_name}",
  "columns": [
    {"name": "id", "type": "integer", "nullable": false},
    {"name": "score", "type": "float"}
  ],
  "properties": {"format": "lance"}
}'
```

The table location is derived from the catalog or schema location. Lance clients reading the table receive the catalog's resolved `lance.storage.*` values as storage options, so they need no credentials of their own.

**Registering an existing table.** Registration links a dataset that already exists, without moving or copying data. Pass an empty column list and Gravitino reads the schema from the dataset.

```shell
curl -X POST "${TABLES}" \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "{table_name}",
  "columns": [],
  "properties": {
    "format": "lance",
    "lance.register": "true",
    "location": "s3://{bucket}/{prefix}/{table_name}.lance"
  }
}'
```

Creating and registering differ in what they do to storage. Create initializes a new dataset and needs the column definitions. Register touches no data and takes the schema from what is already there.

Other table operations follow the standard relational catalog patterns described in [Table Operations](./manage-relational-metadata-using-gravitino.md#table-operations).

## Troubleshooting

| Error                                                     | Cause                                                                                      |
|-----------------------------------------------------------|--------------------------------------------------------------------------------------------|
| `Catalog is not a lakehouse catalog`                      | A catalog of that name exists on another provider. Returned as a 404 on the Lance REST API |
| `Expected at most 2-level and at least 1-level namespace` | A namespace identifier with three or more levels                                           |
| `Expected at 3-level namespace but got`                   | A table identifier that is not `catalog$schema$table`                                      |
| `'location' property is neither set in table properties`  | No location at the table, schema, or catalog level                                         |
| `Unsupported Gravitino type`                              | A column type with no Arrow mapping. Use `External`                                        |
| `Expected precision to be one of 0, 3, 6, 9 but got`      | A timestamp precision Arrow does not have                                                  |
| `Only RENAME alteration is supported currently`           | An `alter_columns` request other than a rename                                             |
| `Unsupported changes to lance table`                      | An alter other than drop column, rename column, or add index                               |
| `EXIST_OK mode is not supported for register operation`   | Register accepts `create` or `overwrite` only                                              |
| `deregisterTable only supports external tables`           | The table was created through the Gravitino REST API without `external=true`               |

## Related Pages

- [Lance REST Service](./lance-rest-service.md) for the Lance-native path, service configuration, and the REST API
- [Lance REST Integration](./lance-rest-integration.md) for `lance-spark` and `lance-ray` versions and examples
- [Lakehouse Generic Catalog](./lakehouse-generic-catalog.md) for the catalog, storage options, and location resolution
- [Delta Lake Tables](./lakehouse-generic-delta-table.md) for the other format the same catalog holds
