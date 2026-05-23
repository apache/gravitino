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

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


## Overview

This document describes how to use Apache Gravitino to manage a generic lakehouse catalog with Delta Lake as the underlying table format. Gravitino supports registering and managing metadata for external Delta tables.

:::info
Gravitino supports **external Delta tables only**: it stores metadata
(schema, location, properties) for tables whose physical data lives
elsewhere. Dropping such a table from Gravitino removes the metadata
entry but leaves the underlying Delta data untouched.
:::

## Table Management

### Supported Operations

For Delta tables in a generic lakehouse catalog, the following operations are supported:

| Operation | Support                                        |
|-----------|------------------------------------------------|
| List      | ✅ Full                                        |
| Load      | ✅ Full                                        |
| Alter     | ❌ Not supported (use Delta Lake APIs directly) |
| Create    | ✅ Register external tables only               |
| Drop      | ✅ Metadata only (data preserved)              |
| Purge     | ❌ Not supported for external tables           |

:::note
Feature limitations:

- Only external Delta tables are supported. Set `external=true` when creating one.
- Alter operations are not supported. Modify tables through Delta Lake APIs or Spark, then update the Gravitino metadata if needed.
- Purge is not applicable to external tables. Use drop to remove the metadata only.
- Only identity partitions are supported, and they are stored as metadata only. You are responsible for keeping them consistent with the actual Delta table.
- Sort orders, distributions, and indexes are not supported in `CREATE TABLE`.
:::

### Data Type Mappings

Delta Lake uses Apache Spark data types. The following table shows type mappings between Gravitino and Delta/Spark:

| Gravitino Type      | Delta/Spark Type       | Notes                           |
|---------------------|------------------------|---------------------------------|
| `Boolean`           | `BooleanType`          |                                 |
| `Byte`              | `ByteType`             |                                 |
| `Short`             | `ShortType`            |                                 |
| `Integer`           | `IntegerType`          |                                 |
| `Long`              | `LongType`             |                                 |
| `Float`             | `FloatType`            |                                 |
| `Double`            | `DoubleType`           |                                 |
| `Decimal(p, s)`     | `DecimalType(p, s)`    |                                 |
| `String`            | `StringType`           |                                 |
| `Binary`            | `BinaryType`           |                                 |
| `Date`              | `DateType`             |                                 |
| `Timestamp`         | `TimestampNTZType`     | No timezone, Spark 3.4+         |
| `Timestamp_tz`      | `TimestampType`        | With timezone                   |
| `List`              | `ArrayType`            |                                 |
| `Map`               | `MapType`              |                                 |
| `Struct`            | `StructType`           |                                 |

### Table Properties

Required and optional properties for Delta tables in a generic lakehouse catalog:

| Property   | Description                                                                                                                                                                                                            | Default | Required | Since Version |
|------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|----------|---------------|
| `format`   | Table format: must be `delta`                                                                                                                                                                                          | (none)  | Yes      | 1.2.0         |
| `location` | Storage path for the Delta table. Must point to a directory containing Delta Lake metadata (_delta_log). Supports file://, s3://, hdfs://, abfs://, gs://, and other Hadoop-compatible file systems.                  | (none)  | Yes      | 1.2.0         |
| `external` | Must be `true` for Delta tables. Indicates that Gravitino manages metadata only <br/>and will not delete physical data when the table is dropped.                                                                           | (none)  | Yes      | 1.2.0         |

The location must be specified at the table level for an external Delta table. See [Location resolution](./lakehouse-generic-catalog.md#key-property-location).

### Table Operations

Table operations follow standard relational catalog patterns with Delta-specific considerations. See [Table operations](./manage-relational-metadata-using-gravitino.md#table-operations) for comprehensive documentation.

The following sections provide examples and important details for working with Delta tables.

#### Register an External Delta Table

Register an existing Delta table in Gravitino without moving or modifying the underlying data:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "customer_orders",
  "comment": "Customer orders Delta table",
  "columns": [
    {
      "name": "order_id",
      "type": "long",
      "comment": "Order identifier",
      "nullable": false
    },
    {
      "name": "customer_id",
      "type": "long",
      "comment": "Customer identifier",
      "nullable": false
    },
    {
      "name": "order_date",
      "type": "date",
      "comment": "Order date",
      "nullable": false
    },
    {
      "name": "total_amount",
      "type": "decimal(10,2)",
      "comment": "Total order amount",
      "nullable": true
    }
  ],
  "properties": {
    "format": "delta",
    "external": "true",
    "location": "s3://my-bucket/delta-tables/customer_orders"
  }
}' http://localhost:8090/api/metalakes/test/catalogs/generic_lakehouse_delta_catalog/schemas/sales/tables
```

</TabItem>
<TabItem value="java" label="Java">

```java
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import com.google.common.collect.ImmutableMap;

Catalog catalog = gravitinoClient.loadCatalog("generic_lakehouse_delta_catalog");
TableCatalog tableCatalog = catalog.asTableCatalog();

Map<String, String> tableProperties = ImmutableMap.<String, String>builder()
    .put("format", "delta")
    .put("external", "true")
    .put("location", "s3://my-bucket/delta-tables/customer_orders")
    .build();

tableCatalog.createTable(
    NameIdentifier.of("sales", "customer_orders"),
    new Column[] {
        Column.of("order_id", Types.LongType.get(), "Order identifier", false, false, null),
        Column.of("customer_id", Types.LongType.get(), "Customer identifier", false, false, null),
        Column.of("order_date", Types.DateType.get(), "Order date", false, false, null),
        Column.of("total_amount", Types.DecimalType.of(10, 2), "Total order amount", true, false, null)
    },
    "Customer orders Delta table",
    tableProperties,
    null,  // partitions (optional, identity only)
    null,  // distributions (not supported)
    null,  // sortOrders (not supported)
    null   // indexes (not supported)
);
```

</TabItem>
</Tabs>

:::important
When registering a Delta table in Gravitino, provide the schema (columns) in the `CREATE TABLE` request. Gravitino stores this schema as metadata but does not validate it against the Delta table's actual schema. Make sure the schema you provide matches the actual Delta table schema to avoid inconsistencies.
:::

#### Load a Delta Table

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/test/catalogs/generic_lakehouse_delta_catalog/schemas/sales/tables/customer_orders
```

</TabItem>
<TabItem value="java" label="Java">

```java
Table table = tableCatalog.loadTable(
    NameIdentifier.of("sales", "customer_orders")
);

System.out.println("Table location: " + table.properties().get("location"));
System.out.println("Columns: " + Arrays.toString(table.columns()));
```

</TabItem>
</Tabs>

#### Drop a Delta Table

Dropping a Delta table from Gravitino removes only the metadata entry. The physical Delta table data remains intact.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/test/catalogs/generic_lakehouse_delta_catalog/schemas/sales/tables/customer_orders
```

</TabItem>
<TabItem value="java" label="Java">

```java
boolean dropped = tableCatalog.dropTable(
    NameIdentifier.of("sales", "customer_orders")
);
// The Delta table files at the location are NOT deleted
```

</TabItem>
</Tabs>

:::tip
Since Delta tables are external, dropping them from Gravitino:

- ✅ Removes the table from Gravitino's metadata.
- ✅ Preserves the Delta table data at its location.
- ✅ Allows re-registering the same table later.

The Delta table can still be accessed directly through Delta Lake APIs, Spark, or other tools.
:::

## Work with Delta Tables

### Modify Delta Tables with Spark

Since Gravitino does not support ALTER operations for Delta tables, use Apache Spark or other Delta Lake tools to modify table structure:

```java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import io.delta.tables.DeltaTable;
import static org.apache.spark.sql.functions.lit;

// Create Spark session with Delta Lake support
SparkSession spark = SparkSession.builder()
    .appName("Delta Table Modification")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate();

// Read the table location from Gravitino
String tableLocation = "s3://my-bucket/delta-tables/customer_orders";

// Add a new column using Delta Lake
DeltaTable deltaTable = DeltaTable.forPath(spark, tableLocation);
Dataset<Row> df = deltaTable.toDF()
    .withColumn("status", lit("pending"));

df.write()
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(tableLocation);
```

After modifying the Delta table, you can:

1. Drop the table from Gravitino
2. Re-register it with the updated schema

### Read Delta Tables via Gravitino

Once registered in Gravitino, you can query Delta table metadata and use the location to read data:

```java
// Load table metadata from Gravitino
Table table = tableCatalog.loadTable(NameIdentifier.of("sales", "customer_orders"));
String location = table.properties().get("location");

// Use the location to read the Delta table with Spark
Dataset<Row> df = spark.read()
    .format("delta")
    .load(location);

df.show();
```

### Partitioned Delta Tables

Delta Lake supports partitioning, and Gravitino can store identity partition metadata for external Delta tables. The partition information is metadata-only and must match the actual Delta table's partitioning scheme defined in the Delta transaction log.

```java
// Register a partitioned Delta table
Map<String, String> properties = ImmutableMap.<String, String>builder()
    .put("format", "delta")
    .put("external", "true")
    .put("location", "s3://my-bucket/delta-tables/sales_partitioned")
    .build();

// Specify identity partitions (metadata only)
Transform[] partitions = new Transform[] {
    Transforms.identity("year"),
    Transforms.identity("month")
};

tableCatalog.createTable(
    NameIdentifier.of("sales", "sales_partitioned"),
    columns,
    "Partitioned sales data",
    properties,
    partitions,  // Identity partitions supported
    null,
    null,
    null);
```

:::note
Partition information in Gravitino is **metadata only**:
- Only **identity transforms** are supported (e.g., `Transforms.identity("column")`)
- Non-identity transforms (bucket, truncate, year, month, etc.) will be rejected
- The actual partitioning is managed by Delta Lake in the _delta_log
- **User responsibility**: Ensure the partition metadata you provide matches the actual Delta table's partitioning
- Gravitino does not validate partition metadata against the Delta transaction log
:::

## Advanced Topics

### Troubleshooting

#### Common Issues

**`Gravitino only supports creating external Delta tables`.** Set `"external": "true"` in the table properties.

**`Property 'location' is required for external Delta tables`.** Specify the `location` property pointing at your Delta table directory.

**`ALTER TABLE operations are not supported`.** Use Delta Lake APIs (Spark, Delta-rs, and so on) to modify the table, then optionally drop and re-register it in Gravitino with the updated schema.

**`Purge operation is not supported for external Delta tables`.** Use `dropTable()` to remove the metadata only. To delete the data, manually remove the files from the storage location.

## Limitations and Future Work

### Limitations

- **Managed tables.** Not supported; only external tables are available.
- **`ALTER` operations.** Schema changes go through Delta Lake APIs, not Gravitino.
- **Partitioning.** Only identity partitions are supported, and they are stored as metadata only (not validated against the Delta log).
- **Indexes.** Not supported in `CREATE TABLE`.
- **Time travel.** Available through Delta Lake APIs directly; not exposed through Gravitino.

### Planned Enhancements

Future versions may include:

- Support for managed Delta tables (requires Delta Lake 4.0+ `CommitCoordinator`).
- Schema-evolution tracking.
- Integration with Delta Lake time-travel features.
- Enhanced metadata synchronization.

## Related

- [Generic lakehouse catalog](./lakehouse-generic-catalog.md)
- [Table operations](./manage-relational-metadata-using-gravitino.md#table-operations)
- [Delta Lake documentation](https://docs.delta.io/)
- [Delta Lake on GitHub](https://github.com/delta-io/delta)
