---
title: "Delta Lake table support"
slug: /delta-table-support
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

This document describes how to use Apache Gravitino to manage a generic lakehouse catalog using Delta Lake as the underlying table format. Gravitino supports registering and managing metadata for external Delta tables.

:::info Current Support
Gravitino currently supports **external Delta tables only**. This means:
- You can register existing Delta tables in Gravitino
- Gravitino manages metadata only (schema, location, properties)
- The physical Delta table data remains independent
- Dropping tables from Gravitino does not delete the underlying Delta data
:::

## Table Management

### Supported Operations

For Delta tables in a Generic Lakehouse Catalog, the following table summarizes supported operations:

| Operation | Support Status                                 |
|-----------|------------------------------------------------|
| List      | ✅ Full                                         |
| Load      | ✅ Full                                         |
| Alter     | ❌ Not supported (use Delta Lake APIs directly) |
| Create    | ✅ Register external tables only                |
| Drop      | ✅ Metadata only (data preserved)               |
| Purge     | ❌ Not supported for external tables            |

:::note Feature Limitations
- **External Tables Only:** Must set `external=true` when creating Delta tables
- **Alter Operations:** Not supported; modify tables using Delta Lake APIs or Spark, then update Gravitino metadata if needed
- **Purge:** Not applicable for external tables; use DROP to remove metadata only
- **Partitioning:** Identity partitions supported as metadata only; user must ensure consistency with actual Delta table
- **Sort Orders:** Not supported in CREATE TABLE
- **Distributions:** Not supported in CREATE TABLE
- **Indexes:** Not supported in CREATE TABLE
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

Required and optional properties for Delta tables in a Generic Lakehouse Catalog:

| Property   | Description                                                                                                                                                                                                            | Default | Required | Since Version |
|------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|----------|---------------|
| `format`   | Table format: must be `delta`                                                                                                                                                                                          | (none)  | Yes      | 1.2.0         |
| `location` | Storage path for the Delta table. Must point to a directory containing Delta Lake metadata (_delta_log). Supports file://, s3://, hdfs://, abfs://, gs://, and other Hadoop-compatible file systems.                  | (none)  | Yes      | 1.2.0         |
| `external` | Must be `true` for Delta tables. Indicates that Gravitino manages metadata only <br/>and will not delete physical data when the table is dropped.                                                                           | (none)  | Yes      | 1.2.0         |

**Location Requirement:** Must be specified at table level for external Delta table. See [Location Resolution](./lakehouse-generic-catalog.md#key-property-location).

### Table Operations

Table operations follow standard relational catalog patterns with Delta-specific considerations. See [Table Operations](./manage-relational-metadata-using-gravitino.md#table-operations) for comprehensive documentation.

The following sections provide examples and important details for working with Delta tables.

#### Registering an External Delta Table

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

:::important Schema Specification
When registering a Delta table in Gravitino, you must provide the schema (columns) in the CREATE TABLE request. Gravitino stores this schema as metadata but does not validate it against the Delta table's actual schema. 

**Best Practice:** Ensure the schema you provide matches the actual Delta table schema to avoid inconsistencies.
:::

#### Loading a Delta Table

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

#### Dropping a Delta Table

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

:::tip Metadata-Only Drop
Since Delta tables are external, dropping them from Gravitino:
- ✅ Removes the table from Gravitino's metadata
- ✅ Preserves the Delta table data at its location
- ✅ Allows you to re-register the same table later

The Delta table can still be accessed directly via Delta Lake APIs, Spark, or other tools.
:::

## Working with Delta Tables

### Using Spark to Modify Delta Tables

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

### Reading Delta Tables via Gravitino

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

**Issue: "Gravitino only supports creating external Delta tables"**
```
Solution: Ensure you set "external": "true" in the table properties
```

**Issue: "Property 'location' is required for external Delta tables"**
```
Solution: Specify the location property pointing to your Delta table directory
```

**Issue: "ALTER TABLE operations are not supported"**
```
Solution: Use Delta Lake APIs (Spark, Delta-rs, etc.) to modify the table,
then optionally drop and re-register in Gravitino with updated schema
```

**Issue: "Purge operation is not supported for external Delta tables"**
```
Solution: Use dropTable() to remove metadata only. To delete data, 
manually remove files from the storage location
```

## Limitations and Future Work

### Current Limitations

- **Managed Tables**: Not supported; only external tables are available
- **ALTER Operations**: Cannot modify table schema through Gravitino; use Delta Lake APIs
- **Partitioning**: Only identity partitions supported; stored as metadata only (not validated against Delta log)
- **Indexes**: Not supported in CREATE TABLE
- **Time Travel**: Access via Delta Lake APIs directly; not exposed through Gravitino

### Planned Enhancements

Future versions may include:
- Support for managed Delta tables (requires Delta Lake 4.0+ CommitCoordinator)
- Schema evolution tracking
- Integration with Delta Lake time travel features
- Enhanced metadata synchronization

## See Also

- [Generic Lakehouse Catalog](./lakehouse-generic-catalog.md)
- [Table Operations](./manage-relational-metadata-using-gravitino.md#table-operations)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Delta Lake GitHub](https://github.com/delta-io/delta)
