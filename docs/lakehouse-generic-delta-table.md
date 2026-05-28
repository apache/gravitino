---
title: "Delta Lake Tables"
slug: "/lakehouse-generic-delta-table"
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

[Delta Lake](https://delta.io) is an open-source storage layer that adds ACID transactions, schema enforcement, and time travel to data lakes on cloud and on-prem object storage. Use the Delta format in a Gravitino Generic Lakehouse catalog when you want to register existing Delta tables for discovery and governance through Gravitino alongside relational, dedicated-lakehouse, and fileset catalogs. Table reads, writes, and schema changes continue to go through Delta Lake's own APIs (Apache Spark with the Delta Spark connector, delta-rs, and so on).

The guide assumes a Generic Lakehouse catalog already exists. See the [Generic Lakehouse catalog doc](./lakehouse-generic-catalog.md) for catalog creation, the catalog/schema/table location resolution model, and the corpus-level Requirements and Limitations.

## Requirements and Limitations

- **Catalog prerequisite.** Delta tables live inside a Generic Lakehouse catalog. See the [Generic Lakehouse catalog doc](./lakehouse-generic-catalog.md) for catalog creation and the catalog/schema/table location resolution rules.
- **External tables only.** Gravitino stores metadata (schema, location, properties) for Delta tables whose physical data lives elsewhere. Managed Delta tables are not supported. Set `external=true` on every Delta table created through Gravitino.
- **Drop and purge semantics.** Dropping an external Delta table from Gravitino removes the metadata entry only; the underlying Delta data is preserved at its location. The `purgeTable` operation is not supported for external Delta tables.
- **No `alterTable`.** Schema changes go through Delta Lake APIs (Spark with the Delta Spark connector, delta-rs, and so on). After modifying the table through Delta APIs, drop and re-register the table in Gravitino with the updated schema. See [Modify Delta Tables with Spark](#modify-delta-tables-with-spark) for a worked example.
- **Identity partitions only.** Partition transforms are limited to identity. Partition metadata is stored in Gravitino only and is not validated against the Delta transaction log; you are responsible for keeping the metadata consistent with the actual Delta table's partitioning. See [Partitioned Delta Tables](#partitioned-delta-tables).
- **No sort orders, distributions, or indexes.** These are not accepted in `CREATE TABLE`.
- **Time travel.** Available through Delta Lake APIs directly. Not exposed through Gravitino.
- **Schema model.** Delta Lake uses Apache Spark data types. Gravitino maps its types to Delta/Spark types as described in [Data Type Mappings](#data-type-mappings).
- **Schema not validated at registration.** Gravitino stores the column schema you provide as metadata but does not check it against the Delta table's actual schema in the `_delta_log`. Provide a schema that matches the actual Delta table to avoid inconsistencies.

## Quick Start

Register an existing external Delta table inside an existing Generic Lakehouse catalog and confirm it is reachable through Gravitino. The walkthrough assumes a Gravitino server at `http://localhost:8090`, a metalake named `test`, a Generic Lakehouse catalog named `lakehouse_catalog`, and a schema named `sales` under that catalog. The example registers a Delta table that already exists at `s3://my-bucket/delta-tables/customer_orders`; substitute a path to an existing Delta table you control.

### Register the Delta Table

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "customer_orders",
    "comment": "Customer orders Delta table",
    "columns": [
      {
        "name": "order_id",
        "type": "long",
        "comment": "Order identifier",
        "nullable": false
      }
    ],
    "properties": {
      "format": "delta",
      "external": "true",
      "location": "s3://my-bucket/delta-tables/customer_orders"
    }
  }' \
  http://localhost:8090/api/metalakes/test/catalogs/lakehouse_catalog/schemas/sales/tables
```

The minimum-viable example shown here uses a single column. Provide the full column list matching the actual Delta table for an accurate registration; see [Register an External Delta Table](#register-an-external-delta-table) below for the fuller example with multiple columns and both shell and Java tabs.

### Verify the Table

```bash
# List tables in the schema. customer_orders should appear.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/lakehouse_catalog/schemas/sales/tables" | jq

# Load the table directly and inspect its properties and columns.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/lakehouse_catalog/schemas/sales/tables/customer_orders" | jq
```

**Success check:** the table-list response includes `customer_orders`, and the load-table response shows `"format":"delta"`, `"external":"true"`, the configured S3 location, and the column you provided. The Delta table files at the location are untouched; Gravitino has registered the metadata only.

## Table Management

### Supported Operations

For Delta tables in a Generic Lakehouse catalog, the following table operations are supported:

| Operation | Support                                |
|-----------|----------------------------------------|
| List      | Supported                              |
| Load      | Supported                              |
| Create    | Supported (registers an external table)|
| Drop      | Supported (metadata only; data preserved)|
| Alter     | Not supported                          |
| Purge     | Not supported                          |

For schema changes on a Delta table, use Delta Lake APIs (Spark with the Delta Spark connector, delta-rs, and so on); see [Modify Delta Tables with Spark](#modify-delta-tables-with-spark). For the full list of unsupported features (managed tables, non-identity partition transforms, sort orders, distributions, indexes, time travel), see [Requirements and Limitations](#requirements-and-limitations) above.

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

- Removes the table from Gravitino's metadata.
- Preserves the Delta table data at its location.
- Allows re-registering the same table later.

The Delta table can still be accessed directly through Delta Lake APIs, Spark, or other tools.
:::

## Work with Delta Tables

### Modify Delta Tables with Spark

To modify a Delta table's schema, use Apache Spark with the Delta Spark connector or another Delta Lake tool. The following example adds a `status` column to an existing Delta table:

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

Register a Delta table with identity partition metadata so downstream consumers can see the partitioning scheme through Gravitino. The partition metadata is stored in Gravitino only and must match the actual partitioning defined in the Delta transaction log.

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
Non-identity transforms (bucket, truncate, year, month, and so on) are rejected at create time. See [Requirements and Limitations](#requirements-and-limitations) for the broader partition-metadata constraints.
:::

## Advanced Topics

### Troubleshooting

#### Common Issues

**`Gravitino only supports creating external Delta tables`.** Set `"external": "true"` in the table properties.

**`Property 'location' is required for external Delta tables`.** Specify the `location` property pointing at your Delta table directory.

**`ALTER TABLE operations are not supported`.** Use Delta Lake APIs (Spark, Delta-rs, and so on) to modify the table, then optionally drop and re-register it in Gravitino with the updated schema.

**`Purge operation is not supported for external Delta tables`.** Use `dropTable()` to remove the metadata only. To delete the data, manually remove the files from the storage location.

## Related

- [Generic lakehouse catalog](./lakehouse-generic-catalog.md)
- [Table operations](./manage-relational-metadata-using-gravitino.md#table-operations)
- [Delta Lake documentation](https://docs.delta.io/)
- [Delta Lake on GitHub](https://github.com/delta-io/delta)
