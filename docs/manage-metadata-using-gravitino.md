---
title: "Manage metadata using Gravitino"
slug: /manage-metadata-using-gravitino
date: 2023-12-10
keyword: Gravitino metadata manage
license: Copyright 2023 Datastrato Pvt Ltd. This software is licensed under the Apache License version 2.
---

This page introduces how to manage metadata by Gravitino. Through Gravitino, you can create, edit, and delete metadata
like metalakes, catalogs, schemas, and tables. The following contents are included in this page:

In this document, Gravitino uses Apache Hive catalog as an example to show how to manage metadata by Gravitino. Other catalogs are similar to Hive catalog,
but they may have some differences, especially in catalog property, table property and column type. For more details, please refer to the related doc.

- [**Apache Hive**](./apache-hive-catalog)
- [**MySQL**](./jdbc-postgresql-catalog)
- [**PostgreSQL**](./jdbc-mysql-catalog)
- [**Apache Iceberg**](./lakehouse-iceberg-catalog)


Assuming Gravitino has just started, and the host and port is `http://localhost:8090`.

## Metalake operations

### Create a metalake

You can create a metalake by sending a `POST` request to the `/api/metalakes` endpoint or just use the Gravitino Java client.
The following is an example of creating a metalake:

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{"name":"metalake","comment":"comment","properties":{}}' \
http://localhost:8090/api/metalakes
```

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://127.0.0.1:8090")
    .build();
GravitinoMetaLake newMetalake = gravitinoClient.createMetalake(
    NameIdentifier.of("metalake"),
    "This is a new metalake",
    new HashMap<>());
  // ...
```

### Load a metalake

You can create a metalake by sending a `GET` request to the `/api/metalakes/{metalake_name}` endpoint or just use the Gravitino Java client. The following is an example of loading a metalake:

```bash
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json"  http://localhost:8090/api/metalakes/metalake
```

```java
// ...
GravitinoMetaLake loaded = gravitinoClient.loadMetalake(
    NameIdentifier.of("metalake"));
// ...
```

### Alter a metalake

You can modify a metalake by sending a `PUT` request to the `/api/metalakes/{metalake_name}` endpoint or just use the Gravitino Java client. The following is an example of altering a metalake:

```bash
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "updates": [
    {
      "@type": "rename",
      "newName": "metalake"
    },
    {
      "@type": "setProperty",
      "property": "key2",
      "value": "value2"
    }
  ]
}' http://localhost:8090/api/metalakes/new_metalake
```

```java
// ...
GravitinoMetaLake renamed = gravitinoClient.alterMetalake(
    NameIdentifier.of("new_metalake"),
    MetalakeChange.rename("new_metalake_renamed")
);
// ...
```

Currently, Gravitino supports the following changes to a metalake:

| Supported modification | Json                                                         | Java                                            |
|------------------------|--------------------------------------------------------------|-------------------------------------------------|
| Rename metalake        | `{"@type":"rename","newName":"metalake_renamed"}`            | `MetalakeChange.rename("metalake_renamed")`     |
| Update comment         | `{"@type":"updateComment","newComment":"new_comment"}`       | `MetalakeChange.updateComment("new_comment")`   |
| Set a property         | `{"@type":"setProperty","property":"key1","value":"value1"}` | `MetalakeChange.setProperty("key1", "value1")`  |
| Remove a property      | `{"@type":"removeProperty","property":"key1"}`               | `MetalakeChange.removeProperty("key1")`         |


### Drop a metalake

You can remove a metalake by sending a `DELETE` request to the `/api/metalakes/{metalake_name}` endpoint or just use the Gravitino Java client. The following is an example of dropping a metalake:

```bash
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" http://localhost:8090/api/metalakes/metalake
````

```java
// ...
boolean success = gravitinoClient.dropMetalake(
    NameIdentifier.of("metalake")
);
// ...
```

:::note
Drop a metalake only removes metadata about the metalake and catalogs, schemas, tables under the metalake in Gravitino, It doesn't remove the real schema and table data in Apache Hive.
:::

### List all metalakes

You can list metalakes by sending a `GET` request to the `/api/metalakes` endpoint or just use the Gravitino Java client. The following is an example of listing all metalake name:

```bash
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json"  http://localhost:8090/api/metalakes
```

```java
// ...
GravitinoMetaLake[] allMetalakes = gravitinoClient.listMetalakes();
// ...
```

## Catalogs operations

### Create a catalog

:::tip
Users should create a metalake before creating a catalog.

The code below is an example of creating a Hive catalog. For other catalogs, the code is similar, but the catalog type, provider and properties may be different. For more details, please refer to the related doc.
:::

You can create a catalog by sending a `POST` request to the `/api/metalakes/{metalake_name}/catalogs` endpoint or just use the Gravitino Java client. The following is an example of creating a catalog:

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "catalog",
  "type": "RELATIONAL",
  "comment": "comment",
  "provider": "hive",
  "properties": {
    "metastore.uris": "thrift://localhost:9083"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs
```

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://127.0.0.1:8090")
    .build();

// Assuming you have just created a metalake named `metalake`
GravitinoMetaLake gravitinoMetaLake =
    gravitinoClient.loadMetalake(NameIdentifier.of("metalake"));

Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
        // You should replace the following with your own hive metastore uris that Gravitino can access
        .put("metastore.uris", "thrift://localhost:9083")
        .build();

Catalog catalog = gravitinoMetaLake.createCatalog(
    NameIdentifier.of("metalake", "catalog"),
    Type.RELATIONAL,
    "hive", // provider, We support hive, jdbc-mysql, jdbc-postgresql, lakehouse-iceberg, etc.
    "This is a hive catalog",
    hiveProperties); // Please change the properties according to the value of provider.
// ...
```

Currently, Gravitino supports the following catalog providers:

| Catalog provider    | Catalog property                                                            | 
|---------------------|-----------------------------------------------------------------------------|
| `hive`              | [Hive catalog property](./apache-hive-catalog#catalog-properties)           | 
| `lakehouse-iceberg` | [Iceberg catalog property](./lakehouse-iceberg-catalog#catalog-properties)  |                                    
| `jdbc-mysql`        | [MySQL catalog property](./jdbc-mysql-catalog#catalog-properties)           |                            
| `jdbc-postgresql`   | [PostgreSQL catalog property](./jdbc-postgresql-catalog#catalog-properties) |          

### Load a catalog

You can load a catalog by sending a `GET` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}` endpoint or just use the Gravitino Java client. The following is an example of loading a catalog:

```bash
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" http://localhost:8090/api/metalakes/metalake/catalogs/catalog
```

```java
// ...
// Assuming you have just created a metalake named `metalake`
GravitinoMetaLake gravitinoMetaLake =
    gravitinoClient.loadMetalake(NameIdentifier.of("metalake"));

Catalog catalog = gravitinoMetaLake.loadCatalog(NameIdentifier.of("metalake", "catalog"));
// ...
```

### Alter a catalog

You can modify a catalog by sending a `PUT` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}` endpoint or just use the Gravitino Java client. The following is an example of altering a catalog:

```bash
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "updates": [
    {
      "@type": "rename",
      "newName": "alter_catalog"
    },
    {
      "@type": "setProperty",
      "property": "key3",
      "value": "value3"
    }
  ]
}' http://localhost:8090/api/metalakes/metalake/catalogs/catalog
```

```java
// ...
// Assuming you have just created a metalake named `metalake`
GravitinoMetaLake gravitinoMetaLake =
    gravitinoClient.loadMetalake(NameIdentifier.of("metalake"));

Catalog catalog = gravitinoMetaLake.alterCatalog(NameIdentifier.of("metalake", "catalog"),
    CatalogChange.rename("alter_catalog"), CatalogChange.updateComment("new comment"));
// ...
```

Currently, Gravitino supports the following changes to a catalog:

| Supported modification | Json                                                         | Java                                           |
|------------------------|--------------------------------------------------------------|------------------------------------------------|
| Rename metalake        | `{"@type":"rename","newName":"metalake_renamed"}`            | `CatalogChange.rename("catalog_renamed")`      |
| Update comment         | `{"@type":"updateComment","newComment":"new_comment"}`       | `CatalogChange.updateComment("new_comment")`   |
| Set a property         | `{"@type":"setProperty","property":"key1","value":"value1"}` | `CatalogChange.setProperty("key1", "value1")`  |
| Remove a property      | `{"@type":"removeProperty","property":"key1"}`               | `CatalogChange.removeProperty("key1")`         |

### Drop a catalog

You can remove a catalog by sending a `DELETE` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}` endpoint or just use the Gravitino Java client. The following is an example of dropping a catalog:

```bash
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/metalake/catalogs/catalog
```

```java
// ...
// Assuming you have just created a metalake named `metalake`
GravitinoMetaLake gravitinoMetaLake =
    gravitinoClient.loadMetalake(NameIdentifier.of("metalake"));
gravitinoMetaLake.dropCatalog(NameIdentifier.of("metalake", "catalog"));
// ...
```

:::note
Drop a catalog only removes metadata about the catalog and schemas, tables under the catalog in Gravitino, It doesn't remove the real data(table and schema) in Apache Hive.
:::

### List all catalogs in a metalake

You can list all catalogs under a metalake by sending a `GET` request to the `/api/metalakes/{metalake_name}/catalogs` endpoint or just use the Gravitino Java client. The following is an example of listing all catalogs in
a metalake:

```bash
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/metalake/catalogs
```

```java
// ...
// Assuming you have just created a metalake named `metalake`
GravitinoMetaLake gravitinoMetaLake =
    gravitinoClient.loadMetalake(NameIdentifier.of("metalake"));

NameIdentifier[] catalogsIdents = gravitinoMetaLake.listCatalogs(Namespace.ofCatalog("metalake"));
// ...
```

## Schemas operations

:::tip
Users should create a metalake and a catalog before creating a schema.
:::

### Create a schema

You can create a schema by sending a `POST` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas` endpoint or just use the Gravitino Java client. The following is an example of creating a schema:

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "schema",
  "comment": "comment",
  "properties": {
    "key1": "value1"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas
```

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://127.0.0.1:8090")
    .build();

// Assuming you have just created a metalake named `metalake`
GravitinoMetaLake gravitinoMetaLake =
    gravitinoClient.loadMetalake(NameIdentifier.of("metalake"));

// Assuming you have just created a Hive catalog named `catalog`
Catalog catalog = gravitinoMetaLake.loadCatalog(NameIdentifier.of("metalake", "catalog"));

SupportsSchemas supportsSchemas = catalog.asSchemas();

Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    .build();
Schema schema = supportsSchemas.createSchema(
    NameIdentifier.of("metalake", "catalog", "schema"),
    "This is a schema",
    schemaProperties
);
// ...
```

Currently, Gravitino supports the following schema property:

| Catalog provider    | Schema property                                                           | 
|---------------------|---------------------------------------------------------------------------|
| `hive`              | [Hive schema property](./apache-hive-catalog#schema-properties)           | 
| `lakehouse-iceberg` | [Iceberg scheme property](./lakehouse-iceberg-catalog#schema-properties)  |                                    
| `jdbc-mysql`        | [MySQL schema property](./jdbc-mysql-catalog#schema-properties)           |                            
| `jdbc-postgresql`   | [PostgreSQL schema property](./jdbc-postgresql-catalog#schema-properties) |        


### Load a schema

You can create a schema by sending a `GET` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}` endpoint or just use the Gravitino Java client. The following is an example of loading a schema:


```bash
curl -X GET \-H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema
```

```java
// ...
// Assuming you have just created a Hive catalog named `hive_catalog`
Catalog catalog = gravitinoMetaLake.loadCatalog(NameIdentifier.of("metalake", "catalog"));
SupportsSchemas supportsSchemas = catalog.asSchemas();
Schema schema = supportsSchemas.loadSchema(NameIdentifier.of("metalake", "catalog", "schema"));
// ...
```

### Alter a schema

You can change a schema by sending a `PUT` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}` endpoint or just use the Gravitino Java client. The following is an example of modifying a schema:

```bash
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "updates": [
    {
      "@type": "removeProperty",
      "property": "key2"
    }, {
      "@type": "setProperty",
      "property": "key3",
      "value": "value3"
    }
  ]
}' http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema
```

```java
// ...
// Assuming you have just created a Hive catalog named `hive_catalog`
Catalog catalog = gravitinoMetaLake.loadCatalog(NameIdentifier.of("metalake", "hive_catalog"));

SupportsSchemas supportsSchemas = catalog.asSchemas();

Schema schema = supportsSchemas.alterSchema(NameIdentifier.of("metalake", "hive_catalog", "schema"),
    SchemaChange.removeProperty("key1"),
    SchemaChange.setProperty("key2", "value2"));
// ...
```

Currently, Gravitino supports the following changes to a schema:

| Supported modification | Json                                                         | Java                                          |
|------------------------|--------------------------------------------------------------|-----------------------------------------------|
| Set a property         | `{"@type":"setProperty","property":"key1","value":"value1"}` | `SchemaChange.setProperty("key1", "value1")`  |
| Remove a property      | `{"@type":"removeProperty","property":"key1"}`               | `SchemaChange.removeProperty("key1")`         |


### Drop a schema
You can remove a schema by sending a `DELETE` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}` endpoint or just use the Gravitino Java client. The following is an example of dropping a schema:


```bash
// cascade can be true or false
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema?cascade=true
```

```java
// ...
// Assuming you have just created a Hive catalog named `hive_catalog`
Catalog catalog = gravitinoMetaLake.loadCatalog(NameIdentifier.of("metalake", "catalog"));

SupportsSchemas supportsSchemas = catalog.asSchemas();
// cascade can be true or false
supportsSchemas.dropSchema(NameIdentifier.of("metalake", "catalog", "schema"), true);
```

### List all schemas under a catalog

You can alter all schemas under a catalog by sending a `GET` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas` endpoint or just use the Gravitino Java client. The following is an example of list all schema
    in a catalog:

```bash
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas
```

```java
// ...
// Assuming you have just created a Hive catalog named `hive_catalog`
Catalog catalog = gravitinoMetaLake.loadCatalog(NameIdentifier.of("metalake", "catalog"));

SupportsSchemas supportsSchemas = catalog.asSchemas();
NameIdentifier[] schemas = supportsSchemas.listSchemas(Namespace.ofSchema("metalake", "catalog"));
```

## Tables operations

:::tip
Users should create a metalake, a catalog and a schema before creating a table.
:::

### Create a table

You can create a table by sending a `POST` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/tables` endpoint or just use the Gravitino Java client. The following is an example of creating a table:

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "table",
  "columns": [
    {
      "name": "id",
      "type": "integer",
      "nullable": true,
      "comment": "Id of the user"
    },
    {
      "name": "name",
      "type": "varchar(2000)",
      "nullable": true,
      "comment": "Name of the user"
    }
  ],
  "comment": "Create a new Table",
  "properties": {
    "format": "ORC"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema/tables
```

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://127.0.0.1:8090")
    .build();

// Assuming you have just created a metalake named `metalake`
GravitinoMetaLake gravitinoMetaLake =
    gravitinoClient.loadMetalake(NameIdentifier.of("metalake"));

// Assuming you have just created a Hive catalog named `hive_catalog`
Catalog catalog = gravitinoMetaLake.loadCatalog(NameIdentifier.of("metalake", "catalog"));

TableCatalog tableCatalog = catalog.asTableCatalog();

// This is an example of creating a Hive table, you should refer to the related doc to get the
// table properties of other catalogs.
Map<String, String> tablePropertiesMap = ImmutableMap.<String, String>builder()
        .put("format", "ORC")
        // For more table properties, please refer to the related doc.
        .build();
tableCatalog.createTable(
    NameIdentifier.of("metalake", "catalog", "schema", "table"),
    new ColumnDTO[] {
        ColumnDTO.builder()
            .withComment("Id of the user")
            .withName("id")
            .withDataType(Types.IntegerType.get())
            .withNullable(true)
            .build(),
        ColumnDTO.builder()
            .withComment("Name of the user")
            .withName("name")
            .withDataType(Types.VarCharType.of(1000))
            .withNullable(true)
            .build(),
    },
    "Create a new Table",
    tablePropertiesMap
);
```

In order to create a table, you need to provide the following information:

- Table column name and type
- Table property 

#### Gravitino table column type

The following the types that Gravitino supports:

| Type                      | Java                                                                     | Json                                                                                                                                 | Description                                                                                      |
|---------------------------|--------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| Boolean                   | `Types.BooleanType.get()`                                                | `boolean`                                                                                                                            | Boolean type                                                                                     |
| Byte                      | `Types.ByteType.get()`                                                   | `byte`                                                                                                                               | Byte type, indicates a numerical value of 1 byte                                                 |
| Short                     | `Types.ShortType.get()`                                                  | `short`                                                                                                                              | Short type, indicates a numerical value of 2 bytes                                               |
| Integer                   | `Types.IntegerType.get()`                                                | `integer`                                                                                                                            | Integer type, indicates a numerical value of 4 bytes                                             |
| Long                      | `Types.LongType.get()`                                                   | `long`                                                                                                                               | Long type, indicates a numerical value of 8 bytes                                                |
| Float                     | `Types.FloatType.get()`                                                  | `float`                                                                                                                              | Float type, indicates a single-precision floating point number                                   |
| Double                    | `Types.DoubleType.get()`                                                 | `double`                                                                                                                             | Double type, indicates a double-precision floating point number                                  |
| Decimal(precision, scale) | `Types.DecimalType.of(precision, scale)`                                 | `decimal(p, s)`                                                                                                                      | Decimal type, indicates a fixed-precision decimal number                                         |
| String                    | `Types.StringType.get()`                                                 | `string`                                                                                                                             | String type                                                                                      |
| FixedChar(length)         | `Types.FixedCharType.of(length)`                                         | `char(l)`                                                                                                                            | Char type, indicates a fixed-length string                                                       |
| VarChar(length)           | `Types.VarCharType.of(length)`                                           | `varchar(l)`                                                                                                                         | Varchar type, indicates a variable-length string, the length is the maximum length of the string |
| Timestamp                 | `Types.TimestampType.withoutTimeZone()`                                  | `timestamp`                                                                                                                          | Timestamp type, indicates a timestamp without timezone                                           |
| TimestampWithTimezone     | `Types.TimestampType.withTimeZone()`                                     | `timestamp_tz`                                                                                                                       | Timestamp with timezone type, indicates a timestamp with timezone                                |
| Date                      | `Types.DateType.get()`                                                   | `date`                                                                                                                               | Date type                                                                                        |
| Time                      | `Types.TimeType.withoutTimeZone()`                                       | `time`                                                                                                                               | Time type                                                                                        |
| IntervalToYearMonth       | `Types.IntervalYearType.get()`                                           | `interval_year`                                                                                                                      | Interval type, indicates an interval of year and month                                           |
| IntervalToDayTime         | `Types.IntervalDayType.get()`                                            | `interval_day`                                                                                                                       | Interval type, indicates an interval of day and time                                             |
| Fixed(length)             | `Types.FixedType.of(length)`                                             | `fixed(l)`                                                                                                                           | Fixed type, indicates a fixed-length binary array                                                |
| Binary                    | `Types.BinaryType.get()`                                                 | `binary`                                                                                                                             | Binary type, indicates a arbitrary-length binary array                                           |
| List                      | `Types.ListType.of(elementType, elementNullable)`                        | `{"type": "list", "containsNull": JSON Boolean, "elementType": type JSON}`                                                           | List type, indicate a list of elements with the same type                                        |
| Map                       | `Types.MapType.of(keyType, valueType)`                                   | `{"type": "map", "keyType": type JSON, "valueType": type JSON, "valueContainsNull": JSON Boolean}`                                   | Map type, indicate a map of key-value pairs                                                      |
| Struct                    | `Types.StructType.of([Types.StructType.Field.of(name, type, nullable)])` | `{"type": "struct", "fields": [JSON StructField, {"name": string, "type": type JSON, "nullable": JSON Boolean, "comment": string}]}` | Struct type, indicate a struct of fields                                                         |
| Union                     | `Types.UnionType.of([type1, type2, ...])`                                | `{"type": "union", "types": [type JSON, ...]}`                                                                                       | Union type, indicate a union of types     


The related java doc is [here](./api/java/com/datastrato/gravitino/rel/types/Type.html).

#### Table property and type mapping

The following is the table property that Gravitino supports:

| Catalog provider    | Table property                                                          | Type mapping                                                            |
|---------------------|-------------------------------------------------------------------------|-------------------------------------------------------------------------|
| `hive`              | [Hive table property](./apache-hive-catalog#table-properties)           | [Hive type mapping](./apache-hive-catalog#table-column-types)           | 
| `lakehouse-iceberg` | [Iceberg table property](./lakehouse-iceberg-catalog#table-properties)  | [Iceberg type mapping](./lakehouse-iceberg-catalog#table-column-types)  |                                    
| `jdbc-mysql`        | [MySQL table property](./jdbc-mysql-catalog#table-properties)           | [MySQL type mapping](./jdbc-mysql-catalog#table-column-types)           |                            
| `jdbc-postgresql`   | [PostgreSQL table property](./jdbc-postgresql-catalog#table-properties) | [PostgreSQL type mapping](./jdbc-postgresql-catalog#table-column-types) |  


In addition to the basic settings, Gravitino supports the following features:

| Feature             | Description                                                                                  | Java doc                                                                                        |
|---------------------|----------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| Partitioned table   | Equal to `PARTITION BY` in Apache Hive and other engine that support partitioning.           | [Partition](./api/java/com/datastrato/gravitino/dto/rel/partitions/Partitioning.html)           |
| Bucketed table      | Equal to `CLUSTERED BY` in Apache Hive, some engine may use different words to describe it.  | [Distribution](./api/java/datastrato/gravitino/rel/expressions/distributions/Distribution.html) |
| Sorted order table  | Equal to `SORTED BY` in Apache Hive, some engine may use different words to describe it.     | [SortOrder](./api/java/com/datastrato/gravitino/rel/expressions/sorts/SortOrder.html)           |

:::tip
**Those features may not be supported by all catalogs**. Please refer to the related doc for more details.
:::

The following is an example of creating a partitioned, bucketed table and sorted order table:

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "table",
  "columns": [
    {
      "name": "id",
      "type": "integer",
      "nullable": true,
      "comment": "Id of the user"
    },
    {
      "name": "name",
      "type": "varchar(2000)",
      "nullable": true,
      "comment": "Name of the user"
    },
    {
      "name": "age",
      "type": "short",
      "nullable": true,
      "comment": "Age of the user"
    },
    {
      "name": "score",
      "type": "double",
      "nullable": true,
      "comment": "Score of the user"
    }
  ],
  "comment": "Create a new Table",
  "properties": {
    "format": "ORC"
  },
  "partitioning": [
    {
      "strategy": "identity",
      "fieldName": ["score"]
    }
  ],
  "distribution": {
    "strategy": "hash",
    "number": 4,
    "funcArgs": [
      {
        "type": "field",
        "fieldName": ["score"] 
      } 
    ]
  },
  "sortOrders": [
    {
      "direction": "asc",
      "nullOrder": "NULLS_LAST",
      "sortTerm":  {
        "type": "field",
        "fieldName": ["name"] 
      } 
    }
  ] 
}' http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema/tables
```

```java
tableCatalog.createTable(
    NameIdentifier.of("metalake", "hive_catalog", "schema", "table"),
    new ColumnDTO[] {
    ColumnDTO.builder()
    .withComment("Id of the user")
    .withName("id")
    .withDataType(Types.IntegerType.get())
    .withNullable(true)
    .build(),
    ColumnDTO.builder()
    .withComment("Name of the user")
    .withName("name")
    .withDataType(Types.VarCharType.of(1000))
    .withNullable(true)
    .build(),
    ColumnDTO.builder()
    .withComment("Age of the user")
    .withName("age")
    .withDataType(Types.ShortType.get())
    .withNullable(true)
    .build(),

    ColumnDTO.builder()
    .withComment("Score of the user")
    .withName("score")
    .withDataType(Types.DoubleType.get())
    .withNullable(true)
    .build(),
    },
    "Create a new Table",
    tablePropertiesMap,
    new Transform[] {
    // Partition by id
    Transforms.identity("score")
    },
    // CLUSTERED BY id
    new DistributionDTO.Builder()
    .withStrategy(Strategy.HASH)
    .withNumber(4)
    .withArgs(FieldReferenceDTO.of("id"))
    .build(),
    // SORTED BY name asc
    new SortOrderDTO[] {
    new SortOrderDTO.Builder()
    .withDirection(SortDirection.ASCENDING)
    .withNullOrder(NullOrdering.NULLS_LAST)
    .withSortTerm(FieldReferenceDTO.of("name"))
    .build()
    }
    );
```

:::note
The code above is an example of creating a Hive table. For other catalogs, the code is similar, but the supported column type, table properties may be different. For more details, please refer to the related doc.
:::

### Load a table

You can load a table by sending a `GET` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}` endpoint or just use the Gravitino Java client. The following is an example of loading a table:

```bash
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json"  \
http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema/tables/table

```

```java
// ...
// Assuming you have just created a Hive catalog named `catalog`
Catalog catalog = gravitinoMetaLake.loadCatalog(NameIdentifier.of("metalake", "catalog"));

TableCatalog tableCatalog = catalog.asTableCatalog();
tableCatalog.loadTable(NameIdentifier.of("metalake", "hive_catalog", "schema", "table"));
// ...
```

### Alter a table

You can modify a table by sending a `PUT` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}` endpoint or just use the Gravitino Java client. The following is an example of modifying a table:

```bash
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "updates": [
    {
      "@type": "removeProperty",
      "property": "key2"
    }, {
      "@type": "setProperty",
      "property": "key3",
      "value": "value3"
    }
}' http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema/tables/table
```

```java
// ...
// Assuming you have just created a Hive catalog named `catalog`
Catalog catalog = gravitinoMetaLake.loadCatalog(NameIdentifier.of("metalake", "catalog"));

TableCatalog tableCatalog = catalog.asTableCatalog();

Table t = tableCatalog.alterTable(NameIdentifier.of("metalake", "catalog", "schema", "table"),
    TableChange.rename("table_renamed"), TableChange.updateComment("xxx"));
// ...
```

Currently, Gravitino supports the following changes to a table:

| Supported modification             | Json                                                                                                                  | Java                                        |
|------------------------------------|-----------------------------------------------------------------------------------------------------------------------|---------------------------------------------|
| Rename table                       | `{"@type":"rename","newName":"table_renamed"}`                                                                        | `TableChange.rename("table_renamed")`       |
| Update comment                     | `{"@type":"updateComment","newComment":"new_comment"}`                                                                | `TableChange.updateComment("new_comment")`  |
| Set a table property               | `{"@type":"setProperty","property":"key1","value":"value1"}`                                                          | `TableChange.setProperty("key1", "value1")` |
| Remove a table property            | `{"@type":"removeProperty","property":"key1"}`                                                                        | `TableChange.removeProperty("key1")`        |
| Add a column                       | `{"@type":"addColumn","fieldName":["position"],"type":"varchar(20)","comment":"Position of user","position":"FIRST"}` | `TableChange.addColumn(...)`                |
| Delete a column                    | `{"@type":"deleteColumn","fieldName": ["name"], "ifExist": true}`                                                     | `TableChange.deleteColumn(...)`             |
| Rename a column                    | `{"@type":"renameColumn","oldFieldName":["name_old"], "newFieldName":"name_new"}`                                     | `TableChange.renameColumn(...)`             |
| Update the column comment          | `{"@type":"updateColumnComment", "fieldName": ["name"], "newComment": "new comment"}`                                 | `TableChange.updateColumnCommment(...)`     |
| Update the type of a column        | `{"@type":"updateColumnType","fieldName": ["name"], "newType":"varchar(100)"}`                                        | `TableChange.updateColumnType(...)`         |
| Update the nullability of a column | `{"@type":"updateColumnNullability","fieldName": ["name"],"nullable":true}`                                           | `TableChange.updateColumnNullability(...)`  |
| Update the position of a column    | `{"@type":"updateColumnPosition","fieldName": ["name"], "newPosition":"default"}`                                     | `TableChange.updateColumnPosition(...)`     |

### Drop a table

You can remove a table by sending a `DELETE` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}` endpoint or just use the Gravitino Java client. The following is an example of dropping a table:

```bash
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema/tables/table
```

```java
// ...
// Assuming you have just created a Hive catalog named `hive_catalog`
Catalog catalog = gravitinoMetaLake.loadCatalog(NameIdentifier.of("metalake", "catalog"));

TableCatalog tableCatalog = catalog.asTableCatalog();
tableCatalog.dropTable(NameIdentifier.of("metalake", "catalog", "schema", "table"));
// ...
```

### List all tables under a schema

You can list all tables in a schema by sending a `GET` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/tables` endpoint or just use the Gravitino Java client. The following is an example of list all tables in a schema:

```bash
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema/tables 
```

```java
// ...
// Assuming you have just created a Hive catalog named `hive_catalog`
Catalog catalog = gravitinoMetaLake.loadCatalog(NameIdentifier.of("metalake", "catalog"));

TableCatalog tableCatalog = catalog.asTableCatalog();
NameIdentifier[] identifiers =
    tableCatalog.listTables(Namespace.ofTable("metalake", "catalog", "schema"));
// ...
```