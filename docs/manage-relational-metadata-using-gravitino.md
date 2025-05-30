---
title: "Manage relational metadata using Apache Gravitino"
slug: /manage-relational-metadata-using-gravitino
date: 2023-12-10
keyword: Gravitino relational metadata manage
license: This software is licensed under the Apache License version 2.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This page introduces how to manage relational metadata by Apache Gravitino, relational metadata refers
to relational catalog, schema, table and partitions. Through Gravitino, you can create, edit, and
delete relational metadata via unified REST APIs or Java client.

In this document, Gravitino uses Apache Hive catalog as an example to show how to manage
relational metadata by Gravitino. Other relational catalogs are similar to Hive catalog,
but they may have some differences, especially in catalog property, table property, and column type.
For more details, please refer to the related doc.

- [**Apache Hive**](./apache-hive-catalog.md)
- [**MySQL**](./jdbc-mysql-catalog.md)
- [**PostgreSQL**](./jdbc-postgresql-catalog.md)
- [**Apache Doris**](./jdbc-doris-catalog.md)
- [**OceanBase**](./jdbc-oceanbase-catalog.md)
- [**Apache Iceberg**](./lakehouse-iceberg-catalog.md)
- [**Apache Paimon**](./lakehouse-paimon-catalog.md)
- [**Apache Hudi**](./lakehouse-hudi-catalog.md)

Assuming:

 - Gravitino has just started, and the host and port is [http://localhost:8090](http://localhost:8090).
 - A metalake has been created and [enabled](./manage-metalake-using-gravitino.md#enable-a-metalake).

## Catalog operations

### Create a catalog

:::caution
It is not recommended to use one data source to create multiple catalogs, 
as multiple catalogs operating on the same source may result in unpredictable behavior.
:::

:::tip
The code below is an example of creating a Hive catalog. For other relational catalogs, the code is
similar, but the catalog type, provider, and properties may be different. For more details, please refer to the related doc.

For relational catalog, you must specify the catalog `type` as `RELATIONAL` when creating a catalog.
:::

You can create a catalog by sending a `POST` request to the `/api/metalakes/{metalake_name}/catalogs` endpoint or just use the Gravitino Java client. The following is an example of creating a catalog:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
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

</TabItem>
<TabItem value="java" label="Java">

```java

// Assuming you have just created a metalake named `metalake`
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://127.0.0.1:8090")
    .withMetalake("metalake")
    .build();

Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
        // You should replace the following with your own hive metastore uris that Gravitino can access
        .put("metastore.uris", "thrift://localhost:9083")
        .build();

Catalog catalog = gravitinoClient.createCatalog("catalog",
    Type.RELATIONAL,
    "hive", // provider, We support hive, jdbc-mysql, jdbc-postgresql, lakehouse-iceberg, lakehouse-paimon etc.
    "This is a hive catalog",
    hiveProperties); // Please change the properties according to the value of the provider.
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
# Assuming you have just created a metalake named `metalake`
gravitino_client = GravitinoClient(uri="http://localhost:8090", metalake_name="metalake")
gravitino_client.create_catalog(name="catalog",
                                catalog_type=CatalogType.RELATIONAL,
                                provider="hive",
                                comment="This is a hive catalog",
                                properties={"metastore.uris": "thrift://localhost:9083"})
```

</TabItem>
</Tabs>

Currently, Gravitino supports the following catalog providers:

| Catalog provider    | Catalog property                                                               |
|---------------------|--------------------------------------------------------------------------------|
| `hive`              | [Hive catalog property](./apache-hive-catalog.md#catalog-properties)           |
| `lakehouse-iceberg` | [Iceberg catalog property](./lakehouse-iceberg-catalog.md#catalog-properties)  |
| `lakehouse-paimon`  | [Paimon catalog property](./lakehouse-paimon-catalog.md#catalog-properties)    |
| `lakehouse-hudi`    | [Hudi catalog property](./lakehouse-hudi-catalog.md#catalog-properties)        |
| `jdbc-mysql`        | [MySQL catalog property](./jdbc-mysql-catalog.md#catalog-properties)           |
| `jdbc-postgresql`   | [PostgreSQL catalog property](./jdbc-postgresql-catalog.md#catalog-properties) |
| `jdbc-doris`        | [Doris catalog property](./jdbc-doris-catalog.md#catalog-properties)           |
| `jdbc-oceanbase`    | [OceanBase catalog property](./jdbc-oceanbase-catalog.md#catalog-properties)   |

### Load a catalog

You can load a catalog by sending a `GET` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}` endpoint or just use the Gravitino Java client. The following is an example of loading a catalog:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" http://localhost:8090/api/metalakes/metalake/catalogs/catalog
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
// Assuming you have created a metalake named `metalake` and a catalog named `catalog`
Catalog catalog = gravitinoClient.loadCatalog("catalog");
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
# ...
# Assuming you have created a metalake named `metalake` and a catalog named `catalog`
catalog = gravitino_client.load_catalog("catalog")
```

</TabItem>
</Tabs>

### Alter a catalog

You can modify a catalog by sending a `PUT` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}` endpoint or just use the Gravitino Java client. The following is an example of altering a catalog:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
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

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
// Assuming you have created a metalake named `metalake` and a catalog named `catalog`
Catalog catalog = gravitinoClient.alterCatalog("catalog",
    CatalogChange.rename("alter_catalog"), CatalogChange.updateComment("new comment"));
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
# ...
# Assuming you have created a metalake named `metalake` and a catalog named `catalog`
changes = (CatalogChange.update_comment("new comment"))
catalog = gravitino_client.alterCatalog("catalog", *changes)
# ...
```

</TabItem>
</Tabs>

Currently, Gravitino supports the following changes to a catalog:

| Supported modification | JSON                                                         | Java                                          |
|------------------------|--------------------------------------------------------------|-----------------------------------------------|
| Rename catalog         | `{"@type":"rename","newName":"catalog_renamed"}`             | `CatalogChange.rename("catalog_renamed")`     |
| Update comment         | `{"@type":"updateComment","newComment":"new_comment"}`       | `CatalogChange.updateComment("new_comment")`  |
| Set a property         | `{"@type":"setProperty","property":"key1","value":"value1"}` | `CatalogChange.setProperty("key1", "value1")` |
| Remove a property      | `{"@type":"removeProperty","property":"key1"}`               | `CatalogChange.removeProperty("key1")`        |

:::warning

Most catalog-altering operations are generally safe. However, if you want to change the catalog's URI, you should proceed with caution. Changing the URI may point to a different cluster, rendering the metadata stored in Gravitino unusable.
For instance, if the old URI and the new URI point to different clusters that both have a database named db1, changing the URI might cause the old metadata, such as audit information, to be used when accessing db1, which is undesirable.

Therefore, do not change the catalog's URI unless you fully understand the consequences of such a modification.

:::

### Enable a catalog

Catalog has a reserved property - `in-use`, which indicates whether the catalog is available for use. By default, the `in-use` property is set to `true`.
To enable a disabled catalog, you can send a `PATCH` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}` endpoint or use the Gravitino Java client.

The following is an example of enabling a catalog:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X PATCH -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{"inUse": true}' \
http://localhost:8090/api/metalakes/metalake/catalogs/catalog
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
// Assuming you have created a metalake named `metalake` and a catalog named `catalog`
gravitinoClient.enableCatalog("catalog");
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
# ...
# Assuming you have created a metalake named `metalake` and a catalog named `catalog`
gravitino_client.enable_catalog("catalog")
# ...
```

</TabItem>
</Tabs>

:::info
This operation does nothing if the catalog is already enabled.
:::

### Disable a catalog

Once a catalog is disabled:
- Users can only [list](#list-all-catalogs-in-a-metalake), [load](#load-a-catalog), [drop](#drop-a-catalog), or [enable](#enable-a-catalog) it.
- Any other operation on the catalog or its sub-entities will result in an error.

To disable a catalog, you can send a `PATCH` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}` endpoint or use the Gravitino Java client.

The following is an example of disabling a catalog:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X PATCH -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{"inUse": false}' \
http://localhost:8090/api/metalakes/metalake/catalogs/catalog
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
// Assuming you have created a metalake named `metalake` and a catalog named `catalog`
gravitinoClient.disableCatalog("catalog");
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
# ...
# Assuming you have created a metalake named `metalake` and a catalog named `catalog`
gravitino_client.disable_catalog("catalog")
# ...
```

</TabItem>
</Tabs>

:::info
This operation does nothing if the catalog is already disabled.
:::

### Drop a catalog

Deleting a catalog by "force" is not a default behavior, so please make sure:

- There are no schemas under the catalog. Otherwise, you will get an error.
- The catalog is [disabled](#disable-a-catalog). Otherwise, you will get an error.

Deleting a catalog by "force" will:

- Delete all sub-entities (schemas, tables, etc.) under the catalog.
- Delete the catalog itself even if it is enabled.
- Not delete the external resources (such as database, table, etc.) associated with sub-entities unless they are managed (such as managed fileset).

You can remove a catalog by sending a `DELETE` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}` endpoint or just use the Gravitino Java client. The following is an example of dropping a catalog:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/metalake/catalogs/catalog?force=false
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
// Assuming you have created a metalake named `metalake` and a catalog named `catalog`
// force can be true or false
gravitinoClient.dropCatalog("catalog", false);
// ...

```

</TabItem>
<TabItem value="python" label="Python">

```python
# ...
# Assuming you have created a metalake named `metalake` and a catalog named `catalog`
# force can be true or false
gravitino_client.drop_catalog(name="catalog", force=False)
# ...
```

</TabItem>
</Tabs>

### List all catalogs in a metalake

You can list all catalogs under a metalake by sending a `GET` request to the `/api/metalakes/{metalake_name}/catalogs` endpoint or just use the Gravitino Java client. The following is an example of listing all the catalogs in
a metalake:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/metalake/catalogs
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
// Assuming you have just created a metalake named `metalake`
String[] catalogNames = gravitinoClient.listCatalogs();
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
# ...
# Assuming you have created a metalake named `metalake` and a catalog named `catalog`
catalog_names = gravitino_client.list_catalogs()
# ...
```

</TabItem>
</Tabs>

### List all catalogs' information in a metalake

You can list all catalogs' information under a metalake by sending a `GET` request to the `/api/metalakes/{metalake_name}/catalogs?details=true` endpoint or just use the Gravitino Java client. The following is an example of listing all the catalogs' information in a metalake:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/metalake/catalogs?details=true
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
// Assuming you have just created a metalake named `metalake`
Catalog[] catalogsInfos = gravitinoMetaLake.listCatalogsInfo();
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
# ...
# Assuming you have created a metalake named `metalake` and a catalog named `catalog`
catalogs_info = gravitino_client.list_catalogs_info()
# ...
```

</TabItem>
</Tabs>


## Schema operations

:::tip
Users should create a metalake and a catalog, then ensure that the metalake and catalog are enabled before operating schemas.
:::

### Create a schema

You can create a schema by sending a `POST` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas` endpoint or just use the Gravitino Java client. The following is an example of creating a schema:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "schema",
  "comment": "comment",
  "properties": {
    "key1": "value1"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
// Assuming you have just created a Hive catalog named `hive_catalog`
Catalog catalog = gravitinoClient.loadCatalog("hive_catalog");

SupportsSchemas supportsSchemas = catalog.asSchemas();

Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    .build();
Schema schema = supportsSchemas.createSchema("schema",
    "This is a schema",
    schemaProperties
);
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://127.0.0.1:8090", metalake_name="metalake")
catalog: Catalog = gravitino_client.load_catalog(name="hive_catalog")
catalog.as_schemas().create_schema(name="schema",
                                   comment="This is a schema",
                                   properties={})
```

</TabItem>
</Tabs>

Currently, Gravitino supports the following schema property:

| Catalog provider    | Schema property                                                              |
|---------------------|------------------------------------------------------------------------------|
| `hive`              | [Hive schema property](./apache-hive-catalog.md#schema-properties)           |
| `lakehouse-iceberg` | [Iceberg scheme property](./lakehouse-iceberg-catalog.md#schema-properties)  |
| `lakehouse-paimon`  | [Paimon scheme property](./lakehouse-paimon-catalog.md#schema-properties)    |
| `lakehouse-hudi`    | [Hudi scheme property](./lakehouse-hudi-catalog.md#schema-properties)        |
| `jdbc-mysql`        | [MySQL schema property](./jdbc-mysql-catalog.md#schema-properties)           |
| `jdbc-postgresql`   | [PostgreSQL schema property](./jdbc-postgresql-catalog.md#schema-properties) |
| `jdbc-doris`        | [Doris schema property](./jdbc-doris-catalog.md#schema-properties)           |
| `jdbc-oceanbase`    | [OceanBase schema property](./jdbc-oceanbase-catalog.md#schema-properties)   |

### Load a schema

You can create a schema by sending a `GET` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}` endpoint or just use the Gravitino Java client. The following is an example of loading a schema:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET \-H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
// Assuming you have just created a Hive catalog named `hive_catalog`
Catalog catalog = gravitinoClient.loadCatalog("hive_catalog");
SupportsSchemas supportsSchemas = catalog.asSchemas();
Schema schema = supportsSchemas.loadSchema("schema");
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://127.0.0.1:8090", metalake_name="metalake")
catalog: Catalog = gravitino_client.load_catalog(name="hive_catalog")
schema: Schema = catalog.as_schemas().load_schema(name="schema")
```

</TabItem>
</Tabs>

### Alter a schema

You can change a schema by sending a `PUT` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}` endpoint or just use the Gravitino Java client. The following is an example of modifying a schema:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
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

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
// Assuming you have just created a Hive catalog named `hive_catalog`
Catalog catalog = gravitinoClient.loadCatalog("hive_catalog");

SupportsSchemas supportsSchemas = catalog.asSchemas();

Schema schema = supportsSchemas.alterSchema("schema",
    SchemaChange.removeProperty("key1"),
    SchemaChange.setProperty("key2", "value2"));
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://127.0.0.1:8090", metalake_name="metalake")
catalog: Catalog = gravitino_client.load_catalog(name="hive_catalog")

changes = (
    SchemaChange.remove_property("schema_properties_key1"),
    SchemaChange.set_property("schema_properties_key2", "schema_properties_new_value"),
)
schema_new: Schema = catalog.as_schemas().alter_schema("schema", 
                                                       *changes)
```

</TabItem>
</Tabs>

Currently, Gravitino supports the following changes to a schema:

| Supported modification | JSON                                                         | Java                                          |
|------------------------|--------------------------------------------------------------|-----------------------------------------------|
| Set a property         | `{"@type":"setProperty","property":"key1","value":"value1"}` | `SchemaChange.setProperty("key1", "value1")`  |
| Remove a property      | `{"@type":"removeProperty","property":"key1"}`               | `SchemaChange.removeProperty("key1")`         |

### Drop a schema
You can remove a schema by sending a `DELETE` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}` endpoint or just use the Gravitino Java client. The following is an example of dropping a schema:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
// cascade can be true or false
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema?cascade=true
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
// Assuming you have just created a Hive catalog named `hive_catalog`
Catalog catalog = gravitinoClient.loadCatalog("hive_catalog");

SupportsSchemas supportsSchemas = catalog.asSchemas();
// cascade can be true or false
supportsSchemas.dropSchema("schema", true);
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://127.0.0.1:8090", metalake_name="metalake")
catalog: Catalog = gravitino_client.load_catalog(name="hive_catalog")

catalog.as_schemas().drop_schema("schema", cascade=True)
```

</TabItem>
</Tabs>

If `cascade` is true, Gravitino will drop all tables under the schema. Otherwise, Gravitino will throw an exception if there are tables under the schema. 
Some catalogs may not support cascading deletion of a schema, please refer to the related doc for more details.

### List all schemas under a catalog

You can list all schemas under a catalog by sending a `GET` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas` endpoint or just use the Gravitino Java client. The following is an example of listing all the schemas
    in a catalog:


<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
// Assuming you have just created a Hive catalog named `hive_catalog`
Catalog catalog = gravitinoClient.loadCatalog("hive_catalog");

SupportsSchemas supportsSchemas = catalog.asSchemas();
String[] schemas = supportsSchemas.listSchemas();
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://127.0.0.1:8090", metalake_name="metalake")
catalog: Catalog = gravitino_client.load_catalog(name="hive_catalog")

schema_list: List[NameIdentifier] = catalog.as_schemas().list_schemas()
```

</TabItem>
</Tabs>

## Table operations

:::tip
Users should create a metalake, a catalog and a schema, then ensure that the metalake and catalog are enabled before before operating tables.
:::

### Create a table

You can create a table by sending a `POST` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/tables` endpoint or just use the Gravitino Java client. The following is an example of creating a table:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "example_table",
  "comment": "This is an example table",
  "columns": [
    {
      "name": "id",
      "type": "integer",
      "comment": "id column comment",
      "nullable": false,
      "autoIncrement": true,
      "defaultValue": {
        "type": "literal",
        "dataType": "integer",
        "value": "-1"
      }
    },
    {
      "name": "name",
      "type": "varchar(500)",
      "comment": "name column comment",
      "nullable": true,
      "autoIncrement": false,
      "defaultValue": {
        "type": "literal",
        "dataType": "null",
        "value": "null"
      }
    },
    {
      "name": "StartingDate",
      "type": "timestamp",
      "comment": "StartingDate column comment",
      "nullable": false,
      "autoIncrement": false,
      "defaultValue": {
        "type": "function",
        "funcName": "current_timestamp",
        "funcArgs": []
      }
    },
    {
      "name": "info",
      "type": {
        "type": "struct",
        "fields": [
          {
            "name": "position",
            "type": "string",
            "nullable": true,
            "comment": "position field comment"
          },
          {
            "name": "contact",
            "type": {
              "type": "list",
              "elementType": "integer",
              "containsNull": false
            },
            "nullable": true,
            "comment": "contact field comment"
          },
          {
            "name": "rating",
            "type": {
              "type": "map",
              "keyType": "string",
              "valueType": "integer",
              "valueContainsNull": false
            },
            "nullable": true,
            "comment": "rating field comment"
          }
        ]
      },
      "comment": "info column comment",
      "nullable": true
    },
    {
      "name": "dt",
      "type": "date",
      "comment": "dt column comment",
      "nullable": true
    }
  ],
  "partitioning": [
    {
      "strategy": "identity",
      "fieldName": [ "dt" ]
    }
  ],
  "distribution": {
    "strategy": "hash",
    "number": 32,
    "funcArgs": [
      {
        "type": "field",
        "fieldName": [ "id" ]
      }
    ]
  },
  "sortOrders": [
    {
      "sortTerm": {
        "type": "field",
        "fieldName": [ "age" ]
      },
      "direction": "asc",
      "nullOrdering": "nulls_first"
    }
  ],
  "indexes": [
    {
      "indexType": "primary_key",
      "name": "PRIMARY",
      "fieldNames": [["id"]]
    }
  ],
  "properties": {
    "format": "ORC"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema/tables
```

</TabItem>
<TabItem value="java" label="Java">

```java
// Assuming you have just created a Hive catalog named `hive_catalog`
Catalog catalog = gravitinoClient.loadCatalog("hive_catalog");

TableCatalog tableCatalog = catalog.asTableCatalog();

// This is an example of creating a Hive table, you should refer to the related doc to get the
// table properties of other catalogs.
Map<String, String> tablePropertiesMap = ImmutableMap.<String, String>builder()
        .put("format", "ORC")
        // For more table properties, please refer to the related doc.
        .build();

tableCatalog.createTable(
  NameIdentifier.of("schema", "example_table"),
  new Column[] {
    Column.of("id", Types.IntegerType.get(), "id column comment", false, true, Literals.integerLiteral(-1)),
    Column.of("name", Types.VarCharType.of(500), "name column comment", true, false, Literals.NULL),
    Column.of("StartingDate", Types.TimestampType.withoutTimeZone(), "StartingDate column comment", false, false, Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP),
    Column.of("info", Types.StructType.of(
        Field.nullableField("position", Types.StringType.get(), "Position of the user"),
        Field.nullableField("contact", Types.ListType.of(Types.IntegerType.get(), false), "contact field comment"),
        Field.nullableField("rating", Types.MapType.of(Types.VarCharType.of(1000), Types.IntegerType.get(), false), "rating field comment")
      ), "info column comment", true, false, null),
    Column.of("dt", Types.DateType.get(), "dt column comment", true, false, null)
  },
  "This is an example table",
  tablePropertiesMap,
  new Transform[] {Transforms.identity("id")},
  Distributions.of(Strategy.HASH, 32, NamedReference.field("id")),
  new SortOrder[] {SortOrders.ascending(NamedReference.field("name"))},
  new Index[] {Indexes.of(IndexType.PRIMARY_KEY, "PRIMARY", new String[][]{{"id"}})}
);
```

</TabItem>
</Tabs>

:::caution
The provided example demonstrates table creation but isn't directly executable in Gravitino, since not all catalogs fully support these capabilities.
:::

In order to create a table, you need to provide the following information:

- Table column name and type
- Table column default value (optional)
- Table column auto-increment (optional)
- Table property (optional)

#### Gravitino table column type

The following types that Gravitino supports:

| Type                      | Java / Python                                                            | JSON                                                                                                                                 | Description                                                                                                                                                                |
|---------------------------|--------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Boolean                   | `Types.BooleanType.get()`                                                | `boolean`                                                                                                                            | Boolean type                                                                                                                                                               |
| Byte                      | `Types.ByteType.get()`                                                   | `byte`                                                                                                                               | Byte type, indicates a numerical value of 1 byte                                                                                                                           |
| Unsigned Byte             | `Types.ByteType.unsigned()`                                              | `byte unsigned`                                                                                                                      | Unsigned Byte type, indicates a unsigned numerical value of 1 byte                                                                                                         |
| Short                     | `Types.ShortType.get()`                                                  | `short`                                                                                                                              | Short type, indicates a numerical value of 2 bytes                                                                                                                         |
| Unsigned Short            | `Types.ShortType.unsigned()`                                             | `short unsigned`                                                                                                                     | Unsigned Short type, indicates a unsigned numerical value of 2 bytes                                                                                                       |
| Integer                   | `Types.IntegerType.get()`                                                | `integer`                                                                                                                            | Integer type, indicates a numerical value of 4 bytes                                                                                                                       |
| Unsigned Integer          | `Types.IntegerType.unsigned()`                                           | `integer unsigned`                                                                                                                   | Unsigned Integer type, indicates a unsigned numerical value of 4 bytes                                                                                                     |
| Long                      | `Types.LongType.get()`                                                   | `long`                                                                                                                               | Long type, indicates a numerical value of 8 bytes                                                                                                                          |
| Unsigned Long             | `Types.LongType.unsigned()`                                              | `long unsigned`                                                                                                                      | Unsigned Long type, indicates a unsigned numerical value of 8 bytes                                                                                                        |
| Float                     | `Types.FloatType.get()`                                                  | `float`                                                                                                                              | Float type, indicates a single-precision floating point number                                                                                                             |
| Double                    | `Types.DoubleType.get()`                                                 | `double`                                                                                                                             | Double type, indicates a double-precision floating point number                                                                                                            |
| Decimal(precision, scale) | `Types.DecimalType.of(precision, scale)`                                 | `decimal(p, s)`                                                                                                                      | Decimal type, indicates a fixed-precision decimal number with the constraint that the precision must be in range `[1, 38]` and the scala must be in range `[0, precision]` |
| String                    | `Types.StringType.get()`                                                 | `string`                                                                                                                             | String type                                                                                                                                                                |
| FixedChar(length)         | `Types.FixedCharType.of(length)`                                         | `char(l)`                                                                                                                            | Char type, indicates a fixed-length string                                                                                                                                 |
| VarChar(length)           | `Types.VarCharType.of(length)`                                           | `varchar(l)`                                                                                                                         | Varchar type, indicates a variable-length string, the length is the maximum length of the string                                                                           |
| Timestamp                 | `Types.TimestampType.withoutTimeZone()`                                  | `timestamp`                                                                                                                          | Timestamp type, indicates a timestamp without timezone                                                                                                                     |
| TimestampWithTimezone     | `Types.TimestampType.withTimeZone()`                                     | `timestamp_tz`                                                                                                                       | Timestamp with timezone type, indicates a timestamp with timezone                                                                                                          |
| Date                      | `Types.DateType.get()`                                                   | `date`                                                                                                                               | Date type                                                                                                                                                                  |
| Time                      | `Types.TimeType.withoutTimeZone()`                                       | `time`                                                                                                                               | Time type                                                                                                                                                                  |
| IntervalToYearMonth       | `Types.IntervalYearType.get()`                                           | `interval_year`                                                                                                                      | Interval type, indicates an interval of year and month                                                                                                                     |
| IntervalToDayTime         | `Types.IntervalDayType.get()`                                            | `interval_day`                                                                                                                       | Interval type, indicates an interval of day and time                                                                                                                       |
| Fixed(length)             | `Types.FixedType.of(length)`                                             | `fixed(l)`                                                                                                                           | Fixed type, indicates a fixed-length binary array                                                                                                                          |
| Binary                    | `Types.BinaryType.get()`                                                 | `binary`                                                                                                                             | Binary type, indicates a arbitrary-length binary array                                                                                                                     |
| List                      | `Types.ListType.of(elementType, elementNullable)`                        | `{"type": "list", "containsNull": JSON Boolean, "elementType": type JSON}`                                                           | List type, indicate a list of elements with the same type                                                                                                                  |
| Map                       | `Types.MapType.of(keyType, valueType)`                                   | `{"type": "map", "keyType": type JSON, "valueType": type JSON, "valueContainsNull": JSON Boolean}`                                   | Map type, indicate a map of key-value pairs                                                                                                                                |
| Struct                    | `Types.StructType.of([Types.StructType.Field.of(name, type, nullable)])` | `{"type": "struct", "fields": [JSON StructField, {"name": string, "type": type JSON, "nullable": JSON Boolean, "comment": string}]}` | Struct type, indicate a struct of fields                                                                                                                                   |
| Union                     | `Types.UnionType.of([type1, type2, ...])`                                | `{"type": "union", "types": [type JSON, ...]}`                                                                                       | Union type, indicates a union of types                                                                                                                                     |
| UUID                      | `Types.UUIDType.get()`                                                   | `uuid`                                                                                                                               | UUID type, indicates a universally unique identifier                                                                                                                       |

The related java doc is [here](pathname:///docs/0.10.0-SNAPSHOT/api/java/org/apache/gravitino/rel/types/Type.html).

##### External type

External type is a special type of column type, when you need to use a data type that is not in the Gravitino type
system, and you explicitly know its string representation in an external catalog (usually used in JDBC catalogs), then
you can use the ExternalType to represent the type. Similarly, if the original type is unsolvable, it will be
represented by ExternalType.
The following shows the data structure of an external type in JSON and Java, enabling easy retrieval of its string value.

<Tabs groupId='language' queryString>
  <TabItem value="Json" label="Json">

```json
{
  "type": "external",
  "catalogString": "user-defined"
}
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
// The result of the following type is a string "user-defined"
String typeString = ((ExternalType) type).catalogString();
```

  </TabItem>
</Tabs>

##### Unparsed type

Unparsed type is a special type of column type, it used to address compatibility issues in type serialization and
deserialization between the server and client. For instance, if a new column type is introduced on the Gravitino server
that the client does not recognize, it will be treated as an unparsed type on the client side.
The following shows the data structure of an unparsed type in JSON and Java, enabling easy retrieval of its value.

<Tabs groupId='language' queryString>
  <TabItem value="Json" label="Json">

```json
{
  "type": "unparsed",
  "unparsedType": "unknown-type"
}
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
// The result of the following type is a string "unknown-type"
String unparsedValue = ((UnparsedType) type).unparsedType();
```

  </TabItem>
</Tabs>

#### Table column default value

When defining a table column, you can specify a [literal](./expression.md#literal) or an [expression](./expression.md) as the default value. The default value typically applies to new rows that are inserted into the table by the underlying catalog.

The following is a table of the column default value that Gravitino supports for different catalogs:

| Catalog provider    | Supported default value |
|---------------------|-------------------------|
| `hive`              | &#10008;                |
| `lakehouse-iceberg` | &#10008;                |
| `lakehouse-paimon`  | &#10008;                |
| `lakehouse-hudi`    | &#10008;                |
| `jdbc-mysql`        | &#10004;                |
| `jdbc-postgresql`   | &#10004;                |
| `jdbc-doris`        | &#10004;                |
| `jdbc-oceanbase`    | &#10004;                |

#### Table column auto-increment

Auto-increment provides a convenient way to ensure that each row in a table has a unique identifier without the need for manually managing identifier allocation.
The following table shows the column auto-increment that Gravitino supports for different catalogs:

| Catalog provider    | Supported auto-increment                                                         |
|---------------------|----------------------------------------------------------------------------------|
| `hive`              | &#10008;                                                                         |
| `lakehouse-iceberg` | &#10008;                                                                         |
| `lakehouse-paimon`  | &#10008;                                                                         |
| `lakehouse-hudi`    | &#10008;                                                                         |
| `jdbc-mysql`        | &#10004;([limitations](./jdbc-mysql-catalog.md#table-column-auto-increment))     |
| `jdbc-postgresql`   | &#10004;                                                                         |
| `jdbc-doris`        | &#10008;                                                                         |
| `jdbc-oceanbase`    | &#10004;([limitations](./jdbc-oceanbase-catalog.md#table-column-auto-increment)) |

#### Table property and type mapping

The following is the table property that Gravitino supports:

| Catalog provider    | Table property                                                              | Type mapping                                                               |
|---------------------|-----------------------------------------------------------------------------|----------------------------------------------------------------------------|
| `hive`              | [Hive table property](./apache-hive-catalog.md#table-properties)            | [Hive type mapping](./apache-hive-catalog.md#table-column-types)           |
| `lakehouse-iceberg` | [Iceberg table property](./lakehouse-iceberg-catalog.md#table-properties)   | [Iceberg type mapping](./lakehouse-iceberg-catalog.md#table-column-types)  |
| `lakehouse-paimon`  | [Paimon table property](./lakehouse-paimon-catalog.md#table-properties)     | [Paimon type mapping](./lakehouse-paimon-catalog.md#table-column-types)    |
| `lakehouse-hudi`    | [Hudi table property](./lakehouse-hudi-catalog.md#table-properties)         | [Hudi type mapping](./lakehouse-hudi-catalog.md#table-column-types)        |
| `jdbc-mysql`        | [MySQL table property](./jdbc-mysql-catalog.md#table-properties)            | [MySQL type mapping](./jdbc-mysql-catalog.md#table-column-types)           |
| `jdbc-postgresql`   | [PostgreSQL table property](./jdbc-postgresql-catalog.md#table-properties)  | [PostgreSQL type mapping](./jdbc-postgresql-catalog.md#table-column-types) |
| `jdbc-doris`        | [Doris table property](./jdbc-doris-catalog.md#table-properties)            | [Doris type mapping](./jdbc-doris-catalog.md#table-column-types)           |
| `jdbc-oceanbase`    | [OceanBase table property](./jdbc-oceanbase-catalog.md#table-properties)    | [OceanBase type mapping](./jdbc-oceanbase-catalog.md#table-column-types)   |

#### Table partitioning, distribution, sort ordering and indexes

In addition to the basic settings, Gravitino supports the following features:

| Feature             | Description                                                                                                                                                                                                                                                                                    | Java doc                                                                                                                                  |
|---------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| Table partitioning  | Equal to `PARTITION BY` in Apache Hive, It is a partitioning strategy that is used to split a table into parts based on partition keys. Some table engine may not support this feature                                                                                                         | [Partition](pathname:///docs/0.10.0-SNAPSHOT/api/java/org/apache/gravitino/dto/rel/partitioning/Partitioning.html)             |
| Table distribution  | Equal to `CLUSTERED BY` in Apache Hive, distribution a.k.a (Clustering) is a technique to split the data into more manageable files/parts, (By specifying the number of buckets to create). The value of the distribution column will be hashed by a user-defined number into buckets.         | [Distribution](pathname:///docs/0.10.0-SNAPSHOT/api/java/org/apache/gravitino/rel/expressions/distributions/Distribution.html) |
| Table sort ordering | Equal to `SORTED BY` in Apache Hive, sort ordering is a method to sort the data in specific ways such as by a column or a function, and then store table data. it will highly improve the query performance under certain scenarios.                                                           | [SortOrder](pathname:///docs/0.10.0-SNAPSHOT/api/java/org/apache/gravitino/rel/expressions/sorts/SortOrder.html)               |
| Table indexes       | Equal to `KEY/INDEX` in MySQL , unique key enforces uniqueness of values in one or more columns within a table. It ensures that no two rows have identical values in specified columns, thereby facilitating data integrity and enabling efficient data retrieval and manipulation operations. | [Index](pathname:///docs/0.10.0-SNAPSHOT/api/java/org/apache/gravitino/rel/indexes/Index.html)                                 |

For more information, please see the related document on [partitioning, bucketing, sorting, and indexes](table-partitioning-bucketing-sort-order-indexes.md).

:::note
The code above is an example of creating a Hive table. For other catalogs, the code is similar, but the supported column type, and table properties may be different. For more details, please refer to the related doc.
:::

### Load a table

You can load a table by sending a `GET` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}` endpoint or just use the Gravitino Java client. The following is an example of loading a table:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json"  \
http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema/tables/table
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
// Assuming you have just created a Hive catalog named `hive_catalog`
Catalog catalog = gravitinoClient.loadCatalog("hive_catalog");

TableCatalog tableCatalog = catalog.asTableCatalog();
tableCatalog.loadTable(NameIdentifier.of("schema", "table"));
// ...
```

</TabItem>
</Tabs>

:::note
- When Gravitino loads a table from a catalog with various data types, if Gravitino is unable to parse the data type, it will use an **[External Type](#external-type)** to preserve the original data type, ensuring that the table can be loaded successfully.
- When Gravitino loads a table from a catalog that supports default value, if Gravitino is unable to parse the default value, it will use an **[Unparsed Expression](./expression.md#unparsed-expression)** to preserve the original default value, ensuring that the table can be loaded successfully.
:::

### Alter a table

You can modify a table by sending a `PUT` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}` endpoint or just use the Gravitino Java client. The following is an example of modifying a table:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
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
}' http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema/tables/table
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
// Assuming you have just created a Hive catalog named `hive_catalog`
Catalog catalog = gravitinoClient.loadCatalog("hive_catalog");

TableCatalog tableCatalog = catalog.asTableCatalog();

Table t = tableCatalog.alterTable(NameIdentifier.of("schema", "table"),
    TableChange.rename("table_renamed"), TableChange.updateComment("xxx"));
// ...
```

</TabItem>
</Tabs>

Currently, Gravitino supports the following changes to a table:

| Supported modification               | JSON                                                                                                                                                                                                                                                         | Java                                        |
|--------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------|
| Rename table                         | `{"@type":"rename","newName":"table_renamed"}`                                                                                                                                                                                                               | `TableChange.rename("table_renamed")`       |
| Update comment                       | `{"@type":"updateComment","newComment":"new_comment"}`                                                                                                                                                                                                       | `TableChange.updateComment("new_comment")`  |
| Set a table property                 | `{"@type":"setProperty","property":"key1","value":"value1"}`                                                                                                                                                                                                 | `TableChange.setProperty("key1", "value1")` |
| Remove a table property              | `{"@type":"removeProperty","property":"key1"}`                                                                                                                                                                                                               | `TableChange.removeProperty("key1")`        |
| Add a column                         | `{"@type":"addColumn","fieldName":["position"],"type":"varchar(20)","comment":"Position of user","position":"FIRST","nullable": true, "autoIncrement": false, "defaultValue" : {"type": "literal", "dataType": "varchar(20)", "value": "Default Position"}}` | `TableChange.addColumn(...)`                |
| Delete a column                      | `{"@type":"deleteColumn","fieldName": ["name"], "ifExists": true}`                                                                                                                                                                                           | `TableChange.deleteColumn(...)`             |
| Rename a column                      | `{"@type":"renameColumn","oldFieldName":["name_old"], "newFieldName":"name_new"}`                                                                                                                                                                            | `TableChange.renameColumn(...)`             |
| Update the column comment            | `{"@type":"updateColumnComment", "fieldName": ["name"], "newComment": "new comment"}`                                                                                                                                                                        | `TableChange.updateColumnCommment(...)`     |
| Update the type of a column          | `{"@type":"updateColumnType","fieldName": ["name"], "newType":"varchar(100)"}`                                                                                                                                                                               | `TableChange.updateColumnType(...)`         |
| Update the nullability of a column   | `{"@type":"updateColumnNullability","fieldName": ["name"],"nullable":true}`                                                                                                                                                                                  | `TableChange.updateColumnNullability(...)`  |
| Update the position of a column      | `{"@type":"updateColumnPosition","fieldName": ["name"], "newPosition":"default"}`                                                                                                                                                                            | `TableChange.updateColumnPosition(...)`     |
| Update the default value of a column | `{"@type":"updateColumnDefaultValue","fieldName": ["name"], "newDefaultValue":{"type":"literal","dataType":"varchar(100)","value":"new default value}}`                                                                                                      | `TableChange.updateColumnDefaultValue(...)` |

### Drop a table

You can remove a table by sending a `DELETE` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}` endpoint or just use the Gravitino Java client. The following is an example of dropping a table:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
## Purge can be true or false, if purge is true, Gravitino will remove the data from the table.

curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema/tables/table?purge=true
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
// Assuming you have just created a Hive catalog named `hive_catalog`
Catalog catalog = gravitinoClient.loadCatalog("hive_catalog");

TableCatalog tableCatalog = catalog.asTableCatalog();

// Drop a table
tableCatalog.dropTable(NameIdentifier.of("schema", "table"));

// Purge a table
tableCatalog.purgeTable(NameIdentifier.of("schema", "table"));
// ...
```

</TabItem>
</Tabs>

There are two ways to remove a table: `dropTable` and `purgeTable`: 

* `dropTable`  removes both the metadata and the directory associated with the table from the file system if the table is not an external table. In case of an external table, only the associated metadata is removed.
* `purgeTable` completely removes both the metadata and the directory associated with the table and skipping trash, if the table is an external table or the catalogs don't support purge table, `UnsupportedOperationException` is thrown.

Hive catalog and lakehouse-iceberg catalog supports `purgeTable` while jdbc-mysql, jdbc-postgresql and lakehouse-paimon catalog doesn't support.

### List all tables under a schema

You can list all tables in a schema by sending a `GET` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/tables` endpoint or just use the Gravitino Java client. The following is an example of listing all the tables in a schema:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema/tables
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
// Assuming you have just created a Hive catalog named `hive_catalog`
Catalog catalog = gravitinoClient.loadCatalog("hive_catalog");

TableCatalog tableCatalog = catalog.asTableCatalog();
NameIdentifier[] identifiers =
    tableCatalog.listTables(Namespace.of("schema"));
// ...
```

</TabItem>
</Tabs>
