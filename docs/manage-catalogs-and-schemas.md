---
title: "Manage Catalogs and Schemas"
slug: "/manage-catalogs-and-schemas"
keyword: "catalog management, schema management, catalog, schema, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

This page covers the Gravitino API for catalogs and schemas. For what a catalog and a schema are,
the catalog types, what Gravitino stores, permissions, and how to work with them in the UI, see
[Catalogs and Schemas](./catalogs-and-schemas.md).

Connection properties differ by provider and are documented on each catalog type's own page.

## Catalog Operations

### Create a Catalog

A catalog needs a name, a type, and for most types a provider. Properties carry the connection
details. The example below registers a Hive metastore; other providers take different properties.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "sales",
  "type": "RELATIONAL",
  "provider": "hive",
  "comment": "Sales estate",
  "properties": {"metastore.uris": "thrift://localhost:9083"}
}' http://localhost:8090/api/metalakes/example/catalogs
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient client = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("example")
    .build();

Catalog catalog = client.createCatalog(
    "sales",
    Catalog.Type.RELATIONAL,
    "hive",
    "Sales estate",
    ImmutableMap.of("metastore.uris", "thrift://localhost:9083"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalog = client.create_catalog(
    name="sales",
    catalog_type=Catalog.Type.RELATIONAL,
    provider="hive",
    comment="Sales estate",
    properties={"metastore.uris": "thrift://localhost:9083"})
```

</TabItem>
</Tabs>

Fileset and model catalogs are managed by Gravitino rather than federated, so they take no provider.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "landing",
  "type": "FILESET",
  "comment": "Landing zone",
  "properties": {"location": "s3a://example-bucket/landing"}
}' http://localhost:8090/api/metalakes/example/catalogs
```

</TabItem>
</Tabs>

### Load a Catalog

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/sales
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = client.loadCatalog("sales");
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalog = client.load_catalog("sales")
```

</TabItem>
</Tabs>

### Alter a Catalog

Changes are applied as a list in one request.

| Change             | JSON                                                         | Java                                          | Python                                          |
|--------------------|--------------------------------------------------------------|-----------------------------------------------|-------------------------------------------------|
| Rename             | `{"@type":"rename","newName":"sales_v2"}`                    | `CatalogChange.rename("sales_v2")`            | `CatalogChange.rename("sales_v2")`              |
| Update the comment | `{"@type":"updateComment","newComment":"new_comment"}`       | `CatalogChange.updateComment("new_comment")`  | `CatalogChange.update_comment("new_comment")`   |
| Set a property     | `{"@type":"setProperty","property":"key1","value":"value1"}` | `CatalogChange.setProperty("key1", "value1")` | `CatalogChange.set_property("key1", "value1")`  |
| Remove a property  | `{"@type":"removeProperty","property":"key1"}`               | `CatalogChange.removeProperty("key1")`        | `CatalogChange.remove_property("key1")`         |

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "updates": [
    {"@type": "updateComment", "newComment": "Sales estate, production"}
  ]
}' http://localhost:8090/api/metalakes/example/catalogs/sales
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = client.alterCatalog(
    "sales", CatalogChange.updateComment("Sales estate, production"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalog = client.alter_catalog(
    "sales", CatalogChange.update_comment("Sales estate, production"))
```

</TabItem>
</Tabs>

### Enable or Disable a Catalog

A catalog that is not in use can only be listed, loaded, enabled, or dropped.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X PATCH -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{"inUse": false}' \
  http://localhost:8090/api/metalakes/example/catalogs/sales
```

</TabItem>
<TabItem value="java" label="Java">

```java
client.disableCatalog("sales");
client.enableCatalog("sales");
```

</TabItem>
<TabItem value="python" label="Python">

```python
client.disable_catalog("sales")
client.enable_catalog("sales")
```

</TabItem>
</Tabs>

### Drop a Catalog

Without `force`, the catalog must have no schemas and must not be in use. With `force`, Gravitino
removes the registration and everything it holds about the contents.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  "http://localhost:8090/api/metalakes/example/catalogs/sales?force=false"
```

</TabItem>
<TabItem value="java" label="Java">

```java
boolean dropped = client.dropCatalog("sales", false);
```

</TabItem>
<TabItem value="python" label="Python">

```python
dropped = client.drop_catalog("sales", force=False)
```

</TabItem>
</Tabs>

### List Catalogs

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs

curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  "http://localhost:8090/api/metalakes/example/catalogs?details=true"
```

</TabItem>
<TabItem value="java" label="Java">

```java
String[] catalogNames = client.listCatalogs();
Catalog[] catalogs = client.listCatalogsInfo();
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalog_names = client.list_catalogs()
catalogs = client.list_catalogs_info()
```

</TabItem>
</Tabs>

## Schema Operations

Schema operations are the same for every catalog type. Creating a schema through Gravitino creates
it in the source system too, where the source supports that.

### Create a Schema

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "public",
  "comment": "Shared datasets",
  "properties": {}
}' http://localhost:8090/api/metalakes/example/catalogs/sales/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = client.loadCatalog("sales");
Schema schema = catalog.asSchemas().createSchema(
    "public", "Shared datasets", Collections.emptyMap());
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalog = client.load_catalog("sales")
schema = catalog.as_schemas().create_schema(
    schema_name="public", comment="Shared datasets", properties={})
```

</TabItem>
</Tabs>

### Load a Schema

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public
```

</TabItem>
<TabItem value="java" label="Java">

```java
Schema schema = catalog.asSchemas().loadSchema("public");
```

</TabItem>
<TabItem value="python" label="Python">

```python
schema = catalog.as_schemas().load_schema("public")
```

</TabItem>
</Tabs>

### Alter a Schema

A schema takes property changes only. It cannot be renamed, and its comment cannot be changed.

| Change            | JSON                                                         | Java                                         | Python                                        |
|-------------------|--------------------------------------------------------------|----------------------------------------------|-----------------------------------------------|
| Set a property    | `{"@type":"setProperty","property":"key1","value":"value1"}` | `SchemaChange.setProperty("key1", "value1")` | `SchemaChange.set_property("key1", "value1")` |
| Remove a property | `{"@type":"removeProperty","property":"key1"}`               | `SchemaChange.removeProperty("key1")`        | `SchemaChange.remove_property("key1")`        |

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "updates": [
    {"@type": "setProperty", "property": "owner", "value": "sales-eng"}
  ]
}' http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public
```

</TabItem>
<TabItem value="java" label="Java">

```java
Schema schema = catalog.asSchemas().alterSchema(
    "public", SchemaChange.setProperty("owner", "sales-eng"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
schema = catalog.as_schemas().alter_schema(
    "public", SchemaChange.set_property("owner", "sales-eng"))
```

</TabItem>
</Tabs>

### Drop a Schema

Without `cascade`, the schema must be empty. With `cascade`, everything inside it goes as well.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  "http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public?cascade=false"
```

</TabItem>
<TabItem value="java" label="Java">

```java
boolean dropped = catalog.asSchemas().dropSchema("public", false);
```

</TabItem>
<TabItem value="python" label="Python">

```python
dropped = catalog.as_schemas().drop_schema("public", cascade=False)
```

</TabItem>
</Tabs>

### List Schemas

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/sales/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
String[] schemaNames = catalog.asSchemas().listSchemas();
```

</TabItem>
<TabItem value="python" label="Python">

```python
schema_names = catalog.as_schemas().list_schemas()
```

</TabItem>
</Tabs>
