---
title: Manage relational metadata
slug: /manage-relational-metadata
date: 2023-12-10
keyword: Gravitino relational metadata manage
license: This software is licensed under the Apache License version 2.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This page introduces how to manage relational metadata by Apache Gravitino,
relational metadata refers to relational catalog, schema, table and partitions.
Through Gravitino, you can create, edit, and delete relational metadata via unified REST APIs or Java client.

In this document, Gravitino uses Apache Hive catalog as an example
to show how to manage relational metadata by Gravitino.
Other relational catalogs are similar to Hive catalog,
but they may have some differences, especially in catalog property,
table property, and column type.
For more details, please refer to the related doc.

- [**Apache Hive**](../catalogs/relational/hive/index.md)
- [**Apache Doris**](../catalogs/relational/jdbc/doris.md)
- [**Apache Hudi**](../catalogs/relational/lakehouse/hudi.md)
- [**Apache Iceberg**](../catalogs/relational/lakehouse/iceberg.md)
- [**Apache Paimon**](../catalogs/relational/lakehouse/paimon.md)
- [**MySQL**](../catalogs/relational/jdbc/mysql.md)
- [**OceanBase**](../catalogs/relational/jdbc/oceanbase.md)
- [**PostgreSQL**](../catalogs/relational/jdbc/postgresql.md)

Assuming:

- Gravitino has just started, and the host and port is [http://localhost:8090](http://localhost:8090).
- A metalake has been created and [enabled](../admin/metalake.md#enable-a-metalake).

## Catalog operations

### Create a catalog

:::caution
It is not recommended to use one data source to create multiple catalogs,
as multiple catalogs operating on the same source may result in unpredictable behavior.
:::

:::tip
The code below is an example of creating a Hive catalog.
For other relational catalogs, the code is similar,
but the catalog type, provider, and properties may vary.
For more details, please refer to the related documentation.

For relational catalog, you must set the catalog `type` to `RELATIONAL` when creating a catalog.
:::

You can create a catalog by sending a `POST` request to the
`/api/metalakes/{metalake}/catalogs` endpoint or just use the Gravitino Java client.
The following is an example of creating a catalog:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{ \
    "name": "catalog", \
    "type": "RELATIONAL", \
    "comment": "comment", \
    "provider": "hive", \
    "properties": { \
      "metastore.uris": "thrift://localhost:9083" \
    } \
  }' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs
```

</TabItem>
<TabItem value="java" label="Java">

```java
// This assumes that you have created a metalake named `metalake`
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://127.0.0.1:8090")
    .withMetalake("metalake")
    .build();

// Please change the properties according to the value of the provider.
Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
        // You should replace the following with your own hive metastore uris
        // that Gravitino can access
        .put("metastore.uris", "thrift://localhost:9083")
        .build();

Catalog catalog = gravitinoClient.createCatalog("catalog",
    Type.RELATIONAL,
    "hive", // provider, We support hive, jdbc-mysql, jdbc-postgresql, lakehouse-iceberg, lakehouse-paimon etc.
    "This is a hive catalog",
    hiveProperties);
```

</TabItem>
<TabItem value="python" label="Python">

```python
# This assumes that you have created a metalake named `metalake`
client = GravitinoClient(uri="http://localhost:8090", metalake_name="metalake")
client.create_catalog(name="catalog",
                      catalog_type=CatalogType.RELATIONAL,
                      provider="hive",
                      comment="This is a hive catalog",
                      properties={"metastore.uris": "thrift://localhost:9083"})
```
</TabItem>
</Tabs>

Currently, Gravitino supports the following catalog providers:

<table>
<thead>
<tr>
  <th>Catalog provider</th>
  <th>Catalog properties</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>hive</tt></td>
  <td>[Hive catalog property](../catalogs/relational/hive/index.md#catalog-properties)</td>
</tr>
<tr>
  <td><tt>jdbc-doris</tt></td>
  <td>[Doris catalog property](../catalogs/relational/jdbc/doris.md#catalog-properties)</td>
</tr>
<tr>
  <td><tt>jdbc-mysql</tt></td>
  <td>[MySQL catalog property](../catalogs/relational/jdbc/mysql.md#catalog-properties)</td>
</tr>
<tr>
  <td><tt>jdbc-oceanbase</tt></td>
  <td>[OceanBase catalog property](../catalogs/relational/jdbc/oceanbase.md#catalog-properties)</td>
</tr>
<tr>
  <td><tt>jdbc-postgresql</tt></td>
  <td>[PostgreSQL catalog property](../catalogs/relational/jdbc/postgresql.md#catalog-properties)</td>
</tr>
<tr>
  <td><tt>lakehouse-hudi</tt></td>
  <td>[Hudi catalog property](../catalogs/relational/lakehouse/hudi.md#catalog-properties)</td>
</tr>
<tr>
  <td><tt>lakehouse-iceberg</tt></td>
  <td>[Iceberg catalog property](../catalogs/relational/lakehouse/iceberg.md#catalog-properties)</td>
</tr>
<tr>
  <td>`lakehouse-paimon`</td>
  <td>[Paimon catalog property](../catalogs/relational/lakehouse/paimon.md#catalog-properties)</td>
</tr>
</tbody>
</table>

### Load a catalog

You can load a catalog by sending a `GET` request to the
`/api/metalakes/{metalake}/catalogs/{catalog}` endpoint or just use the Gravitino Java client.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/catalog
```

</TabItem>
<TabItem value="java" label="Java">

```java
// This assumes that you have a catalog named `catalog`
Catalog catalog = gravitinoClient.loadCatalog("catalog");
```

</TabItem>
<TabItem value="python" label="Python">

```python
# This assumes that you have a catalog named `catalog`
catalog = gravitino_client.load_catalog("catalog")
```
</TabItem>
</Tabs>

### Alter a catalog

You can modify a catalog by sending a `PUT` request to the
`/api/metalakes/{metalake}/catalogs/{catalog}` endpoint
or just use the Gravitino Java client.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X PUT \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{ \
  "updates": [{"@type": "rename", "newName": "alter_catalog"}, { \
      "@type": "setProperty", "property": "key3", "value": "value3"}] \
  }' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/catalog
```

</TabItem>
<TabItem value="java" label="Java">

```java
// This assumes that you have a catalog named `catalog`
Catalog catalog = gravitinoClient.alterCatalog(
    "catalog",
    CatalogChange.rename("alter_catalog"),
    CatalogChange.updateComment("new comment"));
```

</TabItem>
<TabItem value="python" label="Python">
```python
# This assumes that you have a catalog named `catalog`
changes = CatalogChange.update_comment("new comment")
catalog = client.alterCatalog("catalog", (changes))
```
</TabItem>
</Tabs>

Currently, Gravitino supports the following changes to a catalog:

<table>
<thead>
<tr>
  <th>Supported modification</th>
  <th>JSON</th>
  <th>Java</th>
</tr>
</thead>
<tbody>
<tr>
  <td>Rename catalog</td>
  <td>`{"@type":"rename","newName":"catalog_renamed"}`</td>
  <td>`CatalogChange.rename("catalog_renamed")`</td>
</tr>
<tr>
  <td>Update comment</td>
  <td>`{"@type":"updateComment","newComment":"new_comment"}`</td>
  <td>`CatalogChange.updateComment("new_comment")`</td>
</tr>
<tr>
  <td>Set a property</td>
  <td>`{"@type":"setProperty","property":"key1","value":"value1"}`</td>
  <td>`CatalogChange.setProperty("key1", "value1")`</td>
</tr>
<tr>
  <td>Remove a property</td>
  <td>`{"@type":"removeProperty","property":"key1"}`</td>
  <td>`CatalogChange.removeProperty("key1")`</td>
</tr>
</tbody>
</table>

:::warning

Most catalog-altering operations are generally safe.
However, if you want to change the catalog's URI, you should do it with caution.
Changing the URI may point to a different cluster, rendering the metadata stored in Gravitino unusable.
For instance, if the old URI and the new URI point to different clusters
that both have a database named *db1*, changing the URI might cause the old metadata,
such as audit information, to be used when accessing *db1*, which is undesirable.

The general suggestion is that to avoid changing the catalog's URI
unless you fully understand the consequences of such a modification.
:::

### Enable a catalog

Catalog has a reserved property - `inUse`, which indicates whether the catalog is available for use.
By default, the `inUse` property is set to `true` meaning that catalog is enabled.
To enable a disabled catalog, you can send a `PATCH` request to the
`/api/metalakes/{metalake}/catalogs/{catalog}` endpoint
or use the Gravitino Java client.

The following is an example of enabling a catalog:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X PATCH \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{"inUse": true}' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/catalog
```

</TabItem>
<TabItem value="java" label="Java">

```java
// This assumes that you have a catalog named `catalog`
gravitinoClient.enableCatalog("catalog");
```
</TabItem>
<TabItem value="python" label="Python">

```python
# This assumes that you have a catalog named `catalog`
client.enable_catalog("catalog")
```
</TabItem>
</Tabs>

:::info
This operation does nothing if the catalog is already enabled.
:::

### Disable a catalog

Once a catalog is disabled:

- Users can only [list](#list-all-catalogs-in-a-metalake), [load](#load-a-catalog),
  [drop](#drop-a-catalog), or [enable](#enable-a-catalog) it.
- Any other operations on the catalog or its sub-entities will result in an error.

To disable a catalog, you can send a `PATCH` request to the
`/api/metalakes/{metalake}/catalogs/{catalog}` endpoint
or use the Gravitino Java client.

The following is an example of disabling a catalog:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X PATCH \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{"inUse": false}' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/catalog
```

</TabItem>
<TabItem value="java" label="Java">

```java
// This assumes that you have a catalog named `catalog`
gravitinoClient.disableCatalog("catalog");
```

</TabItem>
<TabItem value="python" label="Python">

```python
# This assumes that you have a catalog named `catalog`
gravitino_client.disable_catalog("catalog")
```

</TabItem>
</Tabs>

:::info
This operation does nothing if the catalog is already disabled.
:::

### Drop a catalog

Dropping a catalog means deleting it **by force**, which is not the default behavior.
Before dropping a catalog, please make sure:

- There are no schemas under the catalog. Otherwise, you will get an error.
- The catalog is [disabled](#disable-a-catalog). Otherwise, you will get an error.

Dropping a catalog will:

- Delete all sub-entities (schemas, tables, etc.) under the catalog.
- Delete the catalog itself even if it is enabled.
- Not delete the external resources (such as database, table, etc.)
  associated with sub-entities unless they are managed (such as managed fileset).

You can remove a catalog by sending a `DELETE` request to the
`/api/metalakes/{metalake}/catalogs/{catalog}` endpoint
or just use the Gravitino Java client.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X DELETE \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog?force=false
```

</TabItem>
<TabItem value="java" label="Java">

```java
// The second parameter 'force' can be true or false
gravitinoClient.dropCatalog("mycatalog", false);
```
</TabItem>
<TabItem value="python" label="Python">

```python
# The 'force' keyword parameter can be true or false
client.drop_catalog(name="mycatalog", force=False)
```
</TabItem>
</Tabs>

### List all catalogs in a metalake

You can list all catalogs under a metalake by sending a `GET` request
to the `/api/metalakes/{metalake}/catalogs` endpoint
or just use the Gravitino Java client.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">
```shell
curl -X GET \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs
```

</TabItem>
<TabItem value="java" label="Java">

```java
String[] catalogNames = gravitinoClient.listCatalogs();
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalog_names = client.list_catalogs()
```

</TabItem>
</Tabs>

### List all catalogs' information in a metalake

You can list all catalogs' information under a metalake by sending a `GET` request
to the `/api/metalakes/{metalake}/catalogs?details=true` endpoint
or just use the Gravitino Java client.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs?details=true
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog[] catalogsInfos = gravitinoMetaLake.listCatalogsInfo();
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalogs_info = client.list_catalogs_info()
```
</TabItem>
</Tabs>


## Schema operations

:::tip
Before performing schema operations, you have to have a metalake and a catalog.
The metalake and the catalog have to be enabled.
:::

### Create a schema

You can create a schema by sending a `POST` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas` endpoint
or just use the Gravitino Java client.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{"name": "schema", "comment": "comment", "properties": {"key1": "value1"}}' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
// This assumes that you have a Hive catalog named `mycatalog`
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");

SupportsSchemas supportsSchemas = catalog.asSchemas();

Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    .build();
Schema schema = supportsSchemas.createSchema(
    "myschema",
    "This is a schema",
    schemaProperties
);
```

</TabItem>
<TabItem value="python" label="Python">

```python
client: GravitinoClient = GravitinoClient(uri="http://127.0.0.1:8090",
                                          metalake_name="mymetalake")
catalog: Catalog = client.load_catalog(name="mycatalog")
schema: Schema = catalog.as_schemas().create_schema(
  name="myschema", comment="This is a schema", properties={})
```
</TabItem>
</Tabs>

Currently, Gravitino supports the following schema property:

<table>
<thead>
<tr>
  <th>Catalog provider</th>
  <th>Schema properties</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>hive</tt></td>
  <td>[Hive schema property](../catalogs/relational/hive/index.md#schema-properties)</td>
</tr>
<tr>
  <td><tt>jdbc-doris</tt></td>
  <td>[Doris schema property](../catalogs/relational/jdbc/doris.md#schema-properties)</td>
</tr>
<tr>
  <td><tt>jdbc-mysql</tt></td>
  <td>[MySQL schema property](../catalogs/relational/jdbc/mysql.md#schema-properties)</td>
</tr>
<tr>
  <td><tt>jdbc-oceanbase</tt></td>
  <td>[OceanBase schema property](../catalogs/relational/jdbc/oceanbase.md#schema-properties)</td>
</tr>
<tr>
  <td><tt>jdbc-postgresql</tt></td>
  <td>[PostgreSQL schema property](../catalogs/relational/jdbc/postgresql.md#schema-properties)</td>
</tr>
<tr>
  <td><tt>lakehouse-hudi</tt></td>
  <td>[Hudi schema property](../catalogs/relational/lakehouse/hudi.md#schema-properties)</td>
</tr>
<tr>
  <td><tt>lakehouse-iceberg</tt></td>
  <td>[Iceberg schema property](../catalogs/relational/lakehouse/iceberg.md#schema-properties)</td>
</tr>
<tr>
  <td>`lakehouse-paimon`</td>
  <td>[Paimon schema property](../catalogs/relational/lakehouse/paimon.md#schema-properties)</td>
</tr>
</tbody>
</table>

### Load a schema

You can create a schema by sending a `GET` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}` endpoint
or just use the Gravitino Java client.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET \
  -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema
```

</TabItem>
<TabItem value="java" label="Java">

```java
// This assumes that you have a Hive catalog named `mycatalog`
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
Schema schema = catalog.asSchemas().loadSchema("myschema");
```

</TabItem>
<TabItem value="python" label="Python">
```python
client: GravitinoClient = GravitinoClient(
  uri="http://127.0.0.1:8090", metalake_name="mymetalake")
catalog: Catalog = client.load_catalog(name="mycatalog")
schema: Schema = catalog.as_schemas().load_schema(name="myschema")
```

</TabItem>
</Tabs>

### Alter a schema

You can change a schema by sending a `PUT` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}` endpoint
or just use the Gravitino Java client.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
cat <<EOF >patch.json
{
  "updates": [
    {
      "@type": "removeProperty",
      "property": "key2"
    },
    {
      "@type": "setProperty",
      "property": "key3",
      "value": "value3"
    }
  ]
}
EOF

curl -X PUT \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@patch.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema
```

</TabItem>
<TabItem value="java" label="Java">

```java
// This assumes that you have a Hive catalog named `mycatalog`,
// and a schema named `myschema`.
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
Schema schema = catalog.asSchemas().alterSchema("myschema",
    SchemaChange.removeProperty("key1"),
    SchemaChange.setProperty("key2", "value2"));
```
</TabItem>
<TabItem value="python" label="Python">

```python
client: GravitinoClient = GravitinoClient(
    uri="http://127.0.0.1:8090", metalake_name="mymetalake")
catalog: Catalog = client.load_catalog(name="mycatalog")

changes = (
    SchemaChange.remove_property("key1"),
    SchemaChange.set_property("key2", "new_value"),
)
schema_new: Schema = catalog.as_schemas().alter_schema("myschema", *changes)
```
</TabItem>
</Tabs>

Currently, Gravitino supports the following changes to a schema:

<table>
<thead>
<tr>
  <th>Supported modification</th>
  <th>JSON</th>
  <th>Java</th>
</tr>
</thead>
<tbody>
<tr>
  <td>Set a property</td>
  <td>`{"@type":"setProperty","property":"key1","value":"value1"}`</td>
  <td>`SchemaChange.setProperty("key1", "value1")`</td>
</tr>
<tr>
  <td>Remove a property</td>
  <td>`{"@type":"removeProperty","property":"key1"}`</td>
  <td>`SchemaChange.removeProperty("key1")`</td>
</tr>
</tbody>
</table>

### Drop a schema

You can remove a schema by sending a `DELETE` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}` endpoint
or just use the Gravitino Java client.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
// cascade can be true or false
curl -X DELETE \
  -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema?cascade=true
```

</TabItem>
<TabItem value="java" label="Java">

```java
// This assumes that you have a Hive catalog named `mycatalog`,
// and a schema named `myschema`.
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
SupportsSchemas supportsSchemas = catalog.asSchemas();
// The second parameter '`cascade' can be true or false
supportsSchemas.dropSchema("myschema", true);
```

</TabItem>
<TabItem value="python" label="Python">

```python
client: GravitinoClient = GravitinoClient(
    uri="http://127.0.0.1:8090", metalake_name="mymetalake")
catalog: Catalog = client.load_catalog(name="mycatalog")
catalog.as_schemas().drop_schema("myschema", cascade=True)
```

</TabItem>
</Tabs>

If `cascade` is true, Gravitino will drop all tables under the schema.
Otherwise, Gravitino will throw an exception if there are tables under the schema.
Some catalogs may not support cascading deletion for a schema,
please refer to the catalog specific documentation for more details.

### List all schemas

You can list all schemas under a catalog by sending a `GET` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas` endpoint
or just use the Gravitino Java client.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET \
  -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
// This assumes that you have a Hive catalog named `mycatalog`
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
String[] schemas = catalog.asSchemas().listSchemas();
```

</TabItem>
<TabItem value="python" label="Python">

```python
client: GravitinoClient = GravitinoClient(
    uri="http://127.0.0.1:8090", metalake_name="mymetalake")
catalog: Catalog = client.load_catalog(name="mycatalog")
schema_list: List[NameIdentifier] = catalog.as_schemas().list_schemas()
```

</TabItem>
</Tabs>

## Table operations

You should have created a metalake, a catalog and a schema before operating tables.
The metalake and the catalog have to be enabled.

### Create a table

You can create a table by sending a `POST` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables` endpoint
or just use the Gravitino Java client.

When creating a table, you need to provide the following information:

- Table column name and type
- Table column default value (optional)
- Table column auto-increment (optional)
- Table property (optional)

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
cat << EOF > table1.json
{
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
}
EOF

curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@table1.json` \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/tables
```

</TabItem>
<TabItem value="java" label="Java">

```java
// This assumes that you have a Hive catalog named `mycatalog`
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");

TableCatalog tableCatalog = catalog.asTableCatalog();

// This example is about creating a Hive table.
// Refer to the provider specific doc for table properties supported.
Map<String, String> tablePropertiesMap = ImmutableMap.<String, String>builder()
    .put("format", "ORC")
    .build();

tableCatalog.createTable(
  NameIdentifier.of("myschema", "mytable"),
  new Column[] {
    Column.of("id", Types.IntegerType.get(), "ID column comment",
              false, true, Literals.integerLiteral(-1)),
    Column.of("name", Types.VarCharType.of(500), "name column comment",
              true, false, Literals.NULL),
    Column.of("StartingDate", Types.TimestampType.withoutTimeZone(),
              "StartingDate column comment", false, false,
              Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP),
    Column.of("info", Types.StructType.of(
        Field.nullableField("position", Types.StringType.get(), "Position of the user"),
        Field.nullableField("contact", Types.ListType.of(Types.IntegerType.get(), false),
                            "contact field comment"),
        Field.nullableField("rating",
                            Types.MapType.of(Types.VarCharType.of(1000), Types.IntegerType.get(), false),
                            "rating field comment")
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

:::note
- The code above are examples about creating a Hive table.
  For other types of catalogs, the code is similar, but the column type supported
  and table properties may be different.
  For more details, please refer to the catalog-specific documentation.

- The example above demonstrates table creation but it isn't directly executable in Gravitino.
  The reason is that not all catalogs fully support these capabilities.
:::

#### Apache Gravitino table column type

Gravitino supports the following table column types:

<table>
<thead>
<tr>
  <th>Type</th>
  <th>JSON</th>
  <th>Java/Python</th>
  <th>Description</th>
</tr>
</thead>
<tbody>
<tr>
  <td>Binary</td>
  <td>`binary`</td>
  <td>`Types.BinaryType.get()`</td>
  <td>An arbitrary-length binary array.</td>
</tr>
<tr>
  <td>Boolean</td>
  <td>`boolean`</td>
  <td>`Types.BooleanType.get()`</td>
  <td>Boolean type</td>
</tr>
<tr>
  <td>Byte</td>
  <td>`byte`</td>
  <td>`Types.ByteType.get()`</td>
  <td>A numerical value of 1 byte.</td>
</tr>
<tr>
  <td>Date</td>
  <td>`date`</td>
  <td>`Types.DateType.get()`</td>
  <td>Date type</td>
</tr>
<tr>
  <td>Decimal(precision, scale)</td>
  <td>`decimal(p, s)`</td>
  <td>`Types.DecimalType.of(precision, scale)`</td>
  <td>A fixed-precision decimal number with the constraint that the precision must be in range `[1, 38]`
      and the scala must be in range `[0, precision]`.</td>
</tr>
<tr>
  <td>Double</td>
  <td>`double`</td>
  <td>`Types.DoubleType.get()`</td>
  <td>A double-precision floating-point number.</td>
</tr>
<tr>
  <td>Fixed(length)</td>
  <td>`fixed(l)`</td>
  <td>`Types.FixedType.of(length)`</td>
  <td>A fixed-length binary array.</td>
</tr>
<tr>
  <td>FixedChar(length)</td>
  <td>`char(l)`</td>
  <td>`Types.FixedCharType.of(length)`</td>
  <td>A fixed-length string.</td>
</tr>
<tr>
  <td>Float</td>
  <td>`float`</td>
  <td>`Types.FloatType.get()`</td>
  <td>A single-precision floating-point number.</td>
</tr>
<tr>
  <td>Integer</td>
  <td>`integer`</td>
  <td>`Types.IntegerType.get()`</td>
  <td>A numerical value of 4 bytes.</td>
</tr>
<tr>
  <td>IntervalToDateTime</td>
  <td>`interval_day`</td>
  <td>`Types.IntervalDayType.get()`</td>
  <td>An interval of date and time.</td>
</tr>
<tr>
  <td>IntervalToYearMonth</td>
  <td>`interval_year`</td>
  <td>`Types.IntervalYearType.get()`</td>
  <td>An interval of year and month.</td>
</tr>
<tr>
  <td>List</td>
  <td>`{"type": "list", "containsNull": true/false, "elementType": JSON}`</td>
  <td>`Types.ListType.of(elementType, elementNullable)`</td>
  <td>A list of elements with the same type.</td>
</tr>
<tr>
  <td>Long</td>
  <td>`long`</td>
  <td>`Types.LongType.get()`</td>
  <td>A numeric value of 8 bytes.</td>
</tr>
<tr>
  <td>Map</td>
  <td>`{"type": "map", "keyType": JSON, "valueType": JSON, "valueContainsNull": true/false}`</td>
  <td>`Types.MapType.of(keyType, valueType)`</td>
  <td>A map of key-value pairs.</td>
</tr>
<tr>
  <td>Short</td>
  <td>`short`</td>
  <td>`Types.ShortType.get()`</td>
  <td>A numerical value of 2 bytes.</td>
</tr>
<tr>
  <td>String</td>
  <td>`string`</td>
  <td>`Types.StringType.get()`</td>
  <td>A string.</td>
</tr>
<tr>
  <td>Struct</td>
  <td>`{"type": "struct", "fields": [Struct, {"name": string, "type": JSON, "nullable": true/false, "comment": string}]}`</td>
  <td>`Types.StructType.of([Types.StructType.Field.of(name, type, nullable)])`</td>
  <td>A struct of fields.</td>
</tr>
<tr>
  <td>Time</td>
  <td>`time`</td>
  <td>`Types.TimeType.withoutTimeZone()`</td>
  <td>Time type, without time zone information.</td>
</tr>
<tr>
  <td>Timestamp</td>
  <td>`timestamp`</td>
  <td>`Types.TimestampType.withoutTimeZone()`</td>
  <td>A timestamp without time zone information.</td>
</tr>
<tr>
  <td>TimestampWithTimezone</td>
  <td>`timestamp_tz`</td>
  <td>`Types.TimestampType.withTimeZone()`</td>
  <td>A timestamp with time zone information.</td>
</tr>
<tr>
  <td>Unsigned Byte</td>
  <td>`byte unsigned`</td>
  <td>`Types.ByteType.unsigned()`</td>
  <td>An unsigned numerical value of 1 byte.</td>
</tr>
<tr>
  <td>Unsigned Integer</td>
  <td>`integer unsigned`</td>
  <td>`Types.IntegerType.unsigned()`</td>
  <td>An unsigned numerical value of 4 bytes.</td>
</tr>
<tr>
  <td>Unsigned Long</td>
  <td>`long unsigned`</td>
  <td>`Types.LongType.unsigned()`</td>
  <td>An unsigned numerical value of 8 bytes.</td>
</tr>
<tr>
  <td>Unsigned Short</td>
  <td>`short unsigned`</td>
  <td>`Types.ShortType.unsigned()`</td>
  <td>An unsigned numerical value of 4 bytes.</td>
</tr>
<tr>
  <td>Union</td>
  <td>`{"type": "union", "types": [JSON, ...]}`</td>
  <td>`Types.UnionType.of([type1, type2, ...])`</td>
  <td>A union of two or more types.</td>
</tr>
<tr>
  <td>UUID</td>
  <td>`uuid`</td>
  <td>`Types.UUIDType.get()`</td>
  <td>An universially unique identifier.</td>
</tr>
<tr>
  <td>VarChar(length)</td>
  <td>`varchar(l)`</td>
  <td>`Types.VarCharType.of(length)`</td>
  <td>A string with an variable length; the length is the maximum length of the string.</td>
</tr>
</tbody>
</table>

The related java doc is [here](pathname:///docs/0.9.0-incubating-SNAPSHOT/api/java/org/apache/gravitino/rel/types/Type.html).

##### External type

When you need a data type that is not in the Gravitino type system,
and you explicitly know its string representation in an external catalog
(usually used in JDBC catalogs), you can use an ExternalType for such a column.
If the original type is unsolvable, it will be represented by ExternalType.
The following shows the data structure of an external type in JSON and Java,
enabling easy retrieval of its string value.

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

*Unparsed type* is another special column type.
It is used to address compatibility issues in type serialization and deserialization
when data traverse between the server and the client.
For instance, when a new column type is introduced on the Gravitino server,
if the client cannot recognize it, it will be treated as an unparsed type.
The following shows the data structure of an unparsed type in JSON and Java,
enabling easy retrieval of its value.

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

When defining a table column, you can specify a [literal](../expression.md#literal)
or an [expression](../expression.md) as its default value.
The default value typically applies to new rows that are inserted into the table by the underlying catalog.

The following table is a summary of the support to column default value
in Gravitino regarding different catalogs:

<table>
<thead>
<tr>
  <th>Catalog provider</th>
  <th>Default value supported?</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>hive</tt></td>
  <td>&#10008;</td>
</tr>
<tr>
  <td><tt>jdbc-doris</tt></td>
  <td>&#10004;</td>
</tr>
<tr>
  <td><tt>jdbc-mysql</tt></td>
  <td>&#10004;</td>
</tr>
<tr>
  <td><tt>jdbc-oceanbase</tt></td>
  <td>&#10004;</td>
</tr>
<tr>
  <td><tt>jdbc-postgresql</tt></td>
  <td>&#10004;</td>
</tr>
<tr>
  <td><tt>lakehouse-hudi</tt></td>
  <td>&#10008;</td>
</tr>
<tr>
  <td><tt>lakehouse-iceberg</tt></td>
  <td>&#10008;</td>
</tr>
<tr>
  <td><tt>lakehouse-paimon</tt></td>
  <td>&#10008;</td>
</tr>
</tbody>
</table>

#### Table column auto-increment

Auto-increment provides a convenient way to ensure that
each row in a table has a unique value without the need
for manually assigning a value.
This is very useful for generating unique identifier for records.
The table below summarizes the support status for column auto-increment
in Gravitino regarding different catalogs:

<table>
<thead>
<tr>
  <th>Catalog provider</th>
  <th>Auto-increment supported?</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>hive</tt></td>
  <td>&#10008;</td>
</tr>
<tr>
  <td><tt>jdbc-doris</tt></td>
  <td>&#10008;</td>
</tr>
<tr>
  <td><tt>jdbc-mysql</tt></td>
  <td>&#10004; ([limitations](../catalogs/relational/jdbc/mysql.md#table-column-auto-increment))</td>
</tr>
<tr>
  <td><tt>jdbc-oceanbase</tt></td>
  <td>&#10004; ([limitations](../catalogs/relational/jdbc/oceanbase.md#table-column-auto-increment))</td>
</tr>
<tr>
  <td><tt>jdbc-postgresql</tt></td>
  <td>&#10004;</td>
</tr>
<tr>
  <td><tt>lakehouse-hudi</tt></td>
  <td>&#10008;</td>
</tr>
<tr>
  <td><tt>lakehouse-iceberg</tt></td>
  <td>&#10008;</td>
</tr>
<tr>
  <td><tt>lakehouse-paimon</tt></td>
  <td>&#10008;</td>
</tr>
</tbody>
</table>

#### Table property and type mapping

The support status for table property in Gravitino is as follows:

<table>
<thead>
<tr>
  <th>Catalog provider</th>
  <th>Table property</th>
  <th>Type mapping</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>hive</tt></td>
  <td>[Link](../catalogs/relational/hive/index.md#table-properties)</td>
  <td>[link](../catalogs/relational/hive/index.md#table-column-types)</td>
</tr>
<tr>
  <td><tt>jdbc-doris</tt></td>
  <td>[Link](../catalogs/relational/jdbc/doris.md#table-properties)</td>
  <td>[Link](../catalogs/relational/jdbc/doris.md#table-column-types)</td>
</tr>
<tr>
  <td><tt>jdbc-mysql</tt></td>
  <td>[Link](../catalogs/relational/jdbc/mysql.md#table-properties)</td>
  <td>[Link](../catalogs/relational/jdbc/mysql.md#table-column-types)</td>
</tr>
<tr>
  <td><tt>jdbc-oceanbase</tt></td>
  <td>[Link](../catalogs/relational/jdbc/oceanbase.md#table-properties)</td>
  <td>[Link](../catalogs/relational/jdbc/oceanbase.md#table-column-types)</td>
</tr>
<tr>
  <td><tt>jdbc-postgresql</tt></td>
  <td>[Link](../catalogs/relational/jdbc/postgresql.md#table-properties)</td>
  <td>[Link](../catalogs/relational/jdbc/postgresql.md#table-column-types)</td>
</tr>
<tr>
  <td><tt>lakehouse-hudi</tt></td>
  <td>[Link](../catalogs/relational/lakehouse/hudi.md#table-properties)</td>
  <td>[Link](../catalogs/relational/lakehouse/hudi.md#table-column-types)</td>
</tr>
<tr>
  <td><tt>lakehouse-iceberg</tt></td>
  <td>[Link](../catalogs/relational/lakehouse/iceberg.md#table-properties)</td>
  <td>[Link](../catalogs/relational/lakehouse/iceberg.md#table-column-types)</td>
</tr>
<tr>
  <td><tt>lakehouse-paimon</tt></td>
  <td>[Link](../catalogs/relational/lakehouse/paimon.md#table-properties)</td>
  <td>[Link](../catalogs/relational/lakehouse/paimon.md#table-column-types)</td>
</tr>
</tbody>
</table>

#### Table partitioning, distribution, sort ordering and indexes

In addition to the basic settings, Gravitino supports the following features:

- **Table partitioning**:

  Similar to `PARTITION BY` in Apache Hive.
  It is a partitioning strategy that is used to split a table into parts based on partition keys.
  Some table engines may not support this feature.

  See [Partition](pathname:///docs/0.9.0-incubating-SNAPSHOT/api/java/org/apache/gravitino/dto/rel/partitioning/Partitioning.html)

- **Table distribution**:

  Similar to `CLUSTERED BY` in Apache Hive.
  Distribution a.k.a (Clustering) is a technique to split the data into more manageable files/parts
  (By specifying the number of buckets to create).
  The value of the distribution column will be hashed by a user-defined number into buckets.

  See [Distribution](pathname:///docs/0.9.0-incubating-SNAPSHOT/api/java/org/apache/gravitino/rel/expressions/distributions/Distribution.html)

- **Table sort ordering**:

  Equivalent to `SORTED BY` in Apache Hive.
  Sort ordering is about sorting data in ways such as by a column or a function.
  It will highly improve the query performance under certain scenarios.

  See [SortOrder](pathname:///docs/0.9.0-incubating-SNAPSHOT/api/java/org/apache/gravitino/rel/expressions/sorts/SortOrder.html)

- **Table indice**:

  Equivalent to `KEY/INDEX` in MySQL.
  Unique key enforces uniqueness of values in one or more columns within a table.
  It ensures that no two rows have identical values in specified columns,
  thereby facilitating data integrity and enabling efficient data retrieval and manipulation operations.

  See [Index](pathname:///docs/0.9.0-incubating-SNAPSHOT/api/java/org/apache/gravitino/rel/indexes/Index.html)

For more information, please see the related document on
[partitioning, bucketing, sorting, and indexes](../table-partitioning-bucketing-sort-order-indexes.md).

### Load a table

You can load a table by sending a `GET` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{table}` endpoint
or just use the Gravitino Java client.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET \
  -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/tables/mytable
```

</TabItem>
<TabItem value="java" label="Java">

```java
// This assumes that you have a Hive catalog named `mycatalog`,
// a schema named `myschema` and a table named `mytable`.
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");

TableCatalog tableCatalog = catalog.asTableCatalog();
tableCatalog.loadTable(NameIdentifier.of("myschema", "mytable"));
```

</TabItem>
</Tabs>

:::note
- When Gravitino loads a table with various data types, if it is unable to parse the data type,
  it will use an **[External Type](#external-type)** to preserve the original data type,
  ensuring that the table can be loaded successfully.

- When Gravitino loads a table that supports default value, if it is unable to parse the default value,
  it will use an **[Unparsed Expression](../expression.md#unparsed-expression)**
  to preserve the original default value,
  ensuring that the table can be loaded successfully.
:::

### Alter a table

You can modify a table by sending a `PUT` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{table}` endpoint
or just use the Gravitino Java client.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
cat <<EOF >patch.json
{
  "updates": [
    {
      "@type": "removeProperty",
      "property": "key2"
    },
    {
      "@type": "setProperty",
      "property": "key3",
      "value": "value3"
    }
  ]
}
EOF

curl -X PUT \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@patch.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/tables/mytable
```

</TabItem>
<TabItem value="java" label="Java">

```java
// This assumes that you have a Hive catalog named `mycatalog`,
// a schema named `myschema` and a table named `mytable`.
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");

TableCatalog tableCatalog = catalog.asTableCatalog();

Table t = tableCatalog.alterTable(
    NameIdentifier.of("myschema", "mytable"),
    TableChange.rename("table_renamed"),
    TableChange.updateComment("xxx")
);

```

</TabItem>
</Tabs>

Currently, Gravitino supports the following changes to a table:

<table>
<thead>
<tr>
  <th>Supported modification</th>
  <th>JSON payload</th>
  <th>Java method</th>
</tr>
</thead>
<tbody>
<tr>
  <td>Rename table</td>
  <td>`{"@type":"rename","newName":"table_renamed"}`</td>
  <td>`TableChange.rename("table_renamed")`</td>
</tr>
<tr>
  <td>Update table comment</td>
  <td>`{"@type":"updateComment","newComment":"new_comment"}`</td>
  <td>`TableChange.updateComment("new_comment")`</td>
</tr>
<tr>
  <td>Set table property</td>
  <td>`{"@type":"setProperty","property":"key1","value":"value1"}`</td>
  <td>`TableChange.setProperty("key1", "value1")`</td>
</tr>
<tr>
  <td>Remove table property</td>
  <td>`{"@type":"removeProperty","property":"key1"}`</td>
  <td>`TableChange.removeProperty("key1")`</td>
</tr>
<tr>
  <td>Add column</td>
  <td>`{"@type":"addColumn","fieldName":["position"],"type":"varchar(20)","comment":"Position of user","position":"FIRST","nullable": true, "autoIncrement": false, "defaultValue" : {"type": "literal", "dataType": "varchar(20)", "value": "Default Position"}}`</td>
  <td>`TableChange.addColumn(...)`</td>
</tr>
<tr>
  <td>Delete column</td>
  <td>`{"@type":"deleteColumn","fieldName": ["name"], "ifExists": true}`</td>
  <td>`TableChange.deleteColumn(...)`</td>
</tr>
<tr>
  <td>Rename column</td>
  <td>`{"@type":"renameColumn","oldFieldName":["name_old"], "newFieldName":"name_new"}`</td>
  <td>`TableChange.renameColumn(...)`</td>
</tr>
<tr>
  <td>Update column comment</td>
  <td>`{"@type":"updateColumnComment", "fieldName": ["name"], "newComment": "new comment"}`</td>
  <td>`TableChange.updateColumnCommment(...)`</td>
</tr>
<tr>
  <td>Update column type</td>
  <td>`{"@type":"updateColumnType","fieldName": ["name"], "newType":"varchar(100)"}`</td>
  <td>`TableChange.updateColumnType(...)`</td>
</tr>
<tr>
  <td>Update column nullability</td>
  <td>`{"@type":"updateColumnNullability","fieldName": ["name"],"nullable":true}`</td>
  <td>`TableChange.updateColumnNullability(...)`</td>
</tr>
<tr>
  <td>Update column position</td>
  <td>`{"@type":"updateColumnPosition","fieldName": ["name"], "newPosition":"default"}`</td>
  <td>`TableChange.updateColumnPosition(...)`</td>
</tr>
<tr>
  <td>Update column default value</td>
  <td>`{"@type":"updateColumnDefaultValue","fieldName": ["name"], "newDefaultValue":{"type":"literal","dataType":"varchar(100)","value":"new default value}}`</td>
  <td>`TableChange.updateColumnDefaultValue(...)`</td>
</tr>
</tbody>
</table>

### Drop a table

You can remove a table by sending a `DELETE` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{table}` endpoint
or just use the Gravitino Java client.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
# The query string `purge` can be true or false.
# If `purge` is true, Gravitino will remove the data from the table.
curl -X DELETE \
  -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/tables/mytable?purge=true
```

</TabItem>
<TabItem value="java" label="Java">

```java
// This assumes that you have a Hive catalog named `mycatalog`,
// a schema named `myschema` and a table named `mytable`.
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
TableCatalog tableCatalog = catalog.asTableCatalog();
// Drop a table
tableCatalog.dropTable(NameIdentifier.of("myschema", "mytable"));
// Purge a table
tableCatalog.purgeTable(NameIdentifier.of("myschema", "mytable"));
```
</TabItem>
</Tabs>

There are two ways to remove a table: `dropTable` and `purgeTable`:

* `dropTable` removes both the metadata and the directory associated with the table
  from the file system if the table is not an external table.
  In the case of an external table, only the associated metadata is removed.

* `purgeTable` completely removes both the metadata and the directory associated with the table.
  If the table is an external table or the catalog doesn't support purging table,
  an exception `UnsupportedOperationException` is thrown.

The <tt>hive</tt> catalog and <tt>lakehouse-iceberg</tt> catalog supports `purgeTable`,
while <tt>jdbc-mysql</tt>, <tt>jdbc-postgresql</tt> and <tt>lakehouse-paimon</tt> catalog doesn't support it.

### List all tables under a schema

You can list all tables in a schema by sending a `GET` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables` endpoint
or just use the Gravitino Java client.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET \
  -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/tables
```

</TabItem>
<TabItem value="java" label="Java">

```java
// This assumes that you have a Hive catalog named `mycatalog`,
// a schema named `myschema` and some tables under that schema.
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");

TableCatalog tableCatalog = catalog.asTableCatalog();
NameIdentifier[] identifiers = tableCatalog.listTables(Namespace.of("myschema"));
```

</TabItem>
</Tabs>

