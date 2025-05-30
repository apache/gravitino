---
title: Manage model metadata using Gravitino
slug: /manage-model-metadata-using-gravitino
date: 2024-12-26
keyword: Gravitino model metadata manage
license: This software is licensed under the Apache License version 2.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This page introduces how to manage model metadata in Apache Gravitino. Gravitino model catalog
is a kind of model registry, which provides the ability to manage machine learning models'
versioned metadata. It follows the typical Gravitino 3-level namespace (catalog, schema, and
model) and supports managing the versions for each model.

Currently, it supports model and model version registering, listing, loading, and deleting.

To use the model catalog, please make sure that:

 - The Gravitino server has started, and is serving at, e.g. [http://localhost:8090](http://localhost:8090).
 - A metalake has been created and [enabled](./manage-metalake-using-gravitino.md#enable-a-metalake)

## Catalog operations

### Create a catalog

:::info
For a model catalog, you must specify the catalog `type` as `MODEL` when creating the catalog.
Please also be aware that the `provider` is not required for a model catalog.
:::

You can create a catalog by sending a `POST` request to the `/api/metalakes/{metalake_name}/catalogs`
endpoint or just use the Gravitino Java/Python client. The following is an example of creating a
catalog:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "model_catalog",
  "type": "MODEL",
  "comment": "This is a model catalog",
  "properties": {
    "k1": "v1"
  }
}' http://localhost:8090/api/metalakes/example/catalogs
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("example")
    .build();

Map<String, String> properties = ImmutableMap.<String, String>builder()
    .put("k1", "v1")
    .build();

Catalog catalog = gravitinoClient.createCatalog(
    "model_catalog",
    Type.MODEL,
    "This is a model catalog",
    properties);
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="example")
catalog = gravitino_client.create_catalog(name="model_catalog",
                                          catalog_type=Catalog.Type.MODEL,
                                          provider=None,
                                          comment="This is a model catalog",
                                          properties={"k1": "v1"})
```

</TabItem>
</Tabs>

### Load a catalog

Refer to [Load a catalog](./manage-relational-metadata-using-gravitino.md#load-a-catalog)
in relational catalog for more details. For a model catalog, the load operation is the same.

### Alter a catalog

Refer to [Alter a catalog](./manage-relational-metadata-using-gravitino.md#alter-a-catalog)
in relational catalog for more details. For a model catalog, the alter operation is the same.

### Drop a catalog

Refer to [Drop a catalog](./manage-relational-metadata-using-gravitino.md#drop-a-catalog)
in relational catalog for more details. For a model catalog, the drop operation is the same.

### List all catalogs in a metalake

Please refer to [List all catalogs in a metalake](./manage-relational-metadata-using-gravitino.md#list-all-catalogs-in-a-metalake)
in relational catalog for more details. For a model catalog, the list operation is the same.

### List all catalogs' information in a metalake

Please refer to [List all catalogs' information in a metalake](./manage-relational-metadata-using-gravitino.md#list-all-catalogs-information-in-a-metalake)
in relational catalog for more details. For a model catalog, the list operation is the same.

## Schema operations

`Schema` is a virtual namespace in a model catalog, which is used to organize the models. It
is similar to the concept of `schema` in the relational catalog.

:::tip
Users should create a metalake and a catalog before creating a schema.
:::

### Create a schema

You can create a schema by sending a `POST` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas`
endpoint or just use the Gravitino Java/Python client. The following is an example of creating a
schema:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "model_schema",
  "comment": "This is a model schema",
  "properties": {
    "k1": "v1"
  }
}' http://localhost:8090/api/metalakes/example/catalogs/model_catalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("example")
    .build();

Catalog catalog = gravitinoClient.loadCatalog("model_catalog");

SupportsSchemas supportsSchemas = catalog.asSchemas();

Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    .put("k1", "v1")
    .build();
Schema schema = supportsSchemas.createSchema(
    "model_schema",
    "This is a schema",
    schemaProperties);
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="example")

catalog: Catalog = gravitino_client.load_catalog(name="model_catalog")
catalog.as_schemas().create_schema(name="model_schema",
                                   comment="This is a schema",
                                   properties={"k1": "v1"})
```

</TabItem>
</Tabs>

### Load a schema

Please refer to [Load a schema](./manage-relational-metadata-using-gravitino.md#load-a-schema)
in relational catalog for more details. For a model catalog, the schema load operation is the
same.

### Alter a schema

Please refer to [Alter a schema](./manage-relational-metadata-using-gravitino.md#alter-a-schema)
in relational catalog for more details. For a model catalog, the schema alter operation is the
same.

### Drop a schema

Please refer to [Drop a schema](./manage-relational-metadata-using-gravitino.md#drop-a-schema)
in relational catalog for more details. For a model catalog, the schema drop operation is the
same.

Note that the drop operation will delete all the model metadata under this schema if `cascade`
set to `true`.

### List all schemas under a catalog

Please refer to [List all schemas under a catalog](./manage-relational-metadata-using-gravitino.md#list-all-schemas-under-a-catalog)
in relational catalog for more details. For a model catalog, the schema list operation is the
same.

## Model operations

:::tip
 - Users should create a metalake, a catalog, and a schema before creating a model.
:::

### Register a model

You can register a model by sending a `POST` request to the `/api/metalakes/{metalake_name}
/catalogs/{catalog_name}/schemas/{schema_name}/models` endpoint or just use the Gravitino
Java/Python client. The following is an example of creating a model:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "example_model",
  "comment": "This is an example model",
  "properties": {
    "k1": "v1"
  }
}' http://localhost:8090/api/metalakes/example/catalogs/model_catalog/schemas/model_schema/models
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("example")
    .build();

Catalog catalog = gravitinoClient.loadCatalog("model_catalog");
Map<String, String> propertiesMap = ImmutableMap.<String, String>builder()
        .put("k1", "v1")
        .build();

Model model = catalog.asModelCatalog().registerModel(
    NameIdentifier.of("model_schema", "example_model"),
    "This is an example model",
    propertiesMap);
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="example")

catalog: Catalog = gravitino_client.load_catalog(name="model_catalog")
model: Model = catalog.as_model_catalog().register_model(ident=NameIdentifier.of("model_schema", "example_model"),
                                                         comment="This is an example model",
                                                         properties={"k1": "v1"})
```

</TabItem>
</Tabs>

### Get a model

You can get a model by sending a `GET` request to the `/api/metalakes/{metalake_name}
/catalogs/{catalog_name}/schemas/{schema_name}/models/{model_name}` endpoint or by using the
Gravitino Java/Python client. The following is an example of getting a model:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/example/catalogs/model_catalog/schemas/model_schema/models/example_model
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
Catalog catalog = gravitinoClient.loadCatalog("model_catalog");
Model model = catalog.asModelCatalog().getModel(NameIdentifier.of("model_schema", "example_model"));
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="example")

catalog: Catalog = gravitino_client.load_catalog(name="model_catalog")
model: Model = catalog.as_model_catalog().get_model(ident=NameIdentifier.of("model_schema", "example_model"))
```

</TabItem>
</Tabs>

### Alter a model

You can modify a model's metadata (e.g. rename, update comment, or modify properties) by 
sending a `PUT` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/
{schema_name}/models/{model_name}` endpoint or using the Gravitino Java/Python client. The following is an example of modifying a model:

<Tabs groupId="language" queryString>
 <TabItem value="shell" label="Shell">

```shell
cat <<EOF >model.json
{
  "updates": [
    {
      "@type": "updateComment",
      "newComment": "Updated model comment"
    },
    {
      "@type": "rename",
      "newName": "new_name"
    },
    {
      "@type": "setProperty",
      "property": "k2",
      "value": "v2"
    },
    {
      "@type": "removeProperty",
      "property": "k1"
    }
  ]
}
EOF
 
curl -X PUT \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@model.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/models/mymodel
 ```


 </TabItem>
 <TabItem value="java" label="Java">

 ```java
 // Load the catalog and model
 GravitinoClient gravitinoClient = GravitinoClient
     .builder("http://localhost:8090")
     .withMetalake("example")
     .build();

 Catalog catalog = gravitinoClient.loadCatalog("model_catalog");
 ModelCatalog modelCatalog = catalog.asModelCatalog();

 // Define modifications
 ModelChange[] changes = {
     ModelChange.rename("example_model_renamed"),
     ModelChange.updatComment("new comment"),
     ModelChange.setProperty("k2", "v2"),
     ModelChange.removeProperty("k1")
 };

 // Apply changes
 Model updatedModel = modelCatalog.alterModel(
     NameIdentifier.of("model_schema", "example_model"),
     changes
 );
 ```

 </TabItem>
<TabItem value="python" label="Python">

 ```python
client = GravitinoClient(uri="http://localhost:8090", 
                         metalake_name="mymetalake")

catalog = client.load_catalog(name="mycatalog").as_model_catalog()

# Define modifications
changes = (
    ModelChange.rename("renamed"),
    ModelChange.update_comment("new comment"),
    ModelChange.set_property("k2", "v2"),
    ModelChange.remove_property("k1"),
)

# Apply changes
updated_model = model_catalog.alter_model(
    ident=NameIdentifier.of("myschema", "mymodel"), *changes
)
 ```
 </TabItem>
 </Tabs>

#### Supported modifications

The following operations are supported for altering a model:


| Operation           | JSON Example                                               | Java Method                                | Python Method                               |
 |---------------------|------------------------------------------------------------|--------------------------------------------|---------------------------------------------|
| **Rename model**    | `{"@type":"rename","newName":"new_name"}`                  | `ModelChange.rename("new_name")`           | `ModelChange.rename("new_name")`            |
| **Update comment**  | `{"@type":"updateComment","newComment":"new comment"}`     | `ModelChange.updateComment("new comment")` | `ModelChange.update_comment("new comment")` |
| **Set property**    | `{"@type":"setProperty","property":"key","value":"value"}` | `ModelChange.setProperty("key", "value")`  | `ModelChange.set_property("key", "value")`  |
| **Remove property** | `{"@type":"removeProperty","property":"key"}`              | `ModelChange.removeProperty("key")`        | `ModelChange.remove_property("key")`        |

:::note
- Multiple modifications can be applied in a single request.
- If the target model does not exist, a `404 Not Found` error will be returned.
  :::

### Delete a model

You can delete a model by sending a `DELETE` request to the `/api/metalakes/{metalake_name}
/catalogs/{catalog_name}/schemas/{schema_name}/models/{model_name}` endpoint or by using the
Gravitino Java/Python client. The following is an example of deleting a model:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/example/catalogs/model_catalog/schemas/model_schema/models/example_model
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
Catalog catalog = gravitinoClient.loadCatalog("model_catalog");
catalog.asModelCatalog().deleteModel(NameIdentifier.of("model_schema", "example_model"));
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="example")

catalog: Catalog = gravitino_client.load_catalog(name="model_catalog")
catalog.as_model_catalog().delete_model(NameIdentifier.of("model_schema", "example_model"))
```

</TabItem>
</Tabs>

Note that the delete operation will delete all the model versions under this model.

### List models

You can list all the models in a schema by sending a `GET` request to the `/api/metalakes/
{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/models` endpoint or by using the
Gravitino Java/Python client. The following is an example of listing all the models in a schema:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/example/catalogs/model_catalog/schemas/model_schema/models
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
Catalog catalog = gravitinoClient.loadCatalog("model_catalog");
NameIdentifier[] identifiers = catalog.asModelCatalog().listModels(Namespace.of("model_schema"));
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="example")

catalog: Catalog = gravitino_client.load_catalog(name="model_catalog")
model_list = catalog.as_model_catalog().list_models(namespace=Namespace.of("model_schema")))
```

</TabItem>
</Tabs>

## Model Version operations

:::tip
 - Users should create a metalake, a catalog, a schema, and a model before link a model version
   to the model.
:::

### Link a ModelVersion

You can link a ModelVersion by sending a `POST` request to the `/api/metalakes/{metalake_name}
/catalogs/{catalog_name}/schemas/{schema_name}/models/{model_name}/versions` endpoint or by using
the Gravitino Java/Python client. The following is an example of linking a ModelVersion:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "uri": "path/to/model",
  "aliases": ["alias1", "alias2"],
  "comment": "This is version 0",
  "properties": {
    "k1": "v1"
  }
}' http://localhost:8090/api/metalakes/example/catalogs/model_catalog/schemas/model_schema/models/example_model/versions
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
Catalog catalog = gravitinoClient.loadCatalog("model_catalog");
catalog.asModelCatalog().linkModelVersion(
    NameIdentifier.of("model_schema", "example_model"),
    "path/to/model",
    new String[] {"alias1", "alias2"},
    "This is version 0",
    ImmutableMap.of("k1", "v1"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="example")

catalog: Catalog = gravitino_client.load_catalog(name="model_catalog")
catalog.as_model_catalog().link_model_version(model_ident=NameIdentifier.of("model_schema", "example_model"),
                                              uri="path/to/model",
                                              aliases=["alias1", "alias2"],
                                              comment="This is version 0",
                                              properties={"k1": "v1"})
```

</TabItem>
</Tabs>

The comment and properties of ModelVersion can be different from the model.

### Get a ModelVersion

You can get a ModelVersion by sending a `GET` request to the `/api/metalakes/{metalake_name}
/catalogs/{catalog_name}/schemas/{schema_name}/models/{model_name}/versions/{version_number}`
endpoint or by using the Gravitino Java/Python client. The following is an example of getting
a ModelVersion:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/example/catalogs/model_catalog/schemas/model_schema/models/example_model/versions/0
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
Catalog catalog = gravitinoClient.loadCatalog("model_catalog");
catalog.asModelCatalog().getModelVersion(NameIdentifier.of("model_schema", "example_model"), 0);
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="example")

catalog: Catalog = gravitino_client.load_catalog(name="model_catalog")
catalog.as_model_catalog().get_model_version(model_ident=NameIdentifier.of("model_schema", "example_model"), version=0)
```

</TabItem>
</Tabs>

### Get a ModelVersion by alias

You can also get a ModelVersion by sending a `GET` request to the `/api/metalakes/{metalake_name}
/catalogs/{catalog_name}/schemas/{schema_name}/models/{model_name}/aliases/{alias}` endpoint or
by using the Gravitino Java/Python client. The following is an example of getting a ModelVersion
by alias:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/example/catalogs/model_catalog/schemas/model_schema/models/example_model/aliases/alias1
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
Catalog catalog = gravitinoClient.loadCatalog("model_catalog");
ModelVersion modelVersion = catalog.asModelCatalog().getModelVersion(NameIdentifier.of("model_schema", "example_model"), "alias1");
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="example")

catalog: Catalog = gravitino_client.load_catalog(name="model_catalog")
model_version: ModelVersion = catalog.as_model_catalog().get_model_version_by_alias(model_ident=NameIdentifier.of("model_schema", "example_model"), alias="alias1")
```

</TabItem>
</Tabs>

### Alter a ModelVersion

You can modify a modelVersion's metadata (e.g. update uri, update comment, or modify properties) 
by sending a `PUT` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}
/schemas/{schema_name} /models/{model_name}/versions/{version_number}` endpoint or using the Gravitino 
Java/Python client. The following is an example of modifying a model version:

<Tabs groupId="language" queryString>
 <TabItem value="shell" label="Shell">

```shell
cat <<EOF >model.json
{
  "updates": [
    {
      "@type": "updateComment",
      "newComment": "Updated comment of model version"
    },
    {
      "@type": "updateUri",
      "newUri": "new_uri"
    },
    {
      "@type": "setProperty",
      "property": "key",
      "value": "value"
    },
    {
      "@type": "removeProperty",
      "property": "key"
    },
    {
      "@type": "updateAliases",
      "aliasesToAdd": [
          "alias1",
          "alias2"
      ],
      "aliasesToRemove": [
          "alias3"
      ]
    }
  ]
}
EOF
 
curl -X PUT \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@model.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/models/mymodel/versions/0
```
</TabItem>
<TabItem value="java" label="Java">

```java
// Load the model catalog
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("example")
    .build();

Catalog catalog = gravitinoClient.loadCatalog("model_catalog");
ModelCatalog modelCatalog = catalog.asModelCatalog();

// Define modifications
ModelVersionChange[] changes = {
     ModelVersionChange.updateComment("Updated comment of model version"),
     ModelVersionChange.updateUri("new_uri"),
     ModelVersionChange.setProperty("key", "value"),
     ModelVersionChange.removeProperty("key"),
     ModelVersionChange.updateAliases(new String[] {"alias1", "alias2"}, new String[] {"alias3"})
 };

// Apply changes
ModelVersion updatedModelVersion = modelCatalog.alterModelVersion(
     NameIdentifier.of("model_schema", "example_model"),
     0,
     changes
 );
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(
    uri="http://localhost:8090", metalake_name="mymetalake"
)

# Load Model Catalog
model_catalog = client.load_catalog(name="mycatalog").as_model_catalog()

# Define modifications
changes = (
    ModelVersionChange.update_comment("Updated comment of model version"),
    ModelVersionChange.update_uri("new_uri"),
    ModelVersionChange.set_property("k2", "v2"),
    ModelVersionChange.remove_property("k1"),
    ModelVersionChange.update_aliases(["alias1", "alias2"], ["alias3"])
)

# Apply changes
updated_model = model_catalog.alter_model_version(
    NameIdentifier.of("myschema", "mymodel"), 0, *changes
)
```

</TabItem>
</Tabs>

#### Supported modifications

| Operation           | JSON Example                                                                                   | Java Method                                                                                    | Python Method                                                         |
|---------------------|------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| **Update uri**      | `{"@type":"updateUri","newName":"new_uri"}`                                                    | `ModelVersionChange.updateUri("new_uri")`                                                      | `ModelVersionChange.update_uri("new_uri")`                            |
| **Update comment**  | `{"@type":"updateComment","newComment":"new_comment"}`                                         | `ModelVersionChange.updateComment("new_comment")`                                              | `ModelVersionChange.update_comment("new_comment")`                    |
| **Set property**    | `{"@type":"setProperty","property":"key","value":"value"}`                                     | `ModelVersionChange.setProperty("key", "value")`                                               | `ModelVersionChange.set_property("key", "value")`                     |
| **Remove property** | `{"@type":"removeProperty","property":"key"}`                                                  | `ModelVersionChange.removeProperty("key")`                                                     | `ModelVersionChange.remove_property("key")`                           |
| **Update Aliases**  | `{"@type": "updateAliases","aliasesToAdd": ["alias1","alias2"],"aliasesToRemove": ["alias3"]}` | `ModelVersionChange.updateAliases(new String[] {"alias1", "alias2"}, new String[] {"alias3"})` | `ModelVersionChange.update_aliases(["alias1", "alias2"], ["alias3"])` |

:::note
- Multiple modifications can be applied in a single request.
- If the target model does not exist, a `404 Not Found` error will be returned.
- If the target model version does not exist, a `404 Not Found` error will be returned.
  :::

### Alter a ModelVersion by alias

You can also modify a modelVersion's metadata (e.g. update uri, update comment, or modify 
properties) by sending a `PUT` request to the `/api/metalakes/{metalake_name}/catalogs/
{catalog_name}/schemas/{schema_name}/models/{model_name}/aliases/{alias}` endpoint or using the Gravitino
Java/Python client. The following is an example of modifying a model version:

<Tabs groupId="language" queryString>
 <TabItem value="shell" label="Shell">

```shell
cat <<EOF >model.json
{
  "updates": [
    {
      "@type": "updateComment",
      "newComment": "Updated comment of model version"
    },
    {
      "@type": "updateUri",
      "newUri": "new_uri"
    },
    {
      "@type": "setProperty",
      "property": "key",
      "value": "value"
    },
    {
      "@type": "removeProperty",
      "property": "key"
    },
    {
      "@type": "updateAliases",
      "aliasesToAdd": [
          "alias1",
          "alias2"
      ],
      "aliasesToRemove": [
          "alias3"
      ]
    }
  ]
}
EOF
 
curl -X PUT \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@model.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/models/mymodel/aliases/myalias
```
</TabItem>
<TabItem value="java" label="Java">

```java
// Load the model catalog
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("example")
    .build();

Catalog catalog = gravitinoClient.loadCatalog("model_catalog");
ModelCatalog modelCatalog = catalog.asModelCatalog();

// Define modifications
ModelVersionChange[] changes = {
     ModelVersionChange.updateComment("Updated comment of model version"),
     ModelVersionChange.updateUri("new_uri"),
     ModelVersionChange.setProperty("key", "value"),
     ModelVersionChange.removeProperty("key"),
     ModelVersionChange.updateAliases(new String[] {"alias1", "alias2"}, new String[] {"alias3"})
 };

// Apply changes
ModelVersion updatedModelVersion = modelCatalog.alterModelVersion(
     NameIdentifier.of("model_schema", "example_model"),
     "myalias",
     changes
 );
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(
    uri="http://localhost:8090", metalake_name="mymetalake"
)

# Load Model Catalog
model_catalog = client.load_catalog(name="mycatalog").as_model_catalog()

# Define modifications
changes = (
    ModelVersionChange.update_comment("Updated comment of model version"),
    ModelVersionChange.update_uri("new_uri"),
    ModelVersionChange.set_property("k2", "v2"),
    ModelVersionChange.remove_property("k1"),
    ModelVersionChange.update_aliases(["alias1", "alias2"], ["alias3"])
)

# Apply changes
updated_model = model_catalog.alter_model_version_by_alias(
    NameIdentifier.of("myschema", "myalias", "mymodel"), *changes
)
```

</TabItem>
</Tabs>

#### Supported modifications

| Operation           | JSON Example                                                                                   | Java Method                                                                                    | Python Method                                                         |
|---------------------|------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| **Update uri**      | `{"@type":"updateUri","newName":"new_uri"}`                                                    | `ModelVersionChange.updateUri("new_uri")`                                                      | `ModelVersionChange.update_uri("new_uri")`                            |
| **Update comment**  | `{"@type":"updateComment","newComment":"new_comment"}`                                         | `ModelVersionChange.updateComment("new_comment")`                                              | `ModelVersionChange.update_comment("new_comment")`                    |
| **Set property**    | `{"@type":"setProperty","property":"key","value":"value"}`                                     | `ModelVersionChange.setProperty("key", "value")`                                               | `ModelVersionChange.set_property("key", "value")`                     |
| **Remove property** | `{"@type":"removeProperty","property":"key"}`                                                  | `ModelVersionChange.removeProperty("key")`                                                     | `ModelVersionChange.remove_property("key")`                           |
| **Update Aliases**  | `{"@type": "updateAliases","aliasesToAdd": ["alias1","alias2"],"aliasesToRemove": ["alias3"]}` | `ModelVersionChange.updateAliases(new String[] {"alias1", "alias2"}, new String[] {"alias3"})` | `ModelVersionChange.update_aliases(["alias1", "alias2"], ["alias3"])` |


:::note
- Multiple modifications can be applied in a single request.
- If the target model does not exist, a `404 Not Found` error will be returned.
- If the target model version does not exist, a `404 Not Found` error will be returned.
  :::

### Delete a ModelVersion

You can delete a ModelVersion by sending a `DELETE` request to the `/api/metalakes/{metalake_name}
/catalogs/{catalog_name}/schemas/{schema_name}/models/{model_name}/versions/{version_number}`
endpoint or by using the Gravitino Java/Python client. The following is an example of deleting
a ModelVersion:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/example/catalogs/model_catalog/schemas/model_schema/models/example_model/versions/0
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
Catalog catalog = gravitinoClient.loadCatalog("model_catalog");
catalog.asModelCatalog().deleteModelVersion(NameIdentifier.of("model_schema", "example_model"), 0);
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="example")

catalog: Catalog = gravitino_client.load_catalog(name="model_catalog")
catalog.as_model_catalog().delete_model_version(model_ident=NameIdentifier.of("model_schema", "example_model"), version=0)
```

</TabItem>
</Tabs>

### Delete a ModelVersion by alias

You can also delete a ModelVersion by sending a `DELETE` request to the `/api/metalakes/
{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/models/{model_name}/aliases/{alias}` endpoint or
by using the Gravitino Java/Python client. The following is an example of deleting a ModelVersion
by alias:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/example/catalogs/model_catalog/schemas/model_schema/models/example_model/aliases/alias1
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
Catalog catalog = gravitinoClient.loadCatalog("model_catalog");
catalog.asModelCatalog().deleteModelVersion(NameIdentifier.of("model_schema", "example_model"), "alias1");
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="example")

catalog: Catalog = gravitino_client.load_catalog(name="model_catalog")
catalog.as_model_catalog().delete_model_version_by_alias(model_ident=NameIdentifier.of("model_schema", "example_model"), alias="alias1")
```

</TabItem>
</Tabs>

### List ModelVersions

You can list all the ModelVersions in a model by sending a `GET` request to the `/api/metalakes/
{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/models/{model_name}/versions` endpoint
or by using the Gravitino Java/Python client. The following is an example of listing all the 
ModelVersions in a model:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/example/catalogs/model_catalog/schemas/model_schema/models/example_model/versions
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
Catalog catalog = gravitinoClient.loadCatalog("model_catalog");
int[] modelVersions = catalog.asModelCatalog().listModelVersions(NameIdentifier.of("model_schema", "example_model"));
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="example")

catalog: Catalog = gravitino_client.load_catalog(name="model_catalog")
model_versions: List[int] = catalog.as_model_catalog().list_model_versions(model_ident=NameIdentifier.of("model_schema", "example_model"))
```

</TabItem>
</Tabs>
