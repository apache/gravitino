---
title: Manage model metadata
slug: /manage-model-metadata
date: 2024-12-26
keyword: Gravitino model metadata manage
license: This software is licensed under the Apache License version 2.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This page introduces how to manage model metadata in Apache Gravitino.
A Gravitino model catalog is a kind of model registry that hosts
the versioned metadata for different  machine learning models.
It follows the typical Gravitino 3-level namespace (*catalog*/*schema*/*model*)
and supports managing multiple versions for each model.

Currently, Gravitino supports registering, listing, loading,
and deleting models and model versions.

To use a model catalog, please make sure that:

- The Gravitino server has started and is serving at [http://localhost:8090](http://localhost:8090).
- A metalake has been created and [enabled](../admin/metalake.md#enable-a-metalake)

## Catalog operations

### Create a catalog

:::info
For a model catalog, you must set the catalog `type` to `MODEL` when creating the catalog.
Please also be aware that the `provider` is not required for a model catalog.
:::

You can create a catalog by sending a `POST` request
to the `/api/metalakes/{metalake_name}/catalogs` endpoint
or use the Gravitino client SDKs.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl <<EOF >catalog.json
{
  "name": "mycatalog",
  "type": "MODEL",
  "comment": "This is a model catalog",
  "properties": {
    "k1": "v1"
  }
}
EOF

curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json"
  -d '@modle.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("mymetalake")
    .build();

Map<String, String> properties = ImmutableMap.<String, String>builder()
    .put("k1", "v1")
    .build();

Catalog catalog = gravitinoClient.createCatalog(
    "mycatalog",
    Type.MODEL,
    "This is a model catalog",
    properties);
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="mymetalake")
catalog = client.create_catalog(
    name="model_catalog",
    catalog_type=Catalog.Type.MODEL,
    provider=None,
    comment="This is a model catalog",
    properties={"k1": "v1"})
```

</TabItem>
</Tabs>

### Load a catalog

Refer to [loading a catalog](./relational.md#load-a-catalog)
for relational catalog.

### Alter a catalog

Refer to [altering a catalog](./relational.md#alter-a-catalog)
for a relational catalog.

### Drop a catalog

Refer to [dropping a catalog](./relational.md#drop-a-catalog)
for a relational catalog.

### List all catalogs in a metalake

Refer to [listing all catalogs](./relational.md#list-all-catalogs-in-a-metalake)
for relational catalogs.

### List all catalogs' information in a metalake

Refer to [listing all catalogs' information](./relational.md#list-all-catalogs-information-in-a-metalake)
for relational catalogs.

## Schema operations

A *Schema* in a model catalog is a virtual namespace for organizing models.
It is similar to the concept of *schema* in a relational catalog.

:::tip
Users should create a metalake and a catalog before creating a schema.
:::

### Create a schema

You can create a schema by sending a `POST` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas` endpoint
or use the Gravitino client SDKs.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
cat <<EOF >schema.json
{
  "name": "myschema",
  "comment": "This is a model schema",
  "properties": {
    "k1": "v1"
  }
}
EOF

curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@schema.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("mymetalake")
    .build();

Catalog catalog = gravitinoClient.loadCatalog("mycatalog");

SupportsSchemas supportsSchemas = catalog.asSchemas();

Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    .put("k1", "v1")
    .build();
Schema schema = supportsSchemas.createSchema(
    "myschema",
    "This is a schema",
    schemaProperties);
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="mymetalake")

catalog: Catalog = client.load_catalog(name="mycatalog")
catalog.as_schemas().create_schema(
    name="myschema",
    comment="This is a schema",
    properties={"k1": "v1"})
```

</TabItem>
</Tabs>

### Load a schema

Refer to [loading a schema](./relational.md#load-a-schema) for a relational catalog.

### Alter a schema

Refer to [altering a schema](./relational.md#alter-a-schema) for a relational catalog.

### Drop a schema

Refer to [dropping a schema](./relational.md#drop-a-schema) for a relational catalog.

Note that the drop operation will delete all the model metadata under this schema
if the `cascade` parameter set to `true`.

### List all schemas under a catalog

Refer to [listing all schemas](./relational.md#list-all-schemas-under-a-catalog)
for a relational catalog.

## Model operations

:::tip
- Users should create a metalake, a catalog, and a schema before creating a model.
:::

### Register a model

You can register a model by sending a `POST` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/models` endpoint
or use the Gravitino client SDKs.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
cat <<EOF >model.json
{
  "name": "mymodel",
  "comment": "This is an example model",
  "properties": {
    "k1": "v1"
  }
}
EOF

curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@model.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/models
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("mymetalake")
    .build();

Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
Map<String, String> propertiesMap = ImmutableMap.<String, String>builder()
        .put("k1", "v1")
        .build();

Model model = catalog.asModelCatalog().registerModel(
    NameIdentifier.of("myschema", "mymodel"),
    "This is an example model",
    propertiesMap);
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="mymetalake")

catalog = client.load_catalog(name="mycatalog")
model = catalog.as_model_catalog().register_model(
    ident=NameIdentifier.of("myschema", "mymodel"),
    comment="This is an example model",
    properties={"k1": "v1"})
```

</TabItem>
</Tabs>

### Get a model

You can get a model by sending a `GET` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/models/{model}` endpoint
or by using the Gravitino client SDKs.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET \
  -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/models/mymodel
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
Model model = catalog.asModelCatalog().getModel(
    NameIdentifier.of("myschema", "mymodel"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="mymetalake")
catalog = client.load_catalog(name="mycatalog")
model = catalog.as_model_catalog().get_model(
    ident=NameIdentifier.of("myschema", "mymodel"))
```

</TabItem>
</Tabs>

### Alter a model

You can modify a model's metadata (e.g. rename, update comment or modify properties) by sending a `PUT` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/models/{model}` endpoint
or using the Gravitino Java/Python client.

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
      .withMetalake("mymetalake")
      .build();

  Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
  ModelCatalog modelCatalog = catalog.asModelCatalog();

  // Define modifications
  ModelChange[] changes = {
      ModelChange.rename("example_model_renamed"),
      ModelChange.updateComment("new comment"),
      ModelChange.setProperty("k2", "v2"),
      ModelChange.removeProperty("k1")
  };

  // Apply changes
  Model updatedModel = modelCatalog.alterModel(
      NameIdentifier.of("myschema", "mymodel"),
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
      ModelChange.updateComment("new comment"),
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

<table>
<thead>
<tr>
  <th>Operation</th>
  <th>JSON payload</th>
  <th>Java method</th>
  <th>Python method</th>
</tr>
</thead>
<tbody>
<tr>
  <td>Rename model</td>
  <td>
    ```json
    {"@type":"rename","newName":"new_name"}
    ```
  </td>
  <td>
    ```java
    ModelChange.rename("new_name");
    ```
  </td>
  <td>
    ```python
    ModelChange.rename("new_name")
    ```
  </td>
</tr>
<tr>
  <td>Update comment</td>
  <td>
    ```json
    {"@type": "updateComment", "newComment": "new comment"}
    ```
  </td>
  <td>
    ```java
    ModelChange.updateComment("new comment");
    ```
  </td>
  <td>
    ```python
    ModelChange.updateComment("new comment")
    ```
  </td>
</tr>
<tr>
  <td>Set property</td>
  <td>
    ```json
    {"@type":"setProperty","property":"key","value":"value"}
    ```
  </td>
  <td>
    ```java
    ModelChange.setProperty("key", "value");
    ```
  </td>
  <td>
    ```python
    ModelChange.set_property("key", "value")
    ```
  </td>
</tr>
<tr>
  <td>Remove property</td>
  <td>
    ```json
    {"@type":"removeProperty","property":"key"}
    ```
  </td>
  <td>
    ```java
    ModelChange.removeProperty("key");
    ```
  </td>
  <td>
    ```python
    ModelChange.remove_property("key")
    ```
  </td>
</tr>
</tbody>
</table>

:::note
- Multiple modifications can be applied in a single request.
- If the target model does not exist, a `404 Not Found` error will be returned.
:::


### Delete a model

You can delete a model by sending a `DELETE` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/models/{model}` endpoint
or by using the Gravitino client SDKs.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X DELETE \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/models/mymodel
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
catalog.asModelCatalog().deleteModel(
    NameIdentifier.of("myschema", "mymodel"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="mymetalake")

catalog = client.load_catalog(name="mycatalog")
catalog.as_model_catalog().delete_model(
    NameIdentifier.of("myschema", "mymodel"))
```

</TabItem>
</Tabs>

Note that the delete operation will delete all the model versions under this model.

### List models

You can list all the models in a schema by sending a `GET` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/models` endpoint
or by using the Gravitino client SDKs.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET \
  -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/models
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
NameIdentifier[] identifiers = catalog.asModelCatalog().listModels(
    Namespace.of("myschema"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="mymetalake")

catalog = client.load_catalog(name="mycatalog")
model_list = catalog.as_model_catalog().list_models(
    namespace=Namespace.of("myschema")))
```
</TabItem>
</Tabs>

## ModelVersion operations

:::tip
- Users should create a metalake, a catalog, a schema, and a model
  before linking a model version to the model.
:::

### Link a ModelVersion

You can link a *ModelVersion* by sending a `POST` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/models/{model}/versions` endpoint
or by using the Gravitino client SDKs.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
cat <<EOF >version.json
{
  "uri": "path/to/model",
  "aliases": ["alias1", "alias2"],
  "comment": "This is version 0",
  "properties": {
    "k1": "v1"
  }
}
EOF

curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@version.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/models/mymodel/versions
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
catalog.asModelCatalog().linkModelVersion(
    NameIdentifier.of("myschema", "mymodel"),
    "path/to/model",
    new String[] {"alias1", "alias2"},
    "This is version 0",
    ImmutableMap.of("k1", "v1"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="mymetalake")
catalog = client.load_catalog(name="mycatalog")
catalog.as_model_catalog().link_model_version(
    model_ident=NameIdentifier.of("myschema", "mymodel"),
    uri="path/to/model",
    aliases=["alias1", "alias2"],
    comment="This is version 0",
    properties={"k1": "v1"})
```

</TabItem>
</Tabs>

The `comment` and `properties` of ModelVersion can be different from the model.

### Get a ModelVersion

You can get a ModelVersion by sending a `GET` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/models/{model}/versions/{version}` endpoint
or by using the Gravitino client SDKs.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET \
  -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/models/mymodel/versions/0
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
catalog.asModelCatalog().getModelVersion(
    NameIdentifier.of("myschema", "mymodel"), 0);
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="mymetalake")
catalog = client.load_catalog(name="mycatalog")
catalog.as_model_catalog().get_model_version(
    model_ident=NameIdentifier.of("myschema", "mymodel"),
    version=0)
```

</TabItem>
</Tabs>

### Get a ModelVersion by alias

You can also get a ModelVersion by sending a `GET` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/models/{model}/aliases/{alias}` endpoint
or by using the Gravitino client SDKs.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET \
  -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/myatalog/schemas/myschema/models/mymodel/aliases/alias1
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
ModelVersion modelVersion = catalog.asModelCatalog().getModelVersion(
    NameIdentifier.of("myschema", "mymodel"), "alias1");
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="mymetalake")
catalog = client.load_catalog(name="mycatalog")
model_version = catalog.as_model_catalog().get_model_version_by_alias(
    model_ident=NameIdentifier.of("myschema", "mymodel"),
    alias="alias1")
```

</TabItem>
</Tabs>

### Alter a ModelVersion

You can modify a ModelVersion's metadata (e.g. update its URI, comment, or properties) by sending a `PUT` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/models/{model}/versions/{version}` endpoint
or by using the Gravitino Java/Python client SDK.

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
      .withMetalake("mymetalake")
      .build();

  Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
  ModelCatalog modelCatalog = catalog.asModelCatalog();

  // Define modifications
  ModelVersionChange[] changes = {
       ModelVersionChange.updateComment("Updated comment of model version"),
       ModelVersionChange.updateUri("new_uri"),
       ModelVersionChange.setProperty("key", "value"),
       ModelVersionChange.removeProperty("key")
   };

  // Apply changes
  ModelVersion updatedModelVersion = modelCatalog.alterModelVersion(
       NameIdentifier.of("myschema", "mymodel"),
       0,
       changes
   );
  ```

  </TabItem>
  <TabItem value="python" label="Python">

  ```python
  client = GravitinoClient(uri="http://localhost:8090",
                           metalake_name="mymetalake")

  # Load Model Catalog
  model_catalog = client.load_catalog(name="mycatalog").as_model_catalog()

  # Define modifications
  changes = (
      ModelVersionChange.update_comment("Updated comment of model version"),
      ModelVersionChange.update_uri("new_uri"),
      ModelVersionChange.set_property("k2", "v2"),
      ModelVersionChange.remove_property("k1"),
  )

  # Apply changes
  updated_model = model_catalog.alter_model_version(
      NameIdentifier.of("myschema", "mymodel"), 0, *changes
  )
  ```

  </TabItem>
</Tabs>

#### Supported modifications

<table>
<thead>
<tr>
  <th>Operation</th>
  <th>JSON payload</th>
  <th>Java method</th>
  <th>Python method</th>
</tr>
</thead>
<tbody>
<tr>
  <td>Update URI</td>
  <td>
    ```json
    {"@type":"updateUri","newName":"new_uri"}
    ```
  </td>
  <td>
    ```java
    ModelVersionChange.updateUri("new_uri");
    ```
  </td>
  <td>
    ```python
    ModelVersionChange.update_uri("new_uri")
    ```
  </td>
</tr>
<tr>
  <td>Update comment</td>
  <td>
    ```json
    {"@type":"updateComment", "newComment":"new_comment"}
    ```
  </td>
  <td>
    ```java
    ModelVersionChange.updateComment("new_comment");
    ```
  </td>
  <td>
    ```python
    ModelVersionChange.update_comment("new_comment")
    ```
  </td>
</tr>
<tr>
  <td>Set property</td>
  <td>
    ```json
    {"@type":"setProperty", "property":"key", "value":"value"}
    ```
  </td>
  <td>
    ```java
    ModelVersionChange.setProperty("key", "value");
    ```
  </td>
  <td>
    ```python
    ModelVersionChange.set_property("key", "value")
    ```
  </td>
</tr>
<tr>
  <td>Remove property</td>
  <td>
    ```json
    {"@type": "removeProperty", "property": "key"}
    ```
  </td>
  <td>
    ```java
    ModelVersionChange.removeProperty("key");
    ```
  </td>
  <td>
    ```python
    ModelVersionChange.remove_property("key")
    ```
  </td>
</tr>
</tbody>
</table>

:::note
- Multiple modifications can be applied in a single request.
- If the target Model does not exist, a `404 Not Found` error will be returned.
- If the target ModelVersion does not exist, a `404 Not Found` error will be returned.
:::

### Alter a ModelVersion by alias

You can also modify a ModelVersion's metadata (e.g. update its URI, comment, or properties) by sending a `PUT` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/models/{model}/aliases/{alias}` endpoint
or by using the Gravitino Java/Python client SDK.

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
      .withMetalake("mymetalake")
      .build();

  Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
  ModelCatalog modelCatalog = catalog.asModelCatalog();

  // Define modifications
  ModelVersionChange[] changes = {
      ModelVersionChange.updateComment("Updated comment of model version"),
      ModelVersionChange.updateUri("new_uri"),
      ModelVersionChange.setProperty("key", "value"),
      ModelVersionChange.removeProperty("key")
   };

  // Apply changes
  ModelVersion updatedModelVersion = modelCatalog.alterModelVersion(
      NameIdentifier.of("myschema", "mymodel"),
      "myalias",
      changes
   );
  ```

  </TabItem>
  <TabItem value="python" label="Python">

  ```python
  client = GravitinoClient(uri="http://localhost:8090",
                           metalake_name="mymetalake")

  # Load Model Catalog
  model_catalog = client.load_catalog(name="mycatalog").as_model_catalog()

  # Define modifications
  changes = (
      ModelVersionChange.update_comment("Updated comment of model version"),
      ModelVersionChange.update_uri("new_uri"),
      ModelVersionChange.set_property("k2", "v2"),
      ModelVersionChange.remove_property("k1"),
  )

  # Apply changes
  updated_model = model_catalog.alter_model_version_by_alias(
      NameIdentifier.of("myschema", "mymodel", "myalias"), *changes
  )
  ```

  </TabItem>
</Tabs>

#### Supported modifications

<table>
<thead>
<tr>
  <th>Operation</th>
  <th>JSON payload</th>
  <th>Java method</th>
  <th>Python method</th>
</tr>
</thead>
<tbody>
<tr>
  <td>Update URI</td>
  <td>
    ```json
    {"@type": "updateUri", "newName": "new_uri"}
    ```
  </td>
  <td>
    ```java
    ModelVersionChange.updateUri("new_uri");
    ```
  </td>
  <td>
    ```python
    ModelVersionChange.update_uri("new_uri")
    ```
  </td>
</tr>
<tr>
  <td>Update comment</td>
  <td>
    ```json
    {"@type": "updateComment", "newComment": "new_comment"}
    ```
  </td>
  <td>
    ```java
    ModelVersionChange.updateComment("new_comment");
    ```
  <td>
  </td>
    ```python
    ModelVersionChange.update_comment("new_comment")
    ```
  </td>
</tr>
<tr>
  <td>Set property</td>
  <td>
    ```json
    {"@type": "setProperty", "property": "key", "value": "value"}
    ```
  </td>
  <td>
    ```java
    ModelVersionChange.setProperty("key", "value");
    ```
  </td>
  <td>
    ```python
    ModelVersionChange.set_property("key", "value")
    ```
  </td>
</tr>
<tr>
  <td>Remove property</td>
  <td>
    ```json
    {"@type": "removeProperty", "property": "key"}
    ```
  </td>
  <td>
    ```java
    ModelVersionChange.removeProperty("key");
    ```
  </td>
  <td>
    ```python
    ModelVersionChange.remove_property("key")
    ```
  </td>
</tr>
</tbody>
</table>

:::note
- Multiple modifications can be applied in a single request.
- If the target Model does not exist, a `404 Not Found` error will be returned.
- If the target ModelVersion does not exist, a `404 Not Found` error will be returned.
:::

### Delete a ModelVersion

You can delete a ModelVersion by sending a `DELETE` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/models/{model}/versions/{version}` endpoint
or by using the Gravitino client SDKs.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X DELETE \
  -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/models/mymodel/versions/0
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
catalog.asModelCatalog().deleteModelVersion(
    NameIdentifier.of("myschema", "mymodel"), 0);
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="mymetalake")
catalog = gravitino_client.load_catalog(name="mycatalog")
catalog.as_model_catalog().delete_model_version(
    model_ident=NameIdentifier.of("myschema", "mymodel"),
    version=0)
```

</TabItem>
</Tabs>

### Delete a ModelVersion by alias

You can also delete a ModelVersion by sending a `DELETE` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/models/{model}/aliases/{alias}` endpoint
or by using the Gravitino client SDKs.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X DELETE \
  -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/models/mymodel/aliases/alias1
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
catalog.asModelCatalog().deleteModelVersion(
    NameIdentifier.of("myschema", "mymodel"), "alias1");
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="example")

catalog = client.load_catalog(name="mycatalog")
catalog.as_model_catalog().delete_model_version_by_alias(
    model_ident=NameIdentifier.of("myschema", "mymodel"),
    alias="alias1")
```

</TabItem>
</Tabs>

### List ModelVersions

You can list all the ModelVersions in a model by sending a `GET` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/models/{model}/versions` endpoint
or by using the Gravitino client SDKs.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET \]
  -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/models/mymodel/versions
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
int[] modelVersions = catalog.asModelCatalog().listModelVersions(
    NameIdentifier.of("myschema", "mymodel"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="mymetalake")

catalog = client.load_catalog(name="mycatalog")
model_versions = catalog.as_model_catalog().list_model_versions(
    model_ident=NameIdentifier.of("myschema", "mymodel"))
```
</TabItem>
</Tabs>

