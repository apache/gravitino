---
title: "Manage Model Metadata"
slug: "/manage-model-metadata-using-gravitino"
keyword: "model management, model version, alias, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

This page covers the Gravitino API for models and model versions. For what a model catalog is, how
versions and aliases work, and how several URIs on one version behave, see
[Model Catalog](./model-catalog.md). For creating the catalog and schema a model lives in, see
[Manage Catalogs and Schemas](./manage-catalogs-and-schemas.md).

## Model Operations

### Register a Model

Registering creates the model with no versions. It needs a name, and can carry a comment and
properties.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "churn_predictor",
  "comment": "Customer churn model",
  "properties": {"team": "risk"}
}' http://localhost:8090/api/metalakes/example/catalogs/models/schemas/customer/models
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = client.loadCatalog("models");
ModelCatalog models = catalog.asModelCatalog();

Model model = models.registerModel(
    NameIdentifier.of("customer", "churn_predictor"),
    "Customer churn model",
    ImmutableMap.of("team", "risk"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalog = client.load_catalog("models")
models = catalog.as_model_catalog()

model = models.register_model(
    model_ident=NameIdentifier.of("customer", "churn_predictor"),
    comment="Customer churn model",
    properties={"team": "risk"})
```

</TabItem>
</Tabs>

### Get a Model

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/models/schemas/customer/models/churn_predictor
```

</TabItem>
<TabItem value="java" label="Java">

```java
Model model = models.getModel(NameIdentifier.of("customer", "churn_predictor"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
model = models.get_model(NameIdentifier.of("customer", "churn_predictor"))
```

</TabItem>
</Tabs>

### Alter a Model

| Change             | JSON                                                         | Java                                        |
|--------------------|--------------------------------------------------------------|---------------------------------------------|
| Rename             | `{"@type":"rename","newName":"churn_v2"}`                    | `ModelChange.rename("churn_v2")`            |
| Update the comment | `{"@type":"updateComment","newComment":"new_comment"}`       | `ModelChange.updateComment("new_comment")`  |
| Set a property     | `{"@type":"setProperty","property":"key1","value":"value1"}` | `ModelChange.setProperty("key1", "value1")` |
| Remove a property  | `{"@type":"removeProperty","property":"key1"}`               | `ModelChange.removeProperty("key1")`        |

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "updates": [
    {"@type": "setProperty", "property": "default-uri-name", "value": "us"}
  ]
}' http://localhost:8090/api/metalakes/example/catalogs/models/schemas/customer/models/churn_predictor
```

</TabItem>
<TabItem value="java" label="Java">

```java
Model model = models.alterModel(
    NameIdentifier.of("customer", "churn_predictor"),
    ModelChange.setProperty("default-uri-name", "us"));
```

</TabItem>
</Tabs>

### List and Delete Models

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/models/schemas/customer/models

curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/models/schemas/customer/models/churn_predictor
```

</TabItem>
<TabItem value="java" label="Java">

```java
NameIdentifier[] identifiers = models.listModels(Namespace.of("customer"));
boolean deleted = models.deleteModel(NameIdentifier.of("customer", "churn_predictor"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
identifiers = models.list_models(Namespace.of("customer"))
deleted = models.delete_model(NameIdentifier.of("customer", "churn_predictor"))
```

</TabItem>
</Tabs>

Deleting a model deletes all of its versions.

## Model Version Operations

### Link a Version

Linking creates a version of an existing model. The version number is assigned in sequence starting
at zero.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "uri": "s3a://models/churn/v0",
  "aliases": ["production"],
  "comment": "First release",
  "properties": {"framework": "xgboost"}
}' http://localhost:8090/api/metalakes/example/catalogs/models/schemas/customer/models/churn_predictor/versions
```

</TabItem>
<TabItem value="java" label="Java">

```java
models.linkModelVersion(
    NameIdentifier.of("customer", "churn_predictor"),
    "s3a://models/churn/v0",
    new String[] {"production"},
    "First release",
    ImmutableMap.of("framework", "xgboost"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
models.link_model_version(
    model_ident=NameIdentifier.of("customer", "churn_predictor"),
    uri="s3a://models/churn/v0",
    aliases=["production"],
    comment="First release",
    properties={"framework": "xgboost"})
```

</TabItem>
</Tabs>

To give a version several named URIs, send `uris` as a map instead of a single `uri`, and set
`default-uri-name` to pick the one returned when a caller does not name one. In Java the same
`linkModelVersion` takes a map; in Python it is `link_model_version_with_multiple_uris`.

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "uris": {
    "us": "s3a://models-us/churn/v0",
    "eu": "s3a://models-eu/churn/v0"
  },
  "aliases": ["production"],
  "properties": {"default-uri-name": "us"}
}' http://localhost:8090/api/metalakes/example/catalogs/models/schemas/customer/models/churn_predictor/versions
```

### Get a Version

A version is fetched by number or by alias, and its URI can be fetched directly.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/models/schemas/customer/models/churn_predictor/versions/0

curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/models/schemas/customer/models/churn_predictor/aliases/production

curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/models/schemas/customer/models/churn_predictor/aliases/production/uri
```

</TabItem>
<TabItem value="java" label="Java">

```java
ModelVersion byNumber = models.getModelVersion(
    NameIdentifier.of("customer", "churn_predictor"), 0);

ModelVersion byAlias = models.getModelVersion(
    NameIdentifier.of("customer", "churn_predictor"), "production");
```

</TabItem>
<TabItem value="python" label="Python">

```python
by_number = models.get_model_version(
    NameIdentifier.of("customer", "churn_predictor"), 0)

by_alias = models.get_model_version_by_alias(
    NameIdentifier.of("customer", "churn_predictor"), "production")
```

</TabItem>
</Tabs>

### Alter a Version

Aliases move between versions with `updateAliases`, which adds and removes in one call. URIs are
added, updated, and removed by name.

| Change             | JSON                                                                        | Java                                                       |
|--------------------|-----------------------------------------------------------------------------|------------------------------------------------------------|
| Update the comment | `{"@type":"updateComment","newComment":"new_comment"}`                      | `ModelVersionChange.updateComment("new_comment")`          |
| Set a property     | `{"@type":"setProperty","property":"key1","value":"value1"}`                | `ModelVersionChange.setProperty("key1", "value1")`         |
| Remove a property  | `{"@type":"removeProperty","property":"key1"}`                              | `ModelVersionChange.removeProperty("key1")`                |
| Update the URI     | `{"@type":"updateUri","newUri":"s3a://models/churn/v1"}`                    | `ModelVersionChange.updateUri(...)`                        |
| Add a named URI    | `{"@type":"addUri","uriName":"eu","uri":"s3a://models-eu/churn/v0"}`        | `ModelVersionChange.addUri("eu", ...)`                     |
| Remove a named URI | `{"@type":"removeUri","uriName":"eu"}`                                      | `ModelVersionChange.removeUri("eu")`                       |
| Move aliases       | `{"@type":"updateAliases","aliasesToAdd":["production"],"aliasesToRemove":[]}` | `ModelVersionChange.updateAliases(...)`                  |

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "updates": [
    {"@type": "updateAliases", "aliasesToAdd": ["production"], "aliasesToRemove": []}
  ]
}' http://localhost:8090/api/metalakes/example/catalogs/models/schemas/customer/models/churn_predictor/versions/1
```

</TabItem>
<TabItem value="java" label="Java">

```java
models.alterModelVersion(
    NameIdentifier.of("customer", "churn_predictor"),
    1,
    ModelVersionChange.updateAliases(new String[] {"production"}, new String[] {}));
```

</TabItem>
</Tabs>

Moving an alias onto a new version removes it from the version that held it, since an alias belongs
to one version at a time.

### List and Delete Versions

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/models/schemas/customer/models/churn_predictor/versions

curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/models/schemas/customer/models/churn_predictor/versions/0
```

</TabItem>
<TabItem value="java" label="Java">

```java
int[] versions = models.listModelVersions(
    NameIdentifier.of("customer", "churn_predictor"));

boolean deleted = models.deleteModelVersion(
    NameIdentifier.of("customer", "churn_predictor"), 0);
```

</TabItem>
<TabItem value="python" label="Python">

```python
versions = models.list_model_versions(
    NameIdentifier.of("customer", "churn_predictor"))

deleted = models.delete_model_version(
    NameIdentifier.of("customer", "churn_predictor"), 0)
```

</TabItem>
</Tabs>
