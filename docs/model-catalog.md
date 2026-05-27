---
title: "Model Catalog"
slug: "/model-catalog"
keywords:
  - model
  - ml
  - metadata
license: "This software is licensed under the Apache License version 2."
---

## Introduction

A model catalog is a metadata catalog that provides a unified interface for managing machine-learning model metadata in a centralized way. It follows the typical Gravitino three-level namespace (catalog, schema, model) and supports versioning for each model.

Benefits of a model catalog:

- Centralized management of ML models with user-defined namespaces. Discover and govern models at a semantic level instead of managing the model files directly.
- Version management for each model. Track model versions and manage the model lifecycle.

A model catalog manages the path (URI) of each model. Instead of tracking storage paths separately, the metadata defines the mapping between the model name and its storage path. Through extensible model-metadata properties, users can attach richer information than the storage path alone.

- **Model.** A metadata object in the model catalog that represents an ML model. Each model can have many **model versions**, and each version can carry its own properties. Models are retrieved by name.
- **Model version.** A metadata object in the model catalog that represents a specific version of an ML model. Each version has a unique version number and can carry its own properties and storage path. Model versions can be retrieved by model name and version number. Each version can also have a list of aliases for retrieval.

### Requirements and Limitations

- **Metadata only.** The Model catalog manages model metadata and the mapping from model name to URI; it does not store model artifacts or fetch model files from the URI.
- **Three-level namespace.** Catalog -> schema -> model, with each model carrying one or more versions.
- **URI agnostic.** Model version URIs can point to any storage scheme (for example, `s3://`, `gs://`, `abfss://`, `hdfs://`, `file://`); Gravitino does not validate the URI or check that the artifact exists.
- **Version aliases.** Each model version can be retrieved by an alias (such as `production` or `canary`) in addition to its numeric version.
- **No catalog-specific properties.** The Model catalog uses only the [common catalog properties](./gravitino-server-config.md#catalog-properties).

## Quick Start

Create a minimum-viable Model catalog and confirm it is reachable. The example assumes a Gravitino server at `http://localhost:8090` and a metalake named `test`. Adjust the values for your environment.

### Create the Catalog

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "model_catalog",
    "type": "MODEL",
    "comment": "Model catalog",
    "provider": "model",
    "properties": {}
  }' \
  http://localhost:8090/api/metalakes/test/catalogs
```

The response is a JSON object describing the created catalog.

### Verify the Catalog

```bash
# List catalogs in the metalake. model_catalog should appear.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs" | jq

# Load the catalog directly and inspect its properties.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/model_catalog" | jq

# List schemas. The response is an empty array on a freshly created catalog until a schema is added.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/model_catalog/schemas" | jq
```

**Success check:** the catalog-list response includes `model_catalog`, the load-catalog response shows `"type":"MODEL"` and `"provider":"model"`, and the schema-list response is a JSON array (an empty array on a fresh catalog is expected). If the load-catalog call returns an error, verify that the Gravitino server has the model catalog provider available; the Model catalog is built in and does not require a separate bundle JAR.

## Catalog

### Catalog Capabilities

A Gravitino Model catalog:

- Provides a three-level namespace (catalog -> schema -> model) for ML model metadata, with versions as a fourth sublevel under each model.
- Supports creating, listing, loading, altering, and dropping schemas.
- Supports registering, listing, loading, and deleting models and model versions.
- Stores no model artifacts itself; each model version carries one or more storage URIs that point at the actual artifact.
- Supports retrieval of model versions by version number or by user-defined alias.

### Catalog Properties

The model catalog has no catalog-specific properties; it uses the [common catalog properties](./gravitino-server-config.md#catalog-properties).

### Catalog Operations

Refer to [Catalog operations](./manage-model-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema Capabilities

A schema is the second level of the model catalog namespace. The model catalog supports creating, updating, deleting, and listing schemas.

### Schema Properties

Schemas in the model catalog have no predefined properties. You can define your own properties on each schema.

### Schema Operations

Refer to [Schema operation](./manage-model-metadata-using-gravitino.md#schema-operations) for more details.

## Model

### Model Capabilities

The model catalog supports registering, listing, and deleting models and model versions.

### Model Properties

| Property name      | Description                                         | Default value | Required | Immutable | Since Version |
|--------------------|-----------------------------------------------------|---------------|----------|-----------|---------------|
| `default-uri-name` | The default URI name for the versions of the model. | (none)        | No       | No        | 1.0.0         |

### Model Operations

Refer to [Model operation](./manage-model-metadata-using-gravitino.md#model-operations) for more details.

## Model Version

### Model Version Capabilities

- A model version is a sublevel under a model and is identified by an automatically assigned version number.
- Supports linking (registering a new version), listing, loading, and deleting model versions.
- Each version can carry its own properties and one or more storage URIs; the `default-uri-name` property selects which URI is the default.
- Each version can have one or more aliases (for example, `production` or `canary`) for retrieval by name in addition to by version number.

### Model Version Properties

| Property name      | Description                                                                                                       | Default value | Required | Immutable | Since Version |
|--------------------|-------------------------------------------------------------------------------------------------------------------|---------------|----------|-----------|---------------|
| `default-uri-name` | The default URI name for the model version. When set, it overrides the `default-uri-name` property at model level. | (none)        | No       | No        | 1.0.0         |

### Model Version Operations

Refer to [Model version operations](./manage-model-metadata-using-gravitino.md#modelversion-operations) for more details.
