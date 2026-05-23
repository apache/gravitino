---
title: "Model Catalog"
slug: "/model-catalog"
date: 2024-12-26
keyword: "model catalog"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

A model catalog is a metadata catalog that provides a unified interface for managing machine-learning model metadata in a centralized way. It follows the typical Gravitino three-level namespace (catalog, schema, model) and supports versioning for each model.

Benefits of a model catalog:

* Centralized management of ML models with user-defined namespaces. Discover and govern models at a semantic level instead of managing the model files directly.
* Version management for each model. Track model versions and manage the model lifecycle.

A model catalog manages the path (URI) of each model. Instead of tracking storage paths separately, the metadata defines the mapping between the model name and its storage path. Through extensible model-metadata properties, users can attach richer information than the storage path alone.

* **Model.** A metadata object in the model catalog that represents an ML model. Each model can have many **model versions**, and each version can carry its own properties. Models are retrieved by name.
* **Model version.** A metadata object in the model catalog that represents a specific version of an ML model. Each version has a unique version number and can carry its own properties and storage path. Model versions can be retrieved by model name and version number. Each version can also have a list of aliases for retrieval.

## Catalog

### Catalog Properties

The model catalog has no catalog-specific properties; it uses the [common catalog properties](./gravitino-server-config.md#catalog-properties-configuration).

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

The model catalog supports linking, listing, and deleting model versions.

### Model Version Properties

| Property name      | Description                                                                                                       | Default value | Required | Immutable | Since Version |
|--------------------|-------------------------------------------------------------------------------------------------------------------|---------------|----------|-----------|---------------|
| `default-uri-name` | The default URI name for the model version. When set, it overrides the `default-uri-name` property at model level. | (none)        | No       | No        | 1.0.0         |

### Model Version Operations

Refer to [Model version operations](./manage-model-metadata-using-gravitino.md#modelversion-operations) for more details.
