---
title: "Model catalog"
slug: /model-catalog
date: 2024-12-26
keyword: model catalog
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Model catalog is a metadata catalog that offers the unified interfaces to manage the metadata of
machine learning models in a centralized way. It follows the typical Gravitino 3-level namespace
(catalog, schema, and model) to manage the ML models metadata. In additional, it supports
managing the versions for each model.

The advantages of using model catalog are:

* Centralized management of ML models with user defined namespaces. Users can better discover
  and govern the models from sematic level, rather than managing the model files directly.
* Version management for each model. Users can easily track the model versions and manage the
  model lifecycle.

The key concept of model management is to manage the path (URI) of the model. Instead of
managing the model storage path physically and separately, model metadata defines the mapping
relation between the model name and the storage path. In the meantime, with the support of
extensible properties of model metadata, users can define the model metadata with more detailed information
rather than just the storage path.

* **Model**: The model is a metadata defined in the model catalog, to manage the ML models. Each
  model can have many **Model Versions**, and each version can have its own properties. Models
  can be retrieved by the name.
* **Model Version**: The model version is a metadata defined in the model catalog, to manage each
  version of the ML model. Each version has a unique version number, and can have its own
  properties and storage path. Model version can be retrieved by the model name and version
  number. Also, each version can have a list of aliases, which can also be used to retrieve.

## Catalog

### Catalog properties

Model catalog doesn't have specific properties. It uses the [common catalog properties](./gravitino-server-config.md#apache-gravitino-catalog-properties-configuration).

### Catalog operations

Refer to [Catalog operations](./manage-model-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema capabilities

Schema is the second level of the model catalog namespace, the model catalog supports creating, updating, deleting, and listing schemas.

### Schema properties

Schema in the model catalog doesn't have predefined properties. Users can define the properties for each schema.

### Schema operations

Refer to [Schema operation](./manage-model-metadata-using-gravitino.md#schema-operations) for more details.

## Model

### Model capabilities

The Model catalog supports registering, listing and deleting models and model versions.

### Model properties

Model doesn't have predefined properties. Users can define the properties for each model and model version.

### Model operations

Refer to [Model operation](./manage-model-metadata-using-gravitino.md#model-operations) for more details.

## Model version

### Model version capabilities

The Model catalog supports linking, listing and deleting model versions.

### Model version properties

Model version doesn't have predefined properties. Users can define the properties for each model version.

### Model version operations

Refer to [Model version operation](./manage-model-metadata-using-gravitino.md#model-version-operations) for more details.
