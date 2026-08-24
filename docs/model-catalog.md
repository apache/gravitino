---
title: "Model Catalog"
slug: "/model-catalog"
keyword: "model catalog, ML model, model version, model registry, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

A model catalog is a registry for machine learning models. It follows the same three-level namespace
as every other catalog, catalog then schema then model, and adds a fourth level beneath each model
for its versions.

What it stores is the mapping from a name to a location. Instead of jobs and notebooks carrying a
path to a model file, they resolve a model by name and get the version they asked for. The model
files themselves stay wherever they live.

That indirection is what makes the rest possible. Models sit in the same hierarchy as the tables
they were trained on, carry tags and policies like anything else, and are governed by the same roles
and privileges.

Gravitino manages this catalog itself rather than federating an external registry, so a model
catalog takes no provider.

## Quick Start

**1. Create a model catalog and schema.** See
[Catalogs and Schemas](./catalogs-and-schemas.md). A model catalog takes no provider and has no
required properties.

**2. Register a model.** Registering creates the model as a named object with no versions yet.

**3. Link a version.** A version carries the URI where the model lives, plus any aliases you want to
resolve it by.

## The Model Registry

### Models and Versions

A model is a named object in a schema, and holds no location of its own.

A model version is one release of that model. It carries the URI where the files live, an optional
comment, its own properties, and a version number assigned in sequence starting at zero. A new
release is a new version rather than an edit of an existing one.

### Aliases

A version can carry aliases, and an alias resolves to a version the same way its number does. That
is how a moving pointer such as `production` or `champion` is expressed: link a new version, move
the alias, and everything resolving by that alias picks up the new release without any code
changing.

An alias belongs to one version at a time within a model.

### Several URIs on One Version

A version can carry more than one URI, each with a name, held as a map rather than a single value.
That is how the same release is described in more than one place, such as a copy per region or per
storage system.

`default-uri-name` picks which one is returned when a caller does not name one, and can be set on
the version or on the model as a default for all of its versions. A URI supplied without a name is
recorded as `unknown`.

### Properties

Neither the catalog nor its schemas have predefined properties beyond the
[common catalog properties](./gravitino-server-config.md#catalog-properties-configuration). Models
and versions carry free-form properties of their own, with `default-uri-name` reserved.

## Working With Models in the UI

Opening a model catalog lists its schemas and the models inside them. A model shows its versions,
their URIs, and their aliases, and versions can be linked from there.

Models display the tags they carry, including inherited ones, but cannot be tagged from the UI
today. Attaching a tag to a model goes through the API.

## Permissions

Models follow the privileges of the catalog and schema that hold them. See
[Catalogs and Schemas](./catalogs-and-schemas.md).

## Using the API

Models and versions can be registered, linked, listed, altered, and dropped over REST and through
the Java and Python clients. Endpoints, payload shapes, and worked examples are in
[Manage Model Metadata](./manage-model-metadata-using-gravitino.md).
