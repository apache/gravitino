---
title: "Manage Fileset Metadata"
slug: "/manage-fileset-metadata-using-gravitino"
keyword: "fileset management, fileset, storage location, GVFS, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

This page covers the Gravitino API for filesets. For what a fileset is, the difference between
managed and external, how storage locations and placeholders work, and how GVFS reads them, see
[Filesets](./filesets.md). For creating the catalog and schema a fileset lives in, see
[Manage Catalogs and Schemas](./manage-catalogs-and-schemas.md).

## Fileset Operations

### Create a Fileset

A fileset needs a name, a type, and a storage location. A `MANAGED` fileset is created and deleted
with its data; an `EXTERNAL` one points at a location that already exists.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "raw_events",
  "comment": "Raw event drops",
  "type": "MANAGED",
  "storageLocation": "s3a://example-bucket/landing/raw_events",
  "properties": {"retention": "30d"}
}' http://localhost:8090/api/metalakes/example/catalogs/landing/schemas/raw/filesets
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = client.loadCatalog("landing");
FilesetCatalog filesets = catalog.asFilesetCatalog();

Fileset fileset = filesets.createFileset(
    NameIdentifier.of("raw", "raw_events"),
    "Raw event drops",
    Fileset.Type.MANAGED,
    "s3a://example-bucket/landing/raw_events",
    ImmutableMap.of("retention", "30d"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalog = client.load_catalog("landing")
filesets = catalog.as_fileset_catalog()

fileset = filesets.create_fileset(
    ident=NameIdentifier.of("raw", "raw_events"),
    comment="Raw event drops",
    fileset_type=Fileset.Type.MANAGED,
    storage_location="s3a://example-bucket/landing/raw_events",
    properties={"retention": "30d"})
```

</TabItem>
</Tabs>

### Create a Fileset With Several Locations

`storageLocations` takes a map of location name to path. The name is what a reader selects, and
`default-location-name` picks the one used when none is named.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "raw_events",
  "comment": "Raw event drops, two regions",
  "type": "MANAGED",
  "storageLocations": {
    "us": "s3a://example-us/landing/raw_events",
    "eu": "s3a://example-eu/landing/raw_events"
  },
  "properties": {"default-location-name": "us"}
}' http://localhost:8090/api/metalakes/example/catalogs/landing/schemas/raw/filesets
```

</TabItem>
</Tabs>

### Create a Fileset From a Location Template

A catalog or schema can carry a location template, and a fileset created beneath it fills the
placeholders from its own `placeholder-` properties.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "workspace",
  "type": "MANAGED",
  "properties": {
    "placeholder-project": "risk",
    "placeholder-user": "mhoerth"
  }
}' http://localhost:8090/api/metalakes/example/catalogs/landing/schemas/raw/filesets
```

</TabItem>
</Tabs>

With a catalog location of
`s3a://example-bucket/{{catalog}}/{{schema}}/workspace_{{project}}/{{user}}`, that request resolves
to `s3a://example-bucket/landing/raw/workspace_risk/mhoerth`.

### Load a Fileset

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/landing/schemas/raw/filesets/raw_events
```

</TabItem>
<TabItem value="java" label="Java">

```java
Fileset fileset = filesets.loadFileset(NameIdentifier.of("raw", "raw_events"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
fileset = filesets.load_fileset(NameIdentifier.of("raw", "raw_events"))
```

</TabItem>
</Tabs>

### Alter a Fileset

Changes are applied as a list in one request. Storage locations cannot be changed after creation.

| Change             | JSON                                                         | Java                                          |
|--------------------|--------------------------------------------------------------|-----------------------------------------------|
| Rename             | `{"@type":"rename","newName":"fileset_renamed"}`             | `FilesetChange.rename("fileset_renamed")`     |
| Update the comment | `{"@type":"updateComment","newComment":"new_comment"}`       | `FilesetChange.updateComment("new_comment")`  |
| Set a property     | `{"@type":"setProperty","property":"key1","value":"value1"}` | `FilesetChange.setProperty("key1", "value1")` |
| Remove a property  | `{"@type":"removeProperty","property":"key1"}`               | `FilesetChange.removeProperty("key1")`        |

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "updates": [
    {"@type": "setProperty", "property": "retention", "value": "90d"}
  ]
}' http://localhost:8090/api/metalakes/example/catalogs/landing/schemas/raw/filesets/raw_events
```

</TabItem>
<TabItem value="java" label="Java">

```java
Fileset fileset = filesets.alterFileset(
    NameIdentifier.of("raw", "raw_events"),
    FilesetChange.setProperty("retention", "90d"));
```

</TabItem>
</Tabs>

### Drop a Fileset

Dropping a `MANAGED` fileset deletes its files. Dropping an `EXTERNAL` one removes only the
Gravitino record.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/landing/schemas/raw/filesets/raw_events
```

</TabItem>
<TabItem value="java" label="Java">

```java
boolean dropped = filesets.dropFileset(NameIdentifier.of("raw", "raw_events"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
dropped = filesets.drop_fileset(NameIdentifier.of("raw", "raw_events"))
```

</TabItem>
</Tabs>

### List Filesets

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/landing/schemas/raw/filesets
```

</TabItem>
<TabItem value="java" label="Java">

```java
NameIdentifier[] identifiers = filesets.listFilesets(Namespace.of("raw"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
identifiers = filesets.list_filesets(Namespace.of("raw"))
```

</TabItem>
</Tabs>

### Reading and Writing Files

Fileset metadata operations do not move data. Reading and writing the files themselves goes through
GVFS. See [How to use GVFS](./how-to-use-gvfs.md).
