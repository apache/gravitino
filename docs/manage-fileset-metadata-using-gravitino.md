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

A `MANAGED` fileset can inherit all its locations from its schema or catalog. An `EXTERNAL` fileset
must supply at least one location in the create request.

With filesystem operations enabled, Gravitino creates missing directories for either type. By
default, it rejects existing non-empty directories for a `MANAGED` fileset. Set
`allow-existing-location-as-managed=true` at creation to adopt that data and delete it on drop.
An existing empty directory needs no override. With `disable-filesystem-ops=true`, creation only
registers the fileset metadata.

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

The create request accepts both `storageLocation` and `storageLocations`. A non-null
`storageLocation` overrides `storageLocations["unknown"]` and keeps the other named entries.
In this example, the `raw` schema also supplies `location-eu=s3a://example-eu/landing/raw`,
as shown in [the catalog configuration](./fileset-catalog.md#how-fileset-locations-are-resolved).

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "raw_events",
  "type": "MANAGED",
  "storageLocation": "s3a://example-bucket/imports/raw_events",
  "storageLocations": {
    "unknown": "s3a://example-bucket/staging/raw_events",
    "us": "s3a://example-us/landing/raw_events"
  },
  "properties": {"default-location-name": "us"}
}' http://localhost:8090/api/metalakes/example/catalogs/landing/schemas/raw/filesets
```

</TabItem>
</Tabs>

The resulting fileset has three locations.

| Name | Fileset path | Source |
| --- | --- | --- |
| `unknown` | `s3a://example-bucket/imports/raw_events` | `storageLocation` |
| `us` | `s3a://example-us/landing/raw_events` | `storageLocations["us"]` |
| `eu` | `s3a://example-eu/landing/raw/raw_events` | Inherited from the schema |

With multiple resolved names, `default-location-name` must match one of them. GVFS uses `us` by
default here. A reader can [select another location](./how-to-use-gvfs.md#examples) by specifying
`unknown` or `eu`.

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
| Update the type    | `{"@type":"updateType","newType":"external"}`                | `FilesetChange.updateType(Fileset.Type.EXTERNAL)` |
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

#### Change the Fileset Type

An existing fileset can be changed between `MANAGED` and `EXTERNAL`. This changes metadata only:
its identifier, name, storage locations, properties, and existing files are preserved. Setting the
current type is allowed.

Change a `MANAGED` fileset to `EXTERNAL` before dropping it to retain its files. Changing an
`EXTERNAL` fileset to `MANAGED` explicitly accepts management of the existing data, including
deleting that data when the fileset is dropped. The same type semantics apply when dropping a
schema with cascade. Conversion does not move, create, or delete files, and does not protect data
from deletion through another managed fileset with an overlapping location.

With `disable-filesystem-ops=true`, dropping a fileset removes only its metadata, even after
conversion to `MANAGED`.

Type conversion follows the existing [fileset alter authorization](security/access-control.md#data-objects).
A caller with `WRITE_FILESET` and the required catalog and schema use privileges can convert in
either direction, including changing whether a subsequent drop deletes the data. This does not
grant permission to drop the fileset; drop retains its existing ownership requirements.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "updates": [{"@type": "updateType", "newType": "external"}]
}' http://localhost:8090/api/metalakes/example/catalogs/landing/schemas/raw/filesets/raw_events
```

</TabItem>
<TabItem value="java" label="Java">

```java
Fileset fileset = filesets.alterFileset(
    NameIdentifier.of("raw", "raw_events"),
    FilesetChange.updateType(Fileset.Type.EXTERNAL));
```

</TabItem>
<TabItem value="python" label="Python">

```python
fileset = filesets.alter_fileset(
    NameIdentifier.of("raw", "raw_events"),
    FilesetChange.update_type(Fileset.Type.EXTERNAL),
)
```

</TabItem>
</Tabs>

### Drop a Fileset

With filesystem operations enabled, dropping a `MANAGED` fileset deletes every resolved fileset
directory and its contents, including directories derived from schema or catalog locations.
Dropping an `EXTERNAL` fileset preserves data at every location. With `disable-filesystem-ops=true`,
dropping either type removes only its metadata. See [Managed and External](./filesets.md#managed-and-external).

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
