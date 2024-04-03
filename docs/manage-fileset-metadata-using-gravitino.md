---
title: "Manage fileset metadata using Gravitino"
slug: /manage-fileset-metadata-using-gravitino
date: 2024-4-2
keyword: Gravitino fileset metadata manage
license: Copyright 2024 Datastrato Pvt Ltd. This software is licensed under the Apache License version 2.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This page introduces how to manage fileset metadata by Gravitino. Fileset is a concept brought
out in Gravitino, which is a collection of files and directories. Users can leverage
fileset to manage non-tabular data like training datasets, raw data.

Typically, a fileset is mapping to a directory on a file system like HDFS, S3, ADLS, GCS, etc.
With fileset managed by Gravitino, the non-tabular data can be managed as assets together with
tabular data and others in Gravitino with a unified way.

After fileset is created, users can easily access, manage the files/directories through
Fileset's identifier, without needing to know the physical path of the managed datasets. Also, with
unified access control mechanism, filesets can also be managed via the same role based access
control mechanism without needing to set access controls to different storages.

To use fileset, please make sure that:

 - Gravitino server is launched, and the host and port is [http://localhost:8090](http://localhost:8090).
 - Metalake has been created.

## Catalog operations

### Create a catalog

:::tip
For fileset catalog, you must specify the catalog `type` as `FILESET` when creating a catalog.
:::

You can create a catalog by sending a `POST` request to the `/api/metalakes/{metalake_name}/catalogs`
endpoint or just use the Gravitino Java client. The following is an example of creating a catalog:

<Tabs>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "catalog",
  "type": "FILESET",
  "comment": "comment",
  "provider": "hadoop",
  "properties": {
    "location": "file:/tmp/root"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://127.0.0.1:8090")
    .withMetalake("metalake")
    .build();

Map<String, String> properties = ImmutableMap.<String, String>builder()
    .put("location", "file:/tmp/root")
    // Property "location" is optional, if specified all the managed fileset without
    // specifying storage location will be stored under this location.
    .build();

Catalog catalog = gravitinoClient.createCatalog(
    NameIdentifier.of("metalake", "catalog"),
    Type.FILESET,
    "hadoop", // provider, Gravitino support only "hadoop" for now.
    "This is a hadoop fileset catalog",
    properties);
// ...
```

</TabItem>
</Tabs>

Currently, Gravitino supports the following catalog providers:

| Catalog provider    | Catalog property                                                        |
|---------------------|-------------------------------------------------------------------------|
| `hadoop`            | [Hadoop catalog property](./hadoop-catalog.md#catalog-properties)       |

### Load a catalog

Please refer to [Load a catalog](./manage-relational-metadata-using-gravitino.md#load-a-catalog)
from relational catalog for more details. For fileset catalog, the load operation is the same.

### Alter a catalog

Please refer to [Alter a catalog](./manage-relational-metadata-using-gravitino.md#alter-a-catalog)
from relational catalog for more details. For fileset catalog, the alter operation is the same.

### Drop a catalog

Please refer to [Drop a catalog](./manage-relational-metadata-using-gravitino.md#drop-a-catalog)
from relational catalog for more details. For fileset catalog, the drop operation is the same.

:::note
Currently, Gravitino doesn't support dropping a catalog with schema and filesets under it. You have
to drop all the schemas and filesets under the catalog before dropping the catalog.
:::

### List all catalogs in a metalake

Please refer to [List all catalogs in a metalake](./manage-relational-metadata-using-gravitino.md#list-all-catalogs-in-a-metalake)
from relational catalog for more details. For fileset catalog, the list operation is the same.

### List all catalogs' information in a metalake

Please refer to [List all catalogs' information in a metalake](./manage-relational-metadata-using-gravitino.md#list-all-catalogs-information-in-a-metalake)
from relational catalog for more details. For fileset catalog, the list operation is the same.

## Schema operations

`Schema` is a virtual namespace in fileset catalog, which is used to organize the filesets. It
is similar to the concept of `schema` in relational catalog.

:::tip
Users should create a metalake and a catalog before creating a schema.
:::

### Create a schema

You can create a schema by sending a `POST` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas`
endpoint or just use the Gravitino Java client. The following is an example of creating a schema:

<Tabs>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "schema",
  "comment": "comment",
  "properties": {
    "location": "file:/tmp/root/schema"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://127.0.0.1:8090")
    .withMetalake("metalake")
    .build();

// Assuming you have just created a Hadoop catalog named `catalog`
Catalog catalog = gravitinoClient.loadCatalog(NameIdentifier.of("metalake", "catalog"));

SupportsSchemas supportsSchemas = catalog.asSchemas();

Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    // Property "location" is optional, if specified all the managed fileset without
    // specifying storage location will be stored under this location.
    .put("location", "file:/tmp/root/schema")
    .build();
Schema schema = supportsSchemas.createSchema(
    NameIdentifier.of("metalake", "catalog", "schema"),
    "This is a schema",
    schemaProperties
);
// ...
```

</TabItem>
</Tabs>

Currently, Gravitino supports the following schema property:

| Catalog provider    | Schema property                                                              |
|---------------------|------------------------------------------------------------------------------|
| `hadoop`            | [Hadoop schema property](./hadoop-catalog.md#schema-properties)              |

### Load a schema

Please refer to [Load a schema](./manage-relational-metadata-using-gravitino.md#load-a-schema)
from relational catalog for more details. For fileset catalog, the schema load operation is the
same.

### Alter a schema

Please refer to [Alter a schema](./manage-relational-metadata-using-gravitino.md#alter-a-schema)
from relational catalog for more details. For fileset catalog, the schema alter operation is the
same.

### Drop a schema

Please refer to [Drop a schema](./manage-relational-metadata-using-gravitino.md#drop-a-schema)
from relational catalog for more details. For fileset catalog, the schema drop operation is the
same.

Note that the drop operation will also remove all the filesets as well as the managed files
under this schema path if `cascade` is set to `true`.

### List all schemas under a catalog

Please refer to [List all schemas under a catalog](./manage-relational-metadata-using-gravitino.md#list-all-schemas-under-a-catalog)
from relational catalog for more details. For fileset catalog, the schema list operation is the
same.

## Fileset operations

:::tip
 - Users should create a metalake, a catalog and a schema before creating a fileset.
 - Current Gravitino only supports managing Hadoop Compatible File System (HCFS) locations.
:::

### Create a fileset

You can create a fileset by sending a `POST` request to the `/api/metalakes/{metalake_name}
/catalogs/{catalog_name}/schemas/{schema_name}/filesets` endpoint or just use the Gravitino Java
client. The following is an example of creating a fileset:

<Tabs>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "example_fileset",
  "comment": "This is an example fileset",
  "type": "MANAGED",
  "storageLocation": "file:/tmp/root/schema/example_fileset",
  "properties": {
    "k1": "v1"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema/filesets
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://127.0.0.1:8090")
    .withMetalake("metalake")
    .build();

Catalog catalog = gravitinoClient.loadCatalog(NameIdentifier.of("metalake", "catalog"));
FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();

Map<String, String> propertiesMap = ImmutableMap.<String, String>builder()
        .put("k1", "v1")
        .build();

filesetCatalog.createFileset(
  NameIdentifier.of("metalake", "catalog", "schema", "example_fileset"),
  "This is an example fileset",
  Fileset.Type.MANAGED,
  "file:/tmp/root/schema/example_fileset",
  propertiesMap,
);
```

</TabItem>
</Tabs>

Currently, Gravitino supports two **types** of the fileset:

 - `MANAGED`: The storage location of the fileset is managed by Gravitino, when specified as
   `MANAGED`, the physical location of the fileset will be deleted when this fileset is dropped.
 - `EXTERNAL`: The storage location of the fileset is **not** managed by Gravitino, when
   specified as `EXTERNAL`, the physical location of the fileset will **not** be deleted when
   this fileset is dropped.

**storageLocation**

The `storageLocation` is the physical location of the fileset, user can specify this location
when creating a fileset, or follow the rule of the catalog/schema location if not specified.

For `MANAGED` fileset, the storage location is:

1. The one specified by user in the fileset creation.
2. When catalog property `location` is specified but schema property `location` is not specified,
   the storage location is `catalog location/schema name/fileset name` if not specified.
3. When catalog property `location` is not specified but schema property `location` is specified,
   the storage location is `schema location/fileset name` if not specified.
4. When both catalog property `location` and schema property `location` are specified, the storage
   location is `schema location/fileset name` if not specified.
5. When both catalog property `location` and schema property `location` are not specified, user
   should specify the `storageLocation` in the fileset creation.

For `EXTERNAL` fileset, users should specify `storageLocation` during fileset creation,
otherwise, Gravitino will throw an exception.

### Alter a fileset

You can modify a fileset by sending a `PUT` request to the `/api/metalakes/{metalake_name}
/catalogs/{catalog_name}/schemas/{schema_name}/filesets/{fileset_name}` endpoint or just use the
Gravitino Java client. The following is an example of modifying a fileset:

<Tabs>
<TabItem value="shell" label="Shell">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "updates": [
    {
      "@type": "removeProperty",
      "property": "key2"
    }, {
      "@type": "setProperty",
      "property": "key3",
      "value": "value3"
    }
  ]
}' http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema/filesets/fileset
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
// Assuming you have just created a Fileset catalog named `catalog`
Catalog catalog = gravitinoClient.loadCatalog(NameIdentifier.of("metalake", "catalog"));

FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();

Fileset f = filesetCatalog.alterFileset(NameIdentifier.of("metalake", "catalog", "schema", "fileset"),
    FilesetChange.rename("fileset_renamed"), FilesetChange.updateComment("xxx"));
// ...
```

</TabItem>
</Tabs>

Currently, Gravitino supports the following changes to a fileset:

| Supported modification             | JSON                                                         | Java                                          |
|------------------------------------|--------------------------------------------------------------|-----------------------------------------------|
| Rename fileset                     | `{"@type":"rename","newName":"fileset_renamed"}`             | `FilesetChange.rename("fileset_renamed")`     |
| Update comment                     | `{"@type":"updateComment","newComment":"new_comment"}`       | `FilesetChange.updateComment("new_comment")`  |
| Set a fileset property             | `{"@type":"setProperty","property":"key1","value":"value1"}` | `FilesetChange.setProperty("key1", "value1")` |
| Remove a fileset property          | `{"@type":"removeProperty","property":"key1"}`               | `FilesetChange.removeProperty("key1")`        |

### Drop a fileset

You can remove a fileset by sending a `DELETE` request to the `/api/metalakes/{metalake_name}
/catalogs/{catalog_name}/schemas/{schema_name}/filesets/{fileset_name}` endpoint or just use the
Gravitino Java client. The following is an example of dropping a fileset:

<Tabs>
<TabItem value="shell" label="Shell">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema/filesets/fileset
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
// Assuming you have just created a Fileset catalog named `catalog`
Catalog catalog = gravitinoClient.loadCatalog(NameIdentifier.of("metalake", "catalog"));

FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();

// Drop a fileset
filesetCatalog.dropFileset(NameIdentifier.of("metalake", "catalog", "schema", "fileset"));
// ...
```

</TabItem>
</Tabs>

For `MANAGED` fileset, the physical location of the fileset will be deleted when this fileset is
dropped. For `EXTERNAL` fileset, only the metadata of the fileset will be removed.

### List filesets

You can list all filesets in a schema by sending a `GET` request to the `/api/metalakes/
{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/filesets` endpoint or just use the
Gravitino Java client. The following is an example of list all the filesets in a schema:

<Tabs>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema/filesets
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
Catalog catalog = gravitinoClient.loadCatalog(NameIdentifier.of("metalake", "catalog"));

FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();
NameIdentifier[] identifiers =
    filesetCatalog.listFilesets(Namespace.ofFileset("metalake", "catalog", "schema"));
// ...
```

</TabItem>
</Tabs>
