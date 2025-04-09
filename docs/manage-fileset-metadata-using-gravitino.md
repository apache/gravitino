---
title: Manage fileset metadata using Gravitino
slug: /manage-fileset-metadata-using-gravitino
date: 2024-4-2
keyword: Gravitino fileset metadata manage
license: This software is licensed under the Apache License version 2.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This page introduces how to manage fileset metadata in Apache Gravitino. Filesets 
are a collection of files and directories. Users can leverage
filesets to manage non-tabular data like training datasets and other raw data.

Typically, a fileset is mapped to a directory on a file system like HDFS, S3, ADLS, GCS, etc.
With the fileset managed by Gravitino, the non-tabular data can be managed as assets together with
tabular data in Gravitino in a unified way. The following operations will use HDFS as an example, for other
HCFS like S3, OSS, GCS, etc., please refer to the corresponding operations [hadoop-with-s3](./hadoop-catalog-with-s3.md), [hadoop-with-oss](./hadoop-catalog-with-oss.md), [hadoop-with-gcs](./hadoop-catalog-with-gcs.md) and 
[hadoop-with-adls](./hadoop-catalog-with-adls.md).

After a fileset is created, users can easily access, manage the files/directories through
the fileset's identifier, without needing to know the physical path of the managed dataset. Also, with
unified access control mechanism, filesets can be managed via the same role based access
control mechanism without needing to set access controls across different storage systems.

To use fileset, please make sure that:

 - Gravitino server has started, and the host and port is [http://localhost:8090](http://localhost:8090).
 - A metalake has been created and [enabled](./manage-metalake-using-gravitino.md#enable-a-metalake)

## Catalog operations

### Create a catalog

:::tip
For a fileset catalog, you must specify the catalog `type` as `FILESET` when creating the catalog.
:::

You can create a catalog by sending a `POST` request to the `/api/metalakes/{metalake_name}/catalogs`
endpoint or just use the Gravitino Java client. The following is an example of creating a catalog:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "catalog",
  "type": "FILESET",
  "comment": "comment",
  "provider": "hadoop",
  "properties": {
    "location": "file:///tmp/root"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs

```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("metalake")
    .build();

Map<String, String> properties = ImmutableMap.<String, String>builder()
    .put("location", "file:///tmp/root")
    // Property "location" is optional. If specified, a managed fileset without
    // a storage location will be stored under this location.
    .build();

Catalog catalog = gravitinoClient.createCatalog("catalog",
    Type.FILESET,
    "hadoop", // provider, Gravitino only supports "hadoop" for now.
    "This is a Hadoop fileset catalog",
    properties);
// ...

```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="metalake")
catalog = gravitino_client.create_catalog(name="catalog",
                                          catalog_type=Catalog.Type.FILESET,
                                          provider="hadoop", 
                                          comment="This is a Hadoop fileset catalog",
                                          properties={"location": "/tmp/test1"})
```

</TabItem>
</Tabs>

Currently, Gravitino supports the following catalog providers:

| Catalog provider    | Catalog property                                                        |
|---------------------|-------------------------------------------------------------------------|
| `hadoop`            | [Hadoop catalog property](./hadoop-catalog.md#catalog-properties)       |

### Load a catalog

Refer to [Load a catalog](./manage-relational-metadata-using-gravitino.md#load-a-catalog)
in relational catalog for more details. For a fileset catalog, the load operation is the same.

### Alter a catalog

Refer to [Alter a catalog](./manage-relational-metadata-using-gravitino.md#alter-a-catalog)
in relational catalog for more details. For a fileset catalog, the alter operation is the same.

### Drop a catalog

Refer to [Drop a catalog](./manage-relational-metadata-using-gravitino.md#drop-a-catalog)
in relational catalog for more details. For a fileset catalog, the drop operation is the same.

:::note
Currently, Gravitino doesn't support dropping a catalog with schemas and filesets under it. You have
to drop all the schemas and filesets under the catalog before dropping the catalog.
:::

### List all catalogs in a metalake

Please refer to [List all catalogs in a metalake](./manage-relational-metadata-using-gravitino.md#list-all-catalogs-in-a-metalake)
in relational catalog for more details. For a fileset catalog, the list operation is the same.

### List all catalogs' information in a metalake

Please refer to [List all catalogs' information in a metalake](./manage-relational-metadata-using-gravitino.md#list-all-catalogs-information-in-a-metalake)
in relational catalog for more details. For a fileset catalog, the list operation is the same.

## Schema operations

`Schema` is a virtual namespace in a fileset catalog, which is used to organize the fileset. It
is similar to the concept of `schema` in relational catalog.

:::tip
Users should create a metalake and a catalog before creating a schema.
:::

### Create a schema

You can create a schema by sending a `POST` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas`
endpoint or just use the Gravitino Java client. The following is an example of creating a schema:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "schema",
  "comment": "comment",
  "properties": {
    "location": "file:///tmp/root/schema"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("metalake")
    .build();

// Assuming you have just created a Hadoop catalog named `catalog`
Catalog catalog = gravitinoClient.loadCatalog("catalog");

SupportsSchemas supportsSchemas = catalog.asSchemas();

Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    // Property "location" is optional, if specified all the managed fileset without
    // specifying storage location will be stored under this location.
    .put("location", "file:///tmp/root/schema")
    .build();
Schema schema = supportsSchemas.createSchema("schema",
    "This is a schema",
    schemaProperties
);
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="metalake")

catalog: Catalog = gravitino_client.load_catalog(name="catalog")
catalog.as_schemas().create_schema(name="schema", 
                                   comment="This is a schema",
                                   properties={"location": "/tmp/root/schema"})
```

</TabItem>
</Tabs>

Currently, Gravitino supports the following schema property:

| Catalog provider    | Schema property                                                              |
|---------------------|------------------------------------------------------------------------------|
| `hadoop`            | [Hadoop schema property](./hadoop-catalog.md#schema-properties)              |

### Load a schema

Please refer to [Load a schema](./manage-relational-metadata-using-gravitino.md#load-a-schema)
in relational catalog for more details. For a fileset catalog, the schema load operation is the
same.

### Alter a schema

Please refer to [Alter a schema](./manage-relational-metadata-using-gravitino.md#alter-a-schema)
in relational catalog for more details. For a fileset catalog, the schema alter operation is the
same.

### Drop a schema

Please refer to [Drop a schema](./manage-relational-metadata-using-gravitino.md#drop-a-schema)
in relational catalog for more details. For a fileset catalog, the schema drop operation is the
same.

Note that the drop operation will delete all the fileset metadata under this schema if `cascade`
set to `true`. Besides, for `MANAGED` fileset, this drop operation will also **remove** all the
files/directories of this fileset; for `EXTERNAL` fileset, this drop operation will only delete
the metadata of this fileset.

### List all schemas under a catalog

Please refer to [List all schemas under a catalog](./manage-relational-metadata-using-gravitino.md#list-all-schemas-under-a-catalog)
in relational catalog for more details. For a fileset catalog, the schema list operation is the
same.

## Fileset operations

:::tip
 - Users should create a metalake, a catalog, and a schema before creating a fileset.
 - Currently, Gravitino only supports managing Hadoop Compatible File System (HCFS) locations.
:::

### Create a fileset

You can create a fileset by sending a `POST` request to the `/api/metalakes/{metalake_name}
/catalogs/{catalog_name}/schemas/{schema_name}/filesets` endpoint or just use the Gravitino Java
client. The following is an example of creating a fileset:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "example_fileset",
  "comment": "This is an example fileset",
  "type": "MANAGED",
  "storageLocation": "file:///tmp/root/schema/example_fileset",
  "properties": {
    "k1": "v1"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema/filesets
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("metalake")
    .build();

Catalog catalog = gravitinoClient.loadCatalog("catalog");
FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();

Map<String, String> propertiesMap = ImmutableMap.<String, String>builder()
        .put("k1", "v1")
        .build();

filesetCatalog.createFileset(
  NameIdentifier.of("schema", "example_fileset"),
  "This is an example fileset",
  Fileset.Type.MANAGED,
  "file:///tmp/root/schema/example_fileset",
  propertiesMap,
);
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="metalake")

catalog: Catalog = gravitino_client.load_catalog(name="catalog")
catalog.as_fileset_catalog().create_fileset(ident=NameIdentifier.of("schema", "example_fileset"),
                                            type=Fileset.Type.MANAGED,
                                            comment="This is an example fileset",
                                            storage_location="/tmp/root/schema/example_fileset",
                                            properties={"k1": "v1"})
```

</TabItem>
</Tabs>

Currently, Gravitino supports two **types** of filesets:

 - `MANAGED`: The storage location of the fileset is managed by Gravitino when specified as
   `MANAGED`, the physical location of the fileset will be deleted when this fileset is dropped.
 - `EXTERNAL`: The storage location of the fileset is **not** managed by Gravitino, when
   specified as `EXTERNAL`, the files of the fileset will **not** be deleted when
   the fileset is dropped.

**storageLocation**

The `storageLocation` is the physical location of the fileset. Users can specify this location
when creating a fileset, or follow the rules of the catalog/schema location if not specified.

The `storageLocation` in each level can contain **placeholders**, format as `{{name}}`, which will
be replaced by the corresponding fileset property value when the fileset object is created. The
placeholder property in the fileset object is formed as "placeholder-{{name}}". For example, if
the `storageLocation` is `file:///tmp/{{schema}}-{{fileset}}-{{verion}}`, and the fileset object 
named "catalog1.schema1.fileset1" contains the properties `placeholder-version=v1`, 
the actual `storageLocation` will be `file:///tmp/schema1-fileset1-v1`.

The following is an example of creating a fileset with placeholders in the `storageLocation`:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
# create a calota first
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "test_catalog",
  "type": "FILESET",
  "comment": "comment",
  "provider": "hadoop",
  "properties": {
    "location": "file:///{{catalog}}/{{schema}}/workspace_{{project}}/{{user}}"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs

# create a schema under the catalog
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "test_schema",
  "comment": "comment",
  "properties": {}
}' http://localhost:8090/api/metalakes/metalake/catalogs/test_catalog/schemas

# create a fileset by placeholders
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "example_fileset",
  "comment": "This is an example fileset",
  "type": "MANAGED",
  "properties": {
    "placeholder-project": "test_project",
    "placeholder-user": "test_user"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/test_catalog/schemas/test_schema/filesets

# the actual storage location of the fileset will be: file:///test_catalog/test_schema/workspace_test_project/test_user
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("metalake")
    .build();
// create a catalog first
Catalog catalog = gravitinoClient.createCatalog(
    "test_catalog",
    Type.FILESET,
    "hadoop", // provider
    "comment",
    ImmutableMap.of("location", "file:///{{catalog}}/{{schema}}/workspace_{{project}}/{{user}}"));
FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();

// create a schema under the catalog
filesetCatalog.createSchema("test_schema", "comment", null);

// create a fileset by placeholders
filesetCatalog.createFileset(
  NameIdentifier.of("test_schema", "example_fileset"),
  "This is an example fileset",
  Fileset.Type.MANAGED,
  null,
  ImmutableMap.of("placeholder-project", "test_project", "placeholder-user", "test_user")
);

// the actual storage location of the fileset will be: file:///test_catalog/test_schema/workspace_test_project/test_user
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="metalake")

# create a catalog first
catalog: Catalog = gravitino_client.create_catalog(name="test_catalog",
                                                   catalog_type=Catalog.Type.FILESET,
                                                   provider="hadoop",
                                                   comment="comment",
                                                   properties={"location": "file:///{{catalog}}/{{schema}}/workspace_{{project}}/{{user}}"})

# create a schema under the catalog
catalog.as_schemas().create_schema(name="test_schema", comment="comment", properties={})

# create a fileset by placeholders
catalog.as_fileset_catalog().create_fileset(ident=NameIdentifier.of("test_schema", "example_fileset"),
                                            type=Fileset.Type.MANAGED,
                                            comment="This is an example fileset",
                                            storage_location=None,
                                            properties={"placeholder-project": "test_project", "placeholder-user": "test_user"})

# the actual storage location of the fileset will be: file:///test_catalog/test_schema/workspace_test_project/test_user
```

</TabItem>
</Tabs>

The value of `storageLocation` depends on the configuration settings of the catalog:
- If this is a local fileset catalog, the `storageLocation` should be in the format of `file:///path/to/fileset`.
- If this is a HDFS fileset catalog, the `storageLocation` should be in the format of `hdfs://namenode:port/path/to/fileset`.

For a `MANAGED` fileset, the storage location is:

1. The one specified by the user during the fileset creation, and the placeholder will be replaced by the
   corresponding fileset property value.
2. When the catalog property `location` is specified but the schema property `location` isn't specified, the storage location is:
   1. `catalog location/schema name/fileset name` if `catalog location` does not contain any placeholder. 
   2. `catalog location` - placeholders in the catalog location will be replaced by the corresponding fileset property value.

3. When the catalog property `location` isn't specified but the schema property `location` is specified,
   the storage location is:
   1. `schema location/fileset name` if `schema location` does not contain any placeholder.
   2. `schema location` - placeholders in the schema location will be replaced by the corresponding fileset property value.
   
4. When both the catalog property `location` and the schema property `location` are specified, the storage
   location is:
   1. `schema location/fileset name` if `schema location` does not contain any placeholder.
   2. `schema location` - placeholders in the schema location will be replaced by the corresponding fileset property value.

5. When both the catalog property `location` and schema property `location` isn't specified, the user
   should specify the `storageLocation` in the fileset creation.

For `EXTERNAL` fileset, users should specify `storageLocation` during the fileset creation,
otherwise, Gravitino will throw an exception. If the `storageLocation` contains placeholders, the
placeholder will be replaced by the corresponding fileset property value.

### Alter a fileset

You can modify a fileset by sending a `PUT` request to the `/api/metalakes/{metalake_name}
/catalogs/{catalog_name}/schemas/{schema_name}/filesets/{fileset_name}` endpoint or just use the
Gravitino Java client. The following is an example of modifying a fileset:

<Tabs groupId="language" queryString>
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
Catalog catalog = gravitinoClient.loadCatalog("catalog");

FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();

Fileset f = filesetCatalog.alterFileset(NameIdentifier.of("schema", "fileset"),
    FilesetChange.rename("fileset_renamed"), FilesetChange.updateComment("xxx"));
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="metalake")

catalog: Catalog = gravitino_client.load_catalog(name="catalog")
changes = (
   FilesetChange.remove_property("fileset_properties_key1"),
   FilesetChange.set_property("fileset_properties_key2", "fileset_propertie_new_value"),
)
fileset_new = catalog.as_fileset_catalog().alter_fileset(NameIdentifier.of("schema", "fileset"), 
                                                         *changes)
```

</TabItem>
</Tabs>

Currently, Gravitino supports the following changes to a fileset:

| Supported modification      | JSON                                                         | Java                                          |
|-----------------------------|--------------------------------------------------------------|-----------------------------------------------|
| Rename a fileset            | `{"@type":"rename","newName":"fileset_renamed"}`             | `FilesetChange.rename("fileset_renamed")`     |
| Update a comment            | `{"@type":"updateComment","newComment":"new_comment"}`       | `FilesetChange.updateComment("new_comment")`  |
| Set a fileset property      | `{"@type":"setProperty","property":"key1","value":"value1"}` | `FilesetChange.setProperty("key1", "value1")` |
| Remove a fileset property   | `{"@type":"removeProperty","property":"key1"}`               | `FilesetChange.removeProperty("key1")`        |
| Remove comment (deprecated) | `{"@type":"removeComment"}`                                  | `FilesetChange.removeComment()`               |

### Drop a fileset

You can remove a fileset by sending a `DELETE` request to the `/api/metalakes/{metalake_name}
/catalogs/{catalog_name}/schemas/{schema_name}/filesets/{fileset_name}` endpoint or by using the
Gravitino Java client. The following is an example of dropping a fileset:

<Tabs groupId="language" queryString>
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
Catalog catalog = gravitinoClient.loadCatalog("catalog");

FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();

// Drop a fileset
filesetCatalog.dropFileset(NameIdentifier.of("schema", "fileset"));
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="metalake")

catalog: Catalog = gravitino_client.load_catalog(name="catalog")
catalog.as_fileset_catalog().drop_fileset(ident=NameIdentifier.of("schema", "fileset"))
```

</TabItem>
</Tabs>

For a `MANAGED` fileset, the physical location of the fileset will be deleted when this fileset is
dropped. For `EXTERNAL` fileset, only the metadata of the fileset will be removed.

### List filesets

You can list all filesets in a schema by sending a `GET` request to the `/api/metalakes/
{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/filesets` endpoint or by using the
Gravitino Java client. The following is an example of listing all the filesets in a schema:

<Tabs groupId="language" queryString>
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
Catalog catalog = gravitinoClient.loadCatalog("catalog");

FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();
NameIdentifier[] identifiers =
    filesetCatalog.listFilesets(Namespace.of("schema"));
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="metalake")

catalog: Catalog = gravitino_client.load_catalog(name="catalog")
fileset_list: List[NameIdentifier] = catalog.as_fileset_catalog().list_filesets(namespace=Namespace.of("schema")))
```

</TabItem>
</Tabs>
