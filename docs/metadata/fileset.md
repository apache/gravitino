---
title: Manage fileset metadata
slug: /manage-fileset-metadata
date: 2024-4-2
keyword: Gravitino fileset metadata manage
license: This software is licensed under the Apache License version 2.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This page introduces how to manage fileset metadata in Apache Gravitino.
Filesets are a collection of files and directories.
Users can leverage filesets to manage non-tabular data like training datasets and other raw data.

Typically, a fileset is mapped to a directory on a file system like HDFS, S3, ADLS, GCS, etc.
With the fileset managed by Gravitino, the non-tabular data can be managed as assets together with
tabular data in Gravitino in a unified way. The examples on this page will use HDFS.
For other HCFS like S3, OSS, GCS, etc., please refer to the corresponding documentation:

- [hadoop-with-s3](../catalogs/fileset/hadoop/s3.md)
- [hadoop-with-oss](../catalogs/fileset/hadoop/oss.md)
- [hadoop-with-gcs](../catalogs/fileset/hadoop/gcs.md)
- [hadoop-with-adls](../catalogs/fileset/hadoop/adls.md)

After a fileset is created, users can easily access, manage the files/directories
through the fileset's identifier, without needing to know the physical path of the managed dataset.
Also, with unified access control mechanism, filesets can be managed
using the same role based access control (RBAC) mechanism without needing
to set access controls across different storage systems.

To use fileset, please make sure that:

- The Gravitino server has started and is serving at [http://localhost:8090](http://localhost:8090).
- A metalake has been created and [enabled](../admin/metalake.md#enable-a-metalake).

## Catalog operations

### Create a catalog

:::tip
For a fileset catalog, you must set the catalog `type` to `FILESET` when creating the catalog.
:::

You can create a catalog by sending a `POST` request
to the `/api/metalakes/{metalake}/catalogs` endpoint
or just use the Gravitino Java client.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
cat <<EOF >catalog.json
{
  "name": "catalog",
  "type": "FILESET",
  "comment": "comment",
  "provider": "hadoop",
  "properties": {
    "location": "file:///tmp/root"
  }
}
EOF

curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@catalog.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs

```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("mymetalake")
    .build();

// Property `location` is optional. If specified, a managed fileset without
// a storage location will be stored at this location.
Map<String, String> properties = ImmutableMap.<String, String>builder()
    .put("location", "file:///tmp/root")
    .build();

// The second parameter is the `provider`.
Catalog catalog = gravitinoClient.createCatalog("catalog",
    Type.FILESET,
    "hadoop",
    "This is a Hadoop fileset catalog",
    properties);
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090", metalake_name="mymetalake")
catalog = client.create_catalog(
    name="catalog", catalog_type=Catalog.Type.FILESET,
    provider="hadoop", 
    comment="This is a Hadoop fileset catalog",
    properties={"location": "/tmp/test1"})
```

</TabItem>
</Tabs>

Currently, Gravitino supports the following catalog providers:

<table>
<thead>
<tr>
  <th>Catalog provider</th>
  <th>Catalog properties</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>hadoop</tt></td>
  <td>[Link](../catalogs/fileset/hadoop/hadoop-catalog.md#catalog-properties)</td>
</tr>
</tbody>
</table>

### Load a catalog

Refer to [loading catalog](./relational.md#load-a-catalog) for a relational catalog.

### Alter a catalog

Refer to [altering a catalog](./relational.md#alter-a-catalog) for a relational catalog.

### Drop a catalog

Refer to [dropping a catalog](./relational.md#drop-a-catalog) for a relational catalog.

:::note
Currently, Gravitino doesn't support dropping a catalog with schemas and filesets under it.
You have to drop all the schemas and filesets under the catalog before dropping the catalog.
:::

### List all catalogs in a metalake

Refer to [list all catalogs](./relational.md#list-all-catalogs-in-a-metalake) for a relational catalog.

### List all catalogs' information in a metalake

Refer to [listing all catalogs' information](./relational.md#list-all-catalogs-information-in-a-metalake)
for a relational catalog.

## Schema operations

A *schema*in a fileset catalog is a virtual namespace for organizing the filesets.
It is similar to the concept of `schema` in relational catalog.

:::tip
Users should create a metalake and a catalog before creating a schema.
:::

### Create a schema

You can create a schema by sending a `POST` request to the
`/api/metalakes/{metalake}/catalogs/{catalog}/schemas`
endpoint or just use the Gravitino Java client.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
cat <<EOF >schema.json
{
  "name": "myschema",
  "comment": "comment",
  "properties": {
    "location": "file:///tmp/root/schema"
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

// This assumes that you have a Hadoop catalog named `mycatalog`
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
SupportsSchemas supportsSchemas = catalog.asSchemas();

// The property "location" is optional.
// If specified all the managed fileset withoutspecifying storage location
// will be stored at this location.
Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    .put("location", "file:///tmp/root/schema")
    .build();
Schema schema = supportsSchemas.createSchema("myschema",
    "This is a schema",
    schemaProperties
);
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="mymetalake")

catalog = client.load_catalog(name="mycatalog")
catalog.as_schemas().create_schema(
    name="myschema", comment="This is a schema",
    properties={"location": "/tmp/root/schema"})
```
</TabItem>
</Tabs>

Currently, Gravitino supports the following schema property:

<table>
<thead>
<tr>
  <th>Catalog provider</th>
  <th>Schema properties</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>hadoop</tt></td>
  <td>[Link](../catalogs/fileset/hadoop/hadoop-catalog.md#schema-properties)</td>
</tr>
</tbody>
</table>

### Load a schema

Refer to [loading a schema](./relational.md#load-a-schema) for a relational catalog.

### Alter a schema

Refer to [altering a schema](./relational.md#alter-a-schema) for a relational catalog.

### Drop a schema

Refer to [dropping a schema](./relational.md#drop-a-schema) for a relational catalog.

:::note
- The *drop* operation will delete all the fileset metadata
  under the specified schema if `cascade` set to `true`.
- For `MANAGED` filesets, a drop operation will also **remove**
  all the files/directories of the specified fileset;
- For `EXTERNAL` fileset, a drop operation will only delete the metadata of the target fileset.

### List all schemas under a catalog

Refer to [listing all schemas](./relational.md#list-all-schemas-under-a-catalog)
for relational catalog.

## Fileset operations

:::tip
- Users should create a metalake, a catalog, and a schema before creating a fileset.
- Currently, Gravitino only supports managing Hadoop Compatible File System (HCFS) locations.
:::

### Create a fileset

You can create a fileset by sending a `POST` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/filesets` endpoint
or just use the Gravitino client SDKs.
The following are examples for creating a fileset with a single storage location:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl <<EOF >fileset.json
{
  "name": "myfileset",
  "comment": "This is an example fileset",
  "type": "MANAGED",
  "storageLocation": "file:///tmp/root/schema/example_fileset",
  "properties": {
    "k1": "v1"
  }
}
EOF

curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@fileset.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/filesets
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("mymetalake")
    .build();

Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();

Map<String, String> propertiesMap = ImmutableMap.<String, String>builder()
        .put("k1", "v1")
        .build();

filesetCatalog.createFileset(
  NameIdentifier.of("myschema", "myfileset"),
  "This is an example fileset",
  Fileset.Type.MANAGED,
  "file:///tmp/root/schema/myfileset",
  propertiesMap,
);
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="mymetalake")

catalog = client.load_catalog(name="mycatalog")
catalog.as_fileset_catalog().create_fileset(
    ident=NameIdentifier.of("myschema", "myfileset"),
    type=Fileset.Type.MANAGED,
    comment="This is an example fileset",
    storage_location="/tmp/root/schema/myfileset",
    properties={"k1": "v1"})
```

</TabItem>
</Tabs>

Currently, Gravitino supports two **types** of filesets:

- `MANAGED`: The storage location of the fileset is managed by Gravitino.
  The physical location of the fileset will be deleted when this fileset is dropped.
- `EXTERNAL`: The storage location of the fileset is **not** managed by Gravitino.
  The files of the fileset will **not** be deleted when the fileset is dropped.

During fileset creation or deletion, Gravitino automatically creates or removes the filesystem directories
corresponding to the fileset locations.
This behavior is skipped in either of the following cases:

1. When the catalog property `disable-filesystem-ops` is set to `true`;
1. When the location contains [placeholders](./manage-fileset-metadata-using-gravitino.md#placeholder).
:::


#### storageLocation

The `storageLocation` is the physical location of the fileset.
Users can specify this location when creating a fileset,
or follow the rules of the catalog/schema location if not specified.

For a `MANAGED` fileset, the storage location is determined in the following priority order:

1. If the user specifies `storageLocation` during fileset creation:
   - This location is used, with [placeholders](#placeholder)
     replaced by the corresponding fileset property values.

1. If the user doesn't specify `storageLocation`:

   - If schema property `location` is specified:
      - Use `<schema location>/<fileset name>` if schema location has no placeholders
      - Use `<schema location>` with placeholders replaced by fileset property values

   - If catalog property `location` is specified:

      - Use `<catalog location>/<schema name>/<fileset name>` if catalog location has no placeholders
      - Use `<catalog location>` with placeholders replaced by fileset property values

   - If neither schema nor catalog location is specified:

      - The user must provide `storageLocation` during fileset creation

For an `EXTERNAL` fileset, the user must always specify `storageLocation` during fileset creation. 
If the provided location contains placeholders, the placeholders will be replaced
by the corresponding fileset property values.

#### placeholder

The `storageLocation` in each level can contain **placeholders**, format as `\{\{name}}`,
which will be replaced by the corresponding fileset property value when the fileset object is created.
The placeholder property in the fileset object is formed as `placeholder-{{name}}`.
For example, if the `storageLocation` is `file:///tmp/{{schema}}-{{fileset}}-{{version}}`,
and the fileset object named "catalog1.schema1.fileset1" contains the property `placeholder-version=v1`, 
the actual `storageLocation` will be `file:///tmp/schema1-fileset1-v1`.

The following is an example of creating a fileset with placeholders in the `storageLocation`:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
# create a catalog first
cat <<EOF >catalog.json
{
  "name": "mycatalog",
  "type": "FILESET",
  "comment": "comment",
  "provider": "hadoop",
  "properties": {
    "location": "file:///{{catalog}}/{{schema}}/workspace_{{project}}/{{user}}"
  }
}
EOF

curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json"
  -d '@catalog.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs

# create a schema under the catalog
cat <<EOF >schema.json
{
  "name": "myschema",
  "comment": "comment",
  "properties": {}
}
EOF

curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@schema.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas

# create a fileset by placeholders
cat <<EOF >fileset.json
{
  "name": "myfileset",
  "comment": "This is an example fileset",
  "type": "MANAGED",
  "properties": {
    "placeholder-project": "test_project",
    "placeholder-user": "test_user"
  }
}
EOF

# the actual storage location of the fileset will be:
# file:///mycatalog/myschema/workspace_test_project/test_user
curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@fileset.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/filesets
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("mymetalake")
    .build();
// create a catalog first
Catalog catalog = gravitinoClient.createCatalog(
    "mycatalog",
    Type.FILESET,
    "hadoop", // provider
    "comment",
    ImmutableMap.of("location", "file:///{{catalog}}/{{schema}}/workspace_{{project}}/{{user}}"));
FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();

// create a schema under the catalog
filesetCatalog.createSchema("myschema", "comment", null);

// create a fileset by placeholders
// the actual storage location of the fileset will be:
// file:///test_catalog/test_schema/workspace_test_project/test_user
filesetCatalog.createFileset(
  NameIdentifier.of("myschema", "myfileset"),
  "This is an example fileset",
  Fileset.Type.MANAGED,
  null,
  ImmutableMap.of("placeholder-project", "test_project", "placeholder-user", "test_user")
);

```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="mymetalake")

# create a catalog first
catalog = client.create_catalog(
    name="mycatalog",
    catalog_type=Catalog.Type.FILESET,
    provider="hadoop",
    comment="comment",
    properties={
        "location": "file:///{{catalog}}/{{schema}}/workspace_{{project}}/{{user}}"
    })

# create a schema under the catalog
catalog.as_schemas().create_schema(
    name="myschema",
    comment="comment",
    properties={})

# create a fileset by placeholders
# the actual storage location of the fileset will be:
# file:///test_catalog/test_schema/workspace_test_project/test_user
catalog.as_fileset_catalog().create_fileset(
    ident=NameIdentifier.of("myschema", "myfileset"),
    type=Fileset.Type.MANAGED,
    comment="This is an example fileset",
    storage_location=None,
    properties={
        "placeholder-project": "test_project",
        "placeholder-user": "test_user"
    })
```
</TabItem>
</Tabs>


#### storageLocations

You can also create a fileset with multiple storage locations.
The `storageLocations` is a map of location name to storage location.
The generation rules of each location follow the generation rules of a single location.
The following is an example of creating a fileset with multiple storage locations:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
# create a catalog first
cat <<EOF > catalog.json
{
  "name": "mycatalog",
  "type": "FILESET",
  "comment": "comment",
  "provider": "hadoop",
  "properties": {
    "filesystem-providers": "builtin-local,builtin-hdfs,s3,gcs",
    "location-l1": "file:///{{catalog}}/{{schema}}/workspace_{{project}}/{{user}}",
    "location-l2": "hdfs:///{{catalog}}/{{schema}}/workspace_{{project}}/{{user}}"
  }
}
EOF

curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@catalog.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs

# create a schema under the catalog
cat <<EOF > schema.json
{
  "name": "myschema",
  "comment": "comment",
  "properties": {
    "location-l3": "s3a://myBucket/{{catalog}}/{{schema}}/workspace_{{project}}/{{user}}"
  }
}
EOF

curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@schema.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas

# create a fileset by placeholders
cat <<EOF > fileset.json
{
  "name": "myfileset",
  "comment": "This is an example fileset",
  "type": "MANAGED",
  "storageLocations": {
    "l4": "gs://myBucket/{{catalog}}/{{schema}}/workspace_{{project}}/{{user}}"
  },
  "properties": {
    "placeholder-project": "test_project",
    "placeholder-user": "test_user",
    "default-location-name": "l1"
  }
}
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@fileset.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/filesets
```

The fileset will be created with 4 storage locations, as shown below.

```json
{
  "name": "myfileset",
  "comment": "This is an example fileset",
  "type": "MANAGED",
  "storageLocation": null,
  "storageLocations": {
    "l1": "file:///mycatalog/myschema/workspace_test_project/test_user",
    "l2": "hdfs:///mycatalog/myschema/workspace_test_project/test_user",
    "l3": "s3a://myBucket/mycatalog/myschema/workspace_test_project/test_user",
    "l4": "gs://myBucket/mycatalog/myschema/workspace_test_project/test_user"
  },
  "properties": {
    "placeholder-project": "test_project",
    "placeholder-user": "test_user",
    "default-location-name": "l1"
  }
}
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("mymetalake")
    .build();

// create a catalog first
Catalog catalog = gravitinoClient.createCatalog(
    "mycatalog",
    Type.FILESET,
    "hadoop", // provider
    "comment",
    ImmutableMap.of(
        "filesystem-providers", "builtin-local,builtin-hdfs,s3,gcs",
        "location-l1", "file:///{{catalog}}/{{schema}}/workspace_{{project}}/{{user}}",
        "location-l2", "hdfs:///{{catalog}}/{{schema}}/workspace_{{project}}/{{user}}"));

FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();

// create a schema under the catalog
filesetCatalog.createSchema(
    "myschema",
    "comment",
    ImmutableMap.of("location-l3", "s3a://myBucket/{{catalog}}/{{schema}}/workspace_{{project}}/{{user}}"));

// create a fileset with placeholders
filesetCatalog.createMultipleLocationFileset(
  NameIdentifier.of("myschema", "myfileset"),
  "This is an example fileset",
  Fileset.Type.MANAGED,
  ImmutableMap.of("l4", "gs://myBucket/{{catalog}}/{{schema}}/workspace_{{project}}/{{user}}"),
  ImmutableMap.of(
      "placeholder-project", "test_project", 
      "placeholder-user", "test_user",
      "default-location-name", "l1")
);
```

The fileset will be created with 4 storage locations, as shown below:

```json
{
  "name": "myfileset",
  "comment": "This is an example fileset",
  "type": "MANAGED",
  "storageLocation": null,
  "storageLocations": {
    "l1": "file:///mycatalog/myschema/workspace_test_project/test_user",
    "l2": "hdfs:///mycatalog/myschema/workspace_test_project/test_user",
    "l3": "s3a://myBucket/mycatalog/myschema/workspace_test_project/test_user",
    "l4": "gs://myBucket/mycatalog/myschema/workspace_test_project/test_user"
  },
  "properties": {
    "placeholder-project": "test_project",
    "placeholder-user": "test_user",
    "default-location-name": "l1"
  }
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(
    uri="http://localhost:8090",
    metalake_name="mymetalake")

# create a catalog first
catalog  = client.create_catalog(
   name="mycatalog",
   catalog_type=Catalog.Type.FILESET,
   provider="hadoop",
   comment="comment",
   properties={
      "filesystem-providers": "builtin-local,builtin-hdfs,s3,gcs",
      "location-l1": "file:///{{catalog}}/{{schema}}/workspace_{{project}}/{{user}}",
      "location-l2": "hdfs:///{{catalog}}/{{schema}}/workspace_{{project}}/{{user}}",
    }
)

# create a schema under the catalog
catalog.as_schemas().create_schema(
   name="myschema",
   comment="comment",
   properties={
      "location-l3": "s3a://myBucket/{{catalog}}/{{schema}}/workspace_{{project}}/{{user}}",
   }
)

# create a fileset by placeholders
catalog.as_fileset_catalog().create_multiple_location_fileset(
    ident=NameIdentifier.of("myschema", "myfileset"),
    type=Fileset.Type.MANAGED,
    comment="This is an example fileset",
    storage_locations={
        "l4": "gs://myBucket/{{catalog}}/{{schema}}/workspace_{{project}}/{{user}}",
    },
    properties={
       "placeholder-project": "test_project",
       "placeholder-user": "test_user",
       "default-location-name": "l1",
    }
)
```

The fileset will be created with 4 storage locations, as shown below:

```json
{
  "name": "myfileset",
  "comment": "This is an example fileset",
  "type": "MANAGED",
  "storageLocation": null,
  "storageLocations": {
    "l1": "file:///mycatalog/myschema/workspace_test_project/test_user",
    "l2": "hdfs:///mycatalog/myschema/workspace_test_project/test_user",
    "l3": "s3a://myBucket/mycatalog/myschema/workspace_test_project/test_user",
    "l4": "gs://myBucket/mycatalog/myschema/workspace_test_project/test_user"
  },
  "properties": {
    "placeholder-project": "test_project",
    "placeholder-user": "test_user",
    "default-location-name": "l1"
  }
}
```

</TabItem>
</Tabs>

### Alter a fileset

You can modify a fileset by sending a `PUT` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/filesets/{fileset}` endpoint
or just use the Gravitino client SDKs.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
cat <<EOF >update.json
{
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
}
EOF

curl -X PUT \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@update.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/filesets/myfileset
```

</TabItem>
<TabItem value="java" label="Java">

```java
// This assumes that you have a Fileset catalog named `mycatalog`,
// a schema named `myschema` and a fileset named `myfileset`.
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();

Fileset f = filesetCatalog.alterFileset(
    NameIdentifier.of("myschema", "myfileset"),
    FilesetChange.rename("new_name"),
    FilesetChange.updateComment("new comment"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="mymetalake")
catalog = gravitino_client.load_catalog(name="mycatalog")
changes = (
    FilesetChange.remove_property("key1"),
    FilesetChange.set_property("key2", "new_value"),
)
fileset_new = catalog.as_fileset_catalog().alter_fileset(
    NameIdentifier.of("myschema", "myfileset"),
    *changes)
```

</TabItem>
</Tabs>

Currently, Gravitino supports the following changes to a fileset:

<table>
<thead>
<tr>
  <th>Supported modification</th>
  <th>JSON payload</th>
  <th>Java methods</th>
</tr>
</thead>
<tbody>
<tr>
  <td>Rename fileset</td>
  <td>`{"@type":"rename","newName":"new-name"}`</td>
  <td>`FilesetChange.rename("new-name")`</td>
</tr>
<tr>
  <td>Update fileset comment</td>
  <td>`{"@type":"updateComment","newComment":"new comment"}`</td>
  <td>`FilesetChange.updateComment("new comment")`</td>
</tr>
<tr>
  <td>Remove fileset comment (deprecated)</td>
  <td>`{"@type":"removeComment"}`</td>
  <td>`FilesetChange.removeComment()`</td>
</tr>
<tr>
  <td>Set fileset property</td>
  <td>`{"@type":"setProperty","property":"key1","value":"value1"}`</td>
  <td>`FilesetChange.setProperty("key1", "value1")`</td>
</tr>
<tr>
  <td>Remove a fileset property</td>
  <td>`{"@type":"removeProperty","property":"key1"}`</td>
  <td>`FilesetChange.removeProperty("key1")`</td>
</tr>
</tbody>
</table>

### Drop a fileset

You can remove a fileset by sending a `DELETE` request
to the `/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/filesets/{fileset}` endpoint
or by using the Gravitino client SDKs.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X DELETE \
  -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/filesets/myfileset
```

</TabItem>
<TabItem value="java" label="Java">

```java
// This assumes that you have a Fileset catalog named `catalog`,
// a schema named `myschema` and a fileset named `myfileset`.
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();
filesetCatalog.dropFileset(NameIdentifier.of("myschema", "myfileset"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="mymetalake")

catalog = client.load_catalog(name="mycatalog")
catalog.as_fileset_catalog().drop_fileset(
    ident=NameIdentifier.of("myschema", "myfileset"))
```

</TabItem>
</Tabs>

For a `MANAGED` fileset, the physical location of the fileset will be deleted when this fileset is dropped.
For `EXTERNAL` fileset, only the metadata of the fileset will be removed.

### List filesets

You can list all filesets in a schema by sending a `GET` request to the
`/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/filesets` endpoint
or by using the Gravitino client SDKs.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET \
  -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/filesets
```
</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();
NameIdentifier[] identifiers = filesetCatalog.listFilesets(Namespace.of("myschema"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="mymetalake")

catalog = client.load_catalog(name="mycatalog")
fileset_list = catalog.as_fileset_catalog().list_filesets(
    namespace=Namespace.of("myschema")))
```
</TabItem>
</Tabs>

