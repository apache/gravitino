---
title: "Manage Tags"
slug: "/manage-tags-in-gravitino"
date: 2024-07-24
keyword: "tag management, tag, tags, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

Starting from 0.6.0, Gravitino introduces a new tag system that allows you to manage tags for
metadata objects. Tags are a way to categorize and organize metadata objects in Gravitino.

This document briefly introduces how to use tags in Gravitino by both Gravitino Java client and
REST APIs. If you want to know more about the tag system in Gravitino, refer to the
Javadoc and REST API documentation.

Note that current tag system is a basic implementation, some advanced features will be added in
the future versions.

:::info
1. Metadata objects are objects that are managed in Gravitino, such as `CATALOG`, `SCHEMA`, `TABLE`,
   `COLUMN`, `FILESET`, `TOPIC`, `COLUMN`, `MODEL`, etc. A metadata object is combined by a `type` and a
   dot-separated `name`. For example, a `CATALOG` object has a name "catalog1" with type
   "CATALOG", a `SCHEMA` object has a name "catalog1.schema1" with type "SCHEMA", a `TABLE`
   object has a name "catalog1.schema1.table1" with type "TABLE", a `COLUMN` object has a name 
   "catalog1.schema1.table1.column1" with type "COLUMN".
2`CATALOG`, `SCHEMA`, `TABLE`, `FILESET`, `TOPIC`, `MODEL`, and `COLUMN` objects can be tagged.
3. Tags in Gravitino is inheritable, so listing tags of a metadata object will also list the
   tags of its parent metadata objects. For example, listing tags of a `Table` will also list
   the tags of its parent `Schema` and `Catalog`. For catalogs that support multi-level
   (hierarchical) schemas, such as a schema named `a:b:c` (using the configured schema
   separator), the intermediate parent schemas `a:b` and `a` are also part of the hierarchy, so
   their tags are inherited as well.
4. The same tag can be associated with both parent and child metadata objects. But when you list the
   associated tags of a child metadata object, this tag will be included only once in the result
   list with `inherited` value `false`.
:::

## Tag Operations

### Create New Tags

The first step to manage tags is to create new tags. Create a tag by providing a
name, optional comment, and properties.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "tag1",
  "comment": "This is a tag",
  "properties": {
    "key1": "value1",
    "key2": "value2"
  }
}' http://localhost:8090/api/metalakes/test/tags
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient client = ...
Tag tag =
    client.createTag("tag1", "This is a tag", ImmutableMap.of("key1", "value1", "key2", "value2"));
```

</TabItem>
</Tabs>

### List Tags

List all the tag names as well as tag objects in a metalake in Gravitino.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/metalakes/test/tags

curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/metalakes/test/tags?details=true
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient client = ...
String[] tagNames = client.listTags();

Tag[] tags = client.listTagsInfo();
```

</TabItem>
</Tabs>

### Get a Tag by Name

Get a tag by its name.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/metalakes/test/tags/tag1
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient client = ...
Tag tag = client.getTag("tag1");
```

</TabItem>
</Tabs>

### Update a Tag

Gravitino allows you to update a tag by providing a new tag name, comment and properties.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "updates": [
    {
      "@type": "rename",
      "newName": "tag2"
    },
    {
      "@type": "updateComment",
      "newComment": "This is an updated tag"
    },
    {
      "@type": "setProperty",
      "property": "key3",
      "value": "value3"
    },
    {
      "@type": "removeProperty",
      "property": "key1"
    }
  ]
}' http://localhost:8090/api/metalakes/test/tags/tag1
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient client = ...
Tag tag = client.alterTag(
    "tag1",
    TagChange.rename("tag2"),
    TagChange.updateComment("This is an updated tag"),
    TagChange.setProperty("key3", "value3"),
    TagChange.removeProperty("key1"));
```

</TabItem>
</Tabs>

Gravitino supports the following tag changes:

| Supported modification | JSON                                                         | Java                                      |
|------------------------|--------------------------------------------------------------|-------------------------------------------|
| Rename a tag           | `{"@type":"rename","newName":"tag_renamed"}`                 | `TagChange.rename("tag_renamed")`         |
| Update a comment       | `{"@type":"updateComment","newComment":"new_comment"}`       | `TagChange.updateComment("new_comment")`  |
| Set a tag property     | `{"@type":"setProperty","property":"key1","value":"value1"}` | `TagChange.setProperty("key1", "value1")` |
| Remove a tag property  | `{"@type":"removeProperty","property":"key1"}`               | `TagChange.removeProperty("key1")`        |

### Delete a Tag

Delete a tag by its name.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/metalakes/test/tags/tag2
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient client = ...
client.deleteTag("tag2");
```

</TabItem>
</Tabs>

## Tag Associations

Gravitino lets you associate and disassociate tags with metadata objects. The `CATALOG`, `SCHEMA`, `TABLE`, `FILESET`, `TOPIC`, `MODEL`, and `COLUMN` object types can be tagged.

### Associate and Disassociate Tags with a Metadata Object

Associate and disassociate tags with a metadata object by providing the object type, object
name and tag names.

The request path for REST API is `/api/metalakes/{metalake}/objects/{metadataObjectType}/{metadataObjectName}/tags`.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "tagsToAdd": ["tag1", "tag2"],
  "tagsToRemove": ["tag3"]
}' http://localhost:8090/api/metalakes/test/objects/catalog/catalog1/tags

curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "tagsToAdd": ["tag1"]
}' http://localhost:8090/api/metalakes/test/objects/schema/catalog1.schema1/tags
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog1 = ...
catalog1.supportsTags().associateTags(
    new String[] {"tag1", "tag2"},
    new String[] {"tag3"});

Schema schema1 = ...
schema1.supportsTags().associateTags(new String[] {"tag1"}， null);
```

</TabItem>
</Tabs>

### List Associated Tags for a Metadata Object

List all the tags associated with a metadata object. The tags in Gravitino are
inheritable, so listing tags of a metadata object will also list the tags of its parent metadata
objects, including the intermediate parent schemas of a multi-level (hierarchical) schema.

The request path for REST API is `/api/metalakes/{metalake}/objects/{metadataObjectType}/{metadataObjectName}/tags`.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/metalakes/test/objects/catalog/catalog1/tags

curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/metalakes/test/objects/schema/catalog1.schema1/tags

curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/metalakes/test/objects/catalog/catalog1/tags?details=true

curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/metalakes/test/objects/schema/catalog1.schema1/tags?details=true
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog1 = ...
String[] tags = catalog1.supportsTags().listTags();
Tag[] tagsInfo = catalog1.supportsTags().listTagsInfo();

Schema schema1 = ...
String[] tags = schema1.supportsTags().listTags();
Tag[] tagsInfo = schema1.supportsTags().listTagsInfo();
```

</TabItem>
</Tabs>

### Get an Associated Tag by Name for a Metadata Object

Get an associated tag by its name for a metadata object.

The request path for REST API is `/api/metalakes/{metalake}/objects/{metadataObjectType}/{metadataObjectName}/tags/{tagName}`.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/metalakes/test/objects/catalog/catalog1/tags/tag1

curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/metalakes/test/objects/schema/catalog1.schema1/tags/tag1
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog1 = ...
Tag tag = catalog1.supportsTags().getTag("tag1");

Schema schema1 = ...
Tag tag = schema1.supportsTags().getTag("tag1");
```

</TabItem>
</Tabs>

### List Metadata Objects Associated with a Tag

List all the metadata objects associated with a tag.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/metalakes/test/tags/tag1/objects
```

</TabItem>
<TabItem value="java" label="Java">

```java
Tag tag = ...
MetadataObject[] objects = tag.associatedObjects().objects();
int count = tag.associatedObjects().count();
```

</TabItem>
</Tabs>
