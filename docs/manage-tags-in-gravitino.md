---
title: "Manage tags in Gravitino"
slug: /manage-tags-in-gravitino
date: 2024-07-24
keyword: tag management, tag, tags, Gravitino
license: This software is licensed under the Apache License version 2.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

Starting from 0.6.0, Gravitino introduces a new tag system that allows you to manage tags for
metadata objects. Tags are a way to categorize and organize metadata objects in Gravitino.

This document briefly introduces how to use tags in Gravitino by both Gravitino Java client and
REST APIs. If you want to know more about the tag system in Gravitino, please refer to the
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
2. Currently, `CATALOG`, `SCHEMA`, `TABLE`, `FILESET`, `TOPIC`, `MODEL`, and `COLUMN` objects can be tagged.
3. Tags in Gravitino is inheritable, so listing tags of a metadata object will also list the
   tags of its parent metadata objects. For example, listing tags of a `Table` will also list
   the tags of its parent `Schema` and `Catalog`.
4. The same tag can be associated with both parent and child metadata objects. But when you list the
   associated tags of a child metadata object, this tag will be included only once in the result
   list with `inherited` value `false`.
:::

## Tag operations

### Create new tags

The first step to manage tags is to create new tags. You can create a new tag by providing a tag
name, optional comment and properties.

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

### List created tags

You can list all the created tag names as well as tag objects in a metalake in Gravitino.

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

### Get a tag by name

You can get a tag by its name.

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

### Update a tag

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

Currently, Gravitino support the following tag changes:

| Supported modification | JSON                                                         | Java                                      |
|------------------------|--------------------------------------------------------------|-------------------------------------------|
| Rename a tag           | `{"@type":"rename","newName":"tag_renamed"}`                 | `TagChange.rename("tag_renamed")`         |
| Update a comment       | `{"@type":"updateComment","newComment":"new_comment"}`       | `TagChange.updateComment("new_comment")`  |
| Set a tag property     | `{"@type":"setProperty","property":"key1","value":"value1"}` | `TagChange.setProperty("key1", "value1")` |
| Remove a tag property  | `{"@type":"removeProperty","property":"key1"}`               | `TagChange.removeProperty("key1")`        |

### Delete a tag

You can delete a tag by its name.

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

## Tag associations

Gravitino allows you to associate and disassociate tags with metadata objects. Currently,
`CATALOG`, `SCHEMA`, `TABLE`, `FILESET`, `TOPIC`, `MODEL`, and `COLUMN` objects can be tagged.

### Associate and disassociate tags with a metadata object

You can associate and disassociate tags with a metadata object by providing the object type, object
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

### List associated tags for a metadata object

You can list all the tags associated with a metadata object. The tags in Gravitino are
inheritable, so listing tags of a metadata object will also list the tags of its parent metadata
objects.

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

### Get an associated tag by name for a metadata object

You can get an associated tag by its name for a metadata object.

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

### List metadata objects associated with a tag

You can list all the metadata objects associated with a tag.

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
