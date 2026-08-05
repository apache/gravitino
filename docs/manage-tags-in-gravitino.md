---
title: "Manage Tags"
slug: "/manage-tags-in-gravitino"
keyword: "tag management, tag, tags, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

This page covers the Gravitino API for tags. For what a tag is, which object types can carry one, how inheritance
resolves, and how to work with tags in the UI, see [Tags](./tags.md).

## Tag Operations

### Create a Tag

A tag needs a name, and can carry a comment and properties.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "pii",
  "comment": "Personally identifiable information",
  "properties": {"owner": "data-governance"}
}' http://localhost:8090/api/metalakes/test/tags
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient client = ...
Tag tag = client.createTag(
    "pii",
    "Personally identifiable information",
    ImmutableMap.of("owner", "data-governance"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
tag = client.create_tag(
    tag_name="pii",
    comment="Personally identifiable information",
    properties={"owner": "data-governance"})
```

</TabItem>
</Tabs>

### List Tags

Listing returns names, or full tag objects when `details=true` is set.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/test/tags

curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  "http://localhost:8090/api/metalakes/test/tags?details=true"
```

</TabItem>
<TabItem value="java" label="Java">

```java
String[] tagNames = client.listTags();
Tag[] tags = client.listTagsInfo();
```

</TabItem>
<TabItem value="python" label="Python">

```python
tag_names = client.list_tags()
tags = client.list_tags_info()
```

</TabItem>
</Tabs>

### Get a Tag

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/test/tags/pii
```

</TabItem>
<TabItem value="java" label="Java">

```java
Tag tag = client.getTag("pii");
```

</TabItem>
<TabItem value="python" label="Python">

```python
tag = client.get_tag("pii")
```

</TabItem>
</Tabs>

### Alter a Tag

Changes are applied as a list in one request.

| Change             | JSON                                                         | Java                                      | Python                                       |
|--------------------|--------------------------------------------------------------|-------------------------------------------|----------------------------------------------|
| Rename             | `{"@type":"rename","newName":"tag_renamed"}`                 | `TagChange.rename("tag_renamed")`         | `TagChange.rename("tag_renamed")`            |
| Update the comment | `{"@type":"updateComment","newComment":"new_comment"}`       | `TagChange.updateComment("new_comment")`  | `TagChange.update_comment("new_comment")`    |
| Set a property     | `{"@type":"setProperty","property":"key1","value":"value1"}` | `TagChange.setProperty("key1", "value1")` | `TagChange.set_property("key1", "value1")`   |
| Remove a property  | `{"@type":"removeProperty","property":"key1"}`               | `TagChange.removeProperty("key1")`        | `TagChange.remove_property("key1")`          |

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "updates": [
    {"@type": "updateComment", "newComment": "Reviewed quarterly"},
    {"@type": "setProperty", "property": "owner", "value": "privacy-office"}
  ]
}' http://localhost:8090/api/metalakes/test/tags/pii
```

</TabItem>
<TabItem value="java" label="Java">

```java
Tag tag = client.alterTag(
    "pii",
    TagChange.updateComment("Reviewed quarterly"),
    TagChange.setProperty("owner", "privacy-office"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
tag = client.alter_tag(
    "pii",
    TagChange.update_comment("Reviewed quarterly"),
    TagChange.set_property("owner", "privacy-office"))
```

</TabItem>
</Tabs>

### Delete a Tag

Deleting a tag also removes it from every object it was attached to.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/test/tags/pii
```

</TabItem>
<TabItem value="java" label="Java">

```java
client.deleteTag("pii");
```

</TabItem>
<TabItem value="python" label="Python">

```python
client.delete_tag("pii")
```

</TabItem>
</Tabs>

## Object Operations

### Attach and Detach Tags

Both happen in one request, and either list can be omitted. The object type and full name go in the
path, so the same call covers catalogs, schemas, tables, views, columns, filesets, topics, models,
and functions.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "tagsToAdd": ["pii"],
  "tagsToRemove": ["unreviewed"]
}' http://localhost:8090/api/metalakes/test/objects/table/catalog1.schema1.customers/tags

curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "tagsToAdd": ["pii"]
}' http://localhost:8090/api/metalakes/test/objects/fileset/catalog1.schema1.raw_events/tags
```

</TabItem>
<TabItem value="java" label="Java">

```java
Table customers = ...
customers.supportsTags().associateTags(
    new String[] {"pii"},
    new String[] {"unreviewed"});

Fileset rawEvents = ...
rawEvents.supportsTags().associateTags(new String[] {"pii"}, null);
```

</TabItem>
<TabItem value="python" label="Python">

```python
customers = ...
customers.supports_tags().associate_tags(["pii"], ["unreviewed"])

raw_events = ...
raw_events.supports_tags().associate_tags(["pii"], None)
```

</TabItem>
</Tabs>

### List Tags on an Object

The response includes tags inherited from ancestors. With `details=true` each tag carries an
`inherited` field, which a plain name listing does not.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  "http://localhost:8090/api/metalakes/test/objects/table/catalog1.schema1.customers/tags?details=true"
```

</TabItem>
<TabItem value="java" label="Java">

```java
Table customers = ...
String[] tagNames = customers.supportsTags().listTags();
Tag[] tags = customers.supportsTags().listTagsInfo();
```

</TabItem>
<TabItem value="python" label="Python">

```python
customers = ...
tag_names = customers.supports_tags().list_tags()
tags = customers.supports_tags().list_tags_info()
```

</TabItem>
</Tabs>

### Get One Tag on an Object

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/test/objects/table/catalog1.schema1.customers/tags/pii
```

</TabItem>
<TabItem value="java" label="Java">

```java
Tag tag = customers.supportsTags().getTag("pii");
```

</TabItem>
<TabItem value="python" label="Python">

```python
tag = customers.supports_tags().get_tag("pii")
```

</TabItem>
</Tabs>

### List Objects Carrying a Tag

The response lists direct attachments only, so a tag attached to a catalog returns that catalog
rather than the objects beneath it.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/test/tags/pii/objects
```

</TabItem>
<TabItem value="java" label="Java">

```java
Tag tag = client.getTag("pii");
MetadataObject[] objects = tag.associatedObjects().objects();
int count = tag.associatedObjects().count();
```

</TabItem>
</Tabs>
