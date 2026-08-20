---
title: "Tags"
slug: "/tags"
keyword: "tag, tags, labels, classification, metadata object, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

A tag is a named label that you create once in a metalake and attach to any number of metadata
objects. A tag carries an optional comment and a set of properties, so it can hold a small amount of
structured detail beyond its name. The same tag can be attached to a catalog, a table, and a single
column at the same time.

Tags travel with the metadata rather than with the data, so a tag applied to a table is visible to
every engine and every client that reaches that table through Gravitino, no matter which system
actually stores it. That makes a tag the practical way to say something once about metadata that
lives in several catalogs at once.

Gravitino stores tags, resolves them down the metadata hierarchy, and shows them wherever the object
appears. Common uses:

- Recording a classification once, on the catalog or schema, and having every table and column
  beneath it carry that classification without further work
- Answering coverage questions across catalogs you do not own, such as which objects anywhere in the
  metalake are marked as personal data
- Carrying a classification that arrived from another catalog through to the engines and clients that
  read metadata from Gravitino
- Marking objects for a downstream consumer to act on, such as a job that reads the tags on an object
  before deciding what to do with it

## Quick Start

**1. Create the tag.** Tags are created from the tag list in the UI. A tag needs a name, and can
also carry a comment and any properties you want to keep with it.

**2. Attach it to an object.** Open the catalog, schema, or table you want to label and add the tag
from its tag control. Only tags that already exist in the metalake are offered, so create the tag
first and attach it second.

**3. Label a single column.** A table's column list carries its own tag control on each row, so a
tag can sit on one column without applying to the rest of the table.

**4. See where the tag is attached.** Selecting a tag name in the tag list shows the objects it is
attached to directly.

## The Tag Model

### What Can Carry a Tag

A metadata object is identified by a type and a name, with each level below the catalog separated by
a dot. Nine object types can carry a tag.

| Object type | Name form                                                 |
|-------------|-----------------------------------------------------------|
| `CATALOG`   | `{catalog_name}`                                          |
| `SCHEMA`    | `{catalog_name}.{schema_name}`                            |
| `TABLE`     | `{catalog_name}.{schema_name}.{table_name}`               |
| `VIEW`      | `{catalog_name}.{schema_name}.{view_name}`                |
| `COLUMN`    | `{catalog_name}.{schema_name}.{table_name}.{column_name}` |
| `FILESET`   | `{catalog_name}.{schema_name}.{fileset_name}`             |
| `TOPIC`     | `{catalog_name}.{schema_name}.{topic_name}`               |
| `MODEL`     | `{catalog_name}.{schema_name}.{model_name}`               |
| `FUNCTION`  | `{catalog_name}.{schema_name}.{function_name}`            |

A metalake cannot carry a tag, so there is no single attachment point that covers everything at
once. To reach every object in a catalog, attach the tag to the catalog.

The UI attaches tags on catalogs, schemas, tables, and columns. For the other types, use the
REST API described at the end of this page.

### Names, Properties, and Assignment Values

A tag name is unique within its metalake and is the identifier used everywhere else, so renaming a
tag changes what every stored request has to ask for.

A name is up to 64 characters of letters, digits, underscores, slashes, equals signs, and hyphens.
A separator convention such as `pii/email` keeps a growing set readable.

Properties are free-form key and value pairs on the tag itself rather than on the attachment, so
every object carrying the tag sees the same values. Properties suit facts about the tag, such as
which team owns it or which external system it came from. Properties do not suit facts about one
tagged object.

Assignment values describe one tag on one object. For example, the same `data_domain` tag can have
the value `finance` on one table and `risk` on another. An assignment can have no value, one value,
or several values. Adding a value is incremental, so adding `risk` to an assignment that already has
`finance` leaves both values in place.

When you create a tag, you can choose one of three value constraints:

| Constraint     | Meaning                                                                        |
| -------------- | ------------------------------------------------------------------------------ |
| Any value      | The tag accepts any non-blank string, and can also be assigned without a value |
| No value       | The tag can only be assigned without a value                                   |
| Allowed values | The tag accepts only values from the configured list                           |

Values are case-sensitive strings of up to 256 characters. The constraint cannot be changed after
the tag is created. Tag properties and assignment values are separate: changing a property affects
the tag everywhere, while changing an assignment value affects only that object. Assignment values
are currently managed through the REST API or the Java and Python clients.

### Inheritance

An object shows the tags attached to it plus the tags attached to each of its ancestors, so a tag
on a catalog appears on every schema, table, and column beneath it. For catalogs that support
multi-level schemas, the intermediate schemas are ancestors too, so a schema two levels down
inherits from both of the schemas above it.

Each tag appears once, whether it reaches the object through one ancestor or several. A tag attached
directly to the object counts as direct even when an ancestor carries it too.

Assignment values follow the same inheritance path. If a child has no direct assignment for a tag,
it receives the values from the nearest effective ancestor. A direct assignment on the child
overrides the inherited assignment for that tag; its values are not merged with ancestor values.
For example, a table assigned `data_domain = risk` shows only `risk` even when its catalog is
assigned `data_domain = finance`.

The two are distinguishable. In the UI an inherited tag is marked with a lock icon. Over REST, a tag
listing requested with `details=true` carries an `inherited` field on each tag, which a plain listing
of names does not.

A tag that reaches an object only by inheritance cannot be removed there. Detach it from the
ancestor that carries it, which affects every other object beneath that ancestor as well.

Inheritance is resolved when the object is read rather than stored on the object, so attaching a
tag to a catalog takes effect immediately for tables created afterward.

## Working With Tags in the UI

### Managing the Tag Set

The tag list holds every tag in the metalake with its comment and creation time, and can be
searched. A tag can be renamed and its comment and properties edited from there.

Deleting a tag removes it from every object it was attached to. There is no warning about how many
objects that affects and no way to recover the attachments, so check where a tag is used before
deleting one.

### Attaching and Detaching

Tags attach from the object rather than from the tag, so open the catalog, schema, or table and use
the tag control there. Inherited tags carry no remove control. Detaching removes the direct
attachment only, so an object still shows a tag it inherits from an ancestor.

Column tags are edited from the column list on the table page. A column shows its own tags plus
everything it inherits from the table, schema, and catalog above it, which is usually most of what
is listed.

### Finding Where a Tag Is Used

Selecting a tag name opens a view listing the objects the tag is attached to directly. Inherited
reach is not included, so a tag attached to one catalog lists that catalog rather than the tables
under it. Coverage questions that span a subtree are answered by walking the objects and reading
the tags on each one.

## Permissions

Tag permissions are held on the tag, and apply in addition to permissions on the objects being
tagged.

| Privilege    | Grantable on              | What it allows                              |
|--------------|---------------------------|---------------------------------------------|
| `CREATE_TAG` | Metalake                  | Creating tags in the metalake               |
| `APPLY_TAG`  | Metalake, or a single tag | Attaching or detaching the tag              |
| `VIEW_TAG`   | Metalake, or a single tag | Reading tag metadata and associations       |

Altering and deleting a tag are reserved for the metalake owner and the tag owner.
Attaching a tag also requires access to the object being tagged, so a user who can apply a tag
cannot use it to reach an object they could not otherwise see. Tag listings show only the tags that
user is allowed to read. An `APPLY_TAG` grant also satisfies tag read checks unless `VIEW_TAG` is
explicitly denied.

## Using the API

Tags can be created, attached, and read over REST and through the Java and Python clients, which is
also the only way to tag views, filesets, topics, models, and functions today. Endpoints, payload
shapes, and worked examples are in [Manage Tags](./manage-tags-in-gravitino.md).
