---
title: "Filesets"
slug: "/filesets"
keyword: "fileset, files, storage location, GVFS, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

A fileset is a named pointer to files. Where a table describes rows and columns, a fileset describes
a location, so unstructured and semi-structured data gets the same treatment as everything else in
the catalog: a name, a place in the hierarchy, tags, policies, and an owner.

The point is indirection. Code that reads a fileset by name does not carry a bucket path, so moving
data between clusters or storage systems changes the fileset rather than every job that reads it.

That indirection is delivered by the Gravitino Virtual File System, or GVFS. GVFS is a filesystem
implementation that resolves a fileset name to its storage location and then reads and writes
through to the underlying system, whether that is HDFS, S3, GCS, ADLS, or OSS. Paths take the form
`gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}`, so a Spark job, a pandas script, or a
Hadoop shell command reads a catalog name rather than a bucket URL.

GVFS comes in two implementations. The Java one implements the Hadoop Compatible File System
interface, so anything that already speaks HDFS paths works, and it requires Hadoop 3.3.1 or later.
The Python one is built on fsspec, so it works with pandas, PyArrow, and anything else in that
ecosystem. There is also a FUSE implementation for mounting a fileset as a local directory.

Credentials are the other half of it. A caller reaching storage through GVFS can be given
short-lived credentials vended by Gravitino rather than holding long-lived cloud keys of its own,
which is what makes the indirection a governance boundary rather than only a convenience. See
[Credential vending](./security/credential-vending.md).

Filesets live in a schema inside a fileset catalog. Gravitino manages that catalog itself rather
than federating an external one, so no provider is needed when creating it.

## Quick Start

**1. Create a fileset catalog and schema.** See
[Catalogs and Schemas](./catalogs-and-schemas.md). A fileset catalog usually carries a base
`location`, and schemas can narrow it further.

**2. Create the fileset.** Give it a name, a type, and a storage location. Creating a managed
fileset creates the directory; pointing at an existing path makes it external.

**3. Read it by name.** Engines reach the files through GVFS or the client rather than the raw path.
See [How to use GVFS](./how-to-use-gvfs.md).

## The Fileset Model

### Managed and External

A managed fileset belongs to Gravitino. Creating it creates the directory, and deleting it deletes
the data.

An external fileset points at a location that already exists and stays under someone else's control.
Deleting it removes the Gravitino record and leaves the files alone.

The distinction only matters at deletion time, and it is the single most important thing to get
right when creating one.

### Storage Locations

A fileset has at least one storage location and can have several, each with a name. The default is
chosen by the `default-location-name` property, and a location supplied without a name is recorded
as `unknown`.

Several locations on one fileset is how the same logical dataset is described across clusters or
regions, with readers selecting the one they should use rather than each carrying its own path.

Locations can also be inherited: a catalog or schema with a `location` property supplies a base, and
a fileset created beneath it can take its placement from that rather than spelling out a full path.

A location can be a template rather than a fixed path. Placeholders such as `{{catalog}}`,
`{{schema}}`, `{{user}}`, and `{{project}}` are filled in when the fileset is created, with values
supplied by `placeholder-` properties. One catalog-level template then generates a consistent
directory layout for every fileset created beneath it, rather than each one being spelled out by
hand.

### Properties

Properties are free-form, with `default-location-name` and the `location-{name}` keys reserved for
placement. Everything else is yours, and travels with the fileset.

## Working With Filesets in the UI

Opening a fileset catalog lists its schemas and the filesets inside them. A fileset shows its type,
its storage locations, and its properties.

Filesets display the tags they carry, including inherited ones, but cannot be tagged from the UI
today. Attaching a tag to a fileset goes through the API.

## Permissions

| Privilege        | Grantable on                          | What it allows              |
|------------------|---------------------------------------|-----------------------------|
| `CREATE_FILESET` | Metalake, catalog, schema, or fileset | Creating filesets           |
| `READ_FILESET`   | Metalake, catalog, schema, or fileset | Reading a fileset's files   |
| `WRITE_FILESET`  | Metalake, catalog, schema, or fileset | Writing a fileset's files   |

Granting at a wider scope covers everything beneath it. Dropping a fileset is reserved for the
metalake owner and the object owner.

## Using the API

Filesets can be created, listed, altered, and dropped over REST and through the Java and Python
clients. Endpoints, payload shapes, and worked examples are in
[Manage Fileset Metadata](./manage-fileset-metadata-using-gravitino.md).
