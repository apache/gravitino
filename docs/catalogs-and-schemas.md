---
title: "Catalogs and Schemas"
slug: "/catalogs-and-schemas"
keyword: "catalog, schema, namespace, metadata object, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

A catalog is a connection to a system that holds data or metadata, and a schema is a namespace
inside it. Together they are the two levels between a metalake and the objects you actually work
with.

A catalog is where federation happens. Registering one tells Gravitino how to reach a Hive
metastore, an Iceberg REST catalog, a MySQL database, a Kafka cluster, or object storage, and from
that point the objects inside appear alongside everything else in the metalake. Gravitino does not
copy them. Listing a schema asks the source system, so what you see is what is there now.

A schema is the middle level, and what it means depends on the system underneath. In a relational
catalog it is a database. In a fileset catalog it groups filesets under a location. In a model
catalog it is a namespace for models with no physical counterpart at all.

Catalogs and schemas both carry tags and policies, and both are the level to attach them at when
something should apply broadly. A tag on a catalog reaches every object beneath it.

## Quick Start

**1. Create the catalog.** Catalogs are created from the catalog list in the UI, or over REST. A
catalog needs a name, a type, and for most types a provider, plus the properties that tell Gravitino
how to connect.

**2. Create or discover schemas.** Connecting to an existing system surfaces the schemas already
there. Creating a schema through Gravitino creates it in the source system too, where the source
supports that.

**3. Work with what is inside.** Tables, filesets, topics, models, and functions all live under a
schema and are reached the same way regardless of which system stores them.

## The Catalog Model

### Catalog Types and Providers

Every catalog has a type, which decides what kind of object lives inside it.

| Type         | Contains        |
|--------------|-----------------|
| `RELATIONAL` | Tables and views|
| `FILESET`    | Filesets        |
| `MESSAGING`  | Topics          |
| `MODEL`      | Models          |

Most types also need a provider, which names the system being connected: `hive`,
`lakehouse-iceberg`, `jdbc-mysql`, `kafka`, and so on. Fileset and model catalogs are the exception.
Gravitino manages their metadata itself, so the type is enough and the provider is inferred.

Each provider has its own connection properties and its own page describing them.

### Names and Properties

A catalog name is unique within its metalake, and a schema name is unique within its catalog.

Properties carry the connection details, and which ones apply depends entirely on the provider. Two
reserved keys behave the same everywhere. `in-use` records whether the catalog is available and
defaults to `true`. `location` sets a base path where the catalog type uses one.

### In Use and Not In Use

A catalog that is not in use can only be listed, loaded, enabled, or dropped. Every other operation
on it or on anything inside it fails, which makes disabling a way to take a connection out of
service without removing its metadata.

Disabling a catalog does not touch the source system. The tables and files it points at stay exactly
where they are.

### What Gravitino Stores

Gravitino stores the catalog registration, the schemas and objects created through it, and anything
attached to those objects such as tags, policies, and ownership. It does not store a copy of the
source system's contents.

Listing tables in a schema reaches the source system at request time, so a table created directly in
Hive appears the next time Gravitino is asked. The consequence worth knowing is that Gravitino
cannot answer questions about objects nothing has asked for yet.

## Working With Catalogs and Schemas in the UI

The catalog list holds every catalog in the current metalake. A catalog can be created, edited,
enabled or disabled, and deleted from there, and each one expands into its schemas and their
contents.

Tags and policies attach from the catalog and schema rows, which is the fastest way to classify a
whole subtree.

## Deleting Catalogs and Schemas

Deleting is guarded, and force changes what happens.

Without force, a catalog must have no schemas and a schema must have no objects. Either condition
fails the request rather than removing anything.

With force, Gravitino removes the registration and everything it holds about the contents. For
managed objects, such as a managed fileset or a model, that removes the data too. For federated
objects, the Hive tables or S3 files stay where they are and only Gravitino's record of them goes.

## Permissions

| Privilege        | Grantable on                  | What it allows                        |
|------------------|-------------------------------|---------------------------------------|
| `CREATE_CATALOG` | Metalake                      | Creating catalogs in the metalake     |
| `USE_CATALOG`    | Metalake or catalog           | Reaching a catalog and its contents   |
| `CREATE_SCHEMA`  | Metalake, catalog, or schema  | Creating schemas                      |
| `USE_SCHEMA`     | Metalake, catalog, or schema  | Reaching a schema and its contents    |

Granting at a wider scope covers everything beneath it, so `USE_CATALOG` on the metalake reaches
every catalog in it.

Altering and dropping a catalog or schema are reserved for the metalake owner and the object owner.
Ownership resolves down the hierarchy, so the owner of a catalog has the owner path to everything
beneath it.

## Using the API

Catalogs and schemas can be created, listed, altered, and dropped over REST and through the Java and
Python clients. Endpoints, payload shapes, and worked examples are in
[Manage Catalogs and Schemas](./manage-catalogs-and-schemas.md).
