---
title: "Metalakes"
slug: "/metalakes"
keyword: "metalake, tenant, namespace, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

A metalake is the top of the Gravitino hierarchy and the boundary everything else lives inside.
Catalogs, schemas, tables, filesets, topics, models, and functions all sit beneath one, and so do the
users, groups, roles, tags, policies, and jobs that apply to them.

Nothing crosses that boundary:

- Users and groups are records in one metalake. The same person working in two metalakes needs a
  user in each, and provisioning them is done per metalake
- Roles are defined and granted within one metalake, so a role held in one carries no privilege in
  another
- A tag or policy created in one metalake cannot be attached to an object in another
- Names only have to be unique within one metalake

That makes a metalake the unit to reach for when separating environments, business units, or tenants
that should not see each other's metadata.

Most installations need very few. A metalake per production estate, and perhaps one for development,
is a common shape. Every client connects to a single metalake and works inside it, so adding more
means people have to know which one they are in.

## Quick Start

**1. Create the metalake.** Metalakes are created from the metalake list in the UI, or with the
admin client. A metalake needs a name, and can carry a comment and properties. Only service admins
can create one.

**2. Connect a catalog.** A new metalake is empty. See
[Catalogs and Schemas](./catalogs-and-schemas.md).

**3. Point clients at it.** Clients name the metalake when they connect, so everything they do
afterward happens inside it.

## The Metalake Model

### Names and Properties

A metalake name is unique across the server and is what every client names when it connects, so
renaming one changes what every stored connection has to ask for.

Properties are free-form key and value pairs, with one reserved key. `in-use` records whether the
metalake is available, defaults to `true`, and is set through the enable and disable operations
rather than by writing the property directly.

A metalake cannot carry a tag or a policy, so there is no way to classify or govern everything at
once from the top. The widest attachment point is a catalog.

### In Use and Not In Use

A metalake that is not in use can only be listed, loaded, enabled, or dropped. Every other operation
on it or on anything inside it fails, which makes disabling a way to take an estate out of service
without deleting anything.

Enabling a metalake that is already in use does nothing, and the same is true of disabling one that
is already out of use.

## Working With Metalakes in the UI

The metalake list holds every metalake on the server. A metalake can be created, edited, enabled or
disabled, and deleted from there.

Selecting a metalake is what scopes the rest of the UI. Every other screen, including the catalogs,
compliance, and dashboard views, describes the metalake currently selected.

## Deleting a Metalake

A metalake is deleted with or without force, and the difference matters.

Without force, the metalake must be empty of catalogs and must not be in use. Either condition fails
the request rather than removing anything.

With force, Gravitino deletes the metalake and everything registered under it, including catalogs,
schemas, tags, and policies, whether or not the metalake is in use. External systems are left alone,
so a Hive table or an S3 bucket that was registered rather than created by Gravitino survives.
Managed objects, such as a managed fileset, are removed with their data.

## Permissions

Creating a metalake is reserved for service admins, configured with
`gravitino.authorization.serviceAdmins` in the server configuration. Nobody else can create one, no
matter what privileges they hold.

Altering, enabling, disabling, and dropping a metalake are reserved for its owner.

## Using the API

Metalakes can be created, listed, altered, enabled, disabled, and dropped over REST and through the
Java and Python admin clients. Endpoints, payload shapes, and worked examples are in
[Manage Metalakes](./manage-metalake-using-gravitino.md).
