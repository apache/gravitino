---
title: "Ontologies and Mappings"
slug: "/ontologies-and-mappings"
keyword: "ontology, mapping, semantic model, Ossie, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

An ontology defines the business concepts that models in a metalake can share.
For example, a `Sales` ontology can define `CustomerId` as the identifier for a customer.
A mapping connects that concept to a field or expression in a particular
[Semantic Model](./semantic-models.md). In the sales model, the identifier comes from
`customers.customer_id`. A model in another catalog can map its own customer identifier to the same concept.

Ontology and mapping management is experimental.

## Scope and Lifecycle

An ontology belongs to a metalake. A mapping belongs to one semantic model in a particular catalog and schema
and refers to one ontology in the same metalake. Each model can have one mapping per ontology.
Gravitino checks references to ontology concepts, identifiers, and relationships, including the number
of participants in a link mapping. It stores the model expressions as written.

Before dropping an ontology, remove its mappings. Dropping a model, its schema, or its catalog
also removes the model's mappings. Other models can continue to use the ontology.

## Quick Start

1. [Create the sales model](./manage-semantic-model-metadata-using-gravitino.md#create-a-semantic-model)
   with its `customers` dataset.
2. [Prepare the two Ossie documents](./manage-ontologies-and-mappings-in-gravitino.md#ossie-documents)
   and import the ontology and its mapping with REST, Java, or Python.
3. [Look up the mapping by ontology](./manage-ontologies-and-mappings-in-gravitino.md#find-mappings-for-an-ontology)
   to see which model uses `Sales`, then export the definitions as Ossie documents.

The [MCP server](./gravitino-mcp-server.md) also provides tools tagged `ontology` to list, load,
create, alter, and drop ontologies and mappings. Use REST or a client SDK to import or export Ossie documents.

## Ownership and Permissions

With authorization enabled, the ontology's creator becomes its owner. Use the `ONTOLOGY` metadata
object type when assigning ownership, attaching tags, or looking up policies.

| Operation | Required access |
| --- | --- |
| Create or import an ontology | Own the metalake or have `CREATE_ONTOLOGY` on it |
| Load or export an ontology | Own the metalake or ontology, or have `USE_ONTOLOGY` or `MODIFY_ONTOLOGY` on the ontology |
| Alter an ontology | Own the metalake or ontology, or have `MODIFY_ONTOLOGY` on the ontology |
| Drop an ontology | Own the metalake or ontology |
| Load or export a mapping | Be able to load its parent model |
| Create, import, or replace a mapping definition | Be able to modify its parent model and load the referenced ontology |
| Rename a mapping, change its metadata, or drop it | Be able to modify its parent model |

Looking up mappings from an ontology requires access to that ontology. Results include only
mappings visible to the caller. For a model that has never been published, only callers with
modify access to the model can see its mappings.

## Changes to a Referenced Ontology

Replacing a definition that would invalidate a live mapping returns HTTP 409. The error names the
affected mappings the caller can load and gives a count for the others. Update the affected mappings
before replacing the ontology definition. A write that times out waiting for a concurrent change
returns HTTP 409 with error code `1012` and can be retried.
