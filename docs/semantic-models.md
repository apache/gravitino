---
title: "Semantic Models"
slug: "/semantic-models"
keyword: "semantic model, semantic metadata, Ossie, metrics, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

A Semantic Model describes the business meaning of data: the datasets to use, their fields and
relationships, and the metrics calculated from them. A sales model, for example, can give
analytical applications and AI tools a shared definition of revenue.

Semantic Models live in schemas of relational catalogs. Gravitino stores their definitions and
manages their lifecycle, ownership, privileges, and tags, even when the underlying catalog system
has no semantic-model feature. They are separate from [SQL views](./tables-and-views.md) and the
AI/ML artifacts managed by a [model catalog](./model-catalog.md).

Gravitino manages the definitions. Query compilation, execution, materialization, and publishing
engine-specific semantic objects are responsibilities of the applications that consume them.

## Quick Start

**1. Choose the source data.** Connect a relational catalog and identify the tables or logical views
the model will describe. See [Catalogs and Schemas](./catalogs-and-schemas.md).

**2. Create or import a model.** Define datasets and metrics through REST or a client SDK, or import
an Apache Ossie YAML or JSON document. The source objects must already exist.

**3. Govern and share the definition.** Assign access through roles, attach tags, and let consumers
load the model or export it as an Ossie document. Permission to read a definition does not itself
grant access to the source data.

Worked examples are in [Manage Semantic Model Metadata](./manage-semantic-model-metadata-using-gravitino.md).

## The Semantic Model

A model has a name, an optional comment, string-valued Gravitino properties, a structured
definition, and audit information. The definition uses these components:

| Component | Purpose |
|-----------|---------|
| `datasets` | A required, non-empty list of named sources. Each dataset can define fields, a primary key, and unique keys. |
| `fields` | Named expressions within a dataset, with optional data types, labels, descriptions, and dimension information. |
| `relationships` | Links between datasets in the same model, using source column names in `fromColumns` and `toColumns`. |
| `metrics` | Named expressions such as `SUM(orders.order_amount)`, with optional data types and descriptions. |
| `aiContext` | Context for consumers, represented as text or a structured object. |
| `customExtensions` | Vendor-specific information preserved with the definition. |

Expressions contain one or more dialect-specific representations. The Java and Python SDKs provide
`Dialects` constants for `ANSI_SQL`, `SNOWFLAKE`, `MDX`, `TABLEAU`, `DATABRICKS`, `MAQL`, and `BIGQUERY`.
Other non-empty dialect identifiers are also accepted and preserved. Choose identifiers supported
by the application consuming the model. Gravitino validates the definition's structure but does
not parse or execute the expression text.

### Sources and Validation

Dataset sources use `catalog.schema.object` within the model's metalake. They may reference another
catalog or schema in that metalake. Each source must resolve to an existing table or a logical view
supported by its catalog. Cross-metalake references are not supported.

The `from` and `to` endpoints of a relationship name datasets within the model.
Primary-key, unique-key, and relationship columns refer to columns of the source table or view,
rather than field aliases in the model. Gravitino checks that these columns exist; it does not
check whether the data satisfies the declared keys or relationships.

Creating, importing, or replacing a definition checks source existence and these columns, even
when authorization is disabled. With authorization enabled, source lookups also require the
caller's access to the source metadata.

Loading or exporting a model, renaming it, and changing only its comment or properties do not
revalidate source metadata. Review definitions when their source schemas change: a stored model
can become stale after a source column is changed or removed.

### Exchanging Definitions

The native REST API and standalone Apache Ossie documents represent the same definition in
different forms:

| Aspect | Native REST API | Ossie document |
|--------|-----------------|----------------|
| Definition location | Nested under `definition` in the model object | Datasets, relationships, and metrics at the document root |
| Field names | `primaryKey`, `uniqueKeys`, `fromColumns`, `toColumns`, `datatype` | `primary_key`, `unique_keys`, `from_columns`, `to_columns`, `datatype` |
| Dataset source | `{"namespace":["sales","mart"],"name":"orders"}` | `sales.mart.orders` |
| Model description | `comment` | `description` |
| Gravitino properties | `properties` on the model | A root `GRAVITINO_PROPERTIES` custom extension |

Use the Ossie export endpoint to produce a portable document. A native load response is not an
Ossie import document. Import creates a new model and reports a conflict if its name already
exists. See [Import and Export](./manage-semantic-model-metadata-using-gravitino.md#apache-ossie-import-and-export).

## Governance and Interfaces

The creator becomes the model's owner when authorization is enabled. Role grants control creation,
discovery, and modification; deletion requires ownership. Tags classify models and select policies
through policy-to-tag associations. See
[Ownership and Privileges](./manage-semantic-model-metadata-using-gravitino.md#ownership-and-privileges)
and [Tags and Policies](./manage-semantic-model-metadata-using-gravitino.md#tags-and-policies).

Use REST, the Java or Python client, or the
[MCP server](./gravitino-mcp-server.md#semantic-models) to manage Semantic Models. The current Web UI
does not provide Semantic Model workflows.
