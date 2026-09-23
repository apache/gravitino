---
title: "Policies"
slug: "/policies"
keyword: "policy, policies, governance, metadata object, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

A policy is a named set of rules that you create once in a metalake and attach to metadata objects.
Attaching a policy to a catalog or schema applies it to everything beneath, so a setting that varies
by table can be expressed once at the level where it holds and overridden where it does not.

Tags and policies are close cousins, and the difference is what they carry. A tag classifies, and
its content is its name. A policy prescribes, and its content is a set of rules something acts on.

Policies come in two kinds. A built-in policy has a type that Gravitino defines and a consumer that
acts on it. A custom policy carries rules of your own, which Gravitino stores, inherits, and serves
back to whatever system you build around it.

Common uses:

- Setting table maintenance behavior for a whole catalog rather than table by table, and letting new
  tables pick it up without further work
- Recording a rule once against metadata that lives in several catalogs, so every engine reaching
  those objects through Gravitino sees the same rule
- Feeding an external enforcement or scheduling system that reads policies from Gravitino rather
  than keeping its own copy of what applies where

## Quick Start

**1. Create the policy.** Policies are created from the policy list in the UI, which creates custom
policies. A policy needs a name, the object types it supports, and its rules. Built-in policies are
created over REST.

**2. Attach it to an object.** Open the catalog, schema, table, fileset, topic, model, view, or function you want to
govern and add the policy from its policy control. Only policies that already exist in the metalake
are offered.

**3. See where the policy is attached.** Selecting a policy name in the policy list shows the
objects it is attached to directly.

## The Policy Model

### Policy Types

| Type                        | Rules                                | Consumed by               |
|-----------------------------|--------------------------------------|---------------------------|
| `system_iceberg_compaction` | Compaction thresholds and scheduling | Table maintenance service |
| `custom`                    | A free-form map you define           | A system you provide      |

A built-in type has a name beginning with `system_` and a content shape Gravitino defines. The
compaction policy is documented in [Iceberg compaction policy](./iceberg-compaction-policy.md), and
the service that acts on it in
[Table maintenance service](./table-maintenance-service/optimizer.md).

A custom policy has type `custom`, and Gravitino makes no attempt to interpret what is inside
`customRules`. The rules are stored, inherited down the hierarchy, and returned to any client that
asks.

The UI creates custom policies only. A built-in policy is created over REST with its own content
shape.

### What Can Carry a Policy

A metadata object is identified by a type and a name, with each level below the catalog separated by
a dot. Eight object types can carry a policy.

| Object type | Name form                                     |
|-------------|-----------------------------------------------|
| `CATALOG`   | `{catalog_name}`                              |
| `SCHEMA`    | `{catalog_name}.{schema_name}`                |
| `TABLE`     | `{catalog_name}.{schema_name}.{table_name}`   |
| `FILESET`   | `{catalog_name}.{schema_name}.{fileset_name}` |
| `TOPIC`     | `{catalog_name}.{schema_name}.{topic_name}`   |
| `MODEL`     | `{catalog_name}.{schema_name}.{model_name}`   |
| `VIEW`      | `{catalog_name}.{schema_name}.{view_name}`    |
| `FUNCTION`  | `{catalog_name}.{schema_name}.{function_name}`|

Columns cannot carry a policy, which is narrower than
[tags](./tags.md). A metalake cannot carry one either, so to reach every object
in a catalog, attach the policy to the catalog.

Each policy also declares its own `supportedObjectTypes`, which narrows the list further for that
policy.

### Content

Policy content has three parts: the `supportedObjectTypes` list, the rules, and properties.

`supportedObjectTypes` is fixed when the policy is created and cannot be changed afterward, so a
policy meant for tables only stays that way for its lifetime.

The rules are what a consumer evaluates. For a custom policy they live under `customRules` as a map
you define, where the name is yours and the value is any JSON value.

```json
"customRules": {
  "retentionDays": 30,
  "maxTableSizeGb": 500,
  "requiresApproval": true
}
```

Gravitino does not interpret those names or values. Whatever consumes the policy decides what
`retentionDays` means and what to do about it.

A built-in policy has a rule set Gravitino defines, and the service that consumes it documents how
those rules are applied. The compaction policy carries `minDataFileMse`, `minDeleteFileNumber`,
`dataFileMseWeight`, `deleteFileNumberWeight`, `max-partition-num`, and a trigger and score
expression, plus any `job.options.` entries passed through to the job. Those names and their
meanings are covered in [Iceberg compaction policy](./iceberg-compaction-policy.md).

Properties describe the policy itself rather than the behavior it asks for. Rules change as you
adjust thresholds, and properties stay stable. The compaction policy uses properties for its
strategy type and job template name, which tell the table maintenance service what to run, and those
are set by Gravitino rather than by you. For a custom policy, properties are yours, and suit facts
such as which team owns the policy, which system consumes it, or which version of a rule set it
represents. Anything evaluated against an object belongs in rules instead.

Properties sit on the policy rather than on an attachment, so every object carrying the policy sees
the same values.

### The Enabled Flag

The `enabled` flag marks a policy as active or inactive for readers. Gravitino does not act on it,
so disabling a policy does not detach it or change what a consumer receives. Treat it as a signal to
whoever reads the policy, useful for holding a policy through review without deleting it.

### Inheritance

An object shows the policies attached to it plus the policies attached to each of its ancestors, so
a policy on a catalog applies to every schema, table, fileset, topic, model, view, and function beneath it. For
catalogs that support multi-level schemas, the intermediate schemas are ancestors too.

Each policy appears once, whether it reaches the object through one ancestor or several. A policy
attached directly to the object counts as direct even when an ancestor carries it too.

Direct and inherited attachments are distinguishable. In the UI an inherited policy is marked with a
lock icon. Over REST, a policy listing requested with `details=true` carries an `inherited` field on
each policy, which a plain listing of names does not.

A policy that reaches an object only by inheritance cannot be removed there. Detach it from the
ancestor that carries it, which affects every other object beneath that ancestor as well.

Inheritance is resolved when the object is read rather than stored on the object, so attaching a
policy to a catalog takes effect immediately for tables created afterward.

## Working With Policies in the UI

### Managing the Policy Set

The policy list holds every policy in the metalake and can be searched. A policy can be renamed, its
comment and rules edited, and its enabled flag switched from there. Policies created over REST,
including built-in ones, appear in the list alongside the rest.

Deleting a policy removes it from every object it was attached to, with no warning about how many
objects that affects and no way to recover the attachments.

### Attaching and Detaching

Policies attach from the object rather than from the policy, so open the object and use the policy
control there. Inherited policies carry no remove control. Detaching removes the direct attachment
only, so an object still shows a policy it inherits from an ancestor.

### Finding Where a Policy Is Used

Selecting a policy name opens a view listing the objects the policy is attached to directly.
Inherited reach is not included, so a policy attached to one catalog lists that catalog rather than
the tables under it.

## Permissions

Policy permissions are held on the policy, and apply in addition to permissions on the objects being
governed.

| Privilege       | Grantable on                 | What it allows                                 |
|-----------------|------------------------------|------------------------------------------------|
| `CREATE_POLICY` | Metalake                     | Creating policies in the metalake              |
| `APPLY_POLICY`  | Metalake, or a single policy | Reading a policy and attaching or detaching it |

Altering and deleting a policy are reserved for the metalake owner and the policy owner. Attaching a
policy also requires access to the object being governed. Policy listings show only the policies
that user is allowed to read.

## Using the API

Policies can be created, attached, and read over REST and through the Java client. Endpoints, payload
shapes, and worked examples are in [Manage Policies](./manage-policies-in-gravitino.md).
