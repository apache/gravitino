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

<<<<<<< HEAD
Policies can be created, attached, and read over REST and through the Java client. Endpoints, payload
shapes, and worked examples are in [Manage Policies](./manage-policies-in-gravitino.md).
=======
| Expression                                | Result          |
| ----------------------------------------- | --------------- |
| `NULL IS NULL` / `NULL IS NOT NULL`       | TRUE / FALSE    |
| Comparison with a NULL operand            | UNKNOWN         |
| `NOT NULL`                                | UNKNOWN         |
| `FALSE AND NULL` / `TRUE AND NULL`        | FALSE / UNKNOWN |
| `TRUE OR NULL` / `FALSE OR NULL`          | TRUE / UNKNOWN  |
| NULL input to IN, BETWEEN, LIKE or is_nan | UNKNOWN         |
| Nonmatching IN list containing NULL       | UNKNOWN         |
| Nonmatching NOT IN list containing NULL   | UNKNOWN         |

For example, both `region <> 'US'` and `NOT (region = 'US')` exclude null regions. Use
`region IS NULL OR region <> 'US'` to include them. A constant FALSE filter returns no rows.
All mask result arms must match the declared type. A mask that can return NULL, including an
unselected NULL arm or an omitted ELSE, requires a nullable target. Schema binding rejects such a
mask for a required column. Target conversion and context lookups evaluate only selected CASE
results and stop evaluating later conditions after a match.

#### Function Contract

`CURRENT_USER()` returns the non-null STRING name of the trusted effective subject. Context
values remain stable for a conversion.
`is_group_member('group')` returns BOOLEAN for exact flat membership in the current metalake.
An unknown group, unavailable subject or failed lookup is an error; an existing group without
the subject returns FALSE. Group names must be non-empty string literals. Nested group expansion
is unsupported.

User attributes and implicit string-to-number conversions are unsupported.

The predicate `is_nan(value)` accepts FLOAT or DOUBLE and preserves null input. Date parts
`year`, `month`, and `day` accept DATE or TIMESTAMP, return INTEGER and preserve null input.
Timestamp-with-timezone values use UTC; timestamps without timezone use their recorded calendar
fields. Dates use the proleptic Gregorian calendar. `month(order_date) = 3` means March in any year.

Mask function behavior is as follows; each call takes one compatible scalar argument and preserves
its input logical type:

| Function                                 | Input type                                    | Behavior                                                                                                                                |
| ---------------------------------------- | --------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `mask_alphanum`                          | STRING                                        | ASCII digits become `n`; `(),.@-` are preserved; other Unicode code points become `x`                                                   |
| `show_first_4` / `show_last_4`           | STRING                                        | Preserve the first/last four code points and mask the remainder as above; strings of four or fewer code points are unchanged            |
| `mask_to_fixed_value`                    | STRING, numeric, BOOLEAN, DATE or TIMESTAMP   | Return `XXXXXXXX` for STRING, zero for numeric values, FALSE for BOOLEAN, or the epoch date/timestamp, including for null input         |
| `replace_with_null`                      | Any typed scalar                              | Return a typed null                                                                                                                     |
| `truncate_to_year` / `truncate_to_month` | DATE or TIMESTAMP                             | Truncate to the start of the year/month, using UTC for timestamps with time zone                                                        |
| `sha256_global` / `sha256_query_local`   | STRING, signed INTEGER, signed LONG or BINARY | Type-preserving SHA-256; the query-local variant prepends a cryptographically random salt of at least 16 bytes, shared within one query |

All masks except `mask_to_fixed_value` preserve null input. Hash input encoding is UTF-8 for
strings, little-endian two's-complement for signed integers, and raw bytes for BINARY. STRING
results are 64 lowercase hexadecimal characters; INTEGER/LONG results are the first four/eight
digest bytes interpreted as signed little-endian; BINARY results contain the complete 32-byte
digest. Separate queries require independent query-local salts.

#### Validation Boundary

ANTLR parses the complete stored definition, then an AST visitor builds separate typed operation
nodes. Static validation checks parameter/result declarations, unique names, available local
references, every CASE arm, operand types, allowlisted calls and arguments, exact literal ranges,
and source limits. Golden fixtures pin parsed ASTs and rejected-input diagnostics.

The parser does not execute expressions. The experimental converter binds every declared input
before selecting branches, checks exact schema types and mask nullability, snapshots the effective
subject, and caches trusted group lookups for a conversion. Any binding, translation or
capability error fails the governed load; an unsupported restriction never becomes an unrestricted
response.

#### Experimental Iceberg Conversion

Experimental Iceberg REST read restrictions require authorization to be enabled with
`gravitino.authorization.enable=true` and a policy dispatcher to be available. When a policy dispatcher
is available but authorization is disabled, the Iceberg REST service fails to start with an error
identifying the required configuration, even when no policy applies. In auxiliary mode, this also
aborts startup of the Gravitino server.

`is_group_member` uses group membership supplied by the authenticated principal; enabling
authorization does not populate these groups. An empty group list is valid and means that the user
is not a member of any group. Authenticators that do not supply groups, including `simple`, therefore
select the non-member branch of group-based policies, which may be unrestricted if the policy allows
it. The referenced group must still exist in Gravitino; an unknown group or an unresolved identity
causes the load-table request to fail.

When the experimental Iceberg REST read-restriction loader is enabled, load-table does not support
names containing `.` in the metalake, catalog, schema, table, or top-level column path. The
metadata-object API reserves `.` as a qualified-name separator, so a tag cannot be assigned
directly to such an object. The loader returns HTTP 400 for these names even when no policy is
assigned. A standalone service without a policy dispatcher does not perform policy resolution.

When the experimental Iceberg REST read-restriction loader resolves several row-filter policies
for one table, a filter that resolves to `true` adds no restriction and is ignored. Identical
effective filters are deduplicated. Distinct restrictive filters conflict and cause the load-table
request to fail; the loader does not combine them.

The current converter supports the following enforcement subset of the shared SQL grammar:

| SQL form                                                                 | Iceberg conversion                                                      |
| ------------------------------------------------------------------------ | ----------------------------------------------------------------------- |
| Column and compatible literal/context scalar comparison                  | Bound field-ID predicate, preserving SQL NULL semantics                 |
| Boolean inputs, AND/OR/NOT and Boolean comparisons                       | Three-valued lowering; only TRUE retains a row                          |
| IN/NOT IN, BETWEEN/NOT BETWEEN, null tests, prefix LIKE and NaN tests    | Closed native predicates with explicit unknown handling                 |
| Searched/simple Boolean CASE and scalar CASE inside supported predicates | Ordered conditional lowering; false and unknown conditions fall through |
| `year(date_or_timestamp_column)`                                         | Iceberg year transform with calendar-year literal adjustment            |
| `month`/`day` on constant temporal values                                | Evaluated as scalar constants                                           |
| Mask returning its local input                                           | No projection, preserving the original selected column                  |
| NULL or `replace_with_null`                                              | Null projection on a nullable column                                    |
| One of the nine direct mask functions applied to the local input         | Corresponding Iceberg projection                                        |
| A literal equal to the target type's fixed mask value                    | Fixed-value projection                                                  |
| Mask CASE using conditions that resolve to constants after binding       | First matching compatible projection                                    |

General column-to-column comparisons, month/day extraction from row values, row-dependent mask
selection, composed mask calls and arbitrary literal mask values are not representable by the
current closed target and are rejected. An incompatible selected mask fails the table load. A SQL
mask parameter binds to the selected column by field ID, independently of its local name. Quoted
row-filter declarations match exact physical names; unquoted declarations match case-folded names
and reject ambiguous matches.

Source limits still apply. Conversion additionally bounds generated Iceberg expressions to 16384
nodes and depth 256, and bounds compilation work. This protects conditional lowering and
serialization independently of source width. Exceeding a target limit rejects the load. Closed
filters and mask projections are canonicalized deterministically for deduplication, conflict
checks and response ETags.

### Policy-to-Tag Associations

Each association connects one policy to one tag and stores a selector. The selector determines
whether that association contributes the policy to an object's lookup result.

| Selector     | When it matches                                                        |
| ------------ | ---------------------------------------------------------------------- |
| `ALL_VALUES` | The effective tag is present, including an assignment without a value. |
| `TAG_VALUE`  | One of the effective tag assignment values equals the specified value. |

A policy may be associated with multiple tags. Association listings show those direct relations
and their selectors, even if no object currently matches them. An object policy lookup returns each
matching policy once.

When adding a `TAG_VALUE` association, Gravitino validates its value against the tag's constraint
in the same storage transaction as the association write. `NO_VALUE` tags reject `TAG_VALUE`;
`ALLOWED_VALUES` tags accept only an exact, case-sensitive match. Invalid values
return HTTP 400. `ALL_VALUES` is valid for every constraint.

Tag value constraints are fixed at creation; the tag alteration API does not change them. This
validation applies when creating associations and does not rewrite existing associations.

A selector cannot be changed in place. Remove the policy-to-tag association and add it again with
the new selector. Removing an association leaves the policy, tag, and tag assignments intact.
Deleting a policy removes its associations.

Object policy lookup covers `CATALOG`, `SCHEMA`, `TABLE`, `VIEW`, `COLUMN`, `FILESET`, `TOPIC`,
`MODEL`, `MODEL_VERSION`, `SEMANTIC_MODEL`, and `FUNCTION`. A model version cannot carry a tag directly,
but it can inherit tags from its model and higher ancestors, so their policies appear in model
version lookups.
This also includes columns, which did not support direct policy associations.
A tag assigned to a catalog or schema can therefore make its policies appear in descendant column
lookups. Because lookup does not filter by `supportedObjectTypes`, a policy whose content lists
only `TABLE` can still appear in a column lookup; consumers must enforce the intended scope.

### Effective Tags and Inheritance

An object receives tags assigned directly to it and tags inherited from its metadata object
ancestors. Resolution starts at the object and walks upward, so the nearest assignment of a tag
name wins, including its assignment values. A direct assignment therefore overrides every
ancestor, and a schema assignment overrides the same tag assigned on its catalog for the schema's
descendants. Tag names themselves are flat; tags do not inherit from other tags.

For example, a catalog with `data_domain=finance` gives its tables that effective assignment.
A table assigned `data_domain=risk` instead uses `risk`, so a policy associated with
`TAG_VALUE("finance")` no longer matches the table. `ALL_VALUES` still matches because the
tag is present.

Object policy lookup is read-only. To change its result, update the policy or its enabled state,
change a policy-to-tag association, or change a tag assignment on the object or an ancestor.
With `details=true`, the lookup includes an `inherited` field. It is `true` when the policy
matches only through an inherited tag.

### Enabled State

Disabling a policy preserves the policy and its tag associations, but excludes it from object
policy lookup. Policy and association listings still show it. Enabling it makes matching object
lookups include it again.

## Managing Policies

The UI supports policy lifecycle operations such as creating custom policies, editing their
content, and changing their enabled state. Use the REST API or Java client to manage
policy-to-tag associations and to read the resulting object policies. The
[Manage Policies](./manage-policies-in-gravitino.md) guide has requests and examples.
Creating a policy also creates its server-managed hidden tag and policy-to-tag relation
in the same metadata transaction. The hidden tag starts without object assignments, so creating
a policy does not apply it to any object. Renaming or updating the policy preserves this tag;
deleting the policy removes it and its assignments in the same transaction.
Hidden tags participate in policy resolution but do not appear in ordinary tag listings.
Tag API requests for hidden tags return not-found, and hidden tags cannot receive owners or user grants.
Deleting their policy also removes any existing ownership and grant rows for the hidden tag.
Internal tags use the `gravitino_hidden_{policyId}` name. The `gravitino_hidden_` prefix,
including alternate casing, determines visibility and is reserved for internal use. REST tag
creation and rename requests cannot use this prefix. Other names, including the generic `reserved_`
prefix, remain available to users.
The internal binding operation loads the policy's existing hidden tag and writes the tag-to-object
assignment. Endpoint validation and the transaction belong to the relation operation. It does not
inspect or modify policy-to-tag relations. Updating or overwriting an existing policy does not
create a missing hidden tag.

Do not run old and new server versions together after internal policy tags have been created:
old servers do not recognize the internal name prefix and can expose or modify these tags.

To add or remove an association, a user must own the metalake or have the required access to both
the tag and the policy (`APPLY_TAG` and `APPLY_POLICY`, or ownership of each). Object policy
reads also respect the caller's access to the returned policies. `VIEW_TAG` and `VIEW_POLICY`
grant read-only access to tags and policies; `APPLY_TAG` and `APPLY_POLICY` also allow reads.
>>>>>>> f38f3de65 ([#14749][#14424] fix: Support experimental scans and validate tag selectors (#14779))
