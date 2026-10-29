---
title: "Ontologies"
slug: "/ontologies"
keyword: "ontology, ontologies, Ossie, concepts, relationships, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

An ontology is a metalake-scoped, versioned semantic vocabulary: the concepts of a business and the
relationships between them, written as an Apache Ossie ontology document and managed by Gravitino.
Gravitino stores it alongside the [Semantic Models](./semantic-models.md) it describes, so a planner
or an application can work against one shared vocabulary instead of a vocabulary that each model
reinvents. Apache Ossie keeps the mappings that bind a concept to a dataset or column in a separate
document, so this page covers the ontology document itself.

An ontology is a machine-readable graph, not a glossary. A concept has a type and relationships with
cardinality, but no definition, synonyms, owner or approval state. An ontology holds no assets and
grants no access to data.

## Ontology concepts and relationships

A definition is the concept graph of an ontology, without its name, comment, status or Gravitino
properties. It is the whole of an Apache Ossie ontology document except the root identity fields, so
an import fills it in and an export writes it back.

| Field | On | Meaning |
|-------|----|---------|
| `concepts` | Definition | A required, non-empty list of concepts declared by the ontology. |
| `concept` | Concept | The concept name, unique within the ontology. |
| `type` | Concept | `ENTITY_TYPE` for identifiable objects, `VALUE_TYPE` for values. The set is closed. |
| `description` | Concept, relationship | Free-text description. |
| `extends` | Concept | Concepts this concept specializes. Each name must be declared in the same definition. |
| `derivedBy` | Concept, relationship | ANSI SQL expressions that derive the concept or relationship. |
| `identifyBy` | Concept | Declares the preferred identifier of an instance. |
| `requires` | Definition, concept, relationship | ANSI SQL expressions that constrain population. |
| `relationships` | Concept | The relationships the concept owns. |
| `name` | Relationship, role | The relationship name, unique within the ontology; an optional role name. |
| `verbalizes` | Relationship | One or more natural-language phrasings. At least one is required. |
| `roles` | Relationship | The concepts the relationship connects. Each must be declared in the same definition. |
| `multiplicity` | Relationship | `MANY_TO_ONE`, `ONE_TO_ONE`, or unset for an unconstrained relationship. |
| `iri` | Concept, relationship | A global identifier, either absolute or a QName resolved through `prefixes`. |
| `id` | Concept, relationship | The identifier Gravitino minted for it. Read-only, absent from an exported document. |
| `prefixes` | Definition | A prefix-to-IRI map that resolves a QName such as `fin:Account`. |
| `aiContext` | Definition | Context for applications that consume the vocabulary, as text or a structured object. |

A few rules follow from the document shape rather than from a convention:

- A definition must declare at least one concept, and concept names and relationship names must be
  unique. A relationship name is unique across the whole ontology, not only within its concept.
- `extends` and every role must name a concept the same definition declares. The document has no
  place for external references, so a dangling name is rejected rather than resolved later.
- A relationship without at least one non-blank `verbalizes` phrasing is rejected. Gravitino does not
  default it: a relationship an application cannot phrase is one it cannot use.
- `type` and `multiplicity` are closed sets. An omitted multiplicity means unconstrained, not a
  default cardinality.
- An `iri` must contain a colon. An absolute IRI carries its own scheme (`://` or a `urn:` prefix); a
  QName `prefix:local` must use a prefix declared in `prefixes`. A bare name is rejected.
- An `id` is Gravitino's own handle for a concept or a relationship. It is minted when the ontology is
  stored, kept across versions, and never written to an exported Apache Ossie document, because the
  specification names a concept by its `concept` string and a relationship by its `name` alone. A
  caller sends names; sending an existing `id` alongside a new name renames the concept or
  relationship without breaking a term reference or a mapping that binds it, and leaving it out
  inherits the identifier of the stored object with the same name.

## Lifecycle and versions

A new ontology is in `DRAFT` status. A draft is visible only to the callers who can manage it: it is
omitted from listings, and getting it fails as if it did not exist. Publish a draft with an
`updateStatus` change, and unpublish a published ontology the same way. Do not plan queries against a
draft; its vocabulary is still being written and it is not exposed to readers. Publish the ontology
before a planner or an application consumes it.

Changing content allocates a new version. The name, the comment, the properties, and any definition
section are content, so `rename`, `updateComment`, `setProperty`, `removeProperty`, and
`replaceDefinition` all produce a new version. Flipping the status does not: `updateStatus` only
changes the lifecycle flag, and every existing version keeps the number it was published with.

The version is internal. The REST representation carries the status and audit information but no
version number, so a consumer reads the current state rather than pinning a version.

Deleting an ontology that other metadata still references is rejected rather than cascaded, and a
delete of an ontology that does not exist reports `false` instead of failing.

## Using the REST API

Ontologies are addressed under a metalake:

```
/api/metalakes/{metalake}/ontologies
```

An ontology name is unique per metalake, case-insensitively. A name that contains `/` is allowed and
must be URL-encoded in a path. The examples below use the metalake `example` and the ontology name
`finance`.

The native JSON uses camelCase field names and lower-case enum values (`entity_type`,
`many_to_one`); the server reads them case-insensitively, so `ENTITY_TYPE` is accepted on input but
`entity_type` is what comes back. An Apache Ossie document uses snake_case and the specification
spellings (`EntityType`, `ManyToOne`) instead; see
[Importing and exporting Ossie documents](#importing-and-exporting-ossie-documents).

### Create an ontology

`POST /api/metalakes/{metalake}/ontologies` creates an ontology in `DRAFT` status. The body carries a
name, an optional comment, optional Gravitino properties, and a required definition.

```bash
curl -X POST http://localhost:8090/api/metalakes/example/ontologies \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "finance",
    "comment": "Concepts for accounts, balances, and the customers that hold them",
    "properties": {
      "ossie-version": "0.2.0.dev0"
    },
    "definition": {
      "aiContext": "Use this vocabulary for questions about accounts, balances, and customers.",
      "prefixes": {
        "fin": "https://example.com/finance/"
      },
      "concepts": [
        {
          "concept": "Customer",
          "type": "entity_type",
          "description": "A party that holds one or more accounts",
          "iri": "fin:Customer"
        },
        {
          "concept": "Account",
          "type": "entity_type",
          "description": "An account held by a customer",
          "identifyBy": ["account_id"],
          "iri": "fin:Account",
          "relationships": [
            {
              "name": "heldBy",
              "description": "The customer that owns the account",
              "verbalizes": ["an account is held by a customer"],
              "roles": [{"concept": "Customer"}],
              "multiplicity": "many_to_one",
              "iri": "fin:heldBy"
            }
          ]
        },
        {
          "concept": "Balance",
          "type": "value_type",
          "description": "The amount on an account at a point in time",
          "iri": "fin:Balance"
        }
      ]
    }
  }'
```

The response repeats the definition and adds the assigned status and audit information:

```json
{
  "code": 0,
  "ontology": {
    "name": "finance",
    "comment": "Concepts for accounts, balances, and the customers that hold them",
    "properties": {
      "ossie-version": "0.2.0.dev0"
    },
    "status": "draft",
    "definition": {
      "aiContext": "Use this vocabulary for questions about accounts, balances, and customers.",
      "prefixes": {
        "fin": "https://example.com/finance/"
      },
      "concepts": [
        {
          "concept": "Customer",
          "type": "entity_type",
          "description": "A party that holds one or more accounts",
          "iri": "fin:Customer"
        },
        {
          "concept": "Account",
          "type": "entity_type",
          "description": "An account held by a customer",
          "identifyBy": ["account_id"],
          "iri": "fin:Account",
          "relationships": [
            {
              "name": "heldBy",
              "description": "The customer that owns the account",
              "verbalizes": ["an account is held by a customer"],
              "roles": [{"concept": "Customer"}],
              "multiplicity": "many_to_one",
              "iri": "fin:heldBy"
            }
          ]
        },
        {
          "concept": "Balance",
          "type": "value_type",
          "description": "The amount on an account at a point in time",
          "iri": "fin:Balance"
        }
      ]
    },
    "audit": {
      "creator": "admin",
      "createTime": "2026-10-29T08:00:00Z",
      "lastModifier": "admin",
      "lastModifiedTime": "2026-10-29T08:00:00Z"
    }
  }
}
```

If `ossie-version` is omitted, Gravitino stores `0.2.0.dev0`, the same version a Semantic Model is
written against. An explicitly supplied non-blank value is preserved.

### List ontologies

`GET /api/metalakes/{metalake}/ontologies` returns the ontology names of the metalake. The current
release does not hide drafts from readers, so every caller with access to the metalake sees every
ontology; filtering drafts by privilege arrives with the `USE_ONTOLOGY` / `MANAGE_ONTOLOGY`
privileges described under [Authorization](#authorization).

```bash
curl -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/ontologies
```

```json
{
  "code": 0,
  "names": ["finance"]
}
```

Add `details=true` to return the full ontology objects in an `ontologies` array:

```bash
curl -H "Accept: application/vnd.gravitino.v1+json" \
  "http://localhost:8090/api/metalakes/example/ontologies?details=true"
```

### Get an ontology

`GET /api/metalakes/{metalake}/ontologies/{ontology}` returns one ontology, in the same shape as the
create response.

```bash
curl -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/ontologies/finance
```

A draft the caller cannot manage behaves as if it did not exist, so a listing never reveals a
vocabulary the caller may not read.

### Alter an ontology

`PUT /api/metalakes/{metalake}/ontologies/{ontology}` applies one or more changes. The body is an
`updates` array, and each entry selects its operation with `@type`:

```bash
curl -X PUT http://localhost:8090/api/metalakes/example/ontologies/finance \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "updates": [
      {"@type": "updateComment", "newComment": "Reviewed finance vocabulary"},
      {"@type": "setProperty", "property": "owner", "value": "finance-team"}
    ]
  }'
```

| `@type` | Body | Effect |
|---------|------|--------|
| `rename` | `{"@type": "rename", "newName": "finance_v2"}` | Renames the ontology. Concepts keep their names. |
| `updateComment` | `{"@type": "updateComment", "newComment": "Reviewed vocabulary"}` | Replaces the comment. Send `null` to remove it. |
| `setProperty` | `{"@type": "setProperty", "property": "owner", "value": "finance-team"}` | Sets a property, replacing its value if it exists. |
| `removeProperty` | `{"@type": "removeProperty", "property": "owner"}` | Removes a property. An absent property is not an error. |
| `replaceDefinition` | `{"@type": "replaceDefinition", "definition": { ... }}` | Replaces the whole definition; name, comment, status, and properties are unchanged. |
| `updateStatus` | `{"@type": "updateStatus", "status": "published"}` | Publishes or unpublishes the ontology. Use `"draft"` to unpublish. |

Publishing is an `updateStatus` to `PUBLISHED`:

```bash
curl -X PUT http://localhost:8090/api/metalakes/example/ontologies/finance \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{"updates": [{"@type": "updateStatus", "status": "published"}]}'
```

A replacement definition is validated the same way as a created one: unknown fields, dangling
concept references, and relationships without `verbalizes` are rejected.

### Delete an ontology

`DELETE /api/metalakes/{metalake}/ontologies/{ontology}` removes the ontology.

```bash
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/ontologies/finance
```

```json
{
  "code": 0,
  "dropped": true
}
```

## Importing and exporting Ossie documents

An ontology can be exchanged as a standalone Apache Ossie ontology document in JSON or YAML, written
against the `0.2.0.dev0` ontology schema (`ontology/ontology.json`). Importing creates a new `DRAFT`
ontology; exporting produces a document that imports back into an equivalent ontology.

The document root is closed. Only `version`, `name`, `description`, `ai_context`, `requires`,
`ontology`, and `prefixes` are accepted, and a null or unknown property is rejected. `version` must
be a non-empty string and `name` is required; `description` becomes the ontology comment. The
`ontology` array holds the concepts, and each component and relationship uses the snake_case fields
defined by the specification:

```yaml
version: 0.2.0.dev0
name: finance
description: Concepts for accounts, balances, and the customers that hold them
prefixes:
  fin: https://example.com/finance/
ontology:
  - concept: Customer
    type: EntityType
    iri: fin:Customer
  - concept: Account
    type: EntityType
    identify_by: [account_id]
    iri: fin:Account
    relationships:
      - name: heldBy
        verbalizes: [an account is held by a customer]
        roles:
          - concept: Customer
        multiplicity: ManyToOne
        iri: fin:heldBy
  - concept: Balance
    type: ValueType
    iri: fin:Balance
```

The following rules are checked on import and again when an exported document is generated:

- Every relationship must carry at least one non-blank `verbalizes` phrasing.
- Every concept named by `extends` or by a relationship role must be declared in the same document.
- `type` must be `EntityType` or `ValueType`, and `multiplicity` must be `ManyToOne` or `OneToOne`.
- An `iri` is either absolute (contains `://` or starts with `urn:`) or a QName whose prefix is
  declared in `prefixes`. A bare name is rejected.
- Embedded `ontology_mappings` are rejected. Apache Ossie deprecates the embedded form; write a
  standalone mapping document validated against `ontology/mapping.json` instead.
- The document must declare at least one concept, and concept and relationship names must be unique.

REST endpoints for import and export are not part of this change. The operation is defined on the
Java API as `OntologyOperations.importOssieDocument` and
`OntologyOperations.exportOssieDocument`, and will be exposed over REST later.

```java
Ontology imported = ontologies.importOssieDocument(OssieDocument.yaml(document));
OssieDocument exported = ontologies.exportOssieDocument("finance", OssieFormat.JSON);
```

The `ossie-version` property supplies the exported `version`; when it is unset, Gravitino uses
`0.2.0.dev0`. Gravitino properties other than `ossie-version` are not part of an Ossie ontology
document, so they do not survive a round trip: exporting drops them and importing a document supplies
only the version.

## Ontology vs. Semantic Model

Both are Apache Ossie documents managed by Gravitino, but they answer different questions and live at
different scopes.

| Aspect | Ontology | Semantic Model |
|--------|----------|----------------|
| Scope | Metalake, addressed by name alone | Schema of a relational catalog, addressed by catalog, schema, and name |
| Content | Concepts and the relationships between them, plus prefixes and ontology-level SQL constraints | Datasets, their fields and relationships, and metrics |
| Question it answers | Which concepts exist in the business, and how do they relate | Which data and calculations give those concepts a definition |
| Lifecycle | `DRAFT` or `PUBLISHED` | No lifecycle status |
| Interchange document | Apache Ossie ontology document, validated against `ontology/ontology.json` | Apache Ossie core document |
| Privileges | `USE_ONTOLOGY` and `MANAGE_ONTOLOGY` (specified, not yet enforceable) | `CREATE_SEMANTIC_MODEL`, `USE_SEMANTIC_MODEL`, and `MODIFY_SEMANTIC_MODEL` |

A Semantic Model describes its own data. An ontology describes the vocabulary that a planner can use
across models.

## Authorization

Two metalake-level privileges are specified for ontologies, a read privilege and a manage privilege:

| Privilege | Scope | What it allows |
|-----------|-------|----------------|
| `USE_ONTOLOGY` | Metalake, ontology | Read published ontologies. |
| `MANAGE_ONTOLOGY` | Metalake, ontology | Create, alter, publish, unpublish, and delete ontologies, and see drafts. |

`MANAGE_ONTOLOGY` includes `USE_ONTOLOGY`. Neither privilege grants access to data.

These privileges are not yet part of the authorization model. `Privilege` declares no ontology value
and an ontology is not yet a securable object type, so an ontology-specific grant cannot be made or
enforced. Treat the names above as the intended contract rather than as configuration available
today.

What is enforced today is metalake-scoped. With authorization enabled, reading an ontology requires
the same metalake access as any other metalake-level metadata read, and creating, altering or
deleting one requires metalake ownership. With authorization disabled, every caller may do both.