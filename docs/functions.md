---
title: "Functions"
slug: "/functions"
keyword: "function, UDF, user-defined function, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

A function is a piece of logic registered in the catalog so engines can find it and people can
govern it. Registering one puts it in the same hierarchy as tables, with a name, an owner, tags, and
permissions.

Gravitino stores the definition rather than executing it. Whether a function runs, and how, is a
question for the engine that reads it.

## Quick Start

**1. Open a catalog.** Functions live in a schema, alongside the tables they usually operate on. See
[Catalogs and Schemas](./catalogs-and-schemas.md).

**2. Register the function.** Registration is done through the API rather than the UI. A function
needs a name, its type, and at least one definition: a parameter list, a return type, and an
implementation in SQL, Python, or Java.

**3. Use it from an engine.** An engine reading through Gravitino resolves the function by name.

## The Function Model

### Function Types

| Type        | Returns                                            |
|-------------|----------------------------------------------------|
| `SCALAR`    | One value per row                                  |
| `AGGREGATE` | One value per group                                |
| `TABLE`     | A set of rows, for table-valued operations         |

The type is declared when the function is registered and tells an engine how to call it.

### Determinism

A function is marked deterministic or not. A deterministic function returns the same result for the
same arguments every time, which lets an engine cache, reorder, or eliminate calls to it. Marking a
function deterministic when it is not, such as one that reads the clock or a random source, invites
an engine to optimize in ways that change the answer.

Gravitino records the flag and does not verify it.

### Definitions and Implementations

A function has two levels beneath it.

A definition is an overload: one parameter list and one return type. A function with several
definitions is the same name accepting different arguments, the way `round(x)` and `round(x, digits)`
are one function in most engines.

An implementation is how a definition is expressed for a given engine. Each declares a language and
a runtime, so one definition can carry a SQL version for Trino and a Python version for Spark, and
each engine uses the one it can execute.

| Language | Carries                                          |
|----------|--------------------------------------------------|
| `SQL`    | The SQL text of the function body                |
| `PYTHON` | A handler entrypoint, and optionally inline code |
| `JAVA`   | A class name                                     |

Runtimes are `SPARK` and `TRINO`. Java and Python implementations can also carry resources, such as
a jar or a package the runtime needs to load.

## Working With Functions in the UI

Opening a schema lists its functions, and selecting one shows its type, its determinism flag, its
parameters, and its definitions.

The UI displays functions but does not register them. Registering, altering, and dropping a function
all go through the API, as does attaching a tag, which the function page does not display either.

## Permissions

| Privilege           | Grantable on                           | What it allows                    |
|---------------------|----------------------------------------|-----------------------------------|
| `REGISTER_FUNCTION` | Metalake, catalog, schema, or function | Registering functions             |
| `EXECUTE_FUNCTION`  | Metalake, catalog, schema, or function | Calling a function from an engine |
| `MODIFY_FUNCTION`   | Metalake, catalog, schema, or function | Altering a registered function    |

Granting at a wider scope covers everything beneath it. Dropping a function is reserved for the
metalake owner and the object owner.

## Using the API

Functions can be registered, listed, altered, and dropped over REST and through the Java and Python
clients. Endpoints, payload shapes, and worked examples are in
[Manage User-Defined Functions](./manage-user-defined-function-using-gravitino.md).
