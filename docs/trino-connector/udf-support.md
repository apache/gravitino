---
title: "Trino Connector UDF Support"
slug: "/trino-connector/udf-support"
keyword: "gravitino connector trino udf function"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The Gravitino Trino connector supports user-defined functions (UDFs) registered in Apache Gravitino.
Functions with `RuntimeType.TRINO` and SQL language implementations are automatically exposed as
[Trino language functions](https://trino.io/docs/current/routines/function.html), making them available for use in Trino queries.

## Mechanism

When Gravitino catalogs contain registered functions, the Trino connector:

1. Lists functions from the Gravitino server for each schema.
2. Filters to include only functions with `RuntimeType.TRINO` and `Language.SQL`.
3. Maps each function implementation to a Trino `LanguageFunction` with a signature token derived from the function name and parameter types.

Only functions with language `SQL` and runtime `TRINO` are visible to and callable from Trino. Functions registered for other languages or runtimes (for example a Python or Java implementation with runtime `SPARK`) are managed in Gravitino but are **not** exposed through this connector: they do not appear in `SHOW FUNCTIONS`, and invoking one fails with a Trino `Function "<catalog>.<schema>.<name>" not registered` error. The function still exists in Gravitino; the connector simply filters it out. The Gravitino UI function detail view shows, per implementation, whether it is exposed through the Trino connector.

### SQL body format

The `sql` field of a `SQL`/`TRINO` implementation is the function body. The connector assembles a complete [Trino SQL routine](https://trino.io/docs/current/routines/function.html) specification (`FUNCTION <name>(<params>) RETURNS <type> [NOT] DETERMINISTIC SECURITY INVOKER ...`) from the function name, parameters, return type and deterministic flag before handing it to Trino. The body may be:

- A bare expression, e.g. `x + 1`. The connector wraps it as `RETURN x + 1`.
- A control statement, e.g. `RETURN x + 1` or `BEGIN ... END`.

A body that is itself a complete `FUNCTION ...` specification is not supported and the function is skipped with a warning. Leading SQL comments in the body are ignored when detecting the statement form.

Function, parameter and row field names are quoted in the generated specification. Trino resolves routine and parameter names case-insensitively regardless of quoting, so the body can reference parameters as plain identifiers.

## Prerequisites

- The Gravitino catalog must support function operations (i.e., implement `FunctionCatalog`).
- Functions must be registered in Gravitino via the Gravitino client or REST API before they can be queried from Trino.

## Register a UDF

Use the Gravitino Java client to register a function:

```java
FunctionCatalog functionCatalog = catalog.asFunctionCatalog();
functionCatalog.registerFunction(
    NameIdentifier.of("my_schema", "add_one"),
    "Adds one to input",
    FunctionType.SCALAR,
    true,
    FunctionDefinitions.of(
        FunctionDefinitions.of(
            FunctionParams.of(FunctionParams.of("x", Types.IntegerType.get())),
            Types.IntegerType.get(),
            FunctionImpls.of(
                FunctionImpls.ofSql(FunctionImpl.RuntimeType.TRINO, "RETURN x + 1")))));
```

## Query UDFs from Trino

Once registered, the function appears in Trino:

```sql
-- List available functions in a schema
SHOW FUNCTIONS FROM catalog.my_schema;

-- Invoke the function
SELECT catalog.my_schema.add_one(5);
-- Returns: 6
```

## Limitations

- **Read-only**: The Trino connector supports listing and invoking Gravitino UDFs. Creating or dropping functions via Trino SQL (`CREATE FUNCTION` / `DROP FUNCTION`) is not yet supported.
- **SQL only**: Only SQL-language implementations are mapped. Java and Python implementations are not exposed to Trino.
- **TRINO runtime only**: Only functions with `RuntimeType.TRINO` are visible. Functions registered with `RuntimeType.SPARK` or other runtimes are filtered out and fail with `Function ... not registered` when invoked.
- **Scalar only**: Only `SCALAR` functions are exposed. Aggregate and table-valued functions are skipped.
- **No parameter defaults**: Trino SQL routines do not support parameter default values, so a parameter's `defaultValue` is ignored and the parameter is required when calling from Trino.
- **Type mapping**: Function parameter and return types are converted from Gravitino types to Trino types. Unsupported types will cause the function to be skipped with a warning log.
