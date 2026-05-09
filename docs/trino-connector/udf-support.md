---
title: "Apache Gravitino Trino connector UDF support"
slug: /trino-connector/udf-support
keyword: gravitino connector trino udf function
license: "This software is licensed under the Apache License version 2."
---

The Gravitino Trino connector supports user-defined functions (UDFs) registered in Apache Gravitino.
Functions with `RuntimeType.TRINO` and SQL language implementations are automatically exposed as
[Trino language functions](https://trino.io/docs/current/routines/function.html), making them available for use in Trino queries.

## How it works

When Gravitino catalogs contain registered functions, the Trino connector:

1. Lists functions from the Gravitino server for each schema.
2. Filters to include only functions with `RuntimeType.TRINO` and `Language.SQL`.
3. Maps each function implementation to a Trino `LanguageFunction` with a signature token derived from the function name and parameter types.

Functions registered with other runtimes (e.g., `SPARK`) are **not** visible in Trino.

## Prerequisites

- The Gravitino catalog must support function operations (i.e., implement `FunctionCatalog`).
- Functions must be registered in Gravitino via the Gravitino client or REST API before they can be queried from Trino.

## Registering a UDF

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

## Querying UDFs from Trino

Once registered, the function appears in Trino:

```sql
-- List available functions in a schema
SHOW FUNCTIONS FROM catalog.my_schema;

-- Invoke the function
SELECT catalog.my_schema.add_one(5);
-- Returns: 6
```

## Limitations

- **Read-only**: The Trino connector currently supports listing and invoking Gravitino UDFs. Creating or dropping functions via Trino SQL (`CREATE FUNCTION` / `DROP FUNCTION`) is not yet supported.
- **SQL only**: Only SQL-language implementations are mapped. Java and Python implementations are not exposed to Trino.
- **TRINO runtime only**: Only functions with `RuntimeType.TRINO` are visible. Functions registered with `RuntimeType.SPARK` or other runtimes are filtered out.
- **Type mapping**: Function parameter and return types are converted from Gravitino types to Trino types. Unsupported types will cause the function to be skipped with a warning log.
