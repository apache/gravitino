---
title: "Manage User-Defined Functions"
slug: "/manage-user-defined-function-using-gravitino"
keyword: "function management, UDF, user-defined function, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

This page covers the Gravitino API for functions. For what a function is, the function types,
determinism, and how definitions and implementations relate, see [Functions](./functions.md). For
creating the catalog and schema a function lives in, see
[Manage Catalogs and Schemas](./manage-catalogs-and-schemas.md).

:::note
Registering a function stores its metadata in Gravitino; whether an engine can call it depends on
the engine's connector. The Trino connector exposes only implementations with language `SQL` and
runtime `TRINO`; Python and Java implementations, and any implementation with another runtime, are
managed in Gravitino but are not visible or callable from Trino. See
[Trino Connector UDF Support](./trino-connector/udf-support.md).
:::

## Function Operations

### Register a SQL Function

A function needs a name, a type, a determinism flag, and at least one definition. A definition
carries its parameters, its return type, and one or more implementations.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "add_one",
  "functionType": "SCALAR",
  "deterministic": true,
  "comment": "Adds one to the input",
  "definitions": [
    {
      "parameters": [{"name": "x", "dataType": "integer"}],
      "returnType": "integer",
      "impls": [
        {"language": "SQL", "runtime": "TRINO", "sql": "x + 1"}
      ]
    }
  ]
}' http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/functions
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = client.loadCatalog("sales");
FunctionCatalog functions = catalog.asFunctionCatalog();

FunctionImpl sqlImpl = FunctionImpls.ofSql(FunctionImpl.RuntimeType.TRINO, "x + 1");

FunctionDefinition definition = FunctionDefinitions.of(
    new FunctionParam[] {FunctionParams.of("x", Types.IntegerType.get())},
    Types.IntegerType.get(),
    new FunctionImpl[] {sqlImpl});

Function function = functions.registerFunction(
    NameIdentifier.of("public", "add_one"),
    "Adds one to the input",
    FunctionType.SCALAR,
    true,
    new FunctionDefinition[] {definition});
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalog = client.load_catalog("sales")
functions = catalog.as_function_catalog()

sql_impl = (
    SQLImpl.builder()
    .with_runtime_type(SQLImpl.RuntimeType.TRINO)
    .with_sql("x + 1")
    .build()
)

definition = FunctionDefinitions.of(
    [FunctionParams.of("x", Types.IntegerType.get())],
    Types.IntegerType.get(),
    [sql_impl])

function = functions.register_function(
    ident=NameIdentifier.of("public", "add_one"),
    comment="Adds one to the input",
    function_type=FunctionType.SCALAR,
    deterministic=True,
    definitions=[definition])
```

</TabItem>
</Tabs>

### Register a Python Function

A Python implementation must name a non-blank `handler` entrypoint. Inline code in `codeBlock` is
optional and does not replace the handler. The implementation can also carry the resources the
runtime needs.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "normalize_phone",
  "functionType": "SCALAR",
  "deterministic": true,
  "comment": "Strips formatting from a phone number",
  "definitions": [
    {
      "parameters": [{"name": "raw", "dataType": "varchar(64)"}],
      "returnType": "varchar(64)",
      "impls": [
        {
          "language": "PYTHON",
          "runtime": "SPARK",
          "handler": "normalize.main",
          "codeBlock": "def main(raw):\n    return \"\".join(c for c in raw if c.isdigit())"
        }
      ]
    }
  ]
}' http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/functions
```

</TabItem>
</Tabs>

### Register a Java Function

A Java implementation names a class, and usually the jar that holds it. A table-valued function
declares `returnColumns` rather than a single `returnType`.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "generate_series",
  "functionType": "TABLE",
  "deterministic": true,
  "comment": "Generates a range of integers",
  "definitions": [
    {
      "parameters": [
        {"name": "start_val", "dataType": "integer"},
        {"name": "end_val", "dataType": "integer"}
      ],
      "returnColumns": [
        {"name": "value", "dataType": "integer", "comment": "The generated value"}
      ],
      "impls": [
        {
          "language": "JAVA",
          "runtime": "SPARK",
          "className": "com.example.GenerateSeriesFunction",
          "resources": {"jars": ["hdfs:///path/to/udtf.jar"]}
        }
      ]
    }
  ]
}' http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/functions
```

</TabItem>
</Tabs>

### Register Overloads

A function with several definitions accepts several parameter lists under one name. Each definition
carries its own return type and implementations.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "add",
  "functionType": "SCALAR",
  "deterministic": true,
  "comment": "Adds two or three integers",
  "definitions": [
    {
      "parameters": [
        {"name": "x", "dataType": "integer"},
        {"name": "y", "dataType": "integer"}
      ],
      "returnType": "integer",
      "impls": [{"language": "SQL", "runtime": "TRINO", "sql": "x + y"}]
    },
    {
      "parameters": [
        {"name": "x", "dataType": "integer"},
        {"name": "y", "dataType": "integer"},
        {"name": "z", "dataType": "integer"}
      ],
      "returnType": "integer",
      "impls": [{"language": "SQL", "runtime": "TRINO", "sql": "x + y + z"}]
    }
  ]
}' http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/functions
```

</TabItem>
</Tabs>

### Get a Function

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/functions/add_one
```

</TabItem>
<TabItem value="java" label="Java">

```java
Function function = functions.getFunction(NameIdentifier.of("public", "add_one"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
function = functions.get_function(NameIdentifier.of("public", "add_one"))
```

</TabItem>
</Tabs>

### List Functions

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/functions
```

</TabItem>
<TabItem value="java" label="Java">

```java
NameIdentifier[] identifiers = functions.listFunctions(Namespace.of("public"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
identifiers = functions.list_functions(Namespace.of("public"))
```

</TabItem>
</Tabs>

### Alter a Function

Changes are applied as a list in one request. A definition is identified by its parameter list,
and an implementation within a definition by its runtime.

| Change                    | JSON `@type`       | Other JSON fields                         | Java                                                        |
|---------------------------|--------------------|-------------------------------------------|-------------------------------------------------------------|
| Update the comment        | `updateComment`    | `newComment`                              | `FunctionChange.updateComment("new comment")`               |
| Add a definition          | `addDefinition`    | `definition`                              | `FunctionChange.addDefinition(definition)`                  |
| Remove a definition       | `removeDefinition` | `parameters`                              | `FunctionChange.removeDefinition(parameters)`               |
| Add an implementation     | `addImpl`          | `parameters`, `implementation`            | `FunctionChange.addImpl(parameters, implementation)`        |
| Replace an implementation | `updateImpl`       | `parameters`, `runtime`, `implementation` | `FunctionChange.updateImpl(parameters, runtime, implementation)` |
| Remove an implementation  | `removeImpl`       | `parameters`, `runtime`                   | `FunctionChange.removeImpl(parameters, runtime)`            |

In the Java column, `definition` is a `FunctionDefinition`, `parameters` is a `FunctionParam[]`,
`runtime` is a `FunctionImpl.RuntimeType` and `implementation` is a `FunctionImpl`.

`newComment` must not be null or blank; a function's comment cannot be cleared with `updateComment`.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "updates": [
    {"@type": "updateComment", "newComment": "Adds one, reviewed"},
    {
      "@type": "addImpl",
      "parameters": [{"name": "x", "dataType": "integer"}],
      "implementation": {
        "language": "JAVA",
        "runtime": "SPARK",
        "className": "com.example.AddOneFunction"
      }
    }
  ]
}' http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/functions/add_one
```

</TabItem>
<TabItem value="java" label="Java">

```java
FunctionParam[] parameters = new FunctionParam[] {FunctionParams.of("x", Types.IntegerType.get())};

Function function = functions.alterFunction(
    NameIdentifier.of("public", "add_one"),
    FunctionChange.updateComment("Adds one, reviewed"),
    FunctionChange.addImpl(
        parameters,
        FunctionImpls.ofJava(FunctionImpl.RuntimeType.SPARK, "com.example.AddOneFunction")));
```

</TabItem>
</Tabs>

### Drop a Function

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/functions/add_one
```

</TabItem>
<TabItem value="java" label="Java">

```java
boolean dropped = functions.dropFunction(NameIdentifier.of("public", "add_one"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
dropped = functions.drop_function(NameIdentifier.of("public", "add_one"))
```

</TabItem>
</Tabs>
