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

A Python implementation names a handler entrypoint, and can carry inline code and the packages the
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

Changes are applied as a list in one request.

| Change             | JSON                                                         | Java                                             |
|--------------------|--------------------------------------------------------------|--------------------------------------------------|
| Rename             | `{"@type":"rename","newName":"add_one_v2"}`                  | `FunctionChange.rename("add_one_v2")`            |
| Update the comment | `{"@type":"updateComment","newComment":"new_comment"}`       | `FunctionChange.updateComment("new_comment")`    |
| Set a property     | `{"@type":"setProperty","property":"key1","value":"value1"}` | `FunctionChange.setProperty("key1", "value1")`   |
| Remove a property  | `{"@type":"removeProperty","property":"key1"}`               | `FunctionChange.removeProperty("key1")`          |

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "updates": [
    {"@type": "updateComment", "newComment": "Adds one, reviewed"}
  ]
}' http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/functions/add_one
```

</TabItem>
<TabItem value="java" label="Java">

```java
Function function = functions.alterFunction(
    NameIdentifier.of("public", "add_one"),
    FunctionChange.updateComment("Adds one, reviewed"));
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
