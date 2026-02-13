---
title: Manage user-defined functions using Gravitino
slug: /manage-user-defined-function-using-gravitino
keyword: Gravitino user-defined function UDF manage
license: This software is licensed under the Apache License version 2.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This page introduces how to manage user-defined functions (UDFs) in Apache Gravitino. Gravitino
provides a centralized function registry that allows you to define custom functions once and
share them across multiple compute engines like Spark and Trino.

A function in Gravitino is characterized by:

- **Name**: The function identifier within a schema.
- **Function type**: `SCALAR` (row-by-row operations), `AGGREGATE` (group operations), or
  `TABLE` (set-returning operations).
- **Deterministic**: Whether the function always returns the same result for the same input.
- **Definitions**: One or more overloads, each with a specific parameter list, return type
  (or return columns for table functions), and one or more implementations for different
  runtimes (e.g. Spark, Trino).

Each definition can have multiple implementations in different languages (SQL, Java, Python)
targeting different runtimes. **Each definition must have at most one implementation per
runtime** — for example, you cannot have two implementations both targeting `SPARK` in the
same definition. To replace an existing implementation, use `updateImpl` instead of `addImpl`.

| Language | Key fields             | Description                                          |
|----------|------------------------|------------------------------------------------------|
| SQL      | `sql`                  | An inline SQL expression.                            |
| Java     | `className`            | Fully qualified Java class name.                     |
| Python   | `handler`, `codeBlock` | Python handler entry point and optional inline code. |

To use function management, please make sure that:

 - The Gravitino server has started and is serving at, e.g. [http://localhost:8090](http://localhost:8090).
 - A metalake has been created.
 - A catalog has been created within the metalake.
 - A schema has been created within the catalog.

## Function operations

### Register a function

You can register a function by sending a `POST` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/functions`
endpoint or just use the Gravitino Java/Python client. The following is an example of registering
a scalar function:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "add_one",
  "functionType": "SCALAR",
  "deterministic": true,
  "comment": "A scalar function that adds one to the input",
  "definitions": [
    {
      "parameters": [
        {"name": "x", "dataType": "integer"}
      ],
      "returnType": "integer",
      "impls": [
        {
          "language": "SQL",
          "runtime": "TRINO",
          "sql": "x + 1"
        }
      ]
    }
  ]
}' http://localhost:8090/api/metalakes/example/catalogs/my_catalog/schemas/my_schema/functions
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("example")
    .build();

Catalog catalog = gravitinoClient.loadCatalog("my_catalog");
FunctionCatalog functionCatalog = catalog.asFunctionCatalog();

// Define the function parameter
FunctionParam param = FunctionParams.of("x", Types.IntegerType.get());

// Create a SQL implementation for Trino
FunctionImpl sqlImpl = FunctionImpls.ofSql(
    FunctionImpl.RuntimeType.TRINO, "x + 1");

// Create the function definition with return type
FunctionDefinition definition = FunctionDefinitions.of(
    new FunctionParam[] {param},
    Types.IntegerType.get(),
    new FunctionImpl[] {sqlImpl});

// Register the function
Function function = functionCatalog.registerFunction(
    NameIdentifier.of("my_schema", "add_one"),
    "A scalar function that adds one to the input",
    FunctionType.SCALAR,
    true,
    new FunctionDefinition[] {definition});
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="example")

catalog: Catalog = gravitino_client.load_catalog(name="my_catalog")
function_catalog = catalog.as_function_catalog()

# Define the function parameter
param = FunctionParams.of("x", Types.IntegerType.get())

# Create a SQL implementation for Trino
sql_impl = (
    SQLImpl.builder()
    .with_runtime_type(SQLImpl.RuntimeType.TRINO)
    .with_sql("x + 1")
    .build()
)

# Create the function definition
definition = FunctionDefinitions.of([param], Types.IntegerType.get(), [sql_impl])

# Register the function
function = function_catalog.register_function(
    ident=NameIdentifier.of("my_schema", "add_one"),
    comment="A scalar function that adds one to the input",
    function_type=FunctionType.SCALAR,
    deterministic=True,
    definitions=[definition],
)
```

</TabItem>
</Tabs>

For table-valued functions, use `returnColumns` instead of `returnType` in the function
definition, and use `FunctionType.TABLE` as the function type:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "generate_series",
  "functionType": "TABLE",
  "deterministic": true,
  "comment": "A table function that generates a series of integers",
  "definitions": [
    {
      "parameters": [
        {"name": "start_val", "dataType": "integer"},
        {"name": "end_val", "dataType": "integer"}
      ],
      "returnColumns": [
        {"name": "value", "dataType": "integer", "comment": "The generated integer value"}
      ],
      "impls": [
        {
          "language": "JAVA",
          "runtime": "SPARK",
          "className": "com.example.GenerateSeriesFunction",
          "resources": {
            "jars": ["hdfs:///path/to/udtf.jar"]
          }
        }
      ]
    }
  ]
}' http://localhost:8090/api/metalakes/example/catalogs/my_catalog/schemas/my_schema/functions
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
FunctionCatalog functionCatalog = catalog.asFunctionCatalog();

FunctionParam[] params = new FunctionParam[] {
    FunctionParams.of("start_val", Types.IntegerType.get()),
    FunctionParams.of("end_val", Types.IntegerType.get())
};

FunctionColumn[] returnColumns = new FunctionColumn[] {
    FunctionColumn.of("value", Types.IntegerType.get(), "The generated integer value")
};

FunctionImpl javaImpl = FunctionImpls.ofJava(
    FunctionImpl.RuntimeType.SPARK,
    "com.example.GenerateSeriesFunction",
    FunctionResources.of(
        new String[] {"hdfs:///path/to/udtf.jar"}, null, null),
    null);

FunctionDefinition definition = FunctionDefinitions.of(
    params, returnColumns, new FunctionImpl[] {javaImpl});

Function function = functionCatalog.registerFunction(
    NameIdentifier.of("my_schema", "generate_series"),
    "A table function that generates a series of integers",
    FunctionType.TABLE,
    true,
    new FunctionDefinition[] {definition});
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
# ...
function_catalog = catalog.as_function_catalog()

# Define the function parameters
param_start = FunctionParams.of("start_val", Types.IntegerType.get())
param_end = FunctionParams.of("end_val", Types.IntegerType.get())

# Define the return columns for the table function
return_columns = [
    FunctionColumn.of("value", Types.IntegerType.get(), "The generated integer value")
]

java_impl = (
    JavaImpl.builder()
    .with_runtime_type(JavaImpl.RuntimeType.SPARK)
    .with_class_name("com.example.GenerateSeriesFunction")
    .build()
)

# Use of_table() for table-valued functions
definition = FunctionDefinitions.of_table([param_start, param_end], return_columns, [java_impl])

function = function_catalog.register_function(
    ident=NameIdentifier.of("my_schema", "generate_series"),
    comment="A table function that generates a series of integers",
    function_type=FunctionType.TABLE,
    deterministic=True,
    definitions=[definition],
)
```

</TabItem>
</Tabs>

### Get a function

You can get a function by sending a `GET` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/functions/{function_name}`
endpoint or by using the Gravitino Java/Python client. The following is an example of getting
a function:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/example/catalogs/my_catalog/schemas/my_schema/functions/add_one
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
Catalog catalog = gravitinoClient.loadCatalog("my_catalog");
FunctionCatalog functionCatalog = catalog.asFunctionCatalog();

Function function = functionCatalog.getFunction(
    NameIdentifier.of("my_schema", "add_one"));
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="example")

catalog: Catalog = gravitino_client.load_catalog(name="my_catalog")
function = catalog.as_function_catalog().get_function(
    ident=NameIdentifier.of("my_schema", "add_one"))
```

</TabItem>
</Tabs>

### List functions

You can list all the functions in a schema by sending a `GET` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/functions`
endpoint or by using the Gravitino Java/Python client. The following is an example of listing
all the functions in a schema:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/example/catalogs/my_catalog/schemas/my_schema/functions
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
Catalog catalog = gravitinoClient.loadCatalog("my_catalog");
FunctionCatalog functionCatalog = catalog.asFunctionCatalog();

NameIdentifier[] identifiers = functionCatalog.listFunctions(
    Namespace.of("my_schema"));
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="example")

catalog: Catalog = gravitino_client.load_catalog(name="my_catalog")
functions = catalog.as_function_catalog().list_functions(
    namespace=Namespace.of("my_schema"))
```

</TabItem>
</Tabs>

You can also list functions with detailed information by adding the `details` query parameter.
This returns the full function objects instead of just the identifiers.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
"http://localhost:8090/api/metalakes/example/catalogs/my_catalog/schemas/my_schema/functions?details=true"
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
Catalog catalog = gravitinoClient.loadCatalog("my_catalog");
FunctionCatalog functionCatalog = catalog.asFunctionCatalog();

Function[] functions = functionCatalog.listFunctionInfos(
    Namespace.of("my_schema"));
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="example")

catalog: Catalog = gravitino_client.load_catalog(name="my_catalog")
functions = catalog.as_function_catalog().list_function_infos(
    namespace=Namespace.of("my_schema"))
```

</TabItem>
</Tabs>

### Alter a function

You can modify a function by sending a `PUT` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/functions/{function_name}`
endpoint or using the Gravitino Java/Python client. The following is an example of updating a
function's comment:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "updates": [
    {
      "@type": "updateComment",
      "newComment": "An improved scalar function that adds one"
    }
  ]
}' http://localhost:8090/api/metalakes/example/catalogs/my_catalog/schemas/my_schema/functions/add_one
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
Catalog catalog = gravitinoClient.loadCatalog("my_catalog");
FunctionCatalog functionCatalog = catalog.asFunctionCatalog();

Function updated = functionCatalog.alterFunction(
    NameIdentifier.of("my_schema", "add_one"),
    FunctionChange.updateComment("An improved scalar function that adds one"));
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="example")

catalog: Catalog = gravitino_client.load_catalog(name="my_catalog")
function = catalog.as_function_catalog().alter_function(
    NameIdentifier.of("my_schema", "add_one"),
    FunctionChange.update_comment("An improved scalar function that adds one"),
)
```

</TabItem>
</Tabs>

#### Supported modifications

The following operations are supported for altering a function:

| Operation                 | JSON Example                                                                                | Java Method                                            | Python Method                                           |
|---------------------------|---------------------------------------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| **Update comment**        | `{"@type":"updateComment","newComment":"new comment"}`                                      | `FunctionChange.updateComment("new comment")`          | `FunctionChange.update_comment("new comment")`          |
| **Add definition**        | `{"@type":"addDefinition","definition":{...}}`                                              | `FunctionChange.addDefinition(definition)`             | `FunctionChange.add_definition(definition)`             |
| **Remove definition**     | `{"@type":"removeDefinition","parameters":[{"name":"x","dataType":"integer"}]}`             | `FunctionChange.removeDefinition(params)`              | `FunctionChange.remove_definition(params)`              |
| **Add implementation**    | `{"@type":"addImpl","parameters":[...],"implementation":{...}}`                             | `FunctionChange.addImpl(params, impl)`                 | `FunctionChange.add_impl(params, impl)`                 |
| **Update implementation** | `{"@type":"updateImpl","parameters":[...],"runtime":"SPARK","implementation":{...}}`        | `FunctionChange.updateImpl(params, runtime, impl)`     | `FunctionChange.update_impl(params, runtime, impl)`     |
| **Remove implementation** | `{"@type":"removeImpl","parameters":[{"name":"x","dataType":"integer"}],"runtime":"SPARK"}` | `FunctionChange.removeImpl(params, RuntimeType.SPARK)` | `FunctionChange.remove_impl(params, RuntimeType.SPARK)` |

:::note
When using `addImpl`, the runtime of the new implementation must not already exist in the
 target definition. Use `updateImpl` to replace an existing implementation for a given runtime.
:::

The following is an example of adding a new implementation to an existing function definition:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "updates": [
    {
      "@type": "addImpl",
      "parameters": [
        {"name": "x", "dataType": "integer"}
      ],
      "implementation": {
        "language": "JAVA",
        "runtime": "TRINO",
        "className": "com.example.AddOneFunction",
        "resources": {
          "jars": ["hdfs:///path/to/udf.jar"]
        }
      }
    }
  ]
}' http://localhost:8090/api/metalakes/example/catalogs/my_catalog/schemas/my_schema/functions/add_one
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
FunctionParam[] params = new FunctionParam[] {
    FunctionParams.of("x", Types.IntegerType.get())
};
FunctionImpl javaImpl = FunctionImpls.ofJava(
    FunctionImpl.RuntimeType.TRINO,
    "com.example.AddOneFunction",
    FunctionResources.of(
        new String[] {"hdfs:///path/to/udf.jar"}, null, null),
    null);

Function updated = functionCatalog.alterFunction(
    NameIdentifier.of("my_schema", "add_one"),
    FunctionChange.addImpl(params, javaImpl));
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
# ...
param = FunctionParams.of("x", Types.IntegerType.get())

java_impl = (
    JavaImpl.builder()
    .with_runtime_type(JavaImpl.RuntimeType.TRINO)
    .with_class_name("com.example.AddOneFunction")
    .build()
)

function = catalog.as_function_catalog().alter_function(
    NameIdentifier.of("my_schema", "add_one"),
    FunctionChange.add_impl([param], java_impl),
)
```

</TabItem>
</Tabs>

### Drop a function

You can drop a function by sending a `DELETE` request to the `/api/metalakes/{metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/functions/{function_name}`
endpoint or by using the Gravitino Java/Python client. The following is an example of dropping
a function:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/example/catalogs/my_catalog/schemas/my_schema/functions/add_one
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
Catalog catalog = gravitinoClient.loadCatalog("my_catalog");
boolean dropped = catalog.asFunctionCatalog().dropFunction(
    NameIdentifier.of("my_schema", "add_one"));
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="example")

catalog: Catalog = gravitino_client.load_catalog(name="my_catalog")
dropped: bool = catalog.as_function_catalog().drop_function(
    NameIdentifier.of("my_schema", "add_one"))
```

</TabItem>
</Tabs>

## Advanced examples

### Register a function with multiple overloads

A function can have multiple definitions (overloads) with different parameter lists. Each
definition has its own return type and implementations.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "add",
  "functionType": "SCALAR",
  "deterministic": true,
  "comment": "An overloaded add function",
  "definitions": [
    {
      "parameters": [
        {"name": "x", "dataType": "integer"},
        {"name": "y", "dataType": "integer"}
      ],
      "returnType": "integer",
      "impls": [
        {
          "language": "SQL",
          "runtime": "TRINO",
          "sql": "x + y"
        }
      ]
    },
    {
      "parameters": [
        {"name": "x", "dataType": "double"},
        {"name": "y", "dataType": "double"}
      ],
      "returnType": "double",
      "impls": [
        {
          "language": "SQL",
          "runtime": "TRINO",
          "sql": "x + y"
        }
      ]
    }
  ]
}' http://localhost:8090/api/metalakes/example/catalogs/my_catalog/schemas/my_schema/functions
```

</TabItem>
<TabItem value="java" label="Java">

```java
// ...
FunctionCatalog functionCatalog = catalog.asFunctionCatalog();

// Definition 1: integer overload
FunctionDefinition intDef = FunctionDefinitions.of(
    new FunctionParam[] {
        FunctionParams.of("x", Types.IntegerType.get()),
        FunctionParams.of("y", Types.IntegerType.get())
    },
    Types.IntegerType.get(),
    new FunctionImpl[] {
        FunctionImpls.ofSql(FunctionImpl.RuntimeType.TRINO, "x + y")
    });

// Definition 2: double overload
FunctionDefinition doubleDef = FunctionDefinitions.of(
    new FunctionParam[] {
        FunctionParams.of("x", Types.DoubleType.get()),
        FunctionParams.of("y", Types.DoubleType.get())
    },
    Types.DoubleType.get(),
    new FunctionImpl[] {
        FunctionImpls.ofSql(FunctionImpl.RuntimeType.TRINO, "x + y")
    });

Function function = functionCatalog.registerFunction(
    NameIdentifier.of("my_schema", "add"),
    "An overloaded add function",
    FunctionType.SCALAR,
    true,
    FunctionDefinitions.of(intDef, doubleDef));
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
# ...
function_catalog = catalog.as_function_catalog()

# Definition 1: integer overload
int_param_x = FunctionParams.of("x", Types.IntegerType.get())
int_param_y = FunctionParams.of("y", Types.IntegerType.get())
sql_int = (
    SQLImpl.builder()
    .with_runtime_type(SQLImpl.RuntimeType.TRINO)
    .with_sql("x + y")
    .build()
)
int_def = FunctionDefinitions.of([int_param_x, int_param_y], Types.IntegerType.get(), [sql_int])

# Definition 2: double overload
double_param_x = FunctionParams.of("x", Types.DoubleType.get())
double_param_y = FunctionParams.of("y", Types.DoubleType.get())
sql_double = (
    SQLImpl.builder()
    .with_runtime_type(SQLImpl.RuntimeType.TRINO)
    .with_sql("x + y")
    .build()
)
double_def = FunctionDefinitions.of([double_param_x, double_param_y], Types.DoubleType.get(), [sql_double])

function = function_catalog.register_function(
    ident=NameIdentifier.of("my_schema", "add"),
    comment="An overloaded add function",
    function_type=FunctionType.SCALAR,
    deterministic=True,
    definitions=[int_def, double_def],
)
```

</TabItem>
</Tabs>

## Using functions in compute engines

Once a function is registered in Gravitino, it can be used in supported compute engines.
The engine's connector loads the function from Gravitino and invokes the appropriate
implementation based on the runtime.

| Engine | Runtime             | Documentation                                                                      |
|--------|---------------------|------------------------------------------------------------------------------------|
| Spark  | `RuntimeType.SPARK` | [Spark connector - User-defined functions](spark-connector/spark-connector-udf.md) |

:::note
Support for additional engines (e.g. Trino, Flink) will be documented as they become available.
:::
