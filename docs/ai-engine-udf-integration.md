---
title: AI engine UDF integration
slug: /ai-engine-udf-integration
keyword: Gravitino UDF Ray Dask AI engine integration
license: This software is licensed under the Apache License version 2.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This page describes how to use Gravitino-registered UDFs with AI compute engines
such as [Ray](https://www.ray.io/) and [Dask](https://www.dask.org/).

## Overview

Gravitino's AI engine connectors allow Python-based AI/ML pipelines to load
user-defined functions that are centrally managed in Gravitino and execute them
as distributed tasks on Ray or Dask clusters. Each connector:

1. Connects to the Gravitino server and retrieves function metadata.
2. Filters for implementations that match the target runtime (`RAY` or `DASK`).
3. Resolves the `PythonImpl` handler (either from an installed module or an
   inline code block).
4. Wraps the resolved callable with the engine-specific primitive
   (`ray.remote` or `dask.delayed`).

### Supported runtimes

| Runtime | Wrapper          | Connector class    |
|---------|------------------|--------------------|
| `RAY`   | `ray.remote`     | `RayConnector`     |
| `DASK`  | `dask.delayed`   | `DaskConnector`    |

## Prerequisites

- A running Gravitino server.
- A metalake, catalog, and schema already created.
- Functions registered with a `PythonImpl` implementation targeting the `RAY`
  or `DASK` runtime.
- The `ray` or `dask` Python package installed in your environment.

```bash
# For Ray
pip install apache-gravitino ray

# For Dask
pip install apache-gravitino dask distributed
```

## Registering a function for AI engines

When registering a UDF, add a `PythonImpl` with the desired `RuntimeType`.
The implementation can reference either an installed Python module or provide
inline code.

### Module-level handler

```python
from gravitino.api.function.function_impl import FunctionImpl
from gravitino.api.function.python_impl import PythonImpl

ray_impl = (
    PythonImpl.builder()
    .with_runtime_type(FunctionImpl.RuntimeType.RAY)
    .with_handler("my_package.transforms.add_one")
    .build()
)
```

### Inline code block

```python
code = """
def add_one(x):
    return x + 1
"""

ray_impl = (
    PythonImpl.builder()
    .with_runtime_type(FunctionImpl.RuntimeType.RAY)
    .with_handler("add_one")
    .with_code_block(code)
    .build()
)
```

Register the function using the Python client as described in the
[UDF management guide](manage-user-defined-function-using-gravitino.md).

## Using the Ray connector

```python
import ray
from gravitino.ai_engine.ray_connector import RayConnector

ray.init()

connector = RayConnector(
    gravitino_uri="http://localhost:8090",
    metalake="my_metalake",
    catalog="my_catalog",
    schema="my_schema",
)

# List available Ray functions
print(connector.list_functions())

# Load and invoke a single function
add_one = connector.load_function("add_one")
result = ray.get(add_one.remote(41))
print(result)  # 42

# Load all Ray-runtime functions
all_fns = connector.load_functions()
```

### Ray resource options

You can pass resource hints (CPU, GPU, memory) when constructing the connector.
These options are forwarded to every `ray.remote` call:

```python
connector = RayConnector(
    gravitino_uri="http://localhost:8090",
    metalake="my_metalake",
    catalog="my_catalog",
    schema="my_schema",
    ray_options={"num_cpus": 2, "num_gpus": 1},
)
```

## Using the Dask connector

```python
from dask.distributed import Client
from gravitino.ai_engine.dask_connector import DaskConnector

client = Client()  # connect to a Dask cluster

connector = DaskConnector(
    gravitino_uri="http://localhost:8090",
    metalake="my_metalake",
    catalog="my_catalog",
    schema="my_schema",
)

# List available Dask functions
print(connector.list_functions())

# Load and invoke a single function
add_one = connector.load_function("add_one")
result = add_one(41).compute()
print(result)  # 42

# Load all Dask-runtime functions
all_fns = connector.load_functions()
```

### Dask delayed options

You can pass options to `dask.delayed` via the `dask_options` parameter:

```python
connector = DaskConnector(
    gravitino_uri="http://localhost:8090",
    metalake="my_metalake",
    catalog="my_catalog",
    schema="my_schema",
    dask_options={"pure": True},
)
```

## Architecture

```
┌──────────────────────────────────────────────┐
│                  User Code                    │
│  connector.load_function("add_one")           │
└──────────┬───────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────┐
│       RayConnector / DaskConnector            │
│  1. list / get function from Gravitino        │
│  2. Filter PythonImpl by runtime (RAY/DASK)   │
│  3. Resolve handler → Python callable         │
│  4. Wrap: ray.remote(fn) / dask.delayed(fn)   │
└──────────┬───────────────────────────────────┘
           │  REST API
           ▼
┌──────────────────────────────────────────────┐
│            Gravitino Server                   │
│  /api/metalakes/.../functions/{name}          │
└──────────────────────────────────────────────┘
```
