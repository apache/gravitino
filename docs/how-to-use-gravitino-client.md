---
title: "Java Client"
slug: "/how-to-use-gravitino-client"
date: 2025-07-09
keyword: "Gravitino client"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Use Gravitino Java client library with Spark, Spring and other Java environment or
use Gravitino Python client library with Spark, PyTorch, Tensorflow, Ray and Python environment.

First of all, you must have a Gravitino server set up and run, you can refer document of 
[how to install Gravitino](./how-to-install.md) to build Gravitino server from source code and 
install it in your local.

## Java Client

Customize the Gravitino Java client by using `withClientConfig` like this:

```java
 Map<String, String> properties =
        ImmutableMap.of(
            "gravitino.client.connectionTimeoutMs", "10", 
            "gravitino.client.socketTimeoutMs", "10"
        );

GravitinoClient gravitinoClient = GravitinoClient.builder("http://localhost:8090")
.withMetalake("metalake")
.withClientConfig(properties) // add custom client config (optional)
.builder();

GravitinoAdminClient gravitinoAdminClient = GravitinoAdminClient.builder("http://localhost:8090")
.withClientConfig(properties) // add custom client config (optional)
.builder();
// ...
```

### Java Client Configuration

| Configuration item                     | Description                                          | Default value       | Required |
|----------------------------------------|------------------------------------------------------|---------------------|----------|
| `gravitino.client.connectionTimeoutMs` | An optional http connection timeout in milliseconds. | `180000`(3 minutes) | No       |
| `gravitino.client.socketTimeoutMs`     | An optional http socket timeout in milliseconds.     | `180000`(3 minutes) | No       |

**Note:** Invalid configuration properties will result in exceptions.

## Python Client

Customize the Gravitino Python client with config properties like this:

```python
gravitino_admin_client = GravitinoAdminClient(
   uri="http://localhost:8090",
   client_config={"gravitino_client_request_timeout": 60},
)
# ...

gravitino_client = GravitinoClient(
   uri="http://localhost:8090",
   metalake_name="test",
   client_config={"gravitino_client_request_timeout": 60},
)
# ...
```

### Python Client Configuration

| Configuration item                 | Description                            | Default value | Required |
|------------------------------------|----------------------------------------|---------------|----------|
| `gravitino_client_request_timeout` | An optional client timeout in seconds. | `10`          | No       |

**Note:** Invalid configuration properties will result in exceptions. 

## Retrying concurrent metadata changes

If another writer changes metadata during an alter or drop, the server returns HTTP 409
with error code `1012`. The Java and Python clients raise `OptimisticLockException`.
Import the exception in Java:

```java
import org.apache.gravitino.exceptions.OptimisticLockException;
```

Or in Python:

```python
from gravitino.exceptions.base import OptimisticLockException
```

Catch this exception, reload the latest metadata, reconsider your intended change, and
retry with a bounded number of attempts. Do not replay a stale update unchanged or
retry every HTTP 409 response, since other conflicts may require a different action.
