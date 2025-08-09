---
title: "How to use Apache Gravitino client"
slug: /how-to-use-gravitino-client
date: 2025-07-09
keyword: Gravitino client
license: This software is licensed under the Apache License version 2.
---

## Introduction

You can use Gravitino Java client library with Spark, Spring and other Java environment or
use Gravitino Python client library with Spark, PyTorch, Tensorflow, Ray and Python environment.

First of all, you must have a Gravitino server set up and run, you can refer document of 
[how to install Gravitino](./how-to-install.md) to build Gravitino server from source code and 
install it in your local.

## Gravitino Java client

You can customize the Gravitino Java client by using `withClientConfig` like this:

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

### Gravitino Java client configuration

| Configuration item                     | Description                                          | Default value       | Required | Since version |
|----------------------------------------|------------------------------------------------------|---------------------|----------|---------------|
| `gravitino.client.connectionTimeoutMs` | An optional http connection timeout in milliseconds. | `180000`(3 minutes) | No       | 1.0.0         |
| `gravitino.client.socketTimeoutMs`     | An optional http socket timeout in milliseconds.     | `180000`(3 minutes) | No       | 1.0.0         |

**Note:** Invalid configuration properties will result in exceptions.

## Gravitino Python client

You can customize the Gravitino Python client with config properties like this:

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

### Gravitino Python client configuration

| Configuration item                 | Description                            | Default value | Required | Since version |
|------------------------------------|----------------------------------------|---------------|----------|---------------|
| `gravitino_client_request_timeout` | An optional client timeout in seconds. | `10`          | No       | 1.0.0         |

**Note:** Invalid configuration properties will result in exceptions. 
