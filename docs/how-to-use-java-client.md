---
title: "How to use Apache Gravitino Java client"
slug: /how-to-use-gravitino-java-client
date: 2025-07-09
keyword: Gravitino Java client
license: This software is licensed under the Apache License version 2.
---

## Introduction

You can use Gravitino Java client library with Spark, Spring and other Java environment.

First of all, you must have a Gravitino server set up and run, you can refer document of 
[how to install Gravitino](./how-to-install.md) to build Gravitino server from source code and 
install it in your local.

## Gravitino Java client configurations

You can customize the Gravitino Java client by using `withClientConfig`. 
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
