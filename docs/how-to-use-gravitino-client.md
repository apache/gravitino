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

GravitinoClient gravitinoClient = 
   GravitinoClient.builder("http://localhost:8090")
      .withMetalake("metalake")
      .withClientConfig(properties) // add custom client config (optional)
      .build();

GravitinoAdminClient gravitinoAdminClient = 
   GravitinoAdminClient.builder("http://localhost:8090")
      .withClientConfig(properties) // add custom client config (optional)
      .build();
// ...
```

### Java Client Configuration

| Configuration item                     | Description                                          | Default value       | Required |
|----------------------------------------|------------------------------------------------------|---------------------|----------|
| `gravitino.client.connectionTimeoutMs` | An optional http connection timeout in milliseconds. | `180000`(3 minutes) | No       |
| `gravitino.client.socketTimeoutMs`     | An optional http socket timeout in milliseconds.     | `180000`(3 minutes) | No       |

**Note:** Invalid configuration properties will result in exceptions.

### Java Client TLS Configuration

To connect to a Gravitino server over HTTPS using a private certificate authority or a custom trust store, configure a `TLSConfigurer` and pass it to the client builder:

```java
import java.nio.file.Path;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.TLSConfigurer;
import org.apache.gravitino.client.TLSConfigurers;

TLSConfigurer tlsConfigurer =
    TLSConfigurers.builder()
        .trustStore(
            Path.of("/path/to/client-truststore.p12"),
            "truststore-password")
        .build();

GravitinoClient gravitinoClient =
    GravitinoClient.builder("https://localhost:8433")
        .withMetalake("metalake")
        .withTlsConfigurer(tlsConfigurer)
        .build();

GravitinoAdminClient gravitinoAdminClient =
    GravitinoAdminClient.builder("https://localhost:8433")
        .withTlsConfigurer(tlsConfigurer)
        .build();
```

The trust store contains certificates that the client trusts when verifying the Gravitino server.

For mutual TLS (mTLS), configure both a trust store and a client key store:

```java
TLSConfigurer tlsConfigurer =
    TLSConfigurers.builder()
        .trustStore(
            Path.of("/path/to/client-truststore.p12"),
            "truststore-password")
        .keyStore(
            Path.of("/path/to/client-keystore.p12"),
            "keystore-password")
        .build();

GravitinoClient gravitinoClient =
    GravitinoClient.builder("https://localhost:8433")
        .withMetalake("metalake")
        .withTlsConfigurer(tlsConfigurer)
        .build();
```

The key store contains the client certificate and private key used when the Gravitino server requires client certificate authentication.

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
