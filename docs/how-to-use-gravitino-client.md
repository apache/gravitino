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
.build();

GravitinoAdminClient gravitinoAdminClient = GravitinoAdminClient.builder("http://localhost:8090")
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

The Gravitino Java client supports custom TLS configuration through `TLSConfigurers`. This includes custom truststores, mutual TLS (mTLS) authentication, and restrictions on TLS protocols and cipher suites.

For HTTPS connections using the default JVM truststore without client certificate authentication, no additional TLS configuration is required.

#### Custom Truststore

To connect to a server using a private certificate authority (CA) or a self-signed certificate, use `withTrustStore()`:

```java
import java.nio.file.Path;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.TLSConfigurer;
import org.apache.gravitino.client.TLSConfigurers;

TLSConfigurer tlsConfigurer =
    TLSConfigurers.withTrustStore(
        Path.of("/path/to/truststore.p12"),
        "truststore-password");

GravitinoClient gravitinoClient =
    GravitinoClient.builder("https://localhost:8433")
        .withMetalake("metalake")
        .withTlsConfigurer(tlsConfigurer)
        .build();
```

#### Mutual TLS Authentication

To authenticate with a client certificate while using a custom truststore to verify the server certificate, use `withMutualTls()`:

```java
TLSConfigurer tlsConfigurer =
    TLSConfigurers.withMutualTls(
        Path.of("/path/to/truststore.p12"),
        "truststore-password",
        Path.of("/path/to/keystore.p12"),
        "keystore-password");
```

#### Mutual TLS with Default JVM Truststore
   
If the server certificate is already trusted by the default JVM truststore, only a client keystore is required:

```java
TLSConfigurer tlsConfigurer =
    TLSConfigurers.withKeyStore(
        Path.of("/path/to/keystore.p12"),
        "keystore-password");
```

The default JVM truststore is used to verify the server certificate, while the client keystore provides the certificate used for client authentication.


#### TLS Protocol and Cipher Suite Configuration
   
Use the builder to restrict the TLS protocols and cipher suites that the client may negotiate:

```java
TLSConfigurer tlsConfigurer =
    TLSConfigurers.builder()
        .protocols("TLSv1.3")
        .cipherSuites("TLS_AES_128_GCM_SHA256")
        .build();
```

These settings can also be combined with custom truststores and client keystores:

```java
TLSConfigurer tlsConfigurer =
    TLSConfigurers.builder()
        .trustStore(
            Path.of("/path/to/truststore.p12"),
            "truststore-password")
        .keyStore(
            Path.of("/path/to/keystore.p12"),
            "keystore-password")
        .protocols("TLSv1.3")
        .cipherSuites("TLS_AES_128_GCM_SHA256")
        .build();
```

If no protocol or cipher-suite restrictions are specified, the TLS implementation's defaults are used.

By default, the TLS helper uses the `PKCS12` store type for both truststores and keystores. Use `storeType()` to configure a different store type.

All examples can be used with `GravitinoClient` or `GravitinoAdminClient` through `withTlsConfigurer()`.

For instructions on configuring the Gravitino HTTPS server and creating a local truststore, see [HTTPS](./security/how-to-use-https.md).

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
