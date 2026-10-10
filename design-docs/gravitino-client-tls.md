<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Gravitino Java Client TLS Configuration

## Summary

This document defines the public API for configuring TLS connections made by the Gravitino Java client.

The design supports the following client use cases:

* Connecting to a server whose certificate is signed by a private certificate authority.
* Connecting to a server that requires mutual TLS.
* Presenting a client certificate while continuing to use the JVM's default trust configuration.
* Restricting the enabled TLS protocols and cipher suites.

The TLS configuration is supplied through a narrow, typed client-builder method:

```java
withTlsConfigurer(TLSConfigurer tlsConfigurer)
```

The underlying HTTP client builder is not exposed.

## What does a user write?

### Private-CA server

A user connects to a Gravitino server whose certificate is signed by a private CA by supplying a custom truststore.

```java
TLSConfigurer tlsConfigurer =
    TLSConfigurers.builder()
        .trustStore(
            Paths.get("/etc/gravitino/client-truststore.p12"),
            "truststore-password")
        .build();

GravitinoClient client =
    GravitinoClient.builder("https://gravitino.example.com")
        .withTlsConfigurer(tlsConfigurer)
        .build();
```

The truststore is used instead of the JVM's default trust configuration for this client.

### Mutual TLS

A user supplies both a truststore for validating the server and a keystore containing the client's certificate and private key.

```java
TLSConfigurer tlsConfigurer =
    TLSConfigurers.builder()
        .trustStore(
            Paths.get("/etc/gravitino/client-truststore.p12"),
            "truststore-password")
        .keyStore(
            Paths.get("/etc/gravitino/client-keystore.p12"),
            "keystore-password")
        .build();

GravitinoClient client =
    GravitinoClient.builder("https://gravitino.example.com")
        .withTlsConfigurer(tlsConfigurer)
        .build();
```

### Client certificate with the system truststore

A user supplies a client certificate without supplying a custom truststore.

```java
TLSConfigurer tlsConfigurer =
    TLSConfigurers.builder()
        .keyStore(
            Paths.get("/etc/gravitino/client-keystore.p12"),
            "keystore-password")
        .build();

GravitinoClient client =
    GravitinoClient.builder("https://gravitino.example.com")
        .withTlsConfigurer(tlsConfigurer)
        .build();
```

Because no custom truststore is configured, the client uses the JVM's default trust configuration to validate the server certificate.

### Restricting protocols and cipher suites

A user may restrict the protocols and cipher suites enabled for the connection.

```java
TLSConfigurer tlsConfigurer =
    TLSConfigurers.builder()
        .protocols("TLSv1.3")
        .cipherSuites("TLS_AES_256_GCM_SHA384")
        .build();

GravitinoClient client =
    GravitinoClient.builder("https://gravitino.example.com")
        .withTlsConfigurer(tlsConfigurer)
        .build();
```

When protocols or cipher suites are not specified, the HTTP and TLS implementation defaults are used.

Unsupported protocols or cipher suites cause client construction or connection establishment to fail rather than silently falling back to weaker settings.

## What are we promising?

### Public API

The following types and methods are part of the public client API.

```java
public interface TLSConfigurer
```

`TLSConfigurer` represents a complete optional TLS configuration that can be applied to the Gravitino HTTP client.

```java
public final class TLSConfigurers
```

`TLSConfigurers` is the entry point for constructing a `TLSConfigurer`.

```java
public static TLSConfigurers.Builder builder()
```

Creates a new TLS configuration builder.

```java
public static final class TLSConfigurers.Builder
```

The builder exposes the following methods:

```java
public Builder trustStore(Path path, String password)
```

Configures the truststore used to validate the server certificate.

```java
public Builder keyStore(Path path, String password)
```

Configures the keystore containing the client certificate and private key.

```java
public Builder protocols(String... protocols)
```

Restricts the enabled TLS protocols.

```java
public Builder cipherSuites(String... cipherSuites)
```

Restricts the enabled TLS cipher suites.

```java
public TLSConfigurer build()
```

Creates the TLS configuration.

The Gravitino client builders expose:

```java
public GravitinoClient.Builder withTlsConfigurer(
    TLSConfigurer tlsConfigurer)
```

```java
public GravitinoAdminClient.Builder withTlsConfigurer(
    TLSConfigurer tlsConfigurer)
```

These methods apply the supplied TLS configuration to HTTP connections created by the corresponding client.

The exact return types above should match the existing nested builder types used by `GravitinoClient` and `GravitinoAdminClient`.

### Default behavior

The following defaults are part of the API behavior:

* When no `TLSConfigurer` is supplied, existing client behavior remains unchanged.
* When no truststore is supplied, the JVM's default trust configuration is used.
* When no keystore is supplied, the client does not present a client certificate.
* When protocols are not supplied, the TLS implementation's enabled protocol defaults are used.
* When cipher suites are not supplied, the TLS implementation's enabled cipher-suite defaults are used.
* TLS configuration applies only to HTTPS connections.
* Configuring TLS does not implicitly disable hostname verification.

### Error handling

Invalid TLS configuration must fail with an actionable exception.

Examples include:

* The configured store does not exist or cannot be read.
* The store password is incorrect.
* The store cannot be parsed as the supported store type.
* The keystore contains no usable client key.
* A configured protocol or cipher suite is unsupported.
* The server certificate cannot be validated.
* The server rejects the client's certificate.

Sensitive values, including store passwords and private-key material, must not be included in exception messages or logs.

### Builder-method decision

The public client API will expose the narrow, typed method:

```java
withTlsConfigurer(TLSConfigurer tlsConfigurer)
```

It will not expose a general callback or hook for modifying the underlying HTTP client builder.

A typed TLS configuration API is preferred because:

* It limits the public contract to the capabilities required by the TLS epic.
* It prevents the underlying HTTP client implementation from becoming part of Gravitino's public API.
* It allows the HTTP implementation to be replaced without breaking client code.
* It gives Gravitino control over validation, defaults, compatibility, and error reporting.
* It is easier to widen the typed API in a later release than to narrow a general-purpose HTTP-builder hook after users depend on it.

This decision intentionally favors a narrow API. Additional TLS configuration capabilities can be added to `TLSConfigurer` later when concrete use cases require them.

## What stays internal?

The following implementation details are not public API:

* Loading `KeyStore` instances from configured paths.
* Creating and initializing `KeyManagerFactory`.
* Creating and initializing `TrustManagerFactory`.
* Creating the Java `SSLContext`.
* Creating the Apache HttpComponents TLS strategy.
* Applying protocols and cipher suites to the underlying HTTP transport.
* Translating `TLSConfigurer` into HTTP client configuration.
* The concrete implementation class of `TLSConfigurer`.
* Any wrapper, adapter, or helper classes used by `HTTPClient`.
* The choice of Apache HttpComponents classes used internally.
* Test certificates, test keystores, and test truststores.

Users interact only with `TLSConfigurer`, `TLSConfigurers.Builder`, and the typed `withTlsConfigurer` client-builder method.

## What are we not doing?

This design does not include:

* Server-side TLS configuration.
* Certificate enrollment, renewal, rotation, revocation, or certificate pinning.
* Automatically detecting or reloading keystore or truststore changes after the client has been built. Users must rebuild the client to apply updated stores.
* Configuring TLS globally for the JVM.
* Providing an option to disable or bypass hostname verification.
* Trusting all certificates or providing an insecure trust manager.
* Exposing the underlying Apache HttpComponents client builder.
* Providing a general-purpose HTTP-client customization callback.
* Directly accepting PEM-encoded CA certificates, client certificates, certificate chains, or private keys. These materials must be supplied through PKCS12 keystores or truststores.
* Allowing users to explicitly select which private-key entry or certificate alias is presented when a keystore contains multiple eligible entries. Selection is delegated to the standard Java key manager.
* Supplying keystore or truststore contents through an `InputStream`.
* Guaranteeing that every configured protocol or cipher suite is supported by every JVM or security provider.

Initial file-based keystore and truststore support uses PKCS12 stores. Supporting additional store formats may be considered separately without changing the client-builder integration point.
