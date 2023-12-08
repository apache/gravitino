---
title: "How to customize Gravitino server configurations"
date: 2023-11-20T09:03:20-08:00
license: "Copyright 2023 Datastrato.
This software is licensed under the Apache License version 2."
---

## Authentication

Gravitino supports two kinds of authentication mechanisms: simple and OAuth.

### Simple mode

Simple mode is the default authentication option.
Simple mode allows the client to use the environment variable `GRAVITINO_USER` as the user.
If the environment variable `GRAVITINO_USER` isn't set, the client uses the user of the machine that sends requests.
For the client side, users can enable `simple` mode by the following code:

```java
GravitinoClient client = GravitinoClient.builder(uri)
    .withSimpleAuth()
    .build();
```

### OAuth mode

Gravitino only supports external OAuth 2.0 servers.
First, users need to guarantee that the external correctly configured OAuth 2.0 server supports Bearer JWT.
Then, on the server side, users should set `gravitino.authenticator` as `oauth` and give 
`gravitino.authenticator.oauth.defaultSignKey`, `gravitino.authenticator.oauth.serverUri` and 
`gravitino.authenticator.oauth.tokenPath`  a proper value.
Next, for the client side, users can enable `OAuth` mode by the following code:

```java
DefaultOAuth2TokenProvider authDataProvider = DefaultOAuth2TokenProvider.builder()
    .withUri("oauth server uri")
    .withCredential("yy:xx")
    .withPath("oauth/token")
    .withScope("test")
    .build();

GravitinoClient client = GravitinoClient.builder(uri)
    .withOAuth(authDataProvider)
    .build();
```

### Server configuration

| Configuration item                                | Description                                                                 | Default value     | Since version |
|---------------------------------------------------|-----------------------------------------------------------------------------|-------------------|---------------|
| `gravitino.authenticator`                         | The authenticator which Gravitino uses, setting as `simple` or `oauth`.     | `simple`          | 0.3.0         |
| `gravitino.authenticator.oauth.serviceAudience`   | The audience name when Gravitino uses OAuth as the authenticator.           | `GravitinoServer` | 0.3.0         |
| `gravitino.authenticator.oauth.allowSkewSecs`     | The JWT allows skew seconds when Gravitino uses OAuth as the authenticator. | `0`               | 0.3.0         |
| `gravitino.authenticator.oauth.defaultSignKey`    | The signing key of JWT when Gravitino uses OAuth as the authenticator.      | ``                | 0.3.0         |
| `gravitino.authenticator.oauth.signAlgorithmType` | The signature algorithm when Gravitino uses OAuth as the authenticator.     | `RS256`           | 0.3.0         |
| `gravitino.authenticator.oauth.serverUri`         | The URI of the default OAuth server.                                         | ``                | 0.3.0         |
| `gravitino.authenticator.oauth.tokenPath`         | The path for token of the default OAuth server.                              | ``                | 0.3.0         |

The signature algorithms that Gravitino supports follows:

| Name  | Description                                    |
|-------|------------------------------------------------|
| HS256 | HMAC using SHA-25A                             |
| HS384 | HMAC using SHA-384                             |
| HS512 | HMAC using SHA-51                              |
| RS256 | RSASSA-PKCS-v1_5 using SHA-256                 |
| RS384 | RSASSA-PKCS-v1_5 using SHA-384                 |
| RS512 | RSASSA-PKCS-v1_5 using SHA-512                 |
| ES256 | ECDSA using P-256 and SHA-256                  |
| ES384 | ECDSA using P-384 and SHA-384                  |
| ES512 | ECDSA using P-521 and SHA-512                  |
| PS256 | RSASSA-PSS using SHA-256 and MGF1 with SHA-256 |
| PS384 | RSASSA-PSS using SHA-384 and MGF1 with SHA-384 |
| PS512 | RSASSA-PSS using SHA-512 and MGF1 with SHA-512 |

## HTTPS configuration
Users would better use HTTPS instead of HTTP if users choose OAuth 2.0 as the authenticator.
HTTPS protects the header of the request from smuggling, making it safer.
If users choose to enable HTTPS, Gravitino won't provide the ability of HTTP service.
Both Gravitino server and Iceberg REST service can configure HTTPS.

### Gravitino server's configuration
| Configuration item                                  | Description                                                | Default value | Since version |
|-----------------------------------------------------|------------------------------------------------------------|---------------|---------------|
| `gravitino.server.webserver.enableHttps`            | Enables HTTPS.                                             | `false`       | 0.3.0         |
| `gravitino.server.webserver.httpsPort`              | The HTTPS port number of the Jetty web server.             | `8433`        | 0.3.0         |
| `gravitino.server.webserver.keyStorePath`           | Path to the key store file.                                | ``            | 0.3.0         |
| `gravitino.server.webserver.keyStorePassword`       | Password to the key store.                                 | ``            | 0.3.0         |
| `gravitino.server.webserver.keyStoreType`           | The type to the key store.                                 | `JKS`         | 0.3.0         |
| `gravitino.server.webserver.managerPassword`        | Manager password to the key store.                         | ``            | 0.3.0         |
| `gravitino.server.webserver.tlsProtocol`            | TLS protocol to use. The JVM must support the TLS protocol to use. | none          | 0.3.0         |
| `gravitino.server.webserver.enableCipherAlgorithms` | The collection of enabled cipher algorithms.               | ``            | 0.3.0         |
| `gravitino.server.webserver.enableClientAuth`       | Enables the authentication of the client.                  | `false`       | 0.3.0         |
| `gravitino.server.webserver.trustStorePath`         | Path to the trust store file.                              | ``            | 0.3.0         |
| `gravitino.server.webserver.trustStorePassword`     | Password to the trust store.                               | ``            | 0.3.0         |
| `gravitino.server.webserver.trustStoreType`         | The type to the trust store.                                | `JKS`         | 0.3.0         |

### Iceberg REST service's configuration
| Configuration item                                         | Description                                                | Default value | Since version |
|------------------------------------------------------------|------------------------------------------------------------|---------------|---------------|
| `gravitino.auxService.iceberg-rest.enableHttps`            | Enables HTTPS.                                             | `false`       | 0.3.0         |
| `gravitino.auxService.iceberg-rest.httpsPort`              | The HTTPS port number of the Jetty web server.             | `8433`        | 0.3.0         |
| `gravitino.auxService.iceberg-rest.keyStorePath`           | Path to the key store file.                                | ``            | 0.3.0         |
| `gravitino.auxService.iceberg-rest.keyStorePassword`       | Password to the key store.                                 | ``            | 0.3.0         |
| `gravitino.uxService.iceberg-rest.keyStoreType`            | The type to the key store.                                 | `JKS`         | 0.3.0         |
| `gravitino.auxService.iceberg-rest.managerPassword`        | Manager password to the key store.                         | ``            | 0.3.0         |
| `gravitino.auxService.iceberg-rest.tlsProtocol`            | TLS protocol to use. The JVM must support the TLS protocol to use.| none          | 0.3.0         |
| `gravitino.auxService.iceberg-rest.enableCipherAlgorithms` | The collection of enabled cipher algorithms.               | ``            | 0.3.0         |
| `gravitino.auxService.iceberg-rest.enableClientAuth`       | Enables the authentication of the client.                  | `false`       | 0.3.0         |
| `gravitino.auxService.iceberg-rest.trustStorePath`         | Path to the trust store file.                              | ``            | 0.3.0         |
| `gravitino.auxService.iceberg-rest.trustStorePassword`     | Password to the trust store.                               | ``            | 0.3.0         |
| `gravitino.auxService.iceberg-rest.trustStoreType`         | The type to the trust store.                               | `JKS`         | 0.3.0         |

Refer to the "Additional JSSE Standard Names" section of the [Java security guide](https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#jssenames) for the list of protocols related to tlsProtocol. You can find the list of `tlsProtocol` values for Java 8 in this document.

Refer to the "Additional JSSE Standard Names" section of the [Java security guide](https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites) for the list of protocols related to tlsProtocol. You can find the list of `enableCipherAlgorithms` values for Java 8 in this document.
