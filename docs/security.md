---
title: "How to customize Gravitino server configurations"
date: 2023-11-20T09:03:20-08:00
license: "Copyright 2023 Datastrato.
This software is licensed under the Apache License version 2."
---

## Authentication
Gravitino supports two kinds of authentication mechanism: simple and oauth.

### Simple mode
Simple mode is the default authentication option.
Simple mode allows the client to use the environment variable `GRAVITINO_USER` as the user.
If the environment variable `GRAVITINO_USER` is not set, client will use the user of the machine which sends requests.

### OAuth mode
Gravitino only supports external OAuth2.0 server now.
First, users need to guarantee that the external OAuth 2.0 server supports Bearer JWT and is configured properly.
Then, for server side, users should set `gravitino.authenticator` as `oauth` and give `gravitino.authenticator.oauth.defaultSignKey` a proper value.

### Server configuration

| Configuration item                                | Description                                                                | Default value     | Since version |
|---------------------------------------------------|----------------------------------------------------------------------------|-------------------|---------------|
| `gravitino.authenticator`                         | The authenticator which Gravitino uses, setting as `simple` or `oauth`     | `simple`          | 0.3.0         |
| `gravitino.authenticator.oauth.serviceAudience`   | The audience name when Gravitino uses oauth as the authenticator           | `GravitinoServer` | 0.3.0         |
| `gravitino.authenticator.oauth.allowSkewSecs`     | The jwt allows skew seconds when Gravitino uses oauth as the authenticator | `0`               | 0.3.0         |
| `gravitino.authenticator.oauth.defaultSignKey`    | The sign key of jwt when Gravitino uses oauth as the authenticator         | ``                | 0.3.0         |
| `gravitino.authenticator.oauth.signAlgorithmType` | The signature algorithm when Gravitino uses oauth as the authenticator     | `RS256`           | 0.3.0         |

The signature algorithm which Gravitino supports is as below:

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
Because HTTPS will protect the header of request from smuggling and HTTPS will be safer.
If users choose to enable HTTPS, Gravitino won't provide the ability of HTTP service.
Both Gravitino server and Iceberg REST service can configure HTTPS.

### Gravitino server's configuration
| Configuration item                                  | Description                                                | Default value | Since version |
|-----------------------------------------------------|------------------------------------------------------------|---------------|---------------|
| `gravitino.server.webserver.enableHttps`            | Enables https                                              | `false`       | 0.3.0         |
| `gravitino.server.webserver.httpsPort`              | The https port number of the Jetty web server              | `8433`        | 0.3.0         |
| `gravitino.server.webserver.keyStorePath`           | Path to the key store file                                 | ``            | 0.3.0         |
| `gravitino.server.webserver.keyStorePassword`       | Password to the key store                                  | ``            | 0.3.0         |
| `gravitino.server.webserver.keyStoreType`           | The type to the key store                                  | `JKS`         | 0.3.0         |
| `gravitino.server.webserver.managerPassword`        | Manager password to the key store                          | ``            | 0.3.0         |
| `gravitino.server.webserver.tlsProtocol`            | TLS protocol to use. The protocol must be supported by JVM | none          | 0.3.0         |
| `gravitino.server.webserver.enableCipherAlgorithms` | he collection of the cipher algorithms which are enabled   | ``            | 0.3.0         |
| `gravitino.server.webserver.enableClientAuth`       | Enables the authentication of the client                   | `false`       | 0.3.0         |
| `gravitino.server.webserver.trustStorePath`         | Path to the trust store file                               | ``            | 0.3.0         |
| `gravitino.server.webserver.trustStorePassword`     | Password to the trust store                                | ``            | 0.3.0         |
| `gravitino.server.webserver.trustStoreType`         | The type to the trust store                                | `JKS`         | 0.3.0         |

### Iceberg REST service's configuration
| Configuration item                                         | Description                                                | Default value | Since version |
|------------------------------------------------------------|------------------------------------------------------------|---------------|---------------|
| `gravitino.auxService.iceberg-rest.enableHttps`            | Enables https                                              | `false`       | 0.3.0         |
| `gravitino.auxService.iceberg-rest.httpsPort`              | The https port number of the Jetty web server              | `8433`        | 0.3.0         |
| `gravitino.auxService.iceberg-rest.keyStorePath`           | Path to the key store file                                 | ``            | 0.3.0         |
| `gravitino.auxService.iceberg-rest.keyStorePassword`       | Password to the key store                                  | ``            | 0.3.0         |
| `gravitino.uxService.iceberg-rest.keyStoreType`            | The type to the key store                                  | `JKS`         | 0.3.0         |
| `gravitino.auxService.iceberg-rest.managerPassword`        | Manager password to the key store                          | ``            | 0.3.0         |
| `gravitino.auxService.iceberg-rest.tlsProtocol`            | TLS protocol to use. The protocol must be supported by JVM | none          | 0.3.0         |
| `gravitino.auxService.iceberg-rest.enableCipherAlgorithms` | he collection of the cipher algorithms which are enabled   | ``            | 0.3.0         |
| `gravitino.auxService.iceberg-rest.enableClientAuth`       | Enables the authentication of the client                   | `false`       | 0.3.0         |
| `gravitino.auxService.iceberg-rest.trustStorePath`         | Path to the trust store file                               | ``            | 0.3.0         |
| `gravitino.auxService.iceberg-rest.trustStorePassword`     | Password to the trust store                                | ``            | 0.3.0         |
| `gravitino.auxService.iceberg-rest.trustStoreType`         | The type to the trust store                                | `JKS`         | 0.3.0         |
