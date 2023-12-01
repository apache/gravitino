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
For client side, users can enable `simple` mode by the code as below:
```java
GravitinoClient client = GravitinoClient.builder(uri)
    .withSimpleAuth()
    .build();
```

### OAuth mode
Gravitino only supports external OAuth 2.0 server now.
First, users need to guarantee that the external OAuth 2.0 server supports Bearer JWT and is configured properly.
Then, for server side, users should set `gravitino.authenticator` as `oauth` and give `gravitino.authenticator.oauth.defaultSignKey` a proper value.
Next, for client side, users can enable `oauth` mode by the code as below:
```java
AuthDataProvider authDataProvider = DefaultOAuth2TokenProvider.builder()
    .uri("oauth server uri")
    .withCredential("yy:xx")
    .withPath("oauth/token")
    .withScope("test")
    .build();

GravitinoClient client = GravitinoClient.builder(uri)
    .withOAuth(authDataProvider)
    .build();
```


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
