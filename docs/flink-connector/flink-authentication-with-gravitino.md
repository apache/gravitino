---
title: "Flink Authentication"
slug: "/flink-connector/flink-authentication"
keyword: "flink connector authentication basic oauth2 kerberos"
license: "This software is licensed under the Apache License version 2."
---

## Overview

Flink connector supports `simple`, `basic`, `oauth2`, and `kerberos` authentication when accessing the Gravitino server.

| Property                                                   | Type     | Default Value   | Description                                                                                                                                | Required   | Since Version   |
|------------------------------------------------------------|----------|-----------------|--------------------------------------------------------------------------------------------------------------------------------------------|------------|-----------------|
| table.catalog-store.gravitino.gravitino.client.auth.type   | string   | (none)          | When explicitly set, only `oauth2` and `basic` are supported.                                                                              | No         | 1.2.0           |

## Simple Mode

In simple mode, the username originates from Flink. The resolution order is:
1. `HADOOP_USER_NAME` environment variable
2. The logged-in OS user

## Basic Mode

In Basic mode, the Flink connector authenticates to the Gravitino server using HTTP Basic credentials
against the built-in IDP. The Gravitino server must have Basic authentication enabled. See
[How to authenticate](../security/how-to-authenticate.md#basic-mode) for server-side setup.

| Property                                                                | Type     | Default Value   | Description                                        | Required                         | Since Version   |
|-------------------------------------------------------------------------|----------|-----------------|----------------------------------------------------|----------------------------------|-----------------|
| table.catalog-store.gravitino.gravitino.client.auth.type                | string   | (none)          | Set to `basic` to enable Basic authentication.     | Yes, for Basic mode              | 1.3.0           |
| table.catalog-store.gravitino.gravitino.client.basic.username           | string   | (none)          | The built-in IDP username.                         | Yes, for Basic mode              | 1.3.0           |
| table.catalog-store.gravitino.gravitino.client.basic.password           | string   | (none)          | The built-in IDP password.                         | Yes, for Basic mode              | 1.3.0           |

### Basic Configuration Example

```yaml
table.catalog-store.kind: gravitino
table.catalog-store.gravitino.gravitino.uri: http://localhost:8090
table.catalog-store.gravitino.gravitino.metalake: my_metalake
table.catalog-store.gravitino.gravitino.client.auth.type: basic
table.catalog-store.gravitino.gravitino.client.basic.username: admin
table.catalog-store.gravitino.gravitino.client.basic.password: YourSecureGravitinoPassword
```

## OAuth2 Mode

In OAuth2 mode, configure the following settings to fetch an OAuth2 token to access the Gravitino server:

| Property                                                                | Type     | Default Value   | Description                                        | Required                         | Since Version   |
|-------------------------------------------------------------------------|----------|-----------------|----------------------------------------------------|----------------------------------|-----------------|
| table.catalog-store.gravitino.gravitino.client.oauth2.serverUri         | string   | (none)          | The OAuth2 server URI.                             | Yes, for OAuth2 mode             | 1.2.0           |
| table.catalog-store.gravitino.gravitino.client.oauth2.tokenPath         | string   | (none)          | The token endpoint path on the OAuth2 server.      | Yes, for OAuth2 mode             | 1.2.0           |
| table.catalog-store.gravitino.gravitino.client.oauth2.credential        | string   | (none)          | The credential used to request the OAuth2 token.   | Yes, for OAuth2 mode             | 1.2.0           |
| table.catalog-store.gravitino.gravitino.client.oauth2.scope             | string   | (none)          | The scope used to request the OAuth2 token.        | Yes, for OAuth2 mode             | 1.2.0           |

### OAuth2 Configuration Example

```yaml
table.catalog-store.kind: gravitino
table.catalog-store.gravitino.gravitino.uri: http://localhost:8090
table.catalog-store.gravitino.gravitino.metalake: my_metalake
table.catalog-store.gravitino.gravitino.client.auth.type: oauth2
table.catalog-store.gravitino.gravitino.client.oauth2.serverUri: https://oauth-server.example.com
table.catalog-store.gravitino.gravitino.client.oauth2.tokenPath: /oauth/token
table.catalog-store.gravitino.gravitino.client.oauth2.credential: client-id:client-secret
table.catalog-store.gravitino.gravitino.client.oauth2.scope: your-scope
```

## Kerberos Mode

In Kerberos mode, use Flink security configurations to obtain a Kerberos ticket for accessing the Gravitino server. Configure `security.kerberos.login.principal` and `security.kerberos.login.keytab` for the Kerberos principal and keytab.

The Gravitino server principal follows the pattern `HTTP/$host@$realm`; ensure `$host` matches the host specified in the Gravitino server URI. Ensure `krb5.conf` is available to Flink, for example via `-Djava.security.krb5.conf=/path/to/krb5.conf` in Flink JVM options.
