---
title: "Flink authentication with Gravitino server"
slug: /flink-connector/flink-authentication
keyword: flink connector authentication oauth2 kerberos
license: "This software is licensed under the Apache License version 2."
---

## Overview

Flink connector supports `simple`, `oauth2`, and `kerberos` authentication when accessing the Gravitino server.

| Property                                                  | Type   | Default Value | Description                                                                                                                                | Required | Since Version |
|-----------------------------------------------------------|--------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------|
| table.catalog-store.gravitino.gravitino.client.auth.type  | string | (none)        | When explicitly set, only `oauth` is supported. If unset, Flink selects Kerberos or simple authentication based on its security settings.  | No       | 1.2.0         |

## Simple mode

In simple mode, the username originates from Flink. The resolution order is:
1. `HADOOP_USER_NAME` environment variable
2. The logged-in OS user

## OAuth2 mode

In OAuth2 mode, configure the following settings to fetch an OAuth2 token to access the Gravitino server:

| Property                                                              | Type   | Default Value | Description                                      | Required                       | Since Version |
|-----------------------------------------------------------------------|--------|---------------|--------------------------------------------------|--------------------------------|---------------|
| table.catalog-store.gravitino.gravitino.client.oauth2.serverUri       | string | (none)        | The OAuth2 server URI.                           | Yes, for OAuth2 mode           | 1.2.0         |
| table.catalog-store.gravitino.gravitino.client.oauth2.tokenPath       | string | (none)        | The token endpoint path on the OAuth2 server.    | Yes, for OAuth2 mode           | 1.2.0         |
| table.catalog-store.gravitino.gravitino.client.oauth2.credential      | string | (none)        | The credential used to request the OAuth2 token. | Yes, for OAuth2 mode           | 1.2.0         |
| table.catalog-store.gravitino.gravitino.client.oauth2.scope           | string | (none)        | The scope used to request the OAuth2 token.      | Yes, for OAuth2 mode           | 1.2.0         |

### OAuth2 Configuration Example

```yaml
table.catalog-store.kind: gravitino
table.catalog-store.gravitino.gravitino.uri: http://localhost:8090
table.catalog-store.gravitino.gravitino.metalake: my_metalake
table.catalog-store.gravitino.gravitino.client.auth.type: oauth2
table.catalog-store.gravitino.gravitino.client.oauth2.serverUri: https://oauth-server.example.com
table.catalog-store.gravitino.gravitino.client.oauth2.tokenPath: /oauth/token
table.catalog-store.gravitino.gravitino.client.oauth2.credential: your-client-credentials
table.catalog-store.gravitino.gravitino.client.oauth2.scope: your-scope
```

## Kerberos mode

In Kerberos mode, use Flink security configurations to obtain a Kerberos ticket for accessing the Gravitino server. Configure `security.kerberos.login.principal` and `security.kerberos.login.keytab` for the Kerberos principal and keytab.

The Gravitino server principal follows the pattern `HTTP/$host@$realm`; ensure `$host` matches the host specified in the Gravitino server URI. Ensure `krb5.conf` is available to Flink, for example via `-Djava.security.krb5.conf=/path/to/krb5.conf` in Flink JVM options.
