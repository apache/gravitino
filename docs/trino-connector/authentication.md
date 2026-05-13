---
title: "Apache Gravitino Trino connector Authentication"
slug: /trino-connector/authentication
keyword: gravitino connector trino authentication
license: "This software is licensed under the Apache License version 2."
---

## Authentication

The Gravitino Trino connector supports authenticating to the Gravitino server using the same authentication mechanisms as the Gravitino Java client: Simple, OAuth2, and Kerberos. Authentication is configured through the Trino connector properties file using the `gravitino.client.*` prefix.

If `gravitino.client.authType` is not set, the connector operates in no-authentication mode and connects to the Gravitino server without any credentials.

### Simple Authentication

Simple authentication uses a username to authenticate with the Gravitino server.

**Configuration in `etc/catalog/gravitino.properties`:**

```properties
connector.name=gravitino
gravitino.metalake=metalake
gravitino.uri=http://localhost:8090

# Simple authentication with username
gravitino.client.authType=simple
gravitino.user=admin
```

**Configuration properties:**

| Property                          | Description                                                          | Default value    | Required                               | Since version |
|-----------------------------------|----------------------------------------------------------------------|------------------|----------------------------------------|---------------|
| `gravitino.client.authType`       | Authentication type: `simple`, `oauth2`, or `kerberos`                 | (none)        | No                                     | 1.3.0         |
| `gravitino.user`                  | Username for simple authentication                                   | (none)           | No (uses system user if not specified) | 1.3.0         |

### OAuth2 Authentication

OAuth2 authentication uses OAuth 2.0 tokens to authenticate with the Gravitino server.

**Configuration in `etc/catalog/gravitino.properties`:**

```properties
connector.name=gravitino
gravitino.metalake=metalake
gravitino.uri=http://localhost:8090

# OAuth2 authentication
gravitino.client.authType=oauth2
gravitino.client.oauth2.serverUri=http://oauth-server:8080
gravitino.client.oauth2.credential=client_id:client_secret
gravitino.client.oauth2.path=oauth2/token
gravitino.client.oauth2.scope=gravitino
```

**Configuration properties:**

| Property                             | Description                                            | Default value | Required                    | Since version |
|--------------------------------------|--------------------------------------------------------|---------------|-----------------------------|---------------|
| `gravitino.client.authType`          | Authentication type: `simple`, `oauth2`, or `kerberos` | (none)        | Yes (to enable OAuth2)           | 1.3.0         |
| `gravitino.client.oauth2.serverUri`  | OAuth2 server URI                                      | (none)        | Yes if authType is `oauth2` | 1.3.0         |
| `gravitino.client.oauth2.credential` | OAuth2 credentials in format `client_id:client_secret` | (none)        | Yes if authType is `oauth2` | 1.3.0         |
| `gravitino.client.oauth2.path`       | OAuth2 token endpoint path                             | (none)        | Yes if authType is `oauth2` | 1.3.0         |
| `gravitino.client.oauth2.scope`      | OAuth2 scope                                           | (none)        | Yes if authType is `oauth2` | 1.3.0         |

### Kerberos Authentication

Kerberos authentication uses Kerberos tickets to authenticate with the Gravitino server.

**Configuration in `etc/catalog/gravitino.properties`:**

```properties
connector.name=gravitino
gravitino.metalake=metalake
gravitino.uri=http://localhost:8090

# Kerberos authentication with keytab
gravitino.client.authType=kerberos
gravitino.client.kerberos.principal=user@REALM
gravitino.client.kerberos.keytabFilePath=/path/to/user.keytab
```

**Configuration properties:**

| Property                                   | Description         | Default value | Required                                                   | Since version |
|--------------------------------------------|---------------------|---------------|------------------------------------------------------------|---------------|
| `gravitino.client.authType`                | Authentication type: `simple`, `oauth2`, or `kerberos` | (none)        | Yes (to enable Kerberos)                                   | 1.3.0         |
| `gravitino.client.kerberos.principal`      | Kerberos principal  | (none)        | Yes if authType is `kerberos`           | 1.3.0         |
| `gravitino.client.kerberos.keytabFilePath` | Path to keytab file | (none)        | No (uses ticket cache if not specified) | 1.3.0         |


### Example: Connecting to OAuth-protected Gravitino Server

This example shows how to configure the Trino connector to connect to a Gravitino server protected by OAuth authentication.

**1. Configure Gravitino server with OAuth** (in `conf/gravitino.conf`):

```properties
gravitino.authenticators=oauth
gravitino.authenticator.oauth.serviceAudience=gravitino
gravitino.authenticator.oauth.defaultSignKey=<your-signing-key>
gravitino.authenticator.oauth.tokenPath=/oauth2/token
gravitino.authenticator.oauth.serverUri=http://localhost:8177
```

**2. Configure Trino connector** (in `etc/catalog/gravitino.properties`):

```properties
connector.name=gravitino
gravitino.metalake=my_metalake
gravitino.uri=http://localhost:8090

# OAuth2 authentication
gravitino.client.authType=oauth2
gravitino.client.oauth2.serverUri=http://localhost:8177
gravitino.client.oauth2.credential=test:test
gravitino.client.oauth2.path=oauth2/token
gravitino.client.oauth2.scope=test
```

**3. Verify the connection:**

```sql
SHOW CATALOGS;
```

### Session Credential Forwarding

Setting `gravitino.client.session.forwardUser=true` with `authType=simple` creates a dedicated Gravitino client per Trino session user, so each user is visible in the Gravitino audit log instead of the shared `gravitino.user`.

**Configuration:**

```properties
connector.name=gravitino
gravitino.metalake=metalake
gravitino.uri=http://localhost:8090

gravitino.client.authType=simple
gravitino.client.session.forwardUser=true
```

**Configuration properties:**

| Property                                                   | Description                                                                                | Default value | Required | Since version |
|------------------------------------------------------------|--------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.client.session.forwardUser`                     | When `true` with `authType=simple`, forwards the Trino session user to Gravitino per-query | `false`       | No       | 1.3.0         |
| `gravitino.client.session.cache.maxSize`                   | Maximum number of per-user sessions to keep in the cache                                   | `500`         | No       | 1.3.0         |
| `gravitino.client.session.cache.expireAfterAccessSeconds`  | Seconds before an idle per-user session is evicted from the cache                          | `3600`        | No       | 1.3.0         |

### Notes

- The Gravitino server must be configured with the corresponding authentication mechanism enabled.
- For OAuth2 authentication, ensure the OAuth2 server is accessible from the Trino coordinator and workers.
- For Kerberos authentication, ensure the Kerberos configuration is properly set up on all Trino nodes.
- Authentication configuration is passed through the `gravitino.client.*` prefix to the underlying Gravitino Java client.

### See Also

- [Gravitino Server Authentication Configuration](../security/how-to-authenticate.md)
- [Trino Connector Configuration](./configuration.md)
