---
title: "Trino Connector Authentication"
slug: "/trino-connector/authentication"
keyword: "gravitino connector trino authentication"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The Gravitino Trino connector supports authenticating to the Gravitino server using the same authentication mechanisms as the Gravitino Java client: Simple, Basic, OAuth2, and Kerberos. Authentication is configured through the Trino connector properties file using the `gravitino.client.*` prefix.

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

| Property                    | Description                                                     | Default value | Required                               |
|-----------------------------|-----------------------------------------------------------------|---------------|----------------------------------------|
| `gravitino.client.authType` | Authentication type: `simple`, `basic`, `oauth2`, or `kerberos` | (none)        | No                                     |
| `gravitino.user`            | Username for simple authentication                              | (none)        | No (uses system user if not specified) |

### Basic Authentication

Basic authentication uses HTTP Basic credentials against the Gravitino local user store. The Gravitino
server must have Basic authentication enabled. See
[How to authenticate](../security/how-to-authenticate.md#basic-mode) for server-side setup.

**Configuration in `etc/catalog/gravitino.properties`:**

```properties
connector.name=gravitino
gravitino.metalake=metalake
gravitino.uri=http://localhost:8090

# Basic authentication with local user store
gravitino.client.authType=basic
gravitino.client.basic.username=admin
gravitino.client.basic.password=YourSecureGravitinoPassword
```

**Configuration properties:**

| Property                          | Description                                                     | Default value | Required                   |
|-----------------------------------|-----------------------------------------------------------------|---------------|----------------------------|
| `gravitino.client.authType`       | Authentication type: `simple`, `basic`, `oauth2`, or `kerberos` | (none)        | Yes (to enable Basic)      |
| `gravitino.client.basic.username` | Local user store username                                           | (none)        | Yes if authType is `basic` |
| `gravitino.client.basic.password` | Local user store password                                           | (none)        | Yes if authType is `basic` |

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

| Property                             | Description                                                     | Default value | Required                    |
|--------------------------------------|-----------------------------------------------------------------|---------------|-----------------------------|
| `gravitino.client.authType`          | Authentication type: `simple`, `basic`, `oauth2`, or `kerberos` | (none)        | Yes (to enable OAuth2)      |
| `gravitino.client.oauth2.serverUri`  | OAuth2 server URI                                               | (none)        | Yes if authType is `oauth2` |
| `gravitino.client.oauth2.credential` | OAuth2 credentials in format `client_id:client_secret`          | (none)        | Yes if authType is `oauth2` |
| `gravitino.client.oauth2.path`       | OAuth2 token endpoint path                                      | (none)        | Yes if authType is `oauth2` |
| `gravitino.client.oauth2.scope`      | OAuth2 scope                                                    | (none)        | Yes if authType is `oauth2` |

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

| Property                                   | Description                                                     | Default value | Required                                |
|--------------------------------------------|-----------------------------------------------------------------|---------------|-----------------------------------------|
| `gravitino.client.authType`                | Authentication type: `simple`, `basic`, `oauth2`, or `kerberos` | (none)        | Yes (to enable Kerberos)                |
| `gravitino.client.kerberos.principal`      | Kerberos principal                                              | (none)        | Yes if authType is `kerberos`           |
| `gravitino.client.kerberos.keytabFilePath` | Path to keytab file                                             | (none)        | No (uses ticket cache if not specified) |


### Example: Connecting to OAuth-Protected Gravitino Server

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

| Property                                                  | Description                                                                                | Default value | Required |
|-----------------------------------------------------------|--------------------------------------------------------------------------------------------|---------------|----------|
| `gravitino.client.session.forwardUser`                    | When `true` with `authType=simple`, forwards the Trino session user to Gravitino per-query | `false`       | No       |
| `gravitino.client.session.cache.maxSize`                  | Maximum number of per-user sessions to keep in the cache                                   | `500`         | No       |
| `gravitino.client.session.cache.expireAfterAccessSeconds` | Seconds before an idle per-user session is evicted from the cache                          | `3600`        | No       |

### Notes

- The Gravitino server must be configured with the corresponding authentication mechanism enabled.
- For OAuth2 authentication, ensure the OAuth2 server is accessible from the Trino coordinator and workers.
- For Kerberos authentication, ensure the Kerberos configuration is properly set up on all Trino nodes.
- Authentication configuration is passed through the `gravitino.client.*` prefix to the underlying Gravitino Java client.

### See Also

- [Gravitino Server Authentication Configuration](../security/how-to-authenticate.md)
- [Local users and groups](../security/local-users-and-groups.md)
- [Trino Connector Configuration](./configuration.md)
