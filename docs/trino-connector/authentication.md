---
title: "Apache Gravitino Trino connector Authentication"
slug: /trino-connector/authentication
keyword: gravitino connector trino authentication
license: "This software is licensed under the Apache License version 2."
---

## Authentication

The Gravitino Trino connector supports authenticating to the Gravitino server using the same authentication mechanisms as the Gravitino Java client: Simple, OAuth, and Kerberos.

### Configuration

Authentication is configured through the Trino connector properties file using the `gravitino.client.*` prefix. These properties are passed directly to the underlying Gravitino Java client.

### Simple Authentication

Simple authentication uses a username to authenticate with the Gravitino server.

**Configuration in `etc/catalog/gravitino.properties`:**

```properties
connector.name=gravitino
gravitino.metalake=metalake
gravitino.uri=http://localhost:8090

# Simple authentication with username
gravitino.client.authType=simple
gravitino.client.simpleAuthUser=admin
```

**Configuration properties:**

| Property                          | Description                                           | Default value | Required                               | Since version |
|-----------------------------------|-------------------------------------------------------|---------------|----------------------------------------|---------------|
| `gravitino.client.authType`       | Authentication type: `simple`, `oauth`, or `kerberos` | (none)        | No                                     | 1.2.0         |
| `gravitino.client.simpleAuthUser` | Username for simple authentication                    | (none)        | No (uses system user if not specified) | 1.2.0         |

### OAuth Authentication

OAuth authentication uses OAuth 2.0 tokens to authenticate with the Gravitino server.

**Configuration in `etc/catalog/gravitino.properties`:**

```properties
connector.name=gravitino
gravitino.metalake=metalake
gravitino.uri=http://localhost:8090

# OAuth authentication
gravitino.client.authType=oauth
gravitino.client.oauth.serverUri=http://oauth-server:8080
gravitino.client.oauth.credential=client_id:client_secret
gravitino.client.oauth.path=/oauth2/token
gravitino.client.oauth.scope=gravitino
```

**Configuration properties:**

| Property                            | Description                                           | Default value | Required                   | Since version |
|-------------------------------------|-------------------------------------------------------|---------------|----------------------------|---------------|
| `gravitino.client.authType`         | Authentication type                                   | (none)        | Yes                        | 1.2.0         |
| `gravitino.client.oauth.serverUri`  | OAuth server URI                                      | (none)        | Yes if authType is `oauth` | 1.2.0         |
| `gravitino.client.oauth.credential` | OAuth credentials in format `client_id:client_secret` | (none)        | Yes if authType is `oauth` | 1.2.0         |
| `gravitino.client.oauth.path`       | OAuth token endpoint path                             | (none)        | Yes if authType is `oauth` | 1.2.0         |
| `gravitino.client.oauth.scope`      | OAuth scope                                           | (none)        | Yes if authType is `oauth` | 1.2.0         |

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

| Property                                   | Description         | Default value | Required                                | Since version |
|--------------------------------------------|---------------------|---------------|-----------------------------------------|---------------|
| `gravitino.client.authType`                | Authentication type | (none)        | Yes                                     | 1.2.0         |
| `gravitino.client.kerberos.principal`      | Kerberos principal  | (none)        | Yes if authType is `kerberos`           | 1.2.0         |
| `gravitino.client.kerberos.keytabFilePath` | Path to keytab file | (none)        | No (uses ticket cache if not specified) | 1.2.0         |

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

# OAuth authentication
gravitino.client.authType=oauth
gravitino.client.oauth.serverUri=http://localhost:8177
gravitino.client.oauth.credential=test:test
gravitino.client.oauth.path=/oauth2/token
gravitino.client.oauth.scope=test
```

**3. Verify the connection:**

```sql
SHOW CATALOGS;
```

### Notes

- The Gravitino server must be configured with the corresponding authentication mechanism enabled.
- For OAuth authentication, ensure the OAuth server is accessible from the Trino coordinator and workers.
- For Kerberos authentication, ensure the Kerberos configuration is properly set up on all Trino nodes.
- Authentication configuration is passed through the `gravitino.client.*` prefix to the underlying Gravitino Java client.

### See Also

- [Gravitino Server Authentication Configuration](../security/how-to-authenticate.md)
- [Trino Connector Configuration](./configuration.md)
