---
title: "Trino Connector Authentication"
slug: "/trino-connector/authentication"
keyword: "gravitino connector trino authentication"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The Gravitino Trino connector supports authenticating to the Gravitino server using the same authentication mechanisms as the Gravitino Java client: Simple, Basic, OAuth2, and Kerberos. Authentication is configured through the Trino connector properties file using the `gravitino.client.*` prefix.

If `gravitino.client.authType` is not set, the connector operates in no-authentication mode and connects to the Gravitino server without any credentials.

## Authentication Types

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

| Property                      | Description                                                         | Default value   | Required                                 | Since version   |
|-------------------------------|---------------------------------------------------------------------|-----------------|------------------------------------------|-----------------|
| `gravitino.client.authType`   | Authentication type: `simple`, `basic`, `oauth2`, or `kerberos`     | (none)          | No                                       | 1.3.0           |
| `gravitino.user`              | Username for simple authentication                                  | (none)          | No (uses system user if not specified)   | 1.3.0           |

### Basic Authentication

Basic authentication uses HTTP Basic credentials against the Gravitino built-in IDP. The Gravitino
server must have Basic authentication enabled. See
[How to authenticate](../security/how-to-authenticate.md#basic-mode) for server-side setup.

**Configuration in `etc/catalog/gravitino.properties`:**

```properties
connector.name=gravitino
gravitino.metalake=metalake
gravitino.uri=http://localhost:8090

# Basic authentication with built-in IDP
gravitino.client.authType=basic
gravitino.client.basic.username=admin
gravitino.client.basic.password=YourSecureGravitinoPassword
```

**Configuration properties:**

| Property                            | Description                                                         | Default value   | Required                     | Since version   |
|-------------------------------------|---------------------------------------------------------------------|-----------------|------------------------------|-----------------|
| `gravitino.client.authType`         | Authentication type: `simple`, `basic`, `oauth2`, or `kerberos`     | (none)          | Yes (to enable Basic)        | 1.3.0           |
| `gravitino.client.basic.username`   | Built-in IDP username                                               | (none)          | Yes if authType is `basic`   | 1.3.0           |
| `gravitino.client.basic.password`   | Built-in IDP password                                               | (none)          | Yes if authType is `basic`   | 1.3.0           |

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

| Property                               | Description                                                         | Default value   | Required                     | Since version   |
|----------------------------------------|---------------------------------------------------------------------|-----------------|------------------------------|-----------------|
| `gravitino.client.authType`            | Authentication type: `simple`, `basic`, `oauth2`, or `kerberos`     | (none)          | Yes (to enable OAuth2)       | 1.3.0           |
| `gravitino.client.oauth2.serverUri`    | OAuth2 server URI                                                   | (none)          | Yes if authType is `oauth2`  | 1.3.0           |
| `gravitino.client.oauth2.credential`   | OAuth2 credentials in format `client_id:client_secret`              | (none)          | Yes if authType is `oauth2`  | 1.3.0           |
| `gravitino.client.oauth2.path`         | OAuth2 token endpoint path                                          | (none)          | Yes if authType is `oauth2`  | 1.3.0           |
| `gravitino.client.oauth2.scope`        | OAuth2 scope                                                        | (none)          | Yes if authType is `oauth2`  | 1.3.0           |

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

| Property                                     | Description                                                         | Default value   | Required                                  | Since version   |
|----------------------------------------------|---------------------------------------------------------------------|-----------------|-------------------------------------------|-----------------|
| `gravitino.client.authType`                  | Authentication type: `simple`, `basic`, `oauth2`, or `kerberos`     | (none)          | Yes (to enable Kerberos)                  | 1.3.0           |
| `gravitino.client.kerberos.principal`        | Kerberos principal                                                  | (none)          | Yes if authType is `kerberos`             | 1.3.0           |
| `gravitino.client.kerberos.keytabFilePath`   | Path to keytab file                                                 | (none)          | No (uses ticket cache if not specified)   | 1.3.0           |

## Session Credential Forwarding

Setting `gravitino.client.session.forwardUser=true` creates a dedicated Gravitino client per Trino session user, so each user is visible in the Gravitino audit log instead of the shared `gravitino.user` or service identity. It is supported with `authType=simple` and `authType=oauth2`.

**Configuration (`authType=simple`):**

```properties
connector.name=gravitino
gravitino.metalake=metalake
gravitino.uri=http://localhost:8090

gravitino.client.authType=simple
gravitino.client.session.forwardUser=true
```

With `authType=simple`, the Trino session username is forwarded to Gravitino as the simple-auth identity.

**Configuration (`authType=oauth2`):**

```properties
connector.name=gravitino
gravitino.metalake=metalake
gravitino.uri=http://localhost:8090

gravitino.client.authType=oauth2
gravitino.client.oauth2.serverUri=http://oauth-server:8080
gravitino.client.oauth2.credential=client_id:client_secret
gravitino.client.oauth2.path=oauth2/token
gravitino.client.oauth2.scope=gravitino
gravitino.client.session.forwardUser=true
```

With `authType=oauth2`, the end user's IdP access token is presented to Gravitino directly instead of the shared client-credentials identity. This requires the Trino coordinator to populate the session's extra-credentials with the caller's access token under the key `token`; the connector reads it from there, and `buildForSession` fails with a clear error if it's missing.

Whether the coordinator can populate this extra-credential depends on the Trino distribution:

- **Starburst Enterprise** supports this via `http-server.authentication.type=DELEGATED-OAUTH2` — see [OAuth 2.0 token pass-through](https://docs.starburst.io/latest/security/oauth2-passthrough.html).
- **Open-source Trino does not support this yet.** There is no equivalent coordinator-side mechanism to forward the caller's OAuth2 token into the connector session; see [trinodb/trino discussion #24403](https://github.com/trinodb/trino/discussions/24403) and [issue #27917](https://github.com/trinodb/trino/issues/27917) tracking this feature request upstream.

The `gravitino.client.oauth2.*` properties above still configure the shared bootstrap/admin client used for catalog discovery — they are unrelated to the per-user forwarded token.

For an Iceberg catalog with `catalog-backend=rest` (backed by an Iceberg REST Catalog), the connector does not set `iceberg.rest-catalog.security`/`iceberg.rest-catalog.session` on its own — that catalog's own `gravitino.client.*` config is unrelated to how its underlying Iceberg REST catalog authenticates. To also forward the end user's token to the REST catalog itself, set `trino.bypass.iceberg.rest-catalog.security=OAUTH2` and `trino.bypass.iceberg.rest-catalog.session=USER` explicitly on that catalog's properties, alongside its bootstrap `trino.bypass.iceberg.rest-catalog.oauth2.*` credentials; see the worked example below.

**Configuration properties:**

| Property                                                     | Description                                                                                    | Default value   | Required   | Since version   |
|--------------------------------------------------------------|--------------------------------------------------------------------------------------------------|-----------------|------------|-----------------|
| `gravitino.client.session.forwardUser`                       | When `true` with `authType=simple` or `authType=oauth2`, forwards the Trino session user/token to Gravitino per-query   | `false`         | No         | 1.3.0           |
| `gravitino.client.session.cache.maxSize`                     | Maximum number of per-user sessions to keep in the cache                                       | `500`           | No         | 1.3.0           |
| `gravitino.client.session.cache.expireAfterAccessSeconds`    | Seconds before an idle per-user session is evicted from the cache                              | `3600`          | No         | 1.3.0           |

### Example: OAuth2 Per-User Token Forwarding

This example walks through a full setup where each Trino user's own OAuth2 access token is
forwarded to Gravitino and to an Iceberg REST catalog (IRC), instead of a single shared service
identity.

**1. Trino coordinator: forward the logged-in user's token to connectors.** This is the
prerequisite that makes `authType=oauth2` forwarding possible at all — the coordinator must
populate the session's extra-credentials with the caller's access token under the key `token`.

- **Starburst Enterprise**: set the following in `etc/config.properties`:

  ```properties
  http-server.authentication.type=DELEGATED-OAUTH2
  ```

  See [OAuth 2.0 token pass-through](https://docs.starburst.io/latest/security/oauth2-passthrough.html)
  for details, including its limitation that pass-through tokens are not refreshed and must
  outlive the query.

- **Open-source Trino**: there is currently no equivalent coordinator setting. Track
  [trinodb/trino discussion #24403](https://github.com/trinodb/trino/discussions/24403) and
  [issue #27917](https://github.com/trinodb/trino/issues/27917) for this feature request. Until
  it lands upstream, this connector's `authType=oauth2` forwardUser path requires a Trino
  distribution that provides this extra-credential itself.

**2. Gravitino server: enable OAuth2** (in `conf/gravitino.conf`):

```properties
gravitino.authenticators=oauth
gravitino.authenticator.oauth.serviceAudience=account
gravitino.authenticator.oauth.jwksUri=http://your-idp/realms/gravitino/protocol/openid-connect/certs
gravitino.authenticator.oauth.tokenValidatorClass=org.apache.gravitino.server.authentication.JwksTokenValidator
gravitino.authenticator.oauth.principalFields=preferred_username,email,sub
```

**3. Trino connector: enable OAuth2 forwarding** (in `etc/catalog/gravitino.properties`):

```properties
connector.name=gravitino
gravitino.metalake=my_metalake
gravitino.uri=http://localhost:8090

gravitino.client.authType=oauth2
gravitino.client.oauth2.serverUri=http://your-idp
gravitino.client.oauth2.credential=service-account-id:service-account-secret
gravitino.client.oauth2.path=realms/gravitino/protocol/openid-connect/token
gravitino.client.oauth2.scope=email
gravitino.client.session.forwardUser=true
```

The `gravitino.client.oauth2.*` properties configure the shared service identity used for catalog
discovery; the per-user forwarded token (from step 1) is what each query actually authenticates
with once `forwardUser=true`.

**4. Create the metalake and catalog.** Create the metalake `my_metalake` first (via the
Gravitino REST API, SDK, or CLI — see
[Manage metalakes](../manage-metalake-using-gravitino.md#create-a-metalake)), then create a
REST-backed Iceberg catalog under it from the Trino CLI using the
`gravitino.system.create_catalog` procedure. To also forward the end user's token to the Iceberg
REST catalog (IRC) itself, set `trino.bypass.iceberg.rest-catalog.security=OAUTH2` and
`trino.bypass.iceberg.rest-catalog.session=USER` on the catalog, alongside its bootstrap
`trino.bypass.iceberg.rest-catalog.oauth2.*` credentials:

```sql
call gravitino.system.create_catalog(
    'my_catalog',
    'lakehouse-iceberg',
    map(
        array['uri', 'catalog-backend', 'warehouse',
          'trino.bypass.iceberg.rest-catalog.security', 'trino.bypass.iceberg.rest-catalog.session',
          'trino.bypass.iceberg.rest-catalog.oauth2.credential', 'trino.bypass.iceberg.rest-catalog.oauth2.scope',
          'trino.bypass.iceberg.rest-catalog.oauth2.server-uri'
        ],
        array['http://irc-host:9001/iceberg', 'rest', 'my_catalog',
          'OAUTH2', 'USER',
          'service-account-id:service-account-secret', 'email',
          'http://your-idp/realms/gravitino/protocol/openid-connect/token'
        ]
    )
);
```

This call itself runs with the connector's own shared service identity, not any forwarded user
token — `forwardUser` only affects `SELECT`/`SHOW`-style queries against the catalog afterward,
not catalog registration itself. `create_catalog` both creates the catalog in Gravitino and loads
it into Trino as its own top-level catalog — not as a schema nested under a single `gravitino`
catalog. If the two `trino.bypass.iceberg.rest-catalog.*` properties above are omitted, the REST
catalog keeps its own default security setting, independent of
`gravitino.client.session.forwardUser`, and the end user's token never reaches the IRC.

**5. Query as a specific user.** With a real OIDC login flow, Trino populates the forwarded token
automatically after the user signs in. For manual testing, the same extra-credential can be set
directly on the CLI:

```shell
trino --server http://localhost:8080 \
  --user alice \
  --extra-credential token=<alice-idp-access-token> \
  --execute "SHOW SCHEMAS IN my_catalog"
```

Gravitino sees this request as `alice`, not the shared service identity — `alice`'s own
privileges apply, and a request with a missing or invalid token is rejected before it reaches the
catalog. Because `my_catalog` was created with `trino.bypass.iceberg.rest-catalog.session=USER`
in step 4, the same forwarded token also reaches the IRC directly, so per-user authorization
applies consistently whether Trino talks to Gravitino's native API or straight to the IRC.

## Notes

- The Gravitino server must be configured with the corresponding authentication mechanism enabled.
- For OAuth2 authentication, ensure the OAuth2 server is accessible from the Trino coordinator and workers.
- For Kerberos authentication, ensure the Kerberos configuration is properly set up on all Trino nodes.
- Authentication configuration is passed through the `gravitino.client.*` prefix to the underlying Gravitino Java client.

## See Also

- [Gravitino Server Authentication Configuration](../security/how-to-authenticate.md)
- [How to use the built-in IDP](../security/how-to-use-built-in-idp.md)
- [Trino Connector Configuration](./configuration.md)
