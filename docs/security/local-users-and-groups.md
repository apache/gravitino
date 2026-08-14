---
title: "Local Users and Groups"
slug: "/security/local-users-and-groups"
keywords:
  - security
  - authentication
  - basic authentication
license: "This software is licensed under the Apache License version 2."
---

## Overview

Apache Gravitino can store login identities in its own relational metadata store through the `idp-basic` plugin. Usernames, password hashes, and group membership live alongside the rest of the server's metadata, and clients authenticate with HTTP Basic credentials. Nothing outside Gravitino is required.

The local user store authenticates callers to Gravitino. It does not issue tokens or assertions that other services can consume, so it is not a single sign-on system and not a replacement for Okta, Microsoft Entra ID, or Keycloak. Use it for proofs of concept, offline installations, and isolated deployments where a self-contained identity store is acceptable. For anything else, see [How to Authenticate](how-to-authenticate.md).

Credentials travel in an HTTP header on every request, so run the server behind [HTTPS](how-to-use-https.md) wherever the network is not fully trusted.

The management endpoints are served under `/api/idp/`, which reflects the plugin's original name rather than the feature's scope. For request and response schemas, see the [OpenAPI definition](../open-api/idp/openapi.yaml).

## Quick Start

**1. Configure the server.** Add the following to `gravitino.conf`. Both properties are required, since the `basic` authenticator refuses to start without the REST extension package registered.

```properties
gravitino.authenticators = basic
gravitino.server.rest.extensionPackages = org.apache.gravitino.idp.web.rest.feature
gravitino.authorization.serviceAdmins = admin
```

**2. Set the initial administrator password.** Every username in `gravitino.authorization.serviceAdmins` needs a stored password before it can call the management endpoints. Set this before the first start.

```shell
export GRAVITINO_INITIAL_ADMIN_PASSWORD='{admin_password}'
```

**3. Start Gravitino and confirm the administrator works.**

```shell
curl -s -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Authorization: Basic $(echo -n 'admin:{admin_password}' | base64)" \
  http://localhost:8090/api/version
```

**4. Create a user.**

```shell
curl -s -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'admin:{admin_password}' | base64)" \
  -d '{"user":"alice","password":"{user_password}"}' \
  http://localhost:8090/api/idp/users
```

## Configuration

| Configuration Item                        | Description                                                       | Example                                     |
|-------------------------------------------|-------------------------------------------------------------------|---------------------------------------------|
| `gravitino.authenticators`                | Must be `basic`, and must not include `simple`                    | `basic`                                     |
| `gravitino.server.rest.extensionPackages` | Registers the user and group management endpoints                 | `org.apache.gravitino.idp.web.rest.feature` |
| `gravitino.authorization.serviceAdmins`   | Comma-separated usernames allowed to manage users and groups      | `admin`                                     |

The local user store is incompatible with the `simple` authenticator, which is the server default and accepts the username a client supplies without checking a password. Both authenticators claim the same `Basic` authorization header, and the server uses the first one listed that claims it, so listing `simple` ahead of `basic` means passwords are never checked. Replace `simple` rather than adding to it.

The Web UI reads `gravitino.authenticators` from the server and presents a username and password form when `basic` is the active authenticator.

### Password and Username Rules

These rules apply to user creation, password changes, and `GRAVITINO_INITIAL_ADMIN_PASSWORD` alike.

| Rule            | Value                                     |
|-----------------|-------------------------------------------|
| Username        | Required, and must not contain a colon    |
| Password length | 12 to 64 characters inclusive             |

Passwords are reset by an administrator rather than changed by the user, so a password change request carries the new password only and no current password.

## Managing Users and Groups

All management endpoints are under `http://{host}:{port}/api/idp` and require Basic authentication as a service admin. Send `Accept: application/vnd.gravitino.v1+json` on every request, and `Content-Type: application/json` on requests with a body.

### User Operations

| Operation       | Method | Path                     | Body                                        |
|-----------------|--------|--------------------------|---------------------------------------------|
| Get a user      | GET    | `/api/idp/users/{user}`  | None                                        |
| Add a user      | POST   | `/api/idp/users`         | `{"user":"alice","password":"{password}"}`  |
| Reset a password| PUT    | `/api/idp/users/{user}`  | `{"password":"{new_password}"}`             |
| Remove a user   | DELETE | `/api/idp/users/{user}`  | None                                        |

The add-user body uses the field name `user` rather than `name`.

```shell
curl -s -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'admin:{admin_password}' | base64)" \
  -d '{"user":"alice","password":"{user_password}"}' \
  http://localhost:8090/api/idp/users
```

### Group Operations

| Operation                | Method | Path                                        | Body                                                       |
|--------------------------|--------|---------------------------------------------|------------------------------------------------------------|
| Get a group              | GET    | `/api/idp/groups/{group}`                   | None                                                       |
| Add a group              | POST   | `/api/idp/groups`                           | `{"group":"engineering"}`                                  |
| Remove a group           | DELETE | `/api/idp/groups/{group}?force={true false}`| None                                                       |
| Change group membership  | PUT    | `/api/idp/groups/{group}/users`             | `{"usersToAdd":["alice"],"usersToRemove":["carol"]}`       |

The add-group body uses the field name `group` rather than `name`. Removing a group that still has members fails unless `force=true`. A membership change requires at least one of `usersToAdd` or `usersToRemove`, and accepts both in a single request.

```shell
curl -s -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'admin:{admin_password}' | base64)" \
  -d '{"usersToAdd":["alice","bob"],"usersToRemove":["carol"]}' \
  http://localhost:8090/api/idp/groups/engineering/users
```

## Granting Access to Metadata

A local user can authenticate as soon as it exists, but it can only reach metadata once it is registered in a metalake and granted privileges there. The two are separate steps, and the username must match.

```shell
curl -s -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'admin:{admin_password}' | base64)" \
  -d '{"name":"alice"}' \
  http://localhost:8090/api/metalakes/{metalake}/users
```

When `gravitino.authorization.enable` is set to `true`, only service admins can create metalakes. See [Access Control](access-control.md) for roles, privileges, and ownership, and [Manage Metalakes](../manage-metalake-using-gravitino.md#create-a-metalake) for metalake creation.

## Connecting Engines

An engine reaches Gravitino by one of two paths, and each takes its own Basic credentials.

### Through the Gravitino Connector

Configure the connector for each engine, then add the credentials below. See [Spark Authentication](../spark-connector/spark-authentication-with-gravitino.md), [Flink Authentication](../flink-connector/flink-authentication-with-gravitino.md), and [Trino Authentication](../trino-connector/authentication.md) for the rest of the connector setup.

```properties
# Spark
spark.sql.gravitino.authType=basic
spark.sql.gravitino.basic.username={username}
spark.sql.gravitino.basic.password={password}
```

```yaml
# Flink
table.catalog-store.gravitino.gravitino.client.auth.type: basic
table.catalog-store.gravitino.gravitino.client.basic.username: {username}
table.catalog-store.gravitino.gravitino.client.basic.password: {password}
```

```properties
# Trino, in etc/catalog/gravitino.properties
gravitino.client.authType=basic
gravitino.client.basic.username={username}
gravitino.client.basic.password={password}
```

Trino also needs `catalog.management=dynamic` in `etc/config.properties` and a restart before the catalogs appear.

### Through the Iceberg REST Endpoint

Engines can connect straight to the Iceberg REST service at `http://{host}:9001/iceberg/` with no Gravitino connector plugin. See [Connect Spark via Iceberg REST](../iceberg-rest-engine/spark.md), [Connect Flink via Iceberg REST](../iceberg-rest-engine/flink.md), and [Connect Trino via Iceberg REST](../iceberg-rest-engine/trino.md) for the rest of the setup.

```properties
# Spark
spark.sql.catalog.{catalog}.rest.auth.type=basic
spark.sql.catalog.{catalog}.rest.auth.basic.username={username}
spark.sql.catalog.{catalog}.rest.auth.basic.password={password}
```

```sql
-- Flink
'rest.auth.type' = 'basic',
'rest.auth.basic.username' = '{username}',
'rest.auth.basic.password' = '{password}'
```

Trino has no native Basic mode for Iceberg REST and requires Trino 481 or later, so the header is set directly. Generate the encoded credentials with `echo -n '{username}:{password}' | base64`, then set:

```properties
# Trino, in etc/catalog/{catalog}.properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://localhost:9001/iceberg
iceberg.rest-catalog.http-headers=Authorization: Basic {base64_credentials}
```

## Further Reading

- [OpenAPI definition](../open-api/idp/openapi.yaml) for full request and response schemas
- [How to Use HTTPS](how-to-use-https.md) for protecting credentials in transit
- [Access Control](access-control.md) for what an authenticated user is allowed to do
