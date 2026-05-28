---
title: How to use built-in IDP (local authentication)
slug: /how-to-use-idp
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Apache Gravitino can store **built-in IDP** (identity provider) users and groups in the relational
metadata store through the `idp-basic` plugin. This gives you a self-contained way to manage
**global** login identities (usernames, password hashes, and group membership) without an external
OAuth server.

Built-in IDP is aimed at POC, offline, and isolated deployments. It is **not** a replacement for
enterprise IDPs such as Okta, Azure AD, or Keycloak. Use it only where a lightweight local identity
store is acceptable; restrict management APIs to **service admins**, store password hashes only,
and prefer [HTTPS](./security/how-to-use-https.md) when credentials travel over the network.

This guide describes how to enable and operate the management APIs in `plugins:idp-basic`. For
design background, see
[Design of local authentication support](../design-docs/gravitino-local-authentication.md). For
request and response schemas, see the [Built-in IDP OpenAPI](./open-api/idp/openapi.yaml).

---

## Prerequisites

Before you call `/api/idp/*`, ensure the following:

1. **IDP database tables** — Run the appropriate upgrade script under `${GRAVITINO_HOME}/scripts/`
   so the relational store contains `idp_user_meta`, `idp_group_meta`, and `idp_user_group_rel`
   (for example `scripts/mysql/upgrade-1.2.0-to-1.3.0-mysql.sql`). See
   [How to use relational backend storage](./how-to-use-relational-backend-storage.md).

2. **Service admin** — Management APIs require an authenticated principal listed in
   `gravitino.authorization.serviceAdmins`. `IdpAuthorizationFilter` checks that list; it does not
   use built-in IDP passwords from `idp_user_meta` for `/api/idp/*` authorization today.

   1. Enable authorization and set service admin usernames in `gravitino.conf` (see
      [Access control](./security/access-control.md)):

      ```properties
      gravitino.authorization.enable = true
      gravitino.authorization.serviceAdmins = admin
      ```

   2. Complete [Configuration](#configuration) (including `basic` in `gravitino.authenticators` and
      `gravitino.server.rest.extensionPackages`), then restart Gravitino.

   3. **Call APIs as the service admin** — Use a configured authenticator (for example **simple**)
      with a username that matches `gravitino.authorization.serviceAdmins`. With simple mode, send
      that name in the `Authorization` header; an empty password is allowed when the server permits
      it. See [How to authenticate](./security/how-to-authenticate.md).

   4. **Set a built-in IDP password for the service admin (optional)** — To store a password hash in
      `idp_user_meta` for a service admin username, call `POST /api/idp/users` while authenticated as
      that service admin (see [Add a user](#add-a-user)). Example:

      ```shell
      curl -s -X POST -H "Accept: application/vnd.gravitino.v1+json" \
        -H "Content-Type: application/json" \
        -H "Authorization: Basic $(echo -n 'admin:' | base64)" \
        -d '{"user":"admin","password":"Passw0rd-Admin12"}' \
        http://localhost:8090/api/idp/users
      ```

      Automatic startup initialization via `GRAVITINO_INITIAL_ADMIN_PASSWORD` is **not implemented**
      yet. See
      [Design of local authentication support](../design-docs/gravitino-local-authentication.md) §6
      for the planned flow.

---

## Configuration

Add the following to `gravitino.conf` to expose built-in IDP management REST APIs:

| Configuration item | Description | Example |
|--------------------|-------------|---------|
| `gravitino.server.rest.extensionPackages` | Jersey package that discovers `IdpRESTFeature` | `org.apache.gravitino.idp.web.rest.feature` |
| `gravitino.authorization.serviceAdmins` | Usernames allowed to manage `/api/idp/*` | `admin` |

Example:

```properties
gravitino.server.rest.extensionPackages = org.apache.gravitino.idp.web.rest.feature
gravitino.authorization.serviceAdmins = admin
```

`IdpRESTFeature` registers `/api/idp/*` only when `basic` is included in `gravitino.authenticators`
**and** `org.apache.gravitino.idp.web.rest.feature` is listed in
`gravitino.server.rest.extensionPackages`. If `basic` is absent, those routes are not registered.

General authentication settings (`simple`, `oauth`, `kerberos`, and related keys) are documented in
[How to authenticate](./security/how-to-authenticate.md).

---

## Operations

The following sections show how to call built-in IDP management APIs with `curl`. Replace
`localhost:8090` and the `Authorization` header with values that match your deployment.

### Before you call the APIs

Complete [Prerequisites](#prerequisites) first. The subsections below assume you are calling the APIs
as a configured service admin.

**Base URL** — `http://<host>:<port>/api/idp`

**Common headers**

| Header | Value |
|--------|--------|
| `Accept` | `application/vnd.gravitino.v1+json` |
| `Content-Type` | `application/json` (for POST and PUT bodies) |

Example using **simple** mode as service admin `admin` (empty password is allowed when the server
permits it):

```shell
curl -s -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Authorization: Basic $(echo -n 'admin:' | base64)" \
  http://localhost:8090/api/idp/users/alice
```

### Password and username rules

Add-user and change-password requests are validated by `IdpCredentialValidator`:

| Rule | Value |
|------|--------|
| Username | Required; must **not** contain `:` |
| Password length | 12–64 characters (inclusive) |
| Password storage | Argon2id PHC string in `idp_user_meta.password_hash` |

Password reset is **admin-only** (request body has `password` only; no `oldPassword`).

### User operations

#### Get a user

`GET /api/idp/users/{user}`

```shell
curl -s -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Authorization: Basic $(echo -n 'admin:' | base64)" \
  http://localhost:8090/api/idp/users/alice
```

#### Add a user

`POST /api/idp/users`

The request body uses field `user` (not `name`):

```shell
curl -s -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'admin:' | base64)" \
  -d '{"user":"alice","password":"Passw0rd-Alice"}' \
  http://localhost:8090/api/idp/users
```

#### Change a user password

`PUT /api/idp/users/{user}`

Administrator reset only:

```shell
curl -s -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'admin:' | base64)" \
  -d '{"password":"Passw0rd-Alice-V2"}' \
  http://localhost:8090/api/idp/users/alice
```

#### Remove a user

`DELETE /api/idp/users/{user}`

Soft-deletes the user (`deleted_at`). Physical removal is handled by the IDP garbage collector.

```shell
curl -s -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Authorization: Basic $(echo -n 'admin:' | base64)" \
  http://localhost:8090/api/idp/users/alice
```

### Group operations

#### Get a group

`GET /api/idp/groups/{group}`

```shell
curl -s -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Authorization: Basic $(echo -n 'admin:' | base64)" \
  http://localhost:8090/api/idp/groups/engineering
```

#### Add a group

`POST /api/idp/groups`

The request body uses field `group` (not `name`):

```shell
curl -s -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'admin:' | base64)" \
  -d '{"group":"engineering"}' \
  http://localhost:8090/api/idp/groups
```

#### Remove a group

`DELETE /api/idp/groups/{group}?force={true|false}`

If the group still has members, deletion fails unless `force=true`.

```shell
curl -s -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Authorization: Basic $(echo -n 'admin:' | base64)" \
  'http://localhost:8090/api/idp/groups/engineering?force=true'
```

#### Change group membership

`PUT /api/idp/groups/{group}/users`

Add and/or remove members in one request. At least one of `usersToAdd` or `usersToRemove` is required.

```shell
curl -s -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'admin:' | base64)" \
  -d '{"usersToAdd":["alice","bob"],"usersToRemove":["carol"]}' \
  http://localhost:8090/api/idp/groups/engineering/users
```

For full request and response definitions, see the [Built-in IDP OpenAPI](./open-api/idp/openapi.yaml).

---

## Use with Gravitino access control

Built-in IDP tables are **global** (no `metalake_id`). Metalake-scoped `user_meta` and `group_meta`
used by RBAC are separate objects. They are associated **by name** (`user_name` / `group_name`)
across metalakes, not by a database foreign key.

Typical workflow:

1. Create built-in IDP users and groups with `/api/idp/*` (this guide).
2. In each metalake, create matching Gravitino users and groups for authorization.
3. Grant roles and privileges as described in [Access control](./security/access-control.md).

---

## Further reading

- [Built-in IDP OpenAPI](./open-api/idp/openapi.yaml) — API paths, bodies, and schemas
- [Design of local authentication support](../design-docs/gravitino-local-authentication.md) — architecture and future authentication flows
- [How to authenticate](./security/how-to-authenticate.md) — `simple`, OAuth, and Kerberos
- [How to use HTTPS](./security/how-to-use-https.md) — transport security for credentials
