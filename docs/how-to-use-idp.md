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

2. **Service admin passwords** — Built-in IDP requires every username in
   `gravitino.authorization.serviceAdmins` to have a password stored in `idp_user_meta` before you
   can call management APIs.

   1. Enable authorization and set service admin usernames in `gravitino.conf` (see
      [Access control](./security/access-control.md)):

      ```properties
      gravitino.authorization.enable = true
      gravitino.authorization.serviceAdmins = admin
      ```

   2. **Initialize service admin passwords at startup** — Before the first start, export
      `GRAVITINO_INITIAL_ADMIN_PASSWORD`. The value is a JSON array of `username:password` entries.
      Each username must appear in `gravitino.authorization.serviceAdmins`, and each password must
      satisfy the [password rules](#password-and-username-rules) below.

      ```shell
      export GRAVITINO_INITIAL_ADMIN_PASSWORD='["admin:Passw0rd-Admin12"]'
      ```

      On startup, Gravitino hashes these passwords into `idp_user_meta` for service admins that do
      not already have a stored password. If a configured service admin has no password in the store
      and this variable is not set, startup fails. See
      [Design of local authentication support](../design-docs/gravitino-local-authentication.md) §6
      for the full initialization rules.

   3. **Start or restart Gravitino** so the initialization runs.

   4. **Call APIs with built-in IDP credentials** — Use HTTP Basic authentication with the service
      admin username and the password you initialized (for example `admin` /
      `Passw0rd-Admin12`). `IdpAuthorizationFilter` still requires the authenticated principal to be
      listed in `gravitino.authorization.serviceAdmins`.

---

## Configuration

Add the following to `gravitino.conf` to expose built-in IDP management REST APIs:

| Configuration item | Description | Example |
|--------------------|-------------|---------|
| `gravitino.server.rest.extensionPackages` | Jersey package that discovers `IdpRESTFeature` | `org.apache.gravitino.idp.web.rest.feature` |
| `gravitino.authorization.serviceAdmins` | Usernames allowed to manage `/api/idp/*` | `admin` |

Example:

```properties
gravitino.authorization.enable = true
gravitino.server.rest.extensionPackages = org.apache.gravitino.idp.web.rest.feature
gravitino.authorization.serviceAdmins = admin
```

`IdpRESTFeature` registers `/api/idp/*` when `org.apache.gravitino.idp.web.rest.feature` is listed in
`gravitino.server.rest.extensionPackages`.

---

## Operations

The following sections show how to call built-in IDP management APIs with `curl`. Replace
`localhost:8090`, usernames, and passwords with values that match your deployment.

### Before you call the APIs

Complete [Prerequisites](#prerequisites) first, including service admin password initialization.

**Authentication** — Send HTTP Basic credentials for a service admin whose password exists in
`idp_user_meta`. The examples below use `admin` / `Passw0rd-Admin12` (the same values as in the
`GRAVITINO_INITIAL_ADMIN_PASSWORD` example above).

**Base URL** — `http://<host>:<port>/api/idp`

**Common headers**

| Header | Value |
|--------|--------|
| `Accept` | `application/vnd.gravitino.v1+json` |
| `Content-Type` | `application/json` (for POST and PUT bodies) |

Example:

```shell
curl -s -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Authorization: Basic $(echo -n 'admin:Passw0rd-Admin12' | base64)" \
  http://localhost:8090/api/idp/users/alice
```

### Password and username rules

Add-user, change-password, and `GRAVITINO_INITIAL_ADMIN_PASSWORD` entries are validated by
`IdpCredentialValidator`:

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
  -H "Authorization: Basic $(echo -n 'admin:Passw0rd-Admin12' | base64)" \
  http://localhost:8090/api/idp/users/alice
```

#### Add a user

`POST /api/idp/users`

The request body uses field `user` (not `name`):

```shell
curl -s -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'admin:Passw0rd-Admin12' | base64)" \
  -d '{"user":"alice","password":"Passw0rd-Alice"}' \
  http://localhost:8090/api/idp/users
```

#### Change a user password

`PUT /api/idp/users/{user}`

Administrator reset only:

```shell
curl -s -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'admin:Passw0rd-Admin12' | base64)" \
  -d '{"password":"Passw0rd-Alice-V2"}' \
  http://localhost:8090/api/idp/users/alice
```

#### Remove a user

`DELETE /api/idp/users/{user}`

Soft-deletes the user (`deleted_at`). Physical removal is handled by the IDP garbage collector.

```shell
curl -s -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Authorization: Basic $(echo -n 'admin:Passw0rd-Admin12' | base64)" \
  http://localhost:8090/api/idp/users/alice
```

### Group operations

#### Get a group

`GET /api/idp/groups/{group}`

```shell
curl -s -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Authorization: Basic $(echo -n 'admin:Passw0rd-Admin12' | base64)" \
  http://localhost:8090/api/idp/groups/engineering
```

#### Add a group

`POST /api/idp/groups`

The request body uses field `group` (not `name`):

```shell
curl -s -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'admin:Passw0rd-Admin12' | base64)" \
  -d '{"group":"engineering"}' \
  http://localhost:8090/api/idp/groups
```

#### Remove a group

`DELETE /api/idp/groups/{group}?force={true|false}`

If the group still has members, deletion fails unless `force=true`.

```shell
curl -s -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Authorization: Basic $(echo -n 'admin:Passw0rd-Admin12' | base64)" \
  'http://localhost:8090/api/idp/groups/engineering?force=true'
```

#### Change group membership

`PUT /api/idp/groups/{group}/users`

Add and/or remove members in one request. At least one of `usersToAdd` or `usersToRemove` is required.

```shell
curl -s -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'admin:Passw0rd-Admin12' | base64)" \
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

1. Initialize service admin passwords and create additional built-in IDP users and groups with
   `/api/idp/*` (this guide).
2. In each metalake, create matching Gravitino users and groups for authorization.
3. Grant roles and privileges as described in [Access control](./security/access-control.md).

---

## Further reading

- [Built-in IDP OpenAPI](./open-api/idp/openapi.yaml) — API paths, bodies, and schemas
- [Design of local authentication support](../design-docs/gravitino-local-authentication.md) — service admin initialization and authentication flows
- [How to use HTTPS](./security/how-to-use-https.md) — transport security for credentials
