---
title: How to use built-in IdP (local authentication)
slug: /how-to-use-idp
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Apache Gravitino can store **built-in IdP** (identity provider) users and groups in the relational
metadata store through the `idp-basic` plugin. This gives you a self-contained way to manage
**global** login identities (usernames, password hashes, and group membership) without an external
OAuth server.

Built-in IdP is aimed at POC, offline, and isolated deployments. It is **not** a replacement for
enterprise IdPs such as Okta, Azure AD, or Keycloak.

This guide is based on the current `plugins:idp-basic` implementation. For design background, see
[Design of local authentication support](../design-docs/gravitino-local-authentication.md). For
the machine-readable API contract, see the [Built-in IdP OpenAPI](../open-api/idp/openapi.yaml).

### What is implemented today

| Capability | Status |
|----------|--------|
| Relational tables `idp_user_meta`, `idp_group_meta`, `idp_user_group_rel` | Implemented (schema in 1.3.0 upgrade scripts) |
| Argon2id password hashing (`password_hash` column) | Implemented |
| REST management APIs under `/api/idp/...` | Implemented in `idp-basic` |
| Service-admin authorization on management APIs | Implemented (`IdpAuthorizationFilter`) |
| HTTP Basic authentication against `idp_user_meta` for all Gravitino APIs | **Not implemented** (no `basic` entry in `AuthenticatorFactory`) |
| Startup initialization via `GRAVITINO_INITIAL_ADMIN_PASSWORD` | **Not implemented** |

The value `basic` in `gravitino.authenticators` is used today only as a **feature flag** to
register IdP management REST resources (`IdpRESTFeature`). It is **not** a working HTTP
authenticator name yet: if you list `basic` in `gravitino.authenticators`, server startup fails
because `AuthenticatorFactory` tries to load a Java class named `basic`. Until authenticator
wiring is completed, treat the management APIs below as the implemented contract, not as a
fully enabled production path.

---

## Prerequisites

1. **Database schema** that includes the IdP tables. For MySQL/PostgreSQL, run the appropriate
   upgrade script under `${GRAVITINO_HOME}/scripts/` (for example
   `scripts/mysql/upgrade-1.2.0-to-1.3.0-mysql.sql` creates `idp_user_meta`, `idp_group_meta`, and
   `idp_user_group_rel`). See [How to use relational backend storage](./how-to-use-relational-backend-storage.md).
2. **Plugin JARs** in `${GRAVITINO_HOME}/libs/`:
   - `gravitino-idp-basic-*.jar` (from `./gradlew :plugins:idp-basic:copyLibAndConfigs`)
   - `bcprov-jdk18on-*.jar` (Argon2id dependency; copied by the same task)
3. **Authorization enabled** with at least one **service admin** if you call management APIs
   (see [Access control](./security/access-control.md)).

---

## Server configuration

Add the following to `gravitino.conf` when built-in IdP management is enabled (after
authenticator wiring supports the `basic` flag without breaking startup):

| Configuration item | Description | Example |
|--------------------|-------------|---------|
| `gravitino.authenticators` | Must include `basic` to register IdP REST APIs (`IdpRESTFeature`) | `simple,basic` (once `basic` is a non-loading flag or a real authenticator exists) |
| `gravitino.server.rest.extensionPackages` | Jersey package that discovers `IdpRESTFeature` | `org.apache.gravitino.idp.web.rest.feature` |
| `gravitino.authorization.enable` | Required for service-admin checks on IdP APIs | `true` |
| `gravitino.authorization.serviceAdmins` | Usernames allowed to call `/api/idp/*` | `admin` |

Example (intended end state):

```properties
gravitino.authenticators = simple,basic
gravitino.server.rest.extensionPackages = org.apache.gravitino.idp.web.rest.feature
gravitino.authorization.enable = true
gravitino.authorization.serviceAdmins = admin
```

`IdpRESTFeature` registers management resources only when `basic` appears in
`gravitino.authenticators` **and** `org.apache.gravitino.idp.web.rest.feature` is listed in
`gravitino.server.rest.extensionPackages`. If `basic` is absent, `/api/idp/*` routes are not
registered (HTTP 404).

---

## Calling management APIs

### Authentication for `/api/idp/*`

Management APIs do **not** use built-in IdP passwords for authorization today. `IdpAuthorizationFilter`
requires the **already authenticated** caller to be listed in `gravitino.authorization.serviceAdmins`.

Until HTTP Basic login against `idp_user_meta` exists, use another configured authenticator (for
example **simple**) to authenticate as a service admin, then call IdP APIs.

Example with **simple** mode (empty password is allowed):

```shell
export GRAVITINO_USER=admin
curl -s -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Authorization: Basic $(echo -n 'admin:' | base64)" \
  http://localhost:8090/api/idp/users/alice
```

If the caller is not a service admin, the server returns **403** with a message like
`Only service admins can manage built-in IdP users and groups.`

### Common headers

| Header | Value |
|--------|--------|
| `Accept` | `application/vnd.gravitino.v1+json` |
| `Content-Type` | `application/json` (for POST/PUT bodies) |

Base URL prefix: `http://<host>:<port>/api/idp`.

---

## Password and username rules

Enforced by `IdpCredentialValidator` on add-user and change-password requests:

| Rule | Value |
|------|--------|
| Username | Required; must **not** contain `:` |
| Password length | 12–64 characters (inclusive) |
| Password storage | Argon2id PHC string in `idp_user_meta.password_hash` |

Password reset is **admin-only** (no `oldPassword`, no self-service).

---

## User APIs

### Get user

`GET /api/idp/users/{user}`

```shell
curl -s -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Authorization: Basic $(echo -n 'admin:' | base64)" \
  http://localhost:8090/api/idp/users/alice
```

Response:

```json
{
  "code": 0,
  "user": {
    "name": "alice",
    "groups": ["engineering"]
  }
}
```

### Add user

`POST /api/idp/users`

Request body uses field `user` (not `name`):

```shell
curl -s -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'admin:' | base64)" \
  -d '{"user":"alice","password":"Passw0rd-Alice"}' \
  http://localhost:8090/api/idp/users
```

| HTTP status | Meaning |
|-------------|---------|
| 200 | User created |
| 400 | Invalid username/password |
| 403 | Not a service admin |
| 409 | User already exists |

### Change password (admin reset)

`PUT /api/idp/users/{user}`

```shell
curl -s -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'admin:' | base64)" \
  -d '{"password":"Passw0rd-Alice-V2"}' \
  http://localhost:8090/api/idp/users/alice
```

| HTTP status | Meaning |
|-------------|---------|
| 404 | User does not exist |

### Remove user

`DELETE /api/idp/users/{user}`

Soft-deletes the user (`deleted_at`); physical removal is handled by the IdP garbage collector.

```shell
curl -s -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Authorization: Basic $(echo -n 'admin:' | base64)" \
  http://localhost:8090/api/idp/users/alice
```

Response:

```json
{
  "code": 0,
  "removed": true
}
```

---

## Group APIs

### Get group

`GET /api/idp/groups/{group}`

```shell
curl -s -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Authorization: Basic $(echo -n 'admin:' | base64)" \
  http://localhost:8090/api/idp/groups/engineering
```

Response:

```json
{
  "code": 0,
  "group": {
    "name": "engineering",
    "users": ["alice", "bob"]
  }
}
```

### Add group

`POST /api/idp/groups`

Request body uses field `group` (not `name`):

```shell
curl -s -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'admin:' | base64)" \
  -d '{"group":"engineering"}' \
  http://localhost:8090/api/idp/groups
```

### Remove group

`DELETE /api/idp/groups/{group}?force={true|false}`

If the group still has members, deletion fails unless `force=true` (**405** /
`IllegalStateException` in the implementation).

```shell
curl -s -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Authorization: Basic $(echo -n 'admin:' | base64)" \
  'http://localhost:8090/api/idp/groups/engineering?force=true'
```

### Change group membership

`PUT /api/idp/groups/{group}/users`

Adds and/or removes members in one request. At least one of `usersToAdd` or `usersToRemove` must be
set (this replaces the older separate `/add` and `/remove` paths from early design drafts).

```shell
curl -s -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'admin:' | base64)" \
  -d '{"usersToAdd":["alice","bob"],"usersToRemove":["carol"]}' \
  http://localhost:8090/api/idp/groups/engineering/users
```

| HTTP status | Meaning |
|-------------|---------|
| 404 | Group or referenced user does not exist |

---

## Relationship to Gravitino authorization users

Built-in IdP tables are **global** (no `metalake_id`). Gravitino metalake-scoped `user_meta` /
`group_meta` used by access control are separate. The link is **logical**, by matching
`user_name` / `group_name` strings across metalakes—not a database foreign key.

Typical flow after HTTP Basic auth is wired:

1. Create built-in IdP users/groups via `/api/idp/*`.
2. Create matching Gravitino users/groups in each metalake for RBAC.
3. Assign roles and privileges using [access control](./security/access-control.md).

---

## Security notes

- Store only **password hashes** in the database; never log plaintext passwords.
- Use [HTTPS](./security/how-to-use-https.md) when sending credentials once HTTP Basic auth is enabled.
- Restrict IdP management to **service admins**; do not expose `/api/idp/*` anonymously.
- Built-in IdP is recommended for **POC and isolated** environments, not as a full enterprise IdP.

---

## OpenAPI and further reading

- OpenAPI: [docs/open-api/idp/openapi.yaml](./open-api/idp/openapi.yaml)
- Design doc: [design-docs/gravitino-local-authentication.md](../design-docs/gravitino-local-authentication.md)
- General authentication modes: [How to authenticate](./security/how-to-authenticate.md)
