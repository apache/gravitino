<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Design of Local Authentication Support in Gravitino

## 1. Background

Apache Gravitino already has mature support for OAuth 2.0 authentication. Today, Gravitino acts as
an OAuth 2.0 client and delegates authentication to an external identity provider (IdP), typically
using the Client Credentials flow together with Bearer JWT.

This model works well in enterprise deployments where an external IdP such as Okta, Azure AD, or
Keycloak already exists. However, it introduces friction in several important scenarios:

- **POC and demo environments**: users want to start Gravitino in minutes, without first deploying
  and configuring a dedicated IdP.
- **Offline or isolated environments**: air-gapped, edge, or embedded deployments may not have
  access to an external identity service.
- **Data sovereignty requirements**: some organizations do not allow identity information to be
  managed by an external service.
- **Operational simplicity**: small deployments may not want the cost and maintenance burden of a
  separate OAuth server.

To address these cases, Gravitino should provide an optional local authentication mode with a simple
username/password authentication flow.

---

## 2. Goals

1. **Lower the barrier to entry**: allow users to evaluate and use Gravitino without deploying an
   external IdP.

2. **Support self-contained deployments**: provide a fully local authentication mechanism for
   offline, air-gapped, and privacy-sensitive environments.

3. **Keep the design intentionally simple**: optimize for POC and small deployment scenarios rather
   than building a full-featured general-purpose identity platform.

4. **Avoid vendor lock-in**: let users run Gravitino in environments where third-party IdPs are
   impractical, undesirable, or cost-prohibitive.

---

## 3. Proposal

### 3.1 Authentication Model

Built-in IDP provides local authentication for Gravitino. Gravitino authenticates incoming requests
through Basic authentication:

```text
Authorization: Basic <base64(username:password)>
```

Built-in IDP targets quick-start and isolated deployments with minimal configuration and no
external identity system.

### 3.2 Why Basic Authentication

The surveyed systems show that local authentication support typically converges on
username/password-based flows. For Gravitino, simplicity matters more than protocol richness:

- it shortens time-to-first-use,
- it is easy to explain and operate,
- it fits POC and offline scenarios well,
- and it avoids introducing token lifecycle complexity into the server.

For these reasons, the initial local authentication implementation uses:

| Item | Decision |
|---|---|
| Credential type | Username / password |
| Password storage | Database |
| Local token support | No |
| Recommended deployment scope | POC, offline, and isolated scenarios |

### 3.3 Why Database Storage

Passwords and user/group metadata should be stored in the Gravitino relational store rather than in
files:

- **File-based storage** requires a server restart to add users or rotate passwords.
- **Database storage** supports normal metadata-style CRUD operations and matches Gravitino's
  existing persistence model.

Database-backed storage is the most practical choice for local authentication.

### 3.4 Module Layout

The local authentication feature should be implemented as an independent Gravitino module.

The recommended module name is:

- `plugins:idp-basic`

The `plugins:idp-basic` module provides:

- local user and local group management,
- password hashing and verification,
- service admin initialization support,
- and the local authentication management API wiring.

---

## 4. Password Hashing

User credentials must never be stored in plaintext. Passwords are stored as password hashes in the
database.

Among the common password hashing algorithms, **SHA3-512** (via JDK `MessageDigest`) is the
recommended choice for Gravitino.

| Algorithm          | Status              |
|--------------------|---------------------|
| SHA3-512           | Recommended default |

The initial design uses **SHA3-512** as the only supported algorithm, which keeps the
implementation simple while relying on the JDK built-in digest API and avoiding additional
third-party cryptographic dependencies in the server distribution.

To make this implementable, the password hashing design should also define the storage and
dependency model explicitly:

- use the JDK JCA `MessageDigest` implementation for `SHA3-512`
- store the full SHA3-512 hash string in `password_hash`, including algorithm marker, parameters,
  salt, and hash output
- use a self-describing format so future parameter tuning does not require schema changes

For example, `password_hash` should store a PHC-style string such as:

```text
$sha3-512$i=100000$<salt>$<hash>
```

This keeps verification logic simple and allows future upgrades of SHA3-512 cost parameters without
introducing additional columns.

---

## 5. Data Model

Local authentication requires three new tables:

1. `idp_user_meta` — IdP user records
2. `idp_group_meta` — IdP group records
3. `idp_user_group_rel` — user/group membership mapping

These tables follow Gravitino's existing metadata table conventions:

- numeric primary keys,
- optimistic version fields,
- and `deleted_at` for soft deletion.

Soft-deleted rows in `idp_user_meta` and `idp_group_meta` should be cleaned asynchronously by
Gravitino's GC thread, following the same lifecycle management pattern used by other metadata
tables. When a local user or local group is physically removed by the GC thread, the implementation
should also clean the corresponding soft-deleted rows in `idp_user_group_rel` to avoid leaving
orphaned membership records.

Unlike Gravitino's existing `user_meta` and `group_meta` tables, `idp_user_meta` and
`idp_group_meta` are intentionally designed as **global identity tables** and therefore **do not
contain `metalake_id`**. The purpose of these tables is to store local authentication identities
and credentials once at the server level, instead of duplicating the same login identity in every
metalake.

### 5.1 `idp_user_meta`

```sql
CREATE TABLE IF NOT EXISTS `idp_user_meta` (
    `user_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'user id',
    `user_name` VARCHAR(128) NOT NULL COMMENT 'username',
    `password_hash` VARCHAR(1024) NOT NULL COMMENT 'hashed password',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'user current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'user last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'user deleted at',
    PRIMARY KEY (`user_id`),
    UNIQUE KEY `uk_un_del` (`user_name`, `deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'IdP user metadata';
```

### 5.2 `idp_group_meta`

```sql
CREATE TABLE IF NOT EXISTS `idp_group_meta` (
    `group_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'group id',
    `group_name` VARCHAR(128) NOT NULL COMMENT 'group name',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'group current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'group last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'group deleted at',
    PRIMARY KEY (`group_id`),
    UNIQUE KEY `uk_gn_del` (`group_name`, `deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'IdP group metadata';
```

### 5.3 `idp_user_group_rel`

```sql
CREATE TABLE IF NOT EXISTS `idp_user_group_rel` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `user_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'IdP user id',
    `group_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'IdP group id',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'relation current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'relation last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'relation deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_iuig_del` (`user_id`, `group_id`, `deleted_at`),
    KEY `idx_iuig_uid` (`user_id`),
    KEY `idx_iuig_gid` (`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'IdP user group relation';
```

### 5.4 Relationship Model

The logical entity relationship is straightforward:

```text
idp_user_meta
    └──< idp_user_group_rel >── idp_group_meta
```

For integration with Gravitino's existing access control model, the local authentication tables are also
logically associated with the existing metadata tables:

- `idp_user_meta` is associated with `user_meta` through `user_name`
- `idp_group_meta` is associated with `group_meta` through `group_name`

In other words, `idp_user_meta` stores local authentication credentials, while `user_meta`
continues to represent the Gravitino user object used by the current authorization model.
Similarly, `idp_group_meta` stores local group identities, while `group_meta` remains the
authorization-side group metadata.

Because `user_meta` and `group_meta` are metalake-scoped while `idp_user_meta` and
`idp_group_meta` are global, this association should be treated as a **name-based logical
mapping**, not as a database-level one-to-one foreign key constraint. A single local user or local
group may correspond to multiple `user_meta` or `group_meta` entries with the same name across
different metalakes.

The combined relationship can be viewed as:

```text
idp_user_meta --(user_name, logical mapping)--> user_meta[*]
idp_group_meta --(group_name, logical mapping)--> group_meta[*]

idp_user_meta
    └──< idp_user_group_rel >── idp_group_meta
```

This supports direct username lookup for authentication, group resolution for authorization, and a
clear mapping from local authentication identities to Gravitino's existing user/group metadata model while
preserving the requirement that local identity tables remain global and metalake-agnostic.

---

## 6. Service Admin Initialization

To keep local authentication usable immediately after installation without introducing a hard-coded
default password, Gravitino initializes service admin accounts from an environment variable during
startup.

### 6.1 Initialization Inputs

After Gravitino is installed, the operator should configure:

- `gravitino.authorization.serviceAdmins`, which remains the source of truth for service admin
  usernames
- `GRAVITINO_INITIAL_ADMIN_PASSWORD`, the initial password for service admins that do not yet have a
  row in `idp_user_meta`. Usernames come from `gravitino.authorization.serviceAdmins`.

### 6.2 Initialization Process

The initialization process should be:

1. Install Gravitino and configure `gravitino.authorization.serviceAdmins` and
   `gravitino.server.rest.extensionPackages`.
2. Before the first startup, set the `GRAVITINO_INITIAL_ADMIN_PASSWORD` environment variable when
   any configured service admin does not yet have a password stored in `idp_user_meta`.
3. Validate `GRAVITINO_INITIAL_ADMIN_PASSWORD` before writing anything to the database. The password
   must satisfy the local authentication password policy.
4. For each user configured in `gravitino.authorization.serviceAdmins`, check whether that service
   admin already has a password stored in `idp_user_meta`.
5. If the service admin already has a password in `idp_user_meta`, continue startup without
   modifying the stored password.
6. If `GRAVITINO_INITIAL_ADMIN_PASSWORD` is configured and the service admin does not yet have a
   password stored in `idp_user_meta`, hash the supplied password with SHA3-512 and create that
   service admin account in `idp_user_meta`.
7. If `GRAVITINO_INITIAL_ADMIN_PASSWORD` is not configured and any configured service admin does not
   yet have a password stored in `idp_user_meta`, fail startup immediately and prompt the user to
   declare `GRAVITINO_INITIAL_ADMIN_PASSWORD`.

This design keeps the first-use flow explicit while avoiding any built-in default credential. The
service admin exists before the first authenticated request is served, and only password hashes are
written to the database.

### 6.3 Example Initialization Flow

The following end-to-end flow shows how an operator can provision the initial service admins for a
fresh Gravitino deployment.

1. Deploy Gravitino with built-in IDP enabled:

   ```properties
   gravitino.authenticators=basic
   gravitino.server.rest.extensionPackages=org.apache.gravitino.idp.web.rest.feature
   gravitino.authorization.serviceAdmins=admin1,admin2
   ```

   Built-in IdP is incompatible with the `simple` authenticator (the default),
   `gravitino.authenticators` must include `basic` and must not include `simple`.

2. Export the initial service admin password before starting Gravitino:

   ```bash
   export GRAVITINO_INITIAL_ADMIN_PASSWORD='passwordForServiceAdmins'
   ```

3. Start Gravitino.

4. During startup, Gravitino validates the password, applies it to each username in
   `gravitino.authorization.serviceAdmins` that does not yet have a stored password, and leaves
   existing service admin passwords unchanged.

---

## 7. Authentication Flow

### 7.1 User Verification

The user verification flow is:

1. Read the `Authorization` header and verify that it uses the `Basic` scheme.
2. Decode the Base64 payload and split the decoded credential on the first colon to get `username`
   and `password`.
3. Query `idp_user_meta` by `user_name` and `deleted_at = 0`.
4. If no active user is found, reject the request with **401**.
5. If the user exists, continue to password verification.

### 7.2 Password Verification

The password verification flow is:

1. Query `idp_user_meta` by `user_name` and `deleted_at = 0`.
2. Read the stored `password_hash`.
3. Verify the submitted password against `password_hash` using the configured hashing algorithm.
4. If verification succeeds, authenticate the request; otherwise reject it.

### 7.3 Group Resolution

The local user's groups are resolved by:

1. Query `idp_user_meta` by username and `deleted_at = 0` to get `user_id`.
2. Query `idp_user_group_rel` by `user_id` to get all active `group_id` values.
3. Query `idp_group_meta` by those `group_id` values to load the full group set.

This keeps the model aligned with Gravitino's existing authorization architecture, where user-group
relationships are an input to later privilege evaluation.

### 7.4 Web Filter Behavior

The local authenticator is implemented as a web filter in the request pipeline.

The proposed behavior is:

1. Read the `Authorization` header.
2. Verify that it uses the `Basic` scheme.
3. Decode the Base64 payload. If decoding fails, return **400**.
4. Split the decoded credential on the first colon to extract `username` and `password`.
5. Look up the local user.
6. If the user does not exist or the password verification fails, return **401**.
7. If authentication is challenged, include a `WWW-Authenticate: Basic` response header.
8. If all checks pass, continue request processing as the authenticated principal.

### 7.5 Transport Security

Basic authentication should be used over **HTTPS**.

If Basic authentication is enabled on plain HTTP, Gravitino should emit a warning-level log because
credentials are otherwise exposed on the wire.

---

## 8. Configuration

### 8.1 Built-in IDP settings

| Key                                         | Value                                           | Required when using built-in IDP |
|---------------------------------------------|-------------------------------------------------|----------------------------------|
| `gravitino.authenticators`                  | Must include `basic`                            | Yes                              |
| `gravitino.server.rest.extensionPackages`   | `org.apache.gravitino.idp.web.rest.feature`     | Yes                              |
| `gravitino.authorization.serviceAdmins`     | Comma-separated service admin                   | Yes                              |

List `basic` in `gravitino.authenticators` and `org.apache.gravitino.idp.web.rest.feature` in
`gravitino.server.rest.extensionPackages` so Jersey registers `/api/idp/*` management APIs and the
Web UI can use the built-in IdP login form. Built-in IdP is incompatible with the `simple`
authenticator (the default), `gravitino.authenticators` must include `basic` and must not include
`simple`. Callers must use Basic authentication with a username
in `gravitino.authorization.serviceAdmins` and a password stored in `idp_user_meta`.

### 8.2 Password Algorithm

The initial implementation uses SHA3-512 as the fixed password hashing algorithm.

---

## 9. Administrative Operations

Service admins manage local users and groups under `/api/idp` (global paths, no `{metalake}`).
Operations are available when `org.apache.gravitino.idp.web.rest.feature` is listed in
`gravitino.server.rest.extensionPackages`.

### 9.1 HTTP Interface Design

#### 9.1.1 Get a local user

`GET /api/idp/users/{user}`

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/idp/users/alice
```


#### 9.1.2 Add a local user

`POST /api/idp/users`

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "user": "alice",
  "password": "Passw0rd-For-Gravitino-Alice"
}' http://localhost:8090/api/idp/users
```


#### 9.1.3 Remove a local user

`DELETE /api/idp/users/{user}` (soft-delete)

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/idp/users/alice
```


#### 9.1.4 Reset a local user password

`PUT /api/idp/users/{user}` — administrator-only reset (`password` only; no `oldPassword`). Usernames
must not contain `:`. Password length is 12–64 characters.

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "password": "Passw0rd-For-Gravitino-Alice-V2"
}' http://localhost:8090/api/idp/users/alice
```

#### 9.1.5 Get a local group

`GET /api/idp/groups/{group}`

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
http://localhost:8090/api/idp/groups/engineering
```


#### 9.1.6 Add a local group

`POST /api/idp/groups`

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "group": "engineering"
}' http://localhost:8090/api/idp/groups
```


#### 9.1.7 Remove a local group

`DELETE /api/idp/groups/{group}?force={true|false}` — soft-delete. Use `force=true` when the group
still has members.

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
'http://localhost:8090/api/idp/groups/engineering?force=true'
```


#### 9.1.8 Change local group membership

`PUT /api/idp/groups/{group}/users` — at least one of `usersToAdd` or `usersToRemove` is required.

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "usersToAdd": ["alice", "bob"],
  "usersToRemove": ["carol"]
}' http://localhost:8090/api/idp/groups/engineering/users
```


---

## 10. Work Plan and Checklist

### 10.1 Suggested Work Plan

| Phase | Work Item | Module / Files | Notes |
|---|---|---|---|
| 1 | Built-in IDP module wiring | `settings.gradle.kts`, `server/build.gradle.kts`, `plugins:idp-basic` | Add the new module and register `IdpRESTFeature` through `gravitino.server.rest.extensionPackages`. |
| 2 | Password hashing support | `PasswordHasher`, `Sha3512PasswordHasher`, related tests | Use SHA3-512 as the only supported password hashing algorithm and store PHC-style hash strings. |
| 3 | IdP metadata schema | JDBC schema files, mapper definitions, store layer | Create `idp_user_meta`, `idp_group_meta`, and `idp_user_group_rel` with soft-delete support. |
| 4 | Service admin initialization | startup initialization logic, validation logic | Validate `GRAVITINO_INITIAL_ADMIN_PASSWORD`, initialize missing configured service admins during startup, and fail startup when required credentials are absent. |
| 5 | Basic authentication flow | `BasicAuthenticator`, auth manager, filter integration | Verify Basic credentials against `idp_user_meta` and resolve the authenticated principal. |
| 6 | Group resolution | store layer, auth manager | Load the user's active groups from `idp_user_group_rel` and `idp_group_meta` for later authorization. |
| 7 | Local IdP management APIs | REST resources, DTOs, request/response classes | Implement user CRUD, group CRUD, and group membership management under `/api/idp`. |
| 8 | Tests and documentation | unit tests, integration tests, design and user docs | Cover initialization flow, authentication, metadata persistence, REST APIs, and doc/config alignment. |

### 10.2 Review Checklist

| Area | Checklist |
|---|---|
| Module wiring | The design, module name, and server wiring all consistently use `plugins:idp-basic`. |
| Configuration | All examples use `gravitino.server.rest.extensionPackages` and service-admin settings, and no obsolete configuration keys remain in the document. |
| Schema design | The document consistently uses `idp_user_meta`, `idp_group_meta`, and `idp_user_group_rel`, and the soft-delete lifecycle is clearly described. |
| Security constraints | The document states that passwords are never stored in plaintext, Basic authentication should be used only over HTTPS, and initialization must enforce password policy. |
| Initialization flow | The document explains how configured service admins are initialized during startup, when `GRAVITINO_INITIAL_ADMIN_PASSWORD` is required, and that only hashed passwords are written. |
| Authentication flow | The Basic authentication flow is described end-to-end, including username/password parsing, user lookup, hash verification, and group resolution. |
| API contract | Request paths and bodies for `/api/idp` APIs match the [Built-in IDP OpenAPI](../docs/open-api/idp/openapi.yaml). |
| Implementation alignment | The proposed work items are specific enough that reviewers and AI tools can map each part of the design to code changes and tests. |

---

## 11. Security Considerations

The local authentication capability is intentionally lightweight, but the following constraints remain important:

- passwords are stored as hashes, never plaintext
- Basic authentication should be used only over HTTPS
- the startup initialization flow must validate password policy and write only hashed passwords
- all metadata tables use soft deletion for traceability and operational safety
- local authentication is recommended for POC, offline, and isolated environments rather than as a
  replacement for enterprise-grade external identity systems

---

## 12. Summary

This design adds local authentication support to Gravitino for environments where an external IdP is
either unavailable or unnecessarily heavy. The key design choices are:

- **Basic authentication**
- **username/password credentials**
- **database-backed storage**
- **SHA3-512 password hashing**
- **startup-time service admin initialization**

The result is a self-contained authentication path that is easy to deploy, fast to evaluate, and
well aligned with Gravitino's lightweight quick-start experience.
