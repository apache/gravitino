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

# Design of Local IDP Support in Gravitino

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

To address these cases, Gravitino should provide an optional built-in local IdP mode with a simple
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

The local IdP is introduced as a new Gravitino authenticator mode: **basic**.

When enabled, Gravitino authenticates incoming requests through HTTP Basic authentication:

```text
Authorization: Basic <base64(username:password)>
```

This mode is intended for quick-start deployments and isolated environments. It should work out of
the box with a minimal configuration and without any dependency on an external identity system.

### 3.2 Why Basic Authentication

The surveyed systems show that local authentication support typically converges on
username/password-based flows. For Gravitino, simplicity matters more than protocol richness:

- it shortens time-to-first-use,
- it is easy to explain and operate,
- it fits POC and offline scenarios well,
- and it avoids introducing token lifecycle complexity into the server.

For these reasons, the initial local IdP implementation uses:

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

Database-backed storage is the most practical choice for a built-in local IdP.

---

## 4. Password Hashing

User credentials must never be stored in plaintext. Passwords are stored as password hashes in the
database.

Among the common password hashing algorithms, **Argon2id** is the recommended choice for
Gravitino.

| Algorithm | Status |
|---|---|
| Argon2id | Recommended default |

The initial design uses **Argon2id** as the only supported algorithm, which keeps the
implementation simple while aligning with modern password storage recommendations.

To make this implementable, the password hashing design should also define the storage and
dependency model explicitly:

- introduce one dedicated server-side password-hashing dependency that supports Argon2id
- store the full Argon2id hash string in `password_hash`, including algorithm marker, parameters,
  salt, and hash output
- use a self-describing format so future parameter tuning does not require schema changes

For example, `password_hash` should store a PHC-style string such as:

```text
$argon2id$v=19$m=65536,t=3,p=1$<salt>$<hash>
```

This keeps verification logic simple and allows future upgrades of Argon2id cost parameters without
introducing additional columns.

---

## 5. Data Model

The local IdP requires three new tables:

1. `local_user_meta` — local user records
2. `local_group_meta` — local group records
3. `local_group_user_rel` — user/group membership mapping

These tables follow Gravitino's existing metadata table conventions:

- numeric primary keys,
- `audit_info`,
- optimistic version fields,
- and `deleted_at` for soft deletion.

Unlike Gravitino's existing `user_meta` and `group_meta` tables, `local_user_meta` and
`local_group_meta` are intentionally designed as **global identity tables** and therefore **do not
contain `metalake_id`**. The purpose of these tables is to store local authentication identities
and credentials once at the server level, instead of duplicating the same login identity in every
metalake.

### 5.1 `local_user_meta`

```sql
CREATE TABLE IF NOT EXISTS `local_user_meta` (
    `user_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'user id',
    `user_name` VARCHAR(128) NOT NULL COMMENT 'username',
    `password_hash` VARCHAR(1024) NOT NULL COMMENT 'hashed password',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'user audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'user current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'user last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'user deleted at',
    PRIMARY KEY (`user_id`),
    UNIQUE KEY `uk_un_del` (`user_name`, `deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'local user metadata';
```

### 5.2 `local_group_meta`

```sql
CREATE TABLE IF NOT EXISTS `local_group_meta` (
    `group_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'group id',
    `group_name` VARCHAR(128) NOT NULL COMMENT 'group name',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'group audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'group current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'group last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'group deleted at',
    PRIMARY KEY (`group_id`),
    UNIQUE KEY `uk_gn_del` (`group_name`, `deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'local group metadata';
```

### 5.3 `local_group_user_rel`

```sql
CREATE TABLE IF NOT EXISTS `local_group_user_rel` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `group_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'local group id',
    `user_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'local user id',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'relation audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'relation current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'relation last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'relation deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_gi_ui_del` (`group_id`, `user_id`, `deleted_at`),
    KEY `idx_uid` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'local group user relation';
```

### 5.4 Relationship Model

The logical entity relationship is straightforward:

```text
local_user_meta
    └──< local_group_user_rel >── local_group_meta
```

For integration with Gravitino's existing access control model, the local IdP tables are also
logically associated with the existing metadata tables:

- `local_user_meta` is associated with `user_meta` through `user_name`
- `local_group_meta` is associated with `group_meta` through `group_name`

In other words, `local_user_meta` stores local authentication credentials, while `user_meta`
continues to represent the Gravitino user object used by the current authorization model.
Similarly, `local_group_meta` stores local group identities, while `group_meta` remains the
authorization-side group metadata.

Because `user_meta` and `group_meta` are metalake-scoped while `local_user_meta` and
`local_group_meta` are global, this association should be treated as a **name-based logical
mapping**, not as a database-level one-to-one foreign key constraint. A single local user or local
group may correspond to multiple `user_meta` or `group_meta` entries with the same name across
different metalakes.

The combined relationship can be viewed as:

```text
local_user_meta --(user_name, logical mapping)--> user_meta[*]
local_group_meta --(group_name, logical mapping)--> group_meta[*]

local_user_meta
    └──< local_group_user_rel >── local_group_meta
```

This supports direct username lookup for authentication, group resolution for authorization, and a
clear mapping from local IdP identities to Gravitino's existing user/group metadata model while
preserving the requirement that local identity tables remain global and metalake-agnostic.

---

## 6. Bootstrap Experience

To keep local IdP mode usable immediately after installation, Gravitino should provision a default
administrator account on first startup.

### 6.1 Initial Administrator

- only one bootstrap account is created by default: **service admin**
- the default password is **123456**
- after initial login, the service admin is expected to reset the bootstrap password immediately

This design favors usability for POC scenarios. The default password is intentionally simple so that
users can access the system instantly, but it must be treated as a bootstrap credential rather than
as a secure long-term password.

---

## 7. Authentication Flow

### 7.1 Password Verification

The password verification flow is:

1. Query `local_user_meta` by `user_name` and `deleted_at = 0`.
2. Read the stored `password_hash`.
3. Verify the submitted password against `password_hash` using the configured hashing algorithm.
4. If verification succeeds, authenticate the request; otherwise reject it.

### 7.2 Group Resolution

The local user's groups are resolved by:

1. Query `local_user_meta` by username and `deleted_at = 0` to get `user_id`.
2. Query `local_group_user_rel` by `user_id` to get all active `group_id` values.
3. Query `local_group_meta` by those `group_id` values to load the full group set.

This keeps the model aligned with Gravitino's existing authorization architecture, where user-group
relationships are an input to later privilege evaluation.

### 7.3 Web Filter Behavior

The local authenticator is implemented as a web filter in the request pipeline.

The proposed behavior is:

1. Read the `Authorization` header.
2. Verify that it uses the `Basic` scheme.
3. Decode the Base64 payload. If decoding fails, return **400**.
4. Extract `username:password`.
5. Look up the local user.
6. If the user does not exist or the password verification fails, return **401**.
7. If authentication is challenged, include a `WWW-Authenticate: Basic` response header.
8. If all checks pass, continue request processing as the authenticated principal.

> The exact HTTP status mapping should be aligned with Gravitino's existing authentication and
> exception handling conventions during implementation. A better default is:
> malformed Basic header → `400`; missing user or invalid password → `401`.

### 7.4 Transport Security

HTTP Basic authentication should be used over **HTTPS**.

If Basic authentication is enabled on plain HTTP, Gravitino should emit a warning-level log because
credentials are otherwise exposed on the wire.

---

## 8. Configuration

### 8.1 Authenticator Selection

Add `basic` to the configurable values of `gravitino.authenticators`.

| Key | New Value | Default | Optional Values |
|---|---|---|---|
| `gravitino.authenticators` | `basic` | `simple` | `simple`, `oauth`, `kerberos`, `basic` |

This should follow Gravitino's existing multi-authenticator behavior: multiple authenticators are
comma-separated, and if a request is supported by multiple authenticators simultaneously, the first
matching authenticator wins.

### 8.2 Password Algorithm Configuration

Add a dedicated configuration for password hashing.

To align with Gravitino's current configuration naming convention, authenticator-specific settings
should use the `gravitino.authenticator.<mode>.*` prefix rather than
`gravitino.authenticators.<mode>.*`.

| Key | Default | Optional Values |
|---|---|---|
| `gravitino.authenticator.basic.algorithm` | `Argon2id` | `Argon2id` |

Even though the initial implementation supports only one algorithm, an explicit configuration key
keeps the design extensible and makes the hashing choice visible in configuration.

---

## 9. Administrative Operations

The local IdP must support the following administrator-managed operations:

- get user
- add user
- remove user
- change user password
- get group
- add group
- remove group
- add user to group
- remove user from group

These operations are expected to be performed by the service admin. After membership changes,
administrators should be able to inspect group information and user information to verify the
result.

At a high level:

1. **Get user**: read the local user information and its current group memberships.
2. **Add user**: create a new local user with a hashed password.
3. **Remove user**: soft-delete the user record.
4. **Change user password**: reset the password hash for an existing local user through an
   administrator-managed operation.
5. **Get group**: read the local group information and its current user memberships.
6. **Add group**: create a new local group.
7. **Remove group**: soft-delete the group record.
8. **Add user to group**: create a row in `local_group_user_rel`.
9. **Remove user from group**: soft-delete the corresponding relation row.

### 9.1 HTTP Interface Design

The following APIs are intended for local user, local group, and group-membership management.

Because local IdP identities are global rather than metalake-scoped, these management interfaces do
not include `{metalake}` in their paths and use the `/api/auth/basic` prefix.

#### 9.1.1 Get Local User Info

| Item | Value |
|---|---|
| Method | `GET` |
| Path | `/api/auth/basic/users/{user}` |
| Permission | Only service admin / owner can execute |

**Path parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `user` | `String` | Yes | User name |

**Response**

| Field | Type | Description |
|---|---|---|
| `name` | `String` | User name |
| `audit` | `AuditDTO` | Audit information |
| `groups` | `Array<String>` | Groups to which the user belongs |

#### 9.1.2 Add Local User

| Item | Value |
|---|---|
| Method | `POST` |
| Path | `/api/auth/basic/users/{user}` |
| Permission | Only service admin can execute |

**Parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `user` | `String` | Yes | User name |
| `password` | `String` | Yes | Password |

#### 9.1.3 Remove Local User

| Item | Value |
|---|---|
| Method | `DELETE` |
| Path | `/api/auth/basic/users/{user}` |
| Permission | Only service admin can execute |

**Path parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `user` | `String` | Yes | User name |

#### 9.1.4 Change Password

| Item | Value                          |
|---|--------------------------------|
| Method | `PUT`                          |
| Path | `/api/auth/basic/users/{user}` |
| Permission | Only service admin can execute |

**Parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `user` | `String` | Yes | User name |
| `password` | `String` | Yes | New password |

**Spec**

- This API is an administrator-managed password reset API.
- It does not support end-user self-service password changes.
- It does not accept `oldPassword`.
- Only service admin can change any account password.
- The password cannot contain a colon (`:`). (RFC 7617)
- The password must be at least 12 characters long and at most 64 characters long.

**Error codes**

| Error case | HTTP status |
|---|---|
| Password verification failed | `401` |
| Account doesn't exist | `404` |
| Password same with old | `422` |

#### 9.1.5 Get Local Group Info

| Item | Value |
|---|---|
| Method | `GET` |
| Path | `/api/auth/basic/groups/{group}` |
| Permission | Only service admin can execute |

**Path parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `group` | `String` | Yes | Group name |

**Response**

| Field | Type | Description |
|---|---|---|
| `name` | `String` | Group name |
| `audit` | `AuditDTO` | Audit information |
| `users` | `Array<String>` | All users in the group |

#### 9.1.6 Add Local Group

| Item | Value |
|---|---|
| Method | `POST` |
| Path | `/api/auth/basic/groups/{group}` |
| Permission | Only service admin can execute |

**Path parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `group` | `String` | Yes | Group name |

#### 9.1.7 Remove Local Group

| Item | Value |
|---|---|
| Method | `DELETE` |
| Path | `/api/auth/basic/groups/{group}` |
| Permission | Only service admin can execute |

**Path parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `group` | `String` | Yes | Group name |

#### 9.1.8 Group Add User

| Item | Value |
|---|---|
| Method | `PUT` |
| Path | `/api/auth/basic/groups/{group}/users/add` |
| Permission | Only service admin can execute |

**Parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `group` | `String` | Yes | Group name |
| `userNames` | `Array<String>` | Yes | User names. Must not be null or empty |

#### 9.1.9 Group Remove User

| Item | Value |
|---|---|
| Method | `DELETE` |
| Path | `/api/auth/basic/groups/{group}/users/remove` |
| Permission | Only service admin can execute |

**Parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `group` | `String` | Yes | Group name |
| `userNames` | `Array<String>` | Yes | User names. Must not be null or empty |
---

## 10. Security Considerations

The local IdP is intentionally lightweight, but the following constraints remain important:

- passwords are stored as hashes, never plaintext
- Basic authentication should be used only over HTTPS
- bootstrap credentials must be rotated after installation
- all metadata tables use soft deletion for traceability and operational safety
- local IdP mode is recommended for POC, offline rather than as a
  replacement for enterprise-grade external identity systems

---

## 11. Summary

This design adds a minimal local IdP to Gravitino for environments where an external IdP is either
unavailable or unnecessarily heavy. The key design choices are:

- **HTTP Basic authentication**
- **username/password credentials**
- **database-backed storage**
- **Argon2id password hashing**
- **bootstrap service admin account**

The result is a self-contained authentication path that is easy to deploy, fast to evaluate, and
well aligned with Gravitino's lightweight quick-start experience.
