---
title: How to use built-in IDP
slug: /security/how-to-use-built-in-idp
keyword: security authentication idp
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Apache Gravitino can store **built-in IDP** (identity provider) users and groups in the relational
metadata store through the `idp-basic` plugin. This gives you a self-contained way to manage
**global** login identities (usernames, password hashes, and group membership) without an external server.

Built-in IDP is aimed at POC, offline, and isolated deployments. It is **not** a replacement for
enterprise IDPs such as Okta, Azure AD, or Keycloak. Use it only where a lightweight local identity
store is acceptable; restrict management APIs to **service admins**, store password hashes only,
and prefer [HTTPS](how-to-use-https.md) when credentials travel over the network.

This guide describes how to enable and operate the management APIs in `plugins:idp-basic`. For
request and response schemas, see the [Built-in IDP OpenAPI](../open-api/idp/openapi.yaml).

## Web UI

When built-in IdP Basic authentication is enabled, the Web UI exposes a username and password login
form. Configure **both** of the following in `gravitino.conf`:

```properties
gravitino.authenticators = basic
gravitino.server.rest.extensionPackages = org.apache.gravitino.idp.web.rest.feature
```

The Web UI reads `gravitino.authenticators` from `/configs` and uses the first entry as the active
authentication type. List `basic` first when you want the built-in IdP login form. Built-in IdP is
**incompatible** with the `simple` authenticator (the default). When IdP is enabled, do not include
`simple` in `gravitino.authenticators`.

---

## Prerequisites

Before you call `/api/idp/*`, ensure the following:

1. **IDP REST API registration** — In `gravitino.conf`, set:

   ```properties
   gravitino.authenticators = basic
   gravitino.server.rest.extensionPackages = org.apache.gravitino.idp.web.rest.feature
   ```

2. **Server authenticator** — Built-in IdP is **incompatible** with the `simple` authenticator
   (the default). Do not list `simple` together with `basic` in `gravitino.authenticators`.

3. **Service admin passwords** — Built-in IDP requires every username in
   `gravitino.authorization.serviceAdmins` to have a password stored in `idp_user_meta` before you
   can call management APIs.

   1. Set service admin usernames in `gravitino.conf` (see [Access control](access-control.md)):

      ```properties
      gravitino.authorization.serviceAdmins = admin
      ```

   2. **Initialize service admin passwords at startup** — Before the first start, set
      `GRAVITINO_INITIAL_ADMIN_PASSWORD` to the initial password. Usernames come from
      `gravitino.authorization.serviceAdmins`. The value must satisfy the
      [password rules](#password-and-username-rules) below.

      ```shell
      export GRAVITINO_INITIAL_ADMIN_PASSWORD='Passw0rd-Admin12'
      ```

   3. **Start Gravitino**.

   4. **Call management APIs** — Use Basic authentication with a service admin username and
      password (for example `admin` / `Passw0rd-Admin12`).

---

## Configuration

Set service admins in `gravitino.conf` (see also [Prerequisites](#prerequisites)):

| Configuration item                        | Description                                                              | Example                                     |
|-------------------------------------------|--------------------------------------------------------------------------|---------------------------------------------|
| `gravitino.authenticators`                | Must include `basic` when the built-in IdP plugin is enabled             | `basic`                                     |
| `gravitino.server.rest.extensionPackages` | Registers built-in IdP REST APIs                                         | `org.apache.gravitino.idp.web.rest.feature` |
| `gravitino.authorization.serviceAdmins`   | Comma-separated service admin that can call built-in IDP management APIs | `admin`                                     |

Example:

```properties
gravitino.authenticators = basic
gravitino.server.rest.extensionPackages = org.apache.gravitino.idp.web.rest.feature
gravitino.authorization.serviceAdmins = admin
```

---

## Operations

The following sections show how to call built-in IDP management APIs with `curl`. Replace
`localhost:8090`, usernames, and passwords with values that match your deployment. Examples use
Basic authentication with `admin` / `Passw0rd-Admin12` (from [Prerequisites](#prerequisites)).

**Base URL** — `http://<host>:<port>/api/idp`

**Common headers**

| Header         | Value                                        |
|----------------|----------------------------------------------|
| `Accept`       | `application/vnd.gravitino.v1+json`          |
| `Content-Type` | `application/json` (for POST and PUT bodies) |

Example:

```shell
curl -s -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Authorization: Basic $(echo -n 'admin:Passw0rd-Admin12' | base64)" \
  http://localhost:8090/api/idp/users/alice
```

### Password and username rules

Password rules apply to add-user, change-password, and `GRAVITINO_INITIAL_ADMIN_PASSWORD`:

| Rule            | Value                              |
|-----------------|------------------------------------|
| Username        | Required; must **not** contain `:` |
| Password length | 12–64 characters (inclusive)       |

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

For full request and response definitions, see the [Built-in IDP OpenAPI](../open-api/idp/openapi.yaml).

---

## Engines using Basic authentication

Configure Basic credentials on each engine connector. See
[Spark authentication](../spark-connector/spark-authentication-with-gravitino.md),
[Flink authentication](../flink-connector/flink-authentication-with-gravitino.md), and
[Trino authentication](../trino-connector/authentication.md) for full connector setup.

### Spark

```properties
spark.sql.gravitino.authType=basic
spark.sql.gravitino.basic.username=${username}
spark.sql.gravitino.basic.password=${password}
```

### Flink

```yaml
table.catalog-store.gravitino.gravitino.client.auth.type: basic
table.catalog-store.gravitino.gravitino.client.basic.username: ${username}
table.catalog-store.gravitino.gravitino.client.basic.password: ${password}
```

### Trino

In `etc/catalog/gravitino.properties`:

```properties
gravitino.client.authType=basic
gravitino.client.basic.username=${username}
gravitino.client.basic.password=${password}
```

---

## Engines using Basic authentication for Iceberg REST catalog

Connect Spark, Flink, and Trino directly to the Gravitino Iceberg REST (IRC) endpoint at
`http://<gravitino-host>:9001/iceberg/`. No Gravitino engine connector plugin is required.
Configure only the Basic auth properties below. See
[Connect Spark via Iceberg REST](../iceberg-rest-engine/spark.md),
[Connect Flink via Iceberg REST](../iceberg-rest-engine/flink.md), and
[Connect Trino via Iceberg REST](../iceberg-rest-engine/trino.md) for full IRC setup.

### Spark

```properties
spark.sql.catalog.<catalog-name>.rest.auth.type=basic
spark.sql.catalog.<catalog-name>.rest.auth.basic.username=${username}
spark.sql.catalog.<catalog-name>.rest.auth.basic.password=${password}
```

### Flink

```sql
'rest.auth.type' = 'basic',
'rest.auth.basic.username' = '${username}',
'rest.auth.basic.password' = '${password}'
```

### Trino

Requires Trino **481+**. Trino has no native Basic mode for Iceberg REST; pass
`Authorization` via HTTP headers.

Generate Base64 once:

```shell
echo -n '${username}:${password}' | base64
```

In `etc/catalog/<catalog-name>.properties`:

```properties
iceberg.rest-catalog.http-headers=Authorization: Basic <base64-credentials>
```

Replace `<base64-credentials>` with the output of `echo -n '${username}:${password}' | base64`.

---

## End-to-end setup

The following steps provision Gravitino and engines so a named user can connect with Basic
credentials. The examples use service admin `admin` / `Passw0rd-Admin12`, engine user
`alice` / `Passw0rd-Alice12`, metalake `example`, and Gravitino at `http://localhost:8090`.
Replace these with values that match your deployment.

### 1. Admin initialization

1. Append the following to `gravitino.conf` (see also [Prerequisites](#prerequisites) and
   [Configuration](#configuration)):

   ```properties
   gravitino.authenticators = basic
   gravitino.server.rest.extensionPackages = org.apache.gravitino.idp.web.rest.feature
   gravitino.authorization.enable = true
   gravitino.authorization.serviceAdmins = admin
   ```

   Built-in IdP is **incompatible** with the `simple` authenticator (the default),
   `gravitino.authenticators` must include `basic` and must not include `simple`.

2. Before the first start, set the initial service admin password (see
   [password rules](#password-and-username-rules)):

   ```shell
   export GRAVITINO_INITIAL_ADMIN_PASSWORD='Passw0rd-Admin12'
   ```

3. Start Gravitino and verify the service admin can call the API:

   ```shell
   curl -s -H "Accept: application/vnd.gravitino.v1+json" \
     -H "Authorization: Basic $(echo -n 'admin:Passw0rd-Admin12' | base64)" \
     http://localhost:8090/api/version
   ```

### 2. Create users

Create a built-in IdP user for each engine or operator account. Service admins call
`/api/idp/users` (see [Add a user](#add-a-user)):

```shell
curl -s -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'admin:Passw0rd-Admin12' | base64)" \
  -d '{"user":"alice","password":"Passw0rd-Alice12"}' \
  http://localhost:8090/api/idp/users
```

### 3. Create a metalake

Create a metalake with a service admin account. When authorization is enabled, only service admins
can create metalakes (see [Access control](access-control.md)). See also
[Manage metalakes](../manage-metalake-using-gravitino.md#create-a-metalake).

```shell
curl -s -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'admin:Passw0rd-Admin12' | base64)" \
  -d '{"name":"example","comment":"Basic auth example","properties":{}}' \
  http://localhost:8090/api/metalakes
```

Create catalogs in this metalake before engines can query data. See
[Manage relational metadata](../manage-relational-metadata-using-gravitino.md).

### 4. Add users to the metalake

Register the engine user in the metalake authorization namespace (see
[Add a user](access-control.md#add-a-user)). The username must match a built-in IdP user created in
step 2:

```shell
curl -s -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'admin:Passw0rd-Admin12' | base64)" \
  -d '{"name":"alice"}' \
  http://localhost:8090/api/metalakes/example/users
```

Grant roles or ownership as needed so the user can access catalogs and metadata. See
[Access control](access-control.md).

### 5. Configure engines to use Basic authentication

Configure each engine with `http://localhost:8090`, metalake `example`, and user `alice` /
`Passw0rd-Alice12` from step 2. Deploy the Gravitino connector for each engine first; see
[Spark authentication](../spark-connector/spark-authentication-with-gravitino.md),
[Flink authentication](../flink-connector/flink-authentication-with-gravitino.md), and
[Trino authentication](../trino-connector/authentication.md).

#### Spark

```shell
$SPARK_HOME/bin/spark-sql \
  --conf spark.plugins=org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin \
  --conf spark.sql.gravitino.uri=http://localhost:8090 \
  --conf spark.sql.gravitino.metalake=example \
  --conf spark.sql.gravitino.authType=basic \
  --conf spark.sql.gravitino.basic.username=alice \
  --conf spark.sql.gravitino.basic.password=Passw0rd-Alice12 \
  -e "SHOW CATALOGS;"
```

#### Flink

```shell
$FLINK_HOME/bin/sql-client.sh \
  -D table.catalog-store.kind=gravitino \
  -D table.catalog-store.gravitino.gravitino.uri=http://localhost:8090 \
  -D table.catalog-store.gravitino.gravitino.metalake=example \
  -D table.catalog-store.gravitino.gravitino.client.auth.type=basic \
  -D table.catalog-store.gravitino.gravitino.client.basic.username=alice \
  -D table.catalog-store.gravitino.gravitino.client.basic.password=Passw0rd-Alice12
```

#### Trino

Create `etc/catalog/gravitino.properties`:

```properties
connector.name=gravitino
gravitino.uri=http://localhost:8090
gravitino.metalake=example
gravitino.client.authType=basic
gravitino.client.basic.username=alice
gravitino.client.basic.password=Passw0rd-Alice12
```

Set `catalog.management=dynamic` in `etc/config.properties`, restart Trino, then verify:

```shell
$TRINO_HOME/bin/launcher restart
java -jar trino-cli.jar --server http://localhost:8080 --user alice \
  --execute "SHOW CATALOGS"
```

### 6. Access Iceberg REST catalog with Basic authentication

Connect engines directly to `http://localhost:9001/iceberg`. No Gravitino connector plugin is
required. Use user `alice` / `Passw0rd-Alice12` from step 2. See
[Connect Spark via Iceberg REST](../iceberg-rest-engine/spark.md),
[Connect Flink via Iceberg REST](../iceberg-rest-engine/flink.md), and
[Connect Trino via Iceberg REST](../iceberg-rest-engine/trino.md) for full IRC setup.

#### Spark

```shell
$SPARK_HOME/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.gravitino_irc=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.gravitino_irc.type=rest \
  --conf spark.sql.catalog.gravitino_irc.uri=http://localhost:9001/iceberg \
  --conf spark.sql.catalog.gravitino_irc.rest.auth.type=basic \
  --conf spark.sql.catalog.gravitino_irc.rest.auth.basic.username=alice \
  --conf spark.sql.catalog.gravitino_irc.rest.auth.basic.password=Passw0rd-Alice12 \
  -e "SHOW NAMESPACES IN gravitino_irc;"
```

#### Flink

```shell
cat > /tmp/flink-irc.sql <<'EOF'
CREATE CATALOG gravitino_irc WITH (
  'type'                     = 'iceberg',
  'catalog-type'             = 'rest',
  'uri'                      = 'http://localhost:9001/iceberg',
  'rest.auth.type'           = 'basic',
  'rest.auth.basic.username' = 'alice',
  'rest.auth.basic.password' = 'Passw0rd-Alice12'
);
USE CATALOG gravitino_irc;
SHOW DATABASES;
EOF

$FLINK_HOME/bin/sql-client.sh -f /tmp/flink-irc.sql
```

#### Trino

Requires Trino **481+**. Trino has no native Basic mode for Iceberg REST; pass `Authorization`
via HTTP headers.

```shell
echo -n 'alice:Passw0rd-Alice12' | base64
```

Create `etc/catalog/gravitino_irc.properties`:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://localhost:9001/iceberg
iceberg.rest-catalog.http-headers=Authorization: Basic YWxpc2U6UGFzc3cwcmQtQWxpY2UxMg==
```

Restart Trino and verify:

```shell
$TRINO_HOME/bin/launcher restart
java -jar trino-cli.jar --server http://localhost:8080 --user alice \
  --execute "SHOW SCHEMAS FROM gravitino_irc"
```

---

## Further reading

- [Built-in IDP OpenAPI](../open-api/idp/openapi.yaml) — API paths, bodies, and schemas
- [How to use HTTPS](how-to-use-https.md) — transport security for credentials
