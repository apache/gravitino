---
title: "Access Control"
slug: "/security/access-control"
keyword: "security"
license: "This software is licensed under the Apache License version 2."
---

## Overview

Apache Gravitino federates the catalogs of many systems under a single metalake, so permissions are
defined once there rather than separately in each system. When authorization is enabled, the server
checks every request before the operation runs and rejects it if the caller is not entitled to it.

Two things decide the answer:

- **Ownership** comes with creation. Whoever creates an object owns it, and owning it carries the
  right to alter it, drop it, and hand it to someone else. Ownership reaches down, so owning a
  catalog means administrative control over the schemas and tables inside it.
- **Privileges** are named permissions, each authorizing one kind of operation: `SELECT_TABLE` reads
  a table, `CREATE_SCHEMA` creates a schema in a catalog. A privilege is never given to a person
  directly. Privileges are collected into a role, and the role is granted to users and to groups.

Three rules govern how those apply:

- **Grants reach downward.** A grant covers everything beneath the object it is made on, both what
  exists now and what is created later. `SELECT_TABLE` on a schema covers every table in it.
- **Nothing is permitted unless granted.** A user added to a metalake and given nothing can see the
  metalake and nothing else.
- **An explicit deny overrides everything.** Each privilege in a role carries an `ALLOW` or a `DENY`
  condition, and a `DENY` beats an `ALLOW` held in any other role and at any other level of the
  hierarchy. A denial cannot be undone by granting something elsewhere, which makes `DENY` the way to
  carve one object out of a broad grant.

## Quick Start

Authorization is off by default. Turn it on in `${GRAVITINO_HOME}/conf/gravitino.conf`, name at least
one service administrator, and restart the server:

```properties
gravitino.authorization.enable = true
gravitino.authorization.serviceAdmins = {admin_user}
```

Service administrators are the only users who can create metalakes, and everything after that is done
through the API. The [Walkthrough](#walkthrough) runs a full sequence end to end, from an empty server
to a user with read access to one schema. [Server Configuration](#server-configuration) covers the
remaining settings, including how a caller's identity reaches the server.

## Authorization Model

### Principals and Objects

#### Users and Groups

A user must be added to a metalake before it can do anything there. A group is a set of users, and a
role granted to a group applies to every member, which is how access is usually managed for a team.

Both carry an optional `externalId` correlating them with an external identity provider. Users also
have an `enabled` flag, which suspends access without removing the user and defaults to `true` when
the user is created with an `externalId`.

#### Service Administrators

A service administrator is an ordinary user whose name appears in
`gravitino.authorization.serviceAdmins`. They are added to metalakes, granted roles, and made owners
like anyone else. The list adds exactly one ability, creating a metalake, and being server
configuration rather than a role it cannot be granted or revoked through the API. The check reads the
list alone, which is what lets the first metalake be created before any membership exists.

#### Objects

Everything Gravitino manages is an object with a type and a name. The name is the dotted path to it
below the metalake, so a table is `{catalog}.{schema}.{table}`, and requests identify an object by
both type and name, since the same name can exist at more than one type.

Access to an object is controlled by privileges, granted through roles, and by ownership. Ownership
behaves like a privilege that arrives with the object rather than one you grant, and it carries the
administrative rights, altering, dropping, and transferring, that no privilege name covers.

Everything sits under a metalake, but only the data objects nest below a catalog:

```
Metalake (top level)
├── Catalog (represents a data source)
│   └── Schema
│       ├── Table
│       ├── View
│       ├── Topic
│       ├── Fileset
│       ├── Model
│       └── Function
├── Tag
├── Policy
├── Job Template
├── Role
└── Job
```

Three things about that tree are worth noting:

- Roles and jobs are controlled by ownership alone, since no privilege binds to them, though
  `CREATE_ROLE` and `RUN_JOB` on the metalake gate creating them.
- Columns do not appear at all. They are reached through their table and carry no controls of their
  own, so there is no column-level grant in this model.
- Users and groups are not objects. They are the principals that privileges and ownership are
  assigned to.

### Grants

#### Privileges

A privilege authorizes a specific operation on an object, for example `SELECT_TABLE` or
`CREATE_SCHEMA`. Privileges are added to roles, and roles are granted to users and groups. Privileges
are never granted directly to a user.

#### Roles

A role is a named set of privileges, granted to users and groups. Privileges are never granted
directly to a user.

A role holds objects, and for each one a list of privileges, each carrying an `ALLOW` or `DENY`
condition. A privilege binds only to object types it supports, so `CREATE_TABLE` binds to a metalake,
catalog, or schema, never to a table. Whoever creates a role owns it, and can alter or delete it.

#### Ownership

Ownership can be held by a group as well as a user, in which case every member of that group holds
it, and it can be transferred at any time. It applies to metalakes, catalogs, schemas, tables, views,
topics, filesets, models, functions, roles, tags, policies, job templates, and jobs.

### Resolution

#### Evaluating a Request

Every authorized endpoint declares the conditions under which a caller may invoke it, and the check
passes if any one of them holds. Loading a table, for example, succeeds when:

- The caller owns the metalake or the catalog.
- The caller owns the schema and holds `USE_CATALOG`.
- The caller holds both `USE_CATALOG` and `USE_SCHEMA`, and additionally owns the table or holds
  `SELECT_TABLE` or `MODIFY_TABLE`.

Note the third case. Granting `SELECT_TABLE` on a schema covers every table in that schema, but on
its own it authorizes nothing, because the traversal privileges are still missing.

A failed check returns `403 Forbidden`. Some read paths return `404 Not Found` instead, so that a
caller cannot infer the existence of an object they are not entitled to see. List operations do not
fail; they return only the entries the caller is entitled to see.

#### Allow and Deny

`DENY` always wins. It beats an `ALLOW` in the same role, an `ALLOW` from any other role the user
holds, and an `ALLOW` at any other level of the hierarchy, in either direction: a `DENY` on a catalog
survives an `ALLOW` on its metalake, and a `DENY` on a metalake survives an `ALLOW` on its catalog.
So a denial cannot be circumvented by granting something elsewhere.

Sibling privileges are independent of each other. `DENY MODIFY_TABLE` leaves `ALLOW SELECT_TABLE`
intact, and the same holds for `PRODUCE_TOPIC` and `CONSUME_TOPIC`, and for `READ_FILESET` and
`WRITE_FILESET`. To withhold both read and write, deny both.

## Privileges and What They Allow

**Grantable On** lists the object types a privilege can be bound to, and the object it is bound to
sets the scope of the grant. Binding a privilege to a type not listed for it is rejected.

### Data Object Privileges

| Privilege            | Grantable On                        | What It Allows                                                     |
|----------------------|-------------------------------------|--------------------------------------------------------------------|
| `CREATE_CATALOG`     | Metalake                            | Create catalogs                                                    |
| `USE_CATALOG`        | Metalake, Catalog                   | Use any catalog in scope, and reach the objects inside it          |
| `CREATE_SCHEMA`      | Metalake, Catalog, Schema           | Create schemas or nested schemas in scope                          |
| `USE_SCHEMA`         | Metalake, Catalog, Schema           | Use any schema in scope, and reach the objects inside it           |
| `CREATE_TABLE`       | Metalake, Catalog, Schema           | Create tables in any schema in scope                               |
| `SELECT_TABLE`       | Metalake, Catalog, Schema, Table    | Read any table in scope                                            |
| `MODIFY_TABLE`       | Metalake, Catalog, Schema, Table    | Read and write to, and alter the schema of, any table in scope     |
| `CREATE_VIEW`        | Metalake, Catalog, Schema           | Create views in any schema in scope                                |
| `SELECT_VIEW`        | Metalake, Catalog, Schema, View     | Read any view in scope                                             |
| `CREATE_TOPIC`       | Metalake, Catalog, Schema           | Create topics in any schema in scope                               |
| `CONSUME_TOPIC`      | Metalake, Catalog, Schema, Topic    | Consume from any topic in scope                                    |
| `PRODUCE_TOPIC`      | Metalake, Catalog, Schema, Topic    | Consume from, produce to, and alter any topic in scope             |
| `CREATE_FILESET`     | Metalake, Catalog, Schema           | Create filesets in any schema in scope                             |
| `READ_FILESET`       | Metalake, Catalog, Schema, Fileset  | Read any fileset in scope                                          |
| `WRITE_FILESET`      | Metalake, Catalog, Schema, Fileset  | Read, write, and alter any fileset in scope                        |
| `REGISTER_MODEL`     | Metalake, Catalog, Schema           | Register models in any schema in scope                             |
| `LINK_MODEL_VERSION` | Metalake, Catalog, Schema, Model    | Link versions to any model in scope                                |
| `USE_MODEL`          | Metalake, Catalog, Schema, Model    | Read the metadata of, and download versions of, any model in scope |
| `REGISTER_FUNCTION`  | Metalake, Catalog, Schema           | Register functions in any schema in scope                          |
| `EXECUTE_FUNCTION`   | Metalake, Catalog, Schema, Function | Read the metadata of, and execute, any function in scope           |
| `MODIFY_FUNCTION`    | Metalake, Catalog, Schema, Function | Alter or drop any function in scope                                |

Either `SELECT_TABLE` or `MODIFY_TABLE` is enough to load a table's metadata, and the same pairing
holds for views, topics, and filesets.

`CREATE_MODEL` and `CREATE_MODEL_VERSION` are deprecated aliases for `REGISTER_MODEL` and
`LINK_MODEL_VERSION`. They resolve to identical authorization, so existing grants keep working, but
they will be removed in a future release. Use the current names in new roles.

### Governance and Administrative Privileges

| Privilege               | Grantable On                                                            | What It Allows                                     |
|-------------------------|-------------------------------------------------------------------------|----------------------------------------------------|
| `MANAGE_USERS`          | Metalake                                                                | Add and remove users                               |
| `MANAGE_GROUPS`         | Metalake                                                                | Add and remove groups                              |
| `CREATE_ROLE`           | Metalake                                                                | Create roles                                       |
| `MANAGE_GRANTS`         | Metalake, Catalog, Schema, Table, View, Topic, Fileset, Model, Function | Grant and revoke privileges on any object in scope |
| `CREATE_TAG`            | Metalake                                                                | Create tags                                        |
| `APPLY_TAG`             | Metalake, Tag                                                           | Attach tags to metadata objects                    |
| `CREATE_POLICY`         | Metalake                                                                | Create policies                                    |
| `APPLY_POLICY`          | Metalake, Policy                                                        | Attach policies to metadata objects                |
| `REGISTER_JOB_TEMPLATE` | Metalake                                                                | Register job templates                             |
| `USE_JOB_TEMPLATE`      | Metalake, JobTemplate                                                   | Run jobs from a job template                       |
| `RUN_JOB`               | Metalake                                                                | Run jobs                                           |

`MANAGE_GRANTS` bound to a metalake additionally allows granting and revoking roles for users and
groups across that metalake. Bound to anything else it covers privilege management only, on that
object and its descendants.

`APPLY_TAG`, `APPLY_POLICY`, and `USE_JOB_TEMPLATE` scope differently from every other privilege on
this page. The object they bind to is the instrument the holder may use, not the object the operation
acts on. Granting `APPLY_POLICY` on the policy `pii_masking` lets the holder attach that one policy
and no other, while granting it on the metalake lets them attach any policy in the metalake.

Attaching a tag or a policy is checked twice: the holder needs `APPLY_TAG` or `APPLY_POLICY` for the
tag or policy in question, and separately needs access to the metadata object being tagged. A user
cannot tag an object they could not otherwise reach.

### Required Privileges

Three rules apply throughout, so they are not repeated below:

- Owning the object, or any ancestor of it, satisfies any check on it. **Owner** in the tables means
  ownership is the only route, because no privilege grants that operation.
- Reaching an object inside a catalog and a schema also requires `USE_CATALOG` and `USE_SCHEMA`.
- A privilege counts whether it is held on the object itself or on any ancestor.

List operations never fail. They return the entries the caller is entitled to see, which for a
metalake owner is all of them.

#### Data Objects

| Object   | Create              | Load                                 | Alter             | Drop  |
|----------|---------------------|--------------------------------------|-------------------|-------|
| Catalog  | `CREATE_CATALOG`    | `USE_CATALOG`                        | Owner             | Owner |
| Schema   | `CREATE_SCHEMA`     | `USE_SCHEMA`                         | Owner             | Owner |
| Table    | `CREATE_TABLE`      | `SELECT_TABLE` or `MODIFY_TABLE`     | `MODIFY_TABLE`    | Owner |
| View     | `CREATE_VIEW`       | `SELECT_VIEW`                        | Owner             | Owner |
| Topic    | `CREATE_TOPIC`      | `CONSUME_TOPIC` or `PRODUCE_TOPIC`   | `PRODUCE_TOPIC`   | Owner |
| Fileset  | `CREATE_FILESET`    | `READ_FILESET` or `WRITE_FILESET`    | `WRITE_FILESET`   | Owner |
| Model    | `REGISTER_MODEL`    | `USE_MODEL`                          | Owner             | Owner |
| Function | `REGISTER_FUNCTION` | `EXECUTE_FUNCTION` or `MODIFY_FUNCTION` | `MODIFY_FUNCTION` | Owner |

Table statistics follow the table itself: reading them takes `SELECT_TABLE` or `MODIFY_TABLE`,
writing them takes `MODIFY_TABLE`. Model versions follow the model: `USE_MODEL` to read, owner to
alter or delete. Fetching a credential takes whatever loading the object takes.

Renaming a table or view into a different schema is the one operation needing a privilege on a second
object: the owner of the table or view, plus `CREATE_TABLE` or `CREATE_VIEW` on the target schema.

#### Metalake Objects

| Object       | Create                  | Read                                   | Alter or delete | Use                                       |
|--------------|-------------------------|----------------------------------------|-----------------|-------------------------------------------|
| Metalake     | Service administrator   | Membership                             | Owner           |                                           |
| User         | `MANAGE_USERS`          | `MANAGE_USERS`, or the user themselves | `MANAGE_USERS`  |                                           |
| Group        | `MANAGE_GROUPS`         | `MANAGE_GROUPS`, or a member           | `MANAGE_GROUPS` |                                           |
| Role         | `CREATE_ROLE`           | `MANAGE_GRANTS`, or a holder or owner  | Owner           | Grant or revoke: `MANAGE_GRANTS`          |
| Tag          | `CREATE_TAG`            | `APPLY_TAG`                            | Owner           | Attach: `APPLY_TAG` and access to the object |
| Policy       | `CREATE_POLICY`         | `APPLY_POLICY`                         | Owner           | Attach: `APPLY_POLICY` and access to the object |
| Job template | `REGISTER_JOB_TEMPLATE` | `USE_JOB_TEMPLATE`                     | Owner           | Run a job: `RUN_JOB` and `USE_JOB_TEMPLATE` |
| Job          |                         | Owner                                  | Owner           |                                           |

Granting or revoking a privilege on an object takes `MANAGE_GRANTS` on that object or an ancestor.
Granting or revoking a role, and overriding a role's privileges, takes `MANAGE_GRANTS` on the
metalake. Setting an owner takes ownership.

## Server Configuration

Settings live in `${GRAVITINO_HOME}/conf/gravitino.conf`. Authorization is off by default; the
[Quick Start](#quick-start) shows the two settings that turn it on.

| Setting *                        | Description                                                                                                 | Default  |
|----------------------------------|-------------------------------------------------------------------------------------------------------------|----------|
| `enable`                         | Enable or disable authorization                                                                             | `false`  |
| `serviceAdmins`                  | Comma-separated service administrator usernames. Required when `enable` is `true`                           | (none)   |
| `impl`                           | Metadata authorization implementation                                                                       | †        |
| `threadPoolSize`                 | Thread pool size for metadata authorization requests                                                        | `10`     |
| `jcasbin.cacheExpirationSecs`    | How long a cache entry stays valid. Lowering it reduces staleness and increases backend reads               | `3600`   |
| `jcasbin.roleCacheSize`          | Maximum size of each role-related cache. Applied to three caches, so real memory use is about 3x this value | `10000`  |
| `jcasbin.ownerCacheSize`         | Maximum size of the owner cache                                                                             | `100000` |
| `jcasbin.metadataIdCacheSize`    | Maximum size of the metadata name-to-ID cache                                                               | `100000` |
| `jcasbin.changePollIntervalSecs` | How often the server polls for entity and owner changes to invalidate its caches. Must be greater than zero | `3`      |

\* Setting names omit the leading `gravitino.authorization.` prefix. Write it out in full in the
configuration file:

```properties
gravitino.authorization.jcasbin.roleCacheSize = 10000
```

† The default is `org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer`.

Setting `gravitino.authorization.impl` to
`org.apache.gravitino.server.authorization.PassThroughAuthorizer` runs the server with authorization
enabled but every check bypassed. Pass-through mode exists for migration and is not intended for
production. See [Enabling Authorization on Existing Metalakes](#enabling-authorization-on-existing-metalakes).

The default authorizer keeps role and ownership information in Caffeine caches, so most authorization
decisions need no backend read. When privileges or ownership change through the Gravitino API, the
server handling the change invalidates the affected entries immediately. Other nodes in a multi-node
deployment pick the change up on their next poll, so a revocation can take up to
`jcasbin.changePollIntervalSecs` to take effect across the cluster. Lower the interval if that window
is unacceptable for your environment.

### Authentication

Authorization decides what a caller may do; authentication establishes who the caller is.
`gravitino.authenticators` selects the mechanism, and defaults to `simple`, which reads an
unvalidated HTTP Basic header and suits local evaluation only. For OAuth:

```properties
gravitino.authenticators = oauth
gravitino.authenticator.oauth.jwksUri = {jwks_uri}
gravitino.authenticator.oauth.serviceAudience = {audience}

# The JWT claims that become the Gravitino user name and group memberships
gravitino.authenticator.oauth.principalFields = preferred_username
gravitino.authenticator.oauth.groupsFields = groups
```

Two of those settings connect a token to this page:

- `principalFields` names the JWT claim whose value becomes the Gravitino user name, so it must
  produce the same strings you add to metalakes and grant roles to. It defaults to `sub`, which is
  usually an opaque provider ID rather than a name anyone would type.
- `groupsFields` names the claim supplying group membership, which is how a role granted to a group
  reaches a user.

See [How to Authenticate](how-to-authenticate.md) for Kerberos and the other options.

### Behavior Notes

- Add users to a metalake before creating metadata objects in it.
- If a request carries no user identity, the operation runs as the `anonymous` user.
- When authorization is enabled, the creator of a metalake is automatically added to it as a user.

### Enabling Authorization on Existing Metalakes

Metalakes created while `gravitino.authorization.enable` was `false` have no owner. Once full
authorization is enabled, operations on an ownerless metalake fail, so assign owners first.

**Step 1.** Enable authorization in pass-through mode, which turns on the authorization machinery
while bypassing the checks:

```properties
# Turn authorization on, but bypass every check while you assign owners
gravitino.authorization.enable = true
gravitino.authorization.serviceAdmins = {admin_user_1},{admin_user_2}
gravitino.authorization.impl = org.apache.gravitino.server.authorization.PassThroughAuthorizer
```

Restart the server.

**Step 2.** Set an owner for each existing metalake.

```shell
curl -X PUT \
  "$GRAVITINO/owners/metalake/{metalake}" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
  "name": "{admin_user_1}",
  "type": "USER"
}'
```

**Step 3.** Remove the `gravitino.authorization.impl` line so that the default authorizer takes over:

```properties
# The same settings with the pass-through line gone, so checks now apply
gravitino.authorization.enable = true
gravitino.authorization.serviceAdmins = {admin_user_1},{admin_user_2}
```

**Step 4.** Restart the server.

Confirm that every metalake has an owner before completing step 3. Any metalake left without one
becomes unusable when full authorization takes effect.
## Walkthrough

Three identities act in turn, from an empty server to a user reading one schema. The service
administrator bootstraps the metalake and hands it off, `manager` runs it, and `staff` builds and
shares the data. Each presents its own bearer token.

Every call sends the same two headers, so the examples use a helper:

```shell
GRAVITINO=http://localhost:8090/api/metalakes/{metalake}

gravitino() {
  curl -sS -H "Accept: application/vnd.gravitino.v1+json" \
       -H "Content-Type: application/json" "$@"
}
```

**1. The service administrator creates the metalake and hands it over.** Creating it adds the creator
as a user and makes them owner, which is what authorizes the next two calls. After the transfer,
`manager` owns the metalake and the service administrator has no further part to play.

```shell
gravitino -X POST "http://localhost:8090/api/metalakes" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{"name": "{metalake}", "comment": "example metalake", "properties": {}}'

gravitino -X POST "$GRAVITINO/users" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{"name": "manager"}'

gravitino -X PUT "$GRAVITINO/owners/metalake/{metalake}" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{"name": "manager", "type": "USER"}'
```

**2. `manager` delegates catalog creation to `staff`.** Owning the metalake lets `manager` add users
and create roles without any grant. The role carries `CREATE_CATALOG` on the metalake, so it covers
every catalog `staff` creates, now and later.

```shell
gravitino -X POST "$GRAVITINO/users" \
  -H "Authorization: Bearer $MANAGER_TOKEN" \
  -d '{"name": "staff"}'

gravitino -X POST "$GRAVITINO/roles" \
  -H "Authorization: Bearer $MANAGER_TOKEN" \
  -d '{
  "name": "catalog_manager",
  "properties": {},
  "securableObjects": [
    {
      "fullName": "{metalake}",
      "type": "METALAKE",
      "privileges": [{"name": "CREATE_CATALOG", "condition": "ALLOW"}]
    }
  ]
}'

gravitino -X PUT "$GRAVITINO/permissions/users/staff/grant" \
  -H "Authorization: Bearer $MANAGER_TOKEN" \
  -d '{"roleNames": ["catalog_manager"]}'
```

**3. `staff` builds out the data.** Creating the catalog makes `staff` its owner, and that ownership
carries everything inside it, so the schema needs no further grant. The example uses Hive; any
provider works, with its own properties.

```shell
gravitino -X POST "$GRAVITINO/catalogs" \
  -H "Authorization: Bearer $STAFF_TOKEN" \
  -d '{
  "name": "{catalog}",
  "type": "RELATIONAL",
  "provider": "hive",
  "properties": {"metastore.uris": "thrift://{hive_host}:9083"}
}'

gravitino -X POST "$GRAVITINO/catalogs/{catalog}/schemas" \
  -H "Authorization: Bearer $STAFF_TOKEN" \
  -d '{"name": "{schema}"}'
```

**4. `staff` gives `analyst` read access.** Reading one schema takes three privileges: `USE_CATALOG`
and `USE_SCHEMA` to reach it, and `SELECT_TABLE` to read what is in it. Object names are not
validated, so a typo produces a role that grants nothing. `analyst` must already be a user in the
metalake.

```shell
gravitino -X POST "$GRAVITINO/roles" \
  -H "Authorization: Bearer $STAFF_TOKEN" \
  -d '{
  "name": "schema_reader",
  "properties": {},
  "securableObjects": [
    {
      "fullName": "{catalog}",
      "type": "CATALOG",
      "privileges": [{"name": "USE_CATALOG", "condition": "ALLOW"}]
    },
    {
      "fullName": "{catalog}.{schema}",
      "type": "SCHEMA",
      "privileges": [
        {"name": "USE_SCHEMA", "condition": "ALLOW"},
        {"name": "SELECT_TABLE", "condition": "ALLOW"}
      ]
    }
  ]
}'

gravitino -X PUT "$GRAVITINO/permissions/users/analyst/grant" \
  -H "Authorization: Bearer $STAFF_TOKEN" \
  -d '{"roleNames": ["schema_reader"]}'
```

`analyst` can now read every table in `{catalog}.{schema}`, including tables created there later, and
can do nothing anywhere else in the metalake. Queries through the Gravitino connector are authorized
against the same ownership and privileges that governed the metadata calls above.

## Endpoints

Paths are relative to `http://localhost:8090/api/metalakes/{metalake}`. For request and response
schemas, see the [Gravitino REST API](https://gravitino.apache.org/docs/latest/api/rest/gravitino-rest-api).

Users, groups, and roles share one shape. Substitute `users`, `groups`, or `roles` for
`{collection}`, and the user, group, or role name for `{name}`:

| Operation | Method   | Path                  |
|-----------|----------|-----------------------|
| Create    | `POST`   | `/{collection}`       |
| List      | `GET`    | `/{collection}`       |
| Get       | `GET`    | `/{collection}/{name}` |
| Delete    | `DELETE` | `/{collection}/{name}` |

Add `?details=true` to a list path to get full objects instead of names.

The rest are one of a kind:

| Operation                          | Method       | Path                                                            |
|------------------------------------|--------------|-----------------------------------------------------------------|
| Grant privileges to a role         | `PUT`        | `/permissions/roles/{role}/{object_type}/{object_name}/grant`   |
| Revoke privileges from a role      | `PUT`        | `/permissions/roles/{role}/{object_type}/{object_name}/revoke`  |
| Replace a role's privileges        | `PUT`        | `/permissions/roles/{role}/`                                    |
| Grant roles to a user or group     | `PUT`        | `/permissions/{collection}/{name}/grant`                        |
| Revoke roles from a user or group  | `PUT`        | `/permissions/{collection}/{name}/revoke`                       |
| List the roles bound to an object  | `GET`        | `/objects/{object_type}/{object_name}/roles`                    |
| Get or set an object's owner       | `GET`, `PUT` | `/owners/{object_type}/{object_name}`                           |

Replacing a role's privileges is destructive: afterwards the role holds exactly what the request body
contains, and any object absent from it is dropped.

### Java Client

Most calls map directly onto a `GravitinoClient` method and are covered by the
[Java doc](https://gravitino.apache.org/docs/latest/api/java/org/apache/gravitino/client/GravitinoClient.html).
Three take arguments that are hard to derive from the signature alone.

A role's objects are built as a nested path rather than a dotted string:

```java
SecurableObject table =
    SecurableObjects.ofTable(
        SecurableObjects.ofSchema(
            SecurableObjects.ofCatalog("catalog1", Collections.emptyList()),
            "schema1",
            Collections.emptyList()),
        "table1",
        Lists.newArrayList(Privileges.SelectTable.allow()));

Role role = client.createRole("schema_reader", ImmutableMap.of(), Lists.newArrayList(table));
```

Privileges are passed as a `Set`, and each carries its condition. The `List` overloads of these two
methods are deprecated:

```java
MetadataObject schema =
    MetadataObjects.of(Lists.newArrayList("catalog1", "schema1"), MetadataObject.Type.SCHEMA);

client.grantPrivilegesToRole("schema_reader", schema, ImmutableSet.of(Privileges.SelectTable.allow()));
client.revokePrivilegesFromRole("schema_reader", schema, ImmutableSet.of(Privileges.SelectTable.deny()));
```

An owner is set with an `Owner.Type`, not a string:

```java
client.setOwner(schema, "analyst", Owner.Type.USER);
```

## Related

- [Gravitino REST API](https://gravitino.apache.org/docs/latest/api/rest/gravitino-rest-api), for the
  request and response schemas of every user, group, role, permission, and owner endpoint
- [Java Client](../how-to-use-gravitino-client.md) and
  [Python Client](../how-to-use-python-client.md), for calling those endpoints from a client library
- [`org.apache.gravitino.authorization`](https://gravitino.apache.org/docs/latest/api/java/org/apache/gravitino/authorization/package-summary.html),
  the Java classes behind this page: `Privilege`, `Privileges`, `SecurableObject`, and `Owner`
- [Authorization Pushdown](authorization-pushdown.md), for pushing enforcement down to the underlying
  data source or to an external system such as Apache Ranger
- [How to Authenticate](how-to-authenticate.md), for establishing who the caller is
- [Local users and groups](local-users-and-groups.md)
