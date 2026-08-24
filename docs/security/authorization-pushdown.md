---
title: "Authorization Pushdown"
slug: "/security/authorization-pushdown"
keywords:
  - security
  - authorization
  - ranger
license: "This software is licensed under the Apache License version 2."
---

## Overview

Authorization pushdown applies a grant made in Gravitino to Apache Ranger, so the permission is enforced where the data is rather than only inside Gravitino. An engine that reads the table directly is still subject to it.

Pushdown is configured per catalog with the `authorization-provider` property. Set it to `ranger` when the catalog's permissions live in a single Ranger service, or to `chain` when one catalog needs its grants applied in more than one. A catalog without the property manages access in Gravitino alone.

Gravitino resolves which catalog holds the object being granted on, hands the operation to that catalog's plugin, and the plugin maps the Gravitino privilege onto Ranger's model and writes it through the Ranger admin REST API. The plugin interface is not specific to Ranger, so other permission systems can be added, and Ranger is what ships today.

Once a catalog is configured, grants are made through the ordinary [authorization REST API](https://gravitino.apache.org/docs/latest/api/rest/grant-role-to-user). No separate pushdown call exists.

## Ranger

The Ranger provider covers two Ranger service types. `HadoopSQL` governs schemas, tables, and columns for the Hive, Iceberg, and Paimon catalogs. `HDFS` governs paths, which is what a fileset catalog needs. A catalog selects one of them with `authorization.ranger.service.type`.

Spark reaches these catalogs through the Kyuubi authorization plugin. That plugin cannot push updates or deletes for a Paimon catalog.

### Configuration

| Property Name                                         | Description                                                                                                    | Default Value                     |
|-------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|-----------------------------------|
| `authorization-provider`                              | Set to `ranger` to push grants into Apache Ranger                                                              | (none)                            |
| `authorization.ranger.admin.url`                      | The Ranger admin web URI                                                                                       | (none)                            |
| `authorization.ranger.service.type`                   | `HadoopSQL` or `HDFS`                                                                                          | (none)                            |
| `authorization.ranger.service.name`                   | The Ranger service to write policies into                                                                      | (none)                            |
| `authorization.ranger.auth.type`                      | `simple` or `kerberos`                                                                                         | `simple`                          |
| `authorization.ranger.username`                       | Ranger admin login username, or the Kerberos principal. Requires Ranger administrator permission               | (none)                            |
| `authorization.ranger.password`                       | Ranger admin login password, or the path to the keytab file                                                    | (none)                            |
| `authorization.ranger.service.create-if-absent`       | Creates the Ranger service when it does not already exist                                                      | `false`                           |

The remaining properties apply only when `create-if-absent` is `true`, since they describe the service Gravitino creates.

| Property Name                                         | Description                                                              | Default Value                     |
|-------------------------------------------------------|--------------------------------------------------------------------------|-----------------------------------|
| `authorization.ranger.jdbc.driverClassName`           | Driver class for a new HadoopSQL service                                 | `org.apache.hive.jdbc.HiveDriver` |
| `authorization.ranger.jdbc.url`                       | JDBC URL for a new HadoopSQL service                                     | `jdbc:hive2://127.0.0.1:8081`     |
| `authorization.ranger.hadoop.security.authentication` | Hadoop security authentication for a new HDFS service                    | `simple`                          |
| `authorization.ranger.hadoop.security.authorization`  | Hadoop security authorization for a new HDFS service                     | (none)                            |
| `authorization.ranger.hadoop.rpc.protection`          | Hadoop RPC protection for a new HDFS service                             | `authentication`                  |
| `authorization.ranger.fs.default.name`                | Default filesystem for a new HDFS service                                | `hdfs://127.0.0.1:8090`           |

### Example

A Hive service is already managed by a Ranger service named `hiveRepo`, and Ranger is reachable at `172.0.0.100:6080`. Adding that Hive service to Gravitino as a Hive catalog with pushdown enabled takes the following catalog properties.

```properties
authorization-provider=ranger
authorization.ranger.admin.url=172.0.0.100:6080
authorization.ranger.auth.type=simple
authorization.ranger.username={ranger_admin_user}
authorization.ranger.password={ranger_admin_password}
authorization.ranger.service.type=HadoopSQL
authorization.ranger.service.name=hiveRepo
```

### Roles Gravitino Creates

Gravitino creates the following managed roles in Ranger and manages their membership itself, so treat them as owned by Gravitino rather than editing them in the Ranger UI.

| Role                            | Purpose                                                                                          |
|---------------------------------|---------------------------------------------------------------------------------------------------|
| `GRAVITINO_METALAKE_OWNER_ROLE_<metalake_id>` | Holds the user or group that owns the identified metalake, carrying owner privileges in Ranger policies |
| `GRAVITINO_CATALOG_OWNER_ROLE_<catalog_id>` | Holds the user or group that owns the identified catalog, carrying owner privileges in Ranger policies |
| `GRAVITINO_OWNER_ROLE`          | Labels the policy items covering schema and table owner privileges, and holds no members          |

### Migrate Owner Roles from 1.x to 2.0

Gravitino 1.x used the shared Ranger roles `GRAVITINO_METALAKE_OWNER_ROLE` and
`GRAVITINO_CATALOG_OWNER_ROLE`. If multiple catalogs wrote to the same Ranger service, membership
in either shared role could give an owner access outside the object they owned. Gravitino 2.0 uses
the ID-suffixed roles in the table above, but it does not migrate existing Ranger state
automatically.

After upgrading the Gravitino server, reconcile every existing metalake and Ranger-enabled catalog
by setting its current owner again. Setting the same owner is safe and idempotent. It creates the
ID-suffixed role, grants that role to the current owner, and replaces the legacy shared role in the
matching Ranger policies. Get the current owner before sending the `PUT` request; do not infer it
from membership in a legacy shared role because that role can contain owners of several catalogs.

The following example reconciles the owner of catalog `sales` in metalake `prod`:

```shell
curl -s \
  -H "Accept: application/vnd.gravitino.v1+json" \
  "http://<gravitino-host>:8090/api/metalakes/prod/owners/CATALOG/sales"

curl -X PUT \
  -H "Content-Type: application/json" \
  -d '{"name":"<current-owner-name>","type":"<USER-or-GROUP>"}' \
  "http://<gravitino-host>:8090/api/metalakes/prod/owners/CATALOG/sales"
```

Repeat the same process for each catalog that has Ranger authorization enabled. Reconcile each
metalake as well, using `METALAKE/<metalake-name>` in place of `CATALOG/<catalog-name>`. A metalake
owner update is applied through every authorization-enabled catalog in that metalake.

Keep the two legacy shared roles until all objects have been reconciled. Then use Ranger Admin to:

1. Verify that no policy refers to the exact role names `GRAVITINO_METALAKE_OWNER_ROLE` or
   `GRAVITINO_CATALOG_OWNER_ROLE`.
2. Verify that every `GRAVITINO_METALAKE_OWNER_ROLE_<metalake_id>` and
   `GRAVITINO_CATALOG_OWNER_ROLE_<catalog_id>` contains the expected owner, and test access for two
   catalogs that share a Ranger service.
3. Delete the two legacy shared roles. Do not delete the ID-suffixed roles or
   `GRAVITINO_OWNER_ROLE`.

## Chaining Plugins

One catalog often needs permissions applied in more than one place. A Hive catalog storing its data on HDFS needs both the table grant in the HadoopSQL service and the corresponding path grant in the HDFS service, or an engine reading the files directly bypasses the table permission.

The `chain` provider handles this. Set `authorization.chain.plugins` to a comma-separated list of names you choose, then configure each named plugin with `authorization.chain.{plugin_name}` as its property prefix. Every plugin in the chain is applied on each authorization operation.

| Property Name                                            | Description                                                    |
|----------------------------------------------------------|-----------------------------------------------------------------|
| `authorization-provider`                                 | Set to `chain` to apply several plugins to this catalog         |
| `authorization.chain.plugins`                            | Comma-separated plugin names, each naming a prefix below        |
| `authorization.chain.{plugin_name}.ranger.admin.url`     | The admin URI for that plugin                                   |
| `authorization.chain.{plugin_name}.ranger.service.type`  | `HadoopSQL` or `HDFS` for that plugin                           |
| `authorization.chain.{plugin_name}.ranger.service.name`  | The Ranger service for that plugin                              |
| `authorization.chain.{plugin_name}.ranger.username`      | The Ranger admin login username for that plugin                 |
| `authorization.chain.{plugin_name}.ranger.password`      | The Ranger admin login password for that plugin                 |

The names in `authorization.chain.plugins` are labels rather than plugin types, so any name works as long as it matches the prefix used by its properties. Every plugin in a chain is a Ranger plugin, since Ranger is the only provider available to chain.

### Example

The Hive service is managed by the Ranger service `hiveRepo` and its underlying HDFS storage by `hdfsRepo`. Chaining the two keeps the table grant and the path grant in step.

```properties
authorization-provider=chain
authorization.chain.plugins=hive,hdfs
authorization.chain.hive.ranger.admin.url=http://ranger-service:6080
authorization.chain.hive.ranger.service.type=HadoopSQL
authorization.chain.hive.ranger.service.name=hiveRepo
authorization.chain.hive.ranger.auth.type=simple
authorization.chain.hive.ranger.username={ranger_admin_user}
authorization.chain.hive.ranger.password={ranger_admin_password}
authorization.chain.hdfs.ranger.admin.url=http://ranger-service:6080
authorization.chain.hdfs.ranger.service.type=HDFS
authorization.chain.hdfs.ranger.service.name=hdfsRepo
authorization.chain.hdfs.ranger.auth.type=simple
authorization.chain.hdfs.ranger.username={ranger_admin_user}
authorization.chain.hdfs.ranger.password={ranger_admin_password}
```

## Further Reading

- [Access Control](access-control.md) for the Gravitino privilege model that pushdown translates from
- [Authorization REST API](https://gravitino.apache.org/docs/latest/api/rest/grant-role-to-user) for making the grants
