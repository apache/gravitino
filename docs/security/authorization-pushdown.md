---
title: "Permission System"
slug: /security/permission-system
keyword: security
license: "This software is licensed under the Apache License version 2."
---

## Authorization Push-down

![authorization push down](../assets/security/authorization-pushdown.png)

Gravitino also provides a set of authorization frameworks to interface with different underlying data source
permission systems (e.g., MySQL's own permission management and the Apache Ranger permission management system for big data)
in accordance with its own authorization model and methodology.
On top of this, Gravitino manages different underlying data sources through Catalogs.
When a user performs an authorization operation on the data in a Catalog, Gravitino will call the interface of the Authorization Plugin module in the respective Catalog to translate the Gravitino authorization model into the underlying data source's permission rules of the underlying data source.
Permission is then pushed down to the underlying permission system through the client of the underlying data source (JDBC or Apache Ranger client, etc.).

### Authorization Ranger Hive Plugin properties

The Authorization Ranger Hive Plugin extends the following properties in the [Apache Hive catalog properties](../apache-hive-catalog.md#Catalog properties):

| Property Name                       | Description                                                                                                                                          | Default Value | Required | Since Version |
|-------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `authorization-provider`            | This variable is used as a key in properties of catalogs to use authorization provider in Gravitino. Such as `ranger`.                               | (none)        | No       | 0.6.0         |
| `authorization.ranger.admin.url`    | The Apache Ranger web URIs.                                                                                                                          | (none)        | No       | 0.6.0         |
| `authorization.ranger.auth.type`    | The Apache Ranger authentication type `simple` or `kerberos`.                                                                                        | `simple`      | No       | 0.6.0         |
| `authorization.ranger.username`     | The Apache Ranger admin web login username (auth type=simple), or kerberos principal(auth type=kerberos), Need have Ranger administrator permission. | (none)        | No       | 0.6.0         |
| `authorization.ranger.password`     | The Apache Ranger admin web login user password (auth type=simple), or path of the keytab file(auth type=kerberos)                                   | (none)        | No       | 0.6.0         |
| `authorization.ranger.service.name` | The Apache Ranger service name.                                                                                                                      | (none)        | No       | 0.6.0         |

Once you have used the correct configuration, you can perform authorization operations by calling gravitino's [authorization RESTful API](https://datastrato.ai/docs/latest/api/rest/grant-roles-to-a-user) interface.

#### Example of using the Authorization Ranger Hive Plugin

Suppose you have an Apache Hive service in your datacenter, and you have created a `hiveRepo` in Apache Ranger to manage the permissions of this Apache Hive service.
Assume that the service IP address and port of this Apache Ranger is `172.0.0.100:6080`, and the username with Ranger administrator privileges is `Jack` and the password is `PWD123`.
First, you need to add this Hive service to Gravitino using hive catalog, and you need to configure the following parameters.

```properties
authorization-provider=ranger
authorization.ranger.admin.url=172.0.0.100:6080
authorization.ranger.auth.type=simple
authorization.ranger.username=Jack
authorization.ranger.password=PWD123
authorization.ranger.service.name=hiveRepo
```

:::caution
The Gravitino 0.6.0 version only supports the authorization Apache Ranger Hive service. We plan to support more data source authorization in the future.
:::
