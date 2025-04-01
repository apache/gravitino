---
title: Authorization Push-down
slug: /security/authorization-push-down
keyword: security
license: "This software is licensed under the Apache License version 2."
---

## Authorization Push-down

![authorization push down](../assets/security/authorization-pushdown.png)

Gravitino offers a set of authorization frameworks
that integrate with various underlying data source permission systems,
such as MySQL's native permission management and Apache Ranger for big data.
These frameworks align with Gravitino's own authorization model and methodology.
Gravitino manages different data sources through Catalogs.
When a user performs an authorization operation on data within a catalog,
Gravitino invokes the Authorization Plugin module for that catalog.
This module translates Gravitino's authorization model
into the permission rules of the underlying data source.
The permissions are then enforced by the underlying permission system
via the respective client, such as JDBC or the Apache Ranger client.

### Ranger Hadoop SQL Plugin

In order to use the Ranger Hadoop SQL Plugin, you need to configure the following properties:

<table>
<thead>
<tr>
  <th>Property</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>authorization-provider</tt></td>
  <td>
    Providers to use to implement authorization plugin such as `ranger`.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>authorization.ranger.admin.url</tt></td>
  <td>The Apache Ranger web URIs.</td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>authorization.ranger.service.type</tt></td>
  <td>
    The Apache Ranger service type, Currently only supports `HadoopSQL` or `HDFS`
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>authorization.ranger.auth.type</tt></td>
  <td>
    The Apache Ranger authentication type `simple` or `kerberos`.
  </td>
  <td>`simple`</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>authorization.ranger.username</tt></td>
  <td>
    The Apache Ranger admin web login username (auth type=simple),
    or kerberos principal(auth type=kerberos).
    Need have Ranger administrator permission.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>authorization.ranger.password</tt></td>
  <td>
    The Apache Ranger admin web login user password (auth type=simple),
    or path of the keytab file(auth type=kerberos).
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>authorization.ranger.service.name</tt></td>
  <td>The Apache Ranger service name.</td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>authorization.ranger.service.create-if-absent</tt></td>
  <td>
    If this property is true and the Ranger service doesn't exist,
    Gravitino will create a Ranger service.
  </td>
  <td>false</td>
  <td>No</td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>authorization.ranger.jdbc.driverClassName</tt></td>
  <td>
    The property is used to specify driver class name when creating Ranger HadoopSQL service.
  </td>
  <td>`org.apache.hive.jdbc.HiveDrive`</td>
  <td>No</td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>authorization.ranger.jdbc.url</tt></td>
  <td>
    The property is used to specify JDBC URLwhen creating Ranger HadoopSQL service.
  </td>
  <td>`jdbc:hive2://127.0.0.1:8081`</td>
  <td>No</td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>authorization.ranger.hadoop.security.authentication</tt></td>
  <td>
    The property is used to specify Hadoop security authentication when creating Ranger HDFS service.
  </td>
  <td>`simple`</td>
  <td>No</td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>authorization.ranger.hadoop.rpc.protection</tt></td>
  <td>
    The property is used to specify Hadoop rpc protection when creating Ranger HDFS service.
  </td>
  <td>`authentication`</td>
  <td>No</td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>authorization.ranger.fs.default.name</tt></td>
  <td>
    The property is used to specify default filesystem when creating Ranger HDFS service.
  </td>
  <td>`hdfs://127.0.0.1:8090`</td>
  <td>No</td>
  <td>`0.9.0-incubating`</td>
</tr>
</tbody>
</table>

:::caution
The Gravitino Ranger authorization plugin only supports the Apache Ranger HadoopSQL Plugin and Apache Ranger HDFS Plugin.
:::

Once you have used the correct configuration, you can perform authorization operations
by calling Gravitino [authorization RESTful API](https://gravitino.apache.org/docs/latest/api/rest/grant-roles-to-a-user).

Gravitino will initially create three roles in Apache Ranger:

- <tt>GRAVITINO_METALAKE_OWNER_ROLE</tt>:
  Includes users and user groups designated as metalake owners,
  corresponding to the owner's privileges in Ranger policies.

- <tt>GRAVITINO_CATALOG_OWNER_ROLE</tt>:
  Includes users and user groups designated as catalog owners,
  corresponding to the owner's privileges in Ranger policies.

- <tt>GRAVITINO_OWNER_ROLE</tt>:
  Used to label Ranger policy items related to schema and table owner privileges.
  It does not include any users or user groups.

#### Example of using the Ranger Hadoop SQL Plugin

Suppose you have an Apache Hive service in your datacenter
and have created a `hiveRepo` in Apache Ranger to manage its permissions.
The Ranger service is accessible at `172.0.0.100:6080`,
with the username `Jack` and the password `PWD123`.
To add this Hive service to Gravitino using the Hive catalog,
you'll need to configure the following parameters.

```properties
authorization-provider=ranger
authorization.ranger.admin.url=172.0.0.100:6080
authorization.ranger.auth.type=simple
authorization.ranger.username=Jack
authorization.ranger.password=PWD123
authorization.ranger.service.type=HadoopSQL
authorization.ranger.service.name=hiveRepo
```

:::caution
Gravitino *0.8.0* only supports the authorization Apache Ranger Hive service,
Apache Iceberg service and Apache Paimon Service. 
Spark can use Kyuubi authorization plugin to access Gravitino's catalog.
But the plugin can't support to update or delete data for Paimon catalog.
More data source authorization is under development.
:::

### chain authorization plugin

Gravitino supports chaining multiple authorization plugins to secure one catalog.
The authorization plugin chain is defined in the `authorization.chain.plugins` property,
with the plugin names separated by commas.
When a user performs an authorization operation on data within a catalog,
the chained plugin will apply the authorization rules for every plugin defined in the chain.

In order to use the chained authorization plugin, you need to configure the following properties:

<table>
<thead>
<tr>
  <th>Property Name</th>
  <th>Description</th>
  <th>Default Value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>authorization-provider</tt></td>
  <td>
    Providers to use to implement authorization plugin such as `chain`.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>authorization.chain.plugins</tt></td>
  <td>
    The comma-separated list of plugin names, like `<plugin-name1>,<plugin-name2>,...`.
  </td>
  <td>(none)</td>
  <td>Yes if you use chain plugin</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>authorization.chain.&lt;plugin-name&gt;.ranger.admin.url</tt></td>
  <td>
    The Ranger authorization plugin properties for `<plugin-name>`.
  </td>
  <td>(none)</td>
  <td>Yes if you use chain plugin</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>authorization.chain.&lt;plugin-name&gt;.ranger.service.type</tt></td>
  <td>
    The Ranger authorization plugin properties for `<plugin-name>`.
  </td>
  <td>(none)</td>
  <td>Yes if you use chain plugin</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>authorization.chain.&lt;plugin-name&gt;.ranger.service.name</tt></td>
  <td>
    The Ranger authorization plugin properties for `<plugin-name>`.
  </td>
  <td>(none)</td>
  <td>Yes if you use chain plugin</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>authorization.chain.&lt;plugin-name&gt;.ranger.username</tt></td>
  <td>
    The Ranger authorization plugin properties for `<plugin-name>`.
  </td>
  <td>(none)</td>
  <td>Yes if you use chain plugin</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>authorization.chain.&lt;plugin-name&gt;.ranger.password</tt></td>
  <td>
    The Ranger authorization plugin properties for `<plugin-name>`.
  </td>
  <td>(none)</td>
  <td>Yes if you use chain plugin</td>
  <td>`0.8.0-incubating`</td>
</tr>
</tbody>
</table>

:::caution
The Gravitino chain authorization plugin only supports the Apache Ranger HadoopSQL Plugin and Apache Ranger HDFS Plugin.
The properties of every chained authorization plugin should be prefixed with `authorization.chain.<plugin-name>`.
:::

#### Example of using the chain authorization Plugin

Suppose you have an Apache Hive service in your datacenter,
and you have created a `hiveRepo` in Apache Ranger to manage its permissions.
The Apache Hive service will use HDFS to store its data.
You have created a `hdfsRepo` in Apache Ranger to manage HDFS's permissions.

```properties
authorization-provider=chain
authorization.chain.plugins=hive,hdfs
authorization.chain.hive.ranger.admin.url=http://ranger-service:6080
authorization.chain.hive.ranger.service.type=HadoopSQL
authorization.chain.hive.ranger.service.name=hiveRepo
authorization.chain.hive.ranger.auth.type=simple
authorization.chain.hive.ranger.username=Jack
authorization.chain.hive.ranger.password=PWD123
authorization.chain.hdfs.ranger.admin.url=http://ranger-service:6080
authorization.chain.hdfs.ranger.service.type=HDFS
authorization.chain.hdfs.ranger.service.name=hdfsRepo
authorization.chain.hdfs.ranger.auth.type=simple
authorization.chain.hdfs.ranger.username=Jack
authorization.chain.hdfs.ranger.password=PWD123
```

