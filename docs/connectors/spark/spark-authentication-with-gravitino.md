---
title: "Spark authentication with Gravitino server"
slug: /spark-connector/spark-authentication
keyword: spark connector authentication oauth2 kerberos
license: "This software is licensed under the Apache License version 2."
---

## Overview

Spark connector supports `simple`, `oauth2` and `kerberos` authentication
when accessing the Gravitino server.

<table>
<thead>
<tr>
  <th>Property</th>
  <th>Type</th>
  <th>Default value</th>
  <th>Description</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>spark.sql.gravitino.authType</tt></td>
  <td><tt>string</tt></td>
  <td>`simple`</td>
  <td>
    The authentication mode when communicating with the Gravitino server.
    The valid values are `simple`, `oauth2` or `kerberos`.
  </td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
</tbody>
</table>

## Simple mode

In the simple mode, the username is determined in the following order:

1. The environment variable of `SPARK_USER`
1. The environment variable of `HADOOP_USER_NAME`
1. The current OS user

## OAuth2 mode

In the OAuth2 mode, you can use the following configuration
to fetch an OAuth2 token for accessing the Gravitino server.

<table>
<thead>
<tr>
  <th>Property</th>
  <th>Type</th>
  <th>Default value</th>
  <th>Description</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>spark.sql.gravitino.oauth2.serverUri</tt></td>
  <td><tt>string</tt></td>
  <td>(none)</td>
  <td>
    The URI for the OAuth2 server.
    This property is required when OAuth2 is used.
  </td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>spark.sql.gravitino.oauth2.tokenPath</tt></td>
  <td><tt>string</tt></td>
  <td>(none)</td>
  <td>
    The path for token interface in OAuth2 server.
    This property is required when OAuth2 is used.
  </td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>spark.sql.gravitino.oauth2.credential</tt></td>
  <td><tt>string</tt></td>
  <td>(none)</td>
  <td>
    The credential for requesting an OAuth2 token.
    This property is required when OAuth2 is used.
  </td>
  <td>Yes</td>
  <td>` 0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>spark.sql.gravitino.oauth2.scope</tt></td>
  <td><tt>string</tt></td>
  <td>(none)</td>
  <td>
   The scope to use when requesting an OAuth2 token.
    This property is required when OAuth2 is used.
  </td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
</tbody>
</table>

## Kerberos mode

In kerberos mode, you can use the Spark kerberos configuration
to fetch a kerberos ticket for accessing Gravitino server.
The following properties are used to specify the Kerberos information:

- `spark.kerberos.principal`
- `spark.kerberos.keytab`

The principal of Gravitino server is like `HTTP/$host@$realm`.
The `$host` must be identical to the host in the Gravitino server URI.
You need to ensure that the `krb5.conf` file is accessible by Spark.
You can provide this information  by specifying the configuration
`spark.driver.extraJavaOptions="-Djava.security.krb5.conf=/xx/krb5.conf"`.

