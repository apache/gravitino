---
title: "Spark authentication with Gravitino server"
slug: /spark-connector/spark-authentication
keyword: spark connector authentication oauth2 kerberos
license: "This software is licensed under the Apache License version 2."
---

## Overview

Spark connector supports `simple` `oauth2` and `kerberos` authentication when accessing Gravitino server.

| Property                     | Type   | Default Value | Description                                                                                                         | Required | Since Version    |
|------------------------------|--------|---------------|---------------------------------------------------------------------------------------------------------------------|----------|------------------|
| spark.sql.gravitino.authType | string | `simple`      | The authentication mechanisms when communicating with Gravitino server, supports `simple`, `oauth2` and `kerberos`. | No       | 0.7.0-incubating |

## Simple mode

In the simple mode, the username originates from Spark, and is obtained using the following sequences:
1. The environment variable of `SPARK_USER`
2. The environment variable of `HADOOP_USER_NAME`
3. The user login in the machine

## OAuth2 mode

In the OAuth2 mode, you could use the following configuration to fetch an OAuth2 token to access Gravitino server.

| Property                              | Type   | Default Value | Description                                   | Required             | Since Version    |
|---------------------------------------|--------|---------------|-----------------------------------------------|----------------------|------------------|
| spark.sql.gravitino.oauth2.serverUri  | string | None          | The OAuth2 server uri address.                | Yes, for OAuth2 mode | 0.7.0-incubating |
| spark.sql.gravitino.oauth2.tokenPath  | string | None          | The path of token interface in OAuth2 server. | Yes, for OAuth2 mode | 0.7.0-incubating |
| spark.sql.gravitino.oauth2.credential | string | None          | The credential to request the OAuth2 token.   | Yes, for OAuth2 mode | 0.7.0-incubating |
| spark.sql.gravitino.oauth2.scope      | string | None          | The scope to request the OAuth2 token.        | Yes, for OAuth2 mode | 0.7.0-incubating |

## Kerberos mode

In kerberos mode, you could use the Spark kerberos configuration to fetch a kerberos ticket to access Gravitino server, use `spark.kerberos.principal`, `spark.kerberos.keytab` to specify kerberos principal and keytab.

The principal of Gravitino server is like `HTTP/$host@$realm`, please keep the `$host` consistent with the host in Gravitino server uri address.
Please make sure `krb5.conf` is accessible by Spark, like by specifying the configuration `spark.driver.extraJavaOptions="-Djava.security.krb5.conf=/xx/krb5.conf"`.
