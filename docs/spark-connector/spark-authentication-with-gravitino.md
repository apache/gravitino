---
title: "Spark Authentication"
slug: "/spark-connector/spark-authentication-with-gravitino"
keyword: "spark connector authentication oauth2 kerberos"
license: "This software is licensed under the Apache License version 2."
---

## Overview

The Spark connector supports `simple`, `oauth2`, and `kerberos` authentication when accessing the Gravitino server.

| Property                     | Type   | Default Value | Description                                                                                                         | Required | Since Version    |
|------------------------------|--------|---------------|---------------------------------------------------------------------------------------------------------------------|----------|------------------|
| spark.sql.gravitino.authType | string | `simple`      | The authentication mechanism for communicating with the Gravitino server. Supported values: `simple`, `oauth2`, `kerberos`. | No       | 0.7.0-incubating |

## Simple Mode

In simple mode, the username comes from Spark and is resolved in this order:

1. The `SPARK_USER` environment variable.
2. The `HADOOP_USER_NAME` environment variable.
3. The current OS user.

## OAuth2 Mode

In OAuth2 mode, use the following configuration to fetch an OAuth2 token for accessing the Gravitino server.

| Property                              | Type   | Default Value | Description                                   | Required             | Since Version    |
|---------------------------------------|--------|---------------|-----------------------------------------------|----------------------|------------------|
| spark.sql.gravitino.oauth2.serverUri  | string | None          | The OAuth2 server uri address.                | Yes, for OAuth2 mode | 0.7.0-incubating |
| spark.sql.gravitino.oauth2.tokenPath  | string | None          | The path of token interface in OAuth2 server. | Yes, for OAuth2 mode | 0.7.0-incubating |
| spark.sql.gravitino.oauth2.credential | string | None          | The credential to request the OAuth2 token.   | Yes, for OAuth2 mode | 0.7.0-incubating |
| spark.sql.gravitino.oauth2.scope      | string | None          | The scope to request the OAuth2 token.        | Yes, for OAuth2 mode | 0.7.0-incubating |

## Kerberos Mode

In Kerberos mode, use the Spark Kerberos configuration to fetch a Kerberos ticket for accessing the Gravitino server. Set `spark.kerberos.principal` and `spark.kerberos.keytab` to specify the principal and keytab.

The Gravitino server principal has the form `HTTP/$host@$realm`. Keep `$host` consistent with the host in the Gravitino server URI. Make sure `krb5.conf` is accessible from Spark, for example by setting `spark.driver.extraJavaOptions="-Djava.security.krb5.conf=/xx/krb5.conf"`.
