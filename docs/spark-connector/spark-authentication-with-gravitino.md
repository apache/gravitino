---
title: "Spark Authentication"
slug: "/spark-connector/spark-authentication"
keyword: "spark connector authentication basic oauth2 kerberos"
license: "This software is licensed under the Apache License version 2."
---

## Overview

Spark connector supports `simple`, `basic`, `oauth2`, and `kerberos` authentication when accessing Gravitino server.

| Property                       | Type     | Default Value   | Description                                                                                                                          | Required   | Since Version      |
|--------------------------------|----------|-----------------|--------------------------------------------------------------------------------------------------------------------------------------|------------|--------------------|
| spark.sql.gravitino.authType   | string   | `simple`        | The authentication mechanism for communicating with the Gravitino server. Supported values: `simple`, `basic`, `oauth2`, `kerberos`. | No         | 0.7.0-incubating   |

## Simple Mode

In the simple mode, the username originates from Spark, and is obtained using the following sequences:
1. The environment variable of `SPARK_USER`
2. The environment variable of `HADOOP_USER_NAME`
3. The user login in the machine

## Basic Mode

In Basic mode, the Spark connector authenticates to the Gravitino server using HTTP Basic credentials
against the built-in IDP. The Gravitino server must have Basic authentication enabled. See
[How to authenticate](../security/how-to-authenticate.md#basic-mode) for server-side setup.

| Property                                | Type     | Default Value   | Description                                     | Required               | Since Version      |
|-----------------------------------------|----------|-----------------|-------------------------------------------------|------------------------|--------------------|
| spark.sql.gravitino.authType            | string   | `simple`        | Set to `basic` to enable Basic authentication.  | Yes, for Basic mode    | 1.3.0              |
| spark.sql.gravitino.basic.username      | string   | (none)          | The built-in IDP username.                      | Yes, for Basic mode    | 1.3.0              |
| spark.sql.gravitino.basic.password      | string   | (none)          | The built-in IDP password.                      | Yes, for Basic mode    | 1.3.0              |

### Basic Configuration Example

```properties
spark.plugins=org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin
spark.sql.gravitino.uri=http://localhost:8090
spark.sql.gravitino.metalake=my_metalake
spark.sql.gravitino.authType=basic
spark.sql.gravitino.basic.username=admin
spark.sql.gravitino.basic.password=YourSecureGravitinoPassword
```

## OAuth2 Mode

In the OAuth2 mode, you could use the following configuration to fetch an OAuth2 token to access Gravitino server.

| Property                                | Type     | Default Value   | Description                                     | Required               | Since Version      |
|-----------------------------------------|----------|-----------------|-------------------------------------------------|------------------------|--------------------|
| spark.sql.gravitino.oauth2.serverUri    | string   | None            | The OAuth2 server uri address.                  | Yes, for OAuth2 mode   | 0.7.0-incubating   |
| spark.sql.gravitino.oauth2.tokenPath    | string   | None            | The path of token interface in OAuth2 server.   | Yes, for OAuth2 mode   | 0.7.0-incubating   |
| spark.sql.gravitino.oauth2.credential   | string   | None            | The credential to request the OAuth2 token.     | Yes, for OAuth2 mode   | 0.7.0-incubating   |
| spark.sql.gravitino.oauth2.scope        | string   | None            | The scope to request the OAuth2 token.          | Yes, for OAuth2 mode   | 0.7.0-incubating   |

## Kerberos Mode

In kerberos mode, you could use the Spark kerberos configuration to fetch a kerberos ticket to access Gravitino server, use `spark.kerberos.principal`, `spark.kerberos.keytab` to specify kerberos principal and keytab.

The Gravitino server principal has the form `HTTP/$host@$realm`. Keep `$host` consistent with the host in the Gravitino server URI.
Please make sure `krb5.conf` is accessible by Spark, like by specifying the configuration `spark.driver.extraJavaOptions="-Djava.security.krb5.conf=/xx/krb5.conf"`.
