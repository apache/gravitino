---
title: "Trino Connector Configuration"
slug: "/trino-connector/configuration"
keyword: "gravitino connector trino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

| Property                                    | Type    | Default Value         | Description                                                                                                                                                                                                                                                                                                                                      | Required |
|---------------------------------------------|---------|-----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| connector.name                              | string  | (none)                | The `connector.name` defines the type of Trino connector, this value is always 'gravitino'.                                                                                                                                                                                                                                                      | Yes      |
| gravitino.metalake                          | string  | (none)                | The `gravitino.metalake` defines which metalake in Gravitino server the Trino connector uses. Trino connector should set it at start, the value of `gravitino.metalake` needs to be a valid name, Trino connector can detect and load the metalake with catalogs, schemas and tables once created and keep in sync.                              | Yes      |
| gravitino.uri                               | string  | http://localhost:8090 | The `gravitino.uri` defines the connection URL of the Gravitino server, the default value is `http://localhost:8090`. Trino connector can detect and connect to Gravitino server once it is ready, no need to start Gravitino server beforehand.                                                                                                 | No       |
| trino.jdbc.user                             | string  | admin                 | The jdbc user name of current Trino.                                                                                                                                                                                                                                                                                                             | NO       |
| trino.jdbc.password                         | string  | (none)                | The jdbc password of current Trino.                                                                                                                                                                                                                                                                                                              | NO       |
| trino.jdbc.ssl.enabled                      | boolean | (derived)             | Whether the internal JDBC connection to the Trino coordinator uses TLS. If not set, it is derived from the scheme of the Trino `discovery.uri`, so a coordinator whose `discovery.uri` is `https://...` needs no explicit setting.                                                                                                               | No       |
| trino.jdbc.ssl.truststore.path              | string  | (none)                | Path of the truststore holding the Trino coordinator certificate. If omitted, the default JVM truststore is used. Requires TLS, which is enabled automatically for an HTTPS `discovery.uri` or explicitly with `trino.jdbc.ssl.enabled=true`, and a `trino.jdbc.ssl.verification` other than `NONE`.                                             | No       |
| trino.jdbc.ssl.truststore.password          | string  | (none)                | Password of the truststore configured by `trino.jdbc.ssl.truststore.path`. Requires TLS and `trino.jdbc.ssl.truststore.path`, otherwise the connector fails to start.                                                                                                                                                                            | No       |
| trino.jdbc.ssl.truststore.type              | string  | (none)                | Type of the truststore, for example `JKS` or `PKCS12`. If omitted, the default JVM truststore type is used. Requires TLS and `trino.jdbc.ssl.truststore.path`, otherwise the connector fails to start.                                                                                                                                           | No       |
| trino.jdbc.ssl.keystore.path                | string  | (none)                | Path of the keystore holding the client certificate presented to the coordinator, for coordinators that require mutual TLS. Requires TLS, which is enabled automatically for an HTTPS `discovery.uri` or explicitly with `trino.jdbc.ssl.enabled=true`, and a `trino.jdbc.ssl.verification` other than `NONE`. See the note on mutual TLS below. | No       |
| trino.jdbc.ssl.keystore.password            | string  | (none)                | Password of the keystore configured by `trino.jdbc.ssl.keystore.path`. Requires TLS and `trino.jdbc.ssl.keystore.path`, otherwise the connector fails to start.                                                                                                                                                                                  | No       |
| trino.jdbc.ssl.keystore.type                | string  | (none)                | Type of the keystore, for example `JKS` or `PKCS12`. If omitted, the default JVM keystore type is used. Requires TLS and `trino.jdbc.ssl.keystore.path`, otherwise the connector fails to start.                                                                                                                                                 | No       |
| trino.jdbc.ssl.verification                 | string  | FULL                  | Certificate verification mode of the internal JDBC connection: `FULL`, `CA` or `NONE`. Any value other than `FULL` requires TLS, which may be derived from an HTTPS `discovery.uri`. `NONE` disables certificate verification entirely and should only be used for troubleshooting.                                                              | No       |
| trino.jdbc.roles                            | string  | (none)                | Session roles applied to the internal JDBC connection, for example `system:sysadmin`. Required by deployments that only allow `CREATE CATALOG` with a privileged role.                                                                                                                                                                           | No       |
| trino.jdbc.properties.                      | string  | (none)                | The configuration key prefix for raw Trino JDBC driver properties, see [Connecting to a TLS-enabled coordinator](#connecting-to-a-tls-enabled-coordinator).                                                                                                                                                                                      | No       |
| gravitino.metadata.refresh-interval-seconds | integer | 10                    | The `gravitino.metadata.refresh-interval-seconds` defines the interval in seconds to refresh metadata from Gravitino server, the default value is 10 seconds.                                                                                                                                                                                    | No       |
| gravitino.trino.skip-version-validation     | boolean | false                 | The `gravitino.trino.skip-version-validation` defines whether to skip Trino version validation. Gravitino supports Trino versions between 440 and 478. If this option is `true`, unsupported Trino versions can still be used, but compatibility is not guaranteed.                                                                              | No       |
| gravitino.client.                           | string  | (none)                | The configuration key prefix for the Gravitino client config.                                                                                                                                                                                                                                                                                    | No       |
| gravitino.trino.skip-catalog-patterns       | string  | (none)                | The `gravitino.trino.skip-catalog-patterns` defines a comma-separated list of catalog name regex patterns that should be excluded from loading. For example, `test_.*, .*_tmp` excludes all catalogs starting with `test_` or ending with `_tmp`.                                                                                                | No       |
| gravitino.use-single-metalake               | boolean | true                  | If `true`, only one metalake is used and catalogs are identified by `<catalog_name>`. If `false`, multi-metalake mode is enabled and catalogs are identified by `<metalake_name>.<catalog_name>`.                                                                                                                                                | No       |
| gravitino.iceberg.rest-uri                  | string  | (none)                | The endpoint of the Gravitino Iceberg REST server (IRC). It is discovered automatically from the Gravitino server for this connector's metalake; set this only to override the discovered value. When available, eligible `lakehouse-iceberg` catalogs are loaded through IRC, enabling credential vending.                                               | No       |
| gravitino.iceberg.rest-catalog.             | string  | (none)                | Prefix for properties passed to the internal Trino Iceberg REST catalog. The prefix is rewritten to `iceberg.rest-catalog.`. The `uri`, `warehouse`, and `prefix` keys are reserved and derived by the connector.                                                                                                                               | No       |

To configure the Gravitino client, use properties prefixed with `gravitino.client.`. These properties will directly passed to the Gravitino client.

**Note:** Invalid configuration properties will result in exceptions. Please see [Gravitino Java client configurations](../how-to-use-gravitino-client.md#java-client-configuration) for more support client configuration.

Multi-metalake mode (`gravitino.use-single-metalake=false`) is supported on Trino connector versions 440-445 and 469-478. On versions 446-468, a warning is logged and the connector initializes, but the mode is not fully supported and some operations may fail.

**Note:** In multi-metalake mode, `gravitino.iceberg.rest-uri` is only honored when scoped to a
metalake, as `gravitino.iceberg.rest-uri.<metalake_name>` — the unscoped form is ignored, since a
single Iceberg REST server serves exactly one metalake and applying it to every metalake would
misroute the others. The unscoped form remains valid in single-metalake mode.

## Connecting to a TLS-enabled coordinator

The Gravitino Trino connector registers catalogs by connecting back to the Trino coordinator over
JDBC and running `CREATE CATALOG` / `DROP CATALOG`. This connection is established when the
connector starts and is reused by the metadata refresh loop, so it must be configured for the
coordinator's own TLS and authorization settings.

```properties
connector.name=gravitino
gravitino.metalake=metalake
gravitino.uri=http://localhost:8090

# The internal JDBC connection to the coordinator.
trino.jdbc.user=admin
trino.jdbc.password=YourSecureTrinoPassword
trino.jdbc.ssl.truststore.path=/etc/trino/truststore.jks
trino.jdbc.ssl.truststore.password=YourSecureTruststorePassword
# Required when the deployment only allows CREATE CATALOG with a privileged role.
trino.jdbc.roles=system:sysadmin
```

`trino.jdbc.ssl.enabled` may be omitted when the Trino `discovery.uri` uses the `https` scheme, as
it is derived from that scheme by default. When `discovery.uri` omits the port, the default port of
its scheme is used, that is `443` for `https` and `80` for `http`.

The `trino.jdbc.ssl.*` configurations are only meaningful on a TLS-enabled connection. Setting any
of them while TLS is disabled fails the connector at startup rather than being silently ignored, so
a misconfigured truststore never degrades into a plaintext connection. Likewise, the truststore
password and type require a truststore path, and the keystore password and type require a keystore
path: without one the driver falls back to its default, which they would not apply to.

If the coordinator certificate is signed by a CA the JVM does not trust, import it into a
truststore and point `trino.jdbc.ssl.truststore.path` at it:

```shell
# Export the coordinator certificate, then import it into a dedicated truststore.
openssl s_client -showcerts -connect coordinator.example.com:8443 </dev/null \
  | openssl x509 -outform PEM > coordinator.pem
keytool -importcert -noprompt -alias trino-coordinator -file coordinator.pem \
  -keystore /etc/trino/truststore.jks -storepass YourSecureTruststorePassword
```

:::caution
`trino.jdbc.ssl.verification=NONE` disables certificate verification completely and exposes the
connection to man-in-the-middle attacks. Use it only for troubleshooting; import the coordinator
certificate into a truststore instead.
:::

The `trino.jdbc.*` properties are used by the coordinator only. They are never copied into the
catalogs the connector creates, so the credentials they hold do not reach the generated
`CREATE CATALOG` statements or the Trino catalog properties files.

### Mutual TLS and certificate authentication

A coordinator that requires mutual TLS also expects a client certificate. Point
`trino.jdbc.ssl.keystore.path` at the keystore holding it:

```properties
trino.jdbc.ssl.keystore.path=/etc/trino/client.p12
trino.jdbc.ssl.keystore.password=YourSecureKeystorePassword
```

:::note
The mutual TLS configuration is provided for completeness and has not been verified against a
coordinator that requires client certificates.
:::

Trino can also be configured with `http-server.authentication.type=CERTIFICATE`, where the client
certificate itself is the login and the coordinator derives the username from the certificate
subject. The connector does not support that authentication type today: it always authenticates
with `trino.jdbc.user`, so a coordinator configured this way rejects the internal JDBC connection.

### Passing arbitrary JDBC driver properties

Any Trino JDBC driver property that has no dedicated configuration can be passed through with the
`trino.jdbc.properties.` prefix. The prefix is stripped and the remainder is handed to the driver
verbatim, overriding the value derived from the dedicated `trino.jdbc.*` configurations:

```properties
trino.jdbc.properties.KerberosRemoteServiceName=trino
trino.jdbc.properties.SSLKeyStorePath=/etc/trino/client.p12
trino.jdbc.properties.SSLKeyStorePassword=YourSecureKeystorePassword
```

Properties passed through this prefix are handed to the driver without validation, unlike the
dedicated `trino.jdbc.*` configurations. An unknown name or an invalid value therefore surfaces as a
driver error when the connection is established, not as a configuration error.

See the [Trino JDBC driver documentation](https://trino.io/docs/current/client/jdbc.html) for the
full list of supported property names.

## Authentication

The Gravitino Trino connector supports authenticating to the Gravitino server using Simple, Basic, OAuth, and Kerberos authentication. For detailed authentication configuration, refer to [Trino Connector Authentication](./authentication.md).
