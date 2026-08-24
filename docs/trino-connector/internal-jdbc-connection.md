---
title: Configure the internal Trino JDBC connection
license: "This software is licensed under the Apache License version 2."
---

# Configure the internal Trino JDBC connection

The Gravitino Trino Connector connects back to the Trino coordinator through JDBC to execute
`CREATE CATALOG` and `DROP CATALOG`. Configure this internal connection in the Gravitino catalog
properties file, usually `etc/catalog/gravitino.properties`.

These settings apply only to the coordinator connection. They are not copied to dynamically created
catalogs.

## Basic configuration

For a coordinator that accepts plaintext HTTP connections, the existing configuration continues to
work without additional TLS settings:

```properties
connector.name=gravitino
gravitino.uri=http://gravitino.example.com:8090
gravitino.metalake=production

trino.jdbc.user=admin
trino.jdbc.password=YourSecureTrinoPassword
```

The connector obtains the coordinator host and port from the Trino `discovery.uri` setting. When
`discovery.uri` uses HTTP and omits its port, the internal JDBC connection uses port 80.

## Connect to an HTTPS coordinator

When the Trino coordinator uses HTTPS, set its `discovery.uri` accordingly in
`etc/config.properties`:

```properties
http-server.https.enabled=true
http-server.https.port=8443
discovery.uri=https://coordinator.example.com:8443
```

The connector automatically enables TLS for its internal JDBC connection when `discovery.uri` uses
the HTTPS scheme. You do not need to set `trino.jdbc.ssl.enabled` in this case.

If the HTTPS discovery URI omits its port, the internal connection uses port 443:

```properties
discovery.uri=https://coordinator.example.com
```

### Use the JVM default truststore

If the coordinator certificate is issued by a CA already trusted by the JVM, no dedicated
truststore configuration is required:

```properties
trino.jdbc.user=admin
trino.jdbc.password=YourSecureTrinoPassword
```

Certificate verification defaults to `FULL`, which validates both the certificate chain and the
coordinator hostname.

### Use a dedicated truststore

For a private CA or self-signed coordinator certificate, create a truststore containing that
certificate:

```shell
openssl s_client -showcerts -connect coordinator.example.com:8443 </dev/null \
  | openssl x509 -outform PEM > coordinator.pem

keytool -importcert -noprompt \
  -alias trino-coordinator \
  -file coordinator.pem \
  -keystore /etc/trino/truststore.p12 \
  -storetype PKCS12 \
  -storepass YourSecureTruststorePassword
```

Configure the Gravitino catalog to use it:

```properties
trino.jdbc.ssl.truststore.path=/etc/trino/truststore.p12
trino.jdbc.ssl.truststore.password=YourSecureTruststorePassword
trino.jdbc.ssl.truststore.type=PKCS12
```

The truststore file must be readable by the operating-system user that runs Trino. In a container
deployment, mount the file into the coordinator container and use its container path in the
configuration.

### Enable TLS explicitly

Normally, deriving TLS from `discovery.uri` is sufficient. To override that behavior, set:

```properties
trino.jdbc.ssl.enabled=true
```

Setting truststore properties while TLS is disabled is invalid. The Gravitino Connector reports a
configuration error instead of silently falling back to plaintext.

### Configure mutual TLS

If the coordinator requires mutual TLS, configure a client certificate that the internal JDBC
connection presents to the coordinator:

```properties
trino.jdbc.ssl.keystore.path=/etc/trino/client.p12
trino.jdbc.ssl.keystore.password=YourSecureClientKeystorePassword
trino.jdbc.ssl.keystore.type=PKCS12
```

The keystore path must point to an existing file. The keystore password and type require
`trino.jdbc.ssl.keystore.path`; setting either one without the path causes Connector initialization
to fail.

The client certificate must be trusted by the coordinator. These settings configure transport-level
mutual TLS only. They do not add support for Trino's `CERTIFICATE` authentication type, where the
certificate subject becomes the login identity.

## Select the certificate verification mode

Use `trino.jdbc.ssl.verification` to control certificate verification:

| Mode | Certificate chain | Hostname | Recommended use |
| --- | --- | --- | --- |
| `FULL` | Verified | Verified | Production; this is the default |
| `CA` | Verified | Not verified | Environments where the certificate is trusted but its hostname cannot match the JDBC target |
| `NONE` | Not verified | Not verified | Temporary troubleshooting only |

Production configuration should normally use:

```properties
trino.jdbc.ssl.verification=FULL
```

`CA` can be used when the certificate chain is trusted but the coordinator is reached through an
address that is not present in the certificate:

```properties
trino.jdbc.ssl.verification=CA
```

:::caution
`trino.jdbc.ssl.verification=NONE` disables certificate verification and makes the connection
vulnerable to man-in-the-middle attacks. Do not use it as a permanent solution. A truststore path
cannot be configured together with `NONE`.
:::

## Configure a session role

Some Trino deployments allow `CREATE CATALOG` and `DROP CATALOG` only after a privileged system role
has been enabled. Apply that role to the internal JDBC session with:

```properties
trino.jdbc.roles=system:sysadmin
```

The JDBC user must be allowed to activate the configured role. Without that permission, catalog
synchronization fails with an authorization error.

## Pass additional JDBC driver properties

Use the `trino.jdbc.properties.` prefix for a Trino JDBC driver property that has no dedicated
Gravitino setting. The connector removes the prefix and passes the remainder to the JDBC driver:

```properties
trino.jdbc.properties.KerberosRemoteServiceName=trino
trino.jdbc.properties.SSLKeyStorePath=/etc/trino/client-keystore.p12
trino.jdbc.properties.SSLKeyStorePassword=YourSecureKeystorePassword
```

Raw properties override values generated from dedicated `trino.jdbc.*` settings. For example:

```properties
trino.jdbc.ssl.verification=FULL
trino.jdbc.properties.SSLVerification=CA
```

In this example, the driver receives `SSLVerification=CA`.

Raw properties are not validated by the Gravitino Connector. Unknown names and invalid values are
reported by the Trino JDBC driver when it establishes the connection.

## Complete production example

Trino `etc/config.properties`:

```properties
coordinator=true
http-server.https.enabled=true
http-server.https.port=8443
http-server.https.keystore.path=/etc/trino/server-keystore.p12
http-server.https.keystore.key=YourSecureServerKeystorePassword
discovery.uri=https://coordinator.example.com:8443
catalog.management=dynamic
```

Gravitino catalog `etc/catalog/gravitino.properties`:

```properties
connector.name=gravitino
gravitino.uri=https://gravitino.example.com:8090
gravitino.metalake=production

trino.jdbc.user=gravitino_catalog_manager
trino.jdbc.password=YourSecureTrinoPassword
trino.jdbc.ssl.verification=FULL
trino.jdbc.ssl.truststore.path=/etc/trino/truststore.p12
trino.jdbc.ssl.truststore.password=YourSecureTruststorePassword
trino.jdbc.ssl.truststore.type=PKCS12
# Add these three settings only when the coordinator requires mutual TLS.
trino.jdbc.ssl.keystore.path=/etc/trino/client.p12
trino.jdbc.ssl.keystore.password=YourSecureClientKeystorePassword
trino.jdbc.ssl.keystore.type=PKCS12
trino.jdbc.roles=system:sysadmin
```

Restart Trino after changing these files. Confirm that the Gravitino catalog loads and that a catalog
created in Gravitino appears in `SHOW CATALOGS`.

## Troubleshooting

### Hostname is not verified

An error similar to the following means the JDBC target is not present in the certificate subject
alternative names:

```text
SSLPeerUnverifiedException: Hostname coordinator-address not verified
```

Use a certificate containing the coordinator DNS name and keep `FULL` verification. If that is not
possible, use `CA` only after confirming that hostname verification is intentionally unnecessary.

### PKIX path building failed

This error means the JVM cannot establish trust in the coordinator certificate:

```text
PKIX path building failed: unable to find valid certification path
```

Import the coordinator CA or certificate into the JVM truststore or configure a dedicated truststore.

### Truststore file does not exist

Verify that `trino.jdbc.ssl.truststore.path` is the path visible to the Trino process. Host paths and
container paths are often different.

### Catalog synchronization remains unavailable

TLS connection failures can leave the Trino server running while the Gravitino Connector retries its
internal JDBC connection. Check the Trino server log for `CatalogRegister` warnings and resolve the
certificate, truststore, endpoint, or authorization error.

## Security recommendations

- Use `FULL` verification in production.
- Store passwords in the deployment's secret-management mechanism instead of source control.
- Restrict truststore and keystore file permissions to the Trino operating-system user.
- Use a dedicated JDBC account with only the permissions needed to manage catalogs.
- Do not use `SSLVerification=NONE` except during short-lived troubleshooting.
- Do not include passwords when sharing configuration files or logs.
