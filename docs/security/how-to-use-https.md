---
title: "HTTPS"
slug: "/security/how-to-use-https"
keywords:
  - security
  - https
  - tls
license: "This software is licensed under the Apache License version 2."
---

## Overview

HTTPS encrypts request headers, which matters most when those headers carry credentials. Any deployment using OAuth 2.0 or [local users and groups](local-users-and-groups.md) should enable it, since both put a token or a password in a header on every request.

A server instance serves one protocol. Enabling HTTPS stops the plain HTTP listener rather than adding to it, so clients configured against the HTTP port need updating at the same time.

## Configuration

The Gravitino server and the Iceberg REST service are configured separately with the same property names under different prefixes. Use `gravitino.server.webserver.` for the Gravitino server and `gravitino.iceberg-rest.` for the Iceberg REST service.

| Property Name             | Description                                    | Default Value | Required                       |
|---------------------------|------------------------------------------------|---------------|--------------------------------|
| `enableHttps`             | Enables HTTPS                                  | `false`       | No                             |
| `httpsPort`               | HTTPS port for the Jetty web server            | `8433` and `9433` | No                         |
| `keyStorePath`            | Path to the key store file                     | (none)        | Yes                            |
| `keyStorePassword`        | Password for the key store                     | (none)        | Yes                            |
| `managerPassword`         | Manager password for the key store             | (none)        | Yes                            |
| `keyStoreType`            | Key store type                                 | `JKS`         | No                             |
| `tlsProtocol`             | TLS protocol to use, which the JVM must support| (none)        | No                             |
| `enableCipherAlgorithms`  | Cipher algorithms to enable                    | (empty)       | No                             |
| `enableClientAuth`        | Requires clients to authenticate with a certificate | `false`  | No                             |
| `trustStorePath`          | Path to the trust store file                   | (none)        | Yes with client authentication |
| `trustStorePassword`      | Password for the trust store                   | (none)        | Yes with client authentication |
| `trustStoreType`          | Trust store type                               | `JKS`         | No                             |

The default HTTPS port is `8433` for the Gravitino server and `9433` for the Iceberg REST service. Everything in the Required column applies once `enableHttps` is `true`.

For the values `tlsProtocol` and `enableCipherAlgorithms` accept, see the "Additional JSSE Standard Names" section of the Java security guide, under [protocols](https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#jssenames) and [cipher suites](https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites) respectively.

## Local Development Example

The following produces a self-signed certificate so you can exercise an HTTPS endpoint on one machine. It is not a production setup, since a self-signed certificate trusted by editing a JVM trust store is not how certificates are managed in a real deployment.

**1. Generate a key store.**

```shell
cd $JAVA_HOME
bin/keytool -genkeypair -alias localhost \
  -keyalg RSA -keysize 4096 -keypass {key_password} \
  -sigalg SHA256withRSA \
  -keystore localhost.jks -storetype JKS -storepass {store_password} \
  -dname "cn=localhost,ou=localhost,o=localhost,l=beijing,st=beijing,c=cn" \
  -validity 36500
```

**2. Export the certificate.**

```shell
bin/keytool -export -alias localhost -keystore localhost.jks \
  -file localhost.crt -storepass {store_password}
```

**3. Import it into the JVM trust store** so a local Java client will accept it.

```shell
bin/keytool -import -alias localhost -keystore jre/lib/security/cacerts \
  -file localhost.crt -storepass changeit -noprompt
```

**4. Configure the server.** Append the following to `conf/gravitino.conf`, then start Gravitino. Configuration files do not resolve environment variables, so write the expanded path rather than `${JAVA_HOME}`.

```properties
gravitino.server.webserver.host = localhost
gravitino.server.webserver.enableHttps = true
gravitino.server.webserver.keyStorePath = {java_home}/localhost.jks
gravitino.server.webserver.keyStorePassword = {store_password}
gravitino.server.webserver.managerPassword = {key_password}
```

**5. Connect.** From Java, the client takes the HTTPS URI directly:

```java
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.GravitinoVersion;

public class Main {
    public static void main(String[] args) {
        String uri = "https://localhost:8433";
        GravitinoClient client = GravitinoClient.builder(uri).withMetalake("metalake").build();
        GravitinoVersion gravitinoVersion = client.getVersion();
        System.out.println(gravitinoVersion);
    }
}
```

From `curl`, convert the certificate to PEM first:

```shell
openssl x509 -inform der -in $JAVA_HOME/localhost.crt -out certificate.pem
curl -v -X GET --cacert ./certificate.pem \
  -H "Accept: application/vnd.gravitino.v1+json" \
  https://localhost:8433/api/version
```

## Further Reading

- [Configurations](../gravitino-server-config.md) for the rest of the web server settings
- [How to Authenticate](how-to-authenticate.md) for the authentication methods HTTPS protects
