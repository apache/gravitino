---
title: "How to use HTTPS"
slug: /security/how-to-use-https
keyword: security HTTPS protocol
license: "This software is licensed under the Apache License version 2."
---

## HTTPS

For users choosing OAuth 2.0 as the authentication method, it is recommended to use HTTPS instead of HTTP. HTTPS encrypts the request headers, offering better protection against smuggling attacks.

Note that Gravitino cannot simultaneously support both HTTP and HTTPS within a single server instance. If HTTPS is enabled, Gravitino will no longer provide HTTP service.

Currently, both the Gravitino server and Iceberg REST service can configure and support HTTPS.

### Apache Gravitino server's configuration

| Configuration item                                  | Description                                                        | Default value       | Required                                          | Since version |
|-----------------------------------------------------|--------------------------------------------------------------------|---------------------|---------------------------------------------------|---------------|
| `gravitino.server.webserver.enableHttps`            | Enables HTTPS.                                                     | `false`             | No                                                | 0.3.0         |
| `gravitino.server.webserver.httpsPort`              | The HTTPS port number of the Jetty web server.                     | `8433`              | No                                                | 0.3.0         |
| `gravitino.server.webserver.keyStorePath`           | Path to the key store file.                                        | (none)              | Yes if use HTTPS                                  | 0.3.0         |
| `gravitino.server.webserver.keyStorePassword`       | Password to the key store.                                         | (none)              | Yes if use HTTPS                                  | 0.3.0         |
| `gravitino.server.webserver.keyStoreType`           | The type to the key store.                                         | `JKS`               | No                                                | 0.3.0         |
| `gravitino.server.webserver.managerPassword`        | Manager password to the key store.                                 | (none)              | Yes if use HTTPS                                  | 0.3.0         |
| `gravitino.server.webserver.tlsProtocol`            | TLS protocol to use. The JVM must support the TLS protocol to use. | (none)              | No                                                | 0.3.0         |
| `gravitino.server.webserver.enableCipherAlgorithms` | The collection of enabled cipher algorithms.                       | '' (empty string)   | No                                                | 0.3.0         |
| `gravitino.server.webserver.enableClientAuth`       | Enables the authentication of the client.                          | `false`             | No                                                | 0.3.0         |
| `gravitino.server.webserver.trustStorePath`         | Path to the trust store file.                                      | (none)              | Yes if use HTTPS and the authentication of client | 0.3.0         |
| `gravitino.server.webserver.trustStorePassword`     | Password to the trust store.                                       | (none)              | Yes if use HTTPS and the authentication of client | 0.3.0         |
| `gravitino.server.webserver.trustStoreType`         | The type to the trust store.                                       | `JKS`               | No                                                | 0.3.0         |

### Apache Iceberg REST service's configuration

| Configuration item                              | Description                                                        | Default value     | Required                                          | Since version |
|-------------------------------------------------|--------------------------------------------------------------------|-------------------|---------------------------------------------------|---------------|
| `gravitino.iceberg-rest.enableHttps`            | Enables HTTPS.                                                     | `false`           | No                                                | 0.3.0         |
| `gravitino.iceberg-rest.httpsPort`              | The HTTPS port number of the Jetty web server.                     | `9433`            | No                                                | 0.3.0         |
| `gravitino.iceberg-rest.keyStorePath`           | Path to the key store file.                                        | (none)            | Yes if use HTTPS                                  | 0.3.0         |
| `gravitino.iceberg-rest.keyStorePassword`       | Password to the key store.                                         | (none)            | Yes if use HTTPS                                  | 0.3.0         |
| `gravitino.iceberg-rest.keyStoreType`           | The type to the key store.                                         | `JKS`             | No                                                | 0.3.0         |
| `gravitino.iceberg-rest.managerPassword`        | Manager password to the key store.                                 | (none)            | Yes if use HTTPS                                  | 0.3.0         |
| `gravitino.iceberg-rest.tlsProtocol`            | TLS protocol to use. The JVM must support the TLS protocol to use. | (none)            | No                                                | 0.3.0         |
| `gravitino.iceberg-rest.enableCipherAlgorithms` | The collection of enabled cipher algorithms.                       | '' (empty string) | No                                                | 0.3.0         |
| `gravitino.iceberg-rest.enableClientAuth`       | Enables the authentication of the client.                          | `false`           | No                                                | 0.3.0         |
| `gravitino.iceberg-rest.trustStorePath`         | Path to the trust store file.                                      | (none)            | Yes if use HTTPS and the authentication of client | 0.3.0         |
| `gravitino.iceberg-rest.trustStorePassword`     | Password to the trust store.                                       | (none)            | Yes if use HTTPS and the authentication of client | 0.3.0         |
| `gravitino.iceberg-rest.trustStoreType`         | The type to the trust store.                                       | `JKS`             | No                                                | 0.3.0         |

Refer to the "Additional JSSE Standard Names" section of the [Java security guide](https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#jssenames) for the list of protocols related to tlsProtocol. You can find the list of `tlsProtocol` values for Java 8 in this document.

Refer to the "Additional JSSE Standard Names" section of the [Java security guide](https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites) for the list of protocols related to tlsProtocol. You can find the list of `enableCipherAlgorithms` values for Java 8 in this document.

### Example

You can follow the steps to set up an HTTPS server.

1. Prerequisite
   - You need to install the JDK8, wget, and set the environment JAVA_HOME.
   - If you want to use the command `curl` to request the Gravitino server, you should install openSSL.
2. Generate the key store

```shell
cd $JAVA_HOME
bin/keytool -genkeypair  -alias localhost \
-keyalg RSA -keysize 4096 -keypass localhost \
-sigalg SHA256withRSA \
-keystore localhost.jks -storetype JKS -storepass localhost \
-dname "cn=localhost,ou=localhost,o=localhost,l=beijing,st=beijing,c=cn" \
-validity 36500
```

3. Generate the certificate

```shell
bin/keytool -export -alias localhost -keystore localhost.jks -file  localhost.crt -storepass localhost
```

4. Import the certificate

```shell
bin/keytool -import -alias localhost -keystore jre/lib/security/cacerts -file localhost.crt -storepass changeit -noprompt
```

5. You can refer to the [Configurations](../gravitino-server-config.md) and append the configuration to the conf/gravitino.conf.
   Configuration doesn't support resolving environment variables, so you should replace `${JAVA_HOME}` with the actual value.
   Then, You can start the Gravitino server.

```text
gravitino.server.webserver.host = localhost
gravitino.server.webserver.enableHttps = true
gravitino.server.webserver.keyStorePath = ${JAVA_HOME}/localhost.jks
gravitino.server.webserver.keyStorePassword = localhost
gravitino.server.webserver.managerPassword = localhost
```

6. Request the Gravitino server

- If you use Java, you can copy the code below to a file named Main.java

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

- If you want to use the command `curl`, you can follow the commands:

```shell
openssl x509 -inform der -in $JAVA_HOME/localhost.crt -out certificate.pem
curl -v -X GET --cacert ./certificate.pem -H "Accept: application/vnd.gravitino.v1+json" -H "Content-Type: application/json" https://localhost:8433/api/version
```
