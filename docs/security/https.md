---
title: "How to use HTTPS"
slug: /security/how-to-use-https
keyword: security HTTPS protocol
license: "This software is licensed under the Apache License version 2."
---

## HTTPS

For users choosing OAuth 2.0 as the [authentication](./authentication.md) method,
it is recommended to use HTTPS instead of HTTP.
HTTPS encrypts the request headers, offering better protection against smuggling attacks.

Note that Gravitino cannot simultaneously support both HTTP and HTTPS on a single server instance.
If HTTPS is enabled, Gravitino will no longer provide plain HTTP service.

Currently, both the Gravitino server and Iceberg REST service can be configured to use HTTPS.

### Apache Gravitino server's configuration

<table>
<thead>
 <tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.server.webserver.enableHttps</tt></td>
  <td>Enables HTTPS.</td>
  <td>`false`</td>
  <td>No</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.httpsPort</tt></td>
  <td>The HTTPS port number of the Jetty web server.</td>
  <td>`8433`</td>
  <td>No</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.keyStorePath</tt></td>
  <td>Path to the key store file.</td>
  <td>(none)</td>
  <td>Yes if use HTTPS</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.keyStorePassword</tt></td>
  <td>Password to the key store.</td>
  <td>(none)</td>
  <td>Yes if use HTTPS</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.keyStoreType</tt></td>
  <td>The type to the key store.</td>
  <td>`JKS`</td>
  <td>No</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.managerPassword</tt></td>
  <td>Manager password to the key store.</td>
  <td>(none)</td>
  <td>Yes if use HTTPS</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.tlsProtocol</tt></td>
  <td>TLS protocol to use. The JVM must support the TLS protocol to use.</td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.enableCipherAlgorithms</tt></td>
  <td>The collection of enabled cipher algorithms.</td>
  <td>'' (empty string)</td>
  <td>No</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.enableClientAuth</tt></td>
  <td>Enables the authentication of the client.</td>
  <td>`false`</td>
  <td>No</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.trustStorePath</tt></td>
  <td>
    Path to the trust store file.

    This is required for HTTPS setup at client side.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.trustStorePassword</tt></td>
  <td>
    Password to the trust store.

    This is required for HTTPS setup at client side.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.trustStoreType</tt></td>
  <td>
    The type to the trust store.
  </td>
  <td>`JKS`</td>
  <td>No</td>
  <td>`0.3.0`</td>
</tr>

### Apache Iceberg REST service's configuration

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.iceberg-rest.enableHttps</tt></td>
  <td>Enables HTTPS.</td>
  <td>`false`</td>
  <td>No</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.httpsPort</tt></td>
  <td>The HTTPS port number of the Jetty web server.</td>
  <td>`9433`</td>
  <td>No</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.keyStorePath</tt></td>
  <td>
    Path to the key store file.

    Required when HTTPS is enabled.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.keyStorePassword</tt></td>
  <td>
    Password to the key store.

    Required when HTTPS is enabled.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.keyStoreType</tt></td>
  <td>The type of the key store.</td>
  <td>`JKS`</td>
  <td>No</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.managerPassword</tt></td>
  <td>
    Manager password to the key store.

    Required when HTTPS is enabled.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.tlsProtocol</tt></td>
  <td>
    TLS protocol to use.
    The JVM must support the TLS protocol when using this.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.enableCipherAlgorithms</tt></td>
  <td>
    The collection of enabled cipher algorithms.
  </td>
  <td>'' (empty string)</td>
  <td>No</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.enableClientAuth</tt></td>
  <td>
    Enables the authentication of the client.
  </td>
  <td>`false`</td>
  <td>No</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.trustStorePath</tt></td>
  <td>
    Path to the trust store file.

    Required when HTTPS is enabled at client side.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.trustStorePassword</tt></td>
  <td>
    Password to the trust store.

    Required when setting up HTTPS at client side.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>`0.3.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.trustStoreType</tt></td>
  <td>
    The type to the trust store.
  </td>
  <td>`JKS`</td>
  <td>No</td>
  <td>`0.3.0`</td>
</tr>
</tbody>
</table>

Refer to the "Additional JSSE Standard Names" section of the
[Java security guide](https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#jssenames)
for the list of protocols related to `tlsProtocol`.
You can find the list of `tlsProtocol` and `enableCipherAlgorithms` values for Java 8 in this document.

### Example

You can follow the steps to set up an HTTPS server.

1. Prerequisite

   - You need to install the JDK 8, and set the environment vironment `JAVA_HOME`.
   - If you want to use `curl` for sending requests the Gravitino server,
     you need to install openSSL.

1. Generate the key store

   ```shell
   cd $JAVA_HOME
   bin/keytool -genkeypair  -alias localhost \
     -keyalg RSA -keysize 4096 -keypass localhost \
     -sigalg SHA256withRSA \
     -keystore localhost.jks -storetype JKS -storepass localhost \
     -dname "cn=localhost,ou=localhost,o=localhost,l=beijing,st=beijing,c=cn" \
     -validity 36500
   ```

1. Generate the certificate

   ```shell
   bin/keytool -export -alias localhost \
     -keystore localhost.jks \
     -file localhost.crt \
     -storepass localhost
   ```

1. Import the certificate

   ```shell
   bin/keytool -import -alias localhost \
     -keystore jre/lib/security/cacerts \
     -file localhost.crt \
     -storepass changeit \
     -noprompt
   ```

1. You can refer to the [Configurations](../admin/server-config.md) and
   append the configuration to the `conf/gravitino.conf` file.
   Configuration doesn't support resolving environment variables,
   so you should replace `${JAVA_HOME}` with the actual value,
   and then start the Gravitino server.

   ```text
   gravitino.server.webserver.host = localhost
   gravitino.server.webserver.enableHttps = true
   gravitino.server.webserver.keyStorePath = ${JAVA_HOME}/localhost.jks
   gravitino.server.webserver.keyStorePassword = localhost
   gravitino.server.webserver.managerPassword = localhost
   ```

1. Send request to the Gravitino server

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

   - If you want to use `curl`, you can follow the commands:

     ```shell
     openssl x509 -inform der -in $JAVA_HOME/localhost.crt -out certificate.pem
     curl -v -X GET \
       --cacert ./certificate.pem \
       -H "Accept: application/vnd.gravitino.v1+json" \
       https://localhost:8433/api/version
     ```

