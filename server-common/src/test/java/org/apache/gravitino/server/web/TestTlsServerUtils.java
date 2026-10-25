/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.web;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLHandshakeException;
import javax.servlet.http.HttpServlet;
import org.apache.gravitino.Config;
import org.apache.gravitino.rest.RESTUtils;

public final class TestTlsServerUtils {

  public static final String TEST_STORE_PASSWORD = "changeit";

  private TestTlsServerUtils() {}

  public static int startHttpsServer(
      JettyServer server,
      boolean clientAuth,
      HttpServlet servlet,
      String servletPath,
      Path tlsDirectory)
      throws Exception {

    int port = RESTUtils.findAvailablePort(6000, 7000);

    Config config = new Config(false) {};
    config.set(JettyServerConfig.ENABLE_HTTPS, true);
    config.set(JettyServerConfig.WEBSERVER_HTTPS_PORT, port);

    config.set(
        JettyServerConfig.SSL_KEYSTORE_PATH,
        tlsDirectory.resolve("test-server-keystore.p12").toString());
    config.set(JettyServerConfig.SSL_KEYSTORE_PASSWORD, TEST_STORE_PASSWORD);
    config.set(JettyServerConfig.SSL_MANAGER_PASSWORD, TEST_STORE_PASSWORD);

    config.set(JettyServerConfig.ENABLE_CLIENT_AUTH, clientAuth);

    if (clientAuth) {
      config.set(
          JettyServerConfig.SSL_TRUST_STORE_PATH,
          tlsDirectory.resolve("test-server-truststore.p12").toString());
      config.set(JettyServerConfig.SSL_TRUST_STORE_PASSWORD, TEST_STORE_PASSWORD);
    }

    server.initialize(JettyServerConfig.fromConfig(config), "test", false);
    server.start();
    server.addServlet(servlet, servletPath);

    return port;
  }

  public static int startHttpServer(JettyServer server, HttpServlet servlet, String servletPath)
      throws Exception {

    int port = RESTUtils.findAvailablePort(6000, 7000);

    Config config = new Config(false) {};
    config.set(JettyServerConfig.WEBSERVER_HTTP_PORT, port);

    server.initialize(JettyServerConfig.fromConfig(config), "test", false);
    server.start();
    server.addServlet(servlet, servletPath);

    return port;
  }

  public static void assertHandshakeFailure(Throwable throwable) {
    Throwable current = throwable;

    while (current != null) {
      if (current instanceof SSLHandshakeException) {
        return;
      }

      current = current.getCause();
    }

    fail("Expected an SSL handshake failure, but received: " + throwable, throwable);
  }

  /**
   * Generates TLS identities and truststores for HTTPS tests.
   *
   * @param directory directory that will contain the generated files
   * @throws Exception if generation fails
   */
  public static void generateTlsMaterial(Path directory) throws Exception {
    Files.createDirectories(directory);

    Path server = directory.resolve("test-server-keystore.p12");
    Path trustedClient = directory.resolve("test-trusted-client-keystore.p12");
    Path untrustedClient = directory.resolve("test-untrusted-client-keystore.p12");

    generateServerKeystore(server);

    generateKeystore(trustedClient, "trusted-client", "CN=trusted-client", "EKU=clientAuth");
    generateKeystore(untrustedClient, "untrusted-client", "CN=untrusted-client", "EKU=clientAuth");

    generateTruststore(
        server,
        "test-server",
        directory.resolve("server.cer"),
        directory.resolve("test-client-truststore.p12"));
    generateTruststore(
        trustedClient,
        "trusted-client",
        directory.resolve("trusted-client.cer"),
        directory.resolve("test-server-truststore.p12"));
    generateTruststore(
        untrustedClient,
        "untrusted-client",
        directory.resolve("untrusted-client.cer"),
        directory.resolve("test-untrusted-server-truststore.p12"));
  }

  static void generateKeystore(Path keystore, String alias, String subject, String extension)
      throws Exception {
    runKeytool(
        "-genkeypair",
        "-alias",
        alias,
        "-keyalg",
        "RSA",
        "-keysize",
        "2048",
        "-storetype",
        "PKCS12",
        "-keystore",
        keystore.toString(),
        "-storepass",
        TEST_STORE_PASSWORD,
        "-keypass",
        TEST_STORE_PASSWORD,
        "-dname",
        subject,
        "-ext",
        extension,
        "-validity",
        "30");
  }

  static void generateServerKeystore(Path keystore) throws Exception {
    generateKeystore(keystore, "test-server", "CN=localhost", "SAN=dns:localhost,ip:127.0.0.1");
  }

  static void generateTruststore(
      Path serverKeystore, String alias, Path certificate, Path truststore) throws Exception {
    runKeytool(
        "-exportcert",
        "-alias",
        alias,
        "-keystore",
        serverKeystore.toString(),
        "-storepass",
        TEST_STORE_PASSWORD,
        "-file",
        certificate.toString());

    runKeytool(
        "-importcert",
        "-alias",
        alias,
        "-file",
        certificate.toString(),
        "-keystore",
        truststore.toString(),
        "-storetype",
        "PKCS12",
        "-storepass",
        TEST_STORE_PASSWORD,
        "-noprompt");
  }

  private static void runKeytool(String... arguments) throws Exception {
    List<String> command = new ArrayList<>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
    command.addAll(Arrays.asList(arguments));

    Process process = new ProcessBuilder(command).inheritIO().start();

    try {
      if (!process.waitFor(30, TimeUnit.SECONDS)) {
        throw new IOException("Timed out running keytool");
      }
      if (process.exitValue() != 0) {
        throw new IOException("Keytool failed with exit code: " + process.exitValue());
      }
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
      }
    }
  }
}
