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
package org.apache.gravitino.trino.connector.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.integration.test.container.GravitinoLocalStackContainer;
import org.apache.gravitino.integration.test.container.TrinoContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.integration.test.util.OAuthMockDataProvider;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.server.authentication.OAuthConfig;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;

/**
 * Verifies the complete secured Trino connector path: HTTPS coordinator, OAuth2-authenticated
 * Gravitino and Iceberg REST requests, and Iceberg REST S3 credential vending.
 */
@Tag("gravitino-docker-test")
public class TrinoTlsOAuthCredentialVendingIT extends BaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(TrinoTlsOAuthCredentialVendingIT.class);

  private static final String AUDIENCE = "gravitino-trino-it";
  private static final String CLIENT_CREDENTIAL = "test-client:test-secret";
  private static final String STORE_PASSWORD = "changeit";
  // Deliberately not TrinoContainer.DEFAULT_IMAGE / GRAVITINO_CI_TRINO_DOCKER_IMAGE: that CI image
  // identifies itself as Trino SPI version 435, which trino-connector-473-478 rejects outright.
  // This test needs an actual Trino 473-478 build, so it pins the upstream image directly.
  private static final String TRINO_IMAGE = "trinodb/trino:478";
  private static final String CONTAINER_TRUSTSTORE = "/etc/trino/tls/truststore.p12";

  private final KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);
  private final String metalakeName = randomName("trino_tls_metalake");
  private final String catalogName = randomName("trino_tls_catalog");
  private final String bucketName =
      "trino-tls-bucket-" + UUID.randomUUID().toString().replace("-", "");

  private OAuthServer oauthServer;
  private TrinoContainer trinoContainer;
  private GravitinoLocalStackContainer localStack;
  private Path trinoConfigDirectory;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    Assumptions.assumeFalse(ITUtils.isEmbedded(), "This test requires the deploy distribution");

    containerSuite.startLocalStackContainer();
    localStack = containerSuite.getLocalStackContainer();
    createBucket();
    containerSuite.startPostgreSQLContainer(TestDatabaseName.PG_ICEBERG_AUTHZ_IT);

    oauthServer = new OAuthServer(keyPair, AUDIENCE, "admin", CLIENT_CREDENTIAL);
    oauthServer.start();
    configureGravitino();
    copyIcebergAwsBundle();

    OAuthMockDataProvider.getInstance().setTokenData(mintToken().getBytes(StandardCharsets.UTF_8));
    super.startIntegrationTest();

    createCatalog();
    trinoConfigDirectory = Files.createTempDirectory("trino-tls-oauth-");
    populateTrinoConfig(trinoConfigDirectory);
    startTrino();
  }

  @Test
  public void testTlsOAuthAndCredentialVending() {
    assertTrue(trinoContainer.checkSyncCatalogFromGravitino(10, catalogName));

    // Captured before any writes so the post-write delta can prove LocalStack actually received
    // an STS AssumeRole call, rather than the write silently using the S3TokenGenerator's own
    // static root credential directly (which LocalStack would accept just the same).
    String logsBeforeWrite = localStack.getContainer().getLogs();

    String schema = "secured";
    String table = catalogName + "." + schema + ".people";
    trinoContainer.executeUpdateSQL("CREATE SCHEMA " + catalogName + "." + schema);
    trinoContainer.executeUpdateSQL(
        "CREATE TABLE " + table + " (id bigint, name varchar) WITH (format = 'PARQUET')");
    trinoContainer.executeUpdateSQL("INSERT INTO " + table + " VALUES (1, 'alice'), (2, 'bob')");

    assertEquals(
        "2", trinoContainer.executeQuerySQL("SELECT count(*) FROM " + table).get(0).get(0));

    String logsDuringWrite =
        localStack.getContainer().getLogs().substring(logsBeforeWrite.length());
    assertTrue(
        logsDuringWrite.contains("AssumeRole"),
        "LocalStack did not receive an STS AssumeRole request while writing Iceberg data,"
            + " meaning vended credentials may not actually have been used");
    assertTrue(
        oauthServer.gravitinoTokenRequests() > 0,
        "The Trino connector did not request a Gravitino OAuth2 token");
    assertTrue(
        oauthServer.trinoIcebergTokenRequests() > 0,
        "The Trino Iceberg connector did not request an Iceberg REST OAuth2 token");
    assertEquals(
        0,
        oauthServer.invalidRequests(),
        "The mock OAuth2 server rejected an unexpected or malformed token request");

    Container.ExecResult objects =
        localStack.executeInContainer("awslocal", "s3", "ls", "s3://" + bucketName, "--recursive");
    assertEquals(0, objects.getExitCode(), objects.getStderr());
    assertFalse(objects.getStdout().isBlank(), "No Iceberg objects were written to S3");

    trinoContainer.executeUpdateSQL("DROP TABLE " + table);
    trinoContainer.executeUpdateSQL("DROP SCHEMA " + catalogName + "." + schema);
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    cleanupQuietly(
        "Trino container",
        () -> {
          if (trinoContainer != null) {
            trinoContainer.close();
          }
        });
    cleanupQuietly(
        "Gravitino metalake",
        () -> {
          if (client != null) {
            client.dropMetalake(metalakeName, true);
          }
        });
    cleanupQuietly(
        "OAuth2 mock server",
        () -> {
          if (oauthServer != null) {
            oauthServer.close();
          }
        });
    cleanupQuietly(
        "Trino config directory",
        () -> {
          if (trinoConfigDirectory != null) {
            FileUtils.deleteDirectory(trinoConfigDirectory.toFile());
          }
        });
    super.stopIntegrationTest();
  }

  private void cleanupQuietly(String resource, ThrowingRunnable action) {
    try {
      action.run();
    } catch (Exception e) {
      LOG.warn("Failed to clean up {} during test teardown", resource, e);
    }
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  private void configureGravitino() {
    String publicKey = Base64.getEncoder().encodeToString(keyPair.getPublic().getEncoded());
    customConfigs.putAll(
        ImmutableMap.of(
            Configs.AUTHENTICATORS.getKey(),
            "oauth",
            OAuthConfig.SERVICE_AUDIENCE.getKey(),
            AUDIENCE,
            OAuthConfig.DEFAULT_SIGN_KEY.getKey(),
            publicKey,
            OAuthConfig.DEFAULT_SERVER_URI.getKey(),
            oauthServer.serverUri("127.0.0.1"),
            OAuthConfig.DEFAULT_TOKEN_PATH.getKey(),
            OAuthServer.GRAVITINO_TOKEN_PATH));

    ignoreIcebergAuxRestService = false;
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.ICEBERG_REST_CATALOG_CONFIG_PROVIDER,
        IcebergConstants.DYNAMIC_ICEBERG_CATALOG_CONFIG_PROVIDER_NAME);
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_METALAKE, metalakeName);
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.ICEBERG_REST_DEFAULT_DYNAMIC_CATALOG_NAME,
        catalogName);
    // No gravitino.iceberg-rest.gravitino-oauth2.* config here: with the Iceberg REST service
    // embedded as an auxiliary service (ignoreIcebergAuxRestService = false above),
    // DynamicIcebergConfigProvider resolves catalogs through GravitinoEnv's in-process dispatcher
    // (IcebergRESTServerContext#isAuxMode()), never through an HTTP+OAuth2 client. Configuring
    // that OAuth2 block here would be dead configuration this test can never actually exercise.
  }

  private void createCatalog() {
    GravitinoMetalake metalake = client.createMetalake(metalakeName, "", new HashMap<>());
    String endpoint =
        String.format(
            "http://%s:%d", localStack.getContainerIpAddress(), GravitinoLocalStackContainer.PORT);
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(IcebergConstants.CATALOG_BACKEND, "jdbc")
            .put(
                IcebergConstants.URI,
                containerSuite
                    .getPostgreSQLContainer()
                    .getJdbcUrl(TestDatabaseName.PG_ICEBERG_AUTHZ_IT))
            .put(IcebergConstants.GRAVITINO_JDBC_DRIVER, "org.postgresql.Driver")
            .put(
                IcebergConstants.GRAVITINO_JDBC_USER,
                containerSuite.getPostgreSQLContainer().getUsername())
            .put(
                IcebergConstants.GRAVITINO_JDBC_PASSWORD,
                containerSuite.getPostgreSQLContainer().getPassword())
            .put(IcebergConstants.ICEBERG_JDBC_INITIALIZE, "true")
            .put("gravitino.bypass.jdbc.schema-version", "v1")
            .put(IcebergConstants.WAREHOUSE, "s3://" + bucketName + "/warehouse")
            .put(IcebergConstants.IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO")
            .put(IcebergConstants.DATA_ACCESS, "vended-credentials")
            .put(
                CredentialConstants.CREDENTIAL_PROVIDERS,
                S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE)
            .put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, "test")
            .put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, "test")
            .put(S3Properties.GRAVITINO_S3_REGION, "us-east-1")
            .put(S3Properties.GRAVITINO_S3_ENDPOINT, endpoint)
            .put(S3Properties.GRAVITINO_S3_STS_ENDPOINT, endpoint)
            .put(S3Properties.GRAVITINO_S3_ROLE_ARN, "arn:aws:iam::000000000000:role/gravitino")
            .put(S3Properties.GRAVITINO_S3_PATH_STYLE_ACCESS, "true")
            .build();
    metalake.createCatalog(
        catalogName, Catalog.Type.RELATIONAL, "lakehouse-iceberg", "", properties);
  }

  private void populateTrinoConfig(Path directory) throws Exception {
    String rootDir = System.getenv("GRAVITINO_ROOT_DIR");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(rootDir), "GRAVITINO_ROOT_DIR environment variable is not set");
    FileUtils.copyDirectory(
        Path.of(rootDir, "dev", "docker", "trino", "conf").toFile(), directory.toFile());
    Path tlsDirectory = Files.createDirectories(directory.resolve("tls"));
    createTlsStores(tlsDirectory);

    Files.writeString(
        directory.resolve("config.properties"),
        "coordinator=true\n"
            + "node-scheduler.include-coordinator=true\n"
            + "http-server.http.enabled=true\n"
            + "http-server.http.port=8080\n"
            + "http-server.https.enabled=true\n"
            + "http-server.https.port=8443\n"
            + "http-server.https.keystore.path=/etc/trino/tls/server.p12\n"
            + "http-server.https.keystore.key="
            + STORE_PASSWORD
            + "\n"
            + "discovery.uri=http://localhost:8080\n"
            + "catalog.management=dynamic\n",
        StandardCharsets.UTF_8);
    Files.writeString(
        directory.resolve("node.properties"),
        "node.environment=docker\n"
            + "node.id="
            + UUID.randomUUID()
            + "\nnode.data-dir=/data/trino\nplugin.dir=/usr/lib/trino/plugin\n",
        StandardCharsets.UTF_8);
    Path catalogs = Files.createDirectories(directory.resolve("catalog"));
    String connector =
        "connector.name=gravitino\n"
            + "discovery.uri=https://localhost:8443\n"
            + "gravitino.uri=http://host.docker.internal:"
            + getGravitinoServerPort()
            + "\n"
            + "gravitino.metalake="
            + metalakeName
            + "\n"
            + "gravitino.client.authType=oauth2\n"
            + "gravitino.client.oauth2.serverUri="
            + oauthServer.serverUri("host.docker.internal")
            + "\n"
            + "gravitino.client.oauth2.path="
            + OAuthServer.GRAVITINO_TOKEN_PATH
            + "\n"
            + "gravitino.client.oauth2.credential="
            + CLIENT_CREDENTIAL
            + "\n"
            + "gravitino.client.oauth2.scope=test\n"
            + "gravitino.iceberg.rest-uri="
            + containerIcebergRestUri()
            + "\n"
            + "gravitino.iceberg.rest-catalog.security=OAUTH2\n"
            + "gravitino.iceberg.rest-catalog.oauth2.server-uri="
            + oauthServer.serverUri("host.docker.internal")
            + OAuthServer.ICEBERG_TOKEN_PATH
            + "\n"
            + "trino.jdbc.ssl.enabled=true\n"
            + "trino.jdbc.ssl.verification=FULL\n"
            + "trino.jdbc.ssl.truststore.path="
            + CONTAINER_TRUSTSTORE
            + "\n"
            + "trino.jdbc.ssl.truststore.password="
            + STORE_PASSWORD
            + "\n"
            + "trino.jdbc.ssl.truststore.type=PKCS12\n"
            + "gravitino.dynamic-catalog.environment-variable."
            + "gravitino.iceberg.rest-catalog.oauth2.credential=IRC_CLIENT_CREDENTIAL\n";
    Files.writeString(catalogs.resolve("gravitino.properties"), connector, StandardCharsets.UTF_8);
    Files.writeString(
        catalogs.resolve("gravitino.properties.template"), connector, StandardCharsets.UTF_8);
  }

  private void startTrino() {
    String root = System.getenv("GRAVITINO_ROOT_DIR");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(root), "GRAVITINO_ROOT_DIR environment variable is not set");
    String connectorDirectory = System.getenv("TRINO_CONNECTOR_DIR");
    if (connectorDirectory == null || connectorDirectory.isBlank()) {
      connectorDirectory =
          Path.of(root, "trino-connector", "trino-connector-473-478", "build", "libs").toString();
    }
    Path truststore = trinoConfigDirectory.resolve("tls/truststore.p12");
    trinoContainer =
        TrinoContainer.builder()
            .withImage(TRINO_IMAGE)
            .withTrinoConfDir(trinoConfigDirectory.toString())
            .withFilesToMount(
                ImmutableMap.of(
                    TrinoContainer.TRINO_CONTAINER_PLUGIN_GRAVITINO_DIR, connectorDirectory))
            .withExtraHosts(ImmutableMap.of("host.docker.internal", "host-gateway"))
            .withEnvVars(ImmutableMap.of("IRC_CLIENT_CREDENTIAL", CLIENT_CREDENTIAL))
            .withNetwork(containerSuite.getNetwork())
            .withTls(truststore.toString(), STORE_PASSWORD, "PKCS12")
            .build();
    trinoContainer.start();
  }

  private void createBucket() {
    Container.ExecResult result =
        localStack.executeInContainer("awslocal", "s3", "mb", "s3://" + bucketName);
    assertEquals(0, result.getExitCode(), result.getStderr());
  }

  private void copyIcebergAwsBundle() {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(gravitinoHome), "GRAVITINO_HOME environment variable is not set");
    copyBundleJarsToDirectory(
        "iceberg-aws-bundle", Path.of(gravitinoHome, "iceberg-rest-server", "libs").toString());
    copyBundleJarsToDirectory(
        "iceberg-aws-bundle",
        Path.of(gravitinoHome, "catalogs", "lakehouse-iceberg", "libs").toString());
  }

  private void createTlsStores(Path tlsDirectory) throws Exception {
    Path keytool = Path.of(System.getProperty("java.home"), "bin", "keytool");
    Path serverStore = tlsDirectory.resolve("server.p12");
    Path certificate = tlsDirectory.resolve("server.crt");
    Path truststore = tlsDirectory.resolve("truststore.p12");
    run(
        keytool,
        "-genkeypair",
        "-alias",
        "trino",
        "-keyalg",
        "RSA",
        "-storetype",
        "PKCS12",
        "-keystore",
        serverStore.toString(),
        "-storepass",
        STORE_PASSWORD,
        "-keypass",
        STORE_PASSWORD,
        "-dname",
        "CN=gravitino-ci-trino",
        "-ext",
        "SAN=dns:gravitino-ci-trino,dns:localhost,ip:127.0.0.1");
    run(
        keytool,
        "-exportcert",
        "-alias",
        "trino",
        "-keystore",
        serverStore.toString(),
        "-storepass",
        STORE_PASSWORD,
        "-rfc",
        "-file",
        certificate.toString());
    run(
        keytool,
        "-importcert",
        "-noprompt",
        "-alias",
        "trino",
        "-storetype",
        "PKCS12",
        "-keystore",
        truststore.toString(),
        "-storepass",
        STORE_PASSWORD,
        "-file",
        certificate.toString());
  }

  private void run(Path executable, String... arguments) throws Exception {
    String[] command = new String[arguments.length + 1];
    command[0] = executable.toString();
    System.arraycopy(arguments, 0, command, 1, arguments.length);
    Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    if (process.waitFor() != 0) {
      throw new IllegalStateException("Command failed: " + output);
    }
  }

  private String containerIcebergRestUri() {
    String uri = getIcebergRestServiceUri();
    return uri.replace("127.0.0.1", "host.docker.internal")
        .replace("0.0.0.0", "host.docker.internal");
  }

  @SuppressWarnings("JavaUtilDate")
  private String mintToken() {
    return Jwts.builder()
        .setSubject("admin")
        .setAudience(AUDIENCE)
        .setExpiration(new Date(System.currentTimeMillis() + 3_600_000))
        .signWith(keyPair.getPrivate(), SignatureAlgorithm.RS256)
        .compact();
  }

  private static String randomName(String prefix) {
    return prefix + "_" + UUID.randomUUID().toString().replace("-", "");
  }

  /**
   * A minimal OAuth2 client-credentials token endpoint. It serves two distinct flows, each on its
   * own path with its own counter so the test can tell them apart:
   *
   * <ul>
   *   <li>{@link #GRAVITINO_TOKEN_PATH}: Trino's Gravitino client authenticating to Gravitino.
   *   <li>{@link #ICEBERG_TOKEN_PATH}: Trino's Iceberg REST catalog authenticating to the Iceberg
   *       REST service.
   * </ul>
   */
  private static final class OAuthServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(OAuthServer.class);
    private static final String GRAVITINO_TOKEN_PATH = "/oauth2/gravitino/token";
    private static final String ICEBERG_TOKEN_PATH = "/oauth2/iceberg/token";

    private final HttpServer server;
    private final KeyPair keyPair;
    private final String audience;
    private final String subject;
    private final String expectedClientId;
    private final String expectedClientSecret;
    private final AtomicInteger gravitinoTokenRequests = new AtomicInteger();
    private final AtomicInteger trinoIcebergTokenRequests = new AtomicInteger();
    private final AtomicInteger invalidRequests = new AtomicInteger();

    private OAuthServer(KeyPair keyPair, String audience, String subject, String clientCredential)
        throws IOException {
      this.keyPair = keyPair;
      this.audience = audience;
      this.subject = subject;
      String[] credentialParts = clientCredential.split(":", 2);
      this.expectedClientId = credentialParts[0];
      this.expectedClientSecret = credentialParts[1];
      this.server = HttpServer.create(new InetSocketAddress("0.0.0.0", 0), 0);
      this.server.createContext(
          GRAVITINO_TOKEN_PATH, exchange -> handle(exchange, gravitinoTokenRequests));
      this.server.createContext(
          ICEBERG_TOKEN_PATH, exchange -> handle(exchange, trinoIcebergTokenRequests));
    }

    private void start() {
      server.start();
    }

    private String serverUri(String host) {
      return "http://" + host + ":" + server.getAddress().getPort();
    }

    private int gravitinoTokenRequests() {
      return gravitinoTokenRequests.get();
    }

    private int trinoIcebergTokenRequests() {
      return trinoIcebergTokenRequests.get();
    }

    private int invalidRequests() {
      return invalidRequests.get();
    }

    private void handle(HttpExchange exchange, AtomicInteger requestCounter) throws IOException {
      try {
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
          rejectRequest(exchange, 405, "Only POST is supported");
          return;
        }
        Map<String, String> form = parseFormBody(body);
        if (!"client_credentials".equals(form.get("grant_type"))) {
          rejectRequest(exchange, 400, "Unsupported grant_type: " + form.get("grant_type"));
          return;
        }
        if (!expectedClientId.equals(form.get("client_id"))
            || !expectedClientSecret.equals(form.get("client_secret"))) {
          rejectRequest(exchange, 401, "Invalid client credential");
          return;
        }
        requestCounter.incrementAndGet();
        sendToken(exchange);
      } catch (Exception e) {
        LOG.error("Mock OAuth2 server failed to handle request to {}", exchange.getRequestURI(), e);
        rejectRequest(exchange, 500, "Mock OAuth2 server error: " + e.getMessage());
      }
    }

    @SuppressWarnings("JavaUtilDate")
    private void sendToken(HttpExchange exchange) throws IOException {
      String token =
          Jwts.builder()
              .setSubject(subject)
              .setAudience(audience)
              .setExpiration(new Date(System.currentTimeMillis() + 3_600_000))
              .signWith(keyPair.getPrivate(), SignatureAlgorithm.RS256)
              .compact();
      byte[] response =
          ("{\"access_token\":\"" + token + "\",\"token_type\":\"Bearer\",\"expires_in\":3600}")
              .getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, response.length);
      try (OutputStream output = exchange.getResponseBody()) {
        output.write(response);
      }
    }

    private void rejectRequest(HttpExchange exchange, int status, String message)
        throws IOException {
      invalidRequests.incrementAndGet();
      LOG.warn("Rejecting OAuth2 request to {}: {}", exchange.getRequestURI(), message);
      byte[] response = message.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "text/plain");
      exchange.sendResponseHeaders(status, response.length);
      try (OutputStream output = exchange.getResponseBody()) {
        output.write(response);
      }
    }

    private static Map<String, String> parseFormBody(String body) {
      Map<String, String> form = new HashMap<>();
      for (String pair : body.split("&")) {
        if (pair.isEmpty()) {
          continue;
        }
        String[] keyValue = pair.split("=", 2);
        String key = URLDecoder.decode(keyValue[0], StandardCharsets.UTF_8);
        String value =
            keyValue.length > 1 ? URLDecoder.decode(keyValue[1], StandardCharsets.UTF_8) : "";
        form.put(key, value);
      }
      return form;
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }
}
