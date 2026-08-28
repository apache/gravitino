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

import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
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
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.Container;

/**
 * Verifies the complete secured Trino connector path: HTTPS coordinator, OAuth2-authenticated
 * Gravitino and Iceberg REST requests, and Iceberg REST S3 credential vending.
 */
@Tag("gravitino-docker-test")
@EnabledIfEnvironmentVariable(named = "GRAVITINO_CI_TRINO_DOCKER_IMAGE", matches = ".+")
@EnabledIfEnvironmentVariable(named = "GRAVITINO_CI_LOCALSTACK_DOCKER_IMAGE", matches = ".+")
public class TrinoTlsOAuthCredentialVendingIT extends BaseIT {

  private static final String AUDIENCE = "gravitino-trino-it";
  private static final String CLIENT_CREDENTIAL = "test-client:test-secret";
  private static final String STORE_PASSWORD = "changeit";
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

    oauthServer = new OAuthServer(keyPair, AUDIENCE, "admin");
    oauthServer.start();
    configureGravitino();
    copyIcebergAwsBundle();

    OAuthMockDataProvider.getInstance().setTokenData(mintToken().getBytes(StandardCharsets.UTF_8));
    super.startIntegrationTest();

    createCatalog();
    trinoConfigDirectory = createTrinoConfig();
    startTrino();
  }

  @Test
  public void testTlsOAuthAndCredentialVending() {
    assertTrue(trinoContainer.checkSyncCatalogFromGravitino(10, catalogName));

    String schema = "secured";
    String table = catalogName + "." + schema + ".people";
    trinoContainer.executeUpdateSQL("CREATE SCHEMA " + catalogName + "." + schema);
    trinoContainer.executeUpdateSQL(
        "CREATE TABLE " + table + " (id bigint, name varchar) WITH (format = 'PARQUET')");
    trinoContainer.executeUpdateSQL("INSERT INTO " + table + " VALUES (1, 'alice'), (2, 'bob')");

    assertEquals(
        "2", trinoContainer.executeQuerySQL("SELECT count(*) FROM " + table).get(0).get(0));
    assertTrue(
        oauthServer.gravitinoTokenRequests() > 0,
        "The Trino connector did not request a Gravitino OAuth2 token");
    assertTrue(
        oauthServer.icebergTokenRequests() > 0,
        "The Trino Iceberg connector did not request an Iceberg REST OAuth2 token");

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
    try {
      if (trinoContainer != null) {
        trinoContainer.close();
      }
      if (client != null) {
        client.dropMetalake(metalakeName, true);
      }
    } finally {
      if (oauthServer != null) {
        oauthServer.close();
      }
      if (trinoConfigDirectory != null) {
        FileUtils.deleteDirectory(trinoConfigDirectory.toFile());
      }
      super.stopIntegrationTest();
    }
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
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_AUTH_TYPE, "oauth");
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_OAUTH2_SERVER_URI,
        oauthServer.serverUri("127.0.0.1"));
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_OAUTH2_TOKEN_PATH,
        OAuthServer.ICEBERG_TOKEN_PATH);
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_OAUTH2_CREDENTIAL,
        CLIENT_CREDENTIAL);
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_OAUTH2_SCOPE, "test");
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

  private Path createTrinoConfig() throws Exception {
    Path directory = Files.createTempDirectory("trino-tls-oauth-");
    FileUtils.copyDirectory(
        Path.of(System.getenv("GRAVITINO_ROOT_DIR"), "dev", "docker", "trino", "conf").toFile(),
        directory.toFile());
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
    return directory;
  }

  private void startTrino() {
    String root = System.getenv("GRAVITINO_ROOT_DIR");
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

  @SuppressWarnings("ThreadJoinLoop")
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

  private static final class OAuthServer implements AutoCloseable {
    private static final String GRAVITINO_TOKEN_PATH = "/oauth2/gravitino/token";
    private static final String ICEBERG_TOKEN_PATH = "/oauth2/iceberg/token";

    private final HttpServer server;
    private final KeyPair keyPair;
    private final String audience;
    private final String subject;
    private final AtomicInteger gravitinoTokenRequests = new AtomicInteger();
    private final AtomicInteger icebergTokenRequests = new AtomicInteger();

    private OAuthServer(KeyPair keyPair, String audience, String subject) throws IOException {
      this.keyPair = keyPair;
      this.audience = audience;
      this.subject = subject;
      this.server = HttpServer.create(new InetSocketAddress("0.0.0.0", 0), 0);
      this.server.createContext(
          GRAVITINO_TOKEN_PATH, exchange -> handle(exchange, gravitinoTokenRequests));
      this.server.createContext(
          ICEBERG_TOKEN_PATH, exchange -> handle(exchange, icebergTokenRequests));
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

    private int icebergTokenRequests() {
      return icebergTokenRequests.get();
    }

    @SuppressWarnings("JavaUtilDate")
    private void handle(HttpExchange exchange, AtomicInteger requestCounter) throws IOException {
      requestCounter.incrementAndGet();
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

    @Override
    public void close() {
      server.stop(0);
    }
  }
}
