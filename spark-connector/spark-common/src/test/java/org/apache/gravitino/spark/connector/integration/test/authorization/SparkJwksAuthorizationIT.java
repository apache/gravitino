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
package org.apache.gravitino.spark.connector.integration.test.authorization;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.DefaultOAuth2TokenProvider;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.OAuthMockDataProvider;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.server.authentication.OAuthConfig;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.jdbc.JdbcPropertiesConstants;
import org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end Docker integration test for Spark OAuth2 authentication AND authorization via JWKS
 * token validation.
 *
 * <p>Verifies the complete scenario: user identity is derived solely from the JWT {@code sub} claim
 * (i.e., the OAuth2 credential drives authorization, not environment variables or simple auth). The
 * test lifecycle is:
 *
 * <ol>
 *   <li><b>Alice</b> authenticates via OAuth2 ({@code client_id=alice}) — Gravitino validates the
 *       JWT from the in-process JWKS server. Alice creates a table via Spark SQL.
 *   <li><b>Bob</b> (OAuth2, {@code client_id=bob}) has no catalog privileges — his {@link
 *       GravitinoAdminClient} gets {@link ForbiddenException} on {@code loadCatalog}.
 *   <li><b>Admin</b> grants Bob {@code SELECT_TABLE} access via the Gravitino API.
 *   <li><b>Bob's client</b> can now load the catalog and see the table Alice created.
 * </ol>
 *
 * <p>All user identity is JWT-based: the in-process mock {@code /token} endpoint parses {@code
 * client_id} from the {@code client_credentials} POST body and returns the corresponding pre-minted
 * RS256 JWT. No {@code SPARK_USER} / {@code HADOOP_USER_NAME} environment variables are set.
 */
@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public abstract class SparkJwksAuthorizationIT extends BaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(SparkJwksAuthorizationIT.class);

  // These become the JWT sub claims and therefore the Gravitino principal names.
  private static final String GRAVITINO_ADMIN = "gravitino";
  private static final String ALICE = "alice";
  private static final String BOB = "bob";

  // Credential format understood by DefaultOAuth2TokenProvider: clientId:clientSecret.
  // The mock /token endpoint only cares about clientId (the part before ':').
  private static final String ALICE_CREDENTIAL = ALICE + ":alice-secret";
  private static final String BOB_CREDENTIAL = BOB + ":bob-secret";

  private static final String SERVICE_AUDIENCE = "service1";
  private static final String KEY_ID = "test-kid";
  private static final String METALAKE = "jwks_authz_metalake";
  private static final String CATALOG = "jdbc_catalog";
  private static final String SCHEMA = "jdbc_schema";
  private static final String ALICE_TABLE = "alice_table";
  private static final String ALICE_ROLE = "alice-role";
  private static final String BOB_ROLE = "bob-role";

  protected final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private static HttpServer mockServer;
  private static RSAKey rsaKey;
  /**
   * Maps {@code client_id} → pre-minted JWT. The mock {@code /token} handler looks up the JWT for
   * the requesting client and returns it, making user identity entirely token-driven.
   */
  private static Map<String, String> userTokens;

  private static String gravitinoUri;
  private static int mockPort;

  /** Spark session authenticated as alice ({@code client_id=alice}). */
  private static SparkSession aliceSparkSession;

  /** GravitinoAdminClient authenticated as bob ({@code client_id=bob}). */
  private static GravitinoAdminClient bobClient;

  protected String mysqlUrl;
  protected String mysqlUsername;
  protected String mysqlPassword;
  protected String mysqlDriver;

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    // 1. Generate RSA key pair for JWT signing.
    rsaKey = new RSAKeyGenerator(2048).keyID(KEY_ID).generate();

    // 2. Pre-mint long-lived tokens for each principal.
    Instant farFuture = Instant.now().plusSeconds(1_000_000);
    String adminToken = mintToken(rsaKey, GRAVITINO_ADMIN, SERVICE_AUDIENCE, farFuture);
    String aliceToken = mintToken(rsaKey, ALICE, SERVICE_AUDIENCE, farFuture);
    String bobToken = mintToken(rsaKey, BOB, SERVICE_AUDIENCE, farFuture);

    userTokens = new ConcurrentHashMap<>();
    userTokens.put(GRAVITINO_ADMIN, adminToken);
    userTokens.put(ALICE, aliceToken);
    userTokens.put(BOB, bobToken);

    // 3. Start in-process HTTP server.
    String jwksJson = new JWKSet(rsaKey.toPublicJWK()).toString();
    mockServer = HttpServer.create(new InetSocketAddress(0), 0);

    // /jwks – JWKS document consumed by Gravitino's JwksTokenValidator.
    mockServer.createContext(
        "/jwks",
        exchange -> {
          byte[] body = jwksJson.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.getResponseBody().close();
        });

    // /token – user-aware mock OAuth2 client_credentials endpoint.
    // Parses client_id from the URL-encoded POST body and returns the matching JWT,
    // so every DefaultOAuth2TokenProvider configured with a specific client_id
    // receives the JWT for that user (sub = client_id).
    mockServer.createContext(
        "/token",
        exchange -> {
          String postBody =
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
          String clientId = parseFormParam(postBody, "client_id");
          String token =
              clientId != null ? userTokens.getOrDefault(clientId, adminToken) : adminToken;
          byte[] resp = buildTokenResponse(token).getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, resp.length);
          exchange.getResponseBody().write(resp);
          exchange.getResponseBody().close();
        });

    mockServer.start();
    mockPort = mockServer.getAddress().getPort();
    LOG.info("Mock JWKS+token server started on port {}", mockPort);

    // 4. Configure embedded Gravitino server: JwksTokenValidator + authorization.
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.AUTHENTICATORS.getKey(), AuthenticatorType.OAUTH.name().toLowerCase());
    configs.put(OAuthConfig.SERVICE_AUDIENCE.getKey(), SERVICE_AUDIENCE);
    configs.put(
        OAuthConfig.TOKEN_VALIDATOR_CLASS.getKey(),
        "org.apache.gravitino.server.authentication.JwksTokenValidator");
    configs.put(OAuthConfig.JWKS_URI.getKey(), "http://localhost:" + mockPort + "/jwks");
    configs.put(OAuthConfig.PRINCIPAL_FIELDS.getKey(), "sub");
    configs.put(OAuthConfig.ALLOW_SKEW_SECONDS.getKey(), "6");
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), "true");
    configs.put(Configs.SERVICE_ADMINS.getKey(), GRAVITINO_ADMIN);
    // Disable cache so privilege changes from tests are reflected immediately.
    configs.put(Configs.CACHE_ENABLED.getKey(), "false");
    registerCustomConfigs(configs);

    // 5. Prime OAuthMockDataProvider — BaseIT requires this to build its internal `client`.
    OAuthMockDataProvider.getInstance().setTokenData(adminToken.getBytes(StandardCharsets.UTF_8));

    // 6. Start MySQL container and Gravitino.
    initMysqlContainer();
    super.startIntegrationTest();
    gravitinoUri = String.format("http://127.0.0.1:%d", getGravitinoServerPort());

    // 7. Bootstrap metadata: metalake, catalog, schema, users, alice's role.
    initMetadata();

    // 8. Start alice's Spark session — OAuth2 with client_id=alice.
    aliceSparkSession = buildSparkSession(ALICE_CREDENTIAL);

    // 9. Build bob's GravitinoAdminClient — OAuth2 with client_id=bob.
    bobClient = buildGravitinoClient(BOB_CREDENTIAL);
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    if (aliceSparkSession != null) {
      aliceSparkSession.stop();
    }
    if (bobClient != null) {
      bobClient.close();
    }
    if (mockServer != null) {
      mockServer.stop(0);
    }
    containerSuite.close();
    super.stopIntegrationTest();
  }

  private void initMysqlContainer() throws SQLException {
    containerSuite.startMySQLContainer(TestDatabaseName.MYSQL_CATALOG_MYSQL_IT);
    mysqlUrl = containerSuite.getMySQLContainer().getJdbcUrl();
    mysqlUsername = containerSuite.getMySQLContainer().getUsername();
    mysqlPassword = containerSuite.getMySQLContainer().getPassword();
    mysqlDriver =
        containerSuite
            .getMySQLContainer()
            .getDriverClassName(TestDatabaseName.MYSQL_CATALOG_MYSQL_IT);
  }

  private void initMetadata() {
    client.createMetalake(METALAKE, "", new HashMap<>());
    GravitinoMetalake metalake = client.loadMetalake(METALAKE);
    metalake.addUser(ALICE);
    metalake.addUser(BOB);

    // Create JDBC catalog and schema as the admin.
    Map<String, String> props = Maps.newHashMap();
    props.put(JdbcPropertiesConstants.GRAVITINO_JDBC_URL, mysqlUrl);
    props.put(JdbcPropertiesConstants.GRAVITINO_JDBC_USER, mysqlUsername);
    props.put(JdbcPropertiesConstants.GRAVITINO_JDBC_PASSWORD, mysqlPassword);
    props.put(JdbcPropertiesConstants.GRAVITINO_JDBC_DRIVER, mysqlDriver);
    Catalog catalog =
        metalake.createCatalog(CATALOG, Catalog.Type.RELATIONAL, "jdbc-mysql", "", props);
    catalog.asSchemas().createSchema(SCHEMA, "", new HashMap<>());

    // Grant alice full access to the catalog so she can create and read tables.
    SecurableObject catalogAccess =
        SecurableObjects.ofCatalog(
            CATALOG,
            ImmutableList.of(
                Privileges.UseCatalog.allow(),
                Privileges.UseSchema.allow(),
                Privileges.CreateTable.allow(),
                Privileges.SelectTable.allow()));
    metalake.createRole(ALICE_ROLE, new HashMap<>(), ImmutableList.of(catalogAccess));
    metalake.grantRolesToUser(ImmutableList.of(ALICE_ROLE), ALICE);
  }

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  /**
   * Verifies that Alice can create a table via Spark SQL using her OAuth2 JWT. Gravitino resolves
   * Alice's identity from the JWT {@code sub=alice} claim and enforces her catalog privileges.
   */
  @Test
  @Order(1)
  public void testAliceCreatesTableViaSparkOAuth() {
    aliceSparkSession.sql("USE " + CATALOG + "." + SCHEMA);
    aliceSparkSession.sql("CREATE TABLE " + ALICE_TABLE + " (id INT, name STRING)");

    List<Row> tables = aliceSparkSession.sql("SHOW TABLES").collectAsList();
    Assertions.assertTrue(
        tables.stream().anyMatch(r -> ALICE_TABLE.equalsIgnoreCase(r.getString(1))),
        "Alice's Spark session should see the table she created");

    // Verify that Gravitino recorded Alice (JWT sub=alice) as the table owner.
    GravitinoMetalake adminMetalake = client.loadMetalake(METALAKE);
    Optional<Owner> tableOwner =
        adminMetalake.getOwner(
            MetadataObjects.of(
                ImmutableList.of(CATALOG, SCHEMA, ALICE_TABLE), MetadataObject.Type.TABLE));
    Assertions.assertTrue(tableOwner.isPresent(), "Table should have an owner recorded");
    Assertions.assertEquals(
        ALICE,
        tableOwner.get().name(),
        "Table owner should be Alice (resolved from JWT sub=alice)");
    LOG.info("Alice created '{}' via Spark OAuth2 (sub=alice)", ALICE_TABLE);
  }

  /**
   * Verifies that Bob cannot load the catalog before any privileges are granted. Bob's JWT has
   * {@code sub=bob}; Gravitino resolves his identity and enforces the absence of catalog access.
   */
  @Test
  @Order(2)
  public void testBobCannotAccessCatalogBeforeGrant() {
    GravitinoMetalake bobMetalake = bobClient.loadMetalake(METALAKE);
    Assertions.assertThrows(
        ForbiddenException.class,
        () -> bobMetalake.loadCatalog(CATALOG),
        "Bob should be denied catalog access before grant");
    LOG.info("Bob correctly received ForbiddenException before grant (sub=bob)");
  }

  /**
   * Grants Bob read access to the catalog (via the admin client), then verifies that Bob's
   * JWT-authenticated client can load the catalog and see the table Alice created. Proves the full
   * loop: grant → privilege change propagates → JWT-identified user gains access.
   */
  @Test
  @Order(3)
  public void testBobAccessCatalogAfterGrant() {
    // Admin grants bob USE_CATALOG + USE_SCHEMA + SELECT_TABLE.
    GravitinoMetalake adminMetalake = client.loadMetalake(METALAKE);
    SecurableObject bobCatalogAccess =
        SecurableObjects.ofCatalog(
            CATALOG,
            ImmutableList.of(
                Privileges.UseCatalog.allow(),
                Privileges.UseSchema.allow(),
                Privileges.SelectTable.allow()));
    adminMetalake.createRole(BOB_ROLE, new HashMap<>(), ImmutableList.of(bobCatalogAccess));
    adminMetalake.grantRolesToUser(ImmutableList.of(BOB_ROLE), BOB);

    // Bob's JWT-authenticated client should now be able to load the catalog and see alice's table.
    GravitinoMetalake bobMetalake = bobClient.loadMetalake(METALAKE);
    Catalog catalog = bobMetalake.loadCatalog(CATALOG);
    Assertions.assertNotNull(catalog);

    boolean tableExists =
        catalog.asTableCatalog().tableExists(NameIdentifier.of(SCHEMA, ALICE_TABLE));
    Assertions.assertTrue(
        tableExists, "Bob should see the table Alice created after being granted access");
    LOG.info("Bob's JWT-authenticated client sees '{}' after grant (sub=bob)", ALICE_TABLE);
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  private SparkSession buildSparkSession(String credential) {
    SparkConf conf =
        new SparkConf()
            .set("spark.plugins", GravitinoSparkPlugin.class.getName())
            .set(GravitinoSparkConfig.GRAVITINO_URI, gravitinoUri)
            .set(GravitinoSparkConfig.GRAVITINO_METALAKE, METALAKE)
            .set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, AuthProperties.OAUTH2_AUTH_TYPE)
            .set(GravitinoSparkConfig.GRAVITINO_OAUTH2_URI, "http://localhost:" + mockPort)
            .set(GravitinoSparkConfig.GRAVITINO_OAUTH2_PATH, "token")
            .set(GravitinoSparkConfig.GRAVITINO_OAUTH2_CREDENTIAL, credential)
            .set(GravitinoSparkConfig.GRAVITINO_OAUTH2_SCOPE, "openid");
    return SparkSession.builder()
        .master("local[1]")
        .appName("SparkJwksAuthorizationIT")
        .config(conf)
        .getOrCreate();
  }

  private GravitinoAdminClient buildGravitinoClient(String credential) {
    DefaultOAuth2TokenProvider provider =
        DefaultOAuth2TokenProvider.builder()
            .withUri("http://localhost:" + mockPort)
            .withPath("token")
            .withCredential(credential)
            .withScope("openid")
            .build();
    return GravitinoAdminClient.builder(gravitinoUri).withOAuth(provider).build();
  }

  /**
   * Parses a single parameter from a URL-encoded form body (e.g., the {@code client_credentials}
   * POST body sent by {@link DefaultOAuth2TokenProvider}).
   */
  private static String parseFormParam(String body, String key) {
    for (String pair : body.split("&")) {
      String[] kv = pair.split("=", 2);
      if (kv.length == 2 && URLDecoder.decode(kv[0], StandardCharsets.UTF_8).equals(key)) {
        return URLDecoder.decode(kv[1], StandardCharsets.UTF_8);
      }
    }
    return null;
  }

  /**
   * Mints a compact RS256-signed JWT.
   *
   * @param key RSA key (private component required for signing)
   * @param subject JWT {@code sub} claim — becomes the Gravitino principal
   * @param audience JWT {@code aud} claim
   * @param expiry expiration instant
   * @return serialized compact JWT string
   */
  @SuppressWarnings("JavaUtilDate")
  private static String mintToken(RSAKey key, String subject, String audience, Instant expiry)
      throws Exception {
    JWTClaimsSet claims =
        new JWTClaimsSet.Builder()
            .subject(subject)
            .audience(audience)
            .issueTime(Date.from(Instant.now()))
            .expirationTime(Date.from(expiry))
            .build();
    SignedJWT jwt =
        new SignedJWT(
            new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(key.getKeyID()).build(), claims);
    jwt.sign(new RSASSASigner(key));
    return jwt.serialize();
  }

  private static String buildTokenResponse(String token) {
    return "{\"access_token\":\"" + token + "\",\"token_type\":\"bearer\",\"expires_in\":86400}";
  }
}
