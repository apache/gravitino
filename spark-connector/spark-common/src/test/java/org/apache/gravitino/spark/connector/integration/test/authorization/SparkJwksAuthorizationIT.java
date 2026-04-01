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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.apache.gravitino.integration.test.util.JwksMockServerHelper;
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
 * End-to-end Docker integration test for Spark OAuth2 authentication and authorization via JWKS.
 * User identity is derived solely from the JWT {@code sub} claim.
 */
@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public abstract class SparkJwksAuthorizationIT extends BaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(SparkJwksAuthorizationIT.class);

  private static final String GRAVITINO_ADMIN = "gravitino";
  private static final String ALICE = "alice";
  private static final String BOB = "bob";

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

  private static JwksMockServerHelper mockServerHelper;

  private static String gravitinoUri;

  private static SparkSession aliceSparkSession;
  private static GravitinoAdminClient bobClient;

  private String mysqlUrl;
  private String mysqlUsername;
  private String mysqlPassword;
  private String mysqlDriver;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    mockServerHelper = JwksMockServerHelper.create(KEY_ID);

    Instant farFuture = Instant.now().plusSeconds(1_000_000);
    String adminToken = mockServerHelper.mintToken(GRAVITINO_ADMIN, SERVICE_AUDIENCE, farFuture);
    String aliceToken = mockServerHelper.mintToken(ALICE, SERVICE_AUDIENCE, farFuture);
    String bobToken = mockServerHelper.mintToken(BOB, SERVICE_AUDIENCE, farFuture);

    mockServerHelper.registerUserToken(GRAVITINO_ADMIN, adminToken);
    mockServerHelper.registerUserToken(ALICE, aliceToken);
    mockServerHelper.registerUserToken(BOB, bobToken);
    mockServerHelper.setFallbackToken(adminToken);
    LOG.info("Mock JWKS+token server started on port {}", mockServerHelper.port());

    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.AUTHENTICATORS.getKey(), AuthenticatorType.OAUTH.name().toLowerCase());
    configs.put(OAuthConfig.SERVICE_AUDIENCE.getKey(), SERVICE_AUDIENCE);
    configs.put(
        OAuthConfig.TOKEN_VALIDATOR_CLASS.getKey(),
        "org.apache.gravitino.server.authentication.JwksTokenValidator");
    configs.put(OAuthConfig.JWKS_URI.getKey(), mockServerHelper.jwksUri());
    configs.put(OAuthConfig.PRINCIPAL_FIELDS.getKey(), "sub");
    configs.put(OAuthConfig.ALLOW_SKEW_SECONDS.getKey(), "6");
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), "true");
    configs.put(Configs.SERVICE_ADMINS.getKey(), GRAVITINO_ADMIN);
    configs.put(Configs.CACHE_ENABLED.getKey(), "false");
    registerCustomConfigs(configs);

    OAuthMockDataProvider.getInstance().setTokenData(adminToken.getBytes(StandardCharsets.UTF_8));

    initMysqlContainer();
    super.startIntegrationTest();
    gravitinoUri = String.format("http://127.0.0.1:%d", getGravitinoServerPort());

    initMetadata();

    aliceSparkSession = buildSparkSession(ALICE_CREDENTIAL);
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
    if (mockServerHelper != null) {
      mockServerHelper.close();
    }
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

    Map<String, String> props = Maps.newHashMap();
    props.put(JdbcPropertiesConstants.GRAVITINO_JDBC_URL, mysqlUrl);
    props.put(JdbcPropertiesConstants.GRAVITINO_JDBC_USER, mysqlUsername);
    props.put(JdbcPropertiesConstants.GRAVITINO_JDBC_PASSWORD, mysqlPassword);
    props.put(JdbcPropertiesConstants.GRAVITINO_JDBC_DRIVER, mysqlDriver);
    Catalog catalog =
        metalake.createCatalog(CATALOG, Catalog.Type.RELATIONAL, "jdbc-mysql", "", props);
    catalog.asSchemas().createSchema(SCHEMA, "", new HashMap<>());

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

  @Test
  @Order(1)
  public void testAliceCreatesTableViaSparkOAuth() {
    aliceSparkSession.sql("USE " + CATALOG + "." + SCHEMA);
    aliceSparkSession.sql("CREATE TABLE " + ALICE_TABLE + " (id INT, name STRING)");

    List<Row> tables = aliceSparkSession.sql("SHOW TABLES").collectAsList();
    Assertions.assertTrue(
        tables.stream().anyMatch(r -> ALICE_TABLE.equalsIgnoreCase(r.getString(1))),
        "Alice's Spark session should see the table she created");

    GravitinoMetalake adminMetalake = client.loadMetalake(METALAKE);
    Optional<Owner> tableOwner =
        adminMetalake.getOwner(
            MetadataObjects.of(
                ImmutableList.of(CATALOG, SCHEMA, ALICE_TABLE), MetadataObject.Type.TABLE));
    Assertions.assertTrue(tableOwner.isPresent(), "Table should have an owner recorded");
    Assertions.assertEquals(
        ALICE,
        tableOwner.get().name(),
        "Table owner should be Alice");
    LOG.info("Alice created '{}' via Spark OAuth2 (sub=alice)", ALICE_TABLE);
  }

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

  @Test
  @Order(3)
  public void testBobAccessCatalogAfterGrant() {
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

    GravitinoMetalake bobMetalake = bobClient.loadMetalake(METALAKE);
    Catalog catalog = bobMetalake.loadCatalog(CATALOG);
    Assertions.assertNotNull(catalog);

    boolean tableExists =
        catalog.asTableCatalog().tableExists(NameIdentifier.of(SCHEMA, ALICE_TABLE));
    Assertions.assertTrue(
        tableExists, "Bob should see the table Alice created after being granted access");
    LOG.info("Bob's JWT-authenticated client sees '{}' after grant (sub=bob)", ALICE_TABLE);
  }

  private SparkSession buildSparkSession(String credential) {
    SparkConf conf =
        new SparkConf()
            .set("spark.plugins", GravitinoSparkPlugin.class.getName())
            .set(GravitinoSparkConfig.GRAVITINO_URI, gravitinoUri)
            .set(GravitinoSparkConfig.GRAVITINO_METALAKE, METALAKE)
            .set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, AuthProperties.OAUTH2_AUTH_TYPE)
            .set(GravitinoSparkConfig.GRAVITINO_OAUTH2_URI, mockServerHelper.baseUri())
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
            .withUri(mockServerHelper.baseUri())
            .withPath("token")
            .withCredential(credential)
            .withScope("openid")
            .build();
    return GravitinoAdminClient.builder(gravitinoUri).withOAuth(provider).build();
  }
}
