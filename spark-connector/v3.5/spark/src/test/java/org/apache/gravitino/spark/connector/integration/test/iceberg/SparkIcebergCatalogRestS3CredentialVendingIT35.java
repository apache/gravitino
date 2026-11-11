/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file to
 * you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.spark.connector.integration.test.iceberg;

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
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.PostgreSQLContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.JwksMockServerHelper;
import org.apache.gravitino.integration.test.util.OAuthMockDataProvider;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.server.authentication.OAuthConfig;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.iceberg.GravitinoIcebergCatalog;
import org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Spark 3.5 integration test for OAuth2 IRC routing with AWS credential vending. */
@Tag("gravitino-docker-test")
@EnabledIfEnvironmentVariable(named = "GRAVITINO_TEST_CLOUD_IT", matches = "true")
public class SparkIcebergCatalogRestS3CredentialVendingIT35 extends BaseIT {

  private static final String ADMIN = "gravitino";
  private static final String ALICE = "alice";
  private static final String ALICE_CREDENTIAL = "alice:alice-secret";
  private static final String AUDIENCE = "service1";
  private static final String METALAKE = "spark35_irc_oauth";
  private static final String CATALOG = "iceberg_pg";
  private static final String SCHEMA = "aws_vending";
  private static final String TABLE = "spark35_test";
  private static final String ROLE = "spark35-irc-role";
  private static final String AWS_ROLE_ARN = System.getenv("AWS_ROLE_ARN");

  private final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private JwksMockServerHelper mockServer;
  private SparkSession spark;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    initializeOAuthServer();
    initializeGravitinoConfig();
    initializePostgreSQL();
    super.startIntegrationTest();
    initializeMetadata();
    spark = createSparkSession();
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    if (spark != null) {
      spark.stop();
    }
    if (mockServer != null) {
      mockServer.close();
    }
    super.stopIntegrationTest();
  }

  @Test
  void testPostgreSQLCatalogRoutesThroughOAuthIrcAndVendsS3Credentials() {
    CatalogPlugin catalogPlugin = spark.sessionState().catalogManager().catalog(CATALOG);
    Assertions.assertInstanceOf(GravitinoIcebergCatalog.class, catalogPlugin);
    org.apache.iceberg.catalog.Catalog icebergCatalog =
        ((GravitinoIcebergCatalog) catalogPlugin).icebergCatalog();
    Assertions.assertEquals(
        "org.apache.iceberg.rest.RESTCatalog", icebergCatalog.getClass().getName());

    spark.sql(String.format("DROP TABLE IF EXISTS %s.%s.%s", CATALOG, SCHEMA, TABLE));
    spark.sql(
        String.format(
            "CREATE TABLE %s.%s.%s (id BIGINT, data STRING) USING iceberg",
            CATALOG, SCHEMA, TABLE));
    spark.sql(
        String.format(
            "INSERT INTO %s.%s.%s VALUES (1, 'one'), (2, 'two')", CATALOG, SCHEMA, TABLE));

    List<Row> rows =
        spark
            .sql(String.format("SELECT id, data FROM %s.%s.%s ORDER BY id", CATALOG, SCHEMA, TABLE))
            .collectAsList();
    Assertions.assertEquals(2, rows.size());
    Assertions.assertEquals(1L, rows.get(0).getLong(0));
    Assertions.assertEquals("one", rows.get(0).getString(1));
    Assertions.assertEquals(2L, rows.get(1).getLong(0));
    Assertions.assertEquals("two", rows.get(1).getString(1));

    Optional<Owner> owner =
        client
            .loadMetalake(METALAKE)
            .getOwner(
                MetadataObjects.of(
                    ImmutableList.of(CATALOG, SCHEMA, TABLE), MetadataObject.Type.TABLE));
    Assertions.assertTrue(owner.isPresent());
    Assertions.assertEquals(ALICE, owner.get().name());

    spark.sql(String.format("DROP TABLE %s.%s.%s PURGE", CATALOG, SCHEMA, TABLE));
  }

  private void initializeOAuthServer() throws Exception {
    mockServer = JwksMockServerHelper.create("spark35-irc-kid");
    Instant expiration = Instant.now().plusSeconds(3600);
    String adminToken = mockServer.mintToken(ADMIN, AUDIENCE, expiration);
    String aliceToken = mockServer.mintToken(ALICE, AUDIENCE, expiration);
    mockServer.registerUserToken(ADMIN, adminToken);
    mockServer.registerUserToken(ALICE, aliceToken);
    mockServer.setFallbackToken(adminToken);
    OAuthMockDataProvider.getInstance().setTokenData(adminToken.getBytes(StandardCharsets.UTF_8));
  }

  private void initializeGravitinoConfig() {
    ignoreIcebergAuxRestService = false;
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.AUTHENTICATORS.getKey(), AuthenticatorType.OAUTH.name().toLowerCase());
    configs.put(OAuthConfig.SERVICE_AUDIENCE.getKey(), AUDIENCE);
    configs.put(
        OAuthConfig.TOKEN_VALIDATOR_CLASS.getKey(),
        "org.apache.gravitino.server.authentication.JwksTokenValidator");
    configs.put(OAuthConfig.JWKS_URI.getKey(), mockServer.jwksUri());
    configs.put(OAuthConfig.PRINCIPAL_FIELDS.getKey(), "sub");
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), "true");
    configs.put(Configs.SERVICE_ADMINS.getKey(), ADMIN);
    configs.put("gravitino.iceberg-rest.catalog-config-provider", "dynamic-config-provider");
    configs.put("gravitino.iceberg-rest.gravitino-metalake", METALAKE);
    registerCustomConfigs(configs);
  }

  private void initializePostgreSQL() {
    containerSuite.startPostgreSQLContainer(
        TestDatabaseName.PG_TEST_ICEBERG_CATALOG_MULTIPLE_JDBC_LOAD);
  }

  private void initializeMetadata() throws SQLException {
    PostgreSQLContainer postgres = containerSuite.getPostgreSQLContainer();
    TestDatabaseName database = TestDatabaseName.PG_TEST_ICEBERG_CATALOG_MULTIPLE_JDBC_LOAD;
    client.createMetalake(METALAKE, "", new HashMap<>());
    GravitinoMetalake metalake = client.loadMetalake(METALAKE);
    metalake.addUser(ALICE);

    Map<String, String> properties = Maps.newHashMap();
    properties.put("catalog-backend", "jdbc");
    properties.put("uri", postgres.getJdbcUrl(database));
    properties.put("jdbc-driver", postgres.getDriverClassName(database));
    properties.put("jdbc-user", postgres.getUsername());
    properties.put("jdbc-password", postgres.getPassword());
    properties.put(
        "warehouse",
        String.format(
            "s3://%s/gravitino-irc-oauth-demo/spark-3.5-pg", System.getenv("AWS_S3_TEST_BUCKET")));
    properties.put("credential-providers", "s3-token");
    properties.put("s3-access-key-id", System.getenv("AWS_ACCESS_KEY_ID"));
    properties.put("s3-secret-access-key", System.getenv("AWS_SECRET_ACCESS_KEY"));
    properties.put("s3-region", System.getenv("AWS_DEFAULT_REGION"));
    properties.put("s3-role-arn", AWS_ROLE_ARN);
    properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
    Catalog catalog =
        metalake.createCatalog(
            CATALOG, Catalog.Type.RELATIONAL, "lakehouse-iceberg", "", properties);
    catalog.asSchemas().createSchema(SCHEMA, "", new HashMap<>());

    SecurableObject access =
        SecurableObjects.ofCatalog(
            CATALOG,
            ImmutableList.of(
                Privileges.UseCatalog.allow(),
                Privileges.UseSchema.allow(),
                Privileges.CreateTable.allow(),
                Privileges.ModifyTable.allow(),
                Privileges.SelectTable.allow()));
    metalake.createRole(ROLE, new HashMap<>(), ImmutableList.of(access));
    metalake.grantRolesToUser(ImmutableList.of(ROLE), ALICE);
  }

  private SparkSession createSparkSession() {
    SparkConf conf =
        new SparkConf()
            .set("spark.plugins", GravitinoSparkPlugin.class.getName())
            .set(GravitinoSparkConfig.GRAVITINO_URI, serverUri)
            .set(GravitinoSparkConfig.GRAVITINO_METALAKE, METALAKE)
            .set(GravitinoSparkConfig.GRAVITINO_ENABLE_ICEBERG_SUPPORT, "true")
            .set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, "oauth2")
            .set(GravitinoSparkConfig.GRAVITINO_OAUTH2_URI, mockServer.baseUri())
            .set(GravitinoSparkConfig.GRAVITINO_OAUTH2_PATH, "token")
            .set(GravitinoSparkConfig.GRAVITINO_OAUTH2_CREDENTIAL, ALICE_CREDENTIAL)
            .set(GravitinoSparkConfig.GRAVITINO_OAUTH2_SCOPE, "openid");
    return SparkSession.builder()
        .master("local[1]")
        .appName("SparkIcebergCatalogRestS3CredentialVendingIT35")
        .config(conf)
        .getOrCreate();
  }
}
