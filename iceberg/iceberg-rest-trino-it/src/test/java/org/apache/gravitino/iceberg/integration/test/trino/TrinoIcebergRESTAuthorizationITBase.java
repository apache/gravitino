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
package org.apache.gravitino.iceberg.integration.test.trino;

import static io.trino.testing.TestingSession.testSessionBuilder;

import com.google.common.collect.ImmutableMap;
import io.jsonwebtoken.Jwts;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyPair;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.integration.test.util.OAuthMockDataProvider;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.server.authentication.OAuthConfig;
import org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Boots a deploy-mode Gravitino server with the Iceberg REST auxiliary service, OAuth
 * authentication, and authorization enabled.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TrinoIcebergRESTAuthorizationITBase extends BaseIT {

  private static final Logger LOG =
      LoggerFactory.getLogger(TrinoIcebergRESTAuthorizationITBase.class);
  private static final String GRAVITINO_ICEBERG_REST_PREFIX = "gravitino.iceberg-rest.";

  protected static final String METALAKE_NAME = "test_metalake";
  protected static final String CATALOG_NAME = "iceberg";
  protected static final String SERVICE_AUDIENCE = "gravitino-irc";
  protected static final String SUPER_USER = "super";
  protected static final String NORMAL_USER = "normal";
  protected static final String SUPER_CATALOG = "iceberg_super";
  protected static final String NORMAL_CATALOG = "iceberg_normal";

  protected final KeyPair keyPair = MockOAuthTokenServer.newRsaKeyPair();
  protected MockOAuthTokenServer tokenServer;
  protected String warehouseDir;
  protected GravitinoMetalake metalakeAsSuper;
  protected Catalog catalogAsSuper;
  protected QueryRunner queryRunner;

  /**
   * Mints a signed bearer token for a test user.
   *
   * @param user token subject
   * @return an RS256-signed JWT
   */
  @SuppressWarnings("JavaUtilDate")
  protected String mintToken(String user) {
    return Jwts.builder()
        .subject(user)
        .audience()
        .add(SERVICE_AUDIENCE)
        .and()
        .expiration(new Date(System.currentTimeMillis() + 60 * 60 * 1000))
        .signWith(keyPair.getPrivate(), Jwts.SIG.RS256)
        .compact();
  }

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    // This IT boots the Gravitino server with the Iceberg REST auxiliary service and drives it
    // through Trino's native REST connector, which only works against the packaged distribution in
    // deploy mode. The Java 24 / Trino module does not carry the server on its classpath, so the
    // embedded path would fail with NoClassDefFoundError; skip it when not running in deploy mode.
    Assumptions.assumeFalse(
        ITUtils.isEmbedded(), "Trino Iceberg REST authorization IT only runs in deploy mode");
    containerSuite.startPostgreSQLContainer(TestDatabaseName.PG_ICEBERG_AUTHZ_IT);
    tokenServer = new MockOAuthTokenServer(keyPair, SERVICE_AUDIENCE, SUPER_USER);
    tokenServer.start();
    warehouseDir = Files.createTempDirectory("trino-irc-warehouse").toString();

    String publicKey =
        new String(
            Base64.getEncoder().encode(keyPair.getPublic().getEncoded()), StandardCharsets.UTF_8);

    customConfigs.putAll(
        ImmutableMap.of(
            Configs.AUTHENTICATORS.getKey(),
            "oauth",
            OAuthConfig.SERVICE_AUDIENCE.getKey(),
            SERVICE_AUDIENCE,
            OAuthConfig.DEFAULT_SIGN_KEY.getKey(),
            publicKey,
            OAuthConfig.ALLOW_SKEW_SECONDS.getKey(),
            "30",
            OAuthConfig.DEFAULT_SERVER_URI.getKey(),
            tokenServer.serverUri(),
            OAuthConfig.DEFAULT_TOKEN_PATH.getKey(),
            tokenServer.tokenPath()));
    customConfigs.putAll(
        ImmutableMap.of(
            "gravitino.authorization.serviceAdmins",
            SUPER_USER,
            Configs.ENABLE_AUTHORIZATION.getKey(),
            "true",
            Configs.AUTHORIZATION_IMPL.getKey(),
            JcasbinAuthorizer.class.getCanonicalName(),
            Configs.CACHE_ENABLED.getKey(),
            "false"));

    ignoreIcebergAuxRestService = false;
    Map<String, String> ircConfigs = new HashMap<>();
    ircConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.ICEBERG_REST_CATALOG_CONFIG_PROVIDER,
        IcebergConstants.DYNAMIC_ICEBERG_CATALOG_CONFIG_PROVIDER_NAME);
    ircConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_METALAKE, METALAKE_NAME);
    ircConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.ICEBERG_REST_DEFAULT_DYNAMIC_CATALOG_NAME,
        CATALOG_NAME);
    ircConfigs.put(GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_AUTH_TYPE, "oauth");
    ircConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_OAUTH2_SERVER_URI,
        tokenServer.serverUri());
    ircConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_OAUTH2_TOKEN_PATH,
        tokenServer.tokenPath());
    ircConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_OAUTH2_CREDENTIAL,
        "test-client:test-secret");
    ircConfigs.put(GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_OAUTH2_SCOPE, "test");
    customConfigs.putAll(ircConfigs);

    OAuthMockDataProvider.getInstance()
        .setTokenData(mintToken(SUPER_USER).getBytes(StandardCharsets.UTF_8));

    super.startIntegrationTest();
    initMetalakeAndCatalog();
  }

  private void initMetalakeAndCatalog() {
    metalakeAsSuper = client.createMetalake(METALAKE_NAME, "", new HashMap<>());
    metalakeAsSuper.addUser(NORMAL_USER);
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
            .put(IcebergConstants.WAREHOUSE, "file://" + warehouseDir)
            .build();
    catalogAsSuper =
        metalakeAsSuper.createCatalog(
            CATALOG_NAME, Catalog.Type.RELATIONAL, "lakehouse-iceberg", "comment", properties);
    LOG.info("Created metalake {} and catalog {}", METALAKE_NAME, CATALOG_NAME);
  }

  /** Starts Trino with one native Iceberg REST catalog for each test identity. */
  protected void startTrino() throws Exception {
    queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build()).build();
    queryRunner.installPlugin(new IcebergPlugin());
    queryRunner.createCatalog(
        SUPER_CATALOG, "iceberg", icebergCatalogConfig(mintToken(SUPER_USER)));
    queryRunner.createCatalog(
        NORMAL_CATALOG, "iceberg", icebergCatalogConfig(mintToken(NORMAL_USER)));
  }

  /** Executes SQL through the selected Trino Iceberg REST catalog. */
  protected MaterializedResult sql(String catalog, String query) {
    return queryRunner.execute(testSessionBuilder().setCatalog(catalog).build(), query);
  }

  private Map<String, String> icebergCatalogConfig(String bearerToken) {
    Map<String, String> config = new HashMap<>();
    config.put("iceberg.catalog.type", "rest");
    config.put("iceberg.rest-catalog.uri", getIcebergRestServiceUri());
    config.put("iceberg.rest-catalog.security", "OAUTH2");
    config.put("iceberg.rest-catalog.oauth2.token", bearerToken);
    config.put("fs.hadoop.enabled", "true");
    return config;
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    try {
      if (queryRunner != null) {
        queryRunner.close();
      }
      try {
        if (client != null) {
          client.dropMetalake(METALAKE_NAME, true);
        }
      } catch (Exception e) {
        LOG.warn("Failed to drop test metalake", e);
      }
    } finally {
      if (tokenServer != null) {
        tokenServer.close();
      }
      try {
        super.stopIntegrationTest();
      } finally {
        containerSuite.close();
      }
    }
  }
}
