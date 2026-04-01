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

import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.JwksMockServerHelper;
import org.apache.gravitino.integration.test.util.OAuthMockDataProvider;
import org.apache.gravitino.server.authentication.OAuthConfig;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end integration test for Spark OAuth2 user authentication via JWKS token validation.
 *
 * <p>Verifies that the Spark connector ({@code GravitinoSparkPlugin}) can authenticate against
 * Gravitino using the OAuth2 {@code client_credentials} flow:
 *
 * <ol>
 *   <li>An in-process JDK {@link HttpServer} serves two endpoints on a random port:
 *       <ul>
 *         <li>{@code /jwks} – JWKS public-key document consumed by Gravitino's {@code
 *             JwksTokenValidator} to verify JWT signatures.
 *         <li>{@code /token} – Mock OAuth2 {@code client_credentials} endpoint; returns a
 *             pre-minted RS256 JWT when Spark's {@code DefaultOAuth2TokenProvider} posts its {@code
 *             client_id:secret}.
 *       </ul>
 *   <li>The embedded Gravitino server is configured with {@code JwksTokenValidator} and the JWKS
 *       URI pointing to the in-process server above.
 *   <li>A Spark session is started with {@code GravitinoSparkPlugin} and OAuth2 auth config ({@code
 *       spark.sql.gravitino.authType=oauth2}, URI, path, credential, scope) all pointing to the
 *       mock token endpoint.
 *   <li>Real Spark SQL ({@code SHOW CATALOGS}) is executed so the complete request path — Spark
 *       plugin → {@code client_credentials} POST → JWT fetch → Gravitino REST API → JWKS signature
 *       validation — is exercised without any external services or Docker.
 * </ol>
 *
 * <p>JWT validation negative tests (expired token, wrong key, wrong audience) are covered by the
 * non-Spark {@code JwksTokenValidatorIT} in the {@code client-java} module.
 */
public class SparkJwksAuthIT extends BaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(SparkJwksAuthIT.class);

  private static final String SERVICE_AUDIENCE = "service1";
  private static final String METALAKE = "jwks_test_metalake";
  private static final String KEY_ID = "test-kid";

  private static JwksMockServerHelper mockServerHelper;

  private static String validToken;
  private static SparkSession sparkSession;

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    // 1. Create the mock JWKS+token server (generates RSA key pair, random port).
    mockServerHelper = JwksMockServerHelper.create(KEY_ID);
    validToken =
        mockServerHelper.mintToken(
            "gravitino", SERVICE_AUDIENCE, Instant.now().plusSeconds(1_000_000));
    // Single-token mode: every /token request returns the pre-minted JWT.
    mockServerHelper.setTokenSupplier(() -> validToken);
    int serverPort = mockServerHelper.port();
    LOG.info("Embedded JWKS+token server started on port {}", serverPort);

    // 2. Configure the embedded Gravitino server to use JwksTokenValidator.
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.AUTHENTICATORS.getKey(), AuthenticatorType.OAUTH.name().toLowerCase());
    configs.put(OAuthConfig.SERVICE_AUDIENCE.getKey(), SERVICE_AUDIENCE);
    configs.put(
        OAuthConfig.TOKEN_VALIDATOR_CLASS.getKey(),
        "org.apache.gravitino.server.authentication.JwksTokenValidator");
    configs.put(OAuthConfig.JWKS_URI.getKey(), mockServerHelper.jwksUri());
    configs.put(OAuthConfig.PRINCIPAL_FIELDS.getKey(), "sub");
    configs.put(OAuthConfig.ALLOW_SKEW_SECONDS.getKey(), "6");
    registerCustomConfigs(configs);

    // 3. Prime OAuthMockDataProvider so that BaseIT can build its internal `client` successfully.
    OAuthMockDataProvider.getInstance().setTokenData(validToken.getBytes(StandardCharsets.UTF_8));

    // 4. Start Gravitino (BaseIT wires up `client` via OAuthMockDataProvider automatically).
    super.startIntegrationTest();

    // 5. Create a metalake so Spark has something to discover.
    client.createMetalake(METALAKE, "JWKS auth test metalake", Maps.newHashMap());

    // 6. Start the Spark session using GravitinoSparkPlugin + OAuth2 → mock /token endpoint.
    SparkConf sparkConf =
        new SparkConf()
            .set(
                "spark.plugins", GravitinoSparkPlugin.class.getName())
            .set(GravitinoSparkConfig.GRAVITINO_URI, serverUri)
            .set(GravitinoSparkConfig.GRAVITINO_METALAKE, METALAKE)
            // OAuth2 auth – Spark's DefaultOAuth2TokenProvider fetches the JWT from the mock
            // /token endpoint and sends it as a Bearer token to the Gravitino REST server.
            .set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, AuthProperties.OAUTH2_AUTH_TYPE)
            .set(GravitinoSparkConfig.GRAVITINO_OAUTH2_URI, mockServerHelper.baseUri())
            .set(GravitinoSparkConfig.GRAVITINO_OAUTH2_PATH, "token")
            // credential / scope are syntactically required by DefaultOAuth2TokenProvider;
            // the mock /token endpoint intentionally ignores them.
            .set(GravitinoSparkConfig.GRAVITINO_OAUTH2_CREDENTIAL, "test-client:test-secret")
            .set(GravitinoSparkConfig.GRAVITINO_OAUTH2_SCOPE, "openid");

    sparkSession =
        SparkSession.builder()
            .master("local[1]")
            .appName("SparkJwksAuthIT")
            .config(sparkConf)
            .getOrCreate();

    LOG.info("Spark session started. Gravitino URI: {}", serverUri);
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    if (sparkSession != null) {
      sparkSession.close();
    }
    if (mockServerHelper != null) {
      mockServerHelper.close();
    }
    super.stopIntegrationTest();
  }

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  /**
   * Verifies the full Spark OAuth2 authentication path end-to-end: Spark plugin → {@code
   * client_credentials} POST → JWT fetch from mock {@code /token} → Gravitino REST API → JWKS
   * signature validation → Spark SQL response.
   *
   * <p>If this test passes, the Spark connector correctly uses the configured OAuth2 {@code
   * client_id:secret} to obtain a JWT and send it as a Bearer token to Gravitino.
   */
  @Test
  public void testSparkConnectsWithJwksAuth() {
    // SHOW CATALOGS drives a real Gravitino REST call through the Spark plugin.
    // The plugin fetches a JWT from the mock /token endpoint (using client_id:secret)
    // and presents it as a Bearer token; Gravitino validates the signature via /jwks.
    List<Row> catalogs = sparkSession.sql("SHOW CATALOGS").collectAsList();
    Assertions.assertFalse(catalogs.isEmpty(), "SHOW CATALOGS should return at least one catalog");
    LOG.info("SHOW CATALOGS returned {} rows", catalogs.size());
  }
}
