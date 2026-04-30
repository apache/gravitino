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
import java.util.Map;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.client.DefaultOAuth2TokenProvider;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.JwksMockServerHelper;
import org.apache.gravitino.integration.test.util.OAuthMockDataProvider;
import org.apache.gravitino.server.authentication.OAuthConfig;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end integration test for Spark OAuth2 authentication via JWKS. Exercises the full path:
 * Spark plugin → {@code client_credentials} → JWT fetch → Gravitino REST → JWKS validation.
 */
public class SparkJwksAuthIT extends BaseIT {

  private static final String SERVICE_AUDIENCE = "service1";
  private static final String METALAKE = "jwks_test_metalake";
  private static final String KEY_ID = "test-kid";

  private static JwksMockServerHelper mockServerHelper;

  private static String validToken;
  private static SparkSession sparkSession;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    mockServerHelper = JwksMockServerHelper.create(KEY_ID);
    validToken =
        mockServerHelper.mintToken(
            "gravitino", SERVICE_AUDIENCE, Instant.now().plusSeconds(1_000_000));
    mockServerHelper.setTokenSupplier(() -> validToken);

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

    OAuthMockDataProvider.getInstance().setTokenData(validToken.getBytes(StandardCharsets.UTF_8));
    super.startIntegrationTest();

    client.createMetalake(METALAKE, "JWKS auth test metalake", Maps.newHashMap());

    SparkConf sparkConf =
        new SparkConf()
            .set("spark.plugins", GravitinoSparkPlugin.class.getName())
            .set(GravitinoSparkConfig.GRAVITINO_URI, serverUri)
            .set(GravitinoSparkConfig.GRAVITINO_METALAKE, METALAKE)
            .set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, AuthProperties.OAUTH2_AUTH_TYPE)
            .set(GravitinoSparkConfig.GRAVITINO_OAUTH2_URI, mockServerHelper.baseUri())
            .set(GravitinoSparkConfig.GRAVITINO_OAUTH2_PATH, "token")
            .set(GravitinoSparkConfig.GRAVITINO_OAUTH2_CREDENTIAL, "test-client:test-secret")
            .set(GravitinoSparkConfig.GRAVITINO_OAUTH2_SCOPE, "openid");

    sparkSession =
        SparkSession.builder()
            .master("local[1]")
            .appName("SparkJwksAuthIT")
            .config(sparkConf)
            .getOrCreate();
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

  @Test
  public void testSparkConnectsWithJwksAuth() throws Exception {
    // Trigger the Spark plugin's OAuth handshake with Gravitino on the first SQL call.
    Assertions.assertDoesNotThrow(() -> sparkSession.sql("SHOW CATALOGS").collect());

    // Verify the full client_credentials → JWKS validation path via a direct Gravitino client.
    DefaultOAuth2TokenProvider oauthProvider =
        DefaultOAuth2TokenProvider.builder()
            .withUri(mockServerHelper.baseUri())
            .withPath("token")
            .withCredential("test-client:test-secret")
            .withScope("openid")
            .build();
    try (GravitinoAdminClient oauthClient =
        GravitinoAdminClient.builder(serverUri).withOAuth(oauthProvider).build()) {
      GravitinoMetalake metalake = oauthClient.loadMetalake(METALAKE);
      Assertions.assertEquals(METALAKE, metalake.name());
    }
  }
}
