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
 * End-to-end integration test for Spark OAuth2 authentication via JWKS. Exercises the full path:
 * Spark plugin → {@code client_credentials} → JWT fetch → Gravitino REST → JWKS validation.
 */
public class SparkJwksAuthIT extends BaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(SparkJwksAuthIT.class);

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
    LOG.info("Embedded JWKS+token server started on port {}", mockServerHelper.port());

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

    // Start the Spark session with OAuth2 pointing to the mock /token endpoint.
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

  @Test
  public void testSparkConnectsWithJwksAuth() {
    List<Row> catalogs = sparkSession.sql("SHOW CATALOGS").collectAsList();
    Assertions.assertFalse(catalogs.isEmpty(), "SHOW CATALOGS should return at least one catalog");
    LOG.info("SHOW CATALOGS returned {} rows", catalogs.size());
  }
}
