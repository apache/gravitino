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
package org.apache.gravitino.client.integration.test.authorization;

import com.google.common.collect.Maps;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.client.DefaultOAuth2TokenProvider;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.client.GravitinoVersion;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.integration.test.util.JwksMockServerHelper;
import org.apache.gravitino.integration.test.util.OAuthMockDataProvider;
import org.apache.gravitino.server.authentication.OAuthConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration test for {@link org.apache.gravitino.server.authentication.JwksTokenValidator}.
 * Exercises the full {@code client_credentials → /token → Bearer → JWKS validation} path.
 */
public class JwksTokenValidatorIT extends BaseIT {

  private static final String SERVICE_AUDIENCE = "service1";
  private static final String SUBJECT = "gravitino";
  private static final String KEY_ID = "test-kid";
  private static final String METALAKE_NAME = "jwks_auth_metalake";
  private static final String ALICE = "alice";
  private static final String BOB = "bob";

  private static JwksMockServerHelper mockServerHelper;
  private static RSAKey rsaKey;
  private static volatile String currentToken;

  private static String validToken;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    mockServerHelper = JwksMockServerHelper.create(KEY_ID);
    rsaKey = mockServerHelper.rsaKey();
    validToken =
        JwksMockServerHelper.mintToken(
            rsaKey, SUBJECT, SERVICE_AUDIENCE, Instant.now().plusSeconds(1_000_000));
    currentToken = validToken;
    mockServerHelper.setTokenSupplier(() -> currentToken);

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
    configs.put(Configs.SERVICE_ADMINS.getKey(), SUBJECT);
    registerCustomConfigs(configs);

    OAuthMockDataProvider.getInstance().setTokenData(validToken.getBytes(StandardCharsets.UTF_8));
    super.startIntegrationTest();

    client.createMetalake(METALAKE_NAME, "JWKS auth test metalake", Maps.newHashMap());
    client.loadMetalake(METALAKE_NAME).addUser(ALICE);
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    if (mockServerHelper != null) {
      mockServerHelper.close();
    }
    super.stopIntegrationTest();
  }

  @Test
  public void testValidTokenAuthentication() throws Exception {
    withToken(
        validToken,
        oauthClient -> {
          GravitinoVersion version = oauthClient.serverVersion();
          String projectVersion = System.getenv("PROJECT_VERSION");
          if (projectVersion != null) {
            Assertions.assertEquals(projectVersion, version.version());
          }
          Assertions.assertFalse(version.compileDate().isEmpty());
          if (testMode.equals(ITUtils.EMBEDDED_TEST_MODE)) {
            Assertions.assertEquals(readGitCommitIdFromGitFile(), version.gitCommit());
          }
        });
  }

  @Test
  public void testExpiredTokenFails() throws Exception {
    String expiredToken =
        JwksMockServerHelper.mintToken(
            rsaKey, SUBJECT, SERVICE_AUDIENCE, Instant.now().minusSeconds(3600));
    withToken(
        expiredToken,
        badClient -> Assertions.assertThrows(RuntimeException.class, badClient::serverVersion));
  }

  @Test
  public void testWrongAudienceFails() throws Exception {
    String wrongAudToken =
        JwksMockServerHelper.mintToken(
            rsaKey, SUBJECT, "wrong-audience", Instant.now().plusSeconds(1_000_000));
    withToken(
        wrongAudToken,
        badClient -> Assertions.assertThrows(RuntimeException.class, badClient::serverVersion));
  }

  @Test
  public void testTokenSignedWithDifferentKeyFails() throws Exception {
    RSAKey differentKey = new RSAKeyGenerator(2048).keyID("other-kid").generate();
    String wrongKeyToken =
        JwksMockServerHelper.mintToken(
            differentKey, SUBJECT, SERVICE_AUDIENCE, Instant.now().plusSeconds(1_000_000));
    withToken(
        wrongKeyToken,
        badClient -> Assertions.assertThrows(RuntimeException.class, badClient::serverVersion));
  }

  @Test
  public void testPrivilegedUserCanAccessMetalake() throws Exception {
    String aliceToken =
        JwksMockServerHelper.mintToken(
            rsaKey, ALICE, SERVICE_AUDIENCE, Instant.now().plusSeconds(1_000_000));
    withToken(
        aliceToken,
        aliceClient -> {
          GravitinoMetalake ml = aliceClient.loadMetalake(METALAKE_NAME);
          Assertions.assertNotNull(ml);
          Assertions.assertEquals(METALAKE_NAME, ml.name());
        });
  }

  @Test
  public void testUnprivilegedUserForbidden() throws Exception {
    String bobToken =
        JwksMockServerHelper.mintToken(
            rsaKey, BOB, SERVICE_AUDIENCE, Instant.now().plusSeconds(1_000_000));
    withToken(
        bobToken,
        bobClient ->
            Assertions.assertThrows(
                ForbiddenException.class, () -> bobClient.loadMetalake(METALAKE_NAME)));
  }

  /**
   * Swaps {@code currentToken}, builds a fresh provider+client, runs {@code action}, restores.
   */
  private void withToken(String token, ClientConsumer action) throws Exception {
    String previous = currentToken;
    currentToken = token;
    DefaultOAuth2TokenProvider provider =
        DefaultOAuth2TokenProvider.builder()
            .withUri(mockServerHelper.baseUri())
            .withPath("token")
            .withCredential("test-client:test-secret")
            .withScope("openid")
            .build();
    try (GravitinoAdminClient tempClient =
        GravitinoAdminClient.builder(serverUri).withOAuth(provider).build()) {
      action.accept(tempClient);
    } finally {
      currentToken = previous;
    }
  }

  @FunctionalInterface
  private interface ClientConsumer {
    void accept(GravitinoAdminClient client) throws Exception;
  }
}
