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
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
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
import org.apache.gravitino.integration.test.util.OAuthMockDataProvider;
import org.apache.gravitino.server.authentication.OAuthConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration test for {@link org.apache.gravitino.server.authentication.JwksTokenValidator}.
 *
 * <p>Runs two in-process HTTP endpoints on a single random port (zero additional dependencies):
 *
 * <ul>
 *   <li>{@code /jwks} – serves the JWKS public key consumed by Gravitino's {@code
 *       JwksTokenValidator}.
 *   <li>{@code /token} – mock OAuth2 client-credentials endpoint; always returns {@code
 *       currentToken}, so swapping that field changes what the next {@link
 *       DefaultOAuth2TokenProvider} fetch will return.
 * </ul>
 *
 * <p>All test assertions (positive and negative) go through the full {@code client_credentials →
 * /token fetch → Bearer → Gravitino JWKS validation} path via {@link DefaultOAuth2TokenProvider}.
 */
public class JwksTokenValidatorIT extends BaseIT {

  private static final String SERVICE_AUDIENCE = "service1";
  private static final String SUBJECT = "gravitino";
  private static final String KEY_ID = "test-kid";
  private static final String METALAKE_NAME = "jwks_auth_metalake";
  private static final String ALICE = "alice";
  private static final String BOB = "bob";

  private static HttpServer mockServer;
  private static RSAKey rsaKey;
  /** What the {@code /token} endpoint returns; swap this to inject a different JWT. */
  private static volatile String currentToken;

  private static String validToken;
  private static String mockServerBaseUri;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    rsaKey = new RSAKeyGenerator(2048).keyID(KEY_ID).generate();
    validToken = mintToken(rsaKey, SUBJECT, SERVICE_AUDIENCE, Instant.now().plusSeconds(1_000_000));
    currentToken = validToken;

    String jwksJson = new JWKSet(rsaKey.toPublicJWK()).toString();
    mockServer = HttpServer.create(new InetSocketAddress(0), 0);

    // /jwks – serves the JWKS public key; consumed by JwksTokenValidator on the server.
    mockServer.createContext(
        "/jwks",
        exchange -> {
          byte[] body = jwksJson.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.getResponseBody().close();
        });

    // /token – mock OAuth2 client-credentials endpoint; returns currentToken.
    // DefaultOAuth2TokenProvider posts client_id:secret here and receives the JWT.
    mockServer.createContext(
        "/token",
        exchange -> {
          exchange.getRequestBody().readAllBytes(); // drain POST body
          String json =
              "{\"access_token\":\""
                  + currentToken
                  + "\",\"token_type\":\"bearer\",\"expires_in\":86400}";
          byte[] body = json.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.getResponseBody().close();
        });

    mockServer.start();
    int port = mockServer.getAddress().getPort();
    mockServerBaseUri = "http://localhost:" + port;

    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.AUTHENTICATORS.getKey(), AuthenticatorType.OAUTH.name().toLowerCase());
    configs.put(OAuthConfig.SERVICE_AUDIENCE.getKey(), SERVICE_AUDIENCE);
    configs.put(
        OAuthConfig.TOKEN_VALIDATOR_CLASS.getKey(),
        "org.apache.gravitino.server.authentication.JwksTokenValidator");
    configs.put(OAuthConfig.JWKS_URI.getKey(), mockServerBaseUri + "/jwks");
    configs.put(OAuthConfig.PRINCIPAL_FIELDS.getKey(), "sub");
    configs.put(OAuthConfig.ALLOW_SKEW_SECONDS.getKey(), "6");
    // Enable authorization so that user identity (extracted from JWT sub) is enforced.
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), "true");
    // SUBJECT ("gravitino") is the service admin: the BaseIT internal client runs as this user.
    configs.put(Configs.SERVICE_ADMINS.getKey(), SUBJECT);
    registerCustomConfigs(configs);

    // Prime OAuthMockDataProvider so that BaseIT can build its internal `client` successfully.
    OAuthMockDataProvider.getInstance().setTokenData(validToken.getBytes(StandardCharsets.UTF_8));
    super.startIntegrationTest();

    // Create a metalake and add alice so authorization tests have something to verify.
    // bob is intentionally NOT added — his token should be rejected with ForbiddenException.
    client.createMetalake(METALAKE_NAME, "JWKS auth test metalake", Maps.newHashMap());
    client.loadMetalake(METALAKE_NAME).addUser(ALICE);
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    if (mockServer != null) {
      mockServer.stop(0);
    }
    super.stopIntegrationTest();
  }

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  /**
   * Full happy path: {@code DefaultOAuth2TokenProvider} posts {@code client_id:secret} to {@code
   * /token}, receives a valid RS256 JWT, then sends it as a Bearer token to Gravitino, which
   * validates the signature against {@code /jwks}.
   */
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

  /** The mock {@code /token} returns an already-expired JWT; Gravitino must reject it. */
  @Test
  public void testExpiredTokenFails() throws Exception {
    String expiredToken =
        mintToken(rsaKey, SUBJECT, SERVICE_AUDIENCE, Instant.now().minusSeconds(3600));
    withToken(
        expiredToken,
        badClient -> Assertions.assertThrows(RuntimeException.class, badClient::serverVersion));
  }

  /** The mock {@code /token} returns a JWT with the wrong audience; Gravitino must reject it. */
  @Test
  public void testWrongAudienceFails() throws Exception {
    String wrongAudToken =
        mintToken(rsaKey, SUBJECT, "wrong-audience", Instant.now().plusSeconds(1_000_000));
    withToken(
        wrongAudToken,
        badClient -> Assertions.assertThrows(RuntimeException.class, badClient::serverVersion));
  }

  /**
   * The mock {@code /token} returns a JWT signed by a different RSA key whose public key is NOT in
   * the JWKS endpoint; Gravitino must reject the signature.
   */
  @Test
  public void testTokenSignedWithDifferentKeyFails() throws Exception {
    RSAKey differentKey = new RSAKeyGenerator(2048).keyID("other-kid").generate();
    String wrongKeyToken =
        mintToken(differentKey, SUBJECT, SERVICE_AUDIENCE, Instant.now().plusSeconds(1_000_000));
    withToken(
        wrongKeyToken,
        badClient -> Assertions.assertThrows(RuntimeException.class, badClient::serverVersion));
  }

  /**
   * Verifies that a JWT with {@code sub=alice} is accepted and that alice (who was added to the
   * metalake) can load it. Proves that Gravitino extracts the username from the JWT {@code sub}
   * claim and enforces authorization based on it.
   */
  @Test
  public void testPrivilegedUserCanAccessMetalake() throws Exception {
    String aliceToken =
        mintToken(rsaKey, ALICE, SERVICE_AUDIENCE, Instant.now().plusSeconds(1_000_000));
    withToken(
        aliceToken,
        aliceClient -> {
          GravitinoMetalake ml = aliceClient.loadMetalake(METALAKE_NAME);
          Assertions.assertNotNull(ml);
          Assertions.assertEquals(METALAKE_NAME, ml.name());
        });
  }

  /**
   * Verifies that a JWT with {@code sub=bob} is cryptographically valid but bob has no access to
   * the metalake — Gravitino must throw {@link ForbiddenException}. Proves that authorization is
   * enforced per-user based on the JWT {@code sub} claim.
   */
  @Test
  public void testUnprivilegedUserForbidden() throws Exception {
    String bobToken =
        mintToken(rsaKey, BOB, SERVICE_AUDIENCE, Instant.now().plusSeconds(1_000_000));
    withToken(
        bobToken,
        bobClient ->
            Assertions.assertThrows(
                ForbiddenException.class, () -> bobClient.loadMetalake(METALAKE_NAME)));
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Mints a signed RS256 JWT with the given subject, audience, and expiry.
   *
   * @param key RSA key used for signing (private component required)
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

  /**
   * Sets {@code currentToken} so that the next {@code /token} request returns {@code token}, builds
   * a fresh {@link DefaultOAuth2TokenProvider} (which fetches immediately on construction), wraps
   * it in a {@link GravitinoAdminClient}, runs {@code action}, then restores the previous token.
   *
   * <p>Each call creates an independent provider with no cached state, so the bad token is
   * guaranteed to be fetched fresh from the mock server.
   */
  private void withToken(String token, ClientConsumer action) throws Exception {
    String previous = currentToken;
    currentToken = token;
    DefaultOAuth2TokenProvider provider =
        DefaultOAuth2TokenProvider.builder()
            .withUri(mockServerBaseUri)
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
