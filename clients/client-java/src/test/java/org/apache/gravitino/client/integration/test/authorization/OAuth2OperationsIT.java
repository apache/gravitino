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
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.util.Base64;
import java.util.Date;
import java.util.Map;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.client.GravitinoVersion;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.integration.test.util.OAuthMockDataProvider;
import org.apache.gravitino.server.authentication.OAuthConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class OAuth2OperationsIT extends BaseIT {

  private static final KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);
  private static final String publicKey =
      new String(
          Base64.getEncoder().encode(keyPair.getPublic().getEncoded()), StandardCharsets.UTF_8);

  private static String token;

  @SuppressWarnings("JavaUtilDate")
  @BeforeAll
  public void startIntegrationTest() throws Exception {
    Map<String, String> configs = Maps.newHashMap();
    token =
        Jwts.builder()
            .setSubject("gravitino")
            .setExpiration(new Date(System.currentTimeMillis() + 1000 * 1000))
            .setAudience("service1")
            .signWith(keyPair.getPrivate(), SignatureAlgorithm.RS256)
            .compact();
    configs.put(Configs.AUTHENTICATORS.getKey(), AuthenticatorType.OAUTH.name().toLowerCase());
    configs.put(OAuthConfig.SERVICE_AUDIENCE.getKey(), "service1");
    configs.put(OAuthConfig.DEFAULT_SIGN_KEY.getKey(), publicKey);
    configs.put(OAuthConfig.ALLOW_SKEW_SECONDS.getKey(), "6");
    configs.put(OAuthConfig.DEFAULT_SERVER_URI.getKey(), "test");
    configs.put(OAuthConfig.DEFAULT_TOKEN_PATH.getKey(), "test");

    registerCustomConfigs(configs);
    OAuthMockDataProvider mockDataProvider = OAuthMockDataProvider.getInstance();
    mockDataProvider.setTokenData(token.getBytes(StandardCharsets.UTF_8));
    super.startIntegrationTest();
  }

  @Test
  public void testAuthenticationApi() throws Exception {
    GravitinoVersion gravitinoVersion = client.serverVersion();
    Assertions.assertEquals(System.getenv("PROJECT_VERSION"), gravitinoVersion.version());
    Assertions.assertFalse(gravitinoVersion.compileDate().isEmpty());
    if (testMode.equals(ITUtils.EMBEDDED_TEST_MODE)) {
      final String gitCommitId = readGitCommitIdFromGitFile();
      Assertions.assertEquals(gitCommitId, gravitinoVersion.gitCommit());
    }
  }
}
