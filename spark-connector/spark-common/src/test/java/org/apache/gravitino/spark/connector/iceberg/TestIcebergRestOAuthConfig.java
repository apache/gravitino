/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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

package org.apache.gravitino.spark.connector.iceberg;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests automatic reuse of Gravitino OAuth2 client settings for Iceberg REST catalogs. */
public class TestIcebergRestOAuthConfig {

  @Test
  void testDerivesIcebergOAuthConfig() {
    SparkConf sparkConf = oauthSparkConf("https://identity.example.com/", "/oauth/token");

    Map<String, String> result = IcebergRestOAuthConfig.resolve(sparkConf, Collections.emptyMap());

    Assertions.assertEquals("oauth2", result.get(IcebergRestOAuthConfig.AUTH_TYPE));
    Assertions.assertEquals("alice:secret", result.get(IcebergRestOAuthConfig.CREDENTIAL));
    Assertions.assertEquals("openid", result.get(IcebergRestOAuthConfig.SCOPE));
    Assertions.assertEquals(
        "https://identity.example.com/oauth/token",
        result.get(IcebergRestOAuthConfig.OAUTH2_SERVER_URI));
  }

  @Test
  void testExplicitRestAuthenticationTakesPrecedence() {
    SparkConf sparkConf = oauthSparkConf("https://identity.example.com", "oauth/token");
    Map<String, String> explicit =
        ImmutableMap.of("rest.auth.type", "basic", "rest.auth.basic.username", "admin");

    Map<String, String> result = IcebergRestOAuthConfig.resolve(sparkConf, explicit);

    Assertions.assertEquals(explicit, result);
  }

  @Test
  void testExplicitOAuthCredentialOverridesReusedCredential() {
    SparkConf sparkConf = oauthSparkConf("https://identity.example.com", "oauth/token");
    Map<String, String> explicit = ImmutableMap.of("credential", "irc:secret");

    Map<String, String> result = IcebergRestOAuthConfig.resolve(sparkConf, explicit);

    Assertions.assertEquals("oauth2", result.get(IcebergRestOAuthConfig.AUTH_TYPE));
    Assertions.assertEquals("irc:secret", result.get(IcebergRestOAuthConfig.CREDENTIAL));
    Assertions.assertEquals("openid", result.get(IcebergRestOAuthConfig.SCOPE));
    Assertions.assertEquals(
        "https://identity.example.com/oauth/token",
        result.get(IcebergRestOAuthConfig.OAUTH2_SERVER_URI));
  }

  @Test
  void testAllExplicitOAuthPropertiesOverrideReusedProperties() {
    SparkConf sparkConf = oauthSparkConf("https://identity.example.com", "oauth/token");
    Map<String, String> explicit =
        ImmutableMap.of(
            IcebergRestOAuthConfig.AUTH_TYPE,
            "oauth2",
            IcebergRestOAuthConfig.CREDENTIAL,
            "irc:secret",
            IcebergRestOAuthConfig.SCOPE,
            "irc-scope",
            IcebergRestOAuthConfig.OAUTH2_SERVER_URI,
            "https://irc-identity.example.com/token");

    Map<String, String> result = IcebergRestOAuthConfig.resolve(sparkConf, explicit);

    Assertions.assertEquals(explicit, result);
  }

  @Test
  void testPartialOAuthConfigFailsWhenReuseIsUnavailable() {
    SparkConf sparkConf = new SparkConf(false);
    Map<String, String> explicit = ImmutableMap.of("credential", "irc:secret");

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> IcebergRestOAuthConfig.resolve(sparkConf, explicit));

    // scope is not required: Iceberg itself defaults it. oauth2-server-uri is still required.
    Assertions.assertTrue(exception.getMessage().contains("oauth2-server-uri"));
  }

  @Test
  void testOAuthConfigWithoutScopeSucceeds() {
    SparkConf sparkConf = new SparkConf(false);
    Map<String, String> explicit =
        ImmutableMap.of(
            "credential",
            "irc:secret",
            "oauth2-server-uri",
            "https://irc-identity.example.com/token");

    Map<String, String> result = IcebergRestOAuthConfig.resolve(sparkConf, explicit);

    Assertions.assertEquals("oauth2", result.get(IcebergRestOAuthConfig.AUTH_TYPE));
    Assertions.assertEquals("irc:secret", result.get(IcebergRestOAuthConfig.CREDENTIAL));
    Assertions.assertEquals(
        "https://irc-identity.example.com/token",
        result.get(IcebergRestOAuthConfig.OAUTH2_SERVER_URI));
    Assertions.assertFalse(result.containsKey(IcebergRestOAuthConfig.SCOPE));
  }

  @Test
  void testNamespacedOAuthTokenIsKeptIndependent() {
    SparkConf sparkConf = oauthSparkConf("https://identity.example.com", "oauth/token");
    Map<String, String> explicit =
        ImmutableMap.of("rest.auth.type", "oauth2", "rest.auth.oauth2.token", "irc-token");

    Map<String, String> result = IcebergRestOAuthConfig.resolve(sparkConf, explicit);

    Assertions.assertEquals(explicit, result);
  }

  @Test
  void testLegacyBearerTokenIsKeptIndependent() {
    SparkConf sparkConf = oauthSparkConf("https://identity.example.com", "oauth/token");
    Map<String, String> explicit = ImmutableMap.of("token", "irc-token");

    Map<String, String> result = IcebergRestOAuthConfig.resolve(sparkConf, explicit);

    Assertions.assertEquals(explicit, result);
  }

  @Test
  void testNonAuthenticationRestPropertiesDoNotDisableAutomaticReuse() {
    SparkConf sparkConf = oauthSparkConf("https://identity.example.com", "oauth/token");

    Map<String, String> result =
        IcebergRestOAuthConfig.resolve(
            sparkConf, ImmutableMap.of("header.X-Iceberg-Custom", "value"));

    Assertions.assertEquals("value", result.get("header.X-Iceberg-Custom"));
    Assertions.assertEquals("oauth2", result.get(IcebergRestOAuthConfig.AUTH_TYPE));
    Assertions.assertEquals("alice:secret", result.get(IcebergRestOAuthConfig.CREDENTIAL));
  }

  @Test
  void testCanDisableOAuthReuse() {
    SparkConf sparkConf = oauthSparkConf("https://identity.example.com", "oauth/token");
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_ICEBERG_REUSE_OAUTH2, "false");

    Map<String, String> result = IcebergRestOAuthConfig.resolve(sparkConf, Collections.emptyMap());

    Assertions.assertTrue(result.isEmpty());
  }

  @Test
  void testDoesNotDeriveConfigForOtherAuthenticationTypes() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, "simple");

    Map<String, String> result = IcebergRestOAuthConfig.resolve(sparkConf, Collections.emptyMap());

    Assertions.assertTrue(result.isEmpty());
  }

  @Test
  void testReuseFailsWhenGravitinoOAuthConfigIsIncomplete() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, "oauth2");
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_OAUTH2_CREDENTIAL, "alice:secret");
    // GRAVITINO_OAUTH2_URI/PATH are intentionally left unset, so oauth2-server-uri can't be
    // derived: this is the default-reuse path, not an explicit override, and must still validate.

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> IcebergRestOAuthConfig.resolve(sparkConf, Collections.emptyMap()));

    Assertions.assertTrue(exception.getMessage().contains("oauth2-server-uri"));
  }

  @Test
  void testDerivesIcebergBasicConfig() {
    SparkConf sparkConf = basicSparkConf("alice", "secret");

    Map<String, String> result = IcebergRestOAuthConfig.resolve(sparkConf, Collections.emptyMap());

    Assertions.assertEquals("basic", result.get(IcebergRestOAuthConfig.AUTH_TYPE));
    Assertions.assertEquals("alice", result.get(IcebergRestOAuthConfig.BASIC_USERNAME));
    Assertions.assertEquals("secret", result.get(IcebergRestOAuthConfig.BASIC_PASSWORD));
  }

  @Test
  void testExplicitConfigTakesPrecedenceOverReusedBasic() {
    SparkConf sparkConf = basicSparkConf("alice", "secret");
    Map<String, String> explicit =
        ImmutableMap.of("rest.auth.basic.username", "admin", "rest.auth.basic.password", "pwd");

    Map<String, String> result = IcebergRestOAuthConfig.resolve(sparkConf, explicit);

    Assertions.assertEquals(explicit, result);
  }

  @Test
  void testExplicitTokenSkipsBasicReuse() {
    SparkConf sparkConf = basicSparkConf("alice", "secret");
    Map<String, String> explicit = ImmutableMap.of("token", "bearer-token");

    Map<String, String> result = IcebergRestOAuthConfig.resolve(sparkConf, explicit);

    Assertions.assertEquals(explicit, result);
  }

  @Test
  void testBasicReuseFailsWhenCredentialsAreIncomplete() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, "basic");
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_BASIC_USERNAME, "alice");

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> IcebergRestOAuthConfig.resolve(sparkConf, Collections.emptyMap()));

    Assertions.assertTrue(exception.getMessage().contains("basic.password"));
  }

  @Test
  void testDerivesIcebergTokenConfig() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, "token");
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, " bearer-token ");

    Map<String, String> result = IcebergRestOAuthConfig.resolve(sparkConf, Collections.emptyMap());

    Assertions.assertEquals(ImmutableMap.of("token", "bearer-token"), result);
  }

  @Test
  void testExplicitConfigTakesPrecedenceOverReusedToken() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, "token");
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, "bearer-token");
    Map<String, String> explicit = ImmutableMap.of("token", "irc-token");

    Map<String, String> result = IcebergRestOAuthConfig.resolve(sparkConf, explicit);

    Assertions.assertEquals(explicit, result);
  }

  @Test
  void testTokenReuseFailsWhenTokenIsMissing() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, "token");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> IcebergRestOAuthConfig.resolve(sparkConf, Collections.emptyMap()));
  }

  @Test
  void testKerberosFailsFast() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, "kerberos");

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> IcebergRestOAuthConfig.resolve(sparkConf, Collections.emptyMap()));

    Assertions.assertTrue(exception.getMessage().contains("kerberos"));
  }

  @Test
  void testExplicitConfigSkipsKerberosFailure() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, "kerberos");
    Map<String, String> explicit = ImmutableMap.of("token", "irc-token");

    Map<String, String> result = IcebergRestOAuthConfig.resolve(sparkConf, explicit);

    Assertions.assertEquals(explicit, result);
  }

  @Test
  void testExplicitLegacyOAuth2SkipsTokenAndKerberosHandling() {
    Map<String, String> explicit =
        ImmutableMap.of(
            "credential",
            "irc:secret",
            "oauth2-server-uri",
            "https://identity.example.com/oauth/token");
    for (String authType : new String[] {"token", "kerberos"}) {
      SparkConf sparkConf = new SparkConf(false);
      sparkConf.set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, authType);
      sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, "bearer-token");

      Map<String, String> result = IcebergRestOAuthConfig.resolve(sparkConf, explicit);

      Assertions.assertFalse(result.containsKey(IcebergRestOAuthConfig.TOKEN));
      Assertions.assertEquals("irc:secret", result.get(IcebergRestOAuthConfig.CREDENTIAL));
    }
  }

  private SparkConf basicSparkConf(String username, String password) {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, "basic");
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_BASIC_USERNAME, username);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_BASIC_PASSWORD, password);
    return sparkConf;
  }

  private SparkConf oauthSparkConf(String serverUri, String tokenPath) {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, "oauth2");
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_OAUTH2_URI, serverUri);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_OAUTH2_PATH, tokenPath);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_OAUTH2_CREDENTIAL, "alice:secret");
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_OAUTH2_SCOPE, "openid");
    return sparkConf;
  }
}
