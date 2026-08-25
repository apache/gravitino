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
import com.google.common.collect.ImmutableSet;
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
  void testLegacyExplicitOAuthPropertiesDisableAutomaticReuse() {
    SparkConf sparkConf = oauthSparkConf("https://identity.example.com", "oauth/token");

    for (String property :
        ImmutableSet.of(
            "token",
            "credential",
            "scope",
            "oauth2-server-uri",
            "audience",
            "resource",
            "token-refresh-enabled",
            "token-exchange-enabled")) {
      Map<String, String> explicit = ImmutableMap.of(property, "explicit-value");

      Map<String, String> result = IcebergRestOAuthConfig.resolve(sparkConf, explicit);

      Assertions.assertEquals(explicit, result, property);
    }
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
