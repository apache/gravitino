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

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.spark.SparkConf;

class IcebergRestOAuthConfig {

  static final String AUTH_TYPE = "rest.auth.type";
  static final String AUTH_TYPE_OAUTH2 = "oauth2";
  static final String TOKEN = "token";
  static final String CREDENTIAL = "credential";
  static final String OAUTH2_SERVER_URI = "oauth2-server-uri";
  static final String SCOPE = "scope";

  private static final Set<String> LEGACY_OAUTH_PROPERTIES =
      ImmutableSet.of(
          TOKEN,
          CREDENTIAL,
          SCOPE,
          OAUTH2_SERVER_URI,
          "audience",
          "resource",
          "token-refresh-enabled",
          "token-exchange-enabled");

  private IcebergRestOAuthConfig() {}

  static Map<String, String> resolve(SparkConf sparkConf, Map<String, String> explicitRestConfig) {
    Map<String, String> result = new HashMap<>(explicitRestConfig);
    String explicitAuthType = result.get(AUTH_TYPE);
    if (StringUtils.isNotBlank(explicitAuthType)
        && !AUTH_TYPE_OAUTH2.equalsIgnoreCase(explicitAuthType)) {
      return result;
    }

    // A bearer token is a complete OAuth2 authentication mechanism by itself. Preserve the legacy
    // behavior and do not require client-credential properties alongside it.
    if (StringUtils.isNotBlank(result.get(TOKEN))) {
      return result;
    }

    // Namespaced authentication settings are self-contained, including token-only OAuth2. Do not
    // mix the legacy OAuth2 client properties used by the reuse path into that configuration.
    if (result.keySet().stream()
        .anyMatch(key -> key.startsWith("rest.auth.") && !AUTH_TYPE.equals(key))) {
      return result;
    }

    String authType =
        sparkConf.get(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, AuthProperties.SIMPLE_AUTH_TYPE);
    boolean reuseOAuth2 =
        sparkConf.getBoolean(GravitinoSparkConfig.GRAVITINO_ICEBERG_REUSE_OAUTH2, true)
            && AuthProperties.isOAuth2(authType);
    boolean hasExplicitLegacyOAuth2 =
        StringUtils.equalsIgnoreCase(explicitAuthType, AUTH_TYPE_OAUTH2)
            || result.keySet().stream().anyMatch(LEGACY_OAUTH_PROPERTIES::contains);
    if (!reuseOAuth2 && !hasExplicitLegacyOAuth2) {
      return result;
    }

    result.putIfAbsent(AUTH_TYPE, AUTH_TYPE_OAUTH2);
    if (reuseOAuth2) {
      putIfConfigured(
          result,
          CREDENTIAL,
          sparkConf.get(GravitinoSparkConfig.GRAVITINO_OAUTH2_CREDENTIAL, null));
      putIfConfigured(
          result, SCOPE, sparkConf.get(GravitinoSparkConfig.GRAVITINO_OAUTH2_SCOPE, null));
      if (!result.containsKey(OAUTH2_SERVER_URI)) {
        String serverUri = sparkConf.get(GravitinoSparkConfig.GRAVITINO_OAUTH2_URI, null);
        String tokenPath = sparkConf.get(GravitinoSparkConfig.GRAVITINO_OAUTH2_PATH, null);
        if (StringUtils.isNotBlank(serverUri) && StringUtils.isNotBlank(tokenPath)) {
          result.put(OAUTH2_SERVER_URI, joinUri(serverUri, tokenPath));
        }
      }
    }

    validateLegacyOAuth2(result);
    return result;
  }

  private static void putIfConfigured(Map<String, String> config, String key, String value) {
    if (!config.containsKey(key) && StringUtils.isNotBlank(value)) {
      config.put(key, value);
    }
  }

  private static void validateLegacyOAuth2(Map<String, String> config) {
    List<String> missing = new ArrayList<>();
    for (String key : ImmutableSet.of(AUTH_TYPE, CREDENTIAL, SCOPE, OAUTH2_SERVER_URI)) {
      if (StringUtils.isBlank(config.get(key))) {
        missing.add(key);
      }
    }
    if (!missing.isEmpty()) {
      throw new IllegalArgumentException(
          "Incomplete Iceberg REST OAuth2 configuration; missing: " + String.join(", ", missing));
    }
  }

  private static String joinUri(String serverUri, String tokenPath) {
    return StringUtils.removeEnd(serverUri, "/") + "/" + StringUtils.removeStart(tokenPath, "/");
  }
}
