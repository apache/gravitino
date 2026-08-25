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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.spark.SparkConf;

class IcebergRestOAuthConfig {

  static final String AUTH_TYPE = "rest.auth.type";
  static final String AUTH_TYPE_OAUTH2 = "oauth2";
  static final String CREDENTIAL = "credential";
  static final String OAUTH2_SERVER_URI = "oauth2-server-uri";
  static final String SCOPE = "scope";

  private static final Set<String> LEGACY_OAUTH_PROPERTIES =
      ImmutableSet.of(
          "token",
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
    if (hasExplicitAuthentication(result)
        || !sparkConf.getBoolean(GravitinoSparkConfig.GRAVITINO_ICEBERG_REUSE_OAUTH2, true)) {
      return result;
    }

    String authType =
        sparkConf.get(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, AuthProperties.SIMPLE_AUTH_TYPE);
    if (!AuthProperties.isOAuth2(authType)) {
      return result;
    }

    String serverUri = required(sparkConf, GravitinoSparkConfig.GRAVITINO_OAUTH2_URI);
    String tokenPath = required(sparkConf, GravitinoSparkConfig.GRAVITINO_OAUTH2_PATH);
    result.put(AUTH_TYPE, AUTH_TYPE_OAUTH2);
    result.put(CREDENTIAL, required(sparkConf, GravitinoSparkConfig.GRAVITINO_OAUTH2_CREDENTIAL));
    result.put(SCOPE, required(sparkConf, GravitinoSparkConfig.GRAVITINO_OAUTH2_SCOPE));
    result.put(OAUTH2_SERVER_URI, joinUri(serverUri, tokenPath));
    return result;
  }

  private static boolean hasExplicitAuthentication(Map<String, String> restConfig) {
    return restConfig.keySet().stream()
        .anyMatch(key -> key.startsWith("rest.auth.") || LEGACY_OAUTH_PROPERTIES.contains(key));
  }

  private static String required(SparkConf sparkConf, String key) {
    String value = sparkConf.get(key, null);
    Preconditions.checkArgument(StringUtils.isNotBlank(value), key + " should not be empty");
    return value;
  }

  private static String joinUri(String serverUri, String tokenPath) {
    return StringUtils.removeEnd(serverUri, "/") + "/" + StringUtils.removeStart(tokenPath, "/");
  }
}
