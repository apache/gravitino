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

package org.apache.gravitino.server.authentication;

import io.jsonwebtoken.SignatureAlgorithm;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

public interface OAuthConfig {

  String OAUTH_CONFIG_PREFIX = "gravitino.authenticator.oauth.";

  ConfigEntry<String> SERVICE_AUDIENCE =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "serviceAudience")
          .doc("The audience name when Gravitino uses OAuth as the authenticator")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .createWithDefault("GravitinoServer");

  ConfigEntry<Long> ALLOW_SKEW_SECONDS =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "allowSkewSecs")
          .doc("The JWT allows skew seconds when Gravitino uses OAuth as the authenticator")
          .version(ConfigConstants.VERSION_0_3_0)
          .longConf()
          .createWithDefault(0L);

  ConfigEntry<String> DEFAULT_SIGN_KEY =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "defaultSignKey")
          .doc("The signing key of JWT when Gravitino uses OAuth as the authenticator")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  ConfigEntry<String> SIGNATURE_ALGORITHM_TYPE =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "signAlgorithmType")
          .doc("The signature algorithm when Gravitino uses OAuth as the authenticator")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .createWithDefault(SignatureAlgorithm.RS256.name());

  ConfigEntry<String> DEFAULT_SERVER_URI =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "serverUri")
          .doc("The uri of the default OAuth server")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  ConfigEntry<String> DEFAULT_TOKEN_PATH =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "tokenPath")
          .doc("The path for token of the default OAuth server")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  // OAuth provider configs
  ConfigEntry<String> PROVIDER =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "provider")
          .doc(
              "The OAuth provider to use. This will be used in the Gravitino Web UI to determine the authentication flow.")
          .version(ConfigConstants.VERSION_1_0_0)
          .stringConf()
          .checkValue(
              value -> {
                if (value == null) return false;
                for (ProviderType type : ProviderType.values()) {
                  if (type.name().equalsIgnoreCase(value)) {
                    return true;
                  }
                }
                return false;
              },
              "Invalid OAuth provider type. Supported values: '"
                  + String.join(
                      ", ",
                      java.util.Arrays.stream(ProviderType.values())
                          .map(v -> v.name().toLowerCase())
                          .toArray(String[]::new))
                  + "'")
          .createWithDefault(ProviderType.DEFAULT.name().toLowerCase());

  ConfigEntry<String> CLIENT_ID =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "clientId")
          .doc("OAuth client ID used for Web UI authentication")
          .version(ConfigConstants.VERSION_1_0_0)
          .stringConf()
          .create();

  ConfigEntry<String> AUTHORITY =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "authority")
          .doc("OAuth authority URL (authorization server) used for Web UI authentication")
          .version(ConfigConstants.VERSION_1_0_0)
          .stringConf()
          .create();

  ConfigEntry<String> SCOPE =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "scope")
          .doc("OAuth scopes (space-separated) used for Web UI authentication")
          .version(ConfigConstants.VERSION_1_0_0)
          .stringConf()
          .create();

  ConfigEntry<String> JWKS_URI =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "jwksUri")
          .doc("JWKS URI used for server-side OAuth token validation")
          .version(ConfigConstants.VERSION_1_0_0)
          .stringConf()
          .create();

  ConfigEntry<List<String>> PRINCIPAL_FIELDS =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "principalFields")
          .doc(
              "JWT claim field(s) to use as principal identity. Comma-separated list for fallback in order (e.g., 'preferred_username,email,sub').")
          .version(ConfigConstants.VERSION_1_0_0)
          .stringConf()
          .toSequence()
          .createWithDefault(java.util.Arrays.asList("sub"));

  ConfigEntry<String> TOKEN_VALIDATOR_CLASS =
      new ConfigBuilder(OAUTH_CONFIG_PREFIX + "tokenValidatorClass")
          .doc("Fully qualified class name of the OAuth token validator implementation")
          .version(ConfigConstants.VERSION_1_0_0)
          .stringConf()
          .createWithDefault("org.apache.gravitino.server.authentication.StaticSignKeyValidator");
}
