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

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

/**
 * Configuration class for OAuth Authorization Code Flow. Supports multiple OAuth providers
 * including Azure AD, Google, GitHub, etc.
 */
public class OAuthAuthorizationCodeConfig extends Config {

  // Authorization Code Flow Configuration
  public static final ConfigEntry<String> AUTHORIZATION_URL =
      new ConfigBuilder("gravitino.authenticator.oauth.authorization-url")
          .doc(
              "The authorization URL of the OAuth provider (e.g., https://login.microsoftonline.com/{tenant}/oauth2/v2.0/authorize)")
          .version(ConfigConstants.VERSION_0_8_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .createWithDefault("");

  public static final ConfigEntry<String> TOKEN_URL =
      new ConfigBuilder("gravitino.authenticator.oauth.token-url")
          .doc(
              "The token endpoint URL of the OAuth provider (e.g., https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token)")
          .version(ConfigConstants.VERSION_0_8_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .createWithDefault("");

  public static final ConfigEntry<String> CLIENT_ID =
      new ConfigBuilder("gravitino.authenticator.oauth.client-id")
          .doc("The OAuth client ID registered with the OAuth provider")
          .version(ConfigConstants.VERSION_0_8_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .createWithDefault("");

  public static final ConfigEntry<String> CLIENT_SECRET =
      new ConfigBuilder("gravitino.authenticator.oauth.client-secret")
          .doc("The OAuth client secret registered with the OAuth provider")
          .version(ConfigConstants.VERSION_0_8_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .createWithDefault("");

  public static final ConfigEntry<String> REDIRECT_URI =
      new ConfigBuilder("gravitino.authenticator.oauth.redirect-uri")
          .doc(
              "The redirect URI registered with the OAuth provider (e.g., https://your-gravitino-server/oauth/callback)")
          .version(ConfigConstants.VERSION_0_8_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .createWithDefault("");

  public static final ConfigEntry<String> SCOPE =
      new ConfigBuilder("gravitino.authenticator.oauth.scope")
          .doc("The OAuth scopes to request (e.g., 'openid profile email')")
          .version(ConfigConstants.VERSION_0_8_0)
          .stringConf()
          .createWithDefault("openid profile email");

  public static final ConfigEntry<String> PROVIDER_NAME =
      new ConfigBuilder("gravitino.authenticator.oauth.provider-name")
          .doc("The display name of the OAuth provider (e.g., 'Azure AD', 'Google', 'GitHub')")
          .version(ConfigConstants.VERSION_0_8_0)
          .stringConf()
          .createWithDefault("OAuth Provider");

  // JWKS Configuration for dynamic key fetching
  public static final ConfigEntry<String> JWKS_URI =
      new ConfigBuilder("gravitino.authenticator.oauth.jwks-uri")
          .doc(
              "The JWKS URI of the OAuth provider for dynamic key fetching (e.g., https://login.microsoftonline.com/{tenant}/discovery/v2.0/keys)")
          .version(ConfigConstants.VERSION_0_8_0)
          .stringConf()
          .createWithDefault("");

  public static final ConfigEntry<Boolean> ENABLE_AUTHORIZATION_CODE_FLOW =
      new ConfigBuilder("gravitino.authenticator.oauth.enable-authorization-code-flow")
          .doc("Enable OAuth Authorization Code Flow for user login redirects")
          .version(ConfigConstants.VERSION_0_8_0)
          .booleanConf()
          .createWithDefault(false);

  public static final ConfigEntry<String> FRONTEND_BASE_URL =
      new ConfigBuilder("gravitino.authenticator.oauth.frontend-base-url")
          .doc("Base URL of the Gravitino frontend for redirects after OAuth completion")
          .version(ConfigConstants.VERSION_0_8_0)
          .stringConf()
          .createWithDefault("http://localhost:8090");

  public OAuthAuthorizationCodeConfig() {
    super();
  }

  public OAuthAuthorizationCodeConfig(Map<String, String> properties) {
    super();
    loadFromMap(properties, k -> true);
  }

  // Convenience methods for accessing configuration values
  public String getAuthorizationUrl() {
    return get(AUTHORIZATION_URL);
  }

  public String getTokenUrl() {
    return get(TOKEN_URL);
  }

  public String getClientId() {
    return get(CLIENT_ID);
  }

  public String getClientSecret() {
    return get(CLIENT_SECRET);
  }

  public String getRedirectUri() {
    return get(REDIRECT_URI);
  }

  public String getScope() {
    return get(SCOPE);
  }

  public String getProviderName() {
    return get(PROVIDER_NAME);
  }

  public String getJwksUri() {
    return get(JWKS_URI);
  }

  public boolean isAuthorizationCodeFlowEnabled() {
    return get(ENABLE_AUTHORIZATION_CODE_FLOW);
  }

  public String getFrontendBaseUrl() {
    return get(FRONTEND_BASE_URL);
  }

  /**
   * Validates that all required configuration for Authorization Code Flow is present.
   *
   * @throws IllegalArgumentException if required configuration is missing
   */
  public void validateAuthorizationCodeFlowConfig() {
    if (!isAuthorizationCodeFlowEnabled()) {
      return;
    }

    if (StringUtils.isBlank(getAuthorizationUrl())) {
      throw new IllegalArgumentException(
          "authorization-url is required when authorization code flow is enabled");
    }

    if (StringUtils.isBlank(getTokenUrl())) {
      throw new IllegalArgumentException(
          "token-url is required when authorization code flow is enabled");
    }

    if (StringUtils.isBlank(getClientId())) {
      throw new IllegalArgumentException(
          "client-id is required when authorization code flow is enabled");
    }

    if (StringUtils.isBlank(getClientSecret())) {
      throw new IllegalArgumentException(
          "client-secret is required when authorization code flow is enabled");
    }

    if (StringUtils.isBlank(getRedirectUri())) {
      throw new IllegalArgumentException(
          "redirect-uri is required when authorization code flow is enabled");
    }
  }

  /**
   * Checks if JWKS dynamic key fetching is configured.
   *
   * @return true if JWKS URI is configured, false otherwise
   */
  public boolean isJwksConfigured() {
    return StringUtils.isNotBlank(getJwksUri());
  }
}
