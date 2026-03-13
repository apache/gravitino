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
package org.apache.gravitino.client;

import static org.apache.gravitino.client.GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX;

import java.io.File;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * Helper class to parse authentication configuration and create AuthDataProvider instances.
 *
 * <p>This class provides utility methods to create authentication providers for Gravitino clients
 * based on configuration maps. It supports multiple authentication types including Simple, OAuth,
 * and Kerberos.
 */
public class GravitinoClientAuthenticationConfig {

  /** Authentication type configuration key. */
  public static final String AUTH_TYPE_KEY = GRAVITINO_CLIENT_CONFIG_PREFIX + "authType";

  /** Simple authentication user configuration key. */
  public static final String SIMPLE_AUTH_USER_KEY =
      GRAVITINO_CLIENT_CONFIG_PREFIX + "simpleAuthUser";

  /** OAuth server URI configuration key. */
  public static final String OAUTH_SERVER_URI_KEY =
      GRAVITINO_CLIENT_CONFIG_PREFIX + "oauth.serverUri";
  /** OAuth credential configuration key. */
  public static final String OAUTH_CREDENTIAL_KEY =
      GRAVITINO_CLIENT_CONFIG_PREFIX + "oauth.credential";
  /** OAuth path configuration key. */
  public static final String OAUTH_PATH_KEY = GRAVITINO_CLIENT_CONFIG_PREFIX + "oauth.path";
  /** OAuth scope configuration key. */
  public static final String OAUTH_SCOPE_KEY = GRAVITINO_CLIENT_CONFIG_PREFIX + "oauth.scope";

  /** Kerberos principal configuration key. */
  public static final String KERBEROS_PRINCIPAL_KEY =
      GRAVITINO_CLIENT_CONFIG_PREFIX + "kerberos.principal";
  /** Kerberos keytab file path configuration key. */
  public static final String KERBEROS_KEYTAB_FILE_PATH_KEY =
      GRAVITINO_CLIENT_CONFIG_PREFIX + "kerberos.keytabFilePath";

  /** Authentication types. */
  public enum AuthType {
    /** Simple authentication. */
    SIMPLE,
    /** OAuth authentication. */
    OAUTH,
    /** Kerberos authentication. */
    KERBEROS,
    /** No authentication. */
    NONE
  }

  /**
   * Creates an AuthDataProvider based on the provided configuration.
   *
   * @param config The configuration map containing authentication settings
   * @param serverUri The Gravitino server URI (required for Kerberos to extract host)
   * @return The AuthDataProvider instance, defaults to Simple authentication if not configured
   */
  public static AuthDataProvider createAuthDataProvider(
      Map<String, String> config, String serverUri) {
    String authTypeStr = config.get(AUTH_TYPE_KEY);

    // Default to SIMPLE authentication if auth type is not specified
    if (StringUtils.isBlank(authTypeStr)) {
      return createSimpleAuthProvider(config);
    }

    AuthType authType;
    try {
      authType = AuthType.valueOf(authTypeStr.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid authentication type: %s. Valid values are: simple, oauth, kerberos, none",
              authTypeStr),
          e);
    }

    switch (authType) {
      case SIMPLE:
        return createSimpleAuthProvider(config);
      case OAUTH:
        return createOAuthProvider(config);
      case KERBEROS:
        return createKerberosProvider(config, serverUri);
      case NONE:
        return null;
      default:
        throw new IllegalArgumentException("Unsupported authentication type: " + authType);
    }
  }

  /**
   * Creates a SimpleTokenProvider based on the configuration.
   *
   * @param config The configuration map
   * @return The SimpleTokenProvider instance
   */
  private static AuthDataProvider createSimpleAuthProvider(Map<String, String> config) {
    String user = config.get(SIMPLE_AUTH_USER_KEY);
    if (StringUtils.isBlank(user)) {
      return new SimpleTokenProvider();
    }
    return new SimpleTokenProvider(user);
  }

  /**
   * Creates an OAuth2TokenProvider based on the configuration.
   *
   * @param config The configuration map
   * @return The OAuth2TokenProvider instance
   */
  private static AuthDataProvider createOAuthProvider(Map<String, String> config) {
    String serverUri = config.get(OAUTH_SERVER_URI_KEY);
    String credential = config.get(OAUTH_CREDENTIAL_KEY);
    String path = config.get(OAUTH_PATH_KEY);
    String scope = config.get(OAUTH_SCOPE_KEY);

    if (StringUtils.isBlank(serverUri)) {
      throw new IllegalArgumentException(
          String.format("OAuth server URI is required. Please set %s", OAUTH_SERVER_URI_KEY));
    }
    if (StringUtils.isBlank(credential)) {
      throw new IllegalArgumentException(
          String.format("OAuth credential is required. Please set %s", OAUTH_CREDENTIAL_KEY));
    }
    if (StringUtils.isBlank(path)) {
      throw new IllegalArgumentException(
          String.format("OAuth path is required. Please set %s", OAUTH_PATH_KEY));
    }
    if (StringUtils.isBlank(scope)) {
      throw new IllegalArgumentException(
          String.format("OAuth scope is required. Please set %s", OAUTH_SCOPE_KEY));
    }

    // Remove leading slash from path if present
    String normalizedPath = path.startsWith("/") ? path.substring(1) : path;

    return DefaultOAuth2TokenProvider.builder()
        .withUri(serverUri)
        .withCredential(credential)
        .withPath(normalizedPath)
        .withScope(scope)
        .build();
  }

  /**
   * Creates a KerberosTokenProvider based on the configuration.
   *
   * @param config The configuration map
   * @param serverUri The Gravitino server URI (used to extract host)
   * @return The KerberosTokenProvider instance
   */
  private static AuthDataProvider createKerberosProvider(
      Map<String, String> config, String serverUri) {
    String principal = config.get(KERBEROS_PRINCIPAL_KEY);
    String keytabFilePath = config.get(KERBEROS_KEYTAB_FILE_PATH_KEY);

    if (StringUtils.isBlank(principal)) {
      throw new IllegalArgumentException(
          String.format("Kerberos principal is required. Please set %s", KERBEROS_PRINCIPAL_KEY));
    }

    KerberosTokenProvider.Builder kerberosBuilder =
        KerberosTokenProvider.builder().withClientPrincipal(principal);

    if (StringUtils.isNotBlank(keytabFilePath)) {
      File keytabFile = new File(keytabFilePath);
      if (!keytabFile.exists()) {
        throw new IllegalArgumentException(
            String.format("Keytab file not found: %s", keytabFilePath));
      }
      kerberosBuilder.withKeyTabFile(keytabFile);
    }

    KerberosTokenProvider provider = kerberosBuilder.build();

    // Set host from server URI if available
    if (StringUtils.isNotBlank(serverUri)) {
      try {
        java.net.URI uri = new java.net.URI(serverUri);
        provider.setHost(uri.getHost());
      } catch (java.net.URISyntaxException e) {
        throw new IllegalArgumentException("Invalid server URI: " + serverUri, e);
      }
    }

    return provider;
  }
}
