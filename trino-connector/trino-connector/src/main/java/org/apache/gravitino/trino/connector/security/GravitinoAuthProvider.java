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
package org.apache.gravitino.trino.connector.security;

import io.trino.spi.connector.ConnectorSession;
import java.io.File;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.client.DefaultOAuth2TokenProvider;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoClientConfiguration;
import org.apache.gravitino.client.KerberosTokenProvider;
import org.apache.gravitino.trino.connector.GravitinoConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds a {@link GravitinoAdminClient} with the appropriate authentication provider based on the
 * Gravitino config, and produces a per-user client for session forwarding via {@link
 * #buildForSession(GravitinoConfig, ConnectorSession)}.
 */
public class GravitinoAuthProvider {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoAuthProvider.class);

  /** Authentication type configuration key. */
  public static final String AUTH_TYPE_KEY =
      GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + "authType";

  /** Simple authentication user configuration key. */
  public static final String SIMPLE_AUTH_USER_KEY = "gravitino.user";

  /**
   * When set to {@code true}, the Trino session user/token is forwarded to Gravitino per-request
   * via a per-user {@link GravitinoAdminClient} instead of the shared service client.
   */
  public static final String FORWARD_SESSION_USER_KEY =
      GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + "session.forwardUser";

  /**
   * Maximum number of per-user sessions to keep in the cache when {@code forwardUser=true}.
   * Defaults to {@code 500}.
   */
  public static final String SESSION_CACHE_MAX_SIZE_KEY =
      GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + "session.cache.maxSize";

  /**
   * How long (in seconds) an idle per-user session is kept in the cache when {@code
   * forwardUser=true}. Defaults to {@code 3600} (1 hour).
   */
  public static final String SESSION_CACHE_EXPIRE_AFTER_ACCESS_SECONDS_KEY =
      GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX
          + "session.cache.expireAfterAccessSeconds";

  /** OAuth2 server URI configuration key. */
  public static final String OAUTH_SERVER_URI_KEY =
      GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + "oauth2.serverUri";

  /** OAuth2 credential configuration key. */
  public static final String OAUTH_CREDENTIAL_KEY =
      GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + "oauth2.credential";

  /** OAuth2 path configuration key. */
  public static final String OAUTH_PATH_KEY =
      GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + "oauth2.path";

  /** OAuth2 scope configuration key. */
  public static final String OAUTH_SCOPE_KEY =
      GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + "oauth2.scope";

  /** Kerberos principal configuration key. */
  public static final String KERBEROS_PRINCIPAL_KEY =
      GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + "kerberos.principal";

  /** Kerberos keytab file path configuration key. */
  public static final String KERBEROS_KEYTAB_FILE_PATH_KEY =
      GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + "kerberos.keytabFilePath";

  /** Authentication types supported by the Trino connector. */
  public enum AuthType {
    SIMPLE,
    OAUTH2,
    KERBEROS,
    NONE
  }

  private GravitinoAuthProvider() {}

  /**
   * Builds a {@link GravitinoAdminClient} from the given config, applying authentication settings
   * found in the client config.
   *
   * @param config the Gravitino configuration containing server URI and client properties
   * @return a configured {@link GravitinoAdminClient}
   */
  public static GravitinoAdminClient build(GravitinoConfig config) {
    Map<String, String> clientConfig = config.getClientConfig();
    String uri = config.getURI();
    String authTypeStr = clientConfig.get(AUTH_TYPE_KEY);

    GravitinoAdminClient.AdminClientBuilder builder = GravitinoAdminClient.builder(uri);

    if (StringUtils.isNotBlank(authTypeStr)) {
      AuthType authType = parseAuthType(authTypeStr);

      switch (authType) {
        case SIMPLE:
          buildSimpleAuth(builder, config.getUser());
          break;
        case OAUTH2:
          builder.withOAuth(buildOAuthProvider(clientConfig));
          break;
        case KERBEROS:
          builder.withKerberosAuth(buildKerberosProvider(clientConfig));
          break;
        case NONE:
        default:
          break;
      }
    }

    removeAuthSpecificKeys(clientConfig);
    builder.withClientConfig(clientConfig);

    return builder.build();
  }

  /**
   * Alias for {@link #build(GravitinoConfig)}, kept for backward compatibility with existing tests.
   *
   * @deprecated Use {@link #build(GravitinoConfig)} directly.
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public static GravitinoAdminClient buildClient(GravitinoConfig config) {
    return build(config);
  }

  /**
   * Builds a per-user {@link GravitinoAdminClient} whose credentials come from the given Trino
   * connector session. This is the entry point for the per-user client cache when {@code
   * forwardUser=true}.
   *
   * <p>Currently only {@code authType=simple} is supported: the Trino session username is used as
   * the Gravitino simple-auth identity.
   *
   * @param config the Gravitino connector configuration
   * @param session the current Trino connector session
   * @return a new {@link GravitinoAdminClient} authenticated for the session user
   * @throws IllegalArgumentException if forwarding is not configured or authType is missing
   * @throws UnsupportedOperationException if the authType does not support session forwarding
   */
  public static GravitinoAdminClient buildForSession(
      GravitinoConfig config, ConnectorSession session) {
    Map<String, String> clientConfig = config.getClientConfig();
    String uri = config.getURI();
    String authTypeStr = clientConfig.get(AUTH_TYPE_KEY);
    boolean forwardUser =
        Boolean.parseBoolean(clientConfig.getOrDefault(FORWARD_SESSION_USER_KEY, "false"));

    if (!forwardUser) {
      throw new IllegalArgumentException(
          "buildForSession called but forwardUser is not enabled in config");
    }

    if (StringUtils.isBlank(authTypeStr)) {
      throw new IllegalArgumentException(
          "buildForSession requires an authType to be set in config");
    }

    AuthType authType = parseAuthType(authTypeStr);

    GravitinoAdminClient.AdminClientBuilder builder = GravitinoAdminClient.builder(uri);

    if (authType != AuthType.SIMPLE) {
      throw new UnsupportedOperationException(
          "Auth type "
              + authType
              + " does not support session forwarding. Only simple is supported.");
    }
    builder.withSimpleAuth(session.getUser());

    removeAuthSpecificKeys(clientConfig);
    builder.withClientConfig(clientConfig);
    return builder.build();
  }

  public static AuthType parseAuthType(String authTypeStr) {
    try {
      return AuthType.valueOf(authTypeStr.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid authentication type: %s. Valid values are: simple, oauth2, kerberos, none",
              authTypeStr),
          e);
    }
  }

  private static void removeAuthSpecificKeys(Map<String, String> clientConfig) {
    clientConfig.remove(AUTH_TYPE_KEY);
    clientConfig.remove(OAUTH_SERVER_URI_KEY);
    clientConfig.remove(OAUTH_CREDENTIAL_KEY);
    clientConfig.remove(OAUTH_PATH_KEY);
    clientConfig.remove(OAUTH_SCOPE_KEY);
    clientConfig.remove(KERBEROS_PRINCIPAL_KEY);
    clientConfig.remove(KERBEROS_KEYTAB_FILE_PATH_KEY);
    clientConfig.remove(FORWARD_SESSION_USER_KEY);
    clientConfig.remove(SESSION_CACHE_MAX_SIZE_KEY);
    clientConfig.remove(SESSION_CACHE_EXPIRE_AFTER_ACCESS_SECONDS_KEY);
  }

  private static void buildSimpleAuth(
      GravitinoAdminClient.AdminClientBuilder builder, String simpleUser) {
    if (StringUtils.isNotBlank(simpleUser)) {
      builder.withSimpleAuth(simpleUser);
    } else {
      builder.withSimpleAuth();
    }
  }

  private static DefaultOAuth2TokenProvider buildOAuthProvider(Map<String, String> config) {
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

    LOG.info("Initializing OAuth2 token provider with server URI: {}", serverUri);
    return DefaultOAuth2TokenProvider.builder()
        .withUri(serverUri)
        .withCredential(credential)
        .withPath(normalizedPath)
        .withScope(scope)
        .build();
  }

  private static KerberosTokenProvider buildKerberosProvider(Map<String, String> config) {
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
            String.format(
                "Keytab file configured via %s does not exist: %s",
                KERBEROS_KEYTAB_FILE_PATH_KEY, keytabFilePath));
      }
      if (!keytabFile.isFile()) {
        throw new IllegalArgumentException(
            String.format(
                "Keytab path configured via %s is not a file: %s",
                KERBEROS_KEYTAB_FILE_PATH_KEY, keytabFilePath));
      }
      if (!keytabFile.canRead()) {
        throw new IllegalArgumentException(
            String.format(
                "Keytab file configured via %s is not readable: %s",
                KERBEROS_KEYTAB_FILE_PATH_KEY, keytabFilePath));
      }
      kerberosBuilder.withKeyTabFile(keytabFile);
    } else {
      LOG.warn(
          "No keytab file configured for Kerberos authentication ({}). "
              + "Authentication will fail at runtime unless Kerberos credentials are already "
              + "present in the current security context.",
          KERBEROS_KEYTAB_FILE_PATH_KEY);
    }

    return kerberosBuilder.build();
  }
}
