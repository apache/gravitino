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
package org.apache.gravitino.trino.connector.catalog;

import java.io.File;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;
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
 * Gravitino config, and produces an optional {@link TrinoSessionContext} for per-query credential
 * forwarding.
 */
class GravitinoAuthProvider {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoAuthProvider.class);

  /** Authentication type configuration key. */
  static final String AUTH_TYPE_KEY =
      GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + "authType";

  /** Simple authentication user configuration key. */
  static final String SIMPLE_AUTH_USER_KEY = "gravitino.user";

  /** OAuth2 server URI configuration key. */
  static final String OAUTH_SERVER_URI_KEY =
      GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + "oauth2.serverUri";

  /** OAuth2 credential configuration key. */
  static final String OAUTH_CREDENTIAL_KEY =
      GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + "oauth2.credential";

  /** OAuth2 path configuration key. */
  static final String OAUTH_PATH_KEY =
      GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + "oauth2.path";

  /** OAuth2 scope configuration key. */
  static final String OAUTH_SCOPE_KEY =
      GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + "oauth2.scope";

  /**
   * Extra-credentials key whose value is forwarded as a Bearer token to Gravitino. Only used when
   * {@code authType=oauth2_token}.
   */
  static final String OAUTH2_TOKEN_CREDENTIAL_KEY =
      GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + "oauth2.token.credentialKey";

  /** Kerberos principal configuration key. */
  static final String KERBEROS_PRINCIPAL_KEY =
      GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + "kerberos.principal";

  /** Kerberos keytab file path configuration key. */
  static final String KERBEROS_KEYTAB_FILE_PATH_KEY =
      GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + "kerberos.keytabFilePath";

  /**
   * When set to {@code true}, the Trino session user name is forwarded to Gravitino via the {@code
   * X-Gravitino-User} HTTP header on every request, regardless of the configured auth type. For
   * {@code simple} auth the session user also replaces the static {@code gravitino.user}.
   */
  static final String FORWARD_SESSION_USER_KEY =
      GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + "session.forwardUser";

  /** Authentication types supported by the Trino connector. */
  enum AuthType {
    SIMPLE,
    OAUTH2,
    KERBEROS,
    OAUTH2_TOKEN,
    NONE
  }

  private GravitinoAuthProvider() {}

  /**
   * The result of {@link #build(GravitinoConfig)}.
   *
   * <p>{@link #sessionContext} is non-null when session-forwarding is configured; callers must
   * propagate it to {@link CatalogConnectorContext} so that {@link
   * org.apache.gravitino.trino.connector.GravitinoMetadata} can apply / clear it around each query.
   */
  static final class BuildResult {
    final GravitinoAdminClient client;
    @Nullable final TrinoSessionContext sessionContext;

    BuildResult(GravitinoAdminClient client, @Nullable TrinoSessionContext sessionContext) {
      this.client = client;
      this.sessionContext = sessionContext;
    }
  }

  /**
   * Builds a {@link GravitinoAdminClient} from the given config, applying authentication settings
   * found in the client config.
   *
   * @param config the Gravitino configuration containing server URI and client properties
   * @return a {@link BuildResult} containing the configured client and an optional session context
   */
  static BuildResult build(GravitinoConfig config) {
    Map<String, String> clientConfig = new HashMap<>(config.getClientConfig());
    String uri = config.getURI();
    String authTypeStr = clientConfig.get(AUTH_TYPE_KEY);
    boolean forwardUser =
        Boolean.parseBoolean(clientConfig.getOrDefault(FORWARD_SESSION_USER_KEY, "false"));

    GravitinoAdminClient.AdminClientBuilder builder = GravitinoAdminClient.builder(uri);

    TrinoSessionAuthProvider authProvider = null;
    TrinoUserHeaderProvider headerProvider = forwardUser ? new TrinoUserHeaderProvider() : null;

    if (StringUtils.isNotBlank(authTypeStr)) {
      AuthType authType;
      try {
        authType = AuthType.valueOf(authTypeStr.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid authentication type: %s. Valid values are: simple, oauth2, kerberos,"
                    + " oauth2_token, none",
                authTypeStr),
            e);
      }

      switch (authType) {
        case SIMPLE:
          if (forwardUser) {
            // Use the Trino session user dynamically instead of the static configured user.
            authProvider = new TrinoSessionAuthProvider();
            builder.withCustomTokenAuth(authProvider);
          } else {
            buildSimpleAuth(builder, config.getUser());
          }
          break;
        case OAUTH2:
          builder.withOAuth(buildOAuthProvider(clientConfig));
          break;
        case KERBEROS:
          builder.withKerberosAuth(buildKerberosProvider(clientConfig));
          break;
        case OAUTH2_TOKEN:
          String credentialKey = clientConfig.get(OAUTH2_TOKEN_CREDENTIAL_KEY);
          if (StringUtils.isBlank(credentialKey)) {
            throw new IllegalArgumentException(
                String.format(
                    "oauth2_token auth requires a credential key. Please set %s",
                    OAUTH2_TOKEN_CREDENTIAL_KEY));
          }
          authProvider = new TrinoSessionAuthProvider(credentialKey);
          builder.withCustomTokenAuth(authProvider);
          // Always forward the user header for oauth2_token
          if (headerProvider == null) {
            headerProvider = new TrinoUserHeaderProvider();
          }
          break;
        case NONE:
        default:
          break;
      }
    }

    if (headerProvider != null) {
      builder.withExtraHeaders(headerProvider);
    }

    // Remove all auth-specific and session-forwarding keys before passing remaining config to
    // withClientConfig so that GravitinoClientConfiguration does not reject unknown keys.
    clientConfig.remove(AUTH_TYPE_KEY);
    clientConfig.remove(OAUTH_SERVER_URI_KEY);
    clientConfig.remove(OAUTH_CREDENTIAL_KEY);
    clientConfig.remove(OAUTH_PATH_KEY);
    clientConfig.remove(OAUTH_SCOPE_KEY);
    clientConfig.remove(KERBEROS_PRINCIPAL_KEY);
    clientConfig.remove(KERBEROS_KEYTAB_FILE_PATH_KEY);
    clientConfig.remove(OAUTH2_TOKEN_CREDENTIAL_KEY);
    clientConfig.remove(FORWARD_SESSION_USER_KEY);

    builder.withClientConfig(clientConfig);

    TrinoSessionContext sessionContext =
        (authProvider != null || headerProvider != null)
            ? new TrinoSessionContext(authProvider, headerProvider)
            : null;

    return new BuildResult(builder.build(), sessionContext);
  }

  /**
   * Convenience wrapper kept for callers that only need the {@link GravitinoAdminClient} and do not
   * require session-forwarding (e.g. tests that inject a pre-built client).
   *
   * @deprecated Use {@link #build(GravitinoConfig)} instead.
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  static GravitinoAdminClient buildClient(GravitinoConfig config) {
    return build(config).client;
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

    // host is set by GravitinoAdminClient.Builder.withKerberosAuth() from the server URI
    return kerberosBuilder.build();
  }
}
