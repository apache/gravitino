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
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.client.DefaultOAuth2TokenProvider;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoClientAuthenticationConfig;
import org.apache.gravitino.client.KerberosTokenProvider;
import org.apache.gravitino.trino.connector.GravitinoConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds a {@link GravitinoAdminClient} with the appropriate authentication provider based on the
 * Gravitino config.
 */
class GravitinoAuthProvider {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoAuthProvider.class);

  private GravitinoAuthProvider() {}

  /**
   * Builds a GravitinoAdminClient from the given config, applying authentication settings found in
   * the client config.
   *
   * @param config the Gravitino configuration containing server URI and client properties
   * @return a configured GravitinoAdminClient
   */
  public static GravitinoAdminClient buildClient(GravitinoConfig config) {
    Map<String, String> clientConfig = new HashMap<>(config.getClientConfig());
    String uri = config.getURI();
    String authTypeStr = clientConfig.get(GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY);

    GravitinoAdminClient.AdminClientBuilder builder = GravitinoAdminClient.builder(uri);

    if (StringUtils.isNotBlank(authTypeStr)) {
      GravitinoClientAuthenticationConfig.AuthType authType;
      try {
        authType =
            GravitinoClientAuthenticationConfig.AuthType.valueOf(
                authTypeStr.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid authentication type: %s. Valid values are: simple, oauth, kerberos, none",
                authTypeStr),
            e);
      }

      switch (authType) {
        case SIMPLE:
          buildSimpleAuth(builder, clientConfig);
          break;
        case OAUTH:
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

    // Remove auth-specific keys before passing to withClientConfig
    clientConfig.remove(GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY);
    clientConfig.remove(GravitinoClientAuthenticationConfig.SIMPLE_AUTH_USER_KEY);
    clientConfig.remove(GravitinoClientAuthenticationConfig.OAUTH_SERVER_URI_KEY);
    clientConfig.remove(GravitinoClientAuthenticationConfig.OAUTH_CREDENTIAL_KEY);
    clientConfig.remove(GravitinoClientAuthenticationConfig.OAUTH_PATH_KEY);
    clientConfig.remove(GravitinoClientAuthenticationConfig.OAUTH_SCOPE_KEY);
    clientConfig.remove(GravitinoClientAuthenticationConfig.KERBEROS_PRINCIPAL_KEY);
    clientConfig.remove(GravitinoClientAuthenticationConfig.KERBEROS_KEYTAB_FILE_PATH_KEY);

    builder.withClientConfig(clientConfig);
    return builder.build();
  }

  private static void buildSimpleAuth(
      GravitinoAdminClient.AdminClientBuilder builder, Map<String, String> clientConfig) {
    String simpleUser = clientConfig.get(GravitinoClientAuthenticationConfig.SIMPLE_AUTH_USER_KEY);
    if (StringUtils.isNotBlank(simpleUser)) {
      builder.withSimpleAuth(simpleUser);
    } else {
      builder.withSimpleAuth();
    }
  }

  private static DefaultOAuth2TokenProvider buildOAuthProvider(Map<String, String> config) {
    String serverUri = config.get(GravitinoClientAuthenticationConfig.OAUTH_SERVER_URI_KEY);
    String credential = config.get(GravitinoClientAuthenticationConfig.OAUTH_CREDENTIAL_KEY);
    String path = config.get(GravitinoClientAuthenticationConfig.OAUTH_PATH_KEY);
    String scope = config.get(GravitinoClientAuthenticationConfig.OAUTH_SCOPE_KEY);

    if (StringUtils.isBlank(serverUri)) {
      throw new IllegalArgumentException(
          String.format(
              "OAuth server URI is required. Please set %s",
              GravitinoClientAuthenticationConfig.OAUTH_SERVER_URI_KEY));
    }
    if (StringUtils.isBlank(credential)) {
      throw new IllegalArgumentException(
          String.format(
              "OAuth credential is required. Please set %s",
              GravitinoClientAuthenticationConfig.OAUTH_CREDENTIAL_KEY));
    }
    if (StringUtils.isBlank(path)) {
      throw new IllegalArgumentException(
          String.format(
              "OAuth path is required. Please set %s",
              GravitinoClientAuthenticationConfig.OAUTH_PATH_KEY));
    }
    if (StringUtils.isBlank(scope)) {
      throw new IllegalArgumentException(
          String.format(
              "OAuth scope is required. Please set %s",
              GravitinoClientAuthenticationConfig.OAUTH_SCOPE_KEY));
    }

    // Remove leading slash from path if present
    String normalizedPath = path.startsWith("/") ? path.substring(1) : path;

    LOG.info("Initializing OAuth2 token provider with server URI: {}", serverUri);
    try {
      return DefaultOAuth2TokenProvider.builder()
          .withUri(serverUri)
          .withCredential(credential)
          .withPath(normalizedPath)
          .withScope(scope)
          .build();
    } catch (Exception e) {
      throw new IllegalStateException(
          String.format(
              "Failed to initialize OAuth2 token provider for server URI '%s'. "
                  + "Check that the server is reachable and credentials are correct.",
              serverUri),
          e);
    }
  }

  private static KerberosTokenProvider buildKerberosProvider(Map<String, String> config) {
    String principal = config.get(GravitinoClientAuthenticationConfig.KERBEROS_PRINCIPAL_KEY);
    String keytabFilePath =
        config.get(GravitinoClientAuthenticationConfig.KERBEROS_KEYTAB_FILE_PATH_KEY);

    if (StringUtils.isBlank(principal)) {
      throw new IllegalArgumentException(
          String.format(
              "Kerberos principal is required. Please set %s",
              GravitinoClientAuthenticationConfig.KERBEROS_PRINCIPAL_KEY));
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
    } else {
      LOG.warn(
          "No keytab file configured for Kerberos authentication ({}). "
              + "Authentication will fail at runtime unless Kerberos credentials are already "
              + "present in the current security context.",
          GravitinoClientAuthenticationConfig.KERBEROS_KEYTAB_FILE_PATH_KEY);
    }

    // host is set by GravitinoAdminClient.Builder.withKerberosAuth() from the server URI
    return kerberosBuilder.build();
  }
}
