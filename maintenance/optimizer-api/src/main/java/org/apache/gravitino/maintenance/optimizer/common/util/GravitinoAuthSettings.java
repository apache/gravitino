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

package org.apache.gravitino.maintenance.optimizer.common.util;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.client.DefaultOAuth2TokenProvider;
import org.apache.gravitino.client.GravitinoClientBase;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;

/**
 * Authentication settings for {@code builtin-iceberg-update-stats}.
 *
 * <p>Put credentials in {@code --updater-options} (short names such as {@code auth_type}). {@link
 * #copyAliases(Map)} promotes them onto canonical {@code gravitino.optimizer.auth.*} keys so {@link
 * #from(OptimizerConfig)} can apply them to the Gravitino client (statistics updater) and Iceberg
 * REST Spark catalog configs.
 */
public final class GravitinoAuthSettings {

  public static final String TYPE_NONE = "none";
  public static final String TYPE_SIMPLE = "simple";
  public static final String TYPE_BASIC = "basic";
  public static final String TYPE_OAUTH = "oauth";

  private static final String ICEBERG_REST_AUTH_TYPE = "rest.auth.type";
  private static final String ICEBERG_REST_BASIC_USERNAME = "rest.auth.basic.username";
  private static final String ICEBERG_REST_BASIC_PASSWORD = "rest.auth.basic.password";
  private static final String ICEBERG_REST_OAUTH2_CREDENTIAL = "credential";
  private static final String ICEBERG_REST_OAUTH2_SCOPE = "scope";
  private static final String ICEBERG_REST_OAUTH2_SERVER_URI = "oauth2-server-uri";
  private static final String SPARK_SQL_CATALOG_PREFIX = "spark.sql.catalog.";

  private final String authType;
  private final String username;
  private final String password;
  private final String oauthServerUri;
  private final String oauthPath;
  private final String oauthCredential;
  private final String oauthScope;

  private GravitinoAuthSettings(
      String authType,
      String username,
      String password,
      String oauthServerUri,
      String oauthPath,
      String oauthCredential,
      String oauthScope) {
    this.authType = authType;
    this.username = username;
    this.password = password;
    this.oauthServerUri = oauthServerUri;
    this.oauthPath = oauthPath;
    this.oauthCredential = oauthCredential;
    this.oauthScope = oauthScope;
  }

  /**
   * Resolves settings from optimizer configuration.
   *
   * @param config optimizer configuration, may be {@code null}
   * @return resolved authentication settings
   */
  public static GravitinoAuthSettings from(OptimizerConfig config) {
    return new GravitinoAuthSettings(
        configValue(config, OptimizerConfig.AUTH_TYPE),
        configValue(config, OptimizerConfig.AUTH_USERNAME),
        configValue(config, OptimizerConfig.AUTH_PASSWORD),
        configValue(config, OptimizerConfig.AUTH_OAUTH_SERVER_URI),
        configValue(config, OptimizerConfig.AUTH_OAUTH_PATH),
        configValue(config, OptimizerConfig.AUTH_OAUTH_CREDENTIAL),
        configValue(config, OptimizerConfig.AUTH_OAUTH_SCOPE));
  }

  /**
   * Copies shorthand updater-option keys onto canonical {@code gravitino.optimizer.auth.*} keys.
   *
   * @param properties updater options / optimizer properties
   */
  public static void copyAliases(Map<String, String> properties) {
    if (properties == null) {
      return;
    }
    copyAlias(properties, OptimizerConfig.AUTH_TYPE, "auth_type");
    copyAlias(properties, OptimizerConfig.AUTH_USERNAME, "username", "user");
    copyAlias(properties, OptimizerConfig.AUTH_PASSWORD, "password");
    copyAlias(properties, OptimizerConfig.AUTH_OAUTH_SERVER_URI, "oauth_server_uri");
    copyAlias(properties, OptimizerConfig.AUTH_OAUTH_PATH, "oauth_path");
    copyAlias(properties, OptimizerConfig.AUTH_OAUTH_CREDENTIAL, "oauth_credential");
    copyAlias(properties, OptimizerConfig.AUTH_OAUTH_SCOPE, "oauth_scope");
  }

  /**
   * Applies the matching authenticator to a Gravitino client builder.
   *
   * @param builder Gravitino client builder
   * @param <T> client type
   * @return the same builder
   */
  public <T extends GravitinoClientBase> GravitinoClientBase.Builder<T> applyTo(
      GravitinoClientBase.Builder<T> builder) {
    switch (normalizedType()) {
      case TYPE_SIMPLE:
        if (StringUtils.isNotBlank(username)) {
          builder.withSimpleAuth(username);
        } else {
          builder.withSimpleAuth();
        }
        break;
      case TYPE_BASIC:
        requireValue(username, "username is required for basic authentication");
        requireValue(password, "password is required for basic authentication");
        builder.withBasicAuth(username, password);
        break;
      case TYPE_OAUTH:
        requireValue(oauthServerUri, "oauth server URI is required for oauth authentication");
        requireValue(oauthPath, "oauth path is required for oauth authentication");
        requireValue(oauthCredential, "oauth credential is required for oauth authentication");
        requireValue(oauthScope, "oauth scope is required for oauth authentication");
        builder.withOAuth(
            DefaultOAuth2TokenProvider.builder()
                .withUri(oauthServerUri)
                .withPath(oauthPath)
                .withCredential(oauthCredential)
                .withScope(oauthScope)
                .build());
        break;
      case TYPE_NONE:
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported auth_type: "
                + authType
                + ". Supported values are: none, simple, basic, oauth");
    }
    return builder;
  }

  /**
   * Iceberg REST Spark catalog authentication configs for {@code catalogName}.
   *
   * @param catalogName Spark catalog name
   * @return spark.sql.catalog.&lt;name&gt;.* entries, or empty if auth is unset
   */
  public Map<String, String> icebergRestCatalogConfigs(String catalogName) {
    if (StringUtils.isBlank(catalogName) || !hasAuth()) {
      return Collections.emptyMap();
    }
    String prefix = SPARK_SQL_CATALOG_PREFIX + catalogName + ".";
    Map<String, String> configs = new LinkedHashMap<>();
    switch (normalizedType()) {
      case TYPE_BASIC:
        requireValue(username, "username is required for basic authentication");
        requireValue(password, "password is required for basic authentication");
        configs.put(prefix + ICEBERG_REST_AUTH_TYPE, "basic");
        configs.put(prefix + ICEBERG_REST_BASIC_USERNAME, username);
        configs.put(prefix + ICEBERG_REST_BASIC_PASSWORD, password);
        break;
      case TYPE_OAUTH:
        requireValue(oauthServerUri, "oauth server URI is required for oauth authentication");
        requireValue(oauthPath, "oauth path is required for oauth authentication");
        requireValue(oauthCredential, "oauth credential is required for oauth authentication");
        requireValue(oauthScope, "oauth scope is required for oauth authentication");
        // Iceberg REST expects the full token endpoint in oauth2-server-uri (same as Spark
        // IcebergRestOAuthConfig.joinUri).
        configs.put(prefix + ICEBERG_REST_AUTH_TYPE, "oauth2");
        configs.put(prefix + ICEBERG_REST_OAUTH2_SERVER_URI, joinUri(oauthServerUri, oauthPath));
        configs.put(prefix + ICEBERG_REST_OAUTH2_CREDENTIAL, oauthCredential);
        configs.put(prefix + ICEBERG_REST_OAUTH2_SCOPE, oauthScope);
        break;
      case TYPE_SIMPLE:
        // Gravitino simple auth is a Basic token of user:dummy. Iceberg REST accepts the same
        // username with an unused password when the server authenticators include simple.
        // Servers that do not treat "dummy" as acceptable for simple auth need rest.auth.* set
        // explicitly in spark-conf instead.
        String simpleUser = firstNonBlank(username, System.getProperty("user.name"));
        requireValue(simpleUser, "username is required for simple authentication");
        configs.put(prefix + ICEBERG_REST_AUTH_TYPE, "basic");
        configs.put(prefix + ICEBERG_REST_BASIC_USERNAME, simpleUser);
        configs.put(prefix + ICEBERG_REST_BASIC_PASSWORD, "dummy");
        break;
      case TYPE_NONE:
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported auth_type: "
                + authType
                + ". Supported values are: none, simple, basic, oauth");
    }
    return Collections.unmodifiableMap(configs);
  }

  /**
   * Returns whether an authenticator should be configured.
   *
   * @return true when auth type is simple, basic, or oauth
   */
  public boolean hasAuth() {
    String type = normalizedType();
    return TYPE_SIMPLE.equals(type) || TYPE_BASIC.equals(type) || TYPE_OAUTH.equals(type);
  }

  /**
   * Returns the normalized auth type, or {@link #TYPE_NONE} when unset.
   *
   * @return auth type
   */
  public String authType() {
    return normalizedType();
  }

  private String normalizedType() {
    if (StringUtils.isBlank(authType)) {
      return TYPE_NONE;
    }
    return authType.trim().toLowerCase(Locale.ROOT);
  }

  private static String configValue(OptimizerConfig config, String key) {
    if (config == null) {
      return null;
    }
    String value = config.getRawString(key);
    return StringUtils.isBlank(value) ? null : value.trim();
  }

  private static void copyAlias(
      Map<String, String> properties, String canonical, String... aliases) {
    if (StringUtils.isNotBlank(properties.get(canonical))) {
      return;
    }
    for (String alias : aliases) {
      String value = properties.get(alias);
      if (StringUtils.isNotBlank(value)) {
        properties.put(canonical, value.trim());
        return;
      }
    }
  }

  private static String firstNonBlank(String... values) {
    for (String value : values) {
      if (StringUtils.isNotBlank(value)) {
        return value.trim();
      }
    }
    return null;
  }

  private static void requireValue(String value, String message) {
    if (StringUtils.isBlank(value)) {
      throw new IllegalArgumentException(message);
    }
  }

  /** Joins an OAuth server base URI and token path into a full token endpoint URL. */
  private static String joinUri(String serverUri, String tokenPath) {
    return StringUtils.removeEnd(serverUri.trim(), "/")
        + "/"
        + StringUtils.removeStart(tokenPath.trim(), "/");
  }
}
