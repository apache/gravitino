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

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.client.DefaultOAuth2TokenProvider;
import org.apache.gravitino.client.GravitinoClientBase;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;

/**
 * Authentication settings for optimizer and built-in maintenance jobs.
 *
 * <p>Values are resolved from optimizer configuration first, then from process environment
 * variables so Spark jobs can receive credentials through job-template {@code environments}.
 */
public final class GravitinoAuthSettings {

  public static final String TYPE_NONE = "none";
  public static final String TYPE_SIMPLE = "simple";
  public static final String TYPE_BASIC = "basic";
  public static final String TYPE_OAUTH = "oauth";

  public static final String ENV_AUTH_TYPE = "GRAVITINO_AUTH_TYPE";
  public static final String ENV_AUTH_USERNAME = "GRAVITINO_AUTH_USERNAME";
  public static final String ENV_AUTH_PASSWORD = "GRAVITINO_AUTH_PASSWORD";
  public static final String ENV_AUTH_OAUTH_SERVER_URI = "GRAVITINO_AUTH_OAUTH_SERVER_URI";
  public static final String ENV_AUTH_OAUTH_PATH = "GRAVITINO_AUTH_OAUTH_PATH";
  public static final String ENV_AUTH_OAUTH_CREDENTIAL = "GRAVITINO_AUTH_OAUTH_CREDENTIAL";
  public static final String ENV_AUTH_OAUTH_SCOPE = "GRAVITINO_AUTH_OAUTH_SCOPE";
  public static final String ENV_AUTH_OAUTH_TOKEN = "GRAVITINO_AUTH_OAUTH_TOKEN";
  public static final String ENV_GRAVITINO_USER = "GRAVITINO_USER";

  public static final String JOB_CONF_AUTH_TYPE = "gravitino_auth_type";
  public static final String JOB_CONF_AUTH_USERNAME = "gravitino_auth_username";
  public static final String JOB_CONF_AUTH_PASSWORD = "gravitino_auth_password";
  public static final String JOB_CONF_AUTH_OAUTH_SERVER_URI = "gravitino_auth_oauth_server_uri";
  public static final String JOB_CONF_AUTH_OAUTH_PATH = "gravitino_auth_oauth_path";
  public static final String JOB_CONF_AUTH_OAUTH_CREDENTIAL = "gravitino_auth_oauth_credential";
  public static final String JOB_CONF_AUTH_OAUTH_SCOPE = "gravitino_auth_oauth_scope";
  public static final String JOB_CONF_AUTH_OAUTH_TOKEN = "gravitino_auth_oauth_token";

  private static final String ICEBERG_REST_AUTH_TYPE = "rest.auth.type";
  private static final String ICEBERG_REST_BASIC_USERNAME = "rest.auth.basic.username";
  private static final String ICEBERG_REST_BASIC_PASSWORD = "rest.auth.basic.password";
  private static final String ICEBERG_REST_OAUTH2_TOKEN = "rest.auth.oauth2.token";
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
  private final String oauthToken;

  private GravitinoAuthSettings(
      String authType,
      String username,
      String password,
      String oauthServerUri,
      String oauthPath,
      String oauthCredential,
      String oauthScope,
      String oauthToken) {
    this.authType = authType;
    this.username = username;
    this.password = password;
    this.oauthServerUri = oauthServerUri;
    this.oauthPath = oauthPath;
    this.oauthCredential = oauthCredential;
    this.oauthScope = oauthScope;
    this.oauthToken = oauthToken;
  }

  /**
   * Job-template environment placeholders. Unresolved placeholders are omitted at job runtime so
   * unauthenticated deployments keep working.
   *
   * @return environment name to {@code {{jobConf key}}} mapping
   */
  public static Map<String, String> jobTemplateEnvironments() {
    return ImmutableMap.<String, String>builder()
        .put(ENV_AUTH_TYPE, placeholder(JOB_CONF_AUTH_TYPE))
        .put(ENV_AUTH_USERNAME, placeholder(JOB_CONF_AUTH_USERNAME))
        .put(ENV_GRAVITINO_USER, placeholder(JOB_CONF_AUTH_USERNAME))
        .put(ENV_AUTH_PASSWORD, placeholder(JOB_CONF_AUTH_PASSWORD))
        .put(ENV_AUTH_OAUTH_SERVER_URI, placeholder(JOB_CONF_AUTH_OAUTH_SERVER_URI))
        .put(ENV_AUTH_OAUTH_PATH, placeholder(JOB_CONF_AUTH_OAUTH_PATH))
        .put(ENV_AUTH_OAUTH_CREDENTIAL, placeholder(JOB_CONF_AUTH_OAUTH_CREDENTIAL))
        .put(ENV_AUTH_OAUTH_SCOPE, placeholder(JOB_CONF_AUTH_OAUTH_SCOPE))
        .put(ENV_AUTH_OAUTH_TOKEN, placeholder(JOB_CONF_AUTH_OAUTH_TOKEN))
        .build();
  }

  /**
   * Resolves settings from optimizer config, falling back to process environment variables.
   *
   * @param config optimizer configuration, may be {@code null}
   * @return resolved authentication settings
   */
  public static GravitinoAuthSettings from(OptimizerConfig config) {
    return from(config, System::getenv);
  }

  /**
   * Resolves settings from optimizer config and an environment lookup.
   *
   * @param config optimizer configuration, may be {@code null}
   * @param getenv environment lookup
   * @return resolved authentication settings
   */
  public static GravitinoAuthSettings from(
      OptimizerConfig config, Function<String, String> getenv) {
    Function<String, String> env = getenv == null ? key -> null : getenv;
    return new GravitinoAuthSettings(
        firstNonBlank(configValue(config, OptimizerConfig.AUTH_TYPE), env.apply(ENV_AUTH_TYPE)),
        firstNonBlank(
            configValue(config, OptimizerConfig.AUTH_USERNAME),
            env.apply(ENV_AUTH_USERNAME),
            env.apply(ENV_GRAVITINO_USER)),
        firstNonBlank(
            configValue(config, OptimizerConfig.AUTH_PASSWORD), env.apply(ENV_AUTH_PASSWORD)),
        firstNonBlank(
            configValue(config, OptimizerConfig.AUTH_OAUTH_SERVER_URI),
            env.apply(ENV_AUTH_OAUTH_SERVER_URI)),
        firstNonBlank(
            configValue(config, OptimizerConfig.AUTH_OAUTH_PATH), env.apply(ENV_AUTH_OAUTH_PATH)),
        firstNonBlank(
            configValue(config, OptimizerConfig.AUTH_OAUTH_CREDENTIAL),
            env.apply(ENV_AUTH_OAUTH_CREDENTIAL)),
        firstNonBlank(
            configValue(config, OptimizerConfig.AUTH_OAUTH_SCOPE), env.apply(ENV_AUTH_OAUTH_SCOPE)),
        firstNonBlank(
            configValue(config, OptimizerConfig.AUTH_OAUTH_TOKEN),
            env.apply(ENV_AUTH_OAUTH_TOKEN)));
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
    copyAlias(properties, OptimizerConfig.AUTH_TYPE, "auth_type", JOB_CONF_AUTH_TYPE);
    copyAlias(
        properties, OptimizerConfig.AUTH_USERNAME, "username", "user", JOB_CONF_AUTH_USERNAME);
    copyAlias(properties, OptimizerConfig.AUTH_PASSWORD, "password", JOB_CONF_AUTH_PASSWORD);
    copyAlias(
        properties,
        OptimizerConfig.AUTH_OAUTH_SERVER_URI,
        "oauth_server_uri",
        JOB_CONF_AUTH_OAUTH_SERVER_URI);
    copyAlias(properties, OptimizerConfig.AUTH_OAUTH_PATH, "oauth_path", JOB_CONF_AUTH_OAUTH_PATH);
    copyAlias(
        properties,
        OptimizerConfig.AUTH_OAUTH_CREDENTIAL,
        "oauth_credential",
        JOB_CONF_AUTH_OAUTH_CREDENTIAL);
    copyAlias(
        properties, OptimizerConfig.AUTH_OAUTH_SCOPE, "oauth_scope", JOB_CONF_AUTH_OAUTH_SCOPE);
    copyAlias(
        properties, OptimizerConfig.AUTH_OAUTH_TOKEN, "oauth_token", JOB_CONF_AUTH_OAUTH_TOKEN);
  }

  /**
   * Copies optimizer {@code gravitino.optimizer.auth.*} values into jobConf keys used by built-in
   * Iceberg job templates, without overwriting keys already present.
   *
   * @param jobConf job configuration map to update
   * @param config optimizer configuration
   */
  public static void copyToJobConf(Map<String, String> jobConf, OptimizerConfig config) {
    if (jobConf == null || config == null) {
      return;
    }
    putIfAbsent(jobConf, JOB_CONF_AUTH_TYPE, configValue(config, OptimizerConfig.AUTH_TYPE));
    putIfAbsent(
        jobConf, JOB_CONF_AUTH_USERNAME, configValue(config, OptimizerConfig.AUTH_USERNAME));
    putIfAbsent(
        jobConf, JOB_CONF_AUTH_PASSWORD, configValue(config, OptimizerConfig.AUTH_PASSWORD));
    putIfAbsent(
        jobConf,
        JOB_CONF_AUTH_OAUTH_SERVER_URI,
        configValue(config, OptimizerConfig.AUTH_OAUTH_SERVER_URI));
    putIfAbsent(
        jobConf, JOB_CONF_AUTH_OAUTH_PATH, configValue(config, OptimizerConfig.AUTH_OAUTH_PATH));
    putIfAbsent(
        jobConf,
        JOB_CONF_AUTH_OAUTH_CREDENTIAL,
        configValue(config, OptimizerConfig.AUTH_OAUTH_CREDENTIAL));
    putIfAbsent(
        jobConf, JOB_CONF_AUTH_OAUTH_SCOPE, configValue(config, OptimizerConfig.AUTH_OAUTH_SCOPE));
    putIfAbsent(
        jobConf, JOB_CONF_AUTH_OAUTH_TOKEN, configValue(config, OptimizerConfig.AUTH_OAUTH_TOKEN));
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
        if (StringUtils.isNotBlank(oauthToken)) {
          // Static bearer token; not refreshed. Prefer client-credentials for long jobs.
          builder.withHeaders(ImmutableMap.of("Authorization", "Bearer " + oauthToken));
        } else {
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
        }
        break;
      case TYPE_NONE:
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported gravitino.optimizer.auth.type: "
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
        configs.put(prefix + ICEBERG_REST_AUTH_TYPE, "oauth2");
        if (StringUtils.isNotBlank(oauthToken)) {
          configs.put(prefix + ICEBERG_REST_OAUTH2_TOKEN, oauthToken);
        } else {
          requireValue(oauthServerUri, "oauth server URI is required for oauth authentication");
          requireValue(oauthPath, "oauth path is required for oauth authentication");
          requireValue(oauthCredential, "oauth credential is required for oauth authentication");
          // Iceberg REST expects the full token endpoint in oauth2-server-uri (same as Spark
          // IcebergRestOAuthConfig.joinUri).
          configs.put(prefix + ICEBERG_REST_OAUTH2_SERVER_URI, joinUri(oauthServerUri, oauthPath));
          configs.put(prefix + ICEBERG_REST_OAUTH2_CREDENTIAL, oauthCredential);
          if (StringUtils.isNotBlank(oauthScope)) {
            configs.put(prefix + ICEBERG_REST_OAUTH2_SCOPE, oauthScope);
          }
        }
        break;
      case TYPE_SIMPLE:
        // Gravitino simple auth is a Basic token of user:dummy. Iceberg REST accepts the same
        // username with an unused password when the server authenticators include simple.
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
            "Unsupported gravitino.optimizer.auth.type: "
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

  private static String placeholder(String jobConfKey) {
    return "{{" + jobConfKey + "}}";
  }

  private static String configValue(OptimizerConfig config, String key) {
    if (config == null) {
      return null;
    }
    return config.getRawString(key);
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

  private static void putIfAbsent(Map<String, String> target, String key, String value) {
    if (StringUtils.isBlank(target.get(key)) && StringUtils.isNotBlank(value)) {
      target.put(key, value.trim());
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
