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
package org.apache.gravitino.maintenance.policy;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

/**
 * Cluster-static TMS client auth in {@code gravitino.conf}.
 *
 * <p>{@link #GRAVITINO_AUTH_PREFIX} is a <strong>fixed</strong> camelCase bag in {@code
 * gravitino.conf} ({@code authType}, {@code oauthServerUri}, …). {@link #loadGravitinoAuth} maps
 * those suffixes onto built-in updater short names ({@code auth_type}, {@code oauth_server_uri}, …)
 * before overlaying onto policy {@code updateOptions}.
 *
 * <p>{@link #IRC_AUTH_PREFIX} is an <strong>open</strong> passthrough for Spark / Iceberg REST
 * properties (for example {@code rest.auth.type}); any non-empty suffix under the prefix is loaded
 * and overlaid onto policy {@code jobOptions}. Auth wins on duplicate keys. Live {@code runJob}
 * wiring is a follow-up.
 */
public final class MaintenanceAuthConfigs {

  /** Prefix for Gravitino control-plane client auth (overlaid onto {@code updateOptions}). */
  public static final String GRAVITINO_AUTH_PREFIX = "gravitino.maintenance.gravitinoAuth.";

  /**
   * Prefix for Iceberg REST / Spark client properties overlaid onto {@code jobOptions}. Suffixes
   * are not enumerated; operators may pass arbitrary catalog auth keys such as {@code
   * rest.auth.type}.
   */
  public static final String IRC_AUTH_PREFIX = "gravitino.maintenance.ircAuth.";

  /** Auth mode: {@code none}, {@code simple}, {@code basic}, or {@code oauth}. */
  public static final String AUTH_TYPE_SUFFIX = "authType";

  /** HTTP basic / simple user principal. */
  public static final String USERNAME_SUFFIX = "username";

  /** HTTP basic password or secret reference. */
  public static final String PASSWORD_SUFFIX = "password";

  /** OAuth2 token server base URI. */
  public static final String OAUTH_SERVER_URI_SUFFIX = "oauthServerUri";

  /** OAuth2 token path relative to the server URI. */
  public static final String OAUTH_PATH_SUFFIX = "oauthPath";

  /** OAuth2 client credential, often {@code client_id:client_secret}. */
  public static final String OAUTH_CREDENTIAL_SUFFIX = "oauthCredential";

  /** OAuth2 scope. */
  public static final String OAUTH_SCOPE_SUFFIX = "oauthScope";

  /**
   * Maps camelCase {@code gravitino.conf} suffixes to {@code updateOptions} / updater-option short
   * names consumed by {@code GravitinoAuthSettings}.
   */
  private static final Map<String, String> GRAVITINO_AUTH_TO_UPDATE_OPTIONS =
      ImmutableMap.<String, String>builder()
          .put(AUTH_TYPE_SUFFIX, "auth_type")
          .put(USERNAME_SUFFIX, "username")
          .put(PASSWORD_SUFFIX, "password")
          .put(OAUTH_SERVER_URI_SUFFIX, "oauth_server_uri")
          .put(OAUTH_PATH_SUFFIX, "oauth_path")
          .put(OAUTH_CREDENTIAL_SUFFIX, "oauth_credential")
          .put(OAUTH_SCOPE_SUFFIX, "oauth_scope")
          .build();

  public static final ConfigEntry<Optional<String>> GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_TYPE =
      optionalAuth(
          GRAVITINO_AUTH_PREFIX,
          AUTH_TYPE_SUFFIX,
          "TMS Gravitino client auth mode (none, simple, basic, or oauth).");

  public static final ConfigEntry<Optional<String>> GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_USERNAME =
      optionalAuth(GRAVITINO_AUTH_PREFIX, USERNAME_SUFFIX, "TMS Gravitino client user principal.");

  public static final ConfigEntry<Optional<String>> GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_PASSWORD =
      optionalAuth(
          GRAVITINO_AUTH_PREFIX, PASSWORD_SUFFIX, "TMS Gravitino client HTTP basic password.");

  public static final ConfigEntry<Optional<String>>
      GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_OAUTH_SERVER_URI =
          optionalAuth(
              GRAVITINO_AUTH_PREFIX,
              OAUTH_SERVER_URI_SUFFIX,
              "TMS Gravitino client OAuth2 token server URI.");

  public static final ConfigEntry<Optional<String>>
      GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_OAUTH_PATH =
          optionalAuth(
              GRAVITINO_AUTH_PREFIX, OAUTH_PATH_SUFFIX, "TMS Gravitino client OAuth2 token path.");

  public static final ConfigEntry<Optional<String>>
      GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_OAUTH_CREDENTIAL =
          optionalAuth(
              GRAVITINO_AUTH_PREFIX,
              OAUTH_CREDENTIAL_SUFFIX,
              "TMS Gravitino client OAuth2 client credential.");

  public static final ConfigEntry<Optional<String>>
      GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_OAUTH_SCOPE =
          optionalAuth(
              GRAVITINO_AUTH_PREFIX, OAUTH_SCOPE_SUFFIX, "TMS Gravitino client OAuth2 scope.");

  private MaintenanceAuthConfigs() {}

  /**
   * Loads fixed Gravitino auth entries from server config and remaps camelCase conf suffixes to
   * updater short names for {@code updateOptions}.
   *
   * @param config gravitino server config
   * @return unmodifiable map keyed by updater short names; empty when unset
   */
  public static Map<String, String> loadGravitinoAuth(Config config) {
    Map<String, String> loaded = loadAuthBag(config, GRAVITINO_AUTH_PREFIX);
    if (loaded.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, String> updateOptions = new LinkedHashMap<>();
    GRAVITINO_AUTH_TO_UPDATE_OPTIONS.forEach(
        (suffix, updateKey) -> {
          String value = loaded.get(suffix);
          if (value != null) {
            updateOptions.put(updateKey, value);
          }
        });
    loaded.forEach(
        (suffix, value) -> {
          if (!GRAVITINO_AUTH_TO_UPDATE_OPTIONS.containsKey(suffix)) {
            updateOptions.put(suffix, value);
          }
        });
    return ImmutableMap.copyOf(updateOptions);
  }

  /**
   * Loads arbitrary IRC / Spark property suffixes under {@link #IRC_AUTH_PREFIX}.
   *
   * @param config gravitino server config
   * @return unmodifiable map; empty when unset
   */
  public static Map<String, String> loadIrcAuth(Config config) {
    return loadAuthBag(config, IRC_AUTH_PREFIX);
  }

  /**
   * Merges policy {@code jobOptions} with IRC auth passthrough; auth wins on duplicate keys.
   *
   * @param policyJobOptions options from the effective policy (may be null)
   * @param ircAuth suffix-keyed IRC / Spark properties (may be null)
   * @return merged job options for {@code runJob}
   */
  public static Map<String, String> overlayJobOptions(
      @Nullable Map<String, String> policyJobOptions, @Nullable Map<String, String> ircAuth) {
    return overlay(policyJobOptions, ircAuth);
  }

  /**
   * Merges policy {@code updateOptions} with Gravitino auth; auth wins on duplicate keys.
   *
   * @param policyUpdateOptions options from the effective policy (may be null)
   * @param gravitinoAuth suffix-keyed Gravitino auth bag (may be null)
   * @return merged update options for {@code runJob}
   */
  public static Map<String, String> overlayUpdateOptions(
      @Nullable Map<String, String> policyUpdateOptions,
      @Nullable Map<String, String> gravitinoAuth) {
    return overlay(policyUpdateOptions, gravitinoAuth);
  }

  /**
   * Loads IRC passthrough from {@code config} and overlays it onto policy {@code jobOptions}.
   *
   * @param config gravitino server config
   * @param policyJobOptions options from the effective policy
   * @return merged job options
   */
  public static Map<String, String> overlayJobOptions(
      Config config, @Nullable Map<String, String> policyJobOptions) {
    return overlayJobOptions(policyJobOptions, loadIrcAuth(config));
  }

  /**
   * Loads Gravitino auth from {@code config} and overlays it onto policy {@code updateOptions}.
   *
   * @param config gravitino server config
   * @param policyUpdateOptions options from the effective policy
   * @return merged update options
   */
  public static Map<String, String> overlayUpdateOptions(
      Config config, @Nullable Map<String, String> policyUpdateOptions) {
    return overlayUpdateOptions(policyUpdateOptions, loadGravitinoAuth(config));
  }

  private static Map<String, String> loadAuthBag(Config config, String prefix) {
    Map<String, String> loaded = config.getConfigsWithPrefix(prefix);
    if (loaded.isEmpty()) {
      return Collections.emptyMap();
    }
    loaded.forEach(
        (key, value) -> {
          if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                String.format("Configuration %s%s must have a non-empty value", prefix, key));
          }
        });
    return ImmutableMap.copyOf(loaded);
  }

  private static Map<String, String> overlay(
      @Nullable Map<String, String> base, @Nullable Map<String, String> auth) {
    if ((base == null || base.isEmpty()) && (auth == null || auth.isEmpty())) {
      return Collections.emptyMap();
    }
    Map<String, String> merged = new LinkedHashMap<>();
    if (base != null) {
      merged.putAll(base);
    }
    if (auth != null) {
      merged.putAll(auth);
    }
    return ImmutableMap.copyOf(merged);
  }

  private static ConfigEntry<Optional<String>> optionalAuth(
      String prefix, String suffix, String doc) {
    return new ConfigBuilder(prefix + suffix)
        .doc(doc)
        .version(ConfigConstants.VERSION_2_0_0)
        .stringConf()
        .createWithOptional();
  }
}
