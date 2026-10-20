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
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
 * before overlaying onto policy {@code updaterOptions}. Unknown or blank suffixes are rejected, and
 * {@code authType} plus mode-required fields are validated at load time.
 *
 * <p>{@link #IRC_AUTH_PREFIX} is an <strong>open</strong> passthrough for Spark / Iceberg REST
 * properties (for example {@code rest.auth.type}); any non-empty suffix under the prefix is loaded
 * and overlaid onto policy {@code jobOptions}. Auth wins on duplicate keys. Cron {@code runJob}
 * packs those IRC keys into {@code spark_conf} as {@code spark.sql.catalog.<catalog>.*}.
 */
public final class MaintenanceAuthConfigs {

  /** Prefix for Gravitino control-plane client auth (overlaid onto {@code updaterOptions}). */
  public static final String GRAVITINO_AUTH_PREFIX = "gravitino.maintenance.gravitinoAuth.";

  /**
   * Prefix for Iceberg REST / Spark client properties overlaid onto {@code jobOptions}. Suffixes
   * are not enumerated; operators may pass arbitrary catalog auth keys such as {@code
   * rest.auth.type}.
   */
  public static final String IRC_AUTH_PREFIX = "gravitino.maintenance.ircAuth.";

  /**
   * Cluster-static base URI Spark jobs use for TMS sample/finish callbacks ({@code
   * updater_options.gravitino_uri}). Policy {@code updaterOptions} may override.
   */
  public static final ConfigEntry<Optional<String>> GRAVITINO_MAINTENANCE_GRAVITINO_URI =
      new ConfigBuilder("gravitino.maintenance.gravitinoUri")
          .doc(
              "Base URI for Spark → Gravitino TMS callbacks (sample/finish). Injected into cron "
                  + "updater_options when the policy does not set gravitino_uri.")
          .version(ConfigConstants.VERSION_2_0_0)
          .stringConf()
          .createWithOptional();

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

  private static final String AUTH_TYPE_NONE = "none";
  private static final String AUTH_TYPE_SIMPLE = "simple";
  private static final String AUTH_TYPE_BASIC = "basic";
  private static final String AUTH_TYPE_OAUTH = "oauth";

  private static final Set<String> SUPPORTED_AUTH_TYPES =
      ImmutableSet.of(AUTH_TYPE_NONE, AUTH_TYPE_SIMPLE, AUTH_TYPE_BASIC, AUTH_TYPE_OAUTH);

  /**
   * Maps camelCase {@code gravitino.conf} suffixes to {@code updaterOptions} / updater-option short
   * names consumed by {@code GravitinoAuthSettings}.
   */
  private static final Map<String, String> GRAVITINO_AUTH_TO_UPDATER_OPTIONS =
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
   * updater short names for {@code updaterOptions}.
   *
   * @param config gravitino server config
   * @return unmodifiable map keyed by updater short names; empty when unset
   */
  public static Map<String, String> loadGravitinoAuth(Config config) {
    Map<String, String> loaded = loadAuthBag(config, GRAVITINO_AUTH_PREFIX, /*fixedSchema=*/ true);
    if (loaded.isEmpty()) {
      return Collections.emptyMap();
    }
    validateGravitinoAuth(loaded);
    Map<String, String> updaterOptions = new LinkedHashMap<>();
    GRAVITINO_AUTH_TO_UPDATER_OPTIONS.forEach(
        (suffix, updateKey) -> {
          String value = loaded.get(suffix);
          if (value != null) {
            updaterOptions.put(updateKey, value);
          }
        });
    return ImmutableMap.copyOf(updaterOptions);
  }

  /**
   * Loads arbitrary IRC / Spark property suffixes under {@link #IRC_AUTH_PREFIX}.
   *
   * @param config gravitino server config
   * @return unmodifiable map; empty when unset
   */
  public static Map<String, String> loadIrcAuth(Config config) {
    return loadAuthBag(config, IRC_AUTH_PREFIX, /*fixedSchema=*/ false);
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
   * Merges policy {@code updaterOptions} with Gravitino auth; auth wins on duplicate keys.
   *
   * @param policyUpdaterOptions options from the effective policy (may be null)
   * @param gravitinoAuth updater-short-name Gravitino auth bag (may be null)
   * @return merged updater options for {@code runJob}
   */
  public static Map<String, String> overlayUpdaterOptions(
      @Nullable Map<String, String> policyUpdaterOptions,
      @Nullable Map<String, String> gravitinoAuth) {
    return overlay(policyUpdaterOptions, gravitinoAuth);
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
   * Loads Gravitino auth from {@code config} and overlays it onto policy {@code updaterOptions}.
   *
   * @param config gravitino server config
   * @param policyUpdaterOptions options from the effective policy
   * @return merged updater options
   */
  public static Map<String, String> overlayUpdaterOptions(
      Config config, @Nullable Map<String, String> policyUpdaterOptions) {
    return overlayUpdaterOptions(policyUpdaterOptions, loadGravitinoAuth(config));
  }

  private static Map<String, String> loadAuthBag(
      Config config, String prefix, boolean fixedSchema) {
    Map<String, String> loaded = config.getConfigsWithPrefix(prefix);
    if (loaded.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, String> sanitized = new LinkedHashMap<>();
    loaded.forEach(
        (key, value) -> {
          if (key == null || key.trim().isEmpty()) {
            throw new IllegalArgumentException(
                String.format("Configuration under %s must not use a blank suffix", prefix));
          }
          String suffix = key.trim();
          if (fixedSchema && !GRAVITINO_AUTH_TO_UPDATER_OPTIONS.containsKey(suffix)) {
            throw new IllegalArgumentException(
                String.format(
                    "Unknown Gravitino auth configuration %s%s; supported suffixes are: %s",
                    prefix, suffix, String.join(", ", GRAVITINO_AUTH_TO_UPDATER_OPTIONS.keySet())));
          }
          if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                String.format("Configuration %s%s must have a non-empty value", prefix, suffix));
          }
          sanitized.put(suffix, value.trim());
        });
    return ImmutableMap.copyOf(sanitized);
  }

  private static void validateGravitinoAuth(Map<String, String> loaded) {
    String authType = loaded.get(AUTH_TYPE_SUFFIX);
    if (authType == null) {
      throw new IllegalArgumentException(
          String.format(
              "Configuration %s%s is required when any Gravitino auth setting is present",
              GRAVITINO_AUTH_PREFIX, AUTH_TYPE_SUFFIX));
    }
    String normalized = authType.toLowerCase(Locale.ROOT);
    if (!SUPPORTED_AUTH_TYPES.contains(normalized)) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported %s%s value '%s'; supported values are: none, simple, basic, oauth",
              GRAVITINO_AUTH_PREFIX, AUTH_TYPE_SUFFIX, authType));
    }
    switch (normalized) {
      case AUTH_TYPE_BASIC:
        requireSuffix(loaded, USERNAME_SUFFIX, AUTH_TYPE_BASIC);
        requireSuffix(loaded, PASSWORD_SUFFIX, AUTH_TYPE_BASIC);
        break;
      case AUTH_TYPE_OAUTH:
        requireSuffix(loaded, OAUTH_SERVER_URI_SUFFIX, AUTH_TYPE_OAUTH);
        requireSuffix(loaded, OAUTH_PATH_SUFFIX, AUTH_TYPE_OAUTH);
        requireSuffix(loaded, OAUTH_CREDENTIAL_SUFFIX, AUTH_TYPE_OAUTH);
        requireSuffix(loaded, OAUTH_SCOPE_SUFFIX, AUTH_TYPE_OAUTH);
        break;
      case AUTH_TYPE_NONE:
      case AUTH_TYPE_SIMPLE:
      default:
        break;
    }
  }

  private static void requireSuffix(Map<String, String> loaded, String suffix, String authType) {
    if (!loaded.containsKey(suffix)) {
      throw new IllegalArgumentException(
          String.format(
              "Configuration %s%s is required when %s%s is '%s'",
              GRAVITINO_AUTH_PREFIX, suffix, GRAVITINO_AUTH_PREFIX, AUTH_TYPE_SUFFIX, authType));
    }
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
