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
package org.apache.gravitino.trino.connector;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

import com.google.common.base.Splitter;
import io.trino.spi.TrinoException;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.trino.connector.security.GravitinoAuthProvider;

/** Gravitino config. */
public class GravitinoConfig {

  // Trino config keys
  /** The Trino discovery URI. */
  private static final String TRINO_DISCOVERY_URI = "discovery.uri";
  /** The Trino catalog config directory. */
  private static final String TRINO_CATALOG_CONFIG_DIR = "catalog.config-dir";
  /** The Trino plugin bundles. */
  public static final String TRINO_PLUGIN_BUNDLES = "plugin.bundles";
  /** The Trino catalog store. */
  public static final String TRINO_CATALOG_STORE = "catalog.store";
  /** The Trino catalog management. */
  public static final String TRINO_CATALOG_MANAGEMENT = "catalog.management";
  /** The common prefix of all internal Trino JDBC connection configurations. */
  private static final String TRINO_JDBC_CONFIG_PREFIX = "trino.jdbc.";

  // Trino config default value
  /** The Trino catalog config directory default value. */
  private static final String TRINO_CATALOG_CONFIG_DIR_DEFAULT_VALUE = "etc/catalog";
  /** The Trino catalog store default value. */
  public static final String TRINO_CATALOG_STORE_DEFAULT_VALUE = "file";
  /** The Trino catalog management default value. */
  public static final String TRINO_CATALOG_MANAGEMENT_DEFAULT_VALUE = "static";
  /** The default port used when the Trino `discovery.uri` omits it and the scheme is http. */
  private static final int HTTP_DEFAULT_PORT = 80;
  /** The default port used when the Trino `discovery.uri` omits it and the scheme is https. */
  private static final int HTTPS_DEFAULT_PORT = 443;

  // The Trino configuration of etc/config.properties
  /** The Trino configuration. */
  public static final TrinoConfig trinoConfig = new TrinoConfig();

  // Gravitino config keys
  /** The Gravitino dynamic connector. */
  public static final String GRAVITINO_DYNAMIC_CONNECTOR = "__gravitino.dynamic.connector";
  /** The Gravitino dynamic connector catalog config. */
  public static final String GRAVITINO_DYNAMIC_CONNECTOR_CATALOG_CONFIG =
      "__gravitino.dynamic.connector.catalog.config";

  /** The Trino Iceberg REST catalog property prefix. */
  private static final String TRINO_ICEBERG_REST_CATALOG_PREFIX = "iceberg.rest-catalog.";

  /** Prefix for environment-variable references propagated to dynamic catalogs. */
  static final String GRAVITINO_DYNAMIC_CATALOG_ENV_PREFIX =
      "gravitino.dynamic-catalog.environment-variable.";

  private static final Pattern ENVIRONMENT_VARIABLE_NAME =
      Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  private static final Map<String, ConfigEntry> CONFIG_DEFINITIONS = new HashMap<>();
  private final Map<String, String> config;
  private final List<Pattern> skipCatalogPatternList;
  // Iceberg REST server endpoints discovered from the Gravitino server, keyed by metalake. Written
  // by the catalog connector manager's periodic poll and read on every Iceberg catalog load.
  private final Map<String, String> discoveredIcebergRestUriByMetalake = new ConcurrentHashMap<>();

  // Gravitino config entity
  private static final ConfigEntry GRAVITINO_URI =
      new ConfigEntry(
          "gravitino.uri", "The URI of the Gravitino server", "http://localhost:8090", false);

  private static final ConfigEntry GRAVITINO_METALAKE =
      new ConfigEntry(
          "gravitino.metalake",
          "The name of the metalake (top-level namespace) to connect to",
          "",
          true);

  private static final ConfigEntry GRAVITINO_USER =
      new ConfigEntry(
          "gravitino.user",
          "The username for simple authentication with the Gravitino server",
          "",
          false);

  /**
   * @deprecated Please use {@code gravitino.use-single-metalake} instead.
   */
  @Deprecated
  @SuppressWarnings("UnusedVariable")
  private static final ConfigEntry GRAVITINO_SIMPLIFY_CATALOG_NAMES =
      new ConfigEntry(
          "gravitino.simplify-catalog-names",
          "Deprecated: omits the metalake prefix from catalog names. Use gravitino.use-single-metalake instead.",
          "true",
          false);

  private static final ConfigEntry GRAVITINO_SINGLE_METALAKE_MODE =
      new ConfigEntry(
          "gravitino.use-single-metalake",
          "If true, only one metalake is supported in this connector; identify the catalog by <catalog_name>. "
              + "If false, multiple metalakes are supported; identify the catalog by <metalake_name>.<catalog_name>.",
          "true",
          false);

  private static final ConfigEntry GRAVITINO_CLOUD_REGION_CODE =
      new ConfigEntry(
          "gravitino.cloud.region-code",
          "Cloud region code for filtering catalogs by region. Leave empty for on-premises deployments.",
          "",
          false);

  private static final ConfigEntry GRAVITINO_CATALOG_CONNECTOR_FACTORY_CLASS_NAME =
      new ConfigEntry(
          "gravitino.catalog.connector.factory.class.name",
          "Fully qualified class name of a custom CatalogConnectorFactory implementation. If omitted, the default factory is used.",
          "",
          false);

  private static final ConfigEntry TRINO_JDBC_USER =
      new ConfigEntry(
          "trino.jdbc.user", "The JDBC username for connecting to Trino", "admin", false);

  private static final ConfigEntry TRINO_JDBC_PASSWORD =
      new ConfigEntry(
          "trino.jdbc.password", "The JDBC password for connecting to Trino", "", false);

  private static final ConfigEntry TRINO_JDBC_SSL_ENABLED =
      new ConfigEntry(
          "trino.jdbc.ssl.enabled",
          "Whether the internal JDBC connection to the Trino coordinator uses TLS. "
              + "If not set, it is derived from the scheme of the Trino `discovery.uri`.",
          "",
          false);

  private static final ConfigEntry TRINO_JDBC_SSL_TRUSTSTORE_PATH =
      new ConfigEntry(
          "trino.jdbc.ssl.truststore.path",
          "Path of the truststore holding the Trino coordinator certificate. "
              + "If omitted, the default JVM truststore is used.",
          "",
          false);

  private static final ConfigEntry TRINO_JDBC_SSL_TRUSTSTORE_PASSWORD =
      new ConfigEntry(
          "trino.jdbc.ssl.truststore.password",
          "Password of the truststore configured by `trino.jdbc.ssl.truststore.path`",
          "",
          false);

  private static final ConfigEntry TRINO_JDBC_SSL_TRUSTSTORE_TYPE =
      new ConfigEntry(
          "trino.jdbc.ssl.truststore.type",
          "Type of the truststore, for example JKS or PKCS12. "
              + "If omitted, the default JVM truststore type is used.",
          "",
          false);

  private static final ConfigEntry TRINO_JDBC_SSL_KEYSTORE_PATH =
      new ConfigEntry(
          "trino.jdbc.ssl.keystore.path",
          "Path of the keystore holding the client certificate presented to the Trino coordinator, "
              + "used by coordinators that require mutual TLS.",
          "",
          false);

  private static final ConfigEntry TRINO_JDBC_SSL_KEYSTORE_PASSWORD =
      new ConfigEntry(
          "trino.jdbc.ssl.keystore.password",
          "Password of the keystore configured by `trino.jdbc.ssl.keystore.path`",
          "",
          false);

  private static final ConfigEntry TRINO_JDBC_SSL_KEYSTORE_TYPE =
      new ConfigEntry(
          "trino.jdbc.ssl.keystore.type",
          "Type of the keystore, for example JKS or PKCS12. "
              + "If omitted, the default JVM keystore type is used.",
          "",
          false);

  private static final ConfigEntry TRINO_JDBC_SSL_VERIFICATION =
      new ConfigEntry(
          "trino.jdbc.ssl.verification",
          "Certificate verification mode of the internal JDBC connection: FULL, CA or NONE. "
              + "NONE disables certificate verification and should only be used for troubleshooting.",
          "FULL",
          false);

  private static final ConfigEntry TRINO_JDBC_ROLES =
      new ConfigEntry(
          "trino.jdbc.roles",
          "Session roles applied to the internal JDBC connection, for example `system:sysadmin`. "
              + "Required by deployments that only allow CREATE CATALOG with a privileged role.",
          "",
          false);

  private static final ConfigEntry TRINO_JDBC_EXTRA_PROPERTIES_PREFIX =
      new ConfigEntry(
          "trino.jdbc.properties.",
          "Prefix for Trino JDBC driver properties. Any property beginning with this prefix is "
              + "passed to the driver verbatim with the prefix removed "
              + "(e.g., trino.jdbc.properties.KerberosRemoteServiceName=trino), "
              + "overriding the properties derived from the dedicated `trino.jdbc.*` configurations.",
          "",
          false);

  private static final ConfigEntry GRAVITINO_METADATA_REFRESH_INTERVAL_SECOND =
      new ConfigEntry(
          "gravitino.metadata.refresh-interval-seconds",
          "The interval in seconds to refresh the metadata from Gravitino server",
          "10",
          false);

  private static final ConfigEntry GRAVITINO_TRINO_SKIP_VERSION_VALIDATION =
      new ConfigEntry(
          "gravitino.trino.skip-version-validation",
          "When true, skips Trino version validation and logs a warning instead of throwing an error. Gravitino supports Trino versions 440-478; other versions are untested.",
          "false",
          false);

  private static final ConfigEntry GRAVITINO_CLIENT_CONFIG_PREFIX =
      new ConfigEntry(
          "gravitino.client.",
          "Prefix for Gravitino client properties. Any property beginning with this prefix is passed through to the Gravitino client (e.g., gravitino.client.auth.type=oauth2).",
          "",
          false);

  private static final ConfigEntry GRAVITINO_TRINO_SKIP_CATALOG_PATTERNS =
      new ConfigEntry(
          "gravitino.trino.skip-catalog-patterns",
          "Comma-separated list of regex patterns matching Gravitino catalog names to exclude from loading into Trino.",
          "",
          false);

  private static final ConfigEntry GRAVITINO_SESSION_CACHE_MAX_SIZE =
      new ConfigEntry(
          GravitinoAuthProvider.SESSION_CACHE_MAX_SIZE_KEY,
          "Maximum number of per-user sessions to keep in the cache when session.forwardUser=true",
          "500",
          false);

  private static final ConfigEntry GRAVITINO_SESSION_CACHE_EXPIRE_AFTER_ACCESS_SECONDS =
      new ConfigEntry(
          GravitinoAuthProvider.SESSION_CACHE_EXPIRE_AFTER_ACCESS_SECONDS_KEY,
          "Seconds before an idle per-user session is evicted from the cache when session.forwardUser=true",
          "3600",
          false);

  private static final ConfigEntry GRAVITINO_ICEBERG_REST_URI =
      new ConfigEntry(
          "gravitino.iceberg.rest-uri",
          "The endpoint of the Gravitino Iceberg REST server. Discovered automatically from the "
              + "Gravitino server by default; set this only to override the discovered value, "
              + "for example when the Iceberg REST server is not reachable at the address the "
              + "Gravitino server itself reports.",
          "",
          false);

  private static final ConfigEntry GRAVITINO_ICEBERG_REST_ROUTING_ENABLED =
      new ConfigEntry(
          "gravitino.iceberg.rest-routing-enabled",
          "Whether non-REST Iceberg catalogs must be routed through the Gravitino Iceberg REST "
              + "server. Disable this only to retain the legacy catalog-backend translation.",
          "true",
          false);

  private static final ConfigEntry GRAVITINO_ICEBERG_REST_CATALOG_CONFIG_PREFIX =
      new ConfigEntry(
          "gravitino.iceberg.rest-catalog.",
          "Prefix for properties passed through to the internal Trino Iceberg REST catalog. Any "
              + "property beginning with this prefix is rewritten to iceberg.rest-catalog. and "
              + "passed through (e.g., gravitino.iceberg.rest-catalog.security=OAUTH2).",
          "",
          false);

  /**
   * Constructs a new GravitinoConfig with the specified configuration.
   *
   * @param requiredConfig The map of configuration key-value pairs
   * @throws TrinoException if required configuration is missing
   */
  public GravitinoConfig(Map<String, String> requiredConfig) {
    config = requiredConfig;
    for (Map.Entry<String, ConfigEntry> entry : CONFIG_DEFINITIONS.entrySet()) {
      ConfigEntry configDefinition = entry.getValue();
      if (configDefinition.isRequired && !config.containsKey(configDefinition.key)) {
        String message =
            String.format("Missing gravitino config, %s is required", configDefinition.key);
        throw new TrinoException(GravitinoErrorCode.GRAVITINO_MISSING_CONFIG, message);
      }
    }
    if (isDynamicConnector() && !config.containsKey(GRAVITINO_DYNAMIC_CONNECTOR_CATALOG_CONFIG)) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_MISSING_CONFIG,
          "Incomplete Dynamic catalog connector config");
    }
    try {
      skipCatalogPatternList = initSkipCatalogPatterns();
    } catch (Exception e) {
      throw new TrinoException(
          NOT_SUPPORTED,
          "Config `gravitino.trino.skip-catalog-patterns` is invalid because it contains an illegal regular expression",
          e);
    }
  }

  /**
   * Retrieves the URI of the gravitino web server.
   *
   * @return the URI of the gravitino web server
   */
  public String getURI() {
    return config.getOrDefault(GRAVITINO_URI.key, GRAVITINO_URI.defaultValue);
  }

  /**
   * Retrieves the metalake name for used.
   *
   * @return the metalake name for used
   */
  public String getMetalake() {
    // Trimmed so a stray leading/trailing space in the catalog properties file does not make this
    // value silently stop matching the canonical metalake name the load loop records states
    // under, e.g. in the catalog_status/load_status system tables' per-metalake filtering.
    return config.getOrDefault(GRAVITINO_METALAKE.key, GRAVITINO_METALAKE.defaultValue).trim();
  }

  /**
   * Retrieves the username for simple authentication.
   *
   * @return the username, or empty string if not configured
   */
  public String getUser() {
    return config.getOrDefault(GRAVITINO_USER.key, GRAVITINO_USER.defaultValue);
  }

  /**
   * Retrieves the config for Gravitino client.
   *
   * @return the config properties map
   */
  public Map<String, String> getClientConfig() {
    return config.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(GRAVITINO_CLIENT_CONFIG_PREFIX.key))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Retrieves the single metalake mode.
   *
   * @return the single metalake mode
   */
  public boolean singleMetalakeMode() {
    return Boolean.parseBoolean(
        config.getOrDefault(
            GRAVITINO_SINGLE_METALAKE_MODE.key, GRAVITINO_SINGLE_METALAKE_MODE.defaultValue));
  }

  boolean isDynamicConnector() {
    // 'isDynamicConnector' indicates whether the connector is user-configured within Trino or
    // loaded from the Gravitino server.
    // When a connector is loaded via Trino configuration,
    // it is static and will always create an instance of GravitinoSystemConnector.
    // Otherwise, it is dynamically loaded from the Gravitino server,
    // in which case the connector's configuration is set to '__gravitino.dynamic.connector=true'.
    // It is dynamic and will create an instance of GravitinoConnector.
    return config.getOrDefault(GRAVITINO_DYNAMIC_CONNECTOR, "false").equals("true");
  }

  /**
   * Retrieves the catalog config.
   *
   * @return the catalog config
   */
  public String getCatalogConfig() {
    return config.get(GRAVITINO_DYNAMIC_CONNECTOR_CATALOG_CONFIG);
  }

  /**
   * Retrieves the Trino JDBC URI.
   *
   * @return the Trino JDBC URI
   */
  public String getTrinoJdbcURI() {
    URI trinoURI = parseDiscoveryUri();
    int port = trinoURI.getPort();
    if (port < 0) {
      // `discovery.uri` may omit the port, for example a TLS coordinator behind a load balancer
      // on the standard HTTPS port. Fall back to the default port of the scheme.
      port = isHttpsScheme(trinoURI) ? HTTPS_DEFAULT_PORT : HTTP_DEFAULT_PORT;
    }
    return String.format("jdbc:trino://%s:%s", trinoURI.getHost(), port);
  }

  private URI parseDiscoveryUri() {
    String uriString;
    if (config.containsKey(TRINO_DISCOVERY_URI)) {
      uriString = config.get(TRINO_DISCOVERY_URI);
    } else {
      uriString = trinoConfig.getProperty(TRINO_DISCOVERY_URI);
    }
    try {
      return new URI(uriString);
    } catch (Exception e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_MISSING_CONFIG,
          "The Trino configuration of `discovery.uri` = " + uriString + " is not correct");
    }
  }

  private static boolean parseBooleanConfig(String key, String value) {
    // `Boolean.parseBoolean` maps every unrecognized value to false, which would silently disable
    // TLS for a typo such as `yes`, even when the `discovery.uri` scheme implies it should be on.
    if ("true".equalsIgnoreCase(value)) {
      return true;
    }
    if ("false".equalsIgnoreCase(value)) {
      return false;
    }
    throw new TrinoException(
        GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
        String.format(
            "Invalid value for config '%s': expected true or false, got: %s", key, value));
  }

  private static boolean isHttpsScheme(URI uri) {
    return "https".equalsIgnoreCase(uri.getScheme());
  }

  /**
   * Retrieves the region.
   *
   * @return the region
   */
  public String getRegion() {
    return config.getOrDefault(
        GRAVITINO_CLOUD_REGION_CODE.key, GRAVITINO_CLOUD_REGION_CODE.defaultValue);
  }

  /**
   * Retrieves the catalog config directory.
   *
   * @return the catalog config directory
   */
  public String getCatalogConfigDirectory() {
    if (config.containsKey(TRINO_CATALOG_CONFIG_DIR)) {
      return config.get(TRINO_CATALOG_CONFIG_DIR);
    } else {
      return trinoConfig.getProperty(
          TRINO_CATALOG_CONFIG_DIR, TRINO_CATALOG_CONFIG_DIR_DEFAULT_VALUE);
    }
  }

  /**
   * Retrieves the Trino user.
   *
   * @return the Trino user
   */
  public String getTrinoUser() {
    return config.getOrDefault(TRINO_JDBC_USER.key, TRINO_JDBC_USER.defaultValue);
  }

  /**
   * Retrieves the Trino password.
   *
   * @return the Trino password
   */
  public String getTrinoPassword() {
    return config.getOrDefault(TRINO_JDBC_PASSWORD.key, TRINO_JDBC_PASSWORD.defaultValue);
  }

  /**
   * Returns whether the internal JDBC connection to the Trino coordinator uses TLS.
   *
   * <p>If `trino.jdbc.ssl.enabled` is not set, the value is derived from the scheme of the Trino
   * `discovery.uri`, which is `https` on a TLS enabled coordinator.
   *
   * @return true if the internal JDBC connection uses TLS
   */
  public boolean isTrinoJdbcSslEnabled() {
    String value = config.get(TRINO_JDBC_SSL_ENABLED.key);
    if (StringUtils.isNotBlank(value)) {
      return parseBooleanConfig(TRINO_JDBC_SSL_ENABLED.key, value.trim());
    }
    return isHttpsScheme(parseDiscoveryUri());
  }

  /**
   * Retrieves the truststore path of the internal JDBC connection.
   *
   * @return the truststore path, or an empty string if not configured
   */
  public String getTrinoJdbcSslTruststorePath() {
    return config.getOrDefault(
        TRINO_JDBC_SSL_TRUSTSTORE_PATH.key, TRINO_JDBC_SSL_TRUSTSTORE_PATH.defaultValue);
  }

  /**
   * Retrieves the truststore password of the internal JDBC connection.
   *
   * @return the truststore password, or an empty string if not configured
   */
  public String getTrinoJdbcSslTruststorePassword() {
    return config.getOrDefault(
        TRINO_JDBC_SSL_TRUSTSTORE_PASSWORD.key, TRINO_JDBC_SSL_TRUSTSTORE_PASSWORD.defaultValue);
  }

  /**
   * Retrieves the truststore type of the internal JDBC connection.
   *
   * @return the truststore type, or an empty string if not configured
   */
  public String getTrinoJdbcSslTruststoreType() {
    return config.getOrDefault(
        TRINO_JDBC_SSL_TRUSTSTORE_TYPE.key, TRINO_JDBC_SSL_TRUSTSTORE_TYPE.defaultValue);
  }

  /**
   * Retrieves the keystore path of the internal JDBC connection.
   *
   * @return the keystore path, or an empty string if not configured
   */
  public String getTrinoJdbcSslKeystorePath() {
    return config.getOrDefault(
        TRINO_JDBC_SSL_KEYSTORE_PATH.key, TRINO_JDBC_SSL_KEYSTORE_PATH.defaultValue);
  }

  /**
   * Retrieves the keystore password of the internal JDBC connection.
   *
   * @return the keystore password, or an empty string if not configured
   */
  public String getTrinoJdbcSslKeystorePassword() {
    return config.getOrDefault(
        TRINO_JDBC_SSL_KEYSTORE_PASSWORD.key, TRINO_JDBC_SSL_KEYSTORE_PASSWORD.defaultValue);
  }

  /**
   * Retrieves the keystore type of the internal JDBC connection.
   *
   * @return the keystore type, or an empty string if not configured
   */
  public String getTrinoJdbcSslKeystoreType() {
    return config.getOrDefault(
        TRINO_JDBC_SSL_KEYSTORE_TYPE.key, TRINO_JDBC_SSL_KEYSTORE_TYPE.defaultValue);
  }

  /**
   * Retrieves the certificate verification mode of the internal JDBC connection.
   *
   * @return the verification mode, one of FULL, CA or NONE
   */
  public String getTrinoJdbcSslVerification() {
    String value = config.get(TRINO_JDBC_SSL_VERIFICATION.key);
    if (StringUtils.isBlank(value)) {
      return TRINO_JDBC_SSL_VERIFICATION.defaultValue;
    }
    return value.trim().toUpperCase(Locale.ROOT);
  }

  /**
   * Retrieves the session roles applied to the internal JDBC connection.
   *
   * @return the session roles, or an empty string if not configured
   */
  public String getTrinoJdbcRoles() {
    return config.getOrDefault(TRINO_JDBC_ROLES.key, TRINO_JDBC_ROLES.defaultValue);
  }

  /**
   * Retrieves the Trino JDBC driver properties configured with the `trino.jdbc.properties.` prefix.
   *
   * @return a map of driver property names, with the prefix removed, to their values
   */
  public Map<String, String> getTrinoJdbcExtraProperties() {
    return config.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(TRINO_JDBC_EXTRA_PROPERTIES_PREFIX.key))
        .filter(entry -> entry.getKey().length() > TRINO_JDBC_EXTRA_PROPERTIES_PREFIX.key.length())
        .collect(
            Collectors.toMap(
                entry -> entry.getKey().substring(TRINO_JDBC_EXTRA_PROPERTIES_PREFIX.key.length()),
                Map.Entry::getValue));
  }

  /**
   * Retrieves the catalog connector factory class name.
   *
   * @return the catalog connector factory class name
   */
  public String getCatalogConnectorFactoryClassName() {
    return config.getOrDefault(
        GRAVITINO_CATALOG_CONNECTOR_FACTORY_CLASS_NAME.key,
        GRAVITINO_CATALOG_CONNECTOR_FACTORY_CLASS_NAME.defaultValue);
  }

  /**
   * Converts the config to a catalog config.
   *
   * @return the catalog config
   */
  public String toCatalogConfig() {
    List<String> stringList = new ArrayList<>();
    for (Map.Entry<String, ConfigEntry> entry : CONFIG_DEFINITIONS.entrySet()) {
      // The `trino.jdbc.*` configurations are only used by the coordinator to connect back to
      // Trino. They must not be propagated to the dynamic catalogs, otherwise credentials such as
      // the JDBC password and the truststore password would end up in the generated CREATE CATALOG
      // statement, which is logged and persisted into the Trino catalog properties files.
      if (entry.getKey().startsWith(TRINO_JDBC_CONFIG_PREFIX)) {
        continue;
      }
      String value = config.get(entry.getKey());
      if (value != null
          && !GravitinoConnectorFactory.isSecuritySensitivePropertyName(entry.getKey())) {
        stringList.add(String.format("\"%s\"='%s'", entry.getKey(), value));
      }
    }
    // copy the configuration by the prefix of GRAVITINO_CLIENT_CONFIG_PREFIX and
    // GRAVITINO_ICEBERG_REST_CATALOG_CONFIG_PREFIX
    config.entrySet().stream()
        .filter(
            entry ->
                (entry.getKey().startsWith(GRAVITINO_CLIENT_CONFIG_PREFIX.key)
                        || entry
                            .getKey()
                            .startsWith(GRAVITINO_ICEBERG_REST_CATALOG_CONFIG_PREFIX.key))
                    && !GravitinoConnectorFactory.isSecuritySensitivePropertyName(entry.getKey()))
        .forEach(
            entry ->
                stringList.add(String.format("\"%s\"='%s'", entry.getKey(), entry.getValue())));
    config.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(GRAVITINO_DYNAMIC_CATALOG_ENV_PREFIX))
        .forEach(
            entry -> {
              String propertyName =
                  entry.getKey().substring(GRAVITINO_DYNAMIC_CATALOG_ENV_PREFIX.length());
              String environmentVariable = entry.getValue();
              if (propertyName.isEmpty()
                  || !ENVIRONMENT_VARIABLE_NAME.matcher(environmentVariable).matches()) {
                throw new TrinoException(
                    GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
                    String.format(
                        "Invalid dynamic catalog environment-variable mapping '%s'='%s'",
                        entry.getKey(), environmentVariable));
              }
              stringList.add(
                  String.format("\"%s\"='${ENV:%s}'", propertyName, environmentVariable));
            });
    return StringUtils.join(stringList, ',');
  }

  /**
   * Retrieves the metadata refresh interval in seconds.
   *
   * @return the metadata refresh interval in seconds
   */
  public String getMetadataRefreshIntervalSecond() {
    return config.getOrDefault(
        GRAVITINO_METADATA_REFRESH_INTERVAL_SECOND.key,
        GRAVITINO_METADATA_REFRESH_INTERVAL_SECOND.defaultValue);
  }

  /**
   * Whether skip Trino version validation or not.
   *
   * @return whether skip Trino version validation or not
   */
  public Boolean isSkipTrinoVersionValidation() {
    return Boolean.parseBoolean(
        config.getOrDefault(
            GRAVITINO_TRINO_SKIP_VERSION_VALIDATION.key,
            GRAVITINO_TRINO_SKIP_VERSION_VALIDATION.defaultValue));
  }

  /**
   * Init a comma-separated list of catalog name regex patterns that should be excluded from loading
   *
   * @return a list of catalog name regex patterns
   */
  private List<Pattern> initSkipCatalogPatterns() {
    String skipCatalogConfig =
        config.getOrDefault(
            GRAVITINO_TRINO_SKIP_CATALOG_PATTERNS.key,
            GRAVITINO_TRINO_SKIP_CATALOG_PATTERNS.defaultValue);
    return Splitter.on(',')
        .trimResults()
        .omitEmptyStrings()
        .splitToStream(skipCatalogConfig)
        .map(Pattern::compile)
        .collect(Collectors.toList());
  }

  /**
   * Returns whether Trino session user forwarding is enabled.
   *
   * @return true if forwardUser is set to true
   */
  public boolean isForwardUser() {
    return Boolean.parseBoolean(
        config.getOrDefault(GravitinoAuthProvider.FORWARD_SESSION_USER_KEY, "false"));
  }

  /**
   * Retrieves the maximum number of per-user sessions to keep in the cache.
   *
   * @return the session cache maximum size
   */
  public long getSessionCacheMaxSize() {
    return parseLongConfigEntry(GRAVITINO_SESSION_CACHE_MAX_SIZE);
  }

  /**
   * Retrieves the expiry (in seconds) for idle per-user sessions in the cache.
   *
   * @return the session cache expiry in seconds
   */
  public long getSessionCacheExpireAfterAccessSeconds() {
    return parseLongConfigEntry(GRAVITINO_SESSION_CACHE_EXPIRE_AFTER_ACCESS_SECONDS);
  }

  /**
   * Sets the Iceberg REST server endpoint discovered from the Gravitino server for the given
   * metalake. Called only by the catalog connector manager's periodic metalake poll, which runs on
   * the coordinator only — {@link #getDiscoveredIcebergRestUri} is therefore not by itself a valid
   * routing signal on a worker node. The coordinator is responsible for embedding the discovered
   * value into each catalog's own properties at registration time, so that it travels to every node
   * through the {@code CREATE CATALOG} statement Trino replicates cluster-wide; see {@code
   * CatalogRegister.generateCreateCatalogCommand}.
   *
   * @param metalake the metalake the endpoint was discovered for
   * @param uri the discovered endpoint, or {@code null} when the Iceberg REST server is not running
   *     or does not serve this metalake
   */
  public void setDiscoveredIcebergRestUri(String metalake, String uri) {
    if (StringUtils.isBlank(uri)) {
      discoveredIcebergRestUriByMetalake.remove(metalake);
    } else {
      discoveredIcebergRestUriByMetalake.put(metalake, uri);
    }
  }

  /**
   * Retrieves the Iceberg REST server endpoint discovered from the Gravitino server for the given
   * metalake, with no fallback to the manually configured endpoint. Only valid on the node that
   * runs the periodic discovery poll (the coordinator); see {@link #setDiscoveredIcebergRestUri}.
   *
   * @param metalake the metalake to resolve the endpoint for
   * @return the discovered endpoint, or an empty string when none is available
   */
  public String getDiscoveredIcebergRestUri(String metalake) {
    return discoveredIcebergRestUriByMetalake.getOrDefault(metalake, "");
  }

  /**
   * Returns whether non-REST Iceberg catalogs must be routed through the Gravitino Iceberg REST
   * server.
   *
   * @return {@code true} when Iceberg REST routing is enabled
   */
  public boolean isIcebergRestRoutingEnabled() {
    String value =
        config.getOrDefault(
            GRAVITINO_ICEBERG_REST_ROUTING_ENABLED.key,
            GRAVITINO_ICEBERG_REST_ROUTING_ENABLED.defaultValue);
    return parseBooleanConfig(GRAVITINO_ICEBERG_REST_ROUTING_ENABLED.key, value);
  }

  /**
   * Retrieves the manually configured Iceberg REST server endpoint for the given metalake, if any.
   * Unlike the discovered endpoint, this is plain local file configuration and is therefore
   * identical and valid on every node — coordinator and workers alike.
   *
   * <p>{@code gravitino.iceberg.rest-uri.<metalake>} is checked first. The unscoped {@code
   * gravitino.iceberg.rest-uri} is honored only in single-metalake mode, where it is unambiguous;
   * in multi-metalake mode it is ignored, since a single Iceberg REST server serves exactly one
   * metalake and applying it to every metalake would misroute the others.
   *
   * @param metalake the metalake to resolve the override for
   * @return the manually configured Iceberg REST server endpoint, or an empty string when unset
   */
  public String getManualIcebergRestUri(String metalake) {
    String scopedValue = config.get(GRAVITINO_ICEBERG_REST_URI.key + "." + metalake);
    if (StringUtils.isNotBlank(scopedValue)) {
      return scopedValue;
    }
    if (singleMetalakeMode()) {
      return config.getOrDefault(
          GRAVITINO_ICEBERG_REST_URI.key, GRAVITINO_ICEBERG_REST_URI.defaultValue);
    }
    return GRAVITINO_ICEBERG_REST_URI.defaultValue;
  }

  /**
   * Retrieves the properties passed through to the internal Trino Iceberg REST catalog, with the
   * {@code gravitino.iceberg.rest-catalog.} prefix rewritten to {@code iceberg.rest-catalog.}.
   *
   * @return the Trino Iceberg REST catalog properties
   */
  public Map<String, String> getIcebergRestCatalogConfig() {
    String prefix = GRAVITINO_ICEBERG_REST_CATALOG_CONFIG_PREFIX.key;
    Map<String, String> restCatalogConfig = new HashMap<>();

    if ("oauth2".equalsIgnoreCase(config.get(GravitinoAuthProvider.AUTH_TYPE_KEY))) {
      restCatalogConfig.put(TRINO_ICEBERG_REST_CATALOG_PREFIX + "security", "OAUTH2");
      putIfNotBlank(
          restCatalogConfig,
          TRINO_ICEBERG_REST_CATALOG_PREFIX + "oauth2.credential",
          config.get(GravitinoAuthProvider.OAUTH_CREDENTIAL_KEY));
      putIfNotBlank(
          restCatalogConfig,
          TRINO_ICEBERG_REST_CATALOG_PREFIX + "oauth2.scope",
          config.get(GravitinoAuthProvider.OAUTH_SCOPE_KEY));

      String serverUri = config.get(GravitinoAuthProvider.OAUTH_SERVER_URI_KEY);
      String path = config.get(GravitinoAuthProvider.OAUTH_PATH_KEY);
      if (StringUtils.isNotBlank(serverUri) && StringUtils.isNotBlank(path)) {
        restCatalogConfig.put(
            TRINO_ICEBERG_REST_CATALOG_PREFIX + "oauth2.server-uri",
            StringUtils.removeEnd(serverUri, "/") + "/" + StringUtils.removeStart(path, "/"));
      }
    }

    config.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(prefix))
        .forEach(
            entry ->
                restCatalogConfig.put(
                    TRINO_ICEBERG_REST_CATALOG_PREFIX + entry.getKey().substring(prefix.length()),
                    entry.getValue()));
    return restCatalogConfig;
  }

  private static void putIfNotBlank(Map<String, String> target, String key, String value) {
    if (StringUtils.isNotBlank(value)) {
      target.put(key, value);
    }
  }

  private long parseLongConfigEntry(ConfigEntry entry) {
    String value = config.getOrDefault(entry.key, entry.defaultValue);
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
          "Invalid value for config '" + entry.key + "': expected a number, got: " + value,
          e);
    }
  }

  /**
   * Retrieves a comma-separated list of catalog name regex patterns that should be excluded from
   * loading
   *
   * @return a list of catalog name regex patterns
   */
  public List<Pattern> getSkipCatalogPatterns() {
    return skipCatalogPatternList;
  }

  static class ConfigEntry {
    final String key;

    final String description;

    final String defaultValue;

    final boolean isRequired;

    /**
     * Constructs a new ConfigEntry.
     *
     * @param key The configuration key
     * @param description The description of the configuration parameter
     * @param defaultValue The default value of the configuration parameter
     * @param isRequired Whether this configuration parameter is required
     */
    ConfigEntry(String key, String description, String defaultValue, boolean isRequired) {
      this.key = key;
      this.description = description;
      this.defaultValue = defaultValue;
      this.isRequired = isRequired;

      CONFIG_DEFINITIONS.put(key, this);
    }
  }

  /** Class that handles Trino-specific configuration properties. */
  static class TrinoConfig {

    /** The properties loaded from Trino configuration file */
    private final Properties properties;

    /** Constructs a new TrinoConfig and loads properties from the configuration file. */
    public TrinoConfig() {
      this.properties = new Properties();
      try {
        String configFileName = System.getProperty("config");
        if (StringUtils.isEmpty(configFileName)) {
          return;
        }

        try (FileInputStream input = new FileInputStream(configFileName)) {
          properties.load(input);
        }

        if (properties.containsKey(TRINO_CATALOG_STORE)
            && !properties
                .getProperty(TRINO_CATALOG_STORE)
                .equals(TRINO_CATALOG_STORE_DEFAULT_VALUE)) {
          throw new TrinoException(
              GravitinoErrorCode.GRAVITINO_MISSING_CONFIG,
              "Gravitino connector works only at catalog.store = file mode");
        }

        if (!properties.containsKey(TRINO_CATALOG_MANAGEMENT)
            || TRINO_CATALOG_MANAGEMENT_DEFAULT_VALUE.equals(
                properties.getProperty(TRINO_CATALOG_MANAGEMENT))) {
          throw new TrinoException(
              GravitinoErrorCode.GRAVITINO_MISSING_CONFIG,
              "Gravitino connector works only at catalog.management = dynamic mode");
        }
      } catch (IOException e) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_MISSING_CONFIG,
            "Missing the Trino config file, please verify the jvm args '-Dconfig'");
      }
    }

    /**
     * Gets a property value for the specified key.
     *
     * @param key The property key
     * @return The property value
     */
    String getProperty(String key) {
      return properties.getProperty(key);
    }

    /**
     * Gets a property value for the specified key, or returns the default value if not found.
     *
     * @param key The property key
     * @param defaultValue The default value to return if the key is not found
     * @return The property value or default value
     */
    String getProperty(String key, String defaultValue) {

      return properties.getProperty(key, defaultValue);
    }

    /**
     * Checks if the properties contain the specified key.
     *
     * @param key The property key to check
     * @return true if the key exists, false otherwise
     */
    boolean contains(String key) {
      return properties.containsKey(key);
    }
  }
}
