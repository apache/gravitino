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

import io.trino.spi.TrinoException;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

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

  // Trino config default value
  /** The Trino catalog config directory default value. */
  private static final String TRINO_CATALOG_CONFIG_DIR_DEFAULT_VALUE = "etc/catalog";
  /** The Trino catalog store default value. */
  public static final String TRINO_CATALOG_STORE_DEFAULT_VALUE = "file";
  /** The Trino catalog management default value. */
  public static final String TRINO_CATALOG_MANAGEMENT_DEFAULT_VALUE = "static";

  // The Trino configuration of etc/config.properties
  /** The Trino configuration. */
  public static final TrinoConfig trinoConfig = new TrinoConfig();

  // Gravitino config keys
  /** The Gravitino dynamic connector. */
  public static final String GRAVITINO_DYNAMIC_CONNECTOR = "__gravitino.dynamic.connector";
  /** The Gravitino dynamic connector catalog config. */
  public static final String GRAVITINO_DYNAMIC_CONNECTOR_CATALOG_CONFIG =
      "__gravitino.dynamic.connector.catalog.config";

  private static final Map<String, ConfigEntry> CONFIG_DEFINITIONS = new HashMap<>();
  private final Map<String, String> config;

  // Gravitino config entity
  private static final ConfigEntry GRAVITINO_URI =
      new ConfigEntry(
          "gravitino.uri", "The uri of the gravitino web server", "http://localhost:8090", false);

  private static final ConfigEntry GRAVITINO_METALAKE =
      new ConfigEntry("gravitino.metalake", "The metalake name for used", "", true);

  /** @deprecated Please use {@code gravitino.use-single-metalake} instead. */
  @Deprecated
  @SuppressWarnings("UnusedVariable")
  private static final ConfigEntry GRAVITINO_SIMPLIFY_CATALOG_NAMES =
      new ConfigEntry(
          "gravitino.simplify-catalog-names",
          "Omit metalake prefix for catalog names, is deprecated, use gravitino.use-single-metalake instead",
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
          "The property to specify the region code of the cloud that the catalog is running on.",
          "",
          false);

  private static final ConfigEntry GRAVITINO_CATALOG_CONNECTOR_FACTORY_CLASS_NAME =
      new ConfigEntry(
          "gravitino.catalog.connector.factory.class.name",
          "The class name for the custom CatalogConnectorFactory. The class must implement the CatalogConnectorFactory interface",
          "",
          false);

  private static final ConfigEntry TRINO_JDBC_USER =
      new ConfigEntry("trino.jdbc.user", "The jdbc user name of Trino", "admin", false);

  private static final ConfigEntry TRINO_JDBC_PASSWORD =
      new ConfigEntry("trino.jdbc.password", "The jdbc user password of Trino", "", false);

  private static final ConfigEntry GRAVITINO_METADATA_REFRESH_INTERVAL_SECOND =
      new ConfigEntry(
          "gravitino.metadata.refresh-interval-seconds",
          "The interval in seconds to refresh the metadata from Gravitino server",
          "10",
          false);

  private static final ConfigEntry GRAVITINO_TRINO_SKIP_VERSION_VALIDATION =
      new ConfigEntry(
          "gravitino.trino.skip-version-validation",
          "The property to specify whether skip Trino version validation or not. Note there may be compatiablity problem if true.",
          "false",
          false);

  private static final ConfigEntry GRAVITINO_CLIENT_CONFIG_PREFIX =
      new ConfigEntry("gravitino.client.", "The config prefix for Grivitino client", "", false);

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
    return config.getOrDefault(GRAVITINO_METALAKE.key, GRAVITINO_METALAKE.defaultValue);
  }

  /**
   * Retrieves the config for Grivitino client.
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
    String uriString = "";
    if (config.containsKey(TRINO_DISCOVERY_URI)) {
      uriString = config.get(TRINO_DISCOVERY_URI);
    } else {
      uriString = trinoConfig.getProperty(TRINO_DISCOVERY_URI);
    }
    try {
      URI trinoURI = new URI(uriString);
      return String.format("jdbc:trino://%s:%s", trinoURI.getHost(), trinoURI.getPort());
    } catch (Exception e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_MISSING_CONFIG,
          "The Trino configuration of `discovery.uri` = " + uriString + " is not correct");
    }
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
      String value = config.get(entry.getKey());
      if (value != null) {
        stringList.add(String.format("\"%s\"='%s'", entry.getKey(), value));
      }
    }
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
