/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_MISSING_CONFIG;

import io.trino.spi.TrinoException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class GravitinoConfig {

  public static final String GRAVITINO_DYNAMIC_CONNECTOR = "__gravitino.dynamic.connector";
  public static final String GRAVITINO_DYNAMIC_CONNECTOR_CATALOG_CONFIG =
      "__gravitino.dynamic.connector.catalog.config";
  private static final Map<String, ConfigEntry> CONFIG_DEFINITIONS = new HashMap<>();

  private final Map<String, String> config;

  private static final ConfigEntry GRAVITINO_URI =
      new ConfigEntry(
          "gravitino.uri", "The uri of the gravitino web server", "http://localhost:8090", false);

  private static final ConfigEntry GRAVITINO_METALAKE =
      new ConfigEntry("gravitino.metalake", "The metalake name for used", "", true);

  private static final ConfigEntry GRAVITINO_SIMPLIFY_CATALOG_NAMES =
      new ConfigEntry(
          "gravitino.simplify-catalog-names",
          "Omit metalake prefix for catalog names",
          "true",
          false);

  private static final ConfigEntry TRINO_JDBC_URI =
      new ConfigEntry(
          "trino.jdbc.uri", "The jdbc uri of Trino server", "jdbc:trino://localhost:8080", false);

  private static final ConfigEntry TRINO_CATALOG_STORE =
      new ConfigEntry(
          "trino.catalog.store",
          "The directory stored the catalog configuration of Trino",
          "etc/catalog",
          false);

  private static final ConfigEntry TRINO_JDBC_USER =
      new ConfigEntry("trino.jdbc.user", "The jdbc user name of Trino", "admin", false);

  private static final ConfigEntry TRINO_JDBC_PASSWORD =
      new ConfigEntry("trino.jdbc.password", "The jdbc user password of Trino", "", false);

  public GravitinoConfig(Map<String, String> requiredConfig) {
    config = requiredConfig;
    for (Map.Entry<String, ConfigEntry> entry : CONFIG_DEFINITIONS.entrySet()) {
      ConfigEntry configDefinition = entry.getValue();
      if (configDefinition.isRequired && !config.containsKey(configDefinition.key)) {
        String message =
            String.format("Missing gravitino config, %s is required", configDefinition.key);
        throw new TrinoException(GRAVITINO_MISSING_CONFIG, message);
      }
    }
    if (isDynamicConnector() && !config.containsKey(GRAVITINO_DYNAMIC_CONNECTOR_CATALOG_CONFIG)) {
      throw new TrinoException(
          GRAVITINO_MISSING_CONFIG, "Incomplete Dynamic catalog connector config");
    }
  }

  public String getURI() {
    return config.getOrDefault(GRAVITINO_URI.key, GRAVITINO_URI.defaultValue);
  }

  public String getMetalake() {
    return config.getOrDefault(GRAVITINO_METALAKE.key, GRAVITINO_METALAKE.defaultValue);
  }

  public boolean simplifyCatalogNames() {
    return Boolean.parseBoolean(
        config.getOrDefault(
            GRAVITINO_SIMPLIFY_CATALOG_NAMES.key, GRAVITINO_SIMPLIFY_CATALOG_NAMES.defaultValue));
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

  public String getCatalogConfig() {
    return config.get(GRAVITINO_DYNAMIC_CONNECTOR_CATALOG_CONFIG);
  }

  public String getTrinoURI() {
    return config.getOrDefault(TRINO_JDBC_URI.key, TRINO_JDBC_URI.defaultValue);
  }

  public String getCatalogStoreDirectory() {
    return config.getOrDefault(TRINO_CATALOG_STORE.key, TRINO_CATALOG_STORE.defaultValue);
  }

  public String getTrinoUser() {
    return config.getOrDefault(TRINO_JDBC_USER.key, TRINO_JDBC_USER.defaultValue);
  }

  public String getTrinoPassword() {
    return config.getOrDefault(TRINO_JDBC_PASSWORD.key, TRINO_JDBC_PASSWORD.defaultValue);
  }

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

  static class ConfigEntry {
    final String key;
    final String description;
    final String defaultValue;
    final boolean isRequired;

    ConfigEntry(String key, String description, String defaultValue, boolean isRequired) {
      this.key = key;
      this.description = description;
      this.defaultValue = defaultValue;
      this.isRequired = isRequired;

      CONFIG_DEFINITIONS.put(key, this);
    }
  }
}
