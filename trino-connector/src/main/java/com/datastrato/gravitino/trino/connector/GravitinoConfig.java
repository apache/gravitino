/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_MISSING_CONFIG;

import io.trino.spi.TrinoException;
import java.util.HashMap;
import java.util.Map;

public class GravitinoConfig {

  public static String GRAVITINO_DYNAMIC_CONNECTOR = "__gravitino.dynamic.connector";
  private static final Map<String, ConfigEntry> CONFIG_DEFINITIONS = new HashMap<>();

  private final Map<String, String> config;

  private static final ConfigEntry GRAVITINO_URI =
      new ConfigEntry(
          "gravitino.uri", "The uri of the gravitino web server", "http://localhost:8090", false);

  private static final ConfigEntry GRAVITINO_METALAKE =
      new ConfigEntry("gravitino.metalake", "The metalake name for used", "", true);

  public GravitinoConfig(Map<String, String> requiredConfig) {
    config = requiredConfig;

    if (!isDynamicConnector()) {
      for (Map.Entry<String, ConfigEntry> entry : CONFIG_DEFINITIONS.entrySet()) {
        ConfigEntry configDefinition = entry.getValue();
        if (configDefinition.isRequired && !config.containsKey(configDefinition.key)) {
          String message =
              String.format("Missing gravitino config, %s is required", configDefinition.key);
          throw new TrinoException(GRAVITINO_MISSING_CONFIG, message);
        }
      }
    }
  }

  public String getURI() {
    return config.getOrDefault(GRAVITINO_URI.key, GRAVITINO_URI.defaultValue);
  }

  public String getMetalake() {
    return config.getOrDefault(GRAVITINO_METALAKE.key, GRAVITINO_METALAKE.defaultValue);
  }

  boolean isDynamicConnector() {
    // 'isDynamicConnector' indicates whether the connector is user-configured within Trino or
    // loaded from the Gravitino server.
    // When a connector is loaded via Trino configuration,
    // it is static and will always create an instance of DummyGravitinoConnector.
    // Otherwise, it is dynamically loaded from the Gravitino server,
    // in which case the connector's configuration is set to '__gravitino.dynamic.connector=true'.
    // It is dynamic and will create an instance of GravitinoConnector.
    return config.getOrDefault(GRAVITINO_DYNAMIC_CONNECTOR, "false").equals("true");
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
