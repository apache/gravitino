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

  private static final Map<String, ConfigEntry> CONFIG_DEFINITIONS = new HashMap<>();

  private final Map<String, String> config;

  private static final ConfigEntry GRAVITINO_URI =
      new ConfigEntry(
          "gravitino.uri", "The uri of the gravitino web server", "http://localhost:8090", false);

  private static final ConfigEntry GRAVITINO_METALAKE =
      new ConfigEntry("gravitino.metalake", "The metalake name for used", "", true);

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
  }

  public String getURI() {
    return config.getOrDefault(GRAVITINO_URI.key, GRAVITINO_URI.defaultValue);
  }

  public String getMetalake() {
    return config.getOrDefault(GRAVITINO_METALAKE.key, GRAVITINO_METALAKE.defaultValue);
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
