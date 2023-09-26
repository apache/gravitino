/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector;

import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_MISSING_CONFIG;

import com.google.common.base.Preconditions;
import io.trino.spi.TrinoException;
import java.util.HashMap;
import java.util.Map;

public class GravitonConfig {

  private static final Map<String, ConfigEntry> CONFIG_DEFINITIONS = new HashMap<>();

  private final Map<String, String> config;

  private static final ConfigEntry GRAVITON_URI =
      new ConfigEntry(
          "graviton.uri", "The uri of the graviton web server", "http://localhost:8090", false);

  private static final ConfigEntry GRAVITON_METALAKE =
      new ConfigEntry("graviton.metalake", "The metalake name for used", "", true);

  public GravitonConfig(Map<String, String> requiredConfig) {
    Preconditions.checkArgument(requiredConfig != null, "requiredConfig is not null");
    config = requiredConfig;

    for (Map.Entry<String, ConfigEntry> entry : CONFIG_DEFINITIONS.entrySet()) {
      ConfigEntry configDefinition = entry.getValue();
      if (configDefinition.isRequired && !config.containsKey(configDefinition.key)) {
        String message =
            String.format("Missing graviton config, %s is required", configDefinition.key);
        throw new TrinoException(GRAVITON_MISSING_CONFIG, message);
      }
    }
  }

  public String getURI() {
    return config.getOrDefault(GRAVITON_URI.key, GRAVITON_URI.defaultValue);
  }

  public String getMetalake() {
    return config.getOrDefault(GRAVITON_METALAKE.key, GRAVITON_METALAKE.defaultValue);
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
