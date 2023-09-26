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

  private static final Map<String, ConfigEntry> configDefintions = new HashMap<>();

  private final Map<String, String> config;

  public static final ConfigEntry GRAVITON_URI =
      new ConfigEntry(
          "graviton.uri", "The uri of the graviton web server", "http://localhost:8090", false);

  public static final ConfigEntry GRAVI_METALAKE =
      new ConfigEntry("graviton.metalake", "The metalake name for used", "", true);

  public GravitonConfig(Map<String, String> requiredConfig) {
    config = Preconditions.checkNotNull(requiredConfig, "catalogInjector is not null");

    for (Map.Entry<String, ConfigEntry> entry : configDefintions.entrySet()) {
      ConfigEntry configDefinish = entry.getValue();
      if (configDefinish.isRequired) {
        if (!config.containsKey(configDefinish.key)) {
          String message =
              String.format("Missing graviton config, %s is required", configDefinish.key);
          throw new TrinoException(GRAVITON_MISSING_CONFIG, message);
        }
      }
    }
  }

  public String getURI() {
    return config.getOrDefault(GRAVITON_URI.key, GRAVITON_URI.defaultValue);
  }

  public String getMetalake() {
    return config.getOrDefault(GRAVI_METALAKE.key, GRAVI_METALAKE.defaultValue);
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

      configDefintions.put(key, this);
    }
  }
}
