/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.config.ConfigBuilder;
import com.datastrato.graviton.config.ConfigEntry;
import com.google.common.base.Preconditions;
import java.util.Map;

public class GravitonConfig extends Config {

  private final Map<String, String> config;

  public static final ConfigEntry<String> GRAVITON_URI =
      new ConfigBuilder("graviton.uri")
          .doc("The uri of the graviton web server")
          .version("0.1.0")
          .stringConf()
          .createWithDefault("http://localhost:8090");

  public static final ConfigEntry<String> GRAVI_METALAKE =
      new ConfigBuilder("graviton.metalake")
          .doc("The metalake name for used")
          .version("0.1.0")
          .stringConf()
          .createWithDefault("");

  public GravitonConfig(Map<String, String> requiredConfig) {
    config = Preconditions.checkNotNull(requiredConfig, "catalogInjector is not null");
  }

  public String getURI() {
    return config.getOrDefault(GRAVITON_URI.getKey(), GRAVITON_URI.getDefaultValue());
  }

  public String getMetalake() {
    return config.getOrDefault(GRAVI_METALAKE.getKey(), GRAVI_METALAKE.getDefaultValue());
  }
}
