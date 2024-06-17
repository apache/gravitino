/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.config;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/** Builder class for creating configuration entries. */
public class ConfigBuilder {

  private String key;

  private List<String> alternatives;

  private String doc;

  private String version;

  private boolean isPublic;

  private boolean isDeprecated;

  /**
   * Constructs a ConfigBuilder with the given key.
   *
   * @param key The key for the configuration.
   */
  public ConfigBuilder(String key) {
    this.key = key;

    this.alternatives = Collections.emptyList();
    this.doc = "";
    this.version = "0.1.0";
    this.isPublic = true;
    this.isDeprecated = false;
  }

  /**
   * Sets the alternatives for the configuration.
   *
   * @param alternatives The list of alternative keys.
   * @return The current ConfigBuilder instance.
   */
  public ConfigBuilder alternatives(List<String> alternatives) {
    this.alternatives = alternatives;
    return this;
  }

  /**
   * Sets the documentation for the configuration.
   *
   * @param doc The documentation string.
   * @return The current ConfigBuilder instance.
   */
  public ConfigBuilder doc(String doc) {
    this.doc = doc;
    return this;
  }

  /**
   * Sets the version for the configuration.
   *
   * @param version The version string.
   * @return The current ConfigBuilder instance.
   */
  public ConfigBuilder version(String version) {
    this.version = version;
    return this;
  }

  /**
   * Marks the configuration entry as internal (non-public).
   *
   * @return The current ConfigBuilder instance.
   */
  public ConfigBuilder internal() {
    this.isPublic = false;
    return this;
  }

  /**
   * Marks the configuration entry as deprecated.
   *
   * @return The current ConfigBuilder instance.
   */
  public ConfigBuilder deprecated() {
    this.isDeprecated = true;
    return this;
  }

  /**
   * Creates a configuration entry for String data type.
   *
   * @return The created ConfigEntry instance for String data type.
   */
  public ConfigEntry<String> stringConf() {
    ConfigEntry<String> conf =
        new ConfigEntry<>(key, version, doc, alternatives, isPublic, isDeprecated);
    conf.setValueConverter(s -> s);
    conf.setStringConverter(s -> s);

    return conf;
  }

  /**
   * Creates a configuration entry for Integer data type.
   *
   * @return The created ConfigEntry instance for Integer data type.
   */
  public ConfigEntry<Integer> intConf() {
    ConfigEntry<Integer> conf =
        new ConfigEntry<>(key, version, doc, alternatives, isPublic, isDeprecated);
    Function<String, Integer> func =
        s -> {
          if (s == null || s.isEmpty()) {
            return null;
          } else {
            return Integer.parseInt(s);
          }
        };
    conf.setValueConverter(func);

    Function<Integer, String> stringFunc =
        t -> Optional.ofNullable(t).map(String::valueOf).orElse(null);
    conf.setStringConverter(stringFunc);

    return conf;
  }

  /**
   * Creates a configuration entry for Long data type.
   *
   * @return The created ConfigEntry instance for Long data type.
   */
  public ConfigEntry<Long> longConf() {
    ConfigEntry<Long> conf =
        new ConfigEntry<>(key, version, doc, alternatives, isPublic, isDeprecated);
    Function<String, Long> func =
        s -> {
          if (s == null || s.isEmpty()) {
            return null;
          } else {
            return Long.parseLong(s);
          }
        };
    conf.setValueConverter(func);

    Function<Long, String> stringFunc =
        t -> Optional.ofNullable(t).map(String::valueOf).orElse(null);
    conf.setStringConverter(stringFunc);

    return conf;
  }

  /**
   * Creates a configuration entry for Boolean data type.
   *
   * @return The created ConfigEntry instance for Boolean data type.
   */
  public ConfigEntry<Boolean> booleanConf() {
    ConfigEntry<Boolean> conf =
        new ConfigEntry<>(key, version, doc, alternatives, isPublic, isDeprecated);
    Function<String, Boolean> func =
        s -> {
          if (s == null || s.isEmpty()) {
            return null;
          } else {
            return Boolean.parseBoolean(s);
          }
        };
    conf.setValueConverter(func);

    Function<Boolean, String> stringFunc =
        t -> Optional.ofNullable(t).map(String::valueOf).orElse(null);
    conf.setStringConverter(stringFunc);

    return conf;
  }
}
