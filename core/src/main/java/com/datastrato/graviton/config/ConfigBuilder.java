/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.config;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class ConfigBuilder {

  private String key;

  private List<String> alternatives;

  private String doc;

  private String version;

  private boolean isPublic;

  private boolean isDeprecated;

  public ConfigBuilder(String key) {
    this.key = key;

    this.alternatives = Collections.emptyList();
    this.doc = "";
    this.version = "0.1.0";
    this.isPublic = true;
    this.isDeprecated = false;
  }

  public ConfigBuilder alternatives(List<String> alternatives) {
    this.alternatives = alternatives;
    return this;
  }

  public ConfigBuilder doc(String doc) {
    this.doc = doc;
    return this;
  }

  public ConfigBuilder version(String version) {
    this.version = version;
    return this;
  }

  public ConfigBuilder internal() {
    this.isPublic = false;
    return this;
  }

  public ConfigBuilder deprecated() {
    this.isDeprecated = true;
    return this;
  }

  public ConfigEntry<String> stringConf() {
    ConfigEntry<String> conf =
        new ConfigEntry<>(key, version, doc, alternatives, isPublic, isDeprecated);
    conf.setValueConverter(s -> s);
    conf.setStringConverter(s -> s);

    return conf;
  }

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
