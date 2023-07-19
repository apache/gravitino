/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.config;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigEntry<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigEntry.class);

  @Getter private String key;

  @Getter private List<String> alternatives;

  @Getter private T defaultValue;

  private Function<String, T> valueConverter;

  private Function<T, String> stringConverter;

  @Getter private String doc;

  @Getter private String version;

  @Getter private boolean isPublic;

  @Getter private boolean isDeprecated;

  private boolean isOptional;

  ConfigEntry(
      String key,
      String version,
      String doc,
      List<String> alternatives,
      boolean isPublic,
      boolean isDeprecated) {
    this.key = key;
    this.version = version;
    this.doc = doc;
    this.alternatives = alternatives;
    this.isPublic = isPublic;
    this.isDeprecated = isDeprecated;
    this.isOptional = false;
  }

  void setValueConverter(Function<String, T> valueConverter) {
    this.valueConverter = valueConverter;
  }

  void setStringConverter(Function<T, String> stringConverter) {
    this.stringConverter = stringConverter;
  }

  void setDefaultValue(T t) {
    this.defaultValue = t;
  }

  void setOptional() {
    this.isOptional = true;
  }

  public ConfigEntry<T> createWithDefault(T t) {
    ConfigEntry<T> conf =
        new ConfigEntry<>(key, version, doc, alternatives, isPublic, isDeprecated);
    conf.setValueConverter(valueConverter);
    conf.setStringConverter(stringConverter);
    conf.setDefaultValue(t);

    return conf;
  }

  public ConfigEntry<Optional<T>> createWithOptional() {
    ConfigEntry<Optional<T>> conf =
        new ConfigEntry<>(key, version, doc, alternatives, isPublic, isDeprecated);
    conf.setValueConverter(s -> Optional.ofNullable(valueConverter.apply(s)));
    // null value should not be possible unless the user explicitly sets it
    conf.setStringConverter(t -> t.map(stringConverter).orElse(null));
    conf.setOptional();

    return conf;
  }

  public T readFrom(Map<String, String> properties) throws NoSuchElementException {
    String value = properties.get(key);
    if (value == null) {
      for (String alternative : alternatives) {
        value = properties.get(alternative);
        if (value != null) {
          break;
        }
      }
    }

    if (value == null) {
      if (defaultValue != null) {
        return defaultValue;
      } else if (!isOptional) {
        throw new NoSuchElementException("No configuration found for key " + key);
      }
    }

    return valueConverter.apply(value);
  }

  public void writeTo(Map<String, String> properties, T value) {
    String stringValue = stringConverter.apply(value);
    if (stringValue == null) {
      // We don't want user to set a null value to config, so this basically will not happen;
      LOG.warn("Config {} value to set is null, ignore setting to Config.", stringValue);
      return;
    }

    properties.put(key, stringValue);
  }
}
