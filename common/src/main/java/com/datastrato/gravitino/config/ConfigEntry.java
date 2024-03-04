/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.config;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Defines configuration properties. */
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
  private boolean hasNoDefault;
  private Consumer<T> validator;

  /**
   * Creates a new ConfigEntry instance.
   *
   * @param key The key of the configuration.
   * @param version The Gravitino version that introduces this configuration.
   * @param doc The documentation of the configuration.
   * @param alternatives Alternative keys for the configuration.
   * @param isPublic Whether the configuration is public.
   * @param isDeprecated Whether the configuration is deprecated.
   */
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

  /**
   * Sets a custom value converter function for this configuration.
   *
   * @param valueConverter The function that converts a configuration value string to the desired
   *     type.
   */
  void setValueConverter(Function<String, T> valueConverter) {
    this.valueConverter = valueConverter;
  }

  /**
   * Sets a custom string converter function for this configuration.
   *
   * @param stringConverter The function that converts a configuration value to its string
   *     representation.
   */
  void setStringConverter(Function<T, String> stringConverter) {
    this.stringConverter = stringConverter;
  }

  /**
   * Sets the default value for this configuration.
   *
   * @param t The default value to be used when no value is provided.
   */
  void setDefaultValue(T t) {
    this.defaultValue = t;
  }

  /**
   * Marks this configuration as optional. An optional entry can be absent in the configuration
   * properties without raising an exception.
   */
  void setOptional() {
    this.isOptional = true;
  }

  /** Marks this configuration as no default value. */
  void setHasNoDefault() {
    this.hasNoDefault = true;
  }

  /** Set the validator value. */
  void setValidator(Consumer<T> validator) {
    this.validator = validator;
  }

  /**
   * Checks if the user-provided value for the config matches the validator.
   *
   * @param checkValueFunc The validator of the configuration option
   * @param errorMsg The thrown error message if the value is invalid
   * @return The current ConfigEntry instance
   */
  public ConfigEntry<T> checkValue(Function<T, Boolean> checkValueFunc, String errorMsg) {
    setValidator(
        value -> {
          Preconditions.checkArgument(
              checkValueFunc.apply(value),
              String.format(
                  "%s in %s is invalid. %s", stringConverter.apply(value), key, errorMsg));
        });
    return this;
  }

  /**
   * Split the string to a list, then map each string element to its converted form.
   *
   * @param str The string form of the value list from the conf entry.
   * @param converter The orignal ConfigEntry valueConverter.
   * @return The list of converted type.
   */
  public List<T> strToSeq(String str, Function<String, T> converter) {
    List<String> strList = Arrays.asList(str.split(","));
    List<T> valList = strList.stream().map(converter).collect(Collectors.toList());

    return valList;
  }

  /**
   * Reduce the values then join them as a string.
   *
   * @param seq The sequence of the value list from the conf entry.
   * @param converter The orignal ConfigEntry stringConverter.
   * @return The converted string.
   */
  public String seqToStr(List<T> seq, Function<T, String> converter) {
    List<String> valList = seq.stream().map(converter).collect(Collectors.toList());
    String str = String.join(",", valList);
    return str;
  }

  /**
   * Converts the configuration value to value list.
   *
   * @return The ConfigEntry instance.
   */
  public ConfigEntry<List<T>> toSequence() {
    ConfigEntry<List<T>> conf =
        new ConfigEntry<>(key, version, doc, alternatives, isPublic, isDeprecated);
    conf.setValueConverter((String str) -> strToSeq(str, valueConverter));
    conf.setStringConverter((List<T> val) -> seqToStr(val, stringConverter));
    return conf;
  }

  /**
   * Creates a new ConfigEntry instance based on this configuration entry with a default value.
   *
   * @param t The default value to be used when no value is provided.
   * @return A new ConfigEntry instance with the specified default value.
   */
  public ConfigEntry<T> createWithDefault(T t) {
    ConfigEntry<T> conf =
        new ConfigEntry<>(key, version, doc, alternatives, isPublic, isDeprecated);
    conf.setValueConverter(valueConverter);
    conf.setStringConverter(stringConverter);
    conf.setDefaultValue(t);
    conf.setValidator(validator);

    return conf;
  }

  /**
   * Creates a new ConfigEntry instance based on this configuration entry with optional value
   * handling.
   *
   * @return A new ConfigEntry instance that works with optional values.
   */
  public ConfigEntry<Optional<T>> createWithOptional() {
    ConfigEntry<Optional<T>> conf =
        new ConfigEntry<>(key, version, doc, alternatives, isPublic, isDeprecated);
    conf.setValueConverter(s -> Optional.ofNullable(valueConverter.apply(s)));
    // Unless explicitly set by the user, null values are not expected to occur.
    conf.setStringConverter(t -> t.map(stringConverter).orElse(null));
    conf.setOptional();
    conf.setValidator(
        optionValue -> {
          if (Stream.of(Optional.ofNullable(validator), optionValue)
              .allMatch(Optional::isPresent)) {
            validator.accept(optionValue.get());
          }
        });

    return conf;
  }

  /**
   * Creates a new ConfigEntry instance based on this configuration entry with no default value.
   *
   * @return A new ConfigEntry instance with no default value.
   */
  public ConfigEntry<T> create() {
    ConfigEntry<T> conf =
        new ConfigEntry<>(key, version, doc, alternatives, isPublic, isDeprecated);
    conf.setValueConverter(valueConverter);
    conf.setStringConverter(stringConverter);
    conf.setHasNoDefault();
    conf.setValidator(validator);
    return conf;
  }

  /**
   * Reads the configuration value.
   *
   * @param properties The map containing the configuration properties.
   * @return The value of the configuration entry.
   * @throws NoSuchElementException If the configuration value is not found.
   */
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
      } else if (hasNoDefault) {
        if (validator != null) {
          validator.accept(null);
        }
        return null;
      } else if (!isOptional) {
        throw new NoSuchElementException("No configuration found for key " + key);
      }
    }

    T convertedValue = valueConverter.apply(value);
    if (validator != null) {
      validator.accept(convertedValue);
    }
    return convertedValue;
  }

  /**
   * Writes the provided value to the specified properties map.
   *
   * @param properties The map to write the configuration property to.
   * @param value The value of the configuration entry.
   */
  public void writeTo(Map<String, String> properties, T value) {
    String stringValue = stringConverter.apply(value);
    if (stringValue == null) {
      // To ensure that a null value is not set in the configuration
      LOG.warn("Config {} value to set is null, ignore setting to Config.", stringValue);
      return;
    }

    properties.put(key, stringValue);
  }
}
