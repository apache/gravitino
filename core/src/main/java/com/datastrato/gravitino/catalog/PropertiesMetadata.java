/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/** The PropertiesMetadata class is responsible for managing property metadata. */
public interface PropertiesMetadata {
  Map<String, PropertyEntry<?>> propertyEntries();

  default boolean isReservedProperty(String propertyName) {
    return propertyEntries().containsKey(propertyName)
        && propertyEntries().get(propertyName).isReserved();
  }

  default boolean isRequiredProperty(String propertyName) {
    return propertyEntries().containsKey(propertyName)
        && propertyEntries().get(propertyName).isRequired();
  }

  default boolean isImmutableProperty(String propertyName) {
    return propertyEntries().containsKey(propertyName)
        && propertyEntries().get(propertyName).isImmutable();
  }

  default boolean isHiddenProperty(String propertyName) {
    return propertyEntries().containsKey(propertyName)
        && propertyEntries().get(propertyName).isHidden();
  }

  default boolean containsProperty(String propertyName) {
    return propertyEntries().containsKey(propertyName);
  }

  static <T> T checkValueFormat(String key, String value, Function<String, T> decoder) {
    try {
      return decoder.apply(value);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Invalid value: '%s' for property: '%s'", value, key), e);
    }
  }

  default void validatePropertyForAlter(Map<String, String> upserts, Map<String, String> deletes) {
    for (Map.Entry<String, String> entry : upserts.entrySet()) {
      PropertyEntry<?> propertyEntry = propertyEntries().get(entry.getKey());
      if (Objects.nonNull(propertyEntry)) {
        Preconditions.checkArgument(
            !propertyEntry.isImmutable() && !propertyEntry.isReserved(),
            "Property " + propertyEntry.getName() + " is immutable or reserved, cannot be set");
        checkValueFormat(entry.getKey(), entry.getValue(), propertyEntry::decode);
      }
    }

    for (Map.Entry<String, String> entry : deletes.entrySet()) {
      PropertyEntry<?> propertyEntry = propertyEntries().get(entry.getKey());
      if (Objects.nonNull(propertyEntry)) {
        Preconditions.checkArgument(
            !propertyEntry.isImmutable() && !propertyEntry.isReserved(),
            "Property " + propertyEntry.getName() + " is immutable or reserved, cannot be deleted");
      }
    }
  }

  default void validatePropertyForCreate(Map<String, String> properties)
      throws IllegalArgumentException {
    if (properties == null) {
      return;
    }

    List<String> reservedProperties =
        properties.keySet().stream().filter(this::isReservedProperty).collect(Collectors.toList());
    Preconditions.checkArgument(
        reservedProperties.isEmpty(),
        "Properties are reserved and cannot be set: %s",
        reservedProperties);

    List<String> absentProperties =
        propertyEntries().keySet().stream()
            .filter(this::isRequiredProperty)
            .filter(k -> !properties.containsKey(k))
            .collect(Collectors.toList());
    Preconditions.checkArgument(
        absentProperties.isEmpty(),
        "Properties are required and must be set: %s",
        absentProperties);

    // use decode function to validate the property values
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (containsProperty(key)) {
        try {
          propertyEntries().get(key).decode(value);
        } catch (Exception e) {
          throw new IllegalArgumentException(
              String.format("Invalid value: '%s' for property: '%s'", value, key));
        }
      }
    }
  }

  default Object getOrDefault(Map<String, String> properties, String propertyName) {
    Preconditions.checkArgument(
        containsProperty(propertyName), "Property is not defined: " + propertyName);

    if (properties.containsKey(propertyName)) {
      return propertyEntries().get(propertyName).decode(properties.get(propertyName));
    }
    return propertyEntries().get(propertyName).getDefaultValue();
  }

  default Object getDefaultValue(String propertyName) {
    Preconditions.checkArgument(
        containsProperty(propertyName), "Property is not defined: " + propertyName);

    return propertyEntries().get(propertyName).getDefaultValue();
  }
}
