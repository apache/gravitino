/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog;

import com.datastrato.graviton.CatalogChange;
import com.datastrato.graviton.CatalogChange.RemoveProperty;
import com.datastrato.graviton.CatalogChange.SetProperty;
import com.datastrato.graviton.rel.TableChange;
import com.google.common.base.Preconditions;
import java.util.Arrays;
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

  default void validateAlter(Object[] changes) {
    if (changes == null) {
      return;
    }
    // Maybe we should check all elements in changes are of the same type
    Object o = changes[0];
    if (o instanceof TableChange) {
      validateTableChange(
          Arrays.stream(changes).map(TableChange.class::cast).toArray(TableChange[]::new));
    } else if (o instanceof CatalogChange) {
      validateCatalogChange(
          Arrays.stream(changes).map(CatalogChange.class::cast).toArray(CatalogChange[]::new));
    }
    // TODO(yuqi): add more validation for schema and metalake
  }

  default void validateTableChange(TableChange[] tableChanges) {
    for (TableChange change : tableChanges) {
      if (change instanceof TableChange.SetProperty) {
        String propertyName = ((TableChange.SetProperty) change).getProperty();
        if (isReservedProperty(propertyName) || isImmutableProperty(propertyName)) {
          throw new IllegalArgumentException(
              String.format(
                  "Property %s is reserved or immutable and cannot be set", propertyName));
        }
      }

      if (change instanceof TableChange.RemoveProperty) {
        String propertyName = ((TableChange.RemoveProperty) change).getProperty();
        if (isReservedProperty(propertyName) || isImmutableProperty(propertyName)) {
          throw new IllegalArgumentException(
              String.format("Property %s cannot be removed by user", propertyName));
        }
      }
    }
  }

  default void validateCatalogChange(CatalogChange[] catalogChanges) {
    // Check set property
    Arrays.stream(catalogChanges)
        .filter(SetProperty.class::isInstance)
        .forEach(
            tableChange -> {
              SetProperty setProperty = (SetProperty) tableChange;
              PropertyEntry<?> entry = propertyEntries().get(setProperty.getProperty());
              if (Objects.nonNull(entry)) {
                Preconditions.checkArgument(
                    !entry.isImmutable(), "Property " + entry.getName() + " is immutable");
                checkValueFormat(setProperty.getProperty(), setProperty.getValue(), entry::decode);
              }
            });

    // Check remove property
    Arrays.stream(catalogChanges)
        .filter(RemoveProperty.class::isInstance)
        .forEach(
            tableChange -> {
              RemoveProperty removeProperty = (RemoveProperty) tableChange;
              PropertyEntry<?> entry = propertyEntries().get(removeProperty.getProperty());
              if (Objects.nonNull(entry) && entry.isImmutable()) {
                throw new IllegalArgumentException("Property " + entry.getName() + " is immutable");
              }
            });
  }

  static <T> void checkValueFormat(String key, String value, Function<String, T> decoder) {
    try {
      decoder.apply(value);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Invalid value: '%s' for property: '%s'", value, key), e);
    }
  }

  default void validateCreate(Map<String, String> properties) throws IllegalArgumentException {
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
}
