/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog;

import java.util.Map;

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
}
