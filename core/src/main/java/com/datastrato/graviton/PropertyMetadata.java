/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import java.util.Map;

public interface PropertyMetadata {
  Map<String, PropertyEntry<?>> property();

  static boolean isReservedProperty(
      String propertyName, Map<String, PropertyEntry<?>> propertySpecs) {
    return propertySpecs.containsKey(propertyName) && propertySpecs.get(propertyName).isReserved();
  }

  static boolean isRequiredProperty(
      String propertyName, Map<String, PropertyEntry<?>> propertySpecs) {
    return propertySpecs.containsKey(propertyName) && propertySpecs.get(propertyName).isRequired();
  }

  static boolean isImmutableProperty(
      String propertyName, Map<String, PropertyEntry<?>> propertySpecs) {
    return propertySpecs.containsKey(propertyName) && propertySpecs.get(propertyName).isImmutable();
  }

  static boolean isHiddenProperty(
      String propertyName, Map<String, PropertyEntry<?>> propertySpecs) {
    return propertySpecs.containsKey(propertyName) && propertySpecs.get(propertyName).isHidden();
  }
}
