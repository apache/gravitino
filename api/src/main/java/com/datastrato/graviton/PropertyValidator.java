/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton;

import java.util.Map;

public interface PropertyValidator {

  Map<String, PropertyMeta<?>> getPropertyMetas();

  enum Operation {
    CREATE,
    UPDATE,
    DELETE
  }

  default void validate(
      Map<String, String> config, Map<String, String> oldConfig, Operation operation) {
    Map<String, PropertyMeta<?>> configEntries = getPropertyMetas();

    switch (operation) {
      case CREATE:
        configEntries.forEach(
            (key, propertyMeta) -> {
              String value = config.get(key);
              propertyMeta.check(key, value, null, operation);
            });
        break;
      case UPDATE:
        configEntries.forEach(
            (key, propertyMeta) -> {
              String newValue = config.get(key);
              String oldValue = oldConfig.get(key);
              propertyMeta.check(key, newValue, oldValue, operation);
            });
        break;
      default:
        // Remove
        configEntries.forEach(
            (key, propertyMeta) -> {
              // null value means we would like to remove the key from the config.
              propertyMeta.check(key, null, null, operation);
            });
    }
  }
}
