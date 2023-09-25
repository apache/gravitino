/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton;

import com.datastrato.graviton.PropertyValidator.Operation;
import java.util.function.Function;
import javax.annotation.Nullable;

public class PropertyMeta<T> {
  protected String key;

  protected T defaultValue;

  protected Class<?> type;

  protected boolean required;

  protected boolean immutable;

  protected String description;

  // Other fields and methods will be added later according to the doc.
  protected Function<String, T> encoder;

  /**
   * Check the value of the config key.
   *
   * @param key key of the config
   * @param newValue new value of the config
   * @param oldValue old value of the config if it's an update operation, null otherwise
   * @param operation operation type, CREATE, UPDATE or DELETE
   * @return
   */
  public boolean check(
      String key, String newValue, @Nullable String oldValue, Operation operation) {
    // value == null means that the key is not present in the config.
    if (newValue == null) {
      if (required) {
        throw new IllegalArgumentException(String.format("Missing required config key %s", key));
      }
      // This is an optional key, so we can skip the rest of the tests.
      return true;
    }

    try {
      encoder.apply(newValue);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Invalid value for config key %s: %s", key, newValue), e);
    }

    return true;
  }

  public String getKey() {
    return key;
  }
}
