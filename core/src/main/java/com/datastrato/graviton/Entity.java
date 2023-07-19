/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import java.io.Serializable;
import java.util.Map;

public interface Entity extends Serializable {

  /**
   * Validates the entity if the field arguments are valid.
   *
   * @throws IllegalArgumentException throws IllegalArgumentException if the validation is failed.
   */
  default void validate() throws IllegalArgumentException {
    fields().forEach(Field::validate);
  }

  /**
   * Returns the schema of the entity with values.
   *
   * @return Map of Field to Object to represent the schema of the entity with values.
   */
  Map<Field, Object> fields();
}
