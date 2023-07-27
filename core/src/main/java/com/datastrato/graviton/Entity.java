/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import java.io.Serializable;
import java.util.Map;

public interface Entity extends Serializable {
  enum EntityType {
    METALAKE("ml", 0),
    CATALOG("ca", 1),
    SCHEMA("sc", 2),
    TABLE("tb", 3),
    COLUMN("co", 4),

    AUDIT("au", 9999999);

    // Short name can be used to identify the entity type in the logs, peristent storage, etc.
    private final String shortName;
    private final int index;

    EntityType(String shortName, int index) {
      this.shortName = shortName;
      this.index = index;
    }
  }

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
  /** Return type of the entity, plase see {@link EntityType} to see all types */
  EntityType type();
}
