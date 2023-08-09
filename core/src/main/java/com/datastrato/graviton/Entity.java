/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import java.io.Serializable;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

public interface Entity extends Serializable {
  @Getter
  enum EntityType {
    METALAKE("ml", 0),
    CATALOG("ca", 1),
    SCHEMA("sc", 2),
    TABLE("ta", 3),
    COLUMN("co", 4),

    AUDIT("au", 65534);

    // Short name can be used to identify the entity type in the logs, peristent storage, etc.
    private final String shortName;
    private final int index;

    EntityType(String shortName, int index) {
      this.shortName = shortName;
      this.index = index;
    }
  }

  @Getter
  @AllArgsConstructor
  @ToString
  class EntityIdentifier {
    private NameIdentifier nameIdentifier;
    private EntityType entityType;

    public static EntityIdentifier of(NameIdentifier name, EntityType entityType) {
      return new EntityIdentifier(name, entityType);
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
