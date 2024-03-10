/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import java.io.Serializable;
import java.util.Map;
import lombok.Getter;

/** This interface defines an entity within the Gravitino framework. */
public interface Entity extends Serializable {

  /** Enumeration defining the types of entities in the Gravitino framework. */
  @Getter
  enum EntityType {
    METALAKE("ml", 0),
    CATALOG("ca", 1),
    SCHEMA("sc", 2),
    TABLE("ta", 3),
    COLUMN("co", 4),
    FILESET("fi", 5),
    TOPIC("to", 6),

    AUDIT("au", 65534);

    // Short name can be used to identify the entity type in the logs, persistent storage, etc.
    private final String shortName;
    private final int index;

    EntityType(String shortName, int index) {
      this.shortName = shortName;
      this.index = index;
    }

    public static EntityType fromShortName(String shortName) {
      for (EntityType entityType : EntityType.values()) {
        if (entityType.shortName.equals(shortName)) {
          return entityType;
        }
      }
      throw new IllegalArgumentException("Unknown entity type: " + shortName);
    }
  }

  /**
   * Validates the entity by ensuring the validity of its field arguments.
   *
   * @throws IllegalArgumentException If the validation fails.
   */
  default void validate() throws IllegalArgumentException {
    fields().forEach(Field::validate);
  }

  /**
   * Retrieves the fields and their associated values of the entity.
   *
   * @return A map of Field to Object representing the entity's schema with values.
   */
  Map<Field, Object> fields();

  /**
   * Retrieves the type of the entity.
   *
   * @return The type of the entity as defined by {@link EntityType}.
   */
  EntityType type();
}
