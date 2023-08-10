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

/** This interface defines an entity within the Graviton framework. */
public interface Entity extends Serializable {

  /** Enumeration defining the types of entities in the Graviton framework. */
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

  /** Class representing an identifier for an entity. */
  @Getter
  @AllArgsConstructor
  @ToString
  class EntityIdentifier {
    private NameIdentifier nameIdentifier;
    private EntityType entityType;

    /**
     * Create an instance of EntityIdentifier.
     *
     * @param name The name identifier of the entity.
     * @param entityType The type of the entity.
     * @return The created EntityIdentifier instance.
     */
    public static EntityIdentifier of(NameIdentifier name, EntityType entityType) {
      return new EntityIdentifier(name, entityType);
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
