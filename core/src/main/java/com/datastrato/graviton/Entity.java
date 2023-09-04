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
    METALAKE("ml", 0, 0),
    CATALOG("ca", 1, 1),
    SCHEMA("sc", 2, 2),
    TABLE("ta", 3, 3),
    COLUMN("co", 4, 4),

    AUDIT("au", 65534, -1);

    // Short name can be used to identify the entity type in the logs, peristent storage, etc.
    private final String shortName;
    private final int index;

    // Level represents the layer of a entites, as we orignize entities as the format
    // 'metalake/catalog/scheam/table', the level of a 'metalake' is 0, 'catalog' is 1, 'schema' is
    // 2 and 'table' is 3
    // TODO (yuqi), we should handle the case that an entity that has multiple kinds of
    //  sub-entities. e.g., catlog may has schema and directory as sub-entities.
    private final int level;

    EntityType(String shortName, int index, int level) {
      this.shortName = shortName;
      this.index = index;
      this.level = level;
    }

    public static EntityType getByLevel(int level) {
      for (EntityType entityType : EntityType.values()) {
        if (level == entityType.level) {
          return entityType;
        }
      }

      return null;
    }

    public EntityType nextLevel() {
      // Currently we do not support orignize type column into the layer system.
      return level >= TABLE.level ? null : getByLevel(level + 1);
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
