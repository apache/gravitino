/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import lombok.Getter;

/** This interface defines an entity within the Gravitino framework. */
public interface Entity extends Serializable {

  // The below constants are used for virtual metalakes, catalogs and schemas
  // The system doesn't need to create them. The system uses these constants
  // to organize the system information better.

  /** The system reserved metalake name. */
  String SYSTEM_METALAKE_RESERVED_NAME = "system";

  /** The system reserved catalog name. */
  String SYSTEM_CATALOG_RESERVED_NAME = "system";

  /** The authorization catalog name in the system metalake. */
  String AUTHORIZATION_CATALOG_NAME = "authorization";

  /** The user schema name in the system catalog. */
  String USER_SCHEMA_NAME = "user";

  /** The group schema name in the system catalog. */
  String GROUP_SCHEMA_NAME = "group";
  /** The role schema name in the system catalog. */
  String ROLE_SCHEMA_NAME = "role";

  /** The admin schema name in the authorization catalog of the system metalake. */
  String ADMIN_SCHEMA_NAME = "admin";

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
    USER("us", 7),
    GROUP("gr", 8),
    ROLE("ro", 9),

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

    /**
     * Returns the parent entity types of the given entity type. The parent entity types are the
     * entity types that are higher in the hierarchy than the given entity type. For example, the
     * parent entity types of a table are metalake, catalog, and schema. (Sequence: root to leaf)
     *
     * @param entityType The entity type for which to get the parent entity types.
     * @return The parent entity types of the given entity type.
     */
    public static List<EntityType> getParentEntityTypes(EntityType entityType) {
      switch (entityType) {
        case METALAKE:
          return ImmutableList.of();
        case CATALOG:
          return ImmutableList.of(METALAKE);
        case SCHEMA:
          return ImmutableList.of(METALAKE, CATALOG);
        case TABLE:
        case FILESET:
        case TOPIC:
        case USER:
        case GROUP:
        case ROLE:
          return ImmutableList.of(METALAKE, CATALOG, SCHEMA);
        case COLUMN:
          return ImmutableList.of(METALAKE, CATALOG, SCHEMA, TABLE);
        default:
          throw new IllegalArgumentException("Unknown entity type: " + entityType);
      }
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
