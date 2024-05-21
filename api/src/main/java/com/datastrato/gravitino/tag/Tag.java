/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.tag;

import com.datastrato.gravitino.Auditable;
import com.datastrato.gravitino.annotation.Evolving;
import java.util.Map;
import java.util.Optional;

/**
 * The interface of a tag. A tag is a label that can be attached to a catalog, schema, table,
 * fileset, topic, or column. It can be used to categorize, classify, or annotate these entities.
 */
@Evolving
public interface Tag extends Auditable {

  /**
   * A reserved property to specify the color of the tag. The color is a string of hex code that
   * represents the color of the tag. The color is used to visually distinguish the tag from other
   * tags.
   */
  String PROPERTY_COLOR = "color";

  /** @return The name of the tag. */
  String name();

  /** @return The comment of the tag. */
  String comment();

  /** @return The properties of the tag. */
  Map<String, String> properties();

  /**
   * Check if the tag is inherited from a parent entity or not. If the tag is inherited, it will
   * return true, if it is owned by the entity itself, it will return false.
   *
   * <p>Note. The return value is optional, only when the tag is associated with an entity, and
   * called from the entity, the return value will be present. Otherwise, it will be empty.
   *
   * @return True if the tag is inherited, false if it is owned by the entity itself. Empty if the
   *     tag is not associated with any entity.
   */
  Optional<Boolean> inherited();

  /** @return Extended information of the tag. */
  default Extended extended() {
    throw new UnsupportedOperationException("The extended method is not supported.");
  }

  /** The extended information of the tag. */
  interface Extended {

    /** The supported entities that a tag can be attached to. */
    enum SupportedEntityType {
      CATALOG,
      SCHEMA,
      TABLE,
      FILESET,
      TOPIC,
      COLUMN
    }

    /** A wrapper class to represent an entity that is using the tag. */
    final class UsedEntity {
      private final SupportedEntityType type;
      private final String identifier;

      private UsedEntity(SupportedEntityType type, String identifier) {
        this.type = type;
        this.identifier = identifier;
      }

      public SupportedEntityType type() {
        return type;
      }

      public String identifier() {
        return identifier;
      }

      public static UsedEntity of(SupportedEntityType type, String identifier) {
        return new UsedEntity(type, identifier);
      }
    }

    /** @return The number of entities that are using this tag. */
    int inUse();

    /** @return The list of entities that are using this tag. */
    UsedEntity[] usedEntities();
  }
}
