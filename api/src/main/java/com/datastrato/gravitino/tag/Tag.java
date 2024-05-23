/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.tag;

import com.datastrato.gravitino.Auditable;
import com.datastrato.gravitino.MetadataObject;
import com.datastrato.gravitino.annotation.Evolving;
import java.util.Map;
import java.util.Optional;

/**
 * The interface of a tag. A tag is a label that can be attached to a catalog, schema, table,
 * fileset, topic, or column. It can be used to categorize, classify, or annotate these objects.
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
   * Check if the tag is inherited from a parent object or not. If the tag is inherited, it will
   * return true, if it is owned by the object itself, it will return false.
   *
   * <p>Note. The return value is optional, only when the tag is associated with an object, and
   * called from the object, the return value will be present. Otherwise, it will be empty.
   *
   * @return True if the tag is inherited, false if it is owned by the object itself. Empty if the
   *     tag is not associated with any object.
   */
  Optional<Boolean> inherited();

  /** @return Extended information of the tag. */
  default Extended extended() {
    throw new UnsupportedOperationException("The extended method is not supported.");
  }

  /** The extended information of the tag. */
  interface Extended {

    /** @return The number of objects that are associated with this Tag */
    default int inAssociation() {
      MetadataObject[] objects = associatedObjects();
      return objects == null ? 0 : objects.length;
    }

    /** @return The list of objects that are associated with this tag. */
    MetadataObject[] associatedObjects();
  }
}
