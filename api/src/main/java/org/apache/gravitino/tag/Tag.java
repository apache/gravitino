/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.tag;

import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.Evolving;

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

  /**
   * @return The name of the tag.
   */
  String name();

  /**
   * @return The comment of the tag.
   */
  String comment();

  /**
   * @return The properties of the tag.
   */
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

  /**
   * @return The associated objects of the tag.
   */
  default AssociatedObjects associatedObjects() {
    throw new UnsupportedOperationException("The associatedObjects method is not supported.");
  }

  /** The interface of the associated objects of the tag. */
  interface AssociatedObjects {

    /**
     * @return The number of objects that are associated with this Tag
     */
    default int count() {
      MetadataObject[] objects = objects();
      return objects == null ? 0 : objects.length;
    }

    /**
     * @return The list of objects that are associated with this tag.
     */
    MetadataObject[] objects();
  }
}
