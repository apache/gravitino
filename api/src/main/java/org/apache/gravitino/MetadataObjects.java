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
package org.apache.gravitino;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/** The helper class for {@link MetadataObject}. */
public class MetadataObjects {

  /** The reserved name for the metadata object. */
  public static final String METADATA_OBJECT_RESERVED_NAME = "*";

  private static final Splitter DOT_SPLITTER = Splitter.on('.');

  private static final Joiner DOT_JOINER = Joiner.on('.');

  private MetadataObjects() {}

  /**
   * Create the metadata object with the given name, parent and type.
   *
   * @param parent The parent of the metadata object
   * @param name The name of the metadata object
   * @param type The type of the metadata object
   * @return The created metadata object
   */
  public static MetadataObject of(String parent, String name, MetadataObject.Type type) {
    Preconditions.checkArgument(name != null, "Cannot create a metadata object with null name");
    Preconditions.checkArgument(type != null, "Cannot create a metadata object with no type");

    String fullName = parent == null ? name : DOT_JOINER.join(parent, name);
    return parse(fullName, type);
  }

  /**
   * Create the metadata object with the given names and type.
   *
   * @param names The names of the metadata object
   * @param type The type of the metadata object
   * @return The created metadata object
   */
  public static MetadataObject of(List<String> names, MetadataObject.Type type) {
    Preconditions.checkArgument(names != null, "Cannot create a metadata object with null names");
    Preconditions.checkArgument(!names.isEmpty(), "Cannot create a metadata object with no names");
    Preconditions.checkArgument(
        names.size() <= 4,
        "Cannot create a metadata object with the name length which is greater than 4");
    Preconditions.checkArgument(type != null, "Cannot create a metadata object with no type");

    Preconditions.checkArgument(
        names.size() != 1
            || type == MetadataObject.Type.CATALOG
            || type == MetadataObject.Type.METALAKE
            || type == MetadataObject.Type.ROLE
            || type == MetadataObject.Type.TAG
            || type == MetadataObject.Type.POLICY,
        "If the length of names is 1, it must be the CATALOG, METALAKE,TAG, or ROLE type");

    Preconditions.checkArgument(
        names.size() != 2 || type == MetadataObject.Type.SCHEMA,
        "If the length of names is 2, it must be the SCHEMA type");

    Preconditions.checkArgument(
        names.size() != 3
            || type == MetadataObject.Type.FILESET
            || type == MetadataObject.Type.TABLE
            || type == MetadataObject.Type.TOPIC
            || type == MetadataObject.Type.MODEL,
        "If the length of names is 3, it must be FILESET, TABLE, TOPIC or MODEL");

    Preconditions.checkArgument(
        names.size() != 4 || type == MetadataObject.Type.COLUMN,
        "If the length of names is 4, it must be COLUMN");

    for (String name : names) {
      checkName(name);
    }

    return new MetadataObjectImpl(getParentFullName(names), getLastName(names), type);
  }

  /**
   * Get the parent metadata object of the given metadata object.
   *
   * @param object The metadata object
   * @return The parent metadata object if it exists, otherwise null
   */
  @Nullable
  public static MetadataObject parent(MetadataObject object) {
    if (object == null) {
      return null;
    }

    // Return null if the object is the root object
    if (object.type() == MetadataObject.Type.METALAKE
        || object.type() == MetadataObject.Type.CATALOG
        || object.type() == MetadataObject.Type.ROLE) {
      return null;
    }

    MetadataObject.Type parentType;
    switch (object.type()) {
      case COLUMN:
        parentType = MetadataObject.Type.TABLE;
        break;
      case TABLE:
      case FILESET:
      case TOPIC:
      case MODEL:
        parentType = MetadataObject.Type.SCHEMA;
        break;
      case SCHEMA:
        parentType = MetadataObject.Type.CATALOG;
        break;

      default:
        throw new IllegalArgumentException(
            "Unexpected to reach here for metadata object type: " + object.type());
    }

    return parse(object.parent(), parentType);
  }

  /**
   * Parse the metadata object with the given full name and type.
   *
   * @param fullName The full name of the metadata object
   * @param type The type of the metadata object
   * @return The parsed metadata object
   */
  public static MetadataObject parse(String fullName, MetadataObject.Type type) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(fullName), "Metadata object full name cannot be blank");

    List<String> parts = DOT_SPLITTER.splitToList(fullName);
    if (type == MetadataObject.Type.ROLE) {
      return MetadataObjects.of(Collections.singletonList(fullName), MetadataObject.Type.ROLE);
    }
    if (type == MetadataObject.Type.TAG) {
      return MetadataObjects.of(Collections.singletonList(fullName), MetadataObject.Type.TAG);
    }

    return MetadataObjects.of(parts, type);
  }

  /**
   * Get the parent full name of the given full name.
   *
   * @param names The names of the metadata object
   * @return The parent full name if it exists, otherwise null
   */
  private static String getParentFullName(List<String> names) {
    if (names.size() <= 1) {
      return null;
    }

    return DOT_JOINER.join(names.subList(0, names.size() - 1));
  }

  private static String getLastName(List<String> names) {
    return names.get(names.size() - 1);
  }

  private static void checkName(String name) {
    Preconditions.checkArgument(name != null, "Cannot create a metadata object with null name");
    Preconditions.checkArgument(
        !METADATA_OBJECT_RESERVED_NAME.equals(name),
        "Cannot create a metadata object with `*` name.");
  }

  /** The implementation of the {@link MetadataObject}. */
  public static class MetadataObjectImpl implements MetadataObject {

    private final String name;

    private final String parent;

    private final Type type;

    /**
     * Create the metadata object with the given name, parent and type.
     *
     * @param parent The parent of the metadata object
     * @param name The name of the metadata object
     * @param type The type of the metadata object
     */
    public MetadataObjectImpl(String parent, String name, Type type) {
      this.parent = parent;
      this.name = name;
      this.type = type;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String parent() {
      return parent;
    }

    @Override
    public Type type() {
      return type;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof MetadataObjectImpl)) {
        return false;
      }

      MetadataObjectImpl that = (MetadataObjectImpl) o;
      return java.util.Objects.equals(name, that.name)
          && java.util.Objects.equals(parent, that.parent)
          && type == that.type;
    }

    @Override
    public int hashCode() {
      return java.util.Objects.hash(name, parent, type);
    }

    @Override
    public String toString() {
      return "MetadataObject: [fullName=" + fullName() + "], [type=" + type + "]";
    }
  }
}
