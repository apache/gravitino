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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/** The helper class for {@link MetadataObject}. */
public class MetadataObjects {

  /** The reserved name for the metadata object. */
  public static final String METADATA_OBJECT_RESERVED_NAME = "*";

  private static final Splitter DOT_SPLITTER = Splitter.on('.');

  private static final Joiner DOT_JOINER = Joiner.on('.');

  private static final Set<MetadataObject.Type> VALID_SINGLE_LEVEL_NAME_TYPES =
      Sets.newHashSet(
          MetadataObject.Type.CATALOG,
          MetadataObject.Type.METALAKE,
          MetadataObject.Type.ROLE,
          MetadataObject.Type.TAG,
          MetadataObject.Type.JOB,
          MetadataObject.Type.JOB_TEMPLATE,
          MetadataObject.Type.POLICY);

  private static final Set<MetadataObject.Type> VALID_TWO_LEVEL_NAME_TYPES =
      Sets.newHashSet(MetadataObject.Type.SCHEMA);

  private static final Set<MetadataObject.Type> VALID_THREE_LEVEL_NAME_TYPES =
      Sets.newHashSet(
          MetadataObject.Type.FILESET,
          MetadataObject.Type.TABLE,
          MetadataObject.Type.TOPIC,
          MetadataObject.Type.MODEL);

  private static final Set<MetadataObject.Type> VALID_FOUR_LEVEL_NAME_TYPES =
      Sets.newHashSet(MetadataObject.Type.COLUMN);

  private static final Map<Set<MetadataObject.Type>, Integer> TYPE_TO_EXPECT_LENGTH =
      ImmutableMap.of(
          VALID_SINGLE_LEVEL_NAME_TYPES, 1,
          VALID_TWO_LEVEL_NAME_TYPES, 2,
          VALID_THREE_LEVEL_NAME_TYPES, 3,
          VALID_FOUR_LEVEL_NAME_TYPES, 4);

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
    if (VALID_SINGLE_LEVEL_NAME_TYPES.contains(type)) {
      Preconditions.checkArgument(
          parent == null, "If the type is " + type + ", parent must be null");
    }

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
    Preconditions.checkArgument(type != null, "Cannot create a metadata object with no type");

    Integer expectedLength =
        TYPE_TO_EXPECT_LENGTH.entrySet().stream()
            .filter(entry -> entry.getKey().contains(type))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElseThrow(
                () -> new IllegalArgumentException("Unsupported metadata object type: " + type));

    Preconditions.checkArgument(
        names.size() == expectedLength,
        "If the type is " + type + ", the length of names must be " + expectedLength);

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
    if (VALID_SINGLE_LEVEL_NAME_TYPES.contains(object.type())) {
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
    if (VALID_SINGLE_LEVEL_NAME_TYPES.contains(type)) {
      return of(Collections.singletonList(fullName), type);
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
