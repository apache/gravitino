/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/** The helper class for {@link MetadataObject}. */
public class MetadataObjects {

  /**
   * The reserved name for the metadata object.
   *
   * <p>It is used to represent the root metadata object of all metalakes.
   */
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

    return new MetadataObjectImpl(parent, name, type);
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
            || type == MetadataObject.Type.METALAKE,
        "If the length of names is 1, it must be the CATALOG or METALAKE type");

    Preconditions.checkArgument(
        names.size() != 2 || type == MetadataObject.Type.SCHEMA,
        "If the length of names is 2, it must be the SCHEMA type");

    Preconditions.checkArgument(
        names.size() != 3
            || type == MetadataObject.Type.FILESET
            || type == MetadataObject.Type.TABLE
            || type == MetadataObject.Type.TOPIC,
        "If the length of names is 3, it must be FILESET, TABLE or TOPIC");

    Preconditions.checkArgument(
        names.size() != 4 || type == MetadataObject.Type.COLUMN,
        "If the length of names is 4, it must be COLUMN");

    for (String name : names) {
      checkName(name);
    }

    return new MetadataObjectImpl(getParentFullName(names), getLastName(names), type);
  }

  /**
   * Parse the metadata object with the given full name and type.
   *
   * @param fullName The full name of the metadata object
   * @param type The type of the metadata object
   * @return The parsed metadata object
   */
  public static MetadataObject parse(String fullName, MetadataObject.Type type) {
    if (METADATA_OBJECT_RESERVED_NAME.equals(fullName)) {
      if (type != MetadataObject.Type.METALAKE) {
        throw new IllegalArgumentException("If metadata object isn't metalake, it can't be `*`");
      }
      return new MetadataObjectImpl(null, METADATA_OBJECT_RESERVED_NAME, type);
    }

    Preconditions.checkArgument(
        StringUtils.isNotBlank(fullName), "Metadata object full name cannot be blank");

    List<String> parts = DOT_SPLITTER.splitToList(fullName);

    return MetadataObjects.of(parts, type);
  }

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
