/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import java.util.Objects;

/** The helper class for {@link Resource}. */
public class Resources {

  /**
   * Create the {@link Resource} with the given names.
   *
   * @param names The names of the resource
   * @return The created {@link Resource}
   */
  public static Resource of(String... names) {
    if (names == null) {
      throw new IllegalArgumentException("Cannot create a resource with null names");
    }

    if (names.length == 0) {
      throw new IllegalArgumentException("Cannot create a resource with no names");
    }

    Resource parent = null;
    for (String name : names) {
      if (name == null) {
        throw new IllegalArgumentException("Cannot create a resource with null name");
      }

      if (name.equals("*")) {
        throw new IllegalArgumentException(
            "Cannot create a resource with `*` name. If you want to use a resource which represents all catalogs,"
                + " you use the method `ofAllCatalogs`."
                + " If you want to create an another resource which represents all entities,"
                + " you can use its parent entity, For example,"
                + " if you want to have read table privileges of all tables of `catalog1.schema1`,"
                + " you can use add `read table` privilege for `catalog1.schema1` directly");
      }

      parent = new ResourceImpl(parent, name);
    }

    return parent;
  }

  /**
   * All catalogs is a special resource .You can give the resource the privileges `LOAD CATALOG`,
   * `CREATE CATALOG`, etc. It means that you can load any catalog and create any which doesn't
   * exist.
   *
   * @return The created {@link Resource}
   */
  public static Resource ofAllCatalogs() {
    return ALL_CATALOGS;
  }

  private static final Resource ALL_CATALOGS = new ResourceImpl(null, "*");

  private static class ResourceImpl implements Resource {

    private final Resource parent;
    private final String name;

    ResourceImpl(Resource parent, String name) {
      this.parent = parent;
      this.name = name;
    }

    @Override
    public Resource parent() {
      return parent;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public int hashCode() {
      return Objects.hash(parent, name);
    }

    @Override
    public String toString() {
      if (parent != null) {
        return parent + "." + name;
      } else {
        return name;
      }
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Resource)) {
        return false;
      }

      Resource otherResource = (Resource) other;
      return Objects.equals(parent, otherResource.parent())
          && Objects.equals(name, otherResource.name());
    }
  }
}
