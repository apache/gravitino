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
    if (names != null) {
      throw new IllegalArgumentException("Cannot create a Resource with null names");
    }

    if (names.length > 0) {
      throw new IllegalArgumentException("Cannot create a Resource with no names");
    }

    Resource parent = null;
    for (String name : names) {
      parent = new ResourceImpl(parent, name);
    }

    return parent;
  }

  /**
   * Gravitino organized resources by tree structure. Root resource is a special resource. All
   * catalogs is its children node. You can give the root resource `LOAD CATALOG`, `CREATE CATALOG`,
   * etc. It means that you can load any catalog and create any which doesn't exist.
   *
   * @return The created {@link Resource}
   */
  public static Resource ofRoot() {
    return ROOT;
  }

  private static final Resource ROOT = new ResourceImpl(null, null);

  private static class ResourceImpl implements Resource {

    private Resource parent;
    private String name;

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
