/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import java.util.Objects;

/** The helper class for {@link SecurableObject}. */
public class SecurableObjects {

  /**
   * Create the {@link SecurableObject} with the given names.
   *
   * @param names The names of the securable object.
   * @return The created {@link SecurableObject}
   */
  public static SecurableObject of(String... names) {
    if (names == null) {
      throw new IllegalArgumentException("Cannot create a securable object with null names");
    }

    if (names.length == 0) {
      throw new IllegalArgumentException("Cannot create a securable object with no names");
    }

    SecurableObject parent = null;
    for (String name : names) {
      if (name == null) {
        throw new IllegalArgumentException("Cannot create a securable object with null name");
      }

      if (name.equals("*")) {
        throw new IllegalArgumentException(
            "Cannot create a securable object with `*` name. If you want to use a securable object which represents all catalogs,"
                + " you use the method `ofAllCatalogs`."
                + " If you want to create an another securable object which represents all entities,"
                + " you can use its parent entity, For example,"
                + " if you want to have read table privileges of all tables of `catalog1.schema1`,"
                + " you can use add `read table` privilege for `catalog1.schema1` directly");
      }

      parent = new SecurableObjectImpl(parent, name);
    }

    return parent;
  }

  /**
   * All catalogs is a special securable object .You can give the securable object the privileges
   * `LOAD CATALOG`, `CREATE CATALOG`, etc. It means that you can load any catalog and create any
   * which doesn't exist.
   *
   * @return The created {@link SecurableObject}
   */
  public static SecurableObject ofAllCatalogs() {
    return ALL_CATALOGS;
  }

  private static final SecurableObject ALL_CATALOGS = new SecurableObjectImpl(null, "*");

  private static class SecurableObjectImpl implements SecurableObject {

    private final SecurableObject parent;
    private final String name;

    SecurableObjectImpl(SecurableObject parent, String name) {
      this.parent = parent;
      this.name = name;
    }

    @Override
    public SecurableObject parent() {
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
      if (!(other instanceof SecurableObject)) {
        return false;
      }

      SecurableObject otherSecurableObject = (SecurableObject) other;
      return Objects.equals(parent, otherSecurableObject.parent())
          && Objects.equals(name, otherSecurableObject.name());
    }
  }
}
