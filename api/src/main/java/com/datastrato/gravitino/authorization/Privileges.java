/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

/** The helper class for {@link Privilege}. */
public class Privileges {

  /**
   * Returns the Privilege from the string representation.
   *
   * @param privilege The string representation of the privilege.
   * @return The Privilege.
   */
  public static Privilege fromString(String privilege) {
    Privilege.Name name = Privilege.Name.valueOf(privilege);
    return fromName(name);
  }

  /**
   * Returns the Privilege from the `Privilege.Name`.
   *
   * @param name The `Privilege.Name` of the privilege.
   * @return The Privilege.
   */
  public static Privilege fromName(Privilege.Name name) {
    switch (name) {
      case LOAD_CATALOG:
        return LoadCatalog.get();
      default:
        throw new IllegalArgumentException("Don't support the privilege: " + name);
    }
  }

  /** The privilege of load a catalog. */
  public static class LoadCatalog implements Privilege {
    private static final LoadCatalog INSTANCE = new LoadCatalog();

    /** @return The instance of the privilege. */
    public static LoadCatalog get() {
      return INSTANCE;
    }

    private LoadCatalog() {}

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return Name.LOAD_CATALOG;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "load catalog";
    }
  }
}
