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

    switch (name) {
      case LOAD_METALAKE:
        return LoadMetalake.get();
      default:
        throw new IllegalArgumentException("Don't support the privilege: " + privilege);
    }
  }

  /** The privilege of load a metalake. */
  public static class LoadMetalake implements Privilege {
    private static final LoadMetalake INSTANCE = new LoadMetalake();

    /** @return The instance of the privilege. */
    public static LoadMetalake get() {
      return INSTANCE;
    }

    private LoadMetalake() {}

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return Name.LOAD_METALAKE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "list metalake";
    }
  }
}
