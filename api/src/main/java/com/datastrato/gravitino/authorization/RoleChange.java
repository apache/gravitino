/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.annotation.Evolving;

/** The RoleChange interface defines the public API for managing roles in an authorization. */
@Evolving
public interface RoleChange {
  /**
   * Add a securable object RoleChange.
   *
   * @param securableObject The securable object.
   * @return return a RoleChange for the add securable object.
   */
  static RoleChange addSecurableObject(SecurableObject securableObject) {
    return new AddSecurableObject(securableObject);
  }

  /**
   * Remove a securable object RoleChange.
   *
   * @param securableObject The securable object.
   * @return return a RoleChange for the add securable object.
   */
  static RoleChange removeSecurableObject(SecurableObject securableObject) {
    return new RemoveSecurableObject(securableObject);
  }

  /** A AddSecurableObject to add securable object to role. */
  final class AddSecurableObject implements RoleChange {
    private final SecurableObject securableObject;

    private AddSecurableObject(SecurableObject securableObject) {
      this.securableObject = securableObject;
    }

    /**
     * Returns the securable object to be added.
     *
     * @return return a securable object.
     */
    public SecurableObject getSecurableObject() {
      return this.securableObject;
    }

    /**
     * Compares this AddSecurableObject instance with another object for equality. The comparison is
     * based on the add securable object to role.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same add securable object; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AddSecurableObject that = (AddSecurableObject) o;
      return securableObject.equals(that.securableObject);
    }

    /**
     * Generates a hash code for this AddSecurableObject instance. The hash code is based on the add
     * securable object.
     *
     * @return A hash code value for this add securable object operation.
     */
    @Override
    public int hashCode() {
      return securableObject.hashCode();
    }

    /**
     * Returns a string representation of the AddSecurableObject instance. This string format
     * includes the class name followed by the add securable object operation.
     *
     * @return A string representation of the AddSecurableObject instance.
     */
    @Override
    public String toString() {
      return "ADDSECURABLEOBJECT " + securableObject;
    }
  }

  /** A RemoveSecurableObject to remove securable object from role. */
  final class RemoveSecurableObject implements RoleChange {
    private final SecurableObject securableObject;

    private RemoveSecurableObject(SecurableObject securableObject) {
      this.securableObject = securableObject;
    }

    /**
     * Returns the securable object to be added.
     *
     * @return return a securable object.
     */
    public SecurableObject getSecurableObject() {
      return this.securableObject;
    }

    /**
     * Compares this RemoveSecurableObject instance with another object for equality. The comparison
     * is based on the add securable object to role.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same add securable object; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RemoveSecurableObject that = (RemoveSecurableObject) o;
      return securableObject.equals(that.securableObject);
    }

    /**
     * Generates a hash code for this RemoveSecurableObject instance. The hash code is based on the
     * add securable object.
     *
     * @return A hash code value for this add securable object operation.
     */
    @Override
    public int hashCode() {
      return securableObject.hashCode();
    }

    /**
     * Returns a string representation of the RemoveSecurableObject instance. This string format
     * includes the class name followed by the add securable object operation.
     *
     * @return A string representation of the RemoveSecurableObject instance.
     */
    @Override
    public String toString() {
      return "REMOVESECURABLEOBJECT " + securableObject;
    }
  }
}
