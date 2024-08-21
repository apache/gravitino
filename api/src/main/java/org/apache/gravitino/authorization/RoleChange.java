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
package org.apache.gravitino.authorization;

import java.util.Objects;
import org.apache.gravitino.annotation.Evolving;

/** The RoleChange interface defines the public API for managing roles in an authorization. */
@Evolving
public interface RoleChange {
  /**
   * Create a RoleChange to add a securable object into a role.
   *
   * @param roleName The role name.
   * @param securableObject The securable object.
   * @return return a RoleChange for the added securable object.
   */
  static RoleChange addSecurableObject(String roleName, SecurableObject securableObject) {
    return new AddSecurableObject(roleName, securableObject);
  }

  /**
   * Create a RoleChange to remove a securable object from a role.
   *
   * @param roleName The role name.
   * @param securableObject The securable object.
   * @return return a RoleChange for the added securable object.
   */
  static RoleChange removeSecurableObject(String roleName, SecurableObject securableObject) {
    return new RemoveSecurableObject(roleName, securableObject);
  }

  /**
   * Update a securable object RoleChange.
   *
   * @param roleName The role name.
   * @param securableObject The securable object.
   * @param newSecurableObject The new securable object.
   * @return return a RoleChange for the update securable object.
   */
  static RoleChange updateSecurableObject(
      String roleName, SecurableObject securableObject, SecurableObject newSecurableObject) {
    return new UpdateSecurableObject(roleName, securableObject, newSecurableObject);
  }

  /** A AddSecurableObject to add a securable object to a role. */
  final class AddSecurableObject implements RoleChange {
    private final String roleName;
    private final SecurableObject securableObject;

    private AddSecurableObject(String roleName, SecurableObject securableObject) {
      this.roleName = roleName;
      this.securableObject = securableObject;
    }

    /**
     * Returns the role name to be added.
     *
     * @return return a role name.
     */
    public String getRoleName() {
      return this.roleName;
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
     * based on the added securable object to a role.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same as the add securable object; false
     *     otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AddSecurableObject that = (AddSecurableObject) o;
      return roleName.equals(that.roleName) && securableObject.equals(that.securableObject);
    }

    /**
     * Generates a hash code for this AddSecurableObject instance. The hash code is based on the add
     * securable object.
     *
     * @return A hash code value for this add securable object operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(roleName, securableObject);
    }

    /**
     * Returns a string representation of the AddSecurableObject instance. This string format
     * includes the class name followed by the add securable object operation.
     *
     * @return A string representation of the AddSecurableObject instance.
     */
    @Override
    public String toString() {
      return "ADDSECURABLEOBJECT " + roleName + " " + securableObject;
    }
  }

  /** A RemoveSecurableObject to remove a securable object from a role. */
  final class RemoveSecurableObject implements RoleChange {
    private final String roleName;
    private final SecurableObject securableObject;

    private RemoveSecurableObject(String roleName, SecurableObject securableObject) {
      this.roleName = roleName;
      this.securableObject = securableObject;
    }

    /**
     * Returns the role name.
     *
     * @return return a role name.
     */
    public String getRoleName() {
      return roleName;
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
     * is based on the added securable object to a role.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same as remove securable object; false
     *     otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RemoveSecurableObject that = (RemoveSecurableObject) o;
      return roleName.equals(that.roleName) && securableObject.equals(that.securableObject);
    }

    /**
     * Generates a hash code for this RemoveSecurableObject instance. The hash code is based on the
     * added securable object.
     *
     * @return A hash code value for this adds securable object operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(roleName, securableObject);
    }

    /**
     * Returns a string representation of the RemoveSecurableObject instance. This string format
     * includes the class name followed by the add securable object operation.
     *
     * @return A string representation of the RemoveSecurableObject instance.
     */
    @Override
    public String toString() {
      return "REMOVESECURABLEOBJECT " + roleName + " " + securableObject;
    }
  }

  /**
   * A UpdateSecurableObject is to update securable object's privilege from a role. <br>
   * The securable object's metadata entity must be the same as new securable object's metadata
   * entity. <br>
   * The securable object's privilege must be different from new securable object's privilege. <br>
   */
  final class UpdateSecurableObject implements RoleChange {
    private final String roleName;
    private final SecurableObject securableObject;
    private final SecurableObject newSecurableObject;

    private UpdateSecurableObject(
        String roleName, SecurableObject securableObject, SecurableObject newSecurableObject) {
      if (!securableObject.fullName().equals(newSecurableObject.fullName())) {
        throw new IllegalArgumentException(
            "The securable object's metadata entity must be same as new securable object's metadata entity.");
      }
      if (securableObject.privileges().equals(newSecurableObject.privileges())) {
        throw new IllegalArgumentException(
            "The securable object's privilege must be different as new securable object's privilege.");
      }
      this.roleName = roleName;
      this.securableObject = securableObject;
      this.newSecurableObject = newSecurableObject;
    }

    /**
     * Returns the role name.
     *
     * @return return a role name.
     */
    public String getRoleName() {
      return roleName;
    }

    /**
     * Returns the securable object to be updated.
     *
     * @return return a securable object.
     */
    public SecurableObject getSecurableObject() {
      return this.securableObject;
    }

    /**
     * Returns the new securable object.
     *
     * @return return a securable object.
     */
    public SecurableObject getNewSecurableObject() {
      return this.newSecurableObject;
    }

    /**
     * Compares this UpdateSecurableObject instance with another object for equality. The comparison
     * is based on the old securable object and new securable object.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same update securable object; false
     *     otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdateSecurableObject that = (UpdateSecurableObject) o;
      return roleName.equals(that.roleName)
          && securableObject.equals(that.securableObject)
          && newSecurableObject.equals(that.newSecurableObject);
    }

    /**
     * Generates a hash code for this UpdateSecurableObject instance. The hash code is based on the
     * old securable object and new securable object.
     *
     * @return A hash code value for this update securable object operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(roleName, securableObject, newSecurableObject);
    }

    /**
     * Returns a string representation of the UpdateSecurableObject instance. This string format
     * includes the class name followed by the add securable object operation.
     *
     * @return A string representation of the RemoveSecurableObject instance.
     */
    @Override
    public String toString() {
      return "UPDATESECURABLEOBJECT " + roleName + " " + securableObject + " " + newSecurableObject;
    }
  }
}
