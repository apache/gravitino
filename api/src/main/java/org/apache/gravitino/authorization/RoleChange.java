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

import org.apache.gravitino.annotation.Evolving;

/** The RoleChange interface defines the public API for managing roles in an authorization. */
@Evolving
public interface RoleChange {
  /**
   * Create a RoleChange to add a securable object into a role.
   *
   * @param securableObject The securable object.
   * @return return a RoleChange for the add securable object.
   */
  static RoleChange addSecurableObject(SecurableObject securableObject) {
    return new AddSecurableObject(securableObject);
  }

  /**
   * Create a RoleChange to remove a securable object from a role.
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
