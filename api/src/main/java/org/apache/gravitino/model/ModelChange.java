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

package org.apache.gravitino.model;

import org.apache.gravitino.annotation.Evolving;

/**
 * A model change is a change to a model. It can be used to rename a model, update the comment of a
 * model, set a property and value pair for a model, or remove a property from a model.
 */
@Evolving
public interface ModelChange {
  /**
   * Create a ModelChange for renaming a model.
   *
   * @param newName The new model name.
   * @return A ModelChange for the rename.
   */
  static ModelChange rename(String newName) {
    return new ModelChange.RenameModel(newName);
  }

  /** A ModelChange to rename a model. */
  final class RenameModel implements ModelChange {
    private final String newName;

    /**
     * Constructs a new {@link RenameModel} instance with the specified new name.
     *
     * @param newName The new name of the model.
     */
    public RenameModel(String newName) {
      this.newName = newName;
    }

    /**
     * Retrieves the new name for the model.
     *
     * @return The new name of the model.
     */
    public String newName() {
      return newName;
    }

    /**
     * Compares this {@code RenameModel} instance with another object for equality. The comparison
     * is based on the new name of the model.
     *
     * @param obj The object to compare with this instance.
     * @return {@code true} if the given object represents the same model renaming; {@code false}
     *     otherwise.
     */
    @Override
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (obj == null || getClass() != obj.getClass()) return false;
      RenameModel other = (RenameModel) obj;
      return newName.equals(other.newName);
    }

    /**
     * Generates a hash code for this {@code RenameModel} instance. The hash code is based on the
     * new name of the model.
     *
     * @return A hash code value for this model renaming operation.
     */
    @Override
    public int hashCode() {
      return newName.hashCode();
    }

    /**
     * Returns a string representation of the {@code RenameModel} instance. This string format
     * includes the class name followed by the property name to be renamed.
     *
     * @return A string summary of the property rename instance.
     */
    @Override
    public String toString() {
      return "RenameModel " + newName;
    }
  }
}
