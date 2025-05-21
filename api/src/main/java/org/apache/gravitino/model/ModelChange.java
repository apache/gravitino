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

import java.util.Objects;
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

  /**
   * Create a ModelChange for setting a property and value of a model.
   *
   * @param property The name of the property to be set.
   * @param value The value to be set for the property.
   * @return A ModelChange for the property set.
   */
  static ModelChange setProperty(String property, String value) {
    return new ModelChange.SetProperty(property, value);
  }

  /**
   * Create a ModelChange for removing a property from a model.
   *
   * @param property The name of the property to be removed from the model.
   * @return A ModelChange for the property removal.
   */
  static ModelChange removeProperty(String property) {
    return new ModelChange.RemoveProperty(property);
  }

  /**
   * Create a ModelChange for updating the comment of a model.
   *
   * @param newComment The new comment of the model.
   * @return A ModelChange for the comment update.
   */
  static ModelChange updateComment(String newComment) {
    return new ModelChange.UpdateComment(newComment);
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
      if (!(obj instanceof RenameModel)) return false;
      RenameModel other = (RenameModel) obj;
      return Objects.equals(newName, other.newName);
    }

    /**
     * Generates a hash code for this {@code RenameModel} instance. The hash code is based on the
     * new name of the model.
     *
     * @return A hash code value for this model renaming operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(newName);
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

  /** A ModelChange to set a property and value of a model. */
  final class SetProperty implements ModelChange {
    private final String property;
    private final String value;

    /**
     * Constructs a new {@link SetProperty} instance with the specified property name and value.
     *
     * @param property The name of the property to be set.
     * @param value The value to be set for the property.
     */
    public SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /**
     * Retrieves the name of the property to be set.
     *
     * @return The name of the property to be set.
     */
    public String property() {
      return property;
    }

    /**
     * Retrieves the value to be set for the property.
     *
     * @return The value to be set for the property.
     */
    public String value() {
      return value;
    }

    /**
     * Compares this SetProperty instance with another object for equality. Two instances are
     * considered equal if they target the same property and set the same value.
     *
     * @param obj The object to compare with this instance.
     * @return {@code true} if the given object represents the same property set; {@code false}
     *     otherwise.
     */
    @Override
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (!(obj instanceof SetProperty)) return false;
      SetProperty other = (SetProperty) obj;
      return Objects.equals(property, other.property) && Objects.equals(value, other.value);
    }

    /**
     * Generates a hash code for this SetProperty instance. The hash code is based on the property
     * name and value to be set.
     *
     * @return A hash code value for this property set operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(property, value);
    }

    /**
     * Provides a string representation of the SetProperty instance. This string format includes the
     * class name followed by the property name and value to be set.
     *
     * @return A string summary of the property set operation.
     */
    @Override
    public String toString() {
      return "SETPROPERTY " + property + " " + value;
    }
  }

  /** A ModelChange to remove a property from model. */
  final class RemoveProperty implements ModelChange {
    private final String property;

    /**
     * Constructs a new {@link RemoveProperty} instance with the specified property name.
     *
     * @param property The name of the property to be removed from the model.
     */
    public RemoveProperty(String property) {
      this.property = property;
    }

    /**
     * Retrieves the name of the property to be removed from the model.
     *
     * @return The name of the property for removal.
     */
    public String property() {
      return property;
    }

    /**
     * Compares this RemoveProperty instance with another object for equality. Two instances are
     * considered equal if they target the same property for removal from the fileset.
     *
     * @param obj The object to compare with this instance.
     * @return {@code true} if the given object represents the same property removal; {@code false}
     *     otherwise.
     */
    @Override
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (!(obj instanceof RemoveProperty)) return false;
      RemoveProperty other = (RemoveProperty) obj;
      return Objects.equals(property, other.property);
    }

    /**
     * Generates a hash code for this RemoveProperty instance. The hash code is based on the
     * property name that is to be removed from the fileset.
     *
     * @return A hash code value for this property removal operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(property);
    }

    /**
     * Provides a string representation of the RemoveProperty instance. This string format includes
     * the class name followed by the property name to be removed.
     *
     * @return A string summary of the property removal operation.
     */
    @Override
    public String toString() {
      return "RemoveProperty " + property;
    }
  }

  /** A ModelChange to update the comment of a model. */
  final class UpdateComment implements ModelChange {
    private final String newComment;

    /**
     * Constructs a new {@link UpdateComment} instance with the specified new comment.
     *
     * @param newComment The new comment of the model.
     */
    public UpdateComment(String newComment) {
      this.newComment = newComment;
    }

    /**
     * Retrieves the new comment for the model.
     *
     * @return The new comment of the model.
     */
    public String newComment() {
      return newComment;
    }

    /**
     * Compares this UpdateComment instance with another object for equality. The comparison is
     * based on the new comment of the model.
     *
     * @param obj The object to compare with this instance.
     * @return {@code true} if the given object represents the same model comment update; {@code
     *     false} otherwise.
     */
    @Override
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (!(obj instanceof UpdateComment)) return false;
      UpdateComment other = (UpdateComment) obj;
      return Objects.equals(newComment, other.newComment);
    }

    /**
     * Generates a hash code for this UpdateComment instance. The hash code is based on the new
     * comment of the model.
     *
     * @return A hash code value for this model comment update operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(newComment);
    }

    /**
     * Provides a string representation of the UpdateComment instance. This string format includes
     * the class name followed by the new comment of the model.
     *
     * @return A string summary of the model comment update operation.
     */
    @Override
    public String toString() {
      return "UpdateComment " + newComment;
    }
  }
}
