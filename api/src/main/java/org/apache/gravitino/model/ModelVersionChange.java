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
 * A model version change is a change to a model version. It can be used to update uri of a model
 * version, update the comment of a model, set a property and value pair for a model version, or
 * remove a property from a model.
 */
@Evolving
public interface ModelVersionChange {

  /**
   * Create a ModelVersionChange for updating the comment of a model version.
   *
   * @param newComment new comment to be set for the model version
   * @return A new ModelVersionChange instance for updating the comment of a model version
   */
  static ModelVersionChange updateComment(String newComment) {
    return new ModelVersionChange.UpdateComment(newComment);
  }

  /**
   * Create a ModelVersionChange for setting a property of a model version.
   *
   * @param property name of the property to be set
   * @param value value to be set for the property
   * @return A new ModelVersionChange instance for setting a property of a model version
   */
  static ModelVersionChange setProperty(String property, String value) {
    return new ModelVersionChange.SetProperty(property, value);
  }

  /**
   * Create a ModelVersionChange for removing a property from a model version.
   *
   * @param property The name of the property to be removed.
   * @return The new ModelVersionChange instance for removing a property from a model version
   */
  static ModelVersionChange removeProperty(String property) {
    return new ModelVersionChange.RemoveProperty(property);
  }

  /** A ModelVersionChange to update the model version comment. */
  final class UpdateComment implements ModelVersionChange {

    private final String newComment;

    /**
     * Creates a new {@link UpdateComment} instance with the specified new comment.
     *
     * @param newComment new comment to be set for the model version
     */
    public UpdateComment(String newComment) {
      this.newComment = newComment;
    }

    /**
     * Returns the new comment to be set for the model version.
     *
     * @return the new comment to be set for the model version
     */
    public String newComment() {
      return newComment;
    }

    /**
     * Compares this {@link UpdateComment} instance with another object for equality. The comparison
     * is based on the new comment of the model version.
     *
     * @param obj The object to compare with this instance.
     * @return {@code true} if the given object represents the same model update operation; {@code
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
     * Generates a hash code for this {@link UpdateComment} instance. The hash code is based on the
     * new comment of the model.
     *
     * @return A hash code value for this model renaming operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(newComment);
    }

    /**
     * Returns a string representation of the {@link UpdateComment} instance. This string format
     * includes the class name followed by the comment to be updated.
     *
     * @return A string summary of the {@link UpdateComment} instance.
     */
    @Override
    public String toString() {
      return "UpdateComment " + newComment;
    }
  }

  /** A ModelVersionChange to set a property of a model version. */
  final class SetProperty implements ModelVersionChange {
    private final String property;
    private final String value;

    /**
     * Creates a new {@link SetProperty} instance with the specified property name and value.
     *
     * @param property name of the property to be set
     * @param value value to be set for the property
     */
    public SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /**
     * Returns the name of the property to be set.
     *
     * @return the name of the property to be set
     */
    public String property() {
      return property;
    }

    /**
     * Returns the value to be set for the property.
     *
     * @return the value to be set for the property
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
      if (!(obj instanceof ModelVersionChange.SetProperty)) return false;
      ModelVersionChange.SetProperty other = (ModelVersionChange.SetProperty) obj;
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

  /** A ModelVersionChange to remove a property from a model version. */
  final class RemoveProperty implements ModelVersionChange {
    private final String property;

    /**
     * Creates a new {@link RemoveProperty} instance with the specified property name.
     *
     * @param property name of the property to be removed
     */
    public RemoveProperty(String property) {
      this.property = property;
    }

    /**
     * Returns the name of the property to be removed.
     *
     * @return the name of the property to be removed
     */
    public String property() {
      return property;
    }

    /**
     * Compares this RemoveProperty instance with another object for equality. Two instances are
     * considered equal if they target the same property.
     *
     * @param obj The object to compare with this instance.
     * @return {@code true} if the given object represents the same property removal; {@code false}
     *     otherwise.
     */
    @Override
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (!(obj instanceof ModelVersionChange.RemoveProperty)) return false;
      ModelVersionChange.RemoveProperty other = (ModelVersionChange.RemoveProperty) obj;
      return Objects.equals(property, other.property);
    }

    /**
     * Generates a hash code for this RemoveProperty instance. The hash code is based on the
     * property name to be removed.
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
      return "REMOVEPROPERTY " + property;
    }
  }
}
