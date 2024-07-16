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

package org.apache.gravitino.tag;

import org.apache.gravitino.annotation.Evolving;

/**
 * Interface for supporting tag changes. This interface will be used to provide tag modification
 * operations for each tag.
 */
@Evolving
public interface TagChange {

  /**
   * Creates a new tag change to rename the tag.
   *
   * @param newName The new name of the tag.
   * @return The tag change.
   */
  static TagChange rename(String newName) {
    return new RenameTag(newName);
  }

  /**
   * Creates a new tag change to update the tag comment.
   *
   * @param newComment The new comment for the tag.
   * @return The tag change.
   */
  static TagChange updateComment(String newComment) {
    return new UpdateTagComment(newComment);
  }

  /**
   * Creates a new tag change to set the property and value for the tag.
   *
   * @param property The property name to set.
   * @param value The value to set the property to.
   * @return The tag change.
   */
  static TagChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  /**
   * Creates a new tag change to remove a property from the tag.
   *
   * @param property The property name to remove.
   * @return The tag change.
   */
  static TagChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  /** A tag change to rename the tag. */
  final class RenameTag implements TagChange {
    private final String newName;

    private RenameTag(String newName) {
      this.newName = newName;
    }

    /**
     * Get the new name of the tag.
     *
     * @return The new name of the tag.
     */
    public String getNewName() {
      return newName;
    }

    /**
     * Compare the RenameTag instance with another object for equality. Two instances are considered
     * equal if the new name is the same.
     *
     * @param o The object to compare with this instance.
     * @return True if the object is equal to this instance, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RenameTag renameTag = (RenameTag) o;
      return newName.equals(renameTag.newName);
    }

    /**
     * Generates a hash code for the RenameTag instance.
     *
     * @return The hash code for the RenameTag instance.
     */
    @Override
    public int hashCode() {
      return newName.hashCode();
    }

    /**
     * Get the string representation of the RenameTag instance.
     *
     * @return The string representation of the RenameTag instance.
     */
    @Override
    public String toString() {
      return "RENAMETAG " + newName;
    }
  }

  /** A tag change to update the tag comment. */
  final class UpdateTagComment implements TagChange {
    private final String newComment;

    private UpdateTagComment(String newComment) {
      this.newComment = newComment;
    }

    /**
     * Get the new comment intended for the tag.
     *
     * @return The new comment that will be set for the tag.
     */
    public String getNewComment() {
      return newComment;
    }

    /**
     * Compare the UpdateTagComment instance with another object for equality. Two instances are
     * considered equal if they designate the same new comment for the tag.
     *
     * @param o The object to compare with this instance.
     * @return True if the object is equal to this instance, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      UpdateTagComment updateTagComment = (UpdateTagComment) o;
      return newComment.equals(updateTagComment.newComment);
    }

    /**
     * Generates a hash code for the UpdateTagComment instance.
     *
     * @return The hash code for the UpdateTagComment instance.
     */
    @Override
    public int hashCode() {
      return newComment.hashCode();
    }

    /**
     * Get the string representation of the UpdateTagComment instance.
     *
     * @return The string representation of the UpdateTagComment instance.
     */
    @Override
    public String toString() {
      return "UPDATETAGCOMMENT " + newComment;
    }
  }

  /** A tag change to set the property and value for the tag. */
  final class SetProperty implements TagChange {
    private final String property;
    private final String value;

    private SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /**
     * Get the property name to set.
     *
     * @return The property name to set.
     */
    public String getProperty() {
      return property;
    }

    /**
     * Get the value to set the property to.
     *
     * @return The value to set the property to.
     */
    public String getValue() {
      return value;
    }

    /**
     * Compare the SetProperty instance with another object for equality. Two instances are
     * considered equal if they designate the same property and value.
     *
     * @param o The object to compare with this instance.
     * @return True if the object is equal to this instance, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SetProperty setProperty = (SetProperty) o;
      return property.equals(setProperty.property) && value.equals(setProperty.value);
    }

    /**
     * Generates a hash code for the SetProperty instance.
     *
     * @return The hash code for the SetProperty instance.
     */
    @Override
    public int hashCode() {
      return property.hashCode() + value.hashCode();
    }

    /**
     * Get the string representation of the SetProperty instance.
     *
     * @return The string representation of the SetProperty instance.
     */
    @Override
    public String toString() {
      return "SETTAGPROPERTY " + property + " = " + value;
    }
  }

  /** A tag change to remove a property from the tag. */
  final class RemoveProperty implements TagChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }

    /**
     * Get the property name to remove.
     *
     * @return The property name to remove.
     */
    public String getProperty() {
      return property;
    }

    /**
     * Compare the RemoveProperty instance with another object for equality. Two instances are
     * considered equal if they designate the same property to remove.
     *
     * @param o The object to compare with this instance.
     * @return True if the object is equal to this instance, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RemoveProperty removeProperty = (RemoveProperty) o;
      return property.equals(removeProperty.property);
    }

    /**
     * Generates a hash code for the RemoveProperty instance.
     *
     * @return The hash code for the RemoveProperty instance.
     */
    @Override
    public int hashCode() {
      return property.hashCode();
    }

    /**
     * Get the string representation of the RemoveProperty instance.
     *
     * @return The string representation of the RemoveProperty instance.
     */
    @Override
    public String toString() {
      return "REMOVETAGPROPERTY " + property;
    }
  }
}
