/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.file;

import com.datastrato.gravitino.annotation.Evolving;
import java.util.Objects;

/**
 * A fileset change is a change to a fileset. It can be used to rename a fileset, update the comment
 * of a fileset, set a property and value pair for a fileset, or remove a property from a fileset.
 */
@Evolving
public interface FilesetChange {

  /**
   * Creates a new fileset change to rename the fileset.
   *
   * @param newName The new name of the fileset.
   * @return The fileset change.
   */
  static FilesetChange rename(String newName) {
    return new RenameFileset(newName);
  }

  /**
   * Creates a new fileset change to update the fileset comment.
   *
   * @param newComment The new comment for the fileset.
   * @return The fileset change.
   */
  static FilesetChange updateComment(String newComment) {
    return new UpdateFilesetComment(newComment);
  }

  /**
   * Creates a new fileset change to set the property and value for the fileset.
   *
   * @param property The property name to set.
   * @param value The value to set the property to.
   * @return The fileset change.
   */
  static FilesetChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  /**
   * Creates a new fileset change to remove a property from the fileset.
   *
   * @param property The property name to remove.
   * @return The fileset change.
   */
  static FilesetChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  /** A fileset change to rename the fileset. */
  final class RenameFileset implements FilesetChange {
    private final String newName;

    private RenameFileset(String newName) {
      this.newName = newName;
    }

    /**
     * Retrieves the new name set for the fileset.
     *
     * @return The new name of the fileset.
     */
    public String getNewName() {
      return newName;
    }

    /**
     * Compares this RenameFileset instance with another object for equality. Two instances are
     * considered equal if they designate the same new name for the fileset.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents an identical fileset renaming operation; false
     *     otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RenameFileset that = (RenameFileset) o;
      return Objects.equals(newName, that.newName);
    }

    /**
     * Generates a hash code for this RenameFileset instance. The hash code is primarily based on
     * the new name for the fileset.
     *
     * @return A hash code value for this renaming operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(newName);
    }

    /**
     * Provides a string representation of the RenameFile instance. This string includes the class
     * name followed by the new name of the fileset.
     *
     * @return A string summary of this renaming operation.
     */
    @Override
    public String toString() {
      return "RENAMEFILESET " + newName;
    }
  }

  /** A fileset change to update the fileset comment. */
  final class UpdateFilesetComment implements FilesetChange {
    private final String newComment;

    private UpdateFilesetComment(String newComment) {
      this.newComment = newComment;
    }

    /**
     * Retrieves the new comment intended for the fileset.
     *
     * @return The new comment that has been set for the fileset.
     */
    public String getNewComment() {
      return newComment;
    }

    /**
     * Compares this UpdateFilesetComment instance with another object for equality. Two instances
     * are considered equal if they designate the same new comment for the fileset.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same comment update; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdateFilesetComment that = (UpdateFilesetComment) o;
      return Objects.equals(newComment, that.newComment);
    }

    /**
     * Generates a hash code for this UpdateFileComment instance. The hash code is based on the new
     * comment for the fileset.
     *
     * @return A hash code representing this comment update operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(newComment);
    }

    /**
     * Provides a string representation of the UpdateFilesetComment instance. This string format
     * includes the class name followed by the new comment for the fileset.
     *
     * @return A string summary of this comment update operation.
     */
    @Override
    public String toString() {
      return "UPDATEFILESETCOMMENT " + newComment;
    }
  }

  /** A fileset change to set the property and value for the fileset. */
  final class SetProperty implements FilesetChange {
    private final String property;
    private final String value;

    private SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /**
     * Retrieves the name of the property being set in the fileset.
     *
     * @return The name of the property.
     */
    public String getProperty() {
      return property;
    }

    /**
     * Retrieves the value assigned to the property in the fileset.
     *
     * @return The value of the property.
     */
    public String getValue() {
      return value;
    }

    /**
     * Compares this SetProperty instance with another object for equality. Two instances are
     * considered equal if they have the same property and value for the fileset.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same property setting; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SetProperty that = (SetProperty) o;
      return Objects.equals(property, that.property) && Objects.equals(value, that.value);
    }

    /**
     * Generates a hash code for this SetProperty instance. The hash code is based on both the
     * property name and its assigned value.
     *
     * @return A hash code value for this property setting.
     */
    @Override
    public int hashCode() {
      return Objects.hash(property, value);
    }

    /**
     * Provides a string representation of the SetProperty instance. This string format includes the
     * class name followed by the property and its value.
     *
     * @return A string summary of the property setting.
     */
    @Override
    public String toString() {
      return "SETPROPERTY " + property + " " + value;
    }
  }

  /** A fileset change to remove a property from the fileset. */
  final class RemoveProperty implements FilesetChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }

    /**
     * Retrieves the name of the property to be removed from the fileset.
     *
     * @return The name of the property for removal.
     */
    public String getProperty() {
      return property;
    }

    /**
     * Compares this RemoveProperty instance with another object for equality. Two instances are
     * considered equal if they target the same property for removal from the fileset.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same property removal; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RemoveProperty that = (RemoveProperty) o;
      return Objects.equals(property, that.property);
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
      return "REMOVEPROPERTY " + property;
    }
  }
}
