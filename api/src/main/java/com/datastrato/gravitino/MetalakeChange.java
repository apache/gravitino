/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.annotation.Evolving;
import java.util.Objects;

/**
 * A metalake change is a change to a metalake. It can be used to rename a metalake, update the
 * comment of a metalake, set a property and value pair for a metalake, or remove a property from a
 * metalake.
 */
@Evolving
public interface MetalakeChange {

  /**
   * Creates a new metalake change to rename the metalake.
   *
   * @param newName The New name of the metalake.
   * @return The metalake change.
   */
  static MetalakeChange rename(String newName) {
    return new RenameMetalake(newName);
  }

  /**
   * Creates a new metalake change to update the metalake comment.
   *
   * @param newComment The new comment of the metalake.
   * @return The metalake change.
   */
  static MetalakeChange updateComment(String newComment) {
    return new UpdateMetalakeComment(newComment);
  }

  /**
   * Creates a new metalake change to set a property and value pair for the metalake.
   *
   * @param property The property name to set.
   * @param value The value to set the property to.
   * @return The metalake change.
   */
  static MetalakeChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  /**
   * Creates a new metalake change to remove a property from the metalake.
   *
   * @param property The property name to remove.
   * @return The metalake change.
   */
  static MetalakeChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  /** A metalake change to rename the metalake. */
  final class RenameMetalake implements MetalakeChange {
    private final String newName;

    private RenameMetalake(String newName) {
      this.newName = newName;
    }

    /**
     * Retrieves the new name intended for the Metalake.
     *
     * @return The new name to be set for the Metalake.
     */
    public String getNewName() {
      return newName;
    }

    /**
     * Compares this RenameMetalake instance with another object for equality.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents an identical renaming operation; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RenameMetalake that = (RenameMetalake) o;
      return Objects.equals(newName, that.newName);
    }

    /**
     * Generates a hash code for this RenameMetalake instance.
     *
     * @return A hash code representing this renaming operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(newName);
    }

    /**
     * Provides a string representation of the RenameMetalake instance. This string includes the
     * class name and the new name set for the Metalake
     *
     * @return A string summary of this renaming operation.
     */
    @Override
    public String toString() {
      return "RENAMEMETALAKE " + newName;
    }
  }

  /** A metalake change to update the metalake comment. */
  final class UpdateMetalakeComment implements MetalakeChange {
    private final String newComment;

    private UpdateMetalakeComment(String newComment) {
      this.newComment = newComment;
    }

    /**
     * Retrieves the new comment intended for the Metalake.
     *
     * @return The new comment.
     */
    public String getNewComment() {
      return newComment;
    }

    /**
     * Compares this UpdateMetalakeComment instance with another object for equality.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object is an UpdateMetalakeComment with the same new comment; false
     *     otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdateMetalakeComment that = (UpdateMetalakeComment) o;
      return Objects.equals(newComment, that.newComment);
    }

    /**
     * Generates a hash code for this UpdateMetalakeComment instance.
     *
     * @return A hash code representing this updating operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(newComment);
    }

    /**
     * Provides a string representation of the UpdateMetalakeComment instance. This string format
     * includes the class name followed by the new comment.
     *
     * @return A string summary of the object.
     */
    @Override
    public String toString() {
      return "UPDATEMETALAKECOMMENT " + newComment;
    }
  }

  /** A metalake change to set a property and value pair for the metalake. */
  final class SetProperty implements MetalakeChange {
    private final String property;
    private final String value;

    private SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /**
     * Retrieves the name of the property.
     *
     * @return The name of the property set for the Metalake.
     */
    public String getProperty() {
      return property;
    }

    /**
     * Retrieves the value assigned to the property.
     *
     * @return The value of the property set for the Metalake.
     */
    public String getValue() {
      return value;
    }

    /**
     * Compares this SetProperty instance with another object for equality. Two instances are
     * considered equal if they have the same property name and value.
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
     * Generates a hash code for this SetProperty instance. This hash code is based on both the
     * property name and value.
     *
     * @return A hash code value for this property setting.
     */
    @Override
    public int hashCode() {
      return Objects.hash(property, value);
    }

    /**
     * Provides a string representation of the SetProperty instance. This string format includes the
     * class name followed by the property name and its value.
     *
     * @return A string summary of the property setting.
     */
    @Override
    public String toString() {
      return "SETPROPERTY " + property + " " + value;
    }
  }

  /** A metalake change to remove a property from the metalake. */
  final class RemoveProperty implements MetalakeChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }

    /**
     * Retrieves the name of the property to be removed.
     *
     * @return The name of the property for removal.
     */
    public String getProperty() {
      return property;
    }

    /**
     * Compares this RemoveProperty instance with another object for equality. Two instances are
     * considered equal if they target the same property for removal.
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
     * Generates a hash code for this RemoveProperty instance. This hash code is based on the
     * property name that is to be removed.
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
