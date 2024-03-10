/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.annotation.Evolving;
import java.util.Objects;

/**
 * A catalog change is a change to a catalog. It can be used to rename a catalog, update the comment
 * of a catalog, set a property and value pair for a catalog, or remove a property from a catalog.
 */
@Evolving
public interface CatalogChange {

  /**
   * Creates a new catalog change to rename the catalog.
   *
   * @param newName The new name of the catalog.
   * @return The catalog change.
   */
  static CatalogChange rename(String newName) {
    return new RenameCatalog(newName);
  }

  /**
   * Creates a new catalog change to update the catalog comment.
   *
   * @param newComment The new comment for the catalog.
   * @return The catalog change.
   */
  static CatalogChange updateComment(String newComment) {
    return new UpdateCatalogComment(newComment);
  }

  /**
   * Creates a new catalog change to set the property and value for the catalog.
   *
   * @param property The property name to set.
   * @param value The value to set the property to.
   * @return The catalog change.
   */
  static CatalogChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  /**
   * Creates a new catalog change to remove a property from the catalog.
   *
   * @param property The property name to remove.
   * @return The catalog change.
   */
  static CatalogChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  /** A catalog change to rename the catalog. */
  final class RenameCatalog implements CatalogChange {
    private final String newName;

    private RenameCatalog(String newName) {
      this.newName = newName;
    }

    /**
     * Retrieves the new name set for the catalog.
     *
     * @return The new name of the catalog.
     */
    public String getNewName() {
      return newName;
    }

    /**
     * Compares this RenameCatalog instance with another object for equality. Two instances are
     * considered equal if they designate the same new name for the catalog.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents an identical catalog renaming operation; false
     *     otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RenameCatalog that = (RenameCatalog) o;
      return Objects.equals(newName, that.newName);
    }

    /**
     * Generates a hash code for this RenameCatalog instance. The hash code is primarily based on
     * the new name for the catalog.
     *
     * @return A hash code value for this renaming operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(newName);
    }

    /**
     * Provides a string representation of the RenameCatalog instance. This string includes the
     * class name followed by the new name of the catalog.
     *
     * @return A string summary of this renaming operation.
     */
    @Override
    public String toString() {
      return "RENAMECATALOG " + newName;
    }
  }

  /** A catalog change to update the catalog comment. */
  final class UpdateCatalogComment implements CatalogChange {
    private final String newComment;

    private UpdateCatalogComment(String newComment) {
      this.newComment = newComment;
    }

    /**
     * Retrieves the new comment intended for the catalog.
     *
     * @return The new comment that has been set for the catalog.
     */
    public String getNewComment() {
      return newComment;
    }

    /**
     * Compares this UpdateCatalogComment instance with another object for equality. Two instances
     * are considered equal if they designate the same new comment for the catalog.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same comment update; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdateCatalogComment that = (UpdateCatalogComment) o;
      return Objects.equals(newComment, that.newComment);
    }

    /**
     * Generates a hash code for this UpdateCatalogComment instance. The hash code is based on the
     * new comment for the catalog.
     *
     * @return A hash code representing this comment update operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(newComment);
    }

    /**
     * Provides a string representation of the UpdateCatalogComment instance. This string format
     * includes the class name followed by the new comment for the catalog.
     *
     * @return A string summary of this comment update operation.
     */
    @Override
    public String toString() {
      return "UPDATECATALOGCOMMENT " + newComment;
    }
  }

  /** A catalog change to set the property and value for the catalog. */
  final class SetProperty implements CatalogChange {
    private final String property;
    private final String value;

    private SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /**
     * Retrieves the name of the property being set in the catalog.
     *
     * @return The name of the property.
     */
    public String getProperty() {
      return property;
    }

    /**
     * Retrieves the value assigned to the property in the catalog.
     *
     * @return The value of the property.
     */
    public String getValue() {
      return value;
    }

    /**
     * Compares this SetProperty instance with another object for equality. Two instances are
     * considered equal if they have the same property and value for the catalog.
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

  /** A catalog change to remove a property from the catalog. */
  final class RemoveProperty implements CatalogChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }

    /**
     * Retrieves the name of the property to be removed from the catalog.
     *
     * @return The name of the property for removal.
     */
    public String getProperty() {
      return property;
    }

    /**
     * Compares this RemoveProperty instance with another object for equality. Two instances are
     * considered equal if they target the same property for removal from the catalog.
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
     * property name that is to be removed from the catalog.
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
