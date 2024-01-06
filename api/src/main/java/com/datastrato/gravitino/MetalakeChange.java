/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Objects;

/**
 * A metalake change is a change to a metalake. It can be used to rename a metalake, update the
 * comment of a metalake, set a property and value pair for a metalake, or remove a property from a
 * metalake.
 */
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
  @Getter
  @EqualsAndHashCode
  final class RenameMetalake implements MetalakeChange {
    private final String newName;

    private RenameMetalake(String newName) {
      this.newName = newName;
    }
  }

  /** A metalake change to update the metalake comment. */
  final class UpdateMetalakeComment implements MetalakeChange {
    private final String newComment;

    private UpdateMetalakeComment(String newComment) {
      this.newComment = newComment;
    }

    public String getNewComment() {
      return newComment;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdateMetalakeComment that = (UpdateMetalakeComment) o;
      return Objects.equals(newComment, that.newComment);
    }

    @Override
    public int hashCode() {
      return Objects.hash(newComment);
    }

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

    public String getProperty() {
      return property;
    }

    public String getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SetProperty that = (SetProperty) o;
      return Objects.equals(property, that.property) &&
              Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(property, value);
    }

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

    public String getProperty() {
      return property;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RemoveProperty that = (RemoveProperty) o;
      return Objects.equals(property, that.property);
    }

    @Override
    public int hashCode() {
      return Objects.hash(property);
    }

    @Override
    public String toString() {
      return "REMOVEPROPERTY " + property;
    }
  }
}
