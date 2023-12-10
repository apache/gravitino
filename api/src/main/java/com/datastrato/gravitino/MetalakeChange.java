/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import lombok.EqualsAndHashCode;
import lombok.Getter;

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
  @Getter
  @EqualsAndHashCode
  final class UpdateMetalakeComment implements MetalakeChange {
    private final String newComment;

    private UpdateMetalakeComment(String newComment) {
      this.newComment = newComment;
    }
  }

  /** A metalake change to set a property and value pair for the metalake. */
  @Getter
  @EqualsAndHashCode
  final class SetProperty implements MetalakeChange {
    private final String property;
    private final String value;

    private SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }
  }

  /** A metalake change to remove a property from the metalake. */
  @Getter
  @EqualsAndHashCode
  final class RemoveProperty implements MetalakeChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }
  }
}
