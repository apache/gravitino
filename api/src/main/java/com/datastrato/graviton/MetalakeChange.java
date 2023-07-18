/*
* Copyright 2023 Datastrato.
* This software is licensed under the Apache License version 2.
*/

package com.datastrato.graviton;

import lombok.EqualsAndHashCode;
import lombok.Getter;

public interface MetalakeChange {

  /**
   * Creates a new metalake change to rename the metalake.
   *
   * @param newName the new name of the metalake
   * @return the metalake change
   */
  static MetalakeChange rename(String newName) {
    return new RenameMetalake(newName);
  }

  /**
   * Creates a new metalake change to update the metalake comment.
   *
   * @param newComment the new comment of the metalake
   * @return the metalake change
   */
  static MetalakeChange updateComment(String newComment) {
    return new UpdateMetalakeComment(newComment);
  }

  /**
   * Creates a new metalake change to set the property and value for the metalake.
   *
   * @param property the property name to set
   * @param value the property value to set
   * @return the metalake change
   */
  static MetalakeChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  /**
   * Creates a new metalake change to remove the property from the metalake.
   *
   * @param property the property name to remove
   * @return the metalake change
   */
  static MetalakeChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  @Getter
  @EqualsAndHashCode
  final class RenameMetalake implements MetalakeChange {
    private final String newName;

    private RenameMetalake(String newName) {
      this.newName = newName;
    }
  }

  @Getter
  @EqualsAndHashCode
  final class UpdateMetalakeComment implements MetalakeChange {
    private final String newComment;

    private UpdateMetalakeComment(String newComment) {
      this.newComment = newComment;
    }
  }

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

  @Getter
  @EqualsAndHashCode
  final class RemoveProperty implements MetalakeChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }
  }
}
