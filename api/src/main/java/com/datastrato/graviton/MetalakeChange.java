/*·Copyright·2023·Datastrato.·This·software·is·licensed·under·the·Apache·License·version·2.·*/

package com.datastrato.graviton;

import lombok.EqualsAndHashCode;
import lombok.Getter;

public interface MetalakeChange {

  /**
   * Creates a new metalake change to rename the metalake.
   *
   * @param newNameTthe New name of the metalake.
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
