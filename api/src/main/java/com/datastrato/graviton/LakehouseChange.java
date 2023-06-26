package com.datastrato.graviton;

import lombok.EqualsAndHashCode;
import lombok.Getter;

public interface LakehouseChange {

  /**
   * Creates a new lakehouse change to rename the lakehouse.
   *
   * @param newName the new name of the lakehouse
   * @return the lakehouse change
   */
  static LakehouseChange rename(String newName) {
    return new RenameLakehouse(newName);
  }

  /**
   * Creates a new lakehouse change to update the lakehouse comment.
   *
   * @param newComment the new comment of the lakehouse
   * @return the lakehouse change
   */
  static LakehouseChange updateComment(String newComment) {
    return new UpdateLakehouseComment(newComment);
  }

  /**
   * Creates a new lakehouse change to set the property and value for the lakehouse.
   *
   * @param property the property name to set
   * @param value the property value to set
   * @return the lakehouse change
   */
  static LakehouseChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  /**
   * Creates a new lakehouse change to remove the property from the lakehouse.
   *
   * @param property the property name to remove
   * @return the lakehouse change
   */
  static LakehouseChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  @Getter
  @EqualsAndHashCode
  final class RenameLakehouse implements LakehouseChange {
    private final String newName;

    private RenameLakehouse(String newName) {
      this.newName = newName;
    }
  }

  @Getter
  @EqualsAndHashCode
  final class UpdateLakehouseComment implements LakehouseChange {
    private final String newComment;

    private UpdateLakehouseComment(String newComment) {
      this.newComment = newComment;
    }
  }

  @Getter
  @EqualsAndHashCode
  final class SetProperty implements LakehouseChange {
    private final String property;
    private final String value;

    private SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }
  }

  @Getter
  @EqualsAndHashCode
  final class RemoveProperty implements LakehouseChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }
  }
}
