package com.datastrato.graviton.meta;

import com.datastrato.graviton.EntityChange;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Accessors;

public interface LakehouseChange extends EntityChange {

  static LakehouseChange rename(String newName) {
    return new RenameLakehouse(newName);
  }

  static LakehouseChange updateComment(String newComment) {
    return new UpdateLakehouseComment(newComment);
  }

  static LakehouseChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  static LakehouseChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  @Getter
  @Accessors(fluent = true)
  @EqualsAndHashCode
  final class RenameLakehouse implements LakehouseChange {
    private final String newName;

    private RenameLakehouse(String newName) {
      this.newName = newName;
    }
  }

  @Getter
  @Accessors(fluent = true)
  @EqualsAndHashCode
  final class UpdateLakehouseComment implements LakehouseChange {
    private final String newComment;

    private UpdateLakehouseComment(String newComment) {
      this.newComment = newComment;
    }
  }

  @Getter
  @Accessors(fluent = true)
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
  @Accessors(fluent = true)
  @EqualsAndHashCode
  final class RemoveProperty implements LakehouseChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }
  }
}
