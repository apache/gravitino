package com.datastrato.graviton;

import lombok.EqualsAndHashCode;
import lombok.Getter;

public interface CatalogChange {

  static CatalogChange rename(String newName) {
    return new RenameCatalog(newName);
  }

  static CatalogChange updateComment(String newComment) {
    return new UpdateCatalogComment(newComment);
  }

  static CatalogChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  static CatalogChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  @Getter
  @EqualsAndHashCode
  final class RenameCatalog implements CatalogChange {
    private final String newName;

    private RenameCatalog(String newName) {
      this.newName = newName;
    }
  }

  @Getter
  @EqualsAndHashCode
  final class UpdateCatalogComment implements CatalogChange {
    private final String newComment;

    private UpdateCatalogComment(String newComment) {
      this.newComment = newComment;
    }
  }

  @Getter
  final class SetProperty implements CatalogChange {
    private final String property;
    private final String value;

    private SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }
  }

  final class RemoveProperty implements CatalogChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }
  }
}
