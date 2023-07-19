/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import lombok.EqualsAndHashCode;
import lombok.Getter;

public interface CatalogChange {

  /**
   * Creates a new catalog change to rename the catalog.
   *
   * @param newName the new name of the catalog
   * @return the catalog change
   */
  static CatalogChange rename(String newName) {
    return new RenameCatalog(newName);
  }

  /**
   * Creates a new catalog change to update the catalog comment.
   *
   * @param newComment the new comment of the catalog
   * @return the catalog change
   */
  static CatalogChange updateComment(String newComment) {
    return new UpdateCatalogComment(newComment);
  }

  /**
   * Creates a new catalog change to set the property and value for the catalog.
   *
   * @param property the property name to set
   * @param value the property value to set
   * @return the catalog change
   */
  static CatalogChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  /**
   * Creates a new catalog change to remove the property from the catalog.
   *
   * @param property the property name to remove
   * @return the catalog change
   */
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
