/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import static org.junit.jupiter.api.Assertions.*;

import com.datastrato.graviton.CatalogChange.RemoveProperty;
import com.datastrato.graviton.CatalogChange.RenameCatalog;
import com.datastrato.graviton.CatalogChange.SetProperty;
import com.datastrato.graviton.CatalogChange.UpdateCatalogComment;
import org.junit.jupiter.api.Test;

public class TestCatalogChange {

  @Test
  void testRenameCatalog() {
    String newName = "New Catalog";
    RenameCatalog change = (RenameCatalog) CatalogChange.rename(newName);

    assertEquals(newName, change.getNewName());
  }

  @Test
  void testUpdateCatalogComment() {
    String newComment = "New comment";
    UpdateCatalogComment change = (UpdateCatalogComment) CatalogChange.updateComment(newComment);

    assertEquals(newComment, change.getNewComment());
  }

  @Test
  void testSetProperty() {
    String property = "Jam";
    String value = "Strawberry";
    SetProperty change = (SetProperty) CatalogChange.setProperty(property, value);

    assertEquals(property, change.getProperty());
    assertEquals(value, change.getValue());
  }

  @Test
  void testRemoveProperty() {
    String property = "Jam";
    RemoveProperty change = (RemoveProperty) CatalogChange.removeProperty(property);

    assertEquals(property, change.getProperty());
  }
}
