/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.gravitino.CatalogChange.RemoveProperty;
import com.datastrato.gravitino.CatalogChange.RenameCatalog;
import com.datastrato.gravitino.CatalogChange.SetProperty;
import com.datastrato.gravitino.CatalogChange.UpdateCatalogComment;
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

  @Test
  void testRenameEqualsAndHashCode() {
    String nameA = "Catalog";
    RenameCatalog change1 = (RenameCatalog) CatalogChange.rename(nameA);
    String nameB = "Catalog";
    RenameCatalog change2 = (RenameCatalog) CatalogChange.rename(nameB);

    assertTrue(change1.equals(change2));
    assertTrue(change2.equals(change1));
    assertEquals(change1.hashCode(), change2.hashCode());
  }

  @Test
  void testRenameNotEqualsAndHashCode() {
    String nameA = "Catalog";
    RenameCatalog change1 = (RenameCatalog) CatalogChange.rename(nameA);
    String nameB = "New Catalog";
    RenameCatalog change2 = (RenameCatalog) CatalogChange.rename(nameB);

    assertFalse(change1.equals(null));
    assertFalse(change1.equals(change2));
    assertFalse(change2.equals(change1));
    assertNotEquals(change1.hashCode(), change2.hashCode());
  }

  @Test
  void testUpdateEqualsAndHashCode() {
    String commentA = "a comment";
    UpdateCatalogComment update1 = (UpdateCatalogComment) CatalogChange.updateComment(commentA);
    String commentB = "a comment";
    UpdateCatalogComment update2 = (UpdateCatalogComment) CatalogChange.updateComment(commentB);

    assertTrue(update1.equals(update2));
    assertTrue(update2.equals(update1));
    assertEquals(update1.hashCode(), update2.hashCode());
  }

  @Test
  void testUpdateNotEqualsAndHashCode() {
    String commentA = "a comment";
    UpdateCatalogComment update1 = (UpdateCatalogComment) CatalogChange.updateComment(commentA);
    String commentB = "a new comment";
    UpdateCatalogComment update2 = (UpdateCatalogComment) CatalogChange.updateComment(commentB);

    assertFalse(update1.equals(null));
    assertFalse(update1.equals(update2));
    assertFalse(update2.equals(update1));
    assertNotEquals(update1.hashCode(), update2.hashCode());
  }
}
