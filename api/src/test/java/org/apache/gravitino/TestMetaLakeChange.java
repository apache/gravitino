/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.MetalakeChange.RemoveProperty;
import org.apache.gravitino.MetalakeChange.RenameMetalake;
import org.apache.gravitino.MetalakeChange.SetProperty;
import org.apache.gravitino.MetalakeChange.UpdateMetalakeComment;
import org.junit.jupiter.api.Test;

public class TestMetaLakeChange {

  @Test
  void testRenameCatalog() {
    String newName = "New Metalake";
    RenameMetalake change = (RenameMetalake) MetalakeChange.rename(newName);

    assertEquals(newName, change.getNewName());
  }

  @Test
  void testUpdateCatalogComment() {
    String newComment = "New comment";
    UpdateMetalakeComment change = (UpdateMetalakeComment) MetalakeChange.updateComment(newComment);

    assertEquals(newComment, change.getNewComment());
  }

  @Test
  void testSetProperty() {
    String property = "Jam";
    String value = "Strawberry";
    SetProperty change = (SetProperty) MetalakeChange.setProperty(property, value);

    assertEquals(property, change.getProperty());
    assertEquals(value, change.getValue());
  }

  @Test
  void testRemoveProperty() {
    String property = "Jam";
    RemoveProperty change = (RemoveProperty) MetalakeChange.removeProperty(property);

    assertEquals(property, change.getProperty());
  }

  @Test
  void testRenameEqualsAndHashCode() {
    String nameA = "Metalake";
    RenameMetalake change1 = (RenameMetalake) MetalakeChange.rename(nameA);
    String nameB = "Metalake";
    RenameMetalake change2 = (RenameMetalake) MetalakeChange.rename(nameB);

    assertTrue(change1.equals(change2));
    assertTrue(change2.equals(change1));
    assertEquals(change1.hashCode(), change2.hashCode());
  }

  @Test
  void testRenameNotEqualsAndHashCode() {
    String nameA = "Metalake";
    RenameMetalake change1 = (RenameMetalake) MetalakeChange.rename(nameA);
    String nameB = "New Metalake";
    RenameMetalake change2 = (RenameMetalake) MetalakeChange.rename(nameB);

    assertFalse(change1.equals(null));
    assertFalse(change1.equals(change2));
    assertFalse(change2.equals(change1));
    assertNotEquals(change1.hashCode(), change2.hashCode());
  }

  @Test
  void testUpdateEqualsAndHashCode() {
    String commentA = "a comment";
    UpdateMetalakeComment update1 = (UpdateMetalakeComment) MetalakeChange.updateComment(commentA);
    String commentB = "a comment";
    UpdateMetalakeComment update2 = (UpdateMetalakeComment) MetalakeChange.updateComment(commentB);

    assertTrue(update1.equals(update2));
    assertTrue(update2.equals(update1));
    assertEquals(update1.hashCode(), update2.hashCode());
  }

  @Test
  void testUpdateNotEqualsAndHashCode() {
    String commentA = "a comment";
    UpdateMetalakeComment update1 = (UpdateMetalakeComment) MetalakeChange.updateComment(commentA);
    String commentB = "a new comment";
    UpdateMetalakeComment update2 = (UpdateMetalakeComment) MetalakeChange.updateComment(commentB);

    assertFalse(update1.equals(null));
    assertFalse(update1.equals(update2));
    assertFalse(update2.equals(update1));
    assertNotEquals(update1.hashCode(), update2.hashCode());
  }

  @Test
  void testRemovePropertyEqualsAndHashCode() {
    String propertyA = "property A";
    RemoveProperty changeA = (RemoveProperty) MetalakeChange.removeProperty(propertyA);
    String propertyB = "property A";
    RemoveProperty changeB = (RemoveProperty) MetalakeChange.removeProperty(propertyB);

    assertTrue(changeA.equals(changeB));
    assertTrue(changeB.equals(changeA));
    assertEquals(changeA.hashCode(), changeB.hashCode());
  }

  @Test
  void testRemovePropertyNotEqualsAndHashCode() {
    String propertyA = "property A";
    RemoveProperty changeA = (RemoveProperty) MetalakeChange.removeProperty(propertyA);
    String propertyB = "property B";
    RemoveProperty changeB = (RemoveProperty) MetalakeChange.removeProperty(propertyB);

    assertFalse(changeA.equals(null));
    assertFalse(changeA.equals(changeB));
    assertFalse(changeB.equals(changeA));
    assertNotEquals(changeA.hashCode(), changeB.hashCode());
  }

  @Test
  void testSetPropertyEqualsAndHashCode() {
    String propertyA = "property A";
    String valueA = "A";
    SetProperty changeA = (SetProperty) MetalakeChange.setProperty(propertyA, valueA);
    String propertyB = "property A";
    String valueB = "A";
    SetProperty changeB = (SetProperty) MetalakeChange.setProperty(propertyB, valueB);

    assertTrue(changeA.equals(changeB));
    assertTrue(changeB.equals(changeA));
    assertEquals(changeA.hashCode(), changeB.hashCode());
  }

  @Test
  void testSetPropertyNotEqualsAndHashCode() {
    String propertyA = "property A";
    String valueA = "A";
    SetProperty changeA = (SetProperty) MetalakeChange.setProperty(propertyA, valueA);
    String propertyB = "property B";
    String valueB = "B";
    SetProperty changeB = (SetProperty) MetalakeChange.setProperty(propertyB, valueB);

    assertFalse(changeA.equals(null));
    assertFalse(changeA.equals(changeB));
    assertFalse(changeB.equals(changeA));
    assertNotEquals(changeA.hashCode(), changeB.hashCode());
  }
}
