/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.gravitino.SchemaChange.RemoveProperty;
import com.datastrato.gravitino.SchemaChange.SetProperty;
import org.junit.jupiter.api.Test;

public class TestSchemaChange {

  @Test
  void testSetProperty() {
    String property = "Jam";
    String value = "Strawberry";
    SetProperty change = (SetProperty) SchemaChange.setProperty(property, value);

    assertEquals(property, change.getProperty());
    assertEquals(value, change.getValue());
  }

  @Test
  void testRemoveProperty() {
    String property = "Jam";
    RemoveProperty change = (RemoveProperty) SchemaChange.removeProperty(property);

    assertEquals(property, change.getProperty());
  }

  @Test
  void testRemovePropertyEqualsAndHashCode() {
    String propertyA = "property A";
    RemoveProperty changeA = (RemoveProperty) SchemaChange.removeProperty(propertyA);
    String propertyB = "property A";
    RemoveProperty changeB = (RemoveProperty) SchemaChange.removeProperty(propertyB);

    assertTrue(changeA.equals(changeB));
    assertTrue(changeB.equals(changeA));
    assertEquals(changeA.hashCode(), changeB.hashCode());
  }

  @Test
  void testRemovePropertyNotEqualsAndHashCode() {
    String propertyA = "property A";
    RemoveProperty changeA = (RemoveProperty) SchemaChange.removeProperty(propertyA);
    String propertyB = "property B";
    RemoveProperty changeB = (RemoveProperty) SchemaChange.removeProperty(propertyB);

    assertFalse(changeA.equals(null));
    assertFalse(changeA.equals(changeB));
    assertFalse(changeB.equals(changeA));
    assertNotEquals(changeA.hashCode(), changeB.hashCode());
  }

  @Test
  void testSetPropertyEqualsAndHashCode() {
    String propertyA = "property A";
    String valueA = "A";
    SetProperty changeA = (SetProperty) SchemaChange.setProperty(propertyA, valueA);
    String propertyB = "property A";
    String valueB = "A";
    SetProperty changeB = (SetProperty) SchemaChange.setProperty(propertyB, valueB);

    assertTrue(changeA.equals(changeB));
    assertTrue(changeB.equals(changeA));
    assertEquals(changeA.hashCode(), changeB.hashCode());
  }

  @Test
  void testSetPropertyNotEqualsAndHashCode() {
    String propertyA = "property A";
    String valueA = "A";
    SetProperty changeA = (SetProperty) SchemaChange.setProperty(propertyA, valueA);
    String propertyB = "property B";
    String valueB = "B";
    SetProperty changeB = (SetProperty) SchemaChange.setProperty(propertyB, valueB);

    assertFalse(changeA.equals(null));
    assertFalse(changeA.equals(changeB));
    assertFalse(changeB.equals(changeA));
    assertNotEquals(changeA.hashCode(), changeB.hashCode());
  }
}
