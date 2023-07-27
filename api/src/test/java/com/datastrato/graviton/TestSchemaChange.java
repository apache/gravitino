/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import static org.junit.jupiter.api.Assertions.*;

import com.datastrato.graviton.rel.SchemaChange;
import com.datastrato.graviton.rel.SchemaChange.RemoveProperty;
import com.datastrato.graviton.rel.SchemaChange.SetProperty;
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
}
