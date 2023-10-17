/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class TestField {

  @Test
  void testRequiredFieldCreation() {
    Field field = Field.required("age", Integer.class, "Age of the person");
    assertNotNull(field);
  }

  @Test
  void testOptionalFieldCreation() {
    Field field = Field.optional("email", String.class);
    assertNotNull(field);
  }

  @Test
  void testFieldValidation() {
    Field field = Field.required("name", String.class);

    assertDoesNotThrow(() -> field.validate("John Doe"));

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> field.validate(null));
    assertEquals("Field name is required", ex.getMessage());

    ex = assertThrows(IllegalArgumentException.class, () -> field.validate(42));
    assertEquals("Field name is not of type java.lang.String", ex.getMessage());
  }

  @Test
  void testBuilderMissingName() {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> Field.required(null, Integer.class));
    assertEquals("Field name is required", ex.getMessage());
  }

  @Test
  void testBuilderMissingTypeClass() {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> Field.optional("weight", null));
    assertEquals("Field type class is required", ex.getMessage());
  }
}
