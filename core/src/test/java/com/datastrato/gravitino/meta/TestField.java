/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.meta;

import com.datastrato.gravitino.Field;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestField {

  @Test
  void testFieldRequired() {
    Field required = Field.required("test", String.class, "test");
    required.validate("test");
    Throwable exception =
        Assertions.assertThrows(IllegalArgumentException.class, () -> required.validate(null));
    Assertions.assertEquals("Field test is required", exception.getMessage());
  }

  @Test
  void testFieldOptional() {
    Field optional = Field.optional("test", String.class, "test");
    optional.validate("test");
    optional.validate(null);
  }

  @Test
  void testTypeUnmatched() {
    Field required = Field.required("test", String.class, "test");
    Throwable exception =
        Assertions.assertThrows(IllegalArgumentException.class, () -> required.validate(1));
    Assertions.assertEquals("Field test is not of type java.lang.String", exception.getMessage());

    Field optional = Field.optional("test1", Integer.class, "test");
    Throwable exception1 =
        Assertions.assertThrows(IllegalArgumentException.class, () -> optional.validate(1L));
    Assertions.assertEquals(
        "Field test1 is not of type java.lang.Integer", exception1.getMessage());
  }
}
