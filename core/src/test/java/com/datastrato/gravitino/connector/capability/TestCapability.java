/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector.capability;

import static com.datastrato.gravitino.Entity.SECURABLE_ENTITY_RESERVED_NAME;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCapability {

  @Test
  void testDefaultNameSpecification() {
    for (Capability.Scope scope : Capability.Scope.values()) {
      // test for normal name
      CapabilityResult result = Capability.DEFAULT.specificationOnName(scope, "_name_123_/_=-");
      Assertions.assertTrue(result.supported());

      result = Capability.DEFAULT.specificationOnName(scope, "name_123_/_=-");
      Assertions.assertTrue(result.supported());

      result = Capability.DEFAULT.specificationOnName(scope, "Name_123_/_=-");
      Assertions.assertTrue(result.supported());

      // test for reserved name
      result = Capability.DEFAULT.specificationOnName(scope, SECURABLE_ENTITY_RESERVED_NAME);
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("is reserved"));

      // test for illegal name
      result = Capability.DEFAULT.specificationOnName(scope, "name with space");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("is illegal"));

      result = Capability.DEFAULT.specificationOnName(scope, "name_with_@");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("is illegal"));

      result = Capability.DEFAULT.specificationOnName(scope, "name_with_#");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("is illegal"));

      result = Capability.DEFAULT.specificationOnName(scope, "name_with_$");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("is illegal"));

      result = Capability.DEFAULT.specificationOnName(scope, "name_with_%");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("is illegal"));

      result = Capability.DEFAULT.specificationOnName(scope, "-name_start_with-");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("is illegal"));

      result = Capability.DEFAULT.specificationOnName(scope, "/name_start_with/");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("is illegal"));

      result = Capability.DEFAULT.specificationOnName(scope, "=name_start_with=");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("is illegal"));

      // test for long name
      StringBuilder longName = new StringBuilder();
      for (int i = 0; i < 64; i++) {
        longName.append("a");
      }

      Assertions.assertEquals(64, longName.length());
      result = Capability.DEFAULT.specificationOnName(scope, longName.toString());
      Assertions.assertTrue(result.supported());

      longName.append("a");
      Assertions.assertEquals(65, longName.length());
      result = Capability.DEFAULT.specificationOnName(scope, longName.toString());
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("is illegal"));
    }
  }
}
